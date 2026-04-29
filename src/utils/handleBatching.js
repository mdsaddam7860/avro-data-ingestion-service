import fs from "fs/promises";
import path from "path";
import axios from "axios";
import { logger } from "../index.js";
import { fileURLToPath } from "url";
import { postToWorkato } from "../services/avroService.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BATCH_SIZE = 4500;
const STORAGE_DIR = path.join(__dirname, "../../", "batch_storage");
// const pathDir = path.dirname(STORAGE_DIR);

try {
  await fs.access(STORAGE_DIR);
} catch (error) {
  // logger.error(`Error ensuring storage directory: ${error.message}`);
  await fs.mkdir(STORAGE_DIR);
}
// Ensure storage directory exists

/**
 * Handles the batching logic: saving to file and sending if threshold met.
 */
async function handleBatching(tableName, newData, webhookUrl) {
  const filePath = path.join(STORAGE_DIR, `${tableName}_batch.json`);
  let batch = [];

  try {
    // 1. Read existing data if the file exists
    const fileExists = await fs
      .access(filePath)
      .then(() => true)
      .catch(() => false);
    if (fileExists) {
      const currentData = await fs.readFile(filePath, "utf8");
      batch = JSON.parse(currentData);
    }

    // 2. Append new data
    batch = batch.concat(newData);
    logger.info(
      `📦 Batch for ${tableName} currently at: ${batch.length} records.`
    );

    // 3. Check if threshold is reached
    if (batch.length >= BATCH_SIZE) {
      logger.info(
        `🚀 Threshold reached (${batch.length}). Sending to Workato...`
      );

      // Send the data
      await postToWorkato(webhookUrl, batch);

      // 4. Clear the file after successful send
      await fs.unlink(filePath);
      logger.info(`Batch sent and file cleared for ${tableName}.`);
    } else {
      // 5. Save back to file to wait for next webhook
      await fs.writeFile(filePath, JSON.stringify(batch), "utf8");
      logger.info(`Data saved. Waiting for more records...`);
    }
  } catch (error) {
    logger.error(`Batching Error for ${tableName}: ${error.message}`);
    throw error;
  }
}

export { handleBatching };
