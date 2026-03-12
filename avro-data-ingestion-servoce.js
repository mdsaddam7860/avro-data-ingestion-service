import dotenv from "dotenv";
import path from "path";
import app from "./src/app.js";
import { logger } from "./src/index.js";
import { getHubspotClient } from "./src/configs/hubspot.config.js";
import fs from "fs";
import axios from "axios";

dotenv.config({
  path: path.join(process.cwd(), ".env"),
});

const PORT = process.env.PORT || 5000;

function serverInit() {
  try {
    // Start the Express listener
    app.listen(PORT, () => {
      logger.info(`🚀 Server running on PORT: ${PORT}`);
      logger.info(`🌍 Environment: ${process.env.NODE_ENV || "development"}`);

      // Initialize HubSpot right after the server starts
      // initServices();
    });
  } catch (error) {
    logger.error("❌ Critical startup failure:", error);
    process.exit(1);
  }
}

// async function initServices() {
//   try {
//     const client = getHubspotClient();
//     logger.info(`✅ HubSpot client initialized successfully`);
//   } catch (error) {
//     logger.error("❌ HubSpot client failed to initialize:", error);
//   }
// }

serverInit();

async function testWebhook() {
  try {
    const filePath = path.join(
      process.cwd(),
      "files",
      "avro_files",
      "part-00000-44e02b46-da18-46d4-bc22-071b6f4d6e73-c000.avro"
    );

    // 1. Read binary and convert to hex
    const buffer = fs.readFileSync(filePath);
    const hexString = buffer.toString("hex");

    logger.info(`📤 Sending ${buffer.length} bytes as hex...`);

    // logger.info(`📤 Sending ${hexString}`);

    // 2. Automatically POST to your local/ngrok server
    const response = await axios.post(
      "http://localhost:3243/webhook/avro-json",
      {
        hexData: hexString,
      }
    );

    console.log("✅ Server Response:", response.data);
  } catch (error) {
    console.error("❌ Test failed:", error.response?.data || error.message);
  }
}

// testWebhook();
