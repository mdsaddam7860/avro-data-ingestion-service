import fs from "fs";
import path from "path";
import { logger } from "../index.js";
import axios from "axios";

const flattenAvro = (obj) => {
  if (obj === null || typeof obj !== "object") return obj;

  const keys = Object.keys(obj);
  // Check if it's an Avro union wrapper
  if (
    keys.length === 1 &&
    ["string", "long", "int", "boolean", "double", "float"].includes(keys[0])
  ) {
    return flattenAvro(obj[keys[0]]);
  }

  const flattened = {};
  for (const [key, value] of Object.entries(obj)) {
    flattened[key] = flattenAvro(value);
  }
  return flattened;
};
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
export { flattenAvro, testWebhook };
