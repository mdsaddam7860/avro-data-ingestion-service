import {
  processAvroFile,
  postToTarget,
  postToWorkato,
  processAvroHexToJson,
} from "../services/avroService.js";
import { logger } from "../index.js";
// import fs from "fs"; // Use 'const fs = require('fs')' if not using ESM
import fs from "fs/promises";

// Define the fields you want to keep (yellow-highlighted fields)
const yellowFields = [
  "event_id",
  "user_id",
  "session_id",
  "time",
  "type",
  "library",
  "platform",
  "device_type",
  "country",
  "region",
  "city",
  "referrer",
  "utm_source",
  "utm_campaign",
  "utm_medium",
  "utm_term",
  "utm_content",
  "email",
  "username",
  "phone2",
  "phone1",
  "lastname",
  "userid",
  "companyid",
  "firstname",
  "id",
];

const handleAvroWebhook = (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded." });
    }

    const filePath = req.file.path;
    const fileName = req.file.originalname;

    // 1. Send immediate response to the webhook provider
    res.status(202).json({
      message: "File received and processing started",
      fileName: fileName,
    });

    // 2. Trigger background processing WITHOUT 'await'
    // We wrap it in a self-executing function or just call it
    // to handle errors without crashing the main process.
    processInBackground(filePath);
  } catch (error) {
    logger.error("Webhook Error:", {
      status: error?.status,
      response: error?.response?.data || error?.response,
      message: error?.message,
      stack: error?.stack || error,
    });
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal Server Error" });
    }
  }
};

async function processInBackground(filePath) {
  try {
    const jsonData = await processAvroFile(filePath);

    // Safety check to prevent the 'length' error
    if (!jsonData || !Array.isArray(jsonData)) {
      throw new Error("Avro decoding returned invalid data structure.");
    }

    logger.info(`✅ Processed ${jsonData.length} records successfully`);

    const postToTargetResponse = await postToTarget(jsonData[0]);
    logger.info(
      `✅ Successfully sent ${JSON.stringify(
        postToTargetResponse,
        null,
        2
      )} records to Target.`
    );

    return jsonData;
  } catch (error) {
    logger.error("Webhook Error processInBackground:", {
      status: error?.status,
      message: error?.message,
      stack: error?.stack || error,
    });
  } finally {
    try {
      // Using fs/promises makes 'await' work correctly here
      await fs.unlink(filePath);
      logger.info(`🗑️ Temporary file deleted: ${filePath}`);
    } catch (unlinkError) {
      logger.warn(`⚠️ Cleanup failed: ${unlinkError.message}`);
    }
  }
}

// const handleHexAvroWebhook = async (req, res) => {
//   try {
//     // logger.info(`📤 Received Body ${JSON.stringify(req.body)}`);
//     const rawHex = req.body.hexData;

//     if (!rawHex || typeof rawHex !== "string") {
//       return res
//         .status(400)
//         .json({ error: "Invalid or missing 'hexData' field" });
//     }

//     res.status(200).json({
//       // success: true,
//       message: "File received and processing started",
//     });

//     processInBackgroundAvroToJson(rawHex);
//   } catch (error) {
//     logger.error(`Webhook Error handleHexAvroWebhook : `, {
//       status: error?.status,
//       response: error?.response?.data || error?.response,
//       message: error?.message,
//       stack: error?.stack || error,
//     });
//     res.status(500).json({ error: error.message });
//   }
// };

/**
 * CONTROLLER: Fast and defensive.
 */
const handleHexAvroWebhook = async (req, res) => {
  try {
    const body = req.body;
    const tableName = body?.table_name || null;
    const rawHex = body?.hexData;

    logger.info(`📤 Received Body: ${JSON.stringify(body)}`);

    // 1. Structural Validation
    if (!rawHex || typeof rawHex !== "string") {
      return res
        .status(400)
        .json({ error: "Invalid or missing 'hexData' field" });
    }

    // 2. Immediate "Magic Byte" Check (Gatekeeper)
    // We check the first 8 characters (4 bytes) before doing anything else
    const startOfData = rawHex.trim().substring(0, 8).toLowerCase();
    if (!startOfData.includes("4f626a01")) {
      logger.warn(`🚫 Rejected malformed payload. Header: ${startOfData}`);
      return res.status(400).json({
        error: "Payload is not a valid Avro file (Missing Magic Bytes).",
      });
    }

    // 3. Early Success Response
    res.status(200).json({
      message: "Payload validated. Processing has started.",
    });

    // 4. Fire and Forget with a Safety Net
    processInBackgroundAvroToJson(rawHex, tableName).catch((err) => {
      logger.error("🔥 Critical Background Failure:", err);
    });
  } catch (error) {
    logger.error(`Webhook Error handleHexAvroWebhook: ${error.message}`);
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal Server Error" });
    }
  }
};

/**
 * BACKGROUND SERVICE: Robust and detailed.
 */
async function processInBackgroundAvroToJson(rawHex, tableName) {
  try {
    // 1. Sanitize input (RegEx only allows Hex characters)
    const cleanHex = rawHex.replace(/[^0-9a-fA-F]/g, "");
    const avroBuffer = Buffer.from(cleanHex, "hex");

    if (avroBuffer.length < 16) {
      // Minimum size for Avro header + metadata
      throw new Error(`Buffer too small to be a valid Avro file.`);
    }

    // 2. Decode
    const jsonData = await processAvroHexToJson(avroBuffer);

    // first extract table name

    const extractTableName = extractTableNameFn(tableName);
    const data = modifyData(jsonData, extractTableName);

    // Modify/transform data here if needed before sending to target and add table_name in the record.

    logger.debug(`Data: ${JSON.stringify(data)}`);

    if (!data || data.length === 0) {
      logger.warn("⚠️ Avro decoded successfully but contains 0 records.");
      return;
    }

    let WORKATO_WEBHOOK_URL = null;
    if (extractTableName === "users") {
      WORKATO_WEBHOOK_URL = process.env.WORKATO_WEBHOOK_URL1;
    } else {
      WORKATO_WEBHOOK_URL = process.env.WORKATO_WEBHOOK_URL2;
    }

    // 3. Send to Target (Workato/HubSpot)
    // Wrap in a separate try/catch so a network error doesn't lose the local log of data
    try {
      const postToTargetResponse = await postToWorkato(
        WORKATO_WEBHOOK_URL,
        data
      );
      logger.info(
        `Successfully sent ${data.length} ${JSON.stringify(
          postToTargetResponse,
          null,
          2
        )} records to Target.`
      );
    } catch (apiError) {
      logger.error(`❌ Failed to send to Workato: ${apiError.message}`);
      // OPTIONAL: Save jsonData to a local 'failed_syncs' folder here for manual retry
    }
  } catch (error) {
    logger.error(`Background Job Error: ${error.message}`, {
      stack: error.stack,
    });
  }
}

function modifyData(jsonData, heapTableName2 = null) {
  let modifiedData = null;

  if (heapTableName2 === "users") {
    modifiedData = jsonData
      .filter((item) => item && item.user_email)
      .map((item) => ({
        // const modifiedItem = { ...item };
        // modifiedItem.table_name = heapTableName2;
        // return modifiedItem;
        ...item,
        table_name: heapTableName2,
      }));
  } else {
    // Modified function - keeps ONLY yellow fields + table_name
    modifiedData = jsonData.map((item) => {
      // Create new object with only yellow fields
      const modifiedItem = {};

      // Copy only yellow-highlighted fields from original item
      yellowFields.forEach((field) => {
        if (item.hasOwnProperty(field)) {
          modifiedItem[field] = item[field];
        }
      });

      // Add the table_name field
      modifiedItem.table_name = heapTableName2;

      return modifiedItem;
    });
  }
  return modifiedData;
}

// function extractTableNameFn(tableName) {
//   // Method 1: Regex
//   const match = tableName.match(/\/_heap_table_name=([^/]+)\//);
//   const heapTableName = match ? match[1] : null;
//   logger.info(`heapTableName: ${heapTableName}`); // product_create_estimate_click_save

//   // Method 2: Split
//   const heapTableName2 = tableName.split("/_heap_table_name=")[1].split("/")[0];
//   logger.info(`tableName: ${heapTableName2}`);
//   return heapTableName2;
// }
function extractTableNameFn(tableName) {
  // Add a safety check to ensure tableName exists and is a string
  if (!tableName || typeof tableName !== "string") {
    logger.error(
      `Invalid tableName provided to extractTableNameFn: ${tableName}`
    );
    return null; // Return null or a default value to prevent the crash
  }

  // Method 1: Regex
  const match = tableName.match(/\/_heap_table_name=([^/]+)\//);
  const heapTableName = match ? match[1] : null;
  logger.info(`heapTableName: ${heapTableName}`);

  // Method 2: Split (with an added safety check)
  if (tableName.includes("/_heap_table_name=")) {
    const heapTableName2 = tableName
      .split("/_heap_table_name=")[1]
      .split("/")[0];
    logger.info(`tableName: ${heapTableName2}`);
    return heapTableName2;
  }

  // Fallback if the specific string isn't found
  return heapTableName;
}
// async function processInBackgroundAvroToJson(rawHex) {
//   try {
//     // 1. CLEANING STEP: Remove spaces, colons, newlines, and non-hex junk
//     // This turns "4F 62 6A" into "4F626A"
//     const cleanHex = rawHex.replace(/[^0-9a-fA-F]/g, "");

//     // 2. CONVERSION
//     const avroBuffer = Buffer.from(cleanHex, "hex");

//     // 3. VALIDATION
//     if (avroBuffer.length < 4) {
//       logger.error(`Malformed Hex. Received: ${cleanHex.substring(0, 10)}...`);
//       throw new Error(
//         `Buffer size too small (${avroBuffer.length} bytes). Hex string is malformed.`
//       );
//     }

//     logger.info(
//       `✅ Successfully converted Hex to Buffer. Size: ${avroBuffer.length} bytes`
//     );

//     // 4. PROCESS (Ensure processAvroFile handles Buffers as discussed)
//     const jsonData = await processAvroHexToJson(avroBuffer);

//     // logger.info(
//     //   `✅ Processed ${JSON.stringify(
//     //     jsonData[0],
//     //     null,
//     //     2
//     //   )} records successfully`
//     // );

//     const postToTargetResponse = await postToWorkato(jsonData);
//     logger.info(
//       `✅ Successfully sent ${jsonData.length} ${JSON.stringify(
//         postToTargetResponse,
//         null,
//         2
//       )} records to Target.`
//     );
//     return jsonData;
//   } catch (error) {
//     logger.error(`Webhook Error processInBackground : `, {
//       status: error?.status,
//       response: error?.response?.data || error?.response,
//       message: error?.message,
//       stack: error?.stack || error,
//     });
//   }
// }

// status: error?.status,
// response: error?.response?.data || error?.response,
// message: error?.message,
// stack: error?.stack || error,
export { handleAvroWebhook, handleHexAvroWebhook };

// logger.error("Webhook Error:", {
//   httpStatus: error?.status,
//   message: error?.message,
//   stack: error?.stack,
// });
