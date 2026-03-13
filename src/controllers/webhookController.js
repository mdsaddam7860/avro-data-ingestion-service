import {
  processAvroFile,
  postToTarget,
  postToWorkato,
  processAvroHexToJson,
} from "../services/avroService.js";
import { logger } from "../index.js";
// import fs from "fs"; // Use 'const fs = require('fs')' if not using ESM
import fs from "fs/promises";

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
//     // logger.info(`📤 Received Body ${JSON.stringify(req.body, null, 2)}`);
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
    const rawHex = req.body.hexData;

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
    processInBackgroundAvroToJson(rawHex).catch((err) => {
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
async function processInBackgroundAvroToJson(rawHex) {
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

    if (!jsonData || jsonData.length === 0) {
      logger.warn("⚠️ Avro decoded successfully but contains 0 records.");
      return;
    }

    // 3. Send to Target (Workato/HubSpot)
    // Wrap in a separate try/catch so a network error doesn't lose the local log of data
    try {
      const postToTargetResponse = await postToWorkato(jsonData);
      logger.info(
        `✅ Successfully sent ${jsonData.length} ${JSON.stringify(
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
