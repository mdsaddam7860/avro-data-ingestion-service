import {
  processAvroFile,
  processAvroBuffer,
  postToTarget,
  postToWorkato,
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
    logger.error("Webhook Error:", error.message);
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal Server Error" });
    }
  }
};

// const handleAvroWebhook = (req, res) => {
//   try {
//     if (!req.file || !req.file.buffer) {
//       return res.status(400).json({ error: "No file data received." });
//     }

//     // 1. Respond immediately
//     res.status(202).json({ message: "Processing started in memory" });

//     // 2. Process in background using the buffer
//     // No fs.unlink needed anymore!
//     processAvroBuffer(req.file.buffer)
//       .then(async (jsonData) => {
//         await postToWorkato(jsonData);
//         logger.info(
//           `✅ Successfully sent ${jsonData.length} records to Workato.`
//         );
//       })
//       .catch((err) => {
//         logger.error(`❌ Background Error: ${err.message}`);
//       });
//   } catch (error) {
//     logger.error("Webhook Setup Error:", error.message);
//   }
// };

// Internal helper to keep logic clean
// async function processInBackground(filePath) {
//   try {
//     const jsonData = await processAvroFile(filePath);

//     logger.info(
//       `Processed ${JSON.stringify(jsonData, null, 2)} records successfully`
//     );
//     // Add your HubSpot/API calls here
//     // const res = await postToWorkato(jsonData);
//     // logger.info(`✅ Workato response: ${JSON.stringify(res, null, 2)}`);
//   } catch (error) {
//     logger.error("Webhook Error processInBackground:", {
//       httpStatus: error?.status,
//       message: error?.message,
//       stack: error?.stack,
//     });
//     throw error;
//   } finally {
//     // 3. REMOVE THE FILE
//     try {
//       await fs.unlink(filePath);
//       logger.info(`🗑️ Temporary file deleted: ${filePath}`);
//     } catch (unlinkError) {
//       logger.warn(`⚠️ Failed to delete temp file: ${unlinkError.message}`);
//     }
//   }
// }

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
    // try {
    //   // Using fs/promises makes 'await' work correctly here
    //   await fs.unlink(filePath);
    //   logger.info(`🗑️ Temporary file deleted: ${filePath}`);
    // } catch (unlinkError) {
    //   logger.warn(`⚠️ Cleanup failed: ${unlinkError.message}`);
    // }
  }
}
export { handleAvroWebhook };

// logger.error("Webhook Error:", {
//   httpStatus: error?.status,
//   message: error?.message,
//   stack: error?.stack,
// });
