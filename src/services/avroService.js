import { logger } from "../index.js";
import avro from "avsc";
import SnappyJS from "snappyjs"; // Pure JS version
// import snappy from "snappy";
import fs from "fs";
import { flattenAvro } from "../utils/avroHelper.js";
import axios from "axios";
import { workatoExecutor } from "../utils/executors.js";
import { Readable } from "stream";

const codecs = {
  snappy: (buf, cb) => {
    try {
      // SnappyJS is synchronous, so we wrap it to satisfy the Avro callback
      // Still strip the 4-byte checksum at the end
      const decompressed = SnappyJS.uncompress(buf.slice(0, buf.length - 4));
      cb(null, decompressed);
    } catch (err) {
      cb(err);
    }
  },
};

const processAvroFile = async (filePath) => {
  const records = [];

  return new Promise((resolve, reject) => {
    // Create stream and decoder
    const fileStream = fs.createReadStream(filePath);
    const decoder = new avro.streams.BlockDecoder({ codecs });

    // Handle flow
    fileStream
      .pipe(decoder)
      .on("data", (record) => {
        // This will now actually trigger!
        records.push(flattenAvro(record));
      })
      .on("end", () => {
        logger.info(`Done! Processed ${records.length} records.`);
        resolve(records);
      })
      .on("error", (err) => {
        reject(new Error(`Avro Error: ${err.message}`));
      });

    // Error handling for the source file
    fileStream.on("error", (err) => reject(err));
  });
};

const postToWorkato = async (jsonData) => {
  const WORKATO_WEBHOOK_URL = process.env.WORKATO_WEBHOOK_URL;

  try {
    logger.info(`Sending ${jsonData.length} records to Workato...`);

    const response = await workatoExecutor(
      () => {
        return axios.post(
          WORKATO_WEBHOOK_URL,
          {
            source: "AvroIntegration",
            timestamp: new Date().toISOString(),
            record_count: jsonData.length,
            data: jsonData, // Your flattened array
          },
          {
            headers: {
              "Content-Type": "application/json",
              // If your Workato webhook has security enabled:
              // 'API-Token': process.env.WORKATO_API_TOKEN
            },
            // Increase timeout for larger Avro files
            timeout: 30000,
          }
        );
      },
      { name: `postToWorkato` }
    );

    // const response = await axios.post(
    //   WORKATO_WEBHOOK_URL,
    //   {
    //     source: "AvroIntegration",
    //     timestamp: new Date().toISOString(),
    //     record_count: jsonData.length,
    //     data: jsonData, // Your flattened array
    //   },
    //   {
    //     headers: {
    //       "Content-Type": "application/json",
    //       // If your Workato webhook has security enabled:
    //       // 'API-Token': process.env.WORKATO_API_TOKEN
    //     },
    //     // Increase timeout for larger Avro files
    //     timeout: 30000,
    //   }
    // );

    logger.info(
      `✅ Workato Response: ${response.status} - ${JSON.stringify(
        response.data
      )}`
    );
    return response.data;
  } catch (error) {
    logger.error("❌ Failed to send data to Workato:", {
      message: error.message,
      response: error.response?.data,
    });
    throw error;
  }
};
const postToTarget = async (data) => {
  const res = axios.post(process.env.TARGET_API_URL, data, {
    headers: { "Content-Type": "application/json" },
  });

  return res?.data;
};

// 1. Define the codec logic (Synchronous SnappyJS is best for Buffers)
const codec = {
  snappy: (buf, cb) => {
    try {
      // Avro Snappy blocks have a 4-byte CRC checksum at the end. Strip it.
      const decompressed = SnappyJS.uncompress(buf.slice(0, buf.length - 4));
      cb(null, decompressed);
    } catch (err) {
      cb(err);
    }
  },
};

const processAvroBuffer = async (buffer) => {
  const records = [];

  return new Promise((resolve, reject) => {
    // 2. Convert the Multer buffer into a Readable Stream
    const bufferStream = Readable.from(buffer);

    // 3. IMPORTANT: Pass the { codec } object here!
    const decoder = new avro.streams.BlockDecoder({ codec });

    bufferStream
      .pipe(decoder)
      .on("data", (record) => {
        // Flatten each record as it arrives
        records.push(flattenAvro(record));
      })
      .on("end", () => {
        resolve(records);
      })
      .on("error", (err) => {
        // This is where your "unknown codec: snappy" error was coming from
        reject(new Error(`Avro Error: ${err.message}`));
      });
  });
};

export { processAvroFile, postToTarget, postToWorkato, processAvroBuffer };
