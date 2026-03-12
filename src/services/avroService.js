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
      const decompressed = SnappyJS.uncompress(buf.slice(0, buf.length - 4));
      cb(null, decompressed);
    } catch (err) {
      cb(err);
    }
  },
};

/**
 * Process Avro data from either a File Path (string) or a Buffer
 */
const processAvroHexToJson = async (input) => {
  const records = [];

  return new Promise((resolve, reject) => {
    let sourceStream;

    // 1. Determine the source: Is it a path or a buffer?
    if (Buffer.isBuffer(input)) {
      // It's a Buffer (from Hex conversion)
      sourceStream = Readable.from(input);
      logger.info("Processing Avro from memory buffer...");
    } else if (typeof input === "string") {
      // It's a File Path (from disk storage)
      sourceStream = fs.createReadStream(input);
      logger.info(`Processing Avro from file: ${input}`);
    } else {
      return reject(
        new Error(
          "Invalid input: processAvroFile expects a Buffer or a string path."
        )
      );
    }

    const decoder = new avro.streams.BlockDecoder({ codecs });

    sourceStream
      .pipe(decoder)
      .on("data", (record) => {
        records.push(flattenAvro(record));
      })
      .on("end", () => {
        logger.info(`Successfully processed ${records.length} records.`);
        resolve(records);
      })
      .on("error", (err) => {
        reject(new Error(`Avro Decoding Error: ${err.message}`));
      });

    sourceStream.on("error", (err) => {
      reject(new Error(`Source Stream Error: ${err.message}`));
    });
  });
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

export { processAvroFile, postToTarget, postToWorkato, processAvroHexToJson };
