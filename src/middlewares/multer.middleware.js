import multer from "multer";
import path from "path";
import fs from "fs";
import { logger } from "../index.js";

// Ensure the storage directory exists
const uploadDir = "files/avro_files";
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    logger.info("--- Multer Destination Triggered ---");
    const uploadDir = path.join(process.cwd(), "files", "avro_files");

    // Auto-create folder if it doesn't exist
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    logger.info(`--- Saving File: ${file.originalname}`);
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

const uploadBuffer = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // Optional: Limit to 10MB
});

export { storage, uploadBuffer };
