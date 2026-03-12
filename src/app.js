import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import {
  handleAvroWebhook,
  handleHexAvroWebhook,
} from "./controllers/webhookController.js";
import { storage, uploadBuffer } from "./middlewares/multer.middleware.js";

const app = express();

// In app.js (Check if Multer even triggers)

const upload = multer({
  storage: storage,
  limits: { fileSize: 50 * 1024 * 1024 },
});

// Increase the limit for JSON bodies (e.g., to 50MB)
app.use(express.json({ limit: "50mb" }));

// Also increase for URL-encoded if you use it
app.use(express.urlencoded({ limit: "50mb", extended: true }));

// Routes
app.post("/health", (req, res) => {
  res.status(200).json({ message: "Healthy" });
});
app.post("/webhook/avro-json", handleHexAvroWebhook);
app.post("/webhook/avro-json-file", upload.single("file"), handleAvroWebhook);

export default app;
