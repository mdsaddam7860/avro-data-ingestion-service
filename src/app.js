import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import { handleAvroWebhook } from "./controllers/webhookController.js";
import { storage, uploadBuffer } from "./middlewares/multer.middleware.js";

const app = express();

// In app.js (Check if Multer even triggers)

const upload = multer({
  storage: storage,
  limits: { fileSize: 50 * 1024 * 1024 },
});

app.use(express.json());

// Routes
app.post("/webhook/avro-json", upload.single("file"), handleAvroWebhook);

export { app };
