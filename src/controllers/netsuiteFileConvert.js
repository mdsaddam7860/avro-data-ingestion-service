import { logger } from "../index.js";
import axios from "axios";

async function convertFileToBase64(req, res) {
  try {
    logger.debug("Body", req.body);
    const url = req.body.url;

    if (!url) {
      return res
        .status(400)
        .json({ error: "No URL provided in request body." });
    }

    // Request the file as an arraybuffer to handle binary data
    const response = await axios.get(url, { responseType: "arraybuffer" });

    // Convert the buffer to a Base64 string
    const base64String = Buffer.from(response.data, "binary").toString(
      "base64"
    );

    // Return the JSON structure
    return res.status(200).json({
      status: "Success",
      qb_base64: base64String,
      contentType: "application/json",
    });
  } catch (error) {
    logger.error("Webhook Error convertFileToBase64:", error.message);

    return res.status(500).json({
      status: "Error",
      message: error.message,
    });
  }
}

export { convertFileToBase64 };
