import dotenv from "dotenv";
import app from "./src/app.js";
import { logger } from "./src/index.js";
// import { getHubspotClient } from "./src/configs/hubspot.config.js";
import path from "path";
// import { testWebhook } from "./src/utils/avroHelper.js";

dotenv.config({
  path: path.join(process.cwd(), ".env"),
});

const PORT = process.env.PORT || 5000;

function serverInit() {
  try {
    // Start the Express listener
    app.listen(PORT, () => {
      logger.info(`🚀 Server running on PORT: ${PORT}`);
      logger.info(`🌍 Environment: ${process.env.NODE_ENV || "development"}`);

      // Initialize HubSpot right after the server starts
      // initServices();
    });
  } catch (error) {
    logger.error("❌ Critical startup failure:", error);
    process.exit(1);
  }
}

serverInit();
// async function initServices() {
//   try {
//     const client = getHubspotClient();
//     logger.info(`✅ HubSpot client initialized successfully`);
//   } catch (error) {
//     logger.error("❌ HubSpot client failed to initialize:", error);
//   }
// }

// testWebhook();
