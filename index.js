// worker.js

const cron = require("node-cron");
const { exec } = require("child_process");
const fs = require("fs");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

// Configure the S3 client for Backblaze B2 (S3-compatible) using AWS SDK v3
const s3Client = new S3Client({
  endpoint: "s3.eu-central-003.backblazeb2.com", // Adjust endpoint if needed
  region: "eu-central-003",
  credentials: {
    accessKeyId: process.env.B2_KEY_ID,
    secretAccessKey: process.env.B2_APPLICATION_KEY,
  },
  forcePathStyle: true, // Required for Backblaze B2
});

// Backblaze bucket name from environment variables
const bucketName = process.env.B2_BUCKET_NAME;

// Schedule the job to run every day at 10 PM server time using node-cron
cron.schedule("0 22 * * *", () => {
  console.log("Starting database dump job using mysqldump CLI...");

  // Create a timestamped file name (e.g., dump-2025-03-19.sql)
  const date = new Date().toISOString().split("T")[0]; // Format: YYYY-MM-DD
  const dumpFileName = `dump-${date}.sql`;

  // Construct the mysqldump command
  // Ensure that mysqldump is installed and available in your PATH
  const command = `mysqldump -h ${process.env.DB_HOST} -u ${process.env.DB_USER} -p${process.env.DB_PASSWORD} ${process.env.DB_NAME} > ${dumpFileName}`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error("Error during database dump:", error);
      return;
    }
    console.log(`Database dump created: ${dumpFileName}`);

    // Read the dump file for uploading
    fs.readFile(dumpFileName, async (readErr, fileContents) => {
      if (readErr) {
        console.error("Error reading dump file:", readErr);
        return;
      }

      // Prepare the parameters for the S3 upload
      const params = {
        Bucket: bucketName,
        Key: dumpFileName, // File name in the bucket
        Body: fileContents,
      };

      try {
        // Upload the dump file using the PutObjectCommand from AWS SDK v3
        const data = await s3Client.send(new PutObjectCommand(params));
        console.log("Database dump uploaded. Response:", data);

        // Clean up the local dump file after successful upload
        fs.unlink(dumpFileName, (unlinkErr) => {
          if (unlinkErr) {
            console.error("Error deleting local dump file:", unlinkErr);
            return;
          }
          console.log("Local dump file removed.");
        });
      } catch (uploadErr) {
        console.error("Error uploading to Backblaze S3:", uploadErr);
      }
    });
  });
});

// Log to confirm that the worker is running
console.log("Worker scheduled to run every day at 10 PM.");
