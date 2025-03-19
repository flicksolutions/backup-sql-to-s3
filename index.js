require("dotenv").config(); // Load environment variables from .env

const cron = require("node-cron");
const { exec } = require("child_process");
const fs = require("fs");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

// Configure the S3 client for Backblaze B2 (S3-compatible)
const s3Client = new S3Client({
  endpoint: "https://s3.eu-central-003.backblazeb2.com", // adjust if needed
  region: "eu-central-003", // required but not used by Backblaze
  credentials: {
    accessKeyId: process.env.B2_KEY_ID,
    secretAccessKey: process.env.B2_APPLICATION_KEY,
  },
  forcePathStyle: true, // required for Backblaze B2
});

const bucketName = process.env.B2_BUCKET_NAME;

// Schedule the job to run every day at 10 PM server time
cron.schedule("0 22 * * *", () => {
  console.log("Starting PostgreSQL dump job using pg_dump...");

  // Create a timestamped filename (e.g., dump-2025-03-19.sql)
  const date = new Date().toISOString().split("T")[0];
  const dumpFileName = `dump-${date}.sql`;

  // pg_dump expects the password in the environment variable PGPASSWORD
  process.env.PGPASSWORD = process.env.DB_PASSWORD;

  // Construct the pg_dump command.
  // -F p produces a plain-text SQL dump.
  // Adjust flags as needed for your environment.
  const command = `pg_dump -h ${process.env.DB_HOST} -p ${
    process.env.DB_PORT || 5432
  } -U ${process.env.DB_USER} -d ${
    process.env.DB_NAME
  } -F p -f ${dumpFileName}`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error("Error during database dump:", error);
      return;
    }
    console.log(`Database dump created: ${dumpFileName}`);

    // Read the dump file into memory
    fs.readFile(dumpFileName, async (readErr, fileContents) => {
      if (readErr) {
        console.error("Error reading dump file:", readErr);
        return;
      }

      // Prepare parameters for the S3 upload
      const params = {
        Bucket: bucketName,
        Key: dumpFileName, // File name in the bucket
        Body: fileContents,
      };

      try {
        // Upload the dump file to Backblaze S3
        const data = await s3Client.send(new PutObjectCommand(params));
        console.log("Database dump uploaded. Response:", data);

        // Remove the local dump file after successful upload
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

console.log("Worker scheduled to run every day at 10 PM.");
