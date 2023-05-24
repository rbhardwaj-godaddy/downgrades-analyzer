console.log("Loading function");
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

// Configs
const kafkaBrokers = "<your-kafka-brokers>";
const kafkaTopic = "<your-kafka-topic>";
const batchSize = 3;

// Dependencies s3, kafka producer
const s3Client = new S3Client({ region: "us-east-1" });

const kafka = new Kafka({
  brokers: [kafkaBrokers],
});
const producer = kafka.producer();
await producer.connect();

export const handler = async (event, context) => {
  //console.log('Received event:', JSON.stringify(event, null, 2));

  // Get the object from the event
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );
  const s3Params = {
    Bucket: bucket,
    Key: key,
  };

  try {
    const getObjectCommand = new GetObjectCommand(s3Params);
    const s3Object = await s3Client.send(getObjectCommand);
    const fileContent = await streamToString(s3Object.Body);

    const lines = fileContent.split("\n");

    let batchStartIndex = 0;
    let batchEndIndex = batchSize;
    let batchCounter = 1;

    while (batchStartIndex < lines.length) {
      const batchLines = lines.slice(batchStartIndex, batchEndIndex);

      const message = {
        key: key,
        batchStartIndex: batchEndIndex,
        batchEndIndex: batchEndIndex,
        batchCounter: batchCounter,
        batchLines: batchLines,
      };

      console.log("sending batch {{" + batchCounter + "}}");
      await producer.send({
        topic: kafkaTopic,
        messages: [message],
      });

      // Delay or additional processing can be added between batches if needed

      batchStartIndex = batchEndIndex;
      batchEndIndex += batchSize;
      batchCounter += 1;
    }

    return { statusCode: 200, body: "Lines read successfully " + key };
  } catch (err) {
    console.error("Error reading lines from S3:", err);
    return { statusCode: 500, body: "Error reading lines from S3 " + key };
  } finally {
    // Disconnect from all that is material
    await producer.disconnect();
    await s3Client.disconnect();
  }
};

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf-8");
}
