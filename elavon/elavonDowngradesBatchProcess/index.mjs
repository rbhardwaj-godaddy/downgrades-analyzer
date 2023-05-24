import { Kafka } from "kafkajs";

// Configs
const kafkaBrokers = process.env.KAFKA_BROKERS;
const consumerTopic = process.env.CONSUMER_TOPIC;
const producerTopic = process.env.PRODUCER_TOPIC;
const esNode = process.env.ELASTICSAERCH_NODE; //"http://your-elasticsearch-host:9200"
const esTxnIndex = process.env.ELASTICSAERCH_TXN_INDEX;

export const handler = async (event, context) => {
  try {
    const es = new Client({ node: esNode });
    // Create Kafka consumer and producer instances
    const kafka = new Kafka({
      brokers: [kafkaBrokers],
    });

    const consumer = kafka.consumer({ groupId: "my-consumer-group" });
    const producer = kafka.producer();

    // Connect to the Kafka cluster
    await Promise.all([consumer.connect(), producer.connect()]);

    // Subscribe to the consumer topic
    await consumer.subscribe({ topic: consumerTopic, fromBeginning: true });

    // Consume messages from the consumer topic
    await consumer.run({
      eachMessage: async ({ message }) => {
        const { key, value } = message;

        // Process the consumed message
        console.log(`Received message: key=${key}, value=${value}`);
        await consumeBatch(message);
      },
    });

    return {
      statusCode: 200,
      body: "Kafka message consumption and publishing completed",
    };
  } catch (error) {
    console.error("Failed to consume and publish Kafka messages:", error);

    return {
      statusCode: 500,
      body: "Failed to consume and publish Kafka messages",
    };
  }
};

async function consumeBatch(message) {
  batchLines = message.batchLines;
  batchLines.array.forEach(async (line) => {
    const { transaction, downgradeReason } = queryESForTxn(line);
    const downgradeMsg = {
      key: key,
      txnId: transaction.id,
      downgradeReason: downgradeReason,
    };
    await publishDowngradeEvent(downgradeMsg);
  });
}

async function publishDowngradeEvent(downgradeMsg) {
  // Publish the message to the producer topic
  await producer.send({
    topic: producerTopic,
    messages: [downgradeMsg],
  });
}

async function queryESForTxn(line) {
  // prase line, prep query
  try {
    const { body } = await client.search({
      index: esTxnIndex,
      body: {
        query: {
          match: {
            title: "search keyword",
          },
        },
      },
    });

    return body.hits.hits[0];
  } catch (error) {
    console.error("Failed to execute Elasticsearch query:", error);

    return {
      statusCode: 500,
      body: "Failed to execute Elasticsearch query",
    };
  }
}
