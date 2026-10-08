// consumer.js
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'notification-service',
    brokers: ['localhost:9092']
});

// Use a consumer group so Kafka handles offset tracking
const consumer = kafka.consumer({ groupId: 'notification-group' });

async function runConsumer() {
    await consumer.connect();
    console.log('Kafka Consumer connected successfully.');

    // Subscribe to the topic
    await consumer.subscribe({ topic: 'order-events', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventPayload = JSON.parse(message.value.toString());

            // Simulate Notification Service triggering
            console.log('--------------------------------------------------');
            console.log(`[Notification Service] Triggered for Event!`);
            console.log(`Topic: ${topic} | Partition: ${partition} | Offset: ${message.offset}`);
            console.log('Event Payload:', JSON.stringify(eventPayload, null, 2));
            console.log('--------------------------------------------------');
        },
    });
}

runConsumer().catch(console.error);