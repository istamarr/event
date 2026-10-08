// producer.js
const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const PORT = 3333;

// Initialize Kafka client pointing to your Docker local broker
const kafka = new Kafka({
    clientId: 'order-service',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function startProducer() {
    await producer.connect();
    console.log('Kafka Producer connected successfully.');

    app.post('/orders', async (req, res) => {
        try {
            const { customerId, items, totalAmount } = req.body;

            const orderEvent = {
                orderId: `ORD-${Date.now()}`,
                customerId,
                items,
                totalAmount,
                createdAt: new Date().toISOString()
            };

            // Send the event to the 'order-events' topic
            await producer.send({
                topic: 'order-events',
                messages: [
                    {
                        key: orderEvent.orderId,
                        value: JSON.stringify(orderEvent)
                    },
                ],
            });

            console.log(`[Producer] Order published: ${orderEvent.orderId}`);
            res.status(201).json({
                success: true,
                message: 'Order created and event published!',
                order: orderEvent
            });
        } catch (error) {
            console.error('[Producer Error] Failed to publish order event:', error);
            res.status(500).json({ success: false, error: 'Internal Server Error' });
        }
    });

    app.listen(PORT, () => {
        console.log(`Order Service API running on http://localhost:${PORT}`);
    });
}

startProducer().catch(console.error);