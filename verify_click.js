const { Kafka } = require('kafkajs');
const kafka = new Kafka({ clientId: 'identity-app', brokers: ['localhost:9092'] });
const producer = kafka.producer();

const confirmEmail = async () => {
  await producer.connect();
  
  const event = {
    user_id: 'user_12345',
    status: 'VERIFIED', // Moving to the next state
    timestamp: new Date().toISOString()
  };

  await producer.send({
    topic: 'email-updates',
    messages: [{ key: event.user_id, value: JSON.stringify(event) }],
  });

  console.log(`[Producer] Stage 2: User clicked link. Email is now VERIFIED.`);
  await producer.disconnect();
};

confirmEmail().catch(console.error);