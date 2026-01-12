const { Kafka } = require('kafkajs');
const crypto = require('crypto'); // Built-in for generating tokens

const kafka = new Kafka({ clientId: 'identity-app', brokers: ['localhost:9092'] });
const producer = kafka.producer();

const startEmailChange = async () => {
  await producer.connect();
  
  const event = {
    user_id: 'user_12345',
    new_email: 'new-email@gmail.com',
    status: 'PENDING', // Domain status
    token: crypto.randomBytes(16).toString('hex'), 
    timestamp: new Date().toISOString()
  };

  await producer.send({
    topic: 'email-updates',
    messages: [{ key: event.user_id, value: JSON.stringify(event) }],
  });

  console.log(`[Producer] Stage 1: Verification email sent to ${event.new_email}`);
  await producer.disconnect();
};

startEmailChange().catch(console.error);