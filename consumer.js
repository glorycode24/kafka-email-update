const { Kafka } = require('kafkajs');

const kafka = new Kafka({ 
  clientId: 'identity-app', 
  brokers: ['localhost:9092'] 
});

const consumer = kafka.consumer({ groupId: 'email-notification-group' });

const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'email-updates', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());

        if (data.status === 'PENDING') {
          console.log(`\n--- NEW WORKFLOW ---`);
          console.log(`[Email Service] 📩 Sending link to: ${data.new_email}`);
          console.log(`[Link] http://localhost:3000/verify?token=${data.token}`);
        } 
        else if (data.status === 'VERIFIED') {
          console.log(`\n--- WORKFLOW COMPLETE ---`);
          console.log(`[Database Service] ✅ Saving new email for User ${data.user_id}`);
        }
      } catch (err) {
        console.error("Could not parse message. It might be old manual data:", message.value.toString());
      }
    },
  });
};

startConsumer().catch(e => console.error(`[consumer] ${e.message}`, e));