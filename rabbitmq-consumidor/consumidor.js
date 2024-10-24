const amqp = require('amqplib');

const QUEUE_NAME = 'task_queue';
const RABBITMQ_URL = 'amqp://localhost';

async function processTask(messageContent) {
  console.log(`Processando mensagem: ${messageContent}`);
  await new Promise((resolve) => setTimeout(resolve, 2000));
  console.log(`Processamento concluído: ${messageContent}`);
}

async function consumeMessages() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });

    console.log(`Conectado ao RabbitMQ. Aguardando mensagens na fila "${QUEUE_NAME}"...`);

    channel.consume(
      QUEUE_NAME,
      async (message) => {
        if (message !== null) {
          const content = message.content.toString();

          try {
            await processTask(content);

            channel.ack(message);
          } catch (error) {
            console.error('Erro ao processar a mensagem:', error);
            channel.nack(message);
          }
        }
      },
      { noAck: false }
    );
  } catch (error) {
    console.error('Erro ao conectar ao RabbitMQ:', error);
    setTimeout(consumeMessages, 5000);
  }
}

consumeMessages();
