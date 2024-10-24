const express = require('express');
const amqp = require('amqplib');

const app = express();
const PORT = 3001;

app.use(express.json());

async function sendMessageToQueue(message) {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queue = 'task_queue';

    await channel.assertQueue(queue, { durable: true });

    channel.sendToQueue(queue, Buffer.from(message), {
      persistent: true,
    });

    console.log(`Mensagem enviada: ${message}`);
    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('Erro ao enviar mensagem:', error);
  }
}

app.post('/send', (req, res) => {
  const { message } = req.body;

  if (!message) {
    return res.status(400).send('Mensagem não fornecida.');
  }

  sendMessageToQueue(message);
  res.send('Mensagem enviada para a fila!');
});

app.listen(PORT, () => {
  console.log(`Produtor rodando em http://localhost:${PORT}`);
});
