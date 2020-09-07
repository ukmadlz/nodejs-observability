'use strict';

require('dotenv').config();
const express = require('express');
const amqp = require('amqplib');
const winston = require('winston');
const LogzioWinstonTransport = require('winston-logzio');

// Logger
const logzioWinstonTransport = new LogzioWinstonTransport({
  name: 'winston_logzio',
  token: process.env.SHIPPING_TOKEN,
  host: `${process.env.LISTENER_URI}:5015`,
});
const logger = winston.createLogger({
  transports: [
    new winston.transports.Console(),
    logzioWinstonTransport,
  ]
});

// Constants
const PORT = 8080;
const HOST = '0.0.0.0';
const Q = 'tasks';

// App
const app = express();
app.get('/', async (req, res) => {
  logger.debug('Image request recieved.');
  const { imageUrl } = req.query;
  // Queue
  try {
    const connection = await amqp.connect('amqp://rabbitmq');
    const channel = await connection.createChannel();
    await channel.assertQueue(Q, { durable: true });
    await channel.sendToQueue(Q,  Buffer.from(JSON.stringify({ imageUrl })), {
      contentType: 'application/json',
      persistent: true
    });
    logger.info('Sent to queue');
  } catch (e) {
    logger.error(e);
  }
  res.send(`Image ${imageUrl} saved`);
});

app.listen(PORT, HOST);
logger.info(`Running on http://${HOST}:${PORT}`);