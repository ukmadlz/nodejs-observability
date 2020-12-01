'use strict';

require('dotenv').config();
const express = require('express');
const amqp = require('amqplib');
const winston = require('winston');
const LogzioWinstonTransport = require('winston-logzio');
const opentracing = require('opentracing');
const initTracer = require('jaeger-client').initTracer;

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

// Jaeger
const config = {
  serviceName: 'nodejs-image-save.reciever',
  reporter: {
    collectorEndpoint: 'http://jaegercollector:14268/api/traces',
    logSpans: true,
  },
  sampler: {
    type: 'const',
    param: 1
  }
};
const options = {
  tags: {
    'nodejs-image-save.reciever.version': '0.0.0',
  },
  logger,
};
const tracer = initTracer(config, options);

// Constants
const PORT = 8080;
const HOST = '0.0.0.0';
const Q = 'tasks';

// App
const app = express();
// The request
app.get('/', async (req, res) => {
  const httpSpan = tracer.startSpan("http_request");
  httpSpan.addTags({
    [opentracing.Tags.SPAN_KIND]: opentracing.Tags.SPAN_KIND_MESSAGING_PRODUCER,
    [opentracing.Tags.HTTP_METHOD]: req.method,
    [opentracing.Tags.HTTP_URL]: req.path
  });
  logger.debug('Image request recieved.');
  const { imageUrl } = req.query;
  res.on("finish", () => {
    httpSpan.setTag(opentracing.Tags.HTTP_STATUS_CODE, res.statusCode);
    httpSpan.finish();
  });
  const enqueueSpan = tracer.startSpan("enqueue", {
    childOf: httpSpan
  });

  const traceContext = {};
  tracer.inject(enqueueSpan, opentracing.FORMAT_TEXT_MAP, traceContext);
  // Queue
  try {
    const connection = await amqp.connect('amqp://rabbitmq');
    const channel = await connection.createChannel();
    await channel.assertQueue(Q, { durable: true });
    await channel.sendToQueue(Q,  Buffer.from(JSON.stringify({ imageUrl, trace: traceContext })), {
      contentType: 'application/json',
      persistent: true
    });
    logger.info('Sent to queue');
  } catch (e) {
    enqueueSpan.setTag(opentracing.Tags.ERROR, true).log({ error: e });
    logger.error(e);
  } finally {
    enqueueSpan.finish();
    res.send(`Image ${imageUrl} saved`);
  }
});

app.listen(PORT, HOST);
logger.info(`Running on http://${HOST}:${PORT}`);