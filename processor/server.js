'use strict';

const amqp = require('amqplib');
const download = require('image-downloader');
const opentracing = require('opentracing');
const initTracer = require('jaeger-client').initTracer;
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

// Jaeger
const config = {
  serviceName: 'nodejs-image-save.processor',
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
    'nodejs-image-save.processor.version': '0.0.0',
  },
  logger: console,
};
const tracer = initTracer(config, options);

// Queue consumer
const start = async () => {
  logger.info('Starting connection to rabbitmq')
  const connection = await amqp.connect('amqp://rabbitmq');

  const channel = await connection.createChannel();
  await channel.assertQueue('tasks', { durable: true });
  await channel.prefetch(1);

  logger.info('Waiting tasks...');

  channel.consume('tasks', async (message) => {
    const content = message.content.toString();
    const task = JSON.parse(content);
    const parentSpan = tracer.extract(
      opentracing.FORMAT_TEXT_MAP,
      task.trace
    );
    const span = tracer.startSpan("amqp_request", {
      references: [opentracing.followsFrom(parentSpan)]
    });

    const { imageUrl } = task;

    // Save the image
    try {
      const filename = await download.image({
        url: imageUrl,
        dest: './'
      });
      span.log({'event': 'file_saved'});
      logger.info('Saved to', filename);
    } catch (e) {
      const errorOject = {'event': 'error', 'error.object': e, 'message': e.message, 'stack': e.stack};
      logger.error(errorOject);
      span.setTag(opentracing.Tags.ERROR, true);
      span.log(errorOject);
    }

    channel.ack(message);

    logger.info(`${task.imageUrl} received!`);
    span.log({'event': 'request_end'});
    span.finish();
  });
};

// Lag to allow RabbitMQ to stand-up and be ready for the connection
setTimeout(start, 20000);