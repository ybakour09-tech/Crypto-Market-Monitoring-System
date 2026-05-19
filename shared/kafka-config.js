'use strict';
require('dotenv').config();
const { Kafka, logLevel } = require('kafkajs');

const TOPIC = process.env.KAFKA_TOPIC || 'crypto.trades.raw';

/**
 * Create and return a Kafka producer instance (already connected).
 */
async function createProducer(clientId) {
  const k = new Kafka({
    clientId: clientId || 'crypto-producer',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
  const producer = k.producer();
  await producer.connect();
  console.log(`[Kafka] Producer "${clientId}" connected.`);
  return producer;
}

/**
 * Create and return a Kafka consumer instance (already subscribed to TOPIC).
 */
async function createConsumer(groupId) {
  const k = new Kafka({
    clientId: `consumer-${groupId}`,
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
  const consumer = k.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  console.log(`[Kafka] Consumer group "${groupId}" connected and subscribed to "${TOPIC}".`);
  return consumer;
}

module.exports = { TOPIC, createProducer, createConsumer };
