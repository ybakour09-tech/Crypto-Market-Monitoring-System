'use strict';
require('dotenv').config();
const { MongoClient } = require('mongodb');
const { createConsumer } = require('../shared/kafka-config');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/crypto_monitor';
const DB_NAME = 'crypto_monitor';
const COLLECTION_NAME = 'trades';

async function main() {
  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  console.log('[Parser] Connected to MongoDB.');
  const db = mongoClient.db(DB_NAME);
  const tradesCollection = db.collection(COLLECTION_NAME);

  // Assurer les index
  await tradesCollection.createIndex({ symbol: 1, timestamp: -1 });

  const consumer = await createConsumer('group-parser');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const trade = JSON.parse(message.value.toString());
        
        // Validation basique
        if (!trade.source || !trade.symbol || typeof trade.price !== 'number' || typeof trade.quantity !== 'number') {
          console.warn('[Parser] Trade invalide reçu :', trade);
          return;
        }

        const doc = {
          source: trade.source,
          symbol: trade.symbol,
          price: trade.price,
          quantity: trade.quantity,
          volume: trade.price * trade.quantity,
          timestamp: new Date(trade.timestamp),
          insertedAt: new Date()
        };

        await tradesCollection.insertOne(doc);
      } catch (err) {
        console.error('[Parser] Erreur lors de l\'enregistrement :', err.message);
      }
    }
  });
}

main().catch(console.error);
