'use strict';
require('dotenv').config();
const { MongoClient } = require('mongodb');
const { createConsumer } = require('../shared/kafka-config');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/crypto_monitor';
const DB_NAME = 'crypto_monitor';
const COLLECTION_NAME = 'aggregates';

const WINDOWS = {
  '1m': 60 * 1000,
  '5m': 5 * 60 * 1000,
  '15m': 15 * 60 * 1000,
  '1h': 60 * 60 * 1000
};

// Tampon en mémoire pour les trades du dernier cycle d'une heure
let tradesBuffer = [];

async function main() {
  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  console.log('[Aggregator] Connected to MongoDB.');
  const db = mongoClient.db(DB_NAME);
  const aggregatesCollection = db.collection(COLLECTION_NAME);

  // Assurer les index
  await aggregatesCollection.createIndex({ symbol: 1, window: 1 });

  const consumer = await createConsumer('group-aggregator');

  // Traiter les messages entrants
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const trade = JSON.parse(message.value.toString());
        if (!trade.symbol || typeof trade.price !== 'number' || typeof trade.quantity !== 'number') {
          return;
        }

        tradesBuffer.push({
          symbol: trade.symbol,
          price: trade.price,
          quantity: trade.quantity,
          timestamp: trade.timestamp
        });
      } catch (err) {
        console.error('[Aggregator] Erreur lors de la mise en tampon :', err.message);
      }
    }
  });

  // Calcul périodique des agrégats (toutes les 5 secondes)
  setInterval(async () => {
    if (tradesBuffer.length === 0) return;

    const now = Date.now();
    const oneHourAgo = now - WINDOWS['1h'];

    // Garder uniquement les trades des dernières 60 minutes
    tradesBuffer = tradesBuffer.filter(t => t.timestamp >= oneHourAgo);

    // Récupérer la liste des symboles uniques présents dans le tampon
    const symbols = [...new Set(tradesBuffer.map(t => t.symbol))];

    for (const symbol of symbols) {
      const symbolTrades = tradesBuffer.filter(t => t.symbol === symbol);

      for (const [windowName, duration] of Object.entries(WINDOWS)) {
        const windowStart = now - duration;
        const windowTrades = symbolTrades.filter(t => t.timestamp >= windowStart);

        if (windowTrades.length === 0) continue;

        // Trier par ordre chronologique
        windowTrades.sort((a, b) => a.timestamp - b.timestamp);

        let totalVolumeUsd = 0;
        let totalQuantity = 0;

        for (const t of windowTrades) {
          const vol = t.price * t.quantity;
          totalVolumeUsd += vol;
          totalQuantity += t.quantity;
        }

        const vwap = totalQuantity > 0 ? totalVolumeUsd / totalQuantity : 0;
        const firstPrice = windowTrades[0].price;
        const latestPrice = windowTrades[windowTrades.length - 1].price;
        const priceVariation = firstPrice > 0 ? ((latestPrice - firstPrice) / firstPrice) * 100 : 0;

        const aggregateDoc = {
          symbol,
          window: windowName,
          vwap,
          totalVolumeUsd,
          totalQuantity,
          priceVariation,
          timestamp: new Date(now)
        };

        // Mise à jour ou insertion dans la base
        await aggregatesCollection.updateOne(
          { symbol, window: windowName },
          { $set: aggregateDoc },
          { upsert: true }
        );
      }
    }
  }, 5000);
}

main().catch(console.error);
