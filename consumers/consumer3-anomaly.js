'use strict';
require('dotenv').config();
const { MongoClient } = require('mongodb');
const { createConsumer } = require('../shared/kafka-config');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/crypto_monitor';
const DB_NAME = 'crypto_monitor';
const COLLECTION_NAME = 'alerts';

// États pour la détection d'anomalies
const lastPrices = {
  BTC: { binance: null, coinbase: null }
};
let lastSpreadAlertTime = 0;
const SPREAD_ALERT_THROTTLE_MS = 10000; // Alerte max une fois toutes les 10 secondes

const volumeHistory = {};
const MAX_VOL_HISTORY = 50;

const priceHistory = {};
const PRICE_WINDOW_MS = 10000; // Fenêtre glissante de 10 secondes

async function main() {
  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  console.log('[Anomaly] Connected to MongoDB.');
  const db = mongoClient.db(DB_NAME);
  const alertsCollection = db.collection(COLLECTION_NAME);

  // Assurer les index
  await alertsCollection.createIndex({ severity: 1, timestamp: -1 });

  const consumer = await createConsumer('group-anomaly');

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const trade = JSON.parse(message.value.toString());
        if (!trade.symbol || typeof trade.price !== 'number' || typeof trade.quantity !== 'number') {
          return;
        }

        const now = Date.now();
        const symbol = trade.symbol;
        const volume = trade.price * trade.quantity;

        // --- 1. DÉTECTION DU SPREAD (écart inter-exchanges) ---
        let baseSymbol = null;
        if (symbol === 'BTC/USDT' || symbol === 'BTC-USD') baseSymbol = 'BTC';

        if (baseSymbol) {
          if (trade.source === 'binance') {
            lastPrices[baseSymbol].binance = trade.price;
          } else if (trade.source === 'coinbase') {
            lastPrices[baseSymbol].coinbase = trade.price;
          }

          const binPrice = lastPrices[baseSymbol].binance;
          const coinPrice = lastPrices[baseSymbol].coinbase;

          if (binPrice && coinPrice) {
            const spread = Math.abs(binPrice - coinPrice);
            if (spread > 12 && (now - lastSpreadAlertTime > SPREAD_ALERT_THROTTLE_MS)) {
              lastSpreadAlertTime = now;
              const alert = {
                type: 'HIGH_SPREAD',
                symbol: baseSymbol,
                message: `L'écart de prix entre Binance ($${binPrice}) et Coinbase ($${coinPrice}) est de $${spread.toFixed(2)} (Limite : $12)`,
                severity: 'warning',
                timestamp: new Date()
              };
              await alertsCollection.insertOne(alert);
              console.log(`[Anomaly] ALERT: [${alert.severity}] ${alert.type} sur ${alert.symbol}: ${alert.message}`);
            }
          }
        }

        // --- 2. DÉTECTION DES PICS DE VOLUME ---
        if (!volumeHistory[symbol]) {
          volumeHistory[symbol] = [];
        }

        const volHist = volumeHistory[symbol];
        if (volHist.length >= 10) {
          const avgVol = volHist.reduce((a, b) => a + b, 0) / volHist.length;
          if (volume > 3 * avgVol) {
            const alert = {
              type: 'LARGE_VOLUME',
              symbol,
              message: `Volume de transaction de $${volume.toFixed(2)} est > 3x la moyenne récente ($${avgVol.toFixed(2)})`,
              severity: 'warning',
              timestamp: new Date()
            };
            await alertsCollection.insertOne(alert);
            console.log(`[Anomaly] ALERT: [${alert.severity}] ${alert.type} sur ${alert.symbol}: ${alert.message}`);
          }
        }
        volHist.push(volume);
        if (volHist.length > MAX_VOL_HISTORY) {
          volHist.shift();
        }

        // --- 3. DÉTECTION DE LA VOLATILITÉ DU PRIX ---
        if (!priceHistory[symbol]) {
          priceHistory[symbol] = [];
        }

        // Nettoyer l'historique des prix vieux de plus de 10 secondes
        priceHistory[symbol] = priceHistory[symbol].filter(t => (now - t.timestamp) <= PRICE_WINDOW_MS);
        const prHist = priceHistory[symbol];

        if (prHist.length >= 5) {
          const avgPrice = prHist.reduce((sum, t) => sum + t.price, 0) / prHist.length;
          const deviation = Math.abs(trade.price - avgPrice) / avgPrice;

          if (deviation > 0.01) { // Plus de 1% d'écart
            const alert = {
              type: 'PRICE_VOLATILITY',
              symbol,
              message: `Le prix de $${trade.price} dévie de ${(deviation * 100).toFixed(2)}% par rapport à la moyenne des 10s ($${avgPrice.toFixed(2)})`,
              severity: 'warning',
              timestamp: new Date()
            };
            await alertsCollection.insertOne(alert);
            console.log(`[Anomaly] ALERT: [${alert.severity}] ${alert.type} sur ${alert.symbol}: ${alert.message}`);
          }
        }

        prHist.push({ price: trade.price, timestamp: now });

      } catch (err) {
        console.error('[Anomaly] Erreur lors de la détection d\'anomalies :', err.message);
      }
    }
  });
}

main().catch(console.error);
