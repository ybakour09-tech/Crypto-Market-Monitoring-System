'use strict';
require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { MongoClient } = require('mongodb');
const path = require('path');

const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/crypto_monitor';
const DB_NAME = 'crypto_monitor';
const REFRESH_INTERVAL_MS = 5000;

// Tous les symboles à surveiller
const SYMBOLS = ['BTC/USDT', 'BTC-USD', 'ETH/USDT'];

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

let db;
async function connectMongo() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);
  console.log('[API] Connecté à MongoDB.');
}

// Pipeline dynamique : Prix moyen mobile par minute (30 min) pour n'importe quel symbole
async function getPrixMoyenMobile(symbol) {
  const trenteMinsAgo = new Date(Date.now() - 30 * 60 * 1000);
  return db.collection('trades').aggregate([
    { $match: { symbol, timestamp: { $gte: trenteMinsAgo } } },
    {
      $group: {
        _id: {
          hour:   { $hour:       { $toDate: '$timestamp' } },
          minute: { $minute:     { $toDate: '$timestamp' } }
        },
        prixMoyen:    { $avg: '$price' },
        volumeMinute: { $sum: { $multiply: ['$price', '$quantity'] } },
        nombreTrades: { $sum: 1 }
      }
    },
    {
      $addFields: {
        timeLabel: {
          $concat: [
            { $toString: '$_id.hour' }, ':',
            { $cond: [{ $lt: ['$_id.minute', 10] },
              { $concat: ['0', { $toString: '$_id.minute' }] },
              { $toString: '$_id.minute' }
            ]}
          ]
        }
      }
    },
    { $project: { _id: 0, timeLabel: 1, prixMoyen: 1, volumeMinute: 1, nombreTrades: 1 } },
    { $sort: { timeLabel: 1 } }
  ]).toArray();
}

// Pipeline dynamique : VWAP par fenêtre pour n'importe quel symbole
async function getAggregates(symbol) {
  return db.collection('aggregates').aggregate([
    { $match: { symbol } },
    { $sort: { timestamp: -1 } },
    {
      $group: {
        _id: '$window',
        dernierVwap:         { $first: '$vwap' },
        variationPourcentage:{ $first: '$priceVariation' },
        volumeFenetreUsd:    { $first: '$totalVolumeUsd' },
        misAJourA:           { $first: '$timestamp' }
      }
    },
    {
      $addFields: {
        fenetre:    '$_id',
        tendance:   { $cond: [{ $gt: ['$variationPourcentage', 0] }, 'HAUSSE', 'BAISSE'] },
        vwapArrondi:{ $round: ['$dernierVwap', 2] }
      }
    },
    { $sort: { fenetre: 1 } }
  ]).toArray();
}

// Pipeline global : Statistiques d'alertes (toutes cryptos)
async function getStatistiquesAlertes() {
  return db.collection('alerts').aggregate([
    {
      $group: {
        _id: { type: '$type', symbol: '$symbol' },
        nombreAlertes:  { $sum: 1 },
        derniereAlerte: { $max: '$timestamp' }
      }
    },
    {
      $addFields: {
        typeAlerte: '$_id.type',
        symbole:    '$_id.symbol',
        niveauRisque: {
          $switch: {
            branches: [
              { case: { $gte: ['$nombreAlertes', 1000] }, then: 'CRITIQUE' },
              { case: { $gte: ['$nombreAlertes', 100]  }, then: 'ELEVE'    },
              { case: { $gte: ['$nombreAlertes', 10]   }, then: 'MOYEN'    }
            ],
            default: 'FAIBLE'
          }
        }
      }
    },
    { $project: { _id: 0, typeAlerte: 1, symbole: 1, nombreAlertes: 1, niveauRisque: 1, derniereAlerte: 1 } },
    { $sort: { nombreAlertes: -1 } }
  ]).toArray();
}

// Récupère toutes les données pour tous les symboles en parallèle
async function getAllData() {
  const [symbolsResults, alerts] = await Promise.all([
    Promise.all(SYMBOLS.map(async (symbol) => {
      const [stats, aggregates] = await Promise.all([
        getPrixMoyenMobile(symbol),
        getAggregates(symbol)
      ]);
      return { symbol, stats, aggregates };
    })),
    getStatistiquesAlertes()
  ]);

  // Transformer le tableau en objet indexé par symbole
  const symbols = {};
  for (const { symbol, stats, aggregates } of symbolsResults) {
    symbols[symbol] = { stats, aggregates };
  }

  return { symbols, alerts };
}

function broadcastToAll(type, payload) {
  const message = JSON.stringify({ type, payload, timestamp: new Date() });
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(message);
  });
}

// Routes REST (chargement initial ou requête directe)
app.get('/api/data', async (req, res) => {
  try   { res.json(await getAllData()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', async (req, res) => {
  const symbol = req.query.symbol || 'BTC/USDT';
  try   { res.json(await getPrixMoyenMobile(symbol)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/aggregates', async (req, res) => {
  const symbol = req.query.symbol || 'BTC-USD';
  try   { res.json(await getAggregates(symbol)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/alerts', async (req, res) => {
  try   { res.json(await getStatistiquesAlertes()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// WebSocket : envoi initial + diffusion périodique
wss.on('connection', async (ws) => {
  console.log('[API] Nouveau client Dashboard connecté.');
  try {
    const data = await getAllData();
    ws.send(JSON.stringify({ type: 'INIT', payload: data }));
  } catch (err) {
    console.error('[API] Erreur init client:', err.message);
  }
  ws.on('close', () => console.log('[API] Client Dashboard déconnecté.'));
});

function startRefreshLoop() {
  setInterval(async () => {
    try {
      const data = await getAllData();
      broadcastToAll('UPDATE_ALL', data);
    } catch (err) {
      console.error('[API] Erreur rafraîchissement:', err.message);
    }
  }, REFRESH_INTERVAL_MS);
  console.log(`[API] Boucle de rafraîchissement démarrée (toutes les ${REFRESH_INTERVAL_MS / 1000}s).`);
}

async function main() {
  await connectMongo();
  startRefreshLoop();
  server.listen(PORT, () => {
    console.log(`[API] Serveur démarré sur http://localhost:${PORT}`);
    console.log(`[API] WebSocket disponible sur ws://localhost:${PORT}`);
  });
}

main().catch(console.error);
