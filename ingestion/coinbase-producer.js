'use strict';
require('dotenv').config();
const WebSocket = require('ws');
const { createProducer, TOPIC } = require('../shared/kafka-config');

const COINBASE_WS_URL = process.env.COINBASE_WS_URL ||
  'wss://advanced-trade-ws.coinbase.com';

let producer;
let reconnectDelay = 1000;
const MAX_DELAY = 30000;

const SUBSCRIBE_MSG = JSON.stringify({
  type: 'subscribe',
  product_ids: ['BTC-USD'],
  channel: 'market_trades',
});

/**
 * Normalize a Coinbase market_trades event to the standard trade object.
 */
function normalizeCoinbaseTrade(trade) {
  const price = parseFloat(trade.price);
  const quantity = parseFloat(trade.size);
  return {
    source: 'coinbase',
    symbol: 'BTC-USD',
    price,
    quantity,
    volume: price * quantity,
    timestamp: new Date(trade.time).getTime(),
  };
}

function connect() {
  console.log(`[Coinbase] Connecting to ${COINBASE_WS_URL}`);
  const ws = new WebSocket(COINBASE_WS_URL);

  ws.on('open', () => {
    console.log('[Coinbase] WebSocket connected. Subscribing...');
    ws.send(SUBSCRIBE_MSG);
    reconnectDelay = 1000;
  });

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // Only process market_trades events with actual trade data
      if (msg.channel !== 'market_trades' || !msg.events) return;

      for (const event of msg.events) {
        if (event.type !== 'update' || !event.trades) continue;
        for (const trade of event.trades) {
          const normalized = normalizeCoinbaseTrade(trade);
          await producer.send({
            topic: TOPIC,
            messages: [
              {
                key: normalized.symbol,
                value: JSON.stringify(normalized),
              },
            ],
          });
        }
      }
    } catch (err) {
      console.error('[Coinbase] Error processing message:', err.message);
    }
  });

  ws.on('close', (code) => {
    console.warn(`[Coinbase] WebSocket closed (${code}). Reconnecting in ${reconnectDelay}ms...`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_DELAY);
  });

  ws.on('error', (err) => {
    console.error('[Coinbase] WebSocket error:', err.message);
    ws.terminate();
  });
}

async function main() {
  try {
    producer = await createProducer('coinbase-producer');
    connect();
  } catch (err) {
    console.error('[Coinbase] Fatal startup error:', err.message);
    process.exit(1);
  }
}

main();
