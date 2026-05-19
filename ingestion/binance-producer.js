'use strict';
require('dotenv').config();
const WebSocket = require('ws');
const { createProducer, TOPIC } = require('../shared/kafka-config');

const BINANCE_WS_URL = process.env.BINANCE_WS_URL ||
  'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade';

const SYMBOL_MAP = {
  BTCUSDT: 'BTC/USDT',
  ETHUSDT: 'ETH/USDT',
};

let producer;
let reconnectDelay = 1000;
const MAX_DELAY = 30000;

/**
 * Normalize a Binance trade event to the standard trade object.
 */
function normalize(event) {
  const data = event.data || event;
  const symbol = SYMBOL_MAP[data.s] || data.s;
  const price = parseFloat(data.p);
  const quantity = parseFloat(data.q);
  return {
    source: 'binance',
    symbol,
    price,
    quantity,
    volume: price * quantity,
    timestamp: data.T,
  };
}

function connect() {
  console.log(`[Binance] Connecting to ${BINANCE_WS_URL}`);
  const ws = new WebSocket(BINANCE_WS_URL);

  ws.on('open', () => {
    console.log('[Binance] WebSocket connected.');
    reconnectDelay = 1000; // reset backoff on successful connect
  });

  ws.on('message', async (raw) => {
    try {
      const event = JSON.parse(raw.toString());
      const trade = normalize(event);

      await producer.send({
        topic: TOPIC,
        messages: [
          {
            key: trade.symbol,
            value: JSON.stringify(trade),
          },
        ],
      });
    } catch (err) {
      console.error('[Binance] Error processing message:', err.message);
    }
  });

  ws.on('close', (code, reason) => {
    console.warn(`[Binance] WebSocket closed (${code}). Reconnecting in ${reconnectDelay}ms...`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_DELAY);
  });

  ws.on('error', (err) => {
    console.error('[Binance] WebSocket error:', err.message);
    ws.terminate();
  });
}

async function main() {
  try {
    producer = await createProducer('binance-producer');
    connect();
  } catch (err) {
    console.error('[Binance] Fatal startup error:', err.message);
    process.exit(1);
  }
}

main();
