# 🛠️ Pipeline de Streaming Temps Réel (Kafka & MongoDB)

Ce document décrit l'ensemble du pipeline temps réel : de l'ingestion des flux WebSockets (Binance & Coinbase) à la persistance, l'agrégation statistique et la détection d'anomalies dans MongoDB via Apache Kafka.

---

## 📐 Architecture Technique

```
┌────────────────────────┐         ┌────────────────────────┐
│  Binance WebSocket     │         │   Coinbase WebSocket   │
│  wss://stream.binance  │         │  wss://advanced-trade  │
└───────────┬────────────┘         └───────────┬────────────┘
            │                                  │
            ▼                                  ▼
┌────────────────────────┐         ┌────────────────────────┐
│   binance-producer.js  │         │  coinbase-producer.js  │
│  (Normalisation JSON)  │         │  (Normalisation JSON)  │
└───────────┬────────────┘         └───────────┬────────────┘
            │                                  │
            └─────────────────┬────────────────┘
                              ▼
                 ┌──────────────────────────┐
                 │    Topic Apache Kafka    │
                 │   `crypto.trades.raw`    │
                 │  (3 Partitions, 24h ret) │
                 └────────────┬─────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │ (Chaque groupe consomme en parallèle)  │
         ▼                                         ▼
┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
│ Consumer 1       │      │ Consumer 2       │    │ Consumer 3       │
│ Parser & Storage │      │ Aggregator       │    │ Anomaly Detector │
└────────┬─────────┘      └────────┬─────────┘    └────────┬─────────┘
         │                         │                       │
         ▼                         ▼                       ▼
┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
│ Collection Mongo │      │ Collection Mongo │    │ Collection Mongo │
│    `trades`      │      │   `aggregates`   │    │    `alerts`      │
└──────────────────┘      └──────────────────┘    └──────────────────┘
```

---

## 📦 Schéma de Données Normalisé (Kafka)

Les deux producteurs publient les trades au format suivant dans `crypto.trades.raw` :

```json
{
  "source": "binance" | "coinbase",
  "symbol": "BTC/USDT" | "ETH/USDT" | "BTC-USD",
  "price": 61250.80,
  "quantity": 0.045,
  "timestamp": 1716123456789
}
```

---

## 💾 Structuration des Collections MongoDB

Les données traitées par les consommateurs sont écrites dans la base `crypto_monitor` sous trois collections distinctes :

### 1. Collection `trades` (Historique Brut)
Alimentée par le `consumer1-parser.js`.
```json
{
  "_id": "ObjectId",
  "source": "binance",
  "symbol": "BTC/USDT",
  "price": 61250.80,
  "quantity": 0.045,
  "volume": 2756.286,
  "timestamp": "2026-05-19T14:40:56.548Z",
  "insertedAt": "2026-05-19T14:40:56.600Z"
}
```
* **Index** : `{ symbol: 1, timestamp: -1 }` pour accélérer les requêtes d'historique.

### 2. Collection `aggregates` (Moyennes Glissantes)
Alimentée toutes les 5 secondes par `consumer2-aggregator.js`.
```json
{
  "_id": "ObjectId",
  "symbol": "BTC/USDT",
  "window": "1m" | "5m" | "15m" | "1h",
  "vwap": 61248.12,          // Volume-Weighted Average Price
  "totalVolumeUsd": 1254300.50, // Volume cumulé en USD
  "totalQuantity": 20.48,     // Quantité totale échangée
  "priceVariation": 0.15,     // Variation de prix en % dans la fenêtre
  "timestamp": "2026-05-19T14:41:00.000Z"
}
```
* **Index** : `{ symbol: 1, window: 1 }` (clé unique de mise à jour/upsert).

### 3. Collection `alerts` (Anomalies)
Alimentée en temps réel par `consumer3-anomaly.js`.
```json
{
  "_id": "ObjectId",
  "type": "HIGH_SPREAD" | "LARGE_VOLUME" | "PRICE_VOLATILITY",
  "symbol": "BTC" | "BTC/USDT",
  "message": "L'écart de prix entre Binance ($61250.00) et Coinbase ($61237.50) est de $12.50",
  "severity": "warning",
  "timestamp": "2026-05-19T14:41:02.124Z"
}
```
* **Index** : `{ severity: 1, timestamp: -1 }` pour trier rapidement les dernières alertes.

---

## ⚙️ Les Consommateurs Kafka en Détail

Chaque consommateur tourne dans un **Consumer Group** dédié de KafkaJS pour assurer une consommation parallèle sans interférence :

### 1. Consumer Parser (`consumer1-parser.js`)
* **Consumer Group** : `group-parser`.
* **Rôle** : Reçoit chaque message, effectue une validation de type et de structure, puis l'écrit de manière asynchrone dans MongoDB.

### 2. Consumer Aggregator (`consumer2-aggregator.js`)
* **Consumer Group** : `group-aggregator`.
* **Rôle** : Utilise un tampon en mémoire pour conserver les trades de la dernière heure. Toutes les 5 secondes, il calcule le VWAP, le volume total et la variation en pourcentage pour chaque symbole et chaque fenêtre (`1m`, `5m`, `15m`, `1h`), puis effectue un `updateOne` avec `upsert: true` dans MongoDB.

### 3. Consumer Anomaly Detector (`consumer3-anomaly.js`)
* **Consumer Group** : `group-anomaly`.
* **Rôle** : Analyse le flux de messages pour identifier 3 types d'anomalies :
  1. **Spread d'Arbitrage** : Écart de prix supérieur à $12 pour le BTC entre Binance (`BTC/USDT`) et Coinbase (`BTC-USD`). Alertes limitées à une par 10s maximum via un mécanisme de throttle.
  2. **Pics de volume** : Volume d'un trade supérieur à 3 fois la moyenne des 10 dernières transactions du symbole.
  3. **Volatilité soudaine** : Déviation de prix supérieure à 1% par rapport à la moyenne mobile des 10 dernières secondes.

---

## 🚀 Lancement Complet de l'Application

### Étape 1 : Démarrer la Stack Docker
Lancez Kafka, Zookeeper, Control Center et MongoDB :
```bash
docker compose up -d
```

### Étape 2 : Créer le Topic Kafka
Créez le topic si ce n'est pas déjà fait :
```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic crypto.trades.raw --partitions 3 --replication-factor 1 --config retention.ms=86400000
```

### Étape 3 : Lancer tout le pipeline
Démarrez les producteurs et les consommateurs en parallèle :
```bash
npm start
```

*Pour lancer uniquement les producteurs* : `npm run producers`
*Pour lancer uniquement les consommateurs* : `npm run consumers`

---

## 🔍 Validation du Pipeline

1. **MongoDB** : Connectez-vous à votre instance locale MongoDB (`mongodb://localhost:27017`) pour vérifier le remplissage en temps réel des collections de la base `crypto_monitor`.
2. **Terminal** : Les logs des consommateurs afficheront en direct les alertes détectées par le module d'anomalies.
3. **Kafka Control Center** : Visualisez l'activité des trois consumer groups distincts sur [http://localhost:9021](http://localhost:9021).
