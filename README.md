# ⚡ Real-Time Crypto Monitoring — Pipeline d'Ingestion & Traitement Kafka + MongoDB

Ce projet implémente un pipeline complet d'ingestion et de traitement de données de marché de crypto-monnaies en temps réel. Il récupère les trades en direct de **Binance** et **Coinbase** via WebSockets, les achemine à travers **Apache Kafka** en conservant l'ordre chronologique, puis les traite en parallèle via des **consommateurs autonomes** pour la persistance, le calcul statistique glissant et la détection immédiate d'anomalies dans **MongoDB**.

---

## 📐 Architecture Globale du Flux de Données

```
 ┌────────────────────────┐         ┌────────────────────────┐
 │   Binance WebSocket    │         │   Coinbase WebSocket   │
 │   BTC/USDT, ETH/USDT   │         │        BTC-USD         │
 └───────────┬────────────┘         └───────────┬────────────┘
             │                                  │
             ▼                                  ▼
 ┌────────────────────────┐         ┌────────────────────────┐
 │   binance-producer.js  │         │  coinbase-producer.js  │
 │ (Mapping & Résilience)  │         │ (Mapping & Résilience)  │
 └───────────┬────────────┘         └───────────┬────────────┘
             │                                  │
             └─────────────────┬────────────────┘
                               ▼ (JSON Standardisé avec Clé = Symbole)
                  ┌──────────────────────────┐
                  │    Topic Apache Kafka    │
                  │   `crypto.trades.raw`    │
                  │ (3 Partitions, Clé = Sym)│
                  └────────────┬─────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │ (Consommation parallèle par groupe)      │
          ▼                                         ▼
 ┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
 │ Consumer 1       │      │ Consumer 2       │    │ Consumer 3       │
 │ Parser & Storage │      │ Aggregator       │    │ Anomaly Detector │
 └────────┬─────────┘      └────────┬─────────┘    └────────┬─────────┘
          │                         │                       │
          ▼                         ▼                       ▼
 ┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
 │ MongoDB `trades` │      │  Mongo `aggreg's`│    │  Mongo `alerts`  │
 └──────────────────┘      └──────────────────┘    └──────────────────┘
```

---

## 🧠 Focus : Traitement Temps Réel & Conception

Le traitement en temps réel est optimisé pour la performance, la résilience et le découplage grâce aux concepts suivants :

### 1. Ingestion WebSocket & Résilience
* **Vitesse Maximale** : La connexion WebSocket évite l'overhead du HTTP Polling en poussant instantanément chaque transaction dès qu'elle se produit sur le marché mondial.
* **Mapping Unifié** : Les formats JSON propriétaires sont normalisés en un schéma commun contenant la source, le symbole, le prix, la quantité et le timestamp Unix.
* **Backoff Exponentiel** : En cas de déconnexion réseau, les producteurs s'auto-reconnectent en doublant le délai d'attente à chaque essai (de 1s à un maximum de 30s) pour éviter de saturer les serveurs distants.

### 2. Transport Kafka & Garantie d'Ordre
* **Clé de Partitionnement** : Le `symbol` (ex: `BTC/USDT`) sert de clé de message Kafka. Kafka garantit que tous les messages ayant la même clé atterrissent sur la **même partition** physique. Ainsi, l'ordre chronologique des trades d'un symbole est strictement conservé.
* **Consumer Groups** : Chaque consommateur s'enregistre dans son propre groupe (`group-parser`, `group-aggregator`, `group-anomaly`). Cela permet à Kafka de distribuer le flux de manière indépendante pour que chaque logique s'exécute en parallèle sans ralentir les autres.

### 3. Logique des Consommateurs Temps Réel

#### 📥 Consumer 1 : Parser & Écriture (`consumer1-parser.js`)
* Il lit les messages un par un, convertit le timestamp en objet `Date` natif MongoDB pour de meilleures performances de recherche temporelle, puis insère les documents dans la collection `trades`.

#### 📊 Consumer 2 : Agrégateur Glissant (`consumer2-aggregator.js`)
* **Tampon en mémoire** : Pour ne pas surcharger MongoDB à chaque milliseconde, il accumule les trades entrants dans un tableau mémoire d'une capacité glissante maximale de 1 heure.
* **Flushing Périodique (5s)** : Toutes les 5 secondes, il calcule les métriques pour les fenêtres glissantes (`1m`, `5m`, `15m`, `1h`) :
  - **Volume-Weighted Average Price (VWAP)** : $\frac{\sum (\text{Prix} \times \text{Quantité})}{\sum \text{Quantité}}$. Le prix est ainsi pondéré par le poids financier de chaque trade.
  - **Volume Cumulé** : Somme des volumes (Prix × Quantité) en USD sur la période.
  - **Variation de Prix** : Écart en pourcentage entre la première transaction de la fenêtre et la plus récente.
* Les agrégats sont mis à jour dans la collection `aggregates` via des requêtes `updateOne` avec `upsert: true`.

#### 🚨 Consumer 3 : Détecteur d'Anomalies (`consumer3-anomaly.js`)
Il écoute le flux de transactions en continu et applique immédiatement trois règles d'alerte :
1. **Écart Inter-Exchanges (Spread d'Arbitrage)** : Compare en continu les derniers prix reçus pour le BTC sur Binance (`BTC/USDT`) et Coinbase (`BTC-USD`). Si l'écart dépasse **$12**, une alerte de type `HIGH_SPREAD` est générée. Un système de **throttle** (limiteur) restreint cette alerte à une fois toutes les 10 secondes maximum pour éviter de saturer la base de données.
2. **Pic de Volume (Large Volume)** : Analyse les volumes des 10 dernières transactions d'un symbole. Si le volume du nouveau trade est **supérieur à 3 fois** la moyenne glissante de ces transactions, une alerte `LARGE_VOLUME` est enregistrée.
3. **Volatilité Soudaine (Volatility)** : Calcule la moyenne mobile du prix sur une fenêtre de 10 secondes. Si le prix d'un nouveau trade dévie de **plus de 1%** par rapport à cette moyenne, une alerte `PRICE_VOLATILITY` est déclenchée.

---

## 🛠️ Guide d'Exécution

### 1. Prérequis
Assurez-vous d'avoir installé localement :
* [Node.js](https://nodejs.org/) (v16 ou supérieur)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### 2. Démarrage de l'Infrastructure (Docker)
Démarrez les services Kafka, Zookeeper, Confluent Control Center et MongoDB en arrière-plan :
```bash
docker compose up -d
```

### 3. Initialisation du Topic Kafka
Exécutez cette commande pour créer le topic avec ses 3 partitions et sa rétention de 24 heures :
```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic crypto.trades.raw --partitions 3 --replication-factor 1 --config retention.ms=86400000
```

### 4. Installation des Dépendances Node.js
```bash
npm install
```

### 5. Lancement de l'Application
Lancez les producteurs et les consommateurs simultanément :
```bash
npm start
```

* **Pour exécuter uniquement les flux WebSockets (Ingestion)** : `npm run producers`
* **Pour exécuter uniquement le traitement (Consommateurs)** : `npm run consumers`

---

## 🔗 Liens Associés & Outils

* 📊 **Confluent Control Center** : [http://localhost:9021](http://localhost:9021)
  * *Permet de surveiller la santé des brokers, inspecter le topic `crypto.trades.raw` et suivre le lag de consommation des consumer groups en temps réel.*
* 🗄️ **MongoDB Connection URI** : `mongodb://localhost:27017/crypto_monitor`
  * *Utilisable avec [MongoDB Compass](https://www.mongodb.com/products/tools/compass) pour observer les collections `trades`, `aggregates` et `alerts` se mettre à jour en direct.*
* 🔌 **WebSocket Binance** : `wss://stream.binance.com:9443`
* 🔌 **WebSocket Coinbase** : `wss://advanced-trade-ws.coinbase.com`