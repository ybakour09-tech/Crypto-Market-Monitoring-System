# 🛠️ Streaming Ingestion Pipeline — Guide Complet de Fonctionnement

Ce document détaille le fonctionnement, l'architecture et l'implémentation de la phase d'ingestion temps réel du projet. Ce module se connecte directement aux APIs WebSocket de Binance et Coinbase, normalise les données des transactions (trades), puis les publie dans un topic Apache Kafka partitionné.

---

## 📂 Architecture des Fichiers de l'Ingestion

La structure minimale et optimisée du projet contient les fichiers suivants :

* **`docker-compose.yml`** : Orchestre la stack de serveurs locaux (Zookeeper, Kafka Broker, Schema Registry et Confluent Control Center).
* **`.env`** : Contient les variables de configuration globales (ports, brokers, URLs des WebSockets).
* **`package.json`** : Définit les métadonnées du projet, les dépendances Node.js (`kafkajs`, `ws`, `dotenv`, `concurrently`) et le script de démarrage.
* **`shared/kafka-config.js`** : Centralise le client KafkaJS, l'initialisation du Producer et la définition du topic partagé.
* **`ingestion/binance-producer.js`** : Client WebSocket connecté à l'API Binance qui extrait et normalise les trades de `BTC/USDT` et `ETH/USDT`.
* **`ingestion/coinbase-producer.js`** : Client WebSocket connecté à Coinbase qui souscrit au canal `market_trades` pour `BTC-USD` et normalise les données.

---

## 🐳 Architecture Infrastructure (Docker Compose)

La stack utilise des images de la **Confluent Platform** pour offrir une expérience robuste et une interface utilisateur d'administration complète :

1. **Zookeeper (`confluentinc/cp-zookeeper`)** : Gère la coordination et l'état des brokers Kafka du cluster.
2. **Kafka Broker (`confluentinc/cp-kafka`)** : Le moteur de messagerie qui héberge le topic et les partitions.
   * Configuré sur le port externe `9092` pour le code Node.js local.
   * `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` pour interdire la création automatique de topics avec des paramètres par défaut inadaptés.
3. **Control Center (`confluentinc/cp-enterprise-control-center`)** : L'interface web d'administration de Confluent accessible sur le port **`9021`**. Elle permet de visualiser l'état du cluster, de lister les topics et d'inspecter les messages entrants en temps réel.
4. **Schema Registry (`confluentinc/cp-schema-registry`)** : Gère les schémas de données (ex: Avro) si besoin de typage strict des messages.

---

## 📦 Le Topic Kafka `crypto.trades.raw`

Pour répondre aux exigences de production, le topic est structuré avec les spécificités suivantes :
* **Nombre de Partitions** : `3`. Les messages y sont distribués en fonction de la clé de partitionnement (le symbole). Ainsi, tous les trades de `BTC/USDT` vont sur la même partition, garantissant un ordre chronologique strict indispensable pour l'analyse technique ultérieure.
* **Durée de Rétention** : `24 heures` (`retention.ms=86400000`). Kafka conserve les données brutes sur disque pendant 24h, agissant comme un tampon résilient si les consommateurs s'arrêtent ou tombent en panne.

---

## ⚡ Mécanisme d'Ingestion & Normalisation

### 1. Structure Commune des Messages (Normalisée)
Chaque exchange renvoie les données de transaction dans un format JSON propriétaire. Les producteurs transforment ces messages bruts en un objet standardisé unique avant de l'envoyer dans Kafka :

```json
{
  "source": "binance" | "coinbase",
  "symbol": "BTC/USDT" | "ETH/USDT" | "BTC-USD",
  "price": 61250.80,       // Nombre à virgule flottante
  "quantity": 0.045,       // Nombre à virgule flottante
  "volume": 2756.286,      // Prix * Quantité
  "timestamp": 1716123456789 // Temps Unix en millisecondes
}
```

### 2. Le Flux Binance (`ingestion/binance-producer.js`)
* **Connexion** : Se connecte à l'API WebSocket combinée : `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade`.
* **Événements** : Reçoit des trames JSON contenant la clé `"data"`.
* **Mapping** :
  * `s` (Symbol) -> Traduit en format propre (`BTC/USDT` ou `ETH/USDT`).
  * `p` (Price) -> Converti en Float.
  * `q` (Quantity) -> Converti en Float.
  * `T` (Timestamp) -> Temps Unix de la transaction.

### 3. Le Flux Coinbase (`ingestion/coinbase-producer.js`)
* **Connexion** : Se connecte à l'API WebSocket Avancée : `wss://advanced-trade-ws.coinbase.com`.
* **Abonnement** : Envoie une trame d'abonnement explicite lors de l'ouverture de la connexion :
  ```json
  {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "market_trades"
  }
  ```
* **Mapping** : Reçoit des messages sur le canal `market_trades` contenant une liste de transactions dans `events[].trades[]`. Le timestamp ISO de Coinbase (`time`) est converti en timestamp millisecondes Unix.

---

## 🛡️ Résilience et Gestion des Défaillances

Pour garantir une exécution robuste en production, plusieurs mécanismes de sécurité sont implémentés :

1. **Reconnexion avec Backoff Exponentiel** :
   En cas de déconnexion réseau des WebSockets de Binance ou Coinbase, le script ne plante pas. Il attend un délai initial de `1000ms`, tente de se reconnecter, et double ce délai à chaque échec successif jusqu'à atteindre un plafond maximal de `30000ms`. Le délai est réinitialisé dès que la connexion réussit.
2. **Gestion des Retries Kafka** :
   Le client KafkaJS est configuré avec un mécanisme de retry automatique (`retries: 10`) avec un temps d'attente initial de `300ms`. Si le broker Kafka est temporairement indisponible, le producteur retentera l'envoi des messages sans crasher.

---

## 🚀 Guide d'Exécution Pas-à-Pas

### Étape 1 : Démarrer l'infrastructure Docker
Lancez les conteneurs Kafka, Zookeeper et le Control Center en arrière-plan :
```bash
docker compose up -d
```

### Étape 2 : Créer manuellement le Topic partitionné
Exécutez cette commande dans le conteneur du broker pour créer le topic avec les paramètres optimisés :
```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic crypto.trades.raw --partitions 3 --replication-factor 1 --config retention.ms=86400000
```

### Étape 3 : Installer les dépendances Node.js
Installez les bibliothèques requises définies dans le `package.json` :
```bash
npm install
```

### Étape 4 : Lancer le pipeline d'ingestion
Démarrez les producteurs Binance et Coinbase en parallèle via le script concurrentiel :
```bash
npm start
```

---

## 🔍 Validation et Débogage

### 1. Avec l'interface graphique (Confluent Control Center)
* Ouvrez votre navigateur et accédez à : **[http://localhost:9021](http://localhost:9021)**
* Cliquez sur votre cluster, puis naviguez dans l'onglet **Topics** à gauche.
* Sélectionnez **`crypto.trades.raw`** et cliquez sur l'onglet **Messages** pour observer les flux de transactions défiler en direct.

### 2. Depuis la ligne de commande (CLI)
Pour consommer directement les messages depuis le terminal pour valider l'ingestion :
```bash
docker exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic crypto.trades.raw --from-beginning --property print.key=true
```
Cette commande affichera la clé (le symbole) et le message JSON normalisé pour chaque trade produit dans Kafka.
