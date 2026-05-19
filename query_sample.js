const { MongoClient } = require('mongodb');

async function main() {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db('crypto_monitor');
    
    console.log("--- 1 TRADE ---");
    console.log(await db.collection('trades').findOne({}));
    
    console.log("\n--- 1 AGGREGATE ---");
    console.log(await db.collection('aggregates').findOne({}));
    
    console.log("\n--- 1 ALERT ---");
    console.log(await db.collection('alerts').findOne({}));
    
  } finally {
    await client.close();
  }
}

main().catch(console.error);
