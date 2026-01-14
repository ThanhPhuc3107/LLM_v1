// services/mongo.js
const { MongoClient } = require('mongodb');
const config = require('../config');

let _client;
let _db;

async function connectMongo() {
  if (_db) return _db;

  if (!config.MONGODB_URI) {
    throw new Error('Missing MONGODB_URI in .env');
  }

  _client = new MongoClient(config.MONGODB_URI, {
    maxPoolSize: 10
  });

  await _client.connect();
  _db = _client.db(config.MONGODB_DBNAME);
  return _db;
}

async function getCollection(name = config.MONGODB_COLLECTION) {
  const db = await connectMongo();
  return db.collection(name);
}

async function closeMongo() {
  if (_client) {
    await _client.close();
    _client = null;
    _db = null;
  }
}

module.exports = { connectMongo, getCollection, closeMongo };
