// services/sqlite.js
const Database = require('better-sqlite3');
const config = require('../config');
const path = require('path');

let _db;

function getDb() {
  if (_db) return _db;

  const dbPath = config.SQLITE_PATH || path.join(__dirname, '../data/bim.db');
  _db = new Database(dbPath);
  _db.pragma('journal_mode = WAL'); // Write-Ahead Logging for concurrency
  _db.pragma('foreign_keys = ON');

  return _db;
}

// Initialize schema
function initSchema() {
  const db = getDb();

  // Main elements table
  db.exec(`
    CREATE TABLE IF NOT EXISTS elements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,

      -- Core identifiers
      urn TEXT NOT NULL,
      guid TEXT,
      dbId INTEGER NOT NULL,
      name TEXT,

      -- Structured fields (from BIM properties)
      component_id INTEGER,
      component_type TEXT,
      type_name TEXT,
      family_name TEXT,
      is_asset TEXT,

      -- Location fields
      level_number TEXT,
      room_type TEXT,
      room_name TEXT,

      -- System fields
      system_type TEXT,
      system_name TEXT,

      -- Equipment fields
      manufacturer TEXT,
      model_name TEXT,
      specification TEXT,

      -- OmniClass fields
      omniclass_title TEXT,
      omniclass_number TEXT,

      -- Original properties as JSON
      props_flat TEXT,

      -- Semantic search embedding
      embedding TEXT,
      embedding_model TEXT,

      -- Metadata
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Create indexes
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_urn ON elements(urn);
    CREATE INDEX IF NOT EXISTS idx_urn_component_type ON elements(urn, component_type);
    CREATE INDEX IF NOT EXISTS idx_urn_omniclass ON elements(urn, omniclass_title);
    CREATE INDEX IF NOT EXISTS idx_urn_level ON elements(urn, level_number);
    CREATE INDEX IF NOT EXISTS idx_dbid ON elements(dbId);
  `);

  // Full-text search index for natural language queries
  db.exec(`
    CREATE VIRTUAL TABLE IF NOT EXISTS elements_fts USING fts5(
      name,
      component_type,
      type_name,
      family_name,
      level_number,
      room_name,
      omniclass_title,
      content=elements,
      content_rowid=id
    );
  `);

  // Triggers to keep FTS in sync
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS elements_fts_insert AFTER INSERT ON elements BEGIN
      INSERT INTO elements_fts(rowid, name, component_type, type_name, family_name, level_number, room_name, omniclass_title)
      VALUES (new.id, new.name, new.component_type, new.type_name, new.family_name, new.level_number, new.room_name, new.omniclass_title);
    END;

    CREATE TRIGGER IF NOT EXISTS elements_fts_delete AFTER DELETE ON elements BEGIN
      DELETE FROM elements_fts WHERE rowid = old.id;
    END;

    CREATE TRIGGER IF NOT EXISTS elements_fts_update AFTER UPDATE ON elements BEGIN
      UPDATE elements_fts SET
        name = new.name,
        component_type = new.component_type,
        type_name = new.type_name,
        family_name = new.family_name,
        level_number = new.level_number,
        room_name = new.room_name,
        omniclass_title = new.omniclass_title
      WHERE rowid = new.id;
    END;
  `);

  console.log('✓ SQLite schema initialized');
}

// Cosine similarity calculation
function cosineSimilarity(a, b) {
  if (!a || !b || a.length !== b.length) return 0;

  let dot = 0;
  let magA = 0;
  let magB = 0;

  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }

  const magnitude = Math.sqrt(magA) * Math.sqrt(magB);
  return magnitude === 0 ? 0 : dot / magnitude;
}

// Vector search: Returns top-k element IDs by cosine similarity
function semanticSearch(db, urn, queryEmbedding, k = 20) {
  // Load all embeddings for the given URN
  const stmt = db.prepare(
    'SELECT id, embedding FROM elements WHERE urn = ? AND embedding IS NOT NULL'
  );
  const rows = stmt.all(urn);

  if (rows.length === 0) {
    console.log('⚠ No embeddings found for URN:', urn);
    return [];
  }

  // Calculate similarities
  const scores = rows.map(row => {
    const embedding = JSON.parse(row.embedding);
    const score = cosineSimilarity(queryEmbedding, embedding);
    return { id: row.id, score };
  });

  // Sort by score descending and return top-k IDs
  scores.sort((a, b) => b.score - a.score);
  return scores.slice(0, k).map(s => s.id);
}

// Query helpers (match mongo.js API)
async function getCollection() {
  return getDb(); // Return db instance for compatibility
}

// Close database
function closeDb() {
  if (_db) {
    _db.close();
    _db = null;
  }
}

module.exports = {
  getDb,
  initSchema,
  getCollection,
  semanticSearch,
  closeDb
};
