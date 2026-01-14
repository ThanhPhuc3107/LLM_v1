// config.js
require('dotenv').config();

function intEnv(name, fallback) {
  const v = process.env[name];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

module.exports = {
  // APS (Autodesk Platform Services)
  APS_CLIENT_ID: process.env.APS_CLIENT_ID,
  APS_CLIENT_SECRET: process.env.APS_CLIENT_SECRET,
  APS_BUCKET: process.env.APS_BUCKET,
  APS_CALLBACK_URL: process.env.APS_CALLBACK_URL, // should match APS app settings

  // Server
  PORT: intEnv('PORT', 8080),
  SERVER_SESSION_SECRET: process.env.SERVER_SESSION_SECRET || 'change-me',

  // SQLite (Database)
  SQLITE_PATH: process.env.SQLITE_PATH || require('path').join(__dirname, 'data/bim.db'),

  // OpenAI (Embeddings for semantic search + Chat)
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  OPENAI_CHAT_MODEL: process.env.OPENAI_CHAT_MODEL || 'gpt-4o-mini',
  EMBEDDING_MODEL: process.env.EMBEDDING_MODEL || 'text-embedding-3-small',
  EMBEDDING_DIMENSIONS: parseInt(process.env.EMBEDDING_DIMENSIONS || '512', 10),

  // Gemini (AI Chat)
  GEMINI_API_KEY: process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY,
  GEMINI_MODEL: process.env.GEMINI_MODEL || 'gemini-1.5-flash'
};
