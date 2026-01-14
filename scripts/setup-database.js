// scripts/setup-database.js
// Complete database setup: preprocess data + migrate to SQLite

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

console.log('🚀 BIM Database Setup\n');
console.log('This script will:');
console.log('  1. Preprocess bim-data.json (clean empty/hidden properties)');
console.log('  2. Migrate cleaned data to SQLite');
console.log('  3. Generate embeddings with OpenAI\n');

// Check if bim-data.json exists
const rawDataPath = path.join(__dirname, '../bim-data.json');
if (!fs.existsSync(rawDataPath)) {
    console.error('❌ Error: bim-data.json not found!');
    console.error('   Expected location:', rawDataPath);
    console.error('\n   Please ensure bim-data.json exists before running this script.\n');
    process.exit(1);
}

// Check for OpenAI API key
require('dotenv').config();
if (!process.env.OPENAI_API_KEY) {
    console.error('❌ Error: OPENAI_API_KEY not found in .env file!');
    console.error('\n   Please add your OpenAI API key to .env:');
    console.error('   OPENAI_API_KEY=sk-...\n');
    process.exit(1);
}

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

// Step 1: Preprocess data
console.log('📌 Step 1: Preprocessing data...\n');
try {
    execSync('node scripts/preprocess-data.js', { stdio: 'inherit' });
} catch (error) {
    console.error('\n❌ Preprocessing failed!');
    process.exit(1);
}

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

// Step 2: Migrate to SQLite
console.log('📌 Step 2: Migrating to SQLite with embeddings...\n');
try {
    execSync('node scripts/migrate-to-sqlite.js', { stdio: 'inherit' });
} catch (error) {
    console.error('\n❌ Migration failed!');
    process.exit(1);
}

console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
console.log('✅ Database setup complete!\n');
console.log('Next steps:');
console.log('  1. Start the server: npm start');
console.log('  2. Open http://localhost:8080');
console.log('  3. Click the 💬 Chat button to test the chatbot\n');
