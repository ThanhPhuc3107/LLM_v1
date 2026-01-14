// services/embeddings.js
const OpenAI = require('openai');
const config = require('../config');

let _client;

function getClient() {
  if (_client) return _client;

  if (!config.OPENAI_API_KEY) {
    throw new Error('Missing OPENAI_API_KEY in .env or config');
  }

  _client = new OpenAI({ apiKey: config.OPENAI_API_KEY });
  return _client;
}

// Generate embedding for single text
async function generateEmbedding(text) {
  if (!text || typeof text !== 'string' || text.trim() === '') {
    console.warn('⚠ generateEmbedding: Empty or invalid text provided');
    return null;
  }

  try {
    const client = getClient();
    const response = await client.embeddings.create({
      model: config.EMBEDDING_MODEL || 'text-embedding-3-small',
      input: text.trim(),
      dimensions: config.EMBEDDING_DIMENSIONS || 512,
    });
    return response.data[0].embedding;
  } catch (error) {
    console.error('❌ Error generating embedding:', error.message);
    throw error;
  }
}

// Batch generate embeddings (up to 100 texts per API call)
async function generateEmbeddings(texts) {
  if (!Array.isArray(texts) || texts.length === 0) {
    return [];
  }

  // Filter out empty texts and track original indexes
  const validTexts = [];
  const validIndexes = [];

  texts.forEach((text, index) => {
    if (text && typeof text === 'string' && text.trim() !== '') {
      validTexts.push(text.trim());
      validIndexes.push(index);
    }
  });

  if (validTexts.length === 0) {
    console.warn('⚠ generateEmbeddings: No valid texts provided');
    return Array(texts.length).fill(null);
  }

  const batchSize = 100; // OpenAI API limit
  const allEmbeddings = Array(texts.length).fill(null);

  try {
    const client = getClient();

    for (let i = 0; i < validTexts.length; i += batchSize) {
      const batch = validTexts.slice(i, i + batchSize);
      const batchIndexes = validIndexes.slice(i, i + batchSize);

      console.log(`  Generating embeddings ${i + 1}-${Math.min(i + batchSize, validTexts.length)} of ${validTexts.length}...`);

      const response = await client.embeddings.create({
        model: config.EMBEDDING_MODEL || 'text-embedding-3-small',
        input: batch,
        dimensions: config.EMBEDDING_DIMENSIONS || 512,
      });

      // Map embeddings back to original indexes
      response.data.forEach((item, batchIdx) => {
        const originalIdx = batchIndexes[batchIdx];
        allEmbeddings[originalIdx] = item.embedding;
      });

      // Rate limiting: Small delay between batches
      if (i + batchSize < validTexts.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    console.log(`✓ Generated ${validTexts.length} embeddings`);
    return allEmbeddings;
  } catch (error) {
    console.error('❌ Error in batch embedding generation:', error.message);
    throw error;
  }
}

// Generate searchable text description from element fields
function generateEmbeddingText(element) {
  const parts = [
    element.name,
    element.component_type,
    element.type_name,
    element.family_name,
    element.level_number ? `Tầng ${element.level_number}` : null,
    element.room_name ? `Phòng ${element.room_name}` : null,
    element.room_type,
    element.system_name,
    element.system_type,
    element.manufacturer,
    element.model_name,
    element.omniclass_title,
  ].filter(Boolean);

  return parts.join(' | ');
}

module.exports = {
  generateEmbedding,
  generateEmbeddings,
  generateEmbeddingText
};
