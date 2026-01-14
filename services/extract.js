// services/extract.js
// Phase 1: APS Model Derivative properties -> SQLite "elements" table
const mapping = require('./dmMapping');
const { getDb } = require('./sqlite');
const { getMetadata, getProperties } = require('./aps');
const { generateEmbeddings, generateEmbeddingText } = require('./embeddings');

function normalizeKey(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[_\-]+/g, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isEmpty(v) {
  return v === null || v === undefined || (typeof v === 'string' && v.trim() === '');
}

function flattenProps(propertiesObj) {
  // APS "properties" is usually: { "Group Name": { "Prop Name": value, ... }, ... }
  const flat = {};
  if (!propertiesObj || typeof propertiesObj !== 'object') return flat;

  for (const [groupName, groupProps] of Object.entries(propertiesObj)) {
    if (!groupProps || typeof groupProps !== 'object') continue;
    for (const [propName, value] of Object.entries(groupProps)) {
      const key = `${groupName}.${propName}`;
      flat[key] = value;
    }
  }
  return flat;
}

function findInFlat(flat, keywords = []) {
  const entries = Object.entries(flat || {});
  if (!entries.length) return null;

  for (const kw of keywords) {
    const kwN = normalizeKey(kw);
    if (!kwN) continue;

    for (const [k, v] of entries) {
      if (isEmpty(v)) continue;
      const kN = normalizeKey(k);
      if (kN.includes(kwN)) return v;
    }
  }
  return null;
}

function inferCategory(typeName, familyName) {
  const s = `${typeName || ''} ${familyName || ''}`.toLowerCase();
  if (/(door|cửa)/i.test(s)) return 'Doors';
  if (/(window|cửa sổ)/i.test(s)) return 'Windows';
  if (/wall|tường/i.test(s)) return 'Walls';
  if (/floor|sàn/i.test(s)) return 'Floors';
  if (/room|phòng/i.test(s)) return 'Rooms';
  return null;
}

function build5Groups(flat, dbId) {
  const type_name = findInFlat(flat, mapping.basic.type_name);
  const family_name = findInFlat(flat, mapping.basic.family_name);

  const omniclassTitle = findInFlat(flat, mapping.omniclass.title);
  const omniclassNumber = findInFlat(flat, mapping.omniclass.number);

  const component_type_raw =
    findInFlat(flat, mapping.basic.component_type) ||
    inferCategory(type_name, family_name) ||
    'Unknown';

  // Prefer OmniClass Title as CATEGORY when available (Doors/Windows/...)
  const component_type =
    !isEmpty(omniclassTitle) ? String(omniclassTitle).trim() : String(component_type_raw).trim();

  const basic = {
    component_id: dbId,
    component_type,
    type_name: type_name ? String(type_name) : null,
    family_name: family_name ? String(family_name) : null,
    is_asset: findInFlat(flat, mapping.basic.is_asset)
  };

  const location = {
    level_number: findInFlat(flat, mapping.location.level_number),
    room_type: findInFlat(flat, mapping.location.room_type),
    room_name: findInFlat(flat, mapping.location.room_name)
  };

  const system = {
    system_type: findInFlat(flat, mapping.system.system_type),
    system_name: findInFlat(flat, mapping.system.system_name)
  };

  const equipment = {
    manufacturer: findInFlat(flat, mapping.equipment.manufacturer),
    model_name: findInFlat(flat, mapping.equipment.model_name),
    specification: findInFlat(flat, mapping.equipment.specification)
  };

  const omniclass = {
    title: omniclassTitle ? String(omniclassTitle) : null,
    number: omniclassNumber ? String(omniclassNumber) : null
  };

  return { basic, location, system, equipment, omniclass };
}

function pickFirstGuid(metadata) {
  // Typical: metadata.data.metadata = [{ guid, name }, ...]
  const arr = metadata?.data?.metadata;
  if (Array.isArray(arr) && arr.length) return arr[0].guid;
  // Fallback: metadata.data (older shapes)
  if (Array.isArray(metadata?.data) && metadata.data.length) return metadata.data[0].guid;
  throw new Error('Cannot find viewable GUID in metadata response');
}

async function extractModelToSqlite(urn) {
  const db = getDb();

  // 1) metadata -> guid
  const metadata = await getMetadata(urn);
  const guid = pickFirstGuid(metadata);

  // 2) properties -> collection
  const props = await getProperties(urn, guid);
  const items = props?.data?.collection || [];
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error('No properties collection (Model Derivative not ready, or model has no viewable)');
  }

  console.log(`📦 Extracting ${items.length} elements...`);

  // 3) Build docs
  const docs = [];
  for (const it of items) {
    const dbId = it.objectid ?? it.objectId ?? it.dbId;
    const name = it.name || null;
    const flat = flattenProps(it.properties);
    const groups = build5Groups(flat, dbId);

    docs.push({
      urn,
      guid,
      dbId,
      name,
      ...groups,
      props_flat: JSON.stringify(flat)
    });
  }

  // 4) Generate embeddings
  console.log('🤖 Generating embeddings...');
  const embeddingTexts = docs.map(doc => generateEmbeddingText(doc));

  let embeddings;
  try {
    embeddings = await generateEmbeddings(embeddingTexts);
    console.log('');
  } catch (error) {
    console.error('❌ Failed to generate embeddings:', error.message);
    console.log('⚠  Continuing without embeddings.\n');
    embeddings = Array(docs.length).fill(null);
  }

  // 5) Add embeddings to docs
  docs.forEach((doc, i) => {
    doc.embedding = embeddings[i] ? JSON.stringify(embeddings[i]) : null;
    doc.embedding_model = embeddings[i] ? 'text-embedding-3-small' : null;
  });

  // 6) Clean old docs
  db.prepare('DELETE FROM elements WHERE urn = ? AND guid = ?').run(urn, guid);

  // 7) Insert in chunks using transaction
  console.log('💾 Inserting into SQLite...');

  const insertStmt = db.prepare(`
    INSERT INTO elements (
      urn, guid, dbId, name,
      component_id, component_type, type_name, family_name, is_asset,
      level_number, room_type, room_name,
      system_type, system_name,
      manufacturer, model_name, specification,
      omniclass_title, omniclass_number,
      props_flat, embedding, embedding_model
    ) VALUES (
      ?, ?, ?, ?,
      ?, ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?,
      ?, ?, ?,
      ?, ?,
      ?, ?, ?
    )
  `);

  const insertMany = db.transaction((rows) => {
    for (const row of rows) {
      insertStmt.run(
        row.urn, row.guid, row.dbId, row.name,
        row.basic.component_id, row.basic.component_type, row.basic.type_name, row.basic.family_name, row.basic.is_asset,
        row.location.level_number, row.location.room_type, row.location.room_name,
        row.system.system_type, row.system.system_name,
        row.equipment.manufacturer, row.equipment.model_name, row.equipment.specification,
        row.omniclass.title, row.omniclass.number,
        row.props_flat, row.embedding, row.embedding_model
      );
    }
  });

  insertMany(docs);

  console.log(`✅ Inserted ${docs.length} elements`);

  return {
    urn,
    guid,
    inserted: docs.length
  };
}

module.exports = { extractModelToSqlite };
