// scripts/preprocess-data.js
// Preprocess BIM data: normalize categories, clean properties, enrich metadata

const fs = require('fs');
const path = require('path');

console.log('🧹 Preprocessing bim-data.json...\n');

const inputPath = path.join(__dirname, '../bim-data.json');
const outputPath = path.join(__dirname, '../bim-data.clean.json');

const rawData = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
console.log(`📂 Loaded ${rawData.length} elements from bim-data.json\n`);

// Category normalization map (Revit categories → Standard categories)
const CATEGORY_MAP = {
  // Structural
  'revit walls': 'Walls',
  'revit structural columns': 'Columns',
  'revit structural framing': 'Beams',
  'revit structural foundations': 'Foundations',
  'revit structural rebar': 'Reinforcement',
  'revit floors': 'Floors',
  'revit ceilings': 'Ceilings',
  'revit roofs': 'Roofs',

  // Architectural
  'revit doors': 'Doors',
  'revit windows': 'Windows',
  'revit curtain panels': 'Curtain Panels',
  'revit curtain wall mullions': 'Curtain Mullions',
  'revit wall sweeps': 'Wall Sweeps',
  'revit stairs': 'Stairs',
  'revit railings': 'Railings',

  // MEP
  'revit pipes': 'Pipes',
  'revit pipe fittings': 'Pipe Fittings',
  'revit pipe accessories': 'Pipe Accessories',
  'revit ducts': 'Ducts',
  'revit duct fittings': 'Duct Fittings',
  'revit mechanical equipment': 'Mechanical Equipment',
  'revit plumbing fixtures': 'Plumbing Fixtures',
  'revit lighting fixtures': 'Lighting Fixtures',
  'revit electrical fixtures': 'Electrical Fixtures',
  'revit electrical equipment': 'Electrical Equipment',

  // Other
  'revit furniture': 'Furniture',
  'revit casework': 'Casework',
  'revit specialty equipment': 'Specialty Equipment',
  'revit generic models': 'Generic Models',
};

// Normalize category name
function normalizeCategory(rawCategory) {
  if (!rawCategory) return null;

  const lower = String(rawCategory).toLowerCase().trim();

  // Direct mapping
  if (CATEGORY_MAP[lower]) {
    return CATEGORY_MAP[lower];
  }

  // Try to extract from "Revit XYZ" pattern
  const match = lower.match(/^revit\s+(.+)$/);
  if (match) {
    const extracted = match[1];
    // Capitalize first letter of each word
    return extracted.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  }

  // Return as-is but capitalized
  return rawCategory.trim();
}

// Infer category from type_name or family_name
function inferCategory(typeName, familyName, name) {
  const combined = `${typeName || ''} ${familyName || ''} ${name || ''}`.toLowerCase();

  // Vietnamese + English keywords
  if (/(door|cửa|cua)/i.test(combined) && !/window|sổ|so/.test(combined)) return 'Doors';
  if (/(window|cửa sổ|cua so)/i.test(combined)) return 'Windows';
  if (/(wall|tường|tuong)/i.test(combined)) return 'Walls';
  if (/(floor|sàn|san)/i.test(combined)) return 'Floors';
  if (/(ceiling|trần|tran)/i.test(combined)) return 'Ceilings';
  if (/(column|cột|cot)/i.test(combined)) return 'Columns';
  if (/(beam|dầm|dam)/i.test(combined)) return 'Beams';
  if (/(roof|mái|mai)/i.test(combined)) return 'Roofs';
  if (/(stair|cầu thang|cau thang)/i.test(combined)) return 'Stairs';
  if (/(railing|lan can)/i.test(combined)) return 'Railings';
  if (/(pipe|ống|ong)/i.test(combined)) return 'Pipes';
  if (/(duct|ống gió)/i.test(combined)) return 'Ducts';
  if (/(lighting|đèn|den)/i.test(combined)) return 'Lighting Fixtures';

  return null;
}

// Find property value by keywords
function findProperty(properties, keywords) {
  if (!Array.isArray(properties)) return null;

  for (const keyword of keywords) {
    const kwLower = keyword.toLowerCase();

    for (const prop of properties) {
      if (!prop.displayName) continue;

      const nameLower = prop.displayName.toLowerCase();
      const catLower = (prop.displayCategory || '').toLowerCase();
      const combined = `${catLower}.${nameLower}`;

      if (nameLower === kwLower || combined.includes(kwLower)) {
        const value = prop.displayValue;
        // Return non-empty values
        if (value !== null && value !== undefined && value !== '') {
          return value;
        }
      }
    }
  }

  return null;
}

// Extract key fields from properties
function extractKeyFields(properties, name) {
  // Extract Category
  const rawCategory = findProperty(properties, [
    'category',
    'revit category',
    'category name',
  ]);

  // Extract Type Name
  const typeName = findProperty(properties, [
    'type name',
    'type',
    'typename',
  ]);

  // Extract Family Name
  const familyName = findProperty(properties, [
    'family name',
    'family',
    'familyname',
  ]);

  // Extract OmniClass
  const omniclassTitle = findProperty(properties, [
    'omniclass title',
    'identity data.omniclass title',
  ]);

  const omniclassNumber = findProperty(properties, [
    'omniclass number',
    'omniclass no',
  ]);

  // Normalize category
  let componentType = normalizeCategory(rawCategory);

  // Fallback: infer from type/family/name
  if (!componentType) {
    componentType = inferCategory(typeName, familyName, name) || 'Unknown';
  }

  // Prefer OmniClass Title as category if available and more specific
  if (omniclassTitle && omniclassTitle.length > 0) {
    componentType = String(omniclassTitle).trim();
  }

  return {
    componentType,
    typeName,
    familyName,
    omniclassTitle,
    omniclassNumber,
  };
}

// Clean and structure properties
function cleanProperties(properties) {
  if (!Array.isArray(properties)) return [];

  const cleaned = [];
  const seen = new Set();

  for (const prop of properties) {
    // Skip if missing required fields
    if (!prop.displayCategory || !prop.displayName) continue;

    // Skip hidden properties
    if (prop.hidden === true || prop.hidden === 1) continue;

    // Skip empty values (but keep 0, false)
    const value = prop.displayValue;
    if (value === null || value === undefined) continue;
    if (typeof value === 'string' && value.trim() === '') continue;

    // Skip duplicate properties
    const key = `${prop.displayCategory}.${prop.displayName}`;
    if (seen.has(key)) continue;
    seen.add(key);

    // Keep cleaned property
    cleaned.push({
      displayCategory: prop.displayCategory,
      displayName: prop.displayName,
      displayValue: value,
      type: prop.type,
      units: prop.units || null,
    });
  }

  return cleaned;
}

// Process all elements
let stats = {
  totalElements: rawData.length,
  categoriesNormalized: 0,
  categoriesInferred: 0,
  typeNamesExtracted: 0,
  familyNamesExtracted: 0,
  omniclassFound: 0,
  propertiesBefore: 0,
  propertiesAfter: 0,
};

const categoryDistribution = {};

const cleanData = rawData.map((element) => {
  const keyFields = extractKeyFields(element.properties, element.name);
  const cleanedProps = cleanProperties(element.properties);

  stats.propertiesBefore += (element.properties || []).length;
  stats.propertiesAfter += cleanedProps.length;

  // Track category normalization
  const rawCat = findProperty(element.properties, ['category']);
  if (rawCat && rawCat !== keyFields.componentType) {
    stats.categoriesNormalized++;
  }

  if (!rawCat && keyFields.componentType !== 'Unknown') {
    stats.categoriesInferred++;
  }

  if (keyFields.typeName) stats.typeNamesExtracted++;
  if (keyFields.familyName) stats.familyNamesExtracted++;
  if (keyFields.omniclassTitle) stats.omniclassFound++;

  // Track category distribution
  categoryDistribution[keyFields.componentType] =
    (categoryDistribution[keyFields.componentType] || 0) + 1;

  return {
    dbId: element.dbId,
    name: element.name || null,
    component_type: keyFields.componentType,
    type_name: keyFields.typeName,
    family_name: keyFields.familyName,
    omniclass_title: keyFields.omniclassTitle,
    omniclass_number: keyFields.omniclassNumber,
    properties: cleanedProps,
  };
});

// Write cleaned data
fs.writeFileSync(outputPath, JSON.stringify(cleanData, null, 2), 'utf8');

// Summary
console.log('✨ Preprocessing complete!\n');
console.log('📊 Field Extraction:');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log(`  Elements processed:       ${stats.totalElements}`);
console.log(`  Categories normalized:    ${stats.categoriesNormalized}`);
console.log(`  Categories inferred:      ${stats.categoriesInferred}`);
console.log(`  Type names extracted:     ${stats.typeNamesExtracted}`);
console.log(`  Family names extracted:   ${stats.familyNamesExtracted}`);
console.log(`  OmniClass found:          ${stats.omniclassFound}`);
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

console.log('📊 Properties:');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log(`  Before cleaning:          ${stats.propertiesBefore}`);
console.log(`  After cleaning:           ${stats.propertiesAfter}`);
console.log(`  Removed:                  ${stats.propertiesBefore - stats.propertiesAfter}`);
console.log(`  Reduction:                ${((1 - stats.propertiesAfter / stats.propertiesBefore) * 100).toFixed(1)}%`);
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

console.log('🏗️  Top 15 Categories (by component_type):');
console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
const sortedCats = Object.entries(categoryDistribution)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 15);

sortedCats.forEach(([cat, count]) => {
  console.log(`  ${cat.padEnd(35)} ${count}`);
});

console.log('\n💾 Cleaned data saved to:', outputPath);
console.log('\nNext step: npm run migrate\n');
