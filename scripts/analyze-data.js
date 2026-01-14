// scripts/analyze-data.js
// Analyze data quality issues in bim-data.json

const fs = require('fs');
const path = require('path');

console.log('📊 Analyzing bim-data.json...\n');

const dataPath = path.join(__dirname, '../bim-data.json');
const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

console.log(`Total elements: ${data.length}\n`);

// 1. Check for missing or invalid names
const withName = data.filter(d => d.name && d.name.trim() !== '');
const withoutName = data.filter(d => !d.name || d.name.trim() === '');

console.log('📝 Name field analysis:');
console.log(`  ✓ With valid name: ${withName.length}`);
console.log(`  ✗ Without name or empty: ${withoutName.length}\n`);

// 2. Check for duplicate dbIds
const dbIds = data.map(d => d.dbId);
const uniqueDbIds = new Set(dbIds);
const duplicates = dbIds.length - uniqueDbIds.size;

console.log('🔢 DbId uniqueness:');
console.log(`  Total dbIds: ${dbIds.length}`);
console.log(`  Unique dbIds: ${uniqueDbIds.size}`);
console.log(`  Duplicates: ${duplicates}\n`);

// 3. Check for missing or empty properties
const withoutProps = data.filter(d => !d.properties || !Array.isArray(d.properties) || d.properties.length === 0);

console.log('📋 Properties analysis:');
console.log(`  ✓ With properties: ${data.length - withoutProps.length}`);
console.log(`  ✗ Without properties: ${withoutProps.length}\n`);

// 4. Check for empty property values
let emptyValueCount = 0;
let totalPropCount = 0;

data.forEach(elem => {
  if (elem.properties) {
    elem.properties.forEach(prop => {
      totalPropCount++;
      if (!prop.displayValue || (typeof prop.displayValue === 'string' && prop.displayValue.trim() === '')) {
        emptyValueCount++;
      }
    });
  }
});

console.log('💾 Property values:');
console.log(`  Total properties: ${totalPropCount}`);
console.log(`  Empty/null values: ${emptyValueCount} (${(emptyValueCount / totalPropCount * 100).toFixed(1)}%)\n`);

// 5. Sample empty-name elements
console.log('📌 Sample elements without names:');
withoutName.slice(0, 5).forEach((elem, i) => {
  const category = elem.properties?.find(p => p.displayName === 'Category')?.displayValue || 'Unknown';
  const type = elem.properties?.find(p => p.displayName === 'Type Name')?.displayValue || 'N/A';
  console.log(`  ${i + 1}. dbId=${elem.dbId}, Category="${category}", Type="${type}"`);
});

console.log('\n📊 Data Quality Issues:');
const issues = [];

if (withoutName.length > 0) {
  issues.push(`${withoutName.length} elements without names`);
}
if (duplicates > 0) {
  issues.push(`${duplicates} duplicate dbIds`);
}
if (withoutProps.length > 0) {
  issues.push(`${withoutProps.length} elements without properties`);
}
if (emptyValueCount > totalPropCount * 0.3) {
  issues.push(`High percentage of empty property values (${(emptyValueCount / totalPropCount * 100).toFixed(1)}%)`);
}

if (issues.length === 0) {
  console.log('  ✅ No major data quality issues detected\n');
} else {
  issues.forEach((issue, i) => {
    console.log(`  ${i + 1}. ${issue}`);
  });
  console.log('\n💡 Recommendation: Run preprocessing script to clean data\n');
}
