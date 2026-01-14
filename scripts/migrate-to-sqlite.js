// scripts/migrate-to-sqlite.js
// One-time migration script to load bim-data.json into SQLite with embeddings

const fs = require("fs");
const path = require("path");
const { getDb, initSchema } = require("../services/sqlite");
const {
    generateEmbeddings,
    generateEmbeddingText,
} = require("../services/embeddings");
const mapping = require("../services/dmMapping");

// Helper functions from extract.js
function normalizeKey(s) {
    return String(s || "")
        .toLowerCase()
        .replace(/[_\-]+/g, " ")
        .replace(/[^\p{L}\p{N}]+/gu, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function isEmpty(v) {
    return (
        v === null ||
        v === undefined ||
        (typeof v === "string" && v.trim() === "")
    );
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
    const s = `${typeName || ""} ${familyName || ""}`.toLowerCase();
    if (/(door|cửa)/i.test(s)) return "Doors";
    if (/(window|cửa sổ)/i.test(s)) return "Windows";
    if (/wall|tường/i.test(s)) return "Walls";
    if (/floor|sàn/i.test(s)) return "Floors";
    if (/room|phòng/i.test(s)) return "Rooms";
    return null;
}

// Transform APS properties array to flat object
function flattenProps(properties) {
    const flat = {};
    if (!Array.isArray(properties)) return flat;

    for (const prop of properties) {
        if (!prop.displayCategory || !prop.displayName) continue;
        const key = `${prop.displayCategory}.${prop.displayName}`;
        flat[key] = prop.displayValue;
    }

    return flat;
}

// Extract structured fields from flattened properties
function extractGroups(flat, dbId) {
    const type_name = findInFlat(flat, mapping.basic.type_name);
    const family_name = findInFlat(flat, mapping.basic.family_name);

    const omniclassTitle = findInFlat(flat, mapping.omniclass.title);
    const omniclassNumber = findInFlat(flat, mapping.omniclass.number);

    const component_type_raw =
        findInFlat(flat, mapping.basic.component_type) ||
        inferCategory(type_name, family_name) ||
        "Unknown";

    // Prefer OmniClass Title as CATEGORY when available
    const component_type = !isEmpty(omniclassTitle)
        ? String(omniclassTitle).trim()
        : String(component_type_raw).trim();

    return {
        component_id: dbId,
        component_type,
        type_name: type_name ? String(type_name) : null,
        family_name: family_name ? String(family_name) : null,
        is_asset: findInFlat(flat, mapping.basic.is_asset),

        level_number: findInFlat(flat, mapping.location.level_number),
        room_type: findInFlat(flat, mapping.location.room_type),
        room_name: findInFlat(flat, mapping.location.room_name),

        system_type: findInFlat(flat, mapping.system.system_type),
        system_name: findInFlat(flat, mapping.system.system_name),

        manufacturer: findInFlat(flat, mapping.equipment.manufacturer),
        model_name: findInFlat(flat, mapping.equipment.model_name),
        specification: findInFlat(flat, mapping.equipment.specification),

        omniclass_title: omniclassTitle ? String(omniclassTitle) : null,
        omniclass_number: omniclassNumber ? String(omniclassNumber) : null,
    };
}

// Transform preprocessed element to database row
function transformElement(rawElement, urn, guid) {
    const dbId = rawElement.dbId;
    const name = rawElement.name || null;

    // If data is already preprocessed (has component_type field), use it directly
    if (rawElement.component_type) {
        // Flatten remaining properties
        const flat = flattenProps(rawElement.properties || []);

        // Extract additional fields not in top-level
        const level_number =
            rawElement.level_number ||
            findInFlat(flat, mapping.location.level_number);
        const room_type =
            rawElement.room_type ||
            findInFlat(flat, mapping.location.room_type);
        const room_name =
            rawElement.room_name ||
            findInFlat(flat, mapping.location.room_name);
        const system_type =
            rawElement.system_type ||
            findInFlat(flat, mapping.system.system_type);
        const system_name =
            rawElement.system_name ||
            findInFlat(flat, mapping.system.system_name);
        const manufacturer =
            rawElement.manufacturer ||
            findInFlat(flat, mapping.equipment.manufacturer);
        const model_name =
            rawElement.model_name ||
            findInFlat(flat, mapping.equipment.model_name);
        const specification =
            rawElement.specification ||
            findInFlat(flat, mapping.equipment.specification);
        const is_asset =
            rawElement.is_asset || findInFlat(flat, mapping.basic.is_asset);

        return {
            urn,
            guid,
            dbId,
            name,
            component_id: dbId,
            component_type: rawElement.component_type,
            type_name: rawElement.type_name || null,
            family_name: rawElement.family_name || null,
            is_asset,
            level_number,
            room_type,
            room_name,
            system_type,
            system_name,
            manufacturer,
            model_name,
            specification,
            omniclass_title: rawElement.omniclass_title || null,
            omniclass_number: rawElement.omniclass_number || null,
            props_flat: JSON.stringify(flat),
        };
    }

    // Fallback: raw APS format (old behavior)
    const flat = flattenProps(rawElement.properties);
    const groups = extractGroups(flat, dbId);

    return {
        urn,
        guid,
        dbId,
        name,
        ...groups,
        props_flat: JSON.stringify(flat),
    };
}

async function migrate() {
    console.log("🚀 Starting migration from cleaned BIM data to SQLite...\n");

    // 1. Initialize database
    const db = getDb();
    initSchema();

    // 2. Load cleaned data (use bim-data.clean.json if available, fallback to bim-data.json)
    const cleanPath = path.join(__dirname, "../bim-data.clean.json");
    const rawPath = path.join(__dirname, "../bim-data.json");

    let dataPath = cleanPath;
    if (!fs.existsSync(cleanPath)) {
        console.log("⚠️  bim-data.clean.json not found, using bim-data.json");
        console.log(
            "   Run 'node scripts/preprocess-data.js' first for better results\n"
        );
        dataPath = rawPath;
    }

    console.log(`📂 Reading ${dataPath}...`);

    const rawData = JSON.parse(fs.readFileSync(dataPath, "utf8"));
    console.log(`✓ Loaded ${rawData.length} elements from JSON\n`);

    // 3. Transform elements
    console.log("🔄 Transforming elements...");

    // IMPORTANT: Use YOUR model's URN here (copy from the viewer URL hash or model dropdown)
    const urn =
        process.env.DEFAULT_URN ||
        "dXJuOmFkc2sud2lwcHJvZDpmcy5maWxlOnZmLndVNTdKUGJ5U3VLek95blRDN2tBP3ZlcnNpb249MQ";
    const guid = process.env.DEFAULT_GUID || "default-guid";

    console.log(`📌 Using URN: ${urn}\n`);

    const rows = rawData.map((elem) => transformElement(elem, urn, guid));
    console.log(`✓ Transformed ${rows.length} elements\n`);

    // 4. Generate embeddings
    console.log("🤖 Generating embeddings with OpenAI...");
    console.log("   This may take 30-60 seconds for 2,738 elements...\n");

    const embeddingTexts = rows.map((row) => generateEmbeddingText(row));

    let embeddings;
    try {
        embeddings = await generateEmbeddings(embeddingTexts);
        console.log("");
    } catch (error) {
        console.error("❌ Failed to generate embeddings:", error.message);
        console.log(
            "⚠  Continuing without embeddings. You can add them later.\n"
        );
        embeddings = Array(rows.length).fill(null);
    }

    // 5. Add embeddings to rows
    rows.forEach((row, i) => {
        row.embedding = embeddings[i] ? JSON.stringify(embeddings[i]) : null;
        row.embedding_model = embeddings[i] ? "text-embedding-3-small" : null;
    });

    // 6. Insert into SQLite
    console.log("💾 Inserting into SQLite database...");

    // Clear existing data for this URN
    db.prepare("DELETE FROM elements WHERE urn = ?").run(urn);

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

    // Use transaction for performance
    const insertMany = db.transaction((rows) => {
        for (const row of rows) {
            insertStmt.run(
                row.urn,
                row.guid,
                row.dbId,
                row.name,
                row.component_id,
                row.component_type,
                row.type_name,
                row.family_name,
                row.is_asset,
                row.level_number,
                row.room_type,
                row.room_name,
                row.system_type,
                row.system_name,
                row.manufacturer,
                row.model_name,
                row.specification,
                row.omniclass_title,
                row.omniclass_number,
                row.props_flat,
                row.embedding,
                row.embedding_model
            );
        }
    });

    insertMany(rows);

    // 7. Verify
    const count = db
        .prepare("SELECT COUNT(*) as count FROM elements")
        .get().count;
    const withEmbeddings = db
        .prepare(
            "SELECT COUNT(*) as count FROM elements WHERE embedding IS NOT NULL"
        )
        .get().count;

    console.log(`✓ Inserted ${count} elements`);
    console.log(`✓ ${withEmbeddings} elements have embeddings\n`);

    // 8. Summary
    console.log("📊 Migration Summary:");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log(`  Total elements:     ${count}`);
    console.log(`  With embeddings:    ${withEmbeddings}`);
    console.log(`  URN:                ${urn}`);
    console.log(`  GUID:               ${guid}`);
    console.log(`  Database location:  data/bim.db`);
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Show sample categories
    const categories = db
        .prepare(
            `
    SELECT component_type, COUNT(*) as count
    FROM elements
    GROUP BY component_type
    ORDER BY count DESC
    LIMIT 10
  `
        )
        .all();

    console.log("🏗️  Top Categories:");
    categories.forEach((cat) => {
        console.log(`  ${cat.component_type.padEnd(30)} ${cat.count}`);
    });

    console.log("\n✅ Migration complete!");
    console.log("   You can now start the server with: npm start\n");
}

// Run migration
migrate().catch((error) => {
    console.error("\n❌ Migration failed:", error);
    process.exit(1);
});
