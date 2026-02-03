// ============================================
// SECTION 1: IMPORTS & DEPENDENCIES
// ============================================

const express = require("express");
const router = express.Router();

const { getDb } = require("../services/sqlite");
const { geminiJson, geminiText } = require("../services/gemini");

// ============================================
// SECTION 2: CONSTANTS
// ============================================

const ALLOWED_PARAMS = new Set([
    "level_number",
    "room_name",
    "room_type",
    "system_name",
    "system_type",
    "manufacturer",
    "model_name",
    "type_name",
    "family_name",
    "omniclass_title",
]);

const SAMPLE_KEYS = [
    "level_number",
    "room_name",
    "room_type",
    "system_name",
    "system_type",
    "manufacturer",
    "model_name",
    "omniclass_title",
    "type_name",
    "family_name",
];

// ============================================
// SECTION 3: STRING UTILITIES
// ============================================

function norm(s) {
    return String(s || "").trim();
}

function stripEmpty(arr) {
    return (arr || []).map(norm).filter(Boolean);
}

// ============================================
// SECTION 4: DATABASE HELPERS
// ============================================

async function getMeta(db, urn) {
    const compRows = db
        .prepare(
            "SELECT DISTINCT component_type FROM elements WHERE urn = ? AND component_type IS NOT NULL AND component_type != ''"
        )
        .all(urn);
    const categories = stripEmpty(compRows.map((r) => r.component_type));
    const categoryField = "component_type";

    const paramSamples = {};
    for (const k of SAMPLE_KEYS) {
        const rows = db
            .prepare(
                `SELECT DISTINCT ${k} FROM elements WHERE urn = ? AND ${k} IS NOT NULL AND ${k} != ''`
            )
            .all(urn);
        paramSamples[k] = stripEmpty(rows.map((r) => r[k]));
    }

    const docs = db
        .prepare("SELECT props_flat FROM elements WHERE urn = ?")
        .all(urn);
    const areaKeySet = new Set();
    const volumeKeySet = new Set();
    for (const d of docs) {
        if (!d.props_flat) continue;
        const pf = JSON.parse(d.props_flat);
        for (const k of Object.keys(pf)) {
            if (/area/i.test(k)) areaKeySet.add(k);
            if (/volume/i.test(k)) volumeKeySet.add(k);
        }
    }

    return {
        categoryField,
        categories,
        paramSamples,
        areaKeys: Array.from(areaKeySet),
        volumeKeys: Array.from(volumeKeySet),
    };
}

// ============================================
// SECTION 5: QUERY TASK HANDLERS
// ============================================

function handleCountTask(db, whereClause, params) {
    console.log(
        "🔍 Count query:",
        `SELECT COUNT(*) as count FROM elements WHERE ${whereClause}`
    );
    const row = db
        .prepare(`SELECT COUNT(*) as count FROM elements WHERE ${whereClause}`)
        .get(...params);
    return { kind: "count", count: row.count };
}

function handleDistinctTask(db, whereClause, params, field) {
    if (!field) throw new Error("distinct requires targetParam");

    const rows = db
        .prepare(
            `SELECT DISTINCT ${field} FROM elements WHERE ${whereClause} AND ${field} IS NOT NULL`
        )
        .all(...params);

    const values = rows.map((r) => norm(r[field])).filter(Boolean);
    return { kind: "distinct", field, values };
}

function handleGroupCountTask(db, whereClause, params, field) {
    if (!field) throw new Error("group_count requires targetParam");

    const rows = db
        .prepare(
            `SELECT ${field}, COUNT(*) as count
             FROM elements
             WHERE ${whereClause} AND ${field} IS NOT NULL
             GROUP BY ${field}
             ORDER BY count DESC`
        )
        .all(...params);

    return { kind: "group_count", field, rows };
}

async function handleSumAreaTask(db, whereClause, params, plan, question) {
    const propsFlatKey = plan.propsFlatKey;
    if (!propsFlatKey) throw new Error("sum_area requires propsFlatKey");

    const rows = db
        .prepare(
            `SELECT json_extract(props_flat, ?) as area_raw, name, type_name, level_number FROM elements WHERE ${whereClause}`
        )
        .all(`$."${propsFlatKey}"`, ...params);
    console.log(`SELECT json_extract(props_flat, ?) as area_raw, name, type_name, level_number FROM elements WHERE ${whereClause}`);
    console.log(`$."${propsFlatKey}"`, ...params);
    console.log("📝 Area rows count:", rows.length);
    console.log("📝 Area rows:", rows);

    const areaAnalysisPrompt = `You are a BIM data analyst. Extract and calculate the total area from the provided data.

User question: ${question || plan?.notes}

Query context:
- Category filter: ${plan.category || "All components"}
- Property key: ${propsFlatKey}
- Filter: ${plan.filterParam ? `${plan.filterParam} = ${plan.filterValue}` : "None"}

Data rows (area_raw, name, type_name, level_number):
${JSON.stringify(rows.slice(0, 200), null, 2)}

Instructions:
1. Parse EACH area_raw value:
   - Numbers: 123.45
   - Strings with units: "123.45 m²", "123.45m2"
   - Comma decimals: "123,45"
2. Sum all valid areas
3. Return JSON:
{
  "total_area": number,
  "count": number,
  "unit": "m²",
  "notes": "brief explanation in Vietnamese"
}`;

    const areaResult = await geminiJson(areaAnalysisPrompt, {
        temperature: 0.1,
    });
    console.log("🧮 Area analysis:", areaResult);

    return {
        kind: "sum_area",
        propsFlatKey,
        total_area: areaResult.total_area || 0,
        n: areaResult.count || 0,
        unit: areaResult.unit || "m²",
        notes: areaResult.notes || "",
    };
}

async function handleSumVolumeTask(db, whereClause, params, plan, question) {
    const propsFlatKey = plan.propsFlatKey || "Dimensions.Volume";

    const rows = db
        .prepare(
            `SELECT json_extract(props_flat, ?) as volume_raw, name, type_name, level_number FROM elements WHERE ${whereClause}`
        )
        .all(`$."${propsFlatKey}"`, ...params);
    console.log(`SELECT json_extract(props_flat, ?) as volume_raw, name, type_name, level_number FROM elements WHERE ${whereClause}`);
    console.log(`$."${propsFlatKey}"`, ...params);
    console.log("📝 Volume rows count:", rows.length);
    console.log("📝 Volume rows:", rows);

    const volumeAnalysisPrompt = `You are a BIM data analyst. Extract and calculate the total volume from the provided data.

User question: ${question || plan?.notes}

Query context:
- Category filter: ${plan.category || "All components"}
- Property key: ${propsFlatKey}
- Filter: ${plan.filterParam ? `${plan.filterParam} = ${plan.filterValue}` : "None"}

Data rows (volume_raw, name, type_name, level_number):
${JSON.stringify(rows.slice(0, 200), null, 2)}

Instructions:
1. Parse EACH volume_raw value:
   - Numbers: 123.45
   - Strings with units: "123.45 m³", "123.45m3"
   - Comma decimals: "123,45"
2. Sum all valid volumes
3. Return JSON:
{
  "total_volume": number,
  "count": number,
  "unit": "m³",
  "notes": "brief explanation in Vietnamese"
}`;

    const volumeResult = await geminiJson(volumeAnalysisPrompt, {
        temperature: 0.1,
    });
    console.log("🧮 Volume analysis:", volumeResult);

    return {
        kind: "sum_volume",
        propsFlatKey,
        total_volume: volumeResult.total_volume || 0,
        n: volumeResult.count || 0,
        unit: volumeResult.unit || "m³",
        notes: volumeResult.notes || "",
    };
}

function handleListTask(db, whereClause, params) {
    const rows = db
        .prepare(
            `SELECT urn, guid, dbId, name, component_type, type_name, family_name,
                    level_number, room_name, room_type,
                    system_type, system_name,
                    manufacturer, model_name,
                    omniclass_title, omniclass_number
             FROM elements
             WHERE ${whereClause}`
        )
        .all(...params);

    const docs = rows.map((r) => ({
        urn: r.urn,
        guid: r.guid,
        dbId: r.dbId,
        name: r.name,
        basic: {
            component_type: r.component_type,
            type_name: r.type_name,
            family_name: r.family_name,
        },
        location: {
            level_number: r.level_number,
            room_name: r.room_name,
            room_type: r.room_type,
        },
        system: {
            system_type: r.system_type,
            system_name: r.system_name,
        },
        equipment: {
            manufacturer: r.manufacturer,
            model_name: r.model_name,
        },
        omniclass: {
            title: r.omniclass_title,
            number: r.omniclass_number,
        },
    }));

    return { kind: "list", docs };
}

// ============================================
// SECTION 6: QUERY ORCHESTRATION
// ============================================

async function runQuery(db, meta, plan, question) {
    const { urn } = plan;
    const category = plan.category ? norm(plan.category) : null;

    // Build WHERE clause
    const whereClauses = ["urn = ?"];
    const params = [urn];

    if (category) {
        whereClauses.push(`${meta.categoryField} = ?`);
        params.push(category);
    }

    if (
        plan.filterParam &&
        plan.filterValue !== undefined &&
        plan.filterValue !== null &&
        String(plan.filterValue).trim() !== ""
    ) {
        whereClauses.push(`${plan.filterParam} = ?`);
        params.push(plan.filterValue);
    }

    const whereClause = whereClauses.join(" AND ");

    // Dispatch to task handler
    switch (plan.task) {
        case "count":
            return handleCountTask(db, whereClause, params);
        case "distinct":
            return handleDistinctTask(db, whereClause, params, plan.targetParam);
        case "group_count":
            return handleGroupCountTask(db, whereClause, params, plan.targetParam);
        case "sum_area":
            return handleSumAreaTask(db, whereClause, params, plan, question);
        case "sum_volume":
            return handleSumVolumeTask(db, whereClause, params, plan, question);
        default:
            return handleListTask(db, whereClause, params);
    }
}

// ============================================
// SECTION 7: UNIFIED QUESTION ANALYSIS (SINGLE LLM CALL)
// ============================================

function buildAnalysisPrompt({ question, categories, paramSamples, areaKeys, volumeKeys }) {
    const numberedCats = categories
        .map((c, i) => `${i + 1}. "${c}"`)
        .join("\n");

    const samplesText = Object.entries(paramSamples)
        .map(
            ([k, vals]) =>
                `- ${k}: [${vals.slice(0, 10).map((v) => JSON.stringify(v)).join(", ")}]`
        )
        .join("\n");

    const areaText = areaKeys.slice(0, 30).map((k) => `- ${k}`).join("\n");
    const volumeText = volumeKeys.slice(0, 30).map((k) => `- ${k}`).join("\n");

    return `Bạn là chuyên gia phân tích BIM. Phân tích câu hỏi và trả về query plan.

## DANH SÁCH CATEGORY (chọn CHÍNH XÁC từ danh sách, hoặc null):
${numberedCats}

## TASK TYPES:
- "count": đếm số lượng (bao nhiêu, có mấy, số lượng)
- "distinct": liệt kê giá trị duy nhất (liệt kê các loại, những loại nào)
- "group_count": đếm theo nhóm (theo tầng, theo phòng, phân theo)
- "sum_area": tính tổng diện tích (diện tích, tổng diện tích)
- "sum_volume": tính tổng thể tích (thể tích, tổng thể tích, khối lượng, dung tích, volume)
- "list": liệt kê chi tiết (liệt kê, cho xem, danh sách)

## VIETNAMESE → CATEGORY MAPPING:
- "cửa" (không phải "cửa sổ") → Doors
- "cửa sổ" → Windows
- "tường", "vách" → Walls
- "sàn", "diện tích nhà/tòa nhà" → Floors
- "mái" → Roofs
- "cột", "trụ" → Columns
- "dầm" → Beams
- "cầu thang" → Stairs
- "lan can" → Railings

## AVAILABLE FILTER PARAMETERS & VALUES:
${samplesText}

## AREA KEYS (for sum_area):
${areaText}

## VOLUME KEYS (for sum_volume):
${volumeText}

## CÂU HỎI: "${question}"

## RULES:
1. intent: "bim" nếu hỏi về dữ liệu BIM, "general" nếu hỏi kiến thức chung
2. category: PHẢI là tên CHÍNH XÁC từ danh sách trên, hoặc null
3. filterParam/filterValue: chỉ set nếu câu hỏi đề cập cụ thể (vd: "tầng 1", "phòng khách")
4. targetParam: cho "distinct" → "type_name"; cho "group_count" → field để group
5. propsFlatKey: cho "sum_area" → ưu tiên "Dimensions.Area" nếu có; cho "sum_volume" → ưu tiên "Dimensions.Volume" nếu có

Trả về JSON:
{
  "intent": "bim" | "general",
  "task": "count" | "distinct" | "group_count" | "sum_area" | "sum_volume" | "list",
  "category": "EXACT_NAME_OR_NULL",
  "filterParam": "PARAM_OR_NULL",
  "filterValue": "VALUE_OR_NULL",
  "targetParam": "PARAM_OR_NULL",
  "propsFlatKey": "KEY_OR_NULL",
  "notes": "brief reason"
}`.trim();
}

async function analyzeQuestion(question, meta) {
    const prompt = buildAnalysisPrompt({
        question,
        categories: meta.categories,
        paramSamples: meta.paramSamples,
        areaKeys: meta.areaKeys,
        volumeKeys: meta.volumeKeys,
    });

    console.log("📝 Analysis prompt:", prompt);
    console.log("📝 Analysis prompt length:", prompt.length);

    const result = await geminiJson(prompt, { temperature: 0.1 });
    console.log("📝 Analysis result:", result);

    return result;
}

// ============================================
// SECTION 8: ANSWER PROMPT
// ============================================

function answerPrompt({ question, meta, plan, result }) {
    return `Trả lời câu hỏi bằng tiếng Việt dựa trên kết quả query.

- Trả lời ngắn gọn, chính xác
- Nếu không có kết quả, giải thích và gợi ý 2-3 câu hỏi thay thế
- Nếu task là count: nêu số lượng và loại
- Nếu task là distinct: liệt kê các giá trị
- Nếu task là group_count: hiển thị các nhóm với số lượng
- Nếu task là sum_area: cung cấp tổng diện tích với đơn vị và ghi chú
- Nếu task là sum_volume: cung cấp tổng thể tích với đơn vị và ghi chú

Available categories: ${meta.categories.slice(0, 15).join(", ")}

Plan:
${JSON.stringify(plan, null, 2)}

Result:
${JSON.stringify(result, null, 2)}

Câu hỏi: "${question}"`.trim();
}

function generalPrompt(question) {
    return `Bạn là trợ lý kỹ thuật BIM/APS. Trả lời ngắn gọn, chính xác, bằng tiếng Việt.

Câu hỏi: "${question}"`.trim();
}

// ============================================
// SECTION 9: PLAN HELPERS
// ============================================

function validateAndBuildPlan(analysis, urn, availableCategories) {
    // Validate category
    let category = null;
    if (analysis.category) {
        const normalizedCategory = norm(analysis.category);
        const exactMatch = availableCategories.find((c) => c === normalizedCategory);
        if (exactMatch) {
            category = exactMatch;
        } else {
            const caseMatch = availableCategories.find(
                (c) => c.toLowerCase() === normalizedCategory.toLowerCase()
            );
            if (caseMatch) {
                category = caseMatch;
                console.log(`⚠ Category case fix: "${analysis.category}" → "${category}"`);
            } else {
                console.log(`⚠ Invalid category: "${analysis.category}" - ignoring`);
            }
        }
    }

    // Validate params
    let filterParam = analysis.filterParam || null;
    let targetParam = analysis.targetParam || null;

    if (filterParam && !ALLOWED_PARAMS.has(filterParam)) {
        console.log(`⚠ Invalid filterParam: "${filterParam}" - ignoring`);
        filterParam = null;
    }
    if (targetParam && !ALLOWED_PARAMS.has(targetParam)) {
        console.log(`⚠ Invalid targetParam: "${targetParam}" - ignoring`);
        targetParam = null;
    }

    return {
        urn,
        intent: analysis.intent || "bim",
        task: analysis.task || "count",
        category,
        filterParam,
        filterValue: analysis.filterValue ?? null,
        targetParam,
        propsFlatKey: analysis.propsFlatKey || null,
        notes: analysis.notes || "",
    };
}

// ============================================
// SECTION 10: ROUTE HANDLER
// ============================================

router.post("/", async (req, res, next) => {
    try {
        const { urn, question, debug } = req.body || {};

        if (!urn) return res.status(400).json({ error: "Missing urn" });
        if (!question) return res.status(400).json({ error: "Missing question" });

        const db = getDb();
        const meta = await getMeta(db, urn);
        console.log("📊 Meta:", {
            categoryField: meta.categoryField,
            categoriesCount: meta.categories.length,
            categories: meta.categories.slice(0, 10),
        });

        // Step 1: Analyze question (SINGLE LLM CALL for intent + category + params)
        const analysis = await analyzeQuestion(question, meta);

        // Handle general questions
        if (analysis.intent === "general") {
            const answer = await geminiText(generalPrompt(question), {
                temperature: 0.2,
            });
            return res.json({
                answer,
                ...(debug ? { debug: { meta, analysis } } : {}),
            });
        }

        // Step 2: Build and validate plan
        const plan = validateAndBuildPlan(analysis, urn, meta.categories);
        console.log("🔍 Final plan:", plan);

        // Step 3: Execute query
        const result = await runQuery(db, meta, plan, question);
        console.log("✅ Query result:", result);

        console.log("📝 Answer prompt:", answerPrompt({ question, meta, plan, result }));
        // Step 4: Generate answer
        const answer = await geminiText(
            answerPrompt({ question, meta, plan, result }),
            { temperature: 0.2 }
        );

        return res.json({
            answer,
            hits:
                result.kind === "list"
                    ? { count: result.docs.length, docs: result.docs }
                    : result,
            ...(debug ? { debug: { meta, plan, result } } : {}),
        });
    } catch (err) {
        next(err);
    }
});

module.exports = router;
