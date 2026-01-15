// ============================================
// SECTION 1: IMPORTS & DEPENDENCIES
// ============================================

const express = require("express");
const router = express.Router();

const { getDb, semanticSearch } = require("../services/sqlite");
const { geminiJson, geminiText } = require("../services/gemini");
const { generateEmbedding } = require("../services/embeddings");

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

const DEFAULT_LIMIT = 20;
const DEFAULT_TOP_K = 100;

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
                `SELECT DISTINCT ${k} FROM elements WHERE urn = ? AND ${k} IS NOT NULL AND ${k} != '' LIMIT 15`
            )
            .all(urn);
        paramSamples[k] = stripEmpty(rows.map((r) => r[k]));
    }

    const docs = db
        .prepare("SELECT props_flat FROM elements WHERE urn = ? LIMIT 50")
        .all(urn);
    const areaKeySet = new Set();
    for (const d of docs) {
        if (!d.props_flat) continue;
        const pf = JSON.parse(d.props_flat);
        for (const k of Object.keys(pf)) {
            if (/area/i.test(k)) areaKeySet.add(k);
        }
    }

    return {
        categoryField,
        categories,
        paramSamples,
        areaKeys: Array.from(areaKeySet).slice(0, 200),
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

function handleDistinctTask(db, whereClause, params, field, limit) {
    if (!field) throw new Error("distinct requires targetParam");

    const rows = db
        .prepare(
            `SELECT DISTINCT ${field} FROM elements WHERE ${whereClause} AND ${field} IS NOT NULL LIMIT ?`
        )
        .all(...params, limit);

    const values = rows.map((r) => norm(r[field])).filter(Boolean);
    return { kind: "distinct", field, values };
}

function handleGroupCountTask(db, whereClause, params, field, limit) {
    if (!field) throw new Error("group_count requires targetParam");

    const rows = db
        .prepare(
            `SELECT ${field}, COUNT(*) as count
             FROM elements
             WHERE ${whereClause} AND ${field} IS NOT NULL
             GROUP BY ${field}
             ORDER BY count DESC
             LIMIT ?`
        )
        .all(...params, limit);

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
    console.log("📝 Rows:", rows);

    const areaAnalysisPrompt = `You are a BIM data analyst. Extract and calculate the total area from the provided data based on the user's question and query context.

User question: ${question || plan?.notes}

Query context (what data should be included):
- Category filter: ${plan.category || "All components"}
- Property key being analyzed: ${propsFlatKey}
- Additional filter: ${
        plan.filterParam ? `${plan.filterParam} = ${plan.filterValue}` : "None"
    }

Available data rows (already pre-filtered by SQL, showing area_raw, name, type_name, level_number):
${JSON.stringify(rows.slice(0, 100), null, 2)}

Instructions:
1. Review the user's question
2. The rows provided are ALREADY filtered by SQL based on the user's question
3. Parse EACH area_raw value from the provided rows:
   - Handle numbers: 123.45
   - Handle strings with units: "123.45 m²", "123.45m2"
   - Handle comma decimals: "123,45"
   - Extract only the numeric part and convert to float
4. Pick suitable area_raw value from the provided rows based on the user's question
5. Calcuate and return JSON with:
   {
     "total_area": number (sum of all valid areas),
     "count": number (count of rows with valid area values),
     "unit": "m²" or detected unit from the data,
     "notes": "brief explanation in Vietnamese - what was calculated, which components/floors/filters were included"
   }

CRITICAL:
- Include ALL rows provided (they are already filtered by the SQL query)
- In notes, explain what was calculated (e.g., "Tổng diện tích căn nhà" or "Diện tích sàn tầng 2")`;

    const areaResult = await geminiJson(areaAnalysisPrompt, {
        temperature: 0.1,
    });
    console.log("🧮 Gemini area analysis:", areaResult);

    return {
        kind: "sum_area",
        propsFlatKey,
        total_area: areaResult.total_area || 0,
        n: areaResult.count || 0,
        unit: areaResult.unit || "m²",
        notes: areaResult.notes || "",
    };
}

function handleListTask(db, whereClause, params, limit) {
    const rows = db
        .prepare(
            `SELECT urn, guid, dbId, name, component_type, type_name, family_name,
                    level_number, room_name, room_type,
                    system_type, system_name,
                    manufacturer, model_name,
                    omniclass_title, omniclass_number
             FROM elements
             WHERE ${whereClause}
             LIMIT ?`
        )
        .all(...params, limit);

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
    const limit = Number.isFinite(plan.limit) ? plan.limit : DEFAULT_LIMIT;

    // Semantic search for candidate IDs
    let candidateIds = null;
    if (plan.useSemanticSearch && plan.semanticQuery) {
        try {
            const queryEmbed = await generateEmbedding(plan.semanticQuery);
            if (queryEmbed) {
                candidateIds = semanticSearch(
                    db,
                    urn,
                    queryEmbed,
                    plan.topK || DEFAULT_TOP_K
                );
                console.log(
                    `🔍 Semantic search found ${candidateIds.length} candidates`
                );
            }
        } catch (error) {
            console.error("⚠ Semantic search failed:", error.message);
        }
    }

    // Build WHERE clause
    const whereClauses = ["urn = ?"];
    const params = [urn];

    if (candidateIds && candidateIds.length > 0) {
        whereClauses.push(`id IN (${candidateIds.join(",")})`);
    }

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
            return handleDistinctTask(
                db,
                whereClause,
                params,
                plan.targetParam,
                limit
            );
        case "group_count":
            return handleGroupCountTask(
                db,
                whereClause,
                params,
                plan.targetParam,
                limit
            );
        case "sum_area":
            return handleSumAreaTask(db, whereClause, params, plan, question);
        default:
            return handleListTask(db, whereClause, params, limit);
    }
}

// ============================================
// SECTION 7: LLM INTEGRATION
// ============================================

async function detectHintCategory(question, availableCategories) {
    if (!question || !availableCategories || availableCategories.length === 0) {
        return null;
    }

    console.log("💡 Available categories:", availableCategories);

    try {
        const prompt = `Bạn là chuyên gia BIM. Dựa vào câu hỏi của người dùng, hãy xác định loại thành phần BIM (component_type) phù hợp nhất.

Câu hỏi: "${question}"

Các loại thành phần có sẵn (chọn 1 hoặc null):
${availableCategories.map((c) => `- ${c}`).join("\n")}

Trả về JSON với format:
{
  "category": "tên chính xác từ danh sách trên hoặc null",
  "confidence": "high|medium|low",
  "reason": "lý do ngắn gọn"
}

Lưu ý:
- "cửa" (trừ "cửa sổ") → Doors
- "cửa sổ" → Windows
- "tường" → Walls
- "sàn" → Floors
- "cột" → Columns
- "dầm" → Beams
- "ống" → Pipes hoặc Ducts
- IMPORTANT: For area queries ("diện tích tòa nhà", "tổng diện tích", "diện tích sàn", etc.) → Floors (if available)
- Chỉ trả về category nếu confidence >= medium
- Trả về null nếu không chắc chắn`;
        console.log("💡 Prompt:", prompt);

        const result = await geminiJson(prompt, { temperature: 0.1 });

        if (result.category && result.confidence !== "low") {
            console.log(
                `💡 LLM hint: ${result.category} (${result.confidence}) - ${result.reason}`
            );
            return result.category;
        }
    } catch (error) {
        console.error("⚠ LLM hint detection failed:", error.message);
    }

    return null;
}

// ============================================
// SECTION 8: PROMPT BUILDERS
// ============================================

function intentPrompt({ question, categories, hintCategory }) {
    const cats = categories
        .slice(0, 120)
        .map((c) => `- ${c}`)
        .join("\n");

    return `
You are the PLANNER in a 3-step pipeline: Planner -> Query -> Answer.
User question is in Vietnamese and is about a BIM model.

Step 1 (INTENT): decide if the user wants BIM data from database, or a general explanation.
Return JSON with:
- intent: "bim" | "general"
- task: "count" | "distinct" | "group_count" | "sum_area" | "list"
- category: one of the provided categories OR null if not needed
- limit: integer (default 20)
- notes: short string

Hints:
- "cửa" usually means Doors; "cửa sổ" means Windows.
- If user asks "bao nhiêu" => count.
- If user asks "liệt kê các loại" => distinct (list unique types).
- If user asks "theo tầng" => likely group_count by location.level_number.
- If user asks "diện tích" => sum_area.
- IMPORTANT: If asking about general area or building area ("diện tích tòa nhà", "tổng diện tích", etc.) => category should be "Floors" (if available).

If you choose category, choose ONLY from the list.
If you are unsure, choose null.

Provided categories:
${cats}

Heuristic hintCategory (optional): ${hintCategory || "null"}

User question: ${JSON.stringify(question)}
`.trim();
}

function parameterPrompt({ question, plan1, paramSamples, areaKeys }) {
    const samplesText = Object.entries(paramSamples)
        .map(
            ([k, vals]) =>
                `- ${k}: [${vals
                    .slice(0, 8)
                    .map((v) => JSON.stringify(v))
                    .join(", ")}]`
        )
        .join("\n");

    const areaText = areaKeys
        .slice(0, 40)
        .map((k) => `- ${k}`)
        .join("\n");

    return `
You are the PLANNER (Step 2: PARAMETERS) for BIM database query.

You already decided:
${JSON.stringify(plan1, null, 2)}

Now choose detailed query parameters and return JSON with:
- useSemanticSearch: boolean (true if question describes concepts/characteristics rather than exact categories)
- semanticQuery: string (in English word, only if useSemanticSearch=true)
- topK: integer (number of semantic candidates, default 100, only if useSemanticSearch=true)
- filterParam: null OR one of:
  "level_number", "room_name", "room_type",
  "system_name", "system_type",
  "manufacturer", "model_name",
  "type_name", "family_name", "omniclass_title"
- filterValue: null OR string (only if the question explicitly contains a value like a level name)
- targetParam: for task "distinct" or "group_count": one of the same param list above
- propsFlatKey: for task "sum_area": choose 1 key from areaKeys OR null if none fits
- limit: integer

Semantic Search Guidelines:
- Set useSemanticSearch=true when:
  * Question asks for "structural components" (kết cấu), "electrical equipment" (thiết bị điện), etc.
  * Question describes characteristics: "transparent" (trong suốt), "load-bearing" (chịu lực)
  * Question uses general terms that might map to multiple categories
- Set useSemanticSearch=false when:
  * Question explicitly names a category like "Doors", "Windows", "Walls"
  * Simple count/list queries with exact category match

Rules:
- For task "count": usually no targetParam; filterParam only if question says "ở tầng ..." or "phòng ...".
- For task "distinct": choose targetParam = "type_name" (preferred) or "family_name".
- For task "group_count": choose targetParam based on grouping requested (level/room/system...).
- For task "sum_area": ALWAYS prioritize "Dimensions.Area" if available in areaKeys. Only choose other keys if "Dimensions.Area" is not present.

paramSamples:
${samplesText}

areaKeys:
${areaText}

User question: ${JSON.stringify(question)}
`.trim();
}

function answerPrompt({ question, meta, plan, result }) {
    return `
You are the ANSWER agent (Step 3: ANSWER). Use the query result to answer in Vietnamese.

- Answer should be short, correct, and directly address the question.
- If result is empty or category not found, explain what is missing and suggest 2-3 alternative queries user can try.
- If task is count: state the count and the category.
- If task is distinct: list values (up to 10) and mention if more exist.
- If task is group_count: show top groups with counts.
- If task is sum_area: provide total area with unit (result.unit or default m²), count of elements (result.n), and any additional notes (result.notes).

Context:
categoryField used in DB: ${meta.categoryField}
Available categories example: ${meta.categories.slice(0, 15).join(", ")}

Plan:
${JSON.stringify(plan, null, 2)}

Result:
${JSON.stringify(result, null, 2)}

User question: ${JSON.stringify(question)}
`.trim();
}

function generalPrompt(question) {
    return `
Bạn là trợ lý kỹ thuật BIM/APS. Hãy trả lời câu hỏi sau ngắn gọn, chính xác, bằng tiếng Việt.
Nếu cần, đưa ví dụ lệnh curl/PowerShell hoặc hướng dẫn kiểm tra nhanh.

Câu hỏi: ${JSON.stringify(question)}
`.trim();
}

// ============================================
// SECTION 9: PLAN HELPERS
// ============================================

function buildQueryPlan(plan1, plan2, urn, category) {
    return {
        urn,
        intent: "bim",
        task: plan1.task || "count",
        category,
        limit: plan2.limit || plan1.limit || DEFAULT_LIMIT,
        filterParam: plan2.filterParam || null,
        filterValue: plan2.filterValue ?? null,
        targetParam: plan2.targetParam || null,
        propsFlatKey: plan2.propsFlatKey || null,
        useSemanticSearch: plan2.useSemanticSearch || false,
        semanticQuery: plan2.semanticQuery || null,
        topK: plan2.topK || DEFAULT_TOP_K,
        notes: plan1.notes || "",
    };
}

function validatePlanParams(plan) {
    if (plan.filterParam && !ALLOWED_PARAMS.has(plan.filterParam)) {
        plan.filterParam = null;
    }
    if (plan.targetParam && !ALLOWED_PARAMS.has(plan.targetParam)) {
        plan.targetParam = null;
    }
    return plan;
}

// ============================================
// SECTION 10: ROUTE HANDLER
// ============================================

router.post("/", async (req, res, next) => {
    try {
        const { urn, question, debug } = req.body || {};

        if (!urn) return res.status(400).json({ error: "Missing urn" });
        if (!question)
            return res.status(400).json({ error: "Missing question" });

        const db = getDb();
        const meta = await getMeta(db, urn);
        console.log("📊 Meta:", {
            categoryField: meta.categoryField,
            categories: meta.categories.slice(0, 10),
        });

        // Step 1: Detect hint category
        const hintCategory = await detectHintCategory(
            question,
            meta.categories
        );
        console.log("💡 Hint category:", hintCategory);

        // Step 2: Get intent from LLM
        const plan1Prompt = intentPrompt({
            question,
            categories: meta.categories,
            hintCategory,
        });
        const plan1 = await geminiJson(plan1Prompt);
        console.log("📝 Plan1Prompt:", plan1Prompt);
        console.log("📝 Plan1:", plan1);

        // Handle general questions
        if (plan1.intent === "general") {
            const answer = await geminiText(generalPrompt(question), {
                temperature: 0.2,
            });
            return res.json({
                answer,
                ...(debug ? { debug: { meta, plan1 } } : {}),
            });
        }

        // Fix category if LLM missed but we have a strong hint
        let category = plan1.category ? norm(plan1.category) : null;
        if (
            !category &&
            hintCategory &&
            meta.categories.includes(hintCategory)
        ) {
            category = hintCategory;
        }
        console.log("🎯 Final category:", category);

        // Step 3: Get parameters from LLM
        const plan2Prompt = parameterPrompt({
            question,
            plan1: { ...plan1, category },
            paramSamples: meta.paramSamples,
            areaKeys: meta.areaKeys,
        });
        const plan2 = await geminiJson(plan2Prompt);
        console.log("📝 Plan2Prompt:", plan2Prompt);
        console.log("📝 Plan2:", plan2);

        // Step 4: Build and validate plan
        let plan = buildQueryPlan(plan1, plan2, urn, category);
        plan = validatePlanParams(plan);
        console.log("🔍 Final plan:", plan);

        // Step 5: Execute query
        const result = await runQuery(db, meta, plan, question);
        console.log("✅ Query result:", result);

        // Step 6: Generate answer
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
