// routes/chat.js
const express = require("express");
const router = express.Router();

const { getDb, semanticSearch } = require("../services/sqlite");
const { geminiJson, geminiText } = require("../services/gemini");
const { generateEmbedding } = require("../services/embeddings");

function norm(s) {
    return String(s || "").trim();
}

function stripEmpty(arr) {
    return (arr || []).map(norm).filter(Boolean);
}

// LLM-based category detection (replaced keyword matching)
async function detectHintCategory(question, availableCategories) {
    if (!question || !availableCategories || availableCategories.length === 0) {
        return null;
    }

    console.log("💡 Available categories:", availableCategories);
    // Quick keyword fallback for common cases (for speed)
    const q = question.toLowerCase();

    // Use LLM for intelligent mapping
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

// ----- SQLite helpers -----

async function getMeta(db, urn) {
    // Get distinct component types (primary category field after preprocessing)
    const compRows = db
        .prepare(
            "SELECT DISTINCT component_type FROM elements WHERE urn = ? AND component_type IS NOT NULL AND component_type != ''"
        )
        .all(urn);
    const categories = stripEmpty(compRows.map((r) => r.component_type));

    // Always use component_type as category field (normalized in preprocessing)
    const categoryField = "component_type";

    // param samples (small) for planner
    const sampleKeys = [
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

    const paramSamples = {};
    for (const k of sampleKeys) {
        const rows = db
            .prepare(
                `SELECT DISTINCT ${k} FROM elements WHERE urn = ? AND ${k} IS NOT NULL AND ${k} != '' LIMIT 15`
            )
            .all(urn);
        paramSamples[k] = stripEmpty(rows.map((r) => r[k]));
    }

    // area keys from props_flat (for "diện tích" questions)
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

function buildCountQuery({
    urn,
    categoryField,
    category,
    filterParam,
    filterValue,
}) {
    const q = { urn };
    if (category) q[categoryField] = category;
    if (
        filterParam &&
        filterValue !== undefined &&
        filterValue !== null &&
        String(filterValue).trim() !== ""
    ) {
        q[filterParam] = filterValue;
    }
    return q;
}

async function runQuery(db, meta, plan) {
    const { urn } = plan;

    // Normalize plan fields
    const category = plan.category ? norm(plan.category) : null;
    const limit = Number.isFinite(plan.limit) ? plan.limit : 20;

    // NEW: Semantic search
    let candidateIds = null;
    if (plan.useSemanticSearch && plan.semanticQuery) {
        try {
            const queryEmbed = await generateEmbedding(plan.semanticQuery);
            if (queryEmbed) {
                candidateIds = semanticSearch(
                    db,
                    urn,
                    queryEmbed,
                    plan.topK || 100
                );
                console.log(
                    `🔍 Semantic search found ${candidateIds.length} candidates`
                );
            }
        } catch (error) {
            console.error("⚠ Semantic search failed:", error.message);
            // Continue with regular query
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

    if (plan.task === "count") {
        console.log(
            "🔍 Count query:",
            `SELECT COUNT(*) as count FROM elements WHERE ${whereClause}`
        );
        const row = db
            .prepare(
                `SELECT COUNT(*) as count FROM elements WHERE ${whereClause}`
            )
            .get(...params);
        return { kind: "count", count: row.count };
    }

    if (plan.task === "distinct") {
        const field = plan.targetParam;
        if (!field) throw new Error("distinct requires targetParam");

        const rows = db
            .prepare(
                `SELECT DISTINCT ${field} FROM elements WHERE ${whereClause} AND ${field} IS NOT NULL LIMIT ?`
            )
            .all(...params, limit);

        const values = rows.map((r) => norm(r[field])).filter(Boolean);
        return { kind: "distinct", field, values };
    }

    if (plan.task === "group_count") {
        const field = plan.targetParam;
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

    if (plan.task === "sum_area") {
        // sum numeric area from props_flat dynamic key
        const propsFlatKey = plan.propsFlatKey;
        if (!propsFlatKey) throw new Error("sum_area requires propsFlatKey");

        const rows = db
            .prepare(
                `SELECT json_extract(props_flat, ?) as area_raw FROM elements WHERE ${whereClause}`
            )
            .all(`$.${propsFlatKey}`, ...params);
        console.log("📝 Rows:", rows);

        let total_area = 0;
        let n = 0;

        for (const row of rows) {
            if (!row.area_raw) continue;

            let areaNum = 0;
            if (typeof row.area_raw === "number") {
                areaNum = row.area_raw;
            } else if (typeof row.area_raw === "string") {
                // Extract number from string (handle formats like "123.45 m²" or "123,45")
                const match = String(row.area_raw).match(
                    /[-+]?\d+(?:[.,]\d+)?/
                );
                if (match) {
                    areaNum = parseFloat(match[0].replace(",", "."));
                }
            }

            if (areaNum && !isNaN(areaNum)) {
                total_area += areaNum;
                n++;
            }
        }

        return { kind: "sum_area", propsFlatKey, total_area, n };
    }

    // default: list docs
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

    // Reconstruct nested structure for compatibility
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

// ----- Prompts (Planner -> Query -> Answer) -----

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
- semanticQuery: string (Vietnamese keywords for semantic search, only if useSemanticSearch=true)
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
- If task is sum_area: provide total and unit note (area unit depends on model; usually m²).

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

// ----- Route -----

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

        const hintCategory = await detectHintCategory(
            question,
            meta.categories
        );
        console.log("💡 Hint category:", hintCategory);

        const plan1 = await geminiJson(
            intentPrompt({
                question,
                categories: meta.categories,
                hintCategory,
            })
        );
        const plan1Prompt = intentPrompt({
            question,
            categories: meta.categories,
            hintCategory,
        });
        console.log("📝 Plan1Prompt:", plan1Prompt);
        console.log("📝 Plan1:", plan1);

        if (plan1.intent === "general") {
            const answer = await geminiText(generalPrompt(question), {
                temperature: 0.2,
            });
            return res.json({
                answer,
                ...(debug ? { debug: { meta, plan1 } } : {}),
            });
        }

        // Fix category if LLM missed but we have a strong hint and category exists
        let category = plan1.category ? norm(plan1.category) : null;
        if (!category && hintCategory && meta.categories.includes(hintCategory))
            category = hintCategory;
        console.log("🎯 Final category:", category);

        // Step 2: choose parameters (including semantic search)
        const plan2 = await geminiJson(
            parameterPrompt({
                question,
                plan1: { ...plan1, category },
                paramSamples: meta.paramSamples,
                areaKeys: meta.areaKeys,
            })
        );
        console.log("📝 Plan2:", plan2);
        const plan2Prompt = parameterPrompt({
            question,
            plan1: { ...plan1, category },
            paramSamples: meta.paramSamples,
            areaKeys: meta.areaKeys,
        });
        console.log("📝 Plan2Prompt:", plan2Prompt);

        // Merge plan
        const plan = {
            urn,
            intent: "bim",
            task: plan1.task || "count",
            category,
            limit: plan2.limit || plan1.limit || 20,
            filterParam: plan2.filterParam || null,
            filterValue: plan2.filterValue ?? null,
            targetParam: plan2.targetParam || null,
            propsFlatKey: plan2.propsFlatKey || null,
            useSemanticSearch: plan2.useSemanticSearch || false,
            semanticQuery: plan2.semanticQuery || null,
            topK: plan2.topK || 100,
            notes: plan1.notes || "",
        };

        // Normalize known params (remove nested paths for SQLite)
        const allowedParams = new Set([
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
        if (plan.filterParam && !allowedParams.has(plan.filterParam))
            plan.filterParam = null;
        if (plan.targetParam && !allowedParams.has(plan.targetParam))
            plan.targetParam = null;

        console.log("🔍 Final plan:", plan);

        // Query
        const result = await runQuery(db, meta, plan);
        console.log("✅ Query result:", result);

        // Answer
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
