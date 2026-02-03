// services/gemini.js
const config = require("../config");

let _genAI;
let _openAI;

function getClient() {
    if (_genAI) return _genAI;
    if (!config.GEMINI_API_KEY) {
        throw new Error("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) in .env");
    }
    // Lazy require so the server can start even if dependency is missing (but chat will fail).
    // Install: npm i @google/generative-ai
    // eslint-disable-next-line global-require
    const { GoogleGenerativeAI } = require("@google/generative-ai");
    _genAI = new GoogleGenerativeAI(config.GEMINI_API_KEY);
    return _genAI;
}

function getOpenAIClient() {
    if (_openAI) return _openAI;
    if (!config.OPENAI_API_KEY) {
        throw new Error("Missing OPENAI_API_KEY in .env");
    }
    // Lazy require - Install: npm i openai
    // eslint-disable-next-line global-require
    const { OpenAI } = require("openai");
    _openAI = new OpenAI({ apiKey: config.OPENAI_API_KEY });
    return _openAI;
}

function stripCodeFences(s) {
    if (!s) return s;
    return s
        .replace(/^\s*```(?:json)?\s*/i, "")
        .replace(/\s*```\s*$/i, "")
        .trim();
}

async function geminiText(prompt, opts = {}) {
    const client = getClient();
    const model = client.getGenerativeModel({
        model: opts.model || config.GEMINI_MODEL,
    });
    const result = await model.generateContent({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
            temperature: opts.temperature ?? 0.2,
            topP: opts.topP ?? 0.95,
            maxOutputTokens: opts.maxOutputTokens ?? 2048,
        },
    });
    return result.response.text();
}

/**
 * Request "JSON only" output and parse it.
 * We keep this simple (no schema) to maximize compatibility across Gemini SDK versions.
 */
async function geminiJson(prompt, opts = {}) {
    const maxRetries = opts.maxRetries ?? 2;
    let lastErr;

    for (let i = 0; i <= maxRetries; i += 1) {
        try {
            const text = await geminiText(
                `${prompt}\n\nIMPORTANT: Return ONLY valid JSON. No markdown, no code fences, no comments.`,
                { ...opts, temperature: opts.temperature ?? 0.1 }
            );
            const cleaned = stripCodeFences(text);
            return JSON.parse(cleaned);
        } catch (err) {
            lastErr = err;
        }
    }

    throw lastErr;
}

/**
 * OpenAI text generation - alternative to Gemini
 */
async function openaiText(prompt, opts = {}) {
    const client = getOpenAIClient();

    const payload = {
        model: opts.model || config.OPENAI_CHAT_MODEL || "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        // max_completion_tokens: opts.maxOutputTokens ?? 2048,
    };
    if (payload.model.includes("gpt-5")) {
        payload.max_completion_tokens = opts.maxOutputTokens ?? 2048;
        payload.reasoning_effort = "medium";
    } else {
        payload.max_tokens = opts.maxOutputTokens ?? 2048;
        payload.temperature = opts.temperature ?? 0.2;
        payload.top_p = opts.topP ?? 0.95;
    }

    const response = await client.chat.completions.create({
        ...payload,
    });
    console.log(
        "OpenAI model",
        opts.model || config.OPENAI_CHAT_MODEL || "gpt-4o-mini"
    );
    return response.choices[0].message.content;
}

/**
 * OpenAI JSON generation - alternative to Gemini
 */
async function openaiJson(prompt, opts = {}) {
    const maxRetries = opts.maxRetries ?? 2;
    let lastErr;

    for (let i = 0; i <= maxRetries; i += 1) {
        try {
            const text = await openaiText(
                `${prompt}\n\nIMPORTANT: Return ONLY valid JSON. No markdown, no code fences, no comments.`,
                { ...opts, temperature: opts.temperature ?? 0.1 }
            );
            const cleaned = stripCodeFences(text);
            return JSON.parse(cleaned);
        } catch (err) {
            lastErr = err;
        }
    }
    console.log(
        "OpenAI model",
        opts.model || config.OPENAI_CHAT_MODEL || "gpt-4o-mini"
    );

    throw lastErr;
}

// module.exports = { geminiText, geminiJson, openaiText, openaiJson };
module.exports = {
    // geminiText,
    // geminiJson,
    geminiText: openaiText,
    geminiJson: openaiJson,
};
