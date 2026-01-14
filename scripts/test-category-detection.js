// scripts/test-category-detection.js
// Test LLM-based category detection vs keyword matching

const { geminiJson } = require('../services/gemini');

// Simulated available categories (from preprocessed data)
const availableCategories = [
  'Curtain Mullions',
  'Infill Panels',
  'Curtain Panels',
  'Pipework Fittings',
  'Reinforcement',
  'Center Line',
  'Walls',
  'Pipes',
  'Beams',
  'Columns',
  'Downlights',
  'Floors',
  'Doors',
  'Windows',
  'Ceilings',
  'Roofs',
  'Stairs',
  'Railings',
  'Ducts',
  'Mechanical Equipment',
  'Plumbing Fixtures',
  'Lighting Fixtures',
  'Electrical Equipment',
];

// Old keyword-based detection
function oldDetectHintCategory(question) {
  const q = (question || '').toLowerCase();
  if (q.includes('cửa sổ') || q.includes('cua so')) return 'Windows';
  if (q.includes('cửa') || q.includes('cua ')) return 'Doors';
  if (q.includes('tầng')) return 'Level';
  if (q.includes('phòng')) return 'Room';
  if (q.includes('hệ thống')) return 'System';
  if (q.includes('thiết bị')) return 'Equipment';
  if (q.includes('vật tư')) return 'Material';
  return null;
}

// New LLM-based detection
async function newDetectHintCategory(question, categories) {
  if (!question || !categories || categories.length === 0) {
    return null;
  }

  // Quick keyword fallback
  const q = question.toLowerCase();
  if (q.includes('cửa sổ') || q.includes('cua so')) return 'Windows';
  if ((q.includes('cửa') || q.includes('cua')) && !q.includes('sổ')) return 'Doors';

  // Use LLM
  try {
    const prompt = `Bạn là chuyên gia BIM. Dựa vào câu hỏi của người dùng, hãy xác định loại thành phần BIM (component_type) phù hợp nhất.

Câu hỏi: "${question}"

Các loại thành phần có sẵn (chọn 1 hoặc null):
${categories.slice(0, 50).map(c => `- ${c}`).join('\n')}

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
- Chỉ trả về category nếu confidence >= medium
- Trả về null nếu không chắc chắn`;

    const result = await geminiJson(prompt, { temperature: 0.1 });

    if (result.category && result.confidence !== 'low') {
      return { category: result.category, ...result };
    }
  } catch (error) {
    console.error('⚠ LLM detection failed:', error.message);
  }

  return null;
}

// Test cases
const testQuestions = [
  'Có bao nhiêu cửa?',
  'Có bao nhiêu cửa sổ?',
  'Liệt kê các loại tường',
  'Đếm số lượng cột',
  'Tìm các dầm kết cấu',
  'Có bao nhiêu ống nước?',
  'Đèn chiếu sáng ở tầng 2',
  'Hệ thống điện',
  'Các thành phần kết cấu',
  'Vật liệu trong suốt',
  'Tổng diện tích sàn',
];

async function runTests() {
  console.log('🧪 Testing Category Detection\n');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  for (const question of testQuestions) {
    console.log(`❓ Question: "${question}"`);

    // Old method
    const oldResult = oldDetectHintCategory(question);
    console.log(`   ⚙️  Keyword:  ${oldResult || 'null'}`);

    // New method
    const newResult = await newDetectHintCategory(question, availableCategories);
    if (newResult) {
      console.log(`   🤖 LLM:      ${newResult.category} (${newResult.confidence})`);
      console.log(`   💡 Reason:   ${newResult.reason}`);
    } else {
      console.log(`   🤖 LLM:      null`);
    }

    console.log('');
  }

  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
  console.log('✅ Test complete!\n');
  console.log('Key Improvements:');
  console.log('  • LLM understands Vietnamese semantics better');
  console.log('  • Maps to actual BIM categories (not generic "Equipment", "Material")');
  console.log('  • Provides confidence scores and reasoning');
  console.log('  • Handles complex queries like "structural components"');
  console.log('  • Falls back to keywords for speed on simple queries\n');
}

runTests().catch(console.error);
