# Component Type as Primary Category Field

## Change Summary

Simplified category field logic to **always use `component_type`** as the primary category field for BIM queries.

## Why This Change?

### Before (Complex Logic):
```javascript
// Checked OmniClass vs Component Type
const hasDoorsOrWindows = omni.some((v) => /doors|windows/i.test(v));
const categoryField = omni.length >= 2 && hasDoorsOrWindows
    ? "omniclass_title"   // Sometimes this
    : "component_type";   // Sometimes this

categories: categoryField === "omniclass_title" ? omni : comp
```

**Problems:**
- ❌ Inconsistent: Sometimes used OmniClass, sometimes Component Type
- ❌ Complex: Conditional logic based on doors/windows presence
- ❌ Unpredictable: Hard to know which field would be used
- ❌ OmniClass gaps: Not all elements have OmniClass data (only 896/2738 = 33%)

### After (Simple & Clean):
```javascript
// Always use component_type (normalized in preprocessing)
const categoryField = "component_type";
const categories = stripEmpty(compRows.map((r) => r.component_type));
```

**Benefits:**
- ✅ **Consistent**: Always uses same field
- ✅ **Simple**: No conditional logic
- ✅ **Complete**: All 2,738 elements have component_type (100% coverage)
- ✅ **Normalized**: Clean categories (Walls, Doors, Windows) from preprocessing
- ✅ **Predictable**: Users always query by same field

## Data Coverage

| Field | Coverage | Example Values |
|-------|----------|----------------|
| `component_type` | 2,738/2,738 (100%) | Walls, Doors, Windows, Beams, Pipes |
| `omniclass_title` | 896/2,738 (33%) | Curtain Mullions, Infill Panels, Pipework Fittings |

## Impact on Queries

### Before:
```javascript
// Query might use omniclass_title OR component_type
WHERE urn = ? AND omniclass_title = 'Doors'  // Sometimes
WHERE urn = ? AND component_type = 'Doors'   // Sometimes
```

### After:
```javascript
// Query ALWAYS uses component_type
WHERE urn = ? AND component_type = 'Doors'   // Always
```

## Category Examples

After preprocessing normalization, `component_type` contains clean categories:

```javascript
Top Categories by component_type:
- Curtain Mullions        549
- Infill Panels           295
- Curtain Panels          236
- Pipework Fittings       229
- Reinforcement           201
- Walls                   155
- Pipes                   140
- Beams                   104
- Columns                  62
- Doors                    15
- Windows                  18
- Floors                   38
```

## OmniClass Still Available

OmniClass data is still accessible via:
1. **Database field**: `omniclass_title` and `omniclass_number` columns
2. **Parameter samples**: Available in `paramSamples.omniclass_title` for planner
3. **Queries**: Can still filter by OmniClass if needed

But for **primary categorization**, we use the cleaner, more complete `component_type`.

## Query Flow

```
User: "Có bao nhiêu cửa?"
  ↓
LLM Hint Detection: "Doors" (from component_type categories)
  ↓
Intent Planner: { category: "Doors" }
  ↓
SQL Query: WHERE component_type = 'Doors'  ← Always this field
  ↓
Result: 15 doors found
```

## Code Changes

**File:** `routes/chat.js`

**Before:**
```javascript
async function getMeta(db, urn) {
    const omniRows = db.prepare(...).all(urn);
    const compRows = db.prepare(...).all(urn);

    const hasDoorsOrWindows = omni.some((v) => /doors|windows/i.test(v));
    const categoryField = omni.length >= 2 && hasDoorsOrWindows
        ? "omniclass_title"
        : "component_type";

    return {
        categoryField,
        categories: categoryField === "omniclass_title" ? omni : comp,
        ...
    };
}
```

**After:**
```javascript
async function getMeta(db, urn) {
    const compRows = db.prepare(
        "SELECT DISTINCT component_type FROM elements WHERE urn = ? ..."
    ).all(urn);
    const categories = stripEmpty(compRows.map((r) => r.component_type));

    return {
        categoryField: "component_type",  // Always this
        categories,                        // Always from component_type
        ...
    };
}
```

**Lines changed:** 73-127 (simplified from ~67 lines to ~57 lines)

## Testing

After running preprocessing + migration:

```bash
# 1. Check categories in database
sqlite3 data/bim.db "SELECT DISTINCT component_type FROM elements;"
# Should show: Walls, Doors, Windows, Beams, Pipes, etc.

# 2. Test query
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"urn":"YOUR_URN", "question":"Có bao nhiêu cửa?"}'

# Should return:
# {
#   "answer": "Có 15 cửa trong mô hình",
#   "hits": { "count": 15, ... }
# }
```

## Benefits Summary

1. **Simplicity** - Removed 10+ lines of conditional logic
2. **Consistency** - All queries use same category field
3. **Completeness** - 100% coverage vs 33% with OmniClass
4. **Performance** - One query instead of two (omni + comp)
5. **Maintainability** - Easier to understand and debug
6. **LLM-friendly** - Clean category names for better mapping

## Related Changes

This simplification works because of:
1. **Preprocessing** (`scripts/preprocess-data.js`) - Normalizes all categories into `component_type`
2. **LLM Detection** (`detectHintCategory`) - Maps questions to normalized categories
3. **Migration** (`scripts/migrate-to-sqlite.js`) - Handles preprocessed data properly

All three components work together to ensure `component_type` is the reliable, normalized category field for all queries.
