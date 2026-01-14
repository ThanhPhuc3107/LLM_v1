// routes/bim.js
const express = require('express');
const router = express.Router();
const { getDb } = require('../services/sqlite.js');

function normalizeUrn(u) {
  u = String(u || '').trim();
  if (u.startsWith('<') && u.endsWith('>')) u = u.slice(1, -1).trim();
  return u;
}

function getCategoryField(mode) {
  mode = String(mode || 'type').toLowerCase().trim();
  if (mode === 'omniclass' || mode === 'omni') return 'omniclass_title';
  return 'component_type'; // default (type-level)
}

router.get('/api/bim/categories', async (req, res, next) => {
  try {
    let { urn, mode = 'type' } = req.query;
    urn = normalizeUrn(urn);
    if (!urn) return res.status(400).json({ error: 'Missing urn' });

    const db = getDb();
    const categoryField = getCategoryField(mode);

    const rows = db.prepare(
      `SELECT DISTINCT ${categoryField} FROM elements WHERE urn = ? AND ${categoryField} IS NOT NULL ORDER BY ${categoryField}`
    ).all(urn);

    const categories = rows.map(r => r[categoryField]).filter(Boolean);

    res.json({
      urn,
      mode,
      categoryField,
      categories
    });
  } catch (e) { next(e); }
});

router.get('/api/bim/distinct', async (req, res, next) => {
  try {
    let { urn, category, param, q, limit = 50, mode = 'type' } = req.query;
    urn = normalizeUrn(urn);
    if (!urn || !category || !param)
      return res.status(400).json({ error: 'Missing urn/category/param' });

    const db = getDb();
    const categoryField = getCategoryField(mode);

    // Build query
    let sql = `SELECT DISTINCT ${param} FROM elements WHERE urn = ? AND ${categoryField} = ? AND ${param} IS NOT NULL`;
    const params = [urn, category];

    if (q) {
      sql += ` AND ${param} LIKE ?`;
      params.push(`%${q}%`);
    }

    sql += ` LIMIT ?`;
    params.push(Number(limit));

    const rows = db.prepare(sql).all(...params);
    const values = rows.map(r => r[param]).filter(Boolean);

    res.json({ urn, category, mode, param, values });
  } catch (e) { next(e); }
});

module.exports = router;
