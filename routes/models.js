// routes/models.js
const express = require('express');
const router = express.Router();

const { getObjects, getManifest, getMetadata } = require('../services/aps');
const { extractModelToMongo } = require('../services/extract');

/**
 * GET /api/models
 * List objects in APS bucket (and their base64 URNs).
 */
router.get('/', async (req, res, next) => {
  try {
    const objs = await getObjects();
    // Expect shape: [{ name, urn }]
    res.json(objs);
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/models/:urn/status
 * Check Model Derivative manifest status.
 */
router.get('/:urn/status', async (req, res, next) => {
  try {
    const urn = req.params.urn;
    const manifest = await getManifest(urn);
    res.json({ urn, manifest });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/models/:urn/metadata
 * Get metadata and guid(s).
 */
router.get('/:urn/metadata', async (req, res, next) => {
  try {
    const urn = req.params.urn;
    const metadata = await getMetadata(urn);
    res.json({ urn, metadata });
  } catch (err) {
    next(err);
  }
});

/**
 * POST /api/models/:urn/extract
 * Phase 1 (APS -> MongoDB): extract properties and store into Mongo.
 */
router.post('/:urn/extract', async (req, res, next) => {
  try {
    const urn = req.params.urn;
    const out = await extractModelToMongo(urn);
    res.json(out);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
