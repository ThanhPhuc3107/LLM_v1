// routes/auth.js
const express = require('express');
const router = express.Router();

const { getTwoLeggedToken } = require('../services/aps');

/**
 * GET /api/auth/token
 * Returns an APS 2-legged token for the Viewer (viewables:read).
 */
router.get('/token', async (req, res, next) => {
  try {
    const token = await getTwoLeggedToken(['viewables:read']);
    // Viewer expects: { access_token, expires_in, token_type }
    // We return a conservative expires_in (token cache handles refresh server-side).
    res.json({ access_token: token, token_type: 'Bearer', expires_in: 3599 });
  } catch (err) {
    next(err);
  }
});

/**
 * GET /api/auth/callback
 * If you use 3-legged OAuth, implement here. (Optional)
 */
router.get('/callback', (req, res) => {
  res.status(501).send('3-legged OAuth callback is not implemented in this minimal template.');
});

module.exports = router;
