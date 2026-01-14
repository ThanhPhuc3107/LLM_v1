// services/aps.js
// Minimal APS (Autodesk Platform Services) helpers for:
// - 2-legged token (Viewer + Model Derivative + OSS)
// - list bucket objects
// - Model Derivative manifest/metadata/properties

const axios = require('axios');
const qs = require('querystring');
const config = require('../config');

const APS_AUTH_URL = 'https://developer.api.autodesk.com/authentication/v2/token';
const APS_OSS_URL = 'https://developer.api.autodesk.com/oss/v2';
const APS_MD_URL = 'https://developer.api.autodesk.com/modelderivative/v2/designdata';

let _tokenCache = null; // { access_token, expires_at_ms, scopeKey }

function assertApsConfig() {
  const miss = [];
  if (!config.APS_CLIENT_ID) miss.push('APS_CLIENT_ID');
  if (!config.APS_CLIENT_SECRET) miss.push('APS_CLIENT_SECRET');
  if (!config.APS_BUCKET) miss.push('APS_BUCKET');
  if (miss.length) {
    throw new Error(`Missing ${miss.join(', ')} in .env`);
  }
}

function scopeKey(scopes) {
  return (scopes || []).slice().sort().join(' ');
}

async function getTwoLeggedToken(scopes = ['data:read', 'viewables:read']) {
  assertApsConfig();

  const key = scopeKey(scopes);
  const now = Date.now();

  if (_tokenCache && _tokenCache.scopeKey === key && _tokenCache.expires_at_ms - now > 60_000) {
    return _tokenCache.access_token;
  }

  const body = qs.stringify({
    grant_type: 'client_credentials',
    client_id: config.APS_CLIENT_ID,
    client_secret: config.APS_CLIENT_SECRET,
    scope: scopes.join(' ')
  });

  const resp = await axios.post(APS_AUTH_URL, body, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
  });

  const tok = resp.data.access_token;
  const expiresIn = resp.data.expires_in || 3600;

  _tokenCache = {
    access_token: tok,
    expires_at_ms: now + expiresIn * 1000,
    scopeKey: key
  };

  return tok;
}

function toBase64Urn(objectId) {
  return Buffer.from(objectId).toString('base64').replace(/=+$/g, '');
}

async function getObjects() {
  const token = await getTwoLeggedToken(['bucket:read', 'data:read']);
  const url = `${APS_OSS_URL}/buckets/${encodeURIComponent(config.APS_BUCKET)}/objects`;
  const resp = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });

  const items = (resp.data && resp.data.items) ? resp.data.items : [];
  return items.map(it => ({
    name: it.objectKey,
    objectId: it.objectId,
    urn: toBase64Urn(it.objectId)
  }));
}

async function getManifest(urn) {
  const token = await getTwoLeggedToken(['viewables:read']);
  const url = `${APS_MD_URL}/${encodeURIComponent(urn)}/manifest`;
  const resp = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return resp.data;
}

async function getMetadata(urn) {
  const token = await getTwoLeggedToken(['viewables:read']);
  const url = `${APS_MD_URL}/${encodeURIComponent(urn)}/metadata`;
  const resp = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return resp.data;
}

async function getProperties(urn, guid) {
  const token = await getTwoLeggedToken(['viewables:read']);
  const url = `${APS_MD_URL}/${encodeURIComponent(urn)}/metadata/${encodeURIComponent(guid)}/properties`;
  const resp = await axios.get(url, { headers: { Authorization: `Bearer ${token}` } });
  return resp.data;
}

module.exports = {
  getTwoLeggedToken,
  toBase64Urn,
  getObjects,
  getManifest,
  getMetadata,
  getProperties
};
