// server.js
const express = require('express');
const path = require('path');

const config = require('./config');

const authRouter = require('./routes/auth');
const modelsRouter = require('./routes/models');
const bimRouter = require('./routes/bim');
const chatRouter = require('./routes/chat');

const app = express();

// JSON body (PowerShell Invoke-RestMethod / curl)
app.use(express.json({ limit: '5mb' }));

// Static frontend (optional)
app.use(express.static(path.join(__dirname, 'public')));

// API routes
app.use('/api/auth', authRouter);
app.use('/api/models', modelsRouter);
app.use('/api/bim', bimRouter);
app.use('/api/chat', chatRouter);

// Simple health check
app.get('/api/health', (req, res) => res.json({ ok: true }));

// Error handler: always return JSON
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  const status = err.statusCode || err.status || 500;
  res.status(status).json({
    error: err.message || 'Internal Server Error',
    ...(process.env.NODE_ENV !== 'production' ? { stack: err.stack } : {})
  });
});

app.listen(config.PORT, () => {
  console.log(`Server listening on http://localhost:${config.PORT}`);
});


