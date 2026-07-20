'use strict';
require('dotenv').config();
const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');

const {
  startScheduler, executeBatch, getState, getLastResults,
  generateExcel, initDb, getApiKeys, getLogs, pushToInstantly,
  getManualSentBatches, toggleManualSent, markInstantlySent, resetSentLast2Days,
  generatePersonalization, enrichNewCreators, enrichBatch, resetEnrichment,
  lookupCreator, sortByBest, generateRankedWorkbook,
  isCampaignCreator, CAMPAIGN_TARGET,
} = require('./scheduler');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { etag: false, lastModified: false, setHeaders: (res) => res.setHeader('Cache-Control', 'no-store') }));

// Ensure runtime dirs exist
['data', 'logs'].forEach(dir => {
  const p = path.join(__dirname, dir);
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
});

// ─── STATUS ───────────────────────────────────────────────────────────────────
app.get('/api/status', async (req, res) => {
  const state = getState();
  const keys = getApiKeys();
  let campaignCount = 0;
  try {
    const all = await getLastResults(10000);
    campaignCount = all.filter(isCampaignCreator).length;
  } catch (e) { /* status must never fail on the campaign count */ }
  res.json({
    ...state,
    hasApiKey: keys.length > 0,
    apiKeyCount: keys.length,
    campaignCount,
    campaignTarget: CAMPAIGN_TARGET,
  });
});

// ─── LIVE PROGRESS ────────────────────────────────────────────────────────────
app.get('/api/progress', (req, res) => {
  const state = getState();
  res.json(state.progress || { phase: 'Idle', done: 0, total: 0, currentName: '', foundSoFar: 0 });
});

// ─── LOGS ─────────────────────────────────────────────────────────────────────
app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 80;
  const logs = getLogs();
  res.json({ logs: logs.slice(-limit) });
});

// ─── DEBUG ───────────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
  const state = getState();
  const keys = getApiKeys();
  const logs = getLogs();
  res.json({
    state,
    keyCount: keys.length,
    keyNames: Array.from({ length: 10 }, (_, i) => `YOUTUBE_API_KEY_${i + 1}`)
      .filter(name => !!process.env[name])
      .concat(process.env.YOUTUBE_API_KEY ? ['YOUTUBE_API_KEY'] : []),
    dbUrl: process.env.DATABASE_URL ? process.env.DATABASE_URL.replace(/:([^:@]+)@/, ':***@') : 'NOT SET',
    recentLogs: logs.slice(-30),
  });
});

// ─── TRIGGER ─────────────────────────────────────────────────────────────────
app.post('/api/trigger', async (req, res) => {
  const state = getState();
  if (state.isRunning) return res.status(409).json({ error: 'Batch already running' });
  const keys = getApiKeys();
  if (keys.length === 0) return res.status(400).json({ error: 'No YouTube API keys configured' });
  res.json({ success: true, message: `Batch started with ${keys.length} key(s)` });
  executeBatch(keys).catch(e => console.error('Batch error:', e.message));
});

// ─── RESULTS ─────────────────────────────────────────────────────────────────
app.get('/api/results', async (req, res) => {
  try {
    const results = await getLastResults(parseInt(req.query.limit) || 50);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/results/all', async (req, res) => {
  try {
    const results = await getLastResults(10000);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Lightweight change signature — lets the UI skip re-downloading the full list
app.get('/api/results/meta', async (req, res) => {
  try {
    const rows = await getLastResults(10000);
    let maxId = 0, sentCount = 0;
    for (const r of rows) {
      if (r.id > maxId) maxId = r.id;
      if (r.instantly_sent_at) sentCount++;
    }
    res.json({ count: rows.length, maxId, sentCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── DOWNLOADS ───────────────────────────────────────────────────────────────
app.get('/api/download', async (req, res) => {
  try {
    const all = await getLastResults(10000, { fresh: true });
    // Exclude creators with email that have already been downloaded
    let rows = all.filter(r => !(r.email && r.email !== 'Not listed' && r.instantly_sent_at));
    if (req.query.campaign === 'true') rows = rows.filter(isCampaignCreator);
    if (req.query.hasEmail === 'true') rows = rows.filter(r => r.email && r.email !== 'Not listed');
    if (rows.length === 0) return res.status(404).json({ error: 'No new results to download' });
    const XLSX = require('xlsx');
    const wb = generateExcel(rows);
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="bemellou-creators-${Date.now()}.xlsx"`);
    res.send(buf);
    const emails = rows.map(r => r.email).filter(Boolean);
    markInstantlySent(emails).catch(() => {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/download/csv', async (req, res) => {
  try {
    const batch = req.query.batch;
    const all = await getLastResults(10000, { fresh: true });
    let data = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    // Exclude creators with email that have already been downloaded
    data = data.filter(r => !(r.email && r.email !== 'Not listed' && r.instantly_sent_at));
    if (req.query.campaign === 'true') data = data.filter(isCampaignCreator);
    if (req.query.hasEmail === 'true') data = data.filter(r => r.email && r.email !== 'Not listed');
    if (data.length === 0) return res.status(404).json({ error: 'No new results found' });

    const cols = [
      'first_name', 'handle', 'email', 'niche', 'subscriber_count',
      'avg_views', 'avg_likes', 'avg_comments', 'like_ratio', 'comment_ratio',
      'country', 'upload_frequency', 'total_views', 'video_count',
      'channel_url', 'thumbnail_url', 'date_found', 'batch_number',
      'vibe', 'praise', 'looking_forward',
    ];
    const headers = [
      'Name', 'Handle', 'Email', 'Niche', 'Subscribers',
      'Avg Views', 'Avg Likes', 'Avg Comments', 'Like Ratio', 'Comment Ratio',
      'Country', 'Uploads/Mo', 'Total Views', 'Video Count',
      'Channel URL', 'Thumbnail URL', 'Date Found', 'Batch',
      'VIBE', 'PRAISE', 'LOOKING FORWARD',
    ];

    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const csv = [
      headers.map(esc).join(','),
      ...data.map(r => cols.map(c => {
        const v = r[c];
        if (c === 'like_ratio' || c === 'comment_ratio') return esc(v != null ? (v * 100).toFixed(2) + '%' : '');
        return esc(v ?? '');
      }).join(',')),
    ].join('\n');

    const filename = req.query.campaign === 'true'
      ? 'bemellou-campaign-us-creators.csv'
      : (batch ? `bemellou-batch-${batch}.csv` : 'bemellou-all-creators.csv');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
    const emails = data.map(r => r.email).filter(Boolean);
    markInstantlySent(emails).catch(() => {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── RANKED TOP-N DOWNLOAD ───────────────────────────────────────────────────
// Best creators first (avg views 50%, like ratio 25%, comment ratio 25%),
// separated into Has Email / No Email. Does NOT mark anyone as sent.
const hasEmail = r => r.email && r.email !== 'Not listed';

app.get('/api/download/top', async (req, res) => {
  try {
    const count = Math.max(parseInt(req.query.count) || 0, 0); // 0 = all
    const format = req.query.format === 'csv' ? 'csv' : 'xlsx';
    let rows = await getLastResults(10000, { fresh: true });
    if (req.query.email === 'has') rows = rows.filter(hasEmail);
    if (req.query.email === 'none') rows = rows.filter(r => !hasEmail(r));
    if (req.query.excludeSent === 'true') rows = rows.filter(r => !r.instantly_sent_at);
    rows = sortByBest(rows);
    if (count > 0) rows = rows.slice(0, count);
    if (!rows.length) return res.status(404).json({ error: 'No creators match' });
    rows.forEach((r, i) => { r.rank = i + 1; });

    const withEmail = rows.filter(hasEmail);
    const noEmail = rows.filter(r => !hasEmail(r));
    const label = count > 0 ? `top-${count}` : 'all-ranked';

    if (format === 'xlsx') {
      const XLSX = require('xlsx');
      const wb = generateRankedWorkbook([
        { name: 'Has Email', rows: withEmail },
        { name: 'No Email', rows: noEmail },
      ]);
      const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="bemellou-${label}-creators.xlsx"`);
      return res.send(buf);
    }

    // CSV: Has Email rows first, then No Email, with Rank + Best Score + Has Email columns
    const cols = [
      'rank', 'best_score', 'has_email', 'first_name', 'handle', 'email', 'niche',
      'subscriber_count', 'avg_views', 'avg_likes', 'avg_comments', 'like_ratio',
      'comment_ratio', 'country', 'upload_frequency', 'total_views', 'video_count',
      'channel_url', 'date_found', 'batch_number',
    ];
    const headers = [
      'Rank', 'Best Score', 'Has Email', 'Name', 'Handle', 'Email', 'Niche',
      'Subscribers', 'Avg Views', 'Avg Likes', 'Avg Comments', 'Like Ratio',
      'Comment Ratio', 'Country', 'Uploads/Mo', 'Total Views', 'Video Count',
      'Channel URL', 'Date Found', 'Batch',
    ];
    const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const toLine = r => cols.map(c => {
      if (c === 'has_email') return esc(hasEmail(r) ? 'Yes' : 'No');
      const v = r[c];
      if (c === 'like_ratio' || c === 'comment_ratio') return esc(v != null ? (v * 100).toFixed(2) + '%' : '');
      return esc(v ?? '');
    }).join(',');
    const csv = [headers.map(esc).join(','), ...withEmail.map(toLine), ...noEmail.map(toLine)].join('\n');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="bemellou-${label}-creators.csv"`);
    res.send(csv);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/enrich/reset', async (req, res) => {
  try {
    const count = await resetEnrichment();
    res.json({ ok: true, reset: count });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/enrich', async (req, res) => {
  try {
    const remaining = await enrichBatch(10);
    res.json({ ok: true, remaining });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/enrich/debug', async (req, res) => {
  try {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    const rows = await getLastResults();
    const withoutVibe = rows.filter(r => !r.vibe).length;
    res.json({
      apiKeySet: !!(apiKey && apiKey.trim()),
      apiKeyLength: apiKey ? apiKey.trim().length : 0,
      totalCreators: rows.length,
      creatorsWithoutVibe: withoutVibe,
      sample: rows.slice(0, 2).map(r => ({ handle: r.handle, vibe: r.vibe, praise: r.praise })),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── INSTANTLY AI ─────────────────────────────────────────────────────────────
app.post('/api/instantly/push', async (req, res) => {
  const apiKey = process.env.INSTANTLY_API_KEY;
  if (!apiKey) return res.status(400).json({ error: 'INSTANTLY_API_KEY must be set in .env' });
  try {
    const { batch, campaign } = req.body;
    const all = await getLastResults(10000, { fresh: true });
    let creators = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    if (campaign === true || campaign === 'true') creators = creators.filter(isCampaignCreator);
    if (creators.length === 0) return res.status(404).json({ error: 'No creators found' });
    const batchLabel = campaign ? 'Campaign · US' : (batch ? `Batch ${batch}` : 'All Creators');
    const result = await pushToInstantly(creators, apiKey, batchLabel);
    res.json({ success: true, ...result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── RESET SENT (last 2 days) ────────────────────────────────────────────────
app.post('/api/reset-sent', async (req, res) => {
  try {
    const count = await resetSentLast2Days();
    res.json({ ok: true, reset: count });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── MANUAL SEND TRACKING ────────────────────────────────────────────────────
app.get('/api/batches/manual-sent', (req, res) => {
  res.json({ batches: getManualSentBatches() });
});

app.post('/api/batches/:batch/toggle-manual-sent', async (req, res) => {
  const batch = parseInt(req.params.batch);
  if (isNaN(batch)) return res.status(400).json({ error: 'Invalid batch number' });
  try {
    const isSent = await toggleManualSent(batch);
    res.json({ batchNumber: batch, manuallySent: isSent });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── CREATOR LOOKUP ──────────────────────────────────────────────────────────
app.post('/api/lookup', async (req, res) => {
  const { channel } = req.body;
  if (!channel || !channel.trim()) return res.status(400).json({ error: 'channel is required' });
  const keys = getApiKeys();
  if (!keys.length) return res.status(400).json({ error: 'No YouTube API keys configured' });
  try {
    const result = await lookupCreator(channel.trim());
    if (!result) return res.status(404).json({ error: 'Channel not found or no videos available' });
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── SPA FALLBACK ─────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── START ────────────────────────────────────────────────────────────────────
initDb()
  .then(() => {
    app.listen(PORT, () => {
      const keys = getApiKeys();
      console.log(`\n Bemellou YouTuber Finder running at http://localhost:${PORT}`);
      console.log(` ${keys.length} API key(s) loaded`);
      if (keys.length === 0) console.log(' WARNING: No YouTube API keys found!');
      startScheduler();
    });
  })
  .catch(e => {
    console.error('DB init error:', e.message);
    // Start anyway — server works without DB (in-memory mode)
    app.listen(PORT, () => {
      console.log(`\n Server running on port ${PORT} (no DB — in-memory mode)`);
      startScheduler();
    });
  });
