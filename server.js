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
  setContactStatus, backfillVideos, sortByFire, isContacted, CONTACT_STATUSES,
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
    const all = await getLastResults(Infinity);
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
    const results = await getLastResults(Infinity);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Lightweight change signature — lets the UI skip re-downloading the full list
app.get('/api/results/meta', async (req, res) => {
  try {
    const rows = await getLastResults(Infinity);
    let maxId = 0, sentCount = 0, lastStatusAt = '';
    const byStatus = {};
    for (const r of rows) {
      if (r.id > maxId) maxId = r.id;
      if (isContacted(r)) sentCount++;
      byStatus[r.contact_status] = (byStatus[r.contact_status] || 0) + 1;
      const t = r.status_updated_at ? new Date(r.status_updated_at).toISOString() : '';
      if (t > lastStatusAt) lastStatusAt = t;
    }
    res.json({ count: rows.length, maxId, sentCount, byStatus, lastStatusAt });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── DOWNLOADS ───────────────────────────────────────────────────────────────
app.get('/api/download', async (req, res) => {
  try {
    const all = await getLastResults(Infinity, { fresh: true });
    // New leads only: never-contacted creators. Downloading does NOT mark anyone; use
    // POST /api/creators/status (dashboard "Mark as sent") once emails actually go out.
    let rows = all.filter(r => !isContacted(r));
    if (req.query.campaign === 'true') rows = rows.filter(isCampaignCreator);
    if (req.query.hasEmail === 'true') rows = rows.filter(r => r.email && r.email !== 'Not listed');
    if (rows.length === 0) return res.status(404).json({ error: 'No new results to download' });
    const XLSX = require('xlsx');
    const wb = generateExcel(rows);
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="bemellou-creators-${Date.now()}.xlsx"`);
    res.send(buf);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/download/csv', async (req, res) => {
  try {
    const batch = req.query.batch;
    const all = await getLastResults(Infinity, { fresh: true });
    let data = batch ? all.filter(r => String(r.batch_number) === String(batch)) : all;
    // New leads only (never contacted). Read-only: see /api/creators/status.
    data = data.filter(r => !isContacted(r));
    if (req.query.campaign === 'true') data = data.filter(isCampaignCreator);
    if (req.query.hasEmail === 'true') data = data.filter(r => r.email && r.email !== 'Not listed');
    if (data.length === 0) return res.status(404).json({ error: 'No new results found' });

    const cols = [
      'first_name', 'handle', 'email', 'niche', 'subscriber_count',
      'avg_views', 'avg_likes', 'avg_comments', 'like_ratio', 'comment_ratio',
      'country', 'upload_frequency', 'total_views', 'video_count',
      'channel_url', 'thumbnail_url', 'date_found', 'batch_number',
      'fire_score', 'fire_label', 'us_status', 'contact_status',
      'opener', 'top_video_title', 'top_video_url', 'median_views', 'shorts_share',
      'vibe', 'praise', 'looking_forward',
    ];
    const headers = [
      'Name', 'Handle', 'Email', 'Niche', 'Subscribers',
      'Avg Views', 'Avg Likes', 'Avg Comments', 'Like Ratio', 'Comment Ratio',
      'Country', 'Uploads/Mo', 'Total Views', 'Video Count',
      'Channel URL', 'Thumbnail URL', 'Date Found', 'Batch',
      'FIRE Score', 'FIRE', 'US Status', 'Contact Status',
      'Opener', 'Video To Reference', 'Video URL', 'Median Views', 'Shorts Share',
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
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── RANKED TOP-N DOWNLOAD ───────────────────────────────────────────────────
// Best creators first: FIRE score, then (avg views 50%, like ratio 25%, comment ratio 25%),
// separated into Has Email / No Email. Does NOT mark anyone as sent.
const hasEmail = r => r.email && r.email !== 'Not listed';

app.get('/api/download/top', async (req, res) => {
  try {
    const count = Math.max(parseInt(req.query.count) || 0, 0); // 0 = all
    const format = req.query.format === 'csv' ? 'csv' : 'xlsx';
    let rows = await getLastResults(Infinity, { fresh: true });
    if (req.query.email === 'has') rows = rows.filter(hasEmail);
    if (req.query.email === 'none') rows = rows.filter(r => !hasEmail(r));
    if (req.query.excludeSent === 'true') rows = rows.filter(r => !isContacted(r));
    if (req.query.fire) rows = rows.filter(r => String(req.query.fire).toUpperCase().split(',').includes(r.fire_label));
    rows = sortByFire(rows);
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
      'rank', 'fire_score', 'fire_label', 'best_score', 'has_email', 'first_name', 'handle', 'email', 'niche',
      'subscriber_count', 'avg_views', 'avg_likes', 'avg_comments', 'like_ratio',
      'comment_ratio', 'country', 'upload_frequency', 'total_views', 'video_count',
      'channel_url', 'date_found', 'batch_number',
      'us_status', 'contact_status', 'opener', 'top_video_title', 'top_video_url', 'median_views',
    ];
    const headers = [
      'Rank', 'FIRE Score', 'FIRE', 'Best Score', 'Has Email', 'Name', 'Handle', 'Email', 'Niche',
      'Subscribers', 'Avg Views', 'Avg Likes', 'Avg Comments', 'Like Ratio',
      'Comment Ratio', 'Country', 'Uploads/Mo', 'Total Views', 'Video Count',
      'Channel URL', 'Date Found', 'Batch',
      'US Status', 'Contact Status', 'Opener', 'Video To Reference', 'Video URL', 'Median Views',
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
    const all = await getLastResults(Infinity, { fresh: true });
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

// ─── CONTACT STATUS ──────────────────────────────────────────────────────────
// Body: { handles: [...], status, channel?, notes?, usShare? }
//   or  { filter: { campaign?, batch?, onlyNew?, hasEmail? }, status, ... } to mark a whole list
app.post('/api/creators/status', async (req, res) => {
  try {
    const { handles, filter, status, channel, notes, usShare } = req.body || {};
    if (!CONTACT_STATUSES.includes(status)) return res.status(400).json({ error: `status must be one of: ${CONTACT_STATUSES.join(', ')}` });
    let list = Array.isArray(handles) ? handles.filter(Boolean) : [];
    if (!list.length && filter && typeof filter === 'object') {
      let rows = await getLastResults(Infinity, { fresh: true });
      if (filter.batch) rows = rows.filter(r => String(r.batch_number) === String(filter.batch));
      if (filter.campaign) rows = rows.filter(isCampaignCreator);
      if (filter.hasEmail) rows = rows.filter(r => r.email && r.email !== 'Not listed');
      if (filter.onlyNew) rows = rows.filter(r => !isContacted(r));
      list = rows.map(r => r.handle);
    }
    if (!list.length) return res.status(400).json({ error: 'No creators matched: pass handles or a filter' });
    if (list.length > 5000) return res.status(400).json({ error: 'Refusing to update more than 5000 creators at once' });
    let share;
    if (usShare !== undefined && usShare !== null && usShare !== '') {
      share = Number(usShare);
      if (!(share >= 0 && share <= 100)) return res.status(400).json({ error: 'usShare must be 0-100' });
    }
    const updated = await setContactStatus(list, status, { channel: channel || null, notes: notes || null, usShare: share });
    res.json({ ok: true, updated, status });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── VIDEO BACKFILL ──────────────────────────────────────────────────────────
// Body: { limit?: 500, campaignOnly?: true }. Runs in the background; progress in /api/logs.
app.post('/api/backfill/videos', async (req, res) => {
  try {
    const { limit = 500, campaignOnly = true } = req.body || {};
    const result = await backfillVideos({ limit, campaignOnly: campaignOnly !== false && campaignOnly !== 'false' });
    res.status(result.started ? 202 : 409).json(result);
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
