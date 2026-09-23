'use strict';
require('dotenv').config();
const axios = require('axios');
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');
const cron = require('node-cron');
const Anthropic = require('@anthropic-ai/sdk');

// ─── LOGGING ─────────────────────────────────────────────────────────────────
const LOG_FILE = path.join(__dirname, 'logs', 'scheduler.log');
const MAX_MEM_LOGS = 200;
let recentLogs = [];

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const line = `[${ts}] ${msg}`;
  console.log(line);
  recentLogs.push(line);
  if (recentLogs.length > MAX_MEM_LOGS) recentLogs.shift();
  try { fs.appendFileSync(LOG_FILE, line + '\n'); } catch (e) {}
}

function getLogs() { return [...recentLogs]; }

// ─── STATE FILE (fallback persistence without DB) ─────────────────────────────
const STATE_FILE = path.join(__dirname, 'data', 'state.json');

function loadStateFile() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (e) { return {}; }
}

function saveStateFile(data) {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(data, null, 2)); } catch (e) {}
}

// ─── DATABASE ─────────────────────────────────────────────────────────────────
let pool = null;

function getPool() {
  if (!pool) {
    const url = process.env.DATABASE_URL;
    if (!url) return null;
    pool = new Pool({
      connectionString: url,
      ssl: false,
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });
    pool.on('error', (e) => log('Pool error: ' + e.message));
  }
  return pool;
}

async function initDb() {
  const p = getPool();
  if (!p) { log('No DATABASE_URL — running without persistence'); return; }
  await p.query(`
    CREATE TABLE IF NOT EXISTS creators (
      id SERIAL PRIMARY KEY,
      first_name TEXT,
      handle TEXT UNIQUE,
      email TEXT,
      avg_views NUMERIC,
      avg_likes NUMERIC,
      avg_comments NUMERIC,
      like_ratio NUMERIC,
      comment_ratio NUMERIC,
      subscriber_count NUMERIC,
      niche TEXT,
      channel_url TEXT,
      date_found TEXT,
      batch_number INTEGER,
      video_count INTEGER,
      total_views BIGINT,
      country TEXT,
      upload_frequency NUMERIC,
      thumbnail_url TEXT
    )
  `);
  // Add new columns to existing tables safely
  const newCols = [
    ['video_count', 'INTEGER'],
    ['total_views', 'BIGINT'],
    ['country', 'TEXT'],
    ['upload_frequency', 'NUMERIC'],
    ['thumbnail_url', 'TEXT'],
    ['instantly_sent_at', 'TIMESTAMPTZ'],
    ['vibe', 'TEXT'],
    ['praise', 'TEXT'],
    ['looking_forward', 'TEXT'],
    ['ideal_price', 'NUMERIC'],
    ['last_posted_at', 'TEXT'],
    ['commission_score', 'NUMERIC'],
    // video-specific data (free: same videos.list call, more parts)
    ['channel_id', 'TEXT'],
    ['median_views', 'NUMERIC'],
    ['shorts_share', 'NUMERIC'],
    ['latest_video_title', 'TEXT'],
    ['latest_video_url', 'TEXT'],
    ['latest_video_at', 'TEXT'],
    ['top_video_title', 'TEXT'],
    ['top_video_url', 'TEXT'],
    ['videos_checked_at', 'TIMESTAMPTZ'],
    // real contact tracking (downloads no longer count as "sent")
    ['contact_status', 'TEXT'],
    ['exported_at', 'TIMESTAMPTZ'],
    ['contacted_at', 'TIMESTAMPTZ'],
    ['contact_channel', 'TEXT'],
    ['status_updated_at', 'TIMESTAMPTZ'],
    ['notes', 'TEXT'],
    // US audience share (%) from the creator's own analytics panel, entered by hand
    ['us_share_verified', 'NUMERIC'],
  ];
  for (const [col, type] of newCols) {
    await p.query(`ALTER TABLE creators ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(() => {});
  }
  // One-time migration: instantly_sent_at used to be set by any CSV/xlsx download, so we
  // cannot tell which of these were really emailed. Label them and keep them off new lists.
  await p.query(`
    UPDATE creators SET contact_status = 'legacy-contacted', exported_at = instantly_sent_at,
      status_updated_at = NOW()
    WHERE instantly_sent_at IS NOT NULL AND contact_status IS NULL
  `).then(r => { if (r.rowCount) log(`Migration: ${r.rowCount} rows labelled legacy-contacted`); })
    .catch(e => log(`Migration error: ${e.message}`));
  await p.query(`CREATE TABLE IF NOT EXISTS seen_channels (channel_id TEXT PRIMARY KEY)`);
  await p.query(`CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT)`);
  log('Database ready');
}

const memoryResults = [];
const memorySeenChannels = new Set();
const memoryInstantlySent = new Set();
const memoryManualSentBatches = new Set();

async function saveCreator(row) {
  const p = getPool();
  if (!p) { memoryResults.push(row); return; }
  try {
    await p.query(`
      INSERT INTO creators
        (first_name,handle,email,avg_views,avg_likes,avg_comments,like_ratio,comment_ratio,
         subscriber_count,niche,channel_url,date_found,batch_number,video_count,total_views,
         country,upload_frequency,thumbnail_url,ideal_price,last_posted_at,commission_score,
         channel_id,median_views,shorts_share,latest_video_title,latest_video_url,latest_video_at,
         top_video_title,top_video_url,videos_checked_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,
              $22,$23,$24,$25,$26,$27,$28,$29,NOW())
      ON CONFLICT (handle) DO UPDATE SET
        avg_views=EXCLUDED.avg_views, avg_likes=EXCLUDED.avg_likes,
        avg_comments=EXCLUDED.avg_comments, like_ratio=EXCLUDED.like_ratio,
        comment_ratio=EXCLUDED.comment_ratio, subscriber_count=EXCLUDED.subscriber_count,
        email=COALESCE(EXCLUDED.email, creators.email),
        niche=EXCLUDED.niche, video_count=EXCLUDED.video_count,
        total_views=EXCLUDED.total_views, country=EXCLUDED.country,
        upload_frequency=EXCLUDED.upload_frequency, thumbnail_url=EXCLUDED.thumbnail_url,
        ideal_price=EXCLUDED.ideal_price, last_posted_at=EXCLUDED.last_posted_at,
        commission_score=EXCLUDED.commission_score,
        channel_id=COALESCE(EXCLUDED.channel_id, creators.channel_id),
        median_views=EXCLUDED.median_views, shorts_share=EXCLUDED.shorts_share,
        latest_video_title=EXCLUDED.latest_video_title, latest_video_url=EXCLUDED.latest_video_url,
        latest_video_at=EXCLUDED.latest_video_at, top_video_title=EXCLUDED.top_video_title,
        top_video_url=EXCLUDED.top_video_url, videos_checked_at=NOW()
    `, [row.first_name, row.handle, row.email, row.avg_views, row.avg_likes,
        row.avg_comments, row.like_ratio, row.comment_ratio, row.subscriber_count,
        row.niche, row.channel_url, row.date_found, row.batch_number,
        row.video_count, row.total_views, row.country, row.upload_frequency, row.thumbnail_url,
        row.ideal_price, row.last_posted_at, row.commission_score,
        row.channel_id || null, row.median_views ?? null, row.shorts_share ?? null,
        row.latest_video_title || null, row.latest_video_url || null, row.latest_video_at || null,
        row.top_video_title || null, row.top_video_url || null]);
    invalidateResultsCache();
  } catch (e) { log(`Save error ${row.handle}: ${e.message}`); }
}

async function markSeenBatch(channelIds) {
  const p = getPool();
  for (const id of channelIds) {
    if (!p) { memorySeenChannels.add(id); continue; }
    try { await p.query(`INSERT INTO seen_channels VALUES ($1) ON CONFLICT DO NOTHING`, [id]); }
    catch (e) {}
  }
}

async function getSeenChannels() {
  const p = getPool();
  if (!p) return new Set(memorySeenChannels);
  try {
    const res = await p.query('SELECT channel_id FROM seen_channels');
    return new Set(res.rows.map(r => r.channel_id));
  } catch (e) { return new Set(); }
}

// Short-lived cache so UI polling doesn't hit Postgres on every request
let resultsCache = { rows: null, at: 0 };
const RESULTS_CACHE_TTL = 10_000;

function invalidateResultsCache() { resultsCache = { rows: null, at: 0 }; }

// limit defaults to Infinity = every row. The old hardcoded `LIMIT 10000` in the SQL
// silently dropped the NEWEST rows (ORDER BY is ascending), so exports and the dashboard
// stopped at 10,000 while the table kept growing past it.
// The cache is NOT keyed by limit, so the query must always fetch the FULL table and let
// callers slice in JS. Moving the limit into the SQL would let a small-limit call
// (e.g. /api/results?limit=50) poison the cache for the next full download.
async function getLastResults(limit = Infinity, { fresh = false } = {}) {
  const p = getPool();
  if (!p) return annotateRows(limit === Infinity ? memoryResults.slice() : memoryResults.slice(-limit));
  if (!fresh && resultsCache.rows && Date.now() - resultsCache.at < RESULTS_CACHE_TTL) {
    return resultsCache.rows.slice(0, limit);
  }
  try {
    const res = await p.query('SELECT * FROM creators ORDER BY batch_number ASC, id ASC');
    annotateRows(res.rows);
    resultsCache = { rows: res.rows, at: Date.now() };
    return res.rows.slice(0, limit);
  } catch (e) { return []; }
}

// ─── BEST SCORE ───────────────────────────────────────────────────────────────
// Percentile-ranked composite: avg views 50%, like ratio 25%, comment ratio 25%
function computeBestScores(rows) {
  const pct = (key) => {
    const vals = rows.map(r => Number(r[key]) || 0);
    const sorted = [...vals].sort((a, b) => a - b);
    const firstIdx = new Map();
    sorted.forEach((v, i) => { if (!firstIdx.has(v)) firstIdx.set(v, i); });
    const denom = Math.max(sorted.length - 1, 1);
    return vals.map(v => firstIdx.get(v) / denom);
  };
  const views = pct('avg_views'), likes = pct('like_ratio'), comments = pct('comment_ratio');
  rows.forEach((r, i) => {
    r.best_score = Math.round((0.5 * views[i] + 0.25 * likes[i] + 0.25 * comments[i]) * 100);
  });
  return rows;
}

function sortByBest(rows) {
  computeBestScores(rows);
  return [...rows].sort((a, b) =>
    (b.best_score - a.best_score) || (Number(b.avg_views || 0) - Number(a.avg_views || 0)));
}

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
let liveState = {
  batchNumber: 0, totalFound: 0, lastRunAt: null, isRunning: false,
  progress: { phase: 'Idle', done: 0, total: 0, currentName: '', foundSoFar: 0 },
};

async function loadState() {
  // Always load from state.json first as baseline
  const fileState = loadStateFile();
  if (fileState.lastRunAt) liveState.lastRunAt = fileState.lastRunAt;
  if (fileState.batchNumber) liveState.batchNumber = fileState.batchNumber;
  if (fileState.totalFound) liveState.totalFound = fileState.totalFound;

  const p = getPool();
  if (!p) return; // No DB — state.json is the only persistence
  try {
    const res = await p.query('SELECT key, value FROM app_state');
    for (const row of res.rows) {
      if (row.key === 'manually_sent_batches') {
        try {
          const arr = JSON.parse(row.value);
          if (Array.isArray(arr)) arr.forEach(b => memoryManualSentBatches.add(Number(b)));
        } catch (e) {}
      } else {
        try { liveState[row.key] = JSON.parse(row.value); } catch (e) { liveState[row.key] = row.value; }
      }
    }
  } catch (e) {}
}

function getManualSentBatches() {
  return [...memoryManualSentBatches];
}

async function toggleManualSent(batchNum) {
  const n = Number(batchNum);
  if (memoryManualSentBatches.has(n)) {
    memoryManualSentBatches.delete(n);
  } else {
    memoryManualSentBatches.add(n);
  }
  const arr = [...memoryManualSentBatches];
  const p = getPool();
  if (p) {
    try {
      await p.query(
        `INSERT INTO app_state (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2`,
        ['manually_sent_batches', JSON.stringify(arr)]
      );
    } catch (e) {}
  }
  return memoryManualSentBatches.has(n);
}

async function persistState(updates) {
  Object.assign(liveState, updates);
  // Always write key state fields to state.json for restart resilience
  saveStateFile({
    batchNumber: liveState.batchNumber,
    totalFound: liveState.totalFound,
    lastRunAt: liveState.lastRunAt,
    isRunning: liveState.isRunning,
  });
  const p = getPool();
  if (!p) return;
  for (const [key, value] of Object.entries(updates)) {
    if (key === 'progress') continue; // Don't persist progress to DB — it's fast-changing
    try {
      await p.query(
        `INSERT INTO app_state (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2`,
        [key, JSON.stringify(value)]
      );
    } catch (e) {}
  }
}

function getState() { return { ...liveState }; }

// ─── API KEY MANAGER ──────────────────────────────────────────────────────────
function getApiKeys() {
  const keys = [];
  for (let i = 1; i <= 10; i++) {
    const k = process.env[`YOUTUBE_API_KEY_${i}`];
    if (k && k.trim()) keys.push(k.trim());
  }
  if (process.env.YOUTUBE_API_KEY) {
    const k = process.env.YOUTUBE_API_KEY.trim();
    if (k && !keys.includes(k)) keys.push(k);
  }
  return keys;
}

class KeyManager {
  constructor(keys) {
    this.keys = [...keys];
    this.idx = 0;
    this.exhausted = new Set();
  }

  get current() { return this.keys[this.idx]; }

  rotate() {
    const start = this.idx;
    let next = (this.idx + 1) % this.keys.length;
    while (this.exhausted.has(next) && next !== start) {
      next = (next + 1) % this.keys.length;
    }
    if (this.exhausted.has(next)) return null;
    this.idx = next;
    return this.keys[this.idx];
  }

  markExhausted() {
    log(`Key ${this.idx + 1}/${this.keys.length} exhausted, rotating...`);
    this.exhausted.add(this.idx);
    return this.rotate();
  }

  reset() {
    log('Resetting API key quota tracking');
    this.exhausted.clear();
    this.idx = 0;
  }

  hasKeys() { return this.exhausted.size < this.keys.length; }

  summary() {
    return this.keys.map((k, i) => ({
      n: i + 1,
      exhausted: this.exhausted.has(i),
      key: k.slice(0, 10) + '...',
    }));
  }
}

// ─── YOUTUBE API ──────────────────────────────────────────────────────────────
const YT = 'https://www.googleapis.com/youtube/v3';

async function ytGet(endpoint, params, km) {
  // Separate key-rotation retries from network retries so we always try ALL keys
  let networkRetries = 0;
  const maxNetworkRetries = 2;

  while (true) {
    if (!km.hasKeys()) throw new Error('ALL_KEYS_EXHAUSTED');
    try {
      const res = await axios.get(`${YT}/${endpoint}`, {
        params: { ...params, key: km.current },
        timeout: 20000,
      });
      networkRetries = 0; // reset on success
      return res.data;
    } catch (e) {
      const status = e.response?.status;
      const reason = e.response?.data?.error?.errors?.[0]?.reason;

      // Quota/auth — rotate to next key and retry immediately
      if (status === 403 || status === 429 ||
          reason === 'quotaExceeded' || reason === 'dailyLimitExceeded' || reason === 'forbidden') {
        const next = km.markExhausted();
        if (!next) throw new Error('ALL_KEYS_EXHAUSTED');
        log(`Switched to key ${km.idx + 1}`);
        continue;
      }

      // Bad request — skip
      if (status === 400 || status === 404) return null;

      // Network/transient error — retry same key
      if (!e.response || e.code === 'ECONNRESET' || e.code === 'ETIMEDOUT' || e.code === 'ENOTFOUND') {
        if (networkRetries < maxNetworkRetries) {
          networkRetries++;
          log(`Network error (${e.code || 'unknown'}), retry ${networkRetries}/${maxNetworkRetries}`);
          await sleep(1000 * networkRetries);
          continue;
        }
        return null;
      }

      log(`API error ${status} on ${endpoint}: ${e.response?.data?.error?.message || e.message}`);
      return null;
    }
  }
}

// ─── SEARCH QUERIES ───────────────────────────────────────────────────────────
const SEARCH_QUERIES = [
  // Mental health core
  'mental health tips for women','anxiety relief vlog','depression recovery journey',
  'therapy talk vlog','mental health day in my life','healing journey vlog',
  'mental health awareness creator','emotional wellness vlog','burnout recovery vlog',
  'stress relief routine','panic attack vlog','ocd awareness vlog',
  'mental health check in','mental health journey 2025','mental health routine',
  'coping with anxiety tips','living with depression vlog','social anxiety vlog',
  'mental health for young adults','college mental health vlog',

  // Self care & wellness
  'self care routine aesthetic','self care sunday vlog','self care day in my life',
  'self care reset vlog','morning self care routine','nighttime self care',
  'glow up self care routine','that girl routine','5am morning routine vlog',
  'healthy habits routine','slow morning routine vlog','rest day vlog',
  'mindfulness for beginners','mindfulness daily routine','guided meditation vlog',
  'breathing exercises anxiety','emotional healing journey','inner child healing vlog',
  'shadow work journal vlog','grounding exercises vlog','self compassion vlog',

  // ASMR
  'asmr relaxing sleep sounds','asmr anxiety relief','asmr soft spoken',
  'asmr daily routine','asmr plushies','asmr stuffed animals',
  'asmr cozy vlog','asmr night routine','asmr self care',
  'asmr gentle whispering','asmr tapping sounds','asmr personal attention',

  // Cozy lifestyle
  'cozy vlog aesthetic','cozy day in my life','cozy lifestyle vlog',
  'cozy night routine aesthetic','cottagecore lifestyle vlog','slow living vlog',
  'hygge lifestyle vlog','cozy autumn vlog','cozy winter vlog',
  'soft life lifestyle vlog','cozy apartment life','cozy reading vlog',
  'cozy gaming vlog','cozy study vlog','dark academia vlog',
  'light academia aesthetic vlog','goblincore vlog','fairycore vlog',

  // Kawaii & plush
  'kawaii collection haul','plushie collection vlog','kawaii unboxing',
  'stuffed animal collection','squishmallow collection','sanrio collection haul',
  'cute plushie haul','kawaii room tour','kawaii lifestyle vlog',
  'plushie unboxing asmr','jellycat collection','build a bear vlog',
  'kawaii stationery haul','cute things haul',

  // Neurodivergent
  'adhd vlog day in my life','adhd tips women','living with adhd vlog',
  'autism vlog day in my life','autistic creator lifestyle','adhd and anxiety vlog',
  'neurodivergent lifestyle vlog','adhd productivity vlog','adhd self care routine',
  'adhd hyperfocus vlog','autism acceptance vlog',

  // Chronic illness
  'chronic illness day in my life','chronic pain vlog','invisible illness vlog',
  'fibromyalgia vlog','chronic fatigue vlog','spoonie lifestyle vlog',
  'spoonie self care','endometriosis awareness vlog','ibs vlog lifestyle',
  'autoimmune disease vlog','living with chronic illness',

  // Introvert & solo living
  'introvert vlog day in my life','introvert lifestyle vlog','living alone vlog',
  'solo living aesthetic','apartment alone vlog aesthetic','quiet life vlog',
  'introvert productivity vlog','solitude vlog aesthetic','independent woman vlog',
  'single life vlog aesthetic',

  // Grief & emotional healing
  'grief healing vlog','loss and healing journey','heartbreak recovery vlog',
  'breakup healing vlog','emotional healing journey','toxic relationship recovery',
  'self love journey vlog','attachment healing vlog',

  // Beauty & makeup
  'soft girl makeup tutorial','natural makeup look tutorial','no makeup makeup vlog',
  'drugstore makeup tutorial','makeup for beginners routine','grwm makeup vlog',
  'soft makeup aesthetic tutorial','clean girl makeup routine',
  'dewy skin makeup tutorial','everyday makeup routine',
  'skincare morning routine vlog','skincare nighttime routine',
  'glass skin routine vlog','acne skincare routine','sensitive skin routine',
  'gua sha routine','facial massage routine',

  // Hair & nails
  'natural hair care routine','curly hair routine','protective styles vlog',
  'nail art tutorial beginner','soft nail art ideas','minimal nail art',

  // Fashion & aesthetic
  'aesthetic outfits ideas vlog','soft girl outfit ideas','coquette aesthetic outfits',
  'cottagecore fashion vlog','dark academia outfits','thrift flip fashion',
  'fashion haul aesthetic vlog','outfit of the day aesthetic','vintage fashion haul',
  'y2k fashion vlog','sustainable fashion vlog','capsule wardrobe vlog',
  'slow fashion vlog',

  // Spiritual & tarot
  'tarot reading for healing','daily tarot pull vlog','tarot for beginners',
  'astrology self care reading','manifestation morning routine',
  'law of attraction vlog','crystals for anxiety vlog','spiritual awakening vlog',
  'shadow work journal vlog','spiritual self care routine','birth chart vlog',
  'angel numbers vlog',

  // Books & journaling
  'reading vlog aesthetic','cozy book recommendations','bullet journal setup',
  'journaling for anxiety vlog','gratitude journal routine','booktok recommendations',
  'book unboxing haul','journal with me vlog','stationery haul aesthetic',
  'desk setup aesthetic vlog',

  // Hobbies & crafts
  'crochet for beginners vlog','crochet vlog aesthetic','knitting vlog cozy',
  'embroidery vlog aesthetic','paint with me vlog','pottery aesthetic vlog',
  'art vlog aesthetic','sketchbook tour vlog','diy crafts aesthetic',
  'candle making vlog','watercolor vlog','journaling art vlog',

  // Gentle fitness
  'gentle yoga anxiety','yoga for mental health','pilates for beginners vlog',
  'home workout calm','walking for mental health vlog','movement vlog aesthetic',
  'stretching routine morning',

  // Food & comfort
  'comfort food vlog aesthetic','cozy cooking vlog','meal prep vlog aesthetic',
  'healthy comfort recipes','baking vlog aesthetic','matcha vlog',
  'coffee routine vlog','cafe study vlog',

  // Productivity & mindset
  'soft productivity vlog','morning routine that girl','night routine aesthetic',
  'productivity vlog aesthetic','vision board vlog','goal setting vlog',
  'study with me vlog','productive day aesthetic',

  // College & young adult
  'college vlog anxiety','student mental health vlog','college day in my life aesthetic',
  'dorm room tour aesthetic','adulting vlog','quarter life crisis vlog',
  'first apartment vlog',

  // Community & relationships
  'friendship vlog aesthetic','online community vlog','people pleasing recovery',
  'codependency healing vlog','boundaries vlog',

  // Additional discovery
  'small creator vlog aesthetic','slow youtube vlog','cozy content creator',
  'wellness vlog 2025','healing vlog 2025','authentic vlog lifestyle',

  // Nano/micro creator discovery — commission-likely
  'ugc creator mental health','ugc content creator cozy','nano influencer vlog',
  'micro influencer lifestyle vlog','small youtuber mental health',
  'growing youtube channel mental health','new youtuber anxiety vlog',
  'small channel cozy vlog','small channel kawaii vlog',
  'brand collab mental health creator','affiliate creator mental health',
  'commission collab creator vlog','small creator brand deals vlog',
  'ugc mental health content','ugc cozy aesthetic content',
  'content creator journey mental health','youtube growth vlog mental health',
  'youtube journey small creator cozy','mental health creator 2025',
  'cozy creator 2025 vlog','healing content creator 2025',
  'small youtuber neurodivergent','adhd small creator vlog',
  'anxiety creator vlog 2025','depression awareness small channel',
  'self love content creator vlog','soft life small creator',
  'kawaii small channel vlog','plushie content creator vlog',
  'stuffed animal collector vlog small','squishmallow small channel',
  'emotional support plushie vlog','comfort content creator vlog',

  // -- Expansion: terms aimed at the four SAVED niches (mental health,
  // neurodivergent, emotional healing, chronic illness). getNiche() is
  // first-match-wins and tests asmr / spiritual / kawaii BEFORE those, so
  // these deliberately avoid that vocabulary.

  // neurodivergent - adhd
  'adhd morning routine', 'adhd night routine', 'adhd cleaning motivation', 'adhd time blindness',
  'adhd paralysis vlog', 'adhd burnout recovery', 'adhd diagnosis story adult', 'diagnosed with adhd at 25',
  'adhd in women symptoms', 'late diagnosed adhd woman', 'adhd medication vlog', 'adhd executive dysfunction',
  'adhd body doubling', 'adhd task initiation', 'adhd overstimulated', 'adhd sensory overload',
  'adhd emotional regulation', 'adhd hygiene routine', 'adhd laundry system', 'adhd doom box',
  'adhd revenge bedtime procrastination', 'adhd student vlog', 'adhd work from home', 'adhd friendly cleaning',
  'adhd rejection sensitivity', 'adhd meal prep struggle', 'adhd tips that actually work', 'adhd routine that works',
  'adhd apartment tour', 'adhd organization system', 'adhd brain dump journal', 'adhd and depression vlog',
  'unmedicated adhd vlog', 'adhd woman in her 20s', 'adhd mom vlog', 'adhd tax vlog',

  // neurodivergent - autism
  'autistic burnout recovery', 'autistic masking vlog', 'late diagnosed autistic woman', 'autism diagnosis story adult',
  'autistic meltdown vlog', 'autistic shutdown vlog', 'autistic stimming vlog', 'sensory friendly routine',
  'autistic special interest vlog', 'autism and adhd audhd', 'audhd vlog', 'audhd woman',
  'autistic girl day in my life', 'autism social battery', 'autistic unmasking journey', 'sensory overload grocery store',
  'autistic safe foods', 'autistic sensory kit', 'noise sensitivity vlog', 'autistic adult routine',
  'autism acceptance creator', 'autistic comfort items', 'stim toys vlog', 'weighted blanket anxiety',
  'autistic burnout vs depression', 'level 1 autism vlog', 'autistic woman diagnosed late', 'neurodivergent morning routine',
  'neurodivergent burnout vlog', 'neurodivergent creator day in my life', 'neurodivergent self care routine', 'neurodivergent friendly home',

  // mental health - anxiety
  'high functioning anxiety vlog', 'anxiety morning routine', 'anxiety attack what it feels like', 'health anxiety vlog',
  'generalized anxiety disorder vlog', 'anxiety and overthinking', 'nervous system regulation vlog', 'nervous system reset routine',
  'somatic exercises anxiety', 'vagus nerve exercises anxiety', 'grounding techniques panic attack', 'box breathing anxiety',
  'panic disorder vlog', 'agoraphobia vlog', 'social anxiety exposure', 'driving anxiety vlog',
  'anxiety at work vlog', 'anticipatory anxiety vlog', 'anxiety spiral vlog', 'calming my anxiety routine',
  'what anxiety actually feels like', 'how i manage my anxiety', 'anxiety honest vlog', 'anxiety relief that actually works',
  'hypervigilance vlog', 'derealization anxiety', 'dissociation vlog', 'emotional dysregulation vlog',

  // mental health - depression
  'depression nest cleaning', 'depression room clean with me', 'everything shower depression', 'bare minimum routine depression',
  'low energy day routine', 'surviving a depressive episode', 'functional depression vlog', 'seasonal depression vlog',
  'winter blues routine', 'depression recovery honest vlog', 'living alone with depression', 'depression and motivation vlog',
  'getting out of bed depression', 'depression meal ideas', 'antidepressant vlog', 'starting antidepressants',
  'ssri side effects vlog', 'depression awareness creator', 'bed rotting depression', 'depression naps vlog',

  // mental health - therapy and treatment
  'therapy homework vlog', 'first therapy session', 'starting therapy at 25', 'emdr therapy vlog',
  'cbt for anxiety vlog', 'dbt skills vlog', 'trauma therapy journey', 'therapist recommended habits',
  'in therapy for years vlog', 'therapy is expensive vlog', 'affordable therapy tips', 'mental health resources us',
  'mental health hospital vlog', 'psych ward experience', 'intensive outpatient program vlog', 'mental health day off work',
  'calling in sick mental health', 'medical gaslighting mental health', 'finding a therapist vlog', 'therapy talk honest',

  // mental health - burnout, ocd, and other
  'burnout recovery routine', 'compassion fatigue vlog', 'caregiver burnout vlog', 'work burnout quit vlog',
  'job burnout recovery', 'hustle culture burnout', 'nurse burnout vlog', 'teacher burnout vlog',
  'ocd intrusive thoughts vlog', 'ocd compulsions vlog', 'pure o ocd vlog', 'contamination ocd vlog',
  'ptsd healing vlog', 'cptsd healing journey', 'complex trauma healing', 'childhood trauma healing vlog',
  'bipolar 2 vlog', 'bpd healing vlog', 'eating disorder recovery vlog', 'body image healing vlog',
  'intuitive eating recovery', 'postpartum anxiety vlog', 'pmdd mental health', 'menstrual mental health',
  'overstimulated mom vlog', 'mom burnout vlog', 'loneliness vlog', 'lonely in my 20s',
  'feeling behind in life', 'imposter syndrome vlog', 'perfectionism recovery', 'people pleasing anxiety',
  'mental health check in with me', 'nobody talks about anxiety', 'mental health honest vlog', 'unfiltered mental health vlog',
  'raw mental health vlog', 'mental health real talk', 'mental illness stigma vlog', 'mental health awareness month vlog',

  // emotional healing - grief
  'grief journey vlog', 'grieving a parent vlog', 'losing my mom grief', 'losing my dad vlog',
  'grief anniversary vlog', 'pet loss grief vlog', 'miscarriage grief vlog', 'anticipatory grief vlog',
  'grief and anxiety', 'widow vlog healing', 'grief in your 20s', 'sudden loss grief vlog',
  'first year of grief', 'grief support vlog', 'sibling loss vlog', 'grief comfort items',

  // emotional healing - breakups and relationships
  'breakup recovery routine', 'healing after a breakup vlog', 'no contact healing', 'situationship recovery',
  'almost relationship grief', 'getting over someone vlog', 'post breakup glow up mental health', 'divorce healing vlog',
  'toxic relationship survivor', 'leaving a toxic relationship', 'narcissistic abuse recovery', 'emotional abuse recovery vlog',
  'trauma bond healing', 'codependency recovery vlog', 'boundaries with family vlog', 'estranged from family vlog',
  'no contact with parents', 'family estrangement healing', 'childhood emotional neglect', 'emotionally immature parents',
  'anxious attachment healing', 'avoidant attachment vlog', 'fearful avoidant healing', 'friendship breakup grief',
  'losing friends in your 20s', 'outgrowing friends vlog', 'self abandonment healing', 'reparenting myself vlog',
  'self forgiveness vlog', 'letting go healing vlog', 'talking stage anxiety', 'dating with anxiety vlog',

  // chronic illness
  'chronic illness morning routine', 'chronic illness flare up vlog', 'chronic pain flare day', 'spoonie day in my life',
  'spoon theory explained', 'chronic fatigue syndrome vlog', 'me cfs vlog', 'long covid vlog',
  'long covid recovery', 'pots syndrome vlog', 'pots flare up', 'dysautonomia vlog',
  'eds hypermobility vlog', 'hypermobile eds day in my life', 'fibromyalgia flare up', 'fibromyalgia pain management',
  'endometriosis flare vlog', 'endo belly vlog', 'pcos and mental health', 'autoimmune flare vlog',
  'lupus day in my life', 'rheumatoid arthritis vlog', 'hashimotos vlog', 'crohns disease vlog',
  'ulcerative colitis vlog', 'ibd day in my life', 'celiac disease vlog', 'ms diagnosis vlog',
  'multiple sclerosis vlog', 'chronic migraine routine', 'vestibular migraine vlog', 'invisible disability vlog',
  'disabled creator vlog', 'ambulatory wheelchair user vlog', 'mobility aid vlog', 'cane user vlog',
  'chronic illness bed rotting', 'bed bound day vlog', 'hospital day vlog chronic', 'infusion day vlog',
  'medical gaslighting vlog', 'chronic illness diagnosis journey', 'chronic illness and anxiety', 'chronic illness mental health',
  'chronic illness comfort items', 'chronic illness care package', 'pacing chronic illness', 'chronic illness accommodations',
  'working with chronic illness', 'chronic illness college vlog', 'chronic illness self care', 'chronic illness rest day',

  // format variants anchored to the saved niches
  'day in my life anxiety', 'day in the life depression', 'week in my life mental health', 'night routine anxiety',
  'morning routine depression', 'clean with me anxiety', 'reset day mental health', 'rot day vlog mental health',
  'comfort items anxiety', 'comfort object adult anxiety', 'emotional support items vlog', 'things that help my anxiety',
  'what helps my depression', 'mental health must haves', 'anxiety toolkit vlog', 'coping skills vlog',
  'low spoons day', 'survival mode vlog', 'hard day vlog mental health', 'bad mental health day vlog',
  'recovery era vlog', 'healing era vlog', 'soft healing vlog', 'gentle routine mental health',
  'slow day mental health', 'rest is productive vlog', 'doing the bare minimum vlog', 'surviving not thriving vlog',
];

// Deduplicate
const QUERIES = [...new Set(SEARCH_QUERIES)];

// ─── NICHE DETECTION ──────────────────────────────────────────────────────────
function getNiche(title = '', description = '', keywords = '') {
  const text = (title + ' ' + description + ' ' + keywords).toLowerCase();
  if (/asmr/.test(text)) return 'asmr';
  if (/mental health|anxiety|depression|therapy|mindful|wellness|healing|burnout|panic|ocd/.test(text)) return 'mental health';
  if (/tarot|astrology|spiritual|manifestation|zodiac|crystal|witch|law of attraction|shadow work|angel number/.test(text)) return 'spiritual';
  if (/kawaii|plush|plushie|squishmallow|stuffed animal|sanrio|jellycat|build a bear/.test(text)) return 'kawaii/plush';
  if (/adhd|autism|neurodivergent|autistic/.test(text)) return 'neurodivergent';
  if (/chronic|spoonie|fibromyalgia|endometriosis|autoimmune|invisible illness/.test(text)) return 'chronic illness';
  if (/cottagecore|cottage|fairy|goblin|dark academia|light academia/.test(text)) return 'aesthetic niche';
  if (/cozy|hygge|slow living|soft life/.test(text)) return 'cozy lifestyle';
  if (/introvert|living alone|solo living|solitude/.test(text)) return 'introvert lifestyle';
  if (/grief|heartbreak|breakup|toxic relationship|codependency/.test(text)) return 'emotional healing';
  if (/makeup|beauty|skincare|skin care|nail|grwm|get ready|gua sha/.test(text)) return 'beauty';
  if (/hair care|curly hair|natural hair/.test(text)) return 'hair care';
  if (/fashion|outfit|ootd|style|haul|thrift|capsule wardrobe/.test(text)) return 'fashion';
  if (/book|reading|booktok|booktube/.test(text)) return 'books';
  if (/journal|bullet journal|gratitude|stationery/.test(text)) return 'journaling';
  if (/crochet|knit|embroid|craft|sewing|pottery|paint|art vlog|watercolor/.test(text)) return 'crafts & art';
  if (/yoga|pilates|stretch|gentle fitness|movement/.test(text)) return 'gentle fitness';
  if (/cook|bake|recipe|food|meal prep|matcha|coffee|cafe/.test(text)) return 'food & cooking';
  if (/self.?care|self love|glow up|that girl/.test(text)) return 'self care';
  if (/productiv|morning routine|night routine|vision board|goal setting/.test(text)) return 'productivity';
  if (/college|student|university|dorm|adulting/.test(text)) return 'student life';
  if (/anime|ghibli|manga/.test(text)) return 'anime';
  if (/pet|cat vlog|dog vlog|kitten/.test(text)) return 'pets';
  return 'lifestyle';
}

// ─── COMMISSION SCORE ────────────────────────────────────────────────────────
// 0–100 signal: how likely is this creator to accept an affiliate/commission deal?
// Higher = nano audience, high engagement, no existing sponsors, email in bio.
function computeCommissionScore({ subscriberCount, commentRatio, likeRatio, email, uploadFrequency, niche, description }) {
  let score = 0;

  // Subscriber tier — nano/micro are most commission-hungry (0-40 pts)
  if (subscriberCount < 5000)        score += 40;
  else if (subscriberCount < 25000)  score += 32;
  else if (subscriberCount < 75000)  score += 22;
  else if (subscriberCount < 150000) score += 10;

  // Engagement depth — comments signal loyal community, not passive scrollers (0-30 pts)
  if (commentRatio >= 0.025)      score += 30;
  else if (commentRatio >= 0.012) score += 22;
  else if (commentRatio >= 0.006) score += 14;
  else if (commentRatio >= 0.002) score += 6;

  // Email in bio — actively seeking brand partnerships (0-15 pts)
  if (email && email !== 'Not listed') score += 15;

  // Active uploader — not dormant (0-10 pts)
  if (uploadFrequency >= 4)      score += 10;
  else if (uploadFrequency >= 2) score += 7;
  else if (uploadFrequency >= 1) score += 4;

  // No existing sponsorship language — not locked into exclusivity deals (0-10 pts)
  const desc = (description || '').toLowerCase();
  const sponsorSignals = /sponsored by|this video is sponsored|in partnership with|paid partnership|#ad\b|#sponsored\b|use code \w+ for|affiliate link|amazon storefront/;
  if (!sponsorSignals.test(desc)) score += 10;

  // Niche alignment — Bemellou's natural territory converts better on commission (0-5 pts)
  const commissionNiches = ['mental health', 'neurodivergent', 'cozy lifestyle', 'kawaii/plush', 'asmr', 'emotional healing', 'chronic illness', 'introvert lifestyle'];
  if (commissionNiches.includes(niche)) score += 5;

  return Math.min(100, Math.round(score));
}

// ─── VIDEO ANALYSIS ──────────────────────────────────────────────────────────
// Works on a videos.list response requested with part 'snippet,statistics,contentDetails'.
// videos.list costs 1 quota unit regardless of parts, so titles and durations are free.
const SHORT_MAX_SECONDS = 180; // Shorts can run up to 3 minutes

function isoDurationSeconds(iso = '') {
  const m = /^P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/.exec(iso || '');
  if (!m) return 0;
  return (+m[1] || 0) * 86400 + (+m[2] || 0) * 3600 + (+m[3] || 0) * 60 + (+m[4] || 0);
}

function median(nums) {
  if (!nums.length) return 0;
  const s = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

function analyzeVideos(items = []) {
  const vids = items.map(v => ({
    id: v.id,
    title: v.snippet?.title || '',
    publishedAt: v.snippet?.publishedAt || '',
    seconds: isoDurationSeconds(v.contentDetails?.duration),
    views: parseInt(v.statistics?.viewCount || 0),
    likes: parseInt(v.statistics?.likeCount || 0),
    comments: parseInt(v.statistics?.commentCount || 0),
  }));
  if (!vids.length) return null;
  const n = vids.length;
  const sum = k => vids.reduce((s, v) => s + v[k], 0);
  const avgViews = sum('views') / n;
  const longForm = vids.filter(v => v.seconds > SHORT_MAX_SECONDS);
  const byNewest = [...vids].sort((a, b) => String(b.publishedAt).localeCompare(String(a.publishedAt)));
  const latest = byNewest[0];
  // The video to reference in outreach: best-performing recent long-form upload, else the latest.
  const top = [...(longForm.length ? longForm : vids)].sort((a, b) => b.views - a.views)[0];
  const url = v => (v && v.id ? `https://www.youtube.com/watch?v=${v.id}` : null);
  return {
    avgViews,
    avgLikes: sum('likes') / n,
    avgComments: sum('comments') / n,
    medianViews: median(vids.map(v => v.views)),
    shortsShare: parseFloat(((n - longForm.length) / n).toFixed(2)),
    latestTitle: latest?.title || null,
    latestUrl: url(latest),
    latestAt: latest?.publishedAt || null,
    topTitle: top?.title || null,
    topUrl: url(top),
    titles: vids.map(v => v.title),
  };
}

// ─── FIRE SCORE (v1) ─────────────────────────────────────────────────────────
// 0-100, four 25-point parts. Computed on read, so tuning these constants never needs a
// migration. Pre-deal, Economics is an estimate; it becomes real once post results exist.
// US gate: channel country is self-declared, NOT audience geography. Only a verified share
// (from the creator's own analytics panel, entered by hand) counts as verified.
const FIRE = {
  labelFire: 75, labelWatch: 60,           // >= 75 FIRE, 60-74 WATCH, < 60 CUT (CLAUDE.md)
  minVerifiedUsShare: 50,                  // verified US share below this forces CUT
  pillarWords: /anxiety|panic|depress|lonel|burnout|adhd|autis|neurodivergent|overthink|therapy|healing|grief|breakup|situationship|comfort|cozy|plush|trauma|chronic|spoonie|intrusive|nervous system|overstimulat|mental health/,
  impactLikeFull: 0.06, impactCommentFull: 0.012,
  reachGoodRatio: 0.2, reachOkRatio: 0.1, reachLowRatio: 0.05,
  affordablePrice: 300, stretchPrice: 1000,
};
const FIRE_NICHES_ADJACENT = new Set(['cozy lifestyle', 'kawaii/plush', 'introvert lifestyle', 'journaling', 'self care']);

function computeFire(r, extraText = '') {
  const niche = r.niche || '';
  const text = [r.top_video_title, r.latest_video_title, extraText].filter(Boolean).join(' ').toLowerCase();
  const hits = (text.match(new RegExp(FIRE.pillarWords.source, 'g')) || []).length;
  const F = (CAMPAIGN_NICHES.has(niche) ? 15 : FIRE_NICHES_ADJACENT.has(niche) ? 8 : 0) + Math.min(10, hits * 4);

  const lr = Number(r.like_ratio) || 0, cr = Number(r.comment_ratio) || 0;
  const I = Math.round(Math.min(1, lr / FIRE.impactLikeFull) * 13 + Math.min(1, cr / FIRE.impactCommentFull) * 12);

  const subs = Number(r.subscriber_count) || 0;
  const med = Number(r.median_views) || Number(r.avg_views) || 0;
  const avg = Number(r.avg_views) || 0;
  const ratio = subs > 0 ? med / subs : 0;
  let R = ratio >= FIRE.reachGoodRatio ? 12 : ratio >= FIRE.reachOkRatio ? 8 : ratio >= FIRE.reachLowRatio ? 4 : 0;
  const consistency = avg > 0 ? med / avg : 0;
  R += consistency >= 0.6 ? 6 : consistency >= 0.4 ? 3 : 0;
  const lastAt = r.latest_video_at || r.last_posted_at;
  const days = lastAt ? (Date.now() - new Date(lastAt).getTime()) / 86400000 : Infinity;
  R += days <= 30 ? 7 : days <= 90 ? 3 : 0;

  const price = Number(r.ideal_price) || 0;
  const E = Math.round((Number(r.commission_score) || 0) * 0.2)
    + (price > 0 && price <= FIRE.affordablePrice ? 5 : price <= FIRE.stretchPrice ? 3 : 0);

  const score = Math.max(0, Math.min(100, F + I + R + E));

  const verified = r.us_share_verified !== null && r.us_share_verified !== undefined && r.us_share_verified !== '';
  const usStatus = verified ? 'verified' : r.country === 'US' ? 'self-declared' : 'not-us-or-unknown';
  let label = score >= FIRE.labelFire ? 'FIRE' : score >= FIRE.labelWatch ? 'WATCH' : 'CUT';
  let gate = null;
  if (verified && Number(r.us_share_verified) < FIRE.minVerifiedUsShare) { label = 'CUT'; gate = 'verified US share below 50%'; }
  else if (usStatus === 'not-us-or-unknown') { label = 'CUT'; gate = 'channel not US'; }

  return { fire_score: score, fire_label: label, fire_parts: { F, I, R, E }, us_status: usStatus, fire_gate: gate };
}

// Outreach opener that names a specific video (CLAUDE.md Critical Rule 2).
// Creator-facing copy: changes go through /review first.
// Rules approved by the marketing review team (2026-09-23): no title = no opener = no send;
// crisis-topic titles are held for a person instead of being quoted in a cold email.
const OPENER_MAX_TITLE = 70; // readability cap we chose, not a measured limit
const OPENER_SAFETY_HOLD = /suicid|self[- ]?harm|kill(ing)? myself|overdos|cutting myself|relapse|crisis|psych ward|988/i;

function cleanTitle(raw = '') {
  let t = String(raw)
    .replace(/"/g, "'")                   // keep the outer quotes intact
    .replace(/\s+/g, ' ')                 // collapse spaces and line breaks
    .trim()
    .replace(/(\s*#[\p{L}\p{N}_]+)+$/u, '') // drop trailing hashtags
    .trim();
  if (t.length > OPENER_MAX_TITLE) {
    const cut = t.slice(0, OPENER_MAX_TITLE);
    const lastSpace = cut.lastIndexOf(' ');
    t = (lastSpace > 20 ? cut.slice(0, lastSpace) : cut).replace(/[\s,.;:!?|/-]+$/, '') + '...';
  }
  return t; // capitalization and emoji left as the creator wrote them (keeps ADHD, OCD)
}

// Returns { opener, hold } where hold is null (sendable) or a reason to keep it off auto-send.
function openerFor(r) {
  const raw = r.top_video_title || r.latest_video_title || '';
  if (OPENER_SAFETY_HOLD.test(raw)) return { opener: null, hold: 'safety: crisis topic in title, review by hand' };
  const title = cleanTitle(raw);
  if (!title) return { opener: null, hold: 'no video title yet (run backfill)' };
  return { opener: `Your video "${title}" is the reason I'm writing.`, hold: null };
}

function buildOpener(r) { return openerFor(r).opener; }

// Adds computed fields to rows read from the DB (mutates and returns rows).
function annotateRows(rows) {
  for (const r of rows) {
    Object.assign(r, computeFire(r));
    const o = openerFor(r);
    r.opener = o.opener;
    r.opener_hold = o.hold;
    r.contact_status = r.contact_status || 'new';
  }
  return rows;
}

// Statuses that mean "do not put this creator on a new send list".
const CONTACTED_STATUSES = new Set(['legacy-contacted', 'exported', 'queued', 'sent', 'replied', 'negotiating', 'deal', 'declined', 'bounced', 'do_not_contact']);
const CONTACT_STATUSES = ['new', ...CONTACTED_STATUSES];
function isContacted(r) { return CONTACTED_STATUSES.has(r.contact_status); }

// ─── EMAIL EXTRACTION ─────────────────────────────────────────────────────────
function extractEmail(text = '') {
  if (!text) return null;
  const matches = text.match(/\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b/g) || [];
  const valid = matches.filter(e =>
    !e.includes('example.com') && !e.includes('youtu') &&
    !e.includes('google') && !e.includes('sentry') &&
    !e.includes('email@') && e.length < 80
  );
  return valid[0] || null;
}

// ─── UTILITIES ────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function fmtNum(n) {
  const x = Number(n);
  if (isNaN(x) || x === 0) return '0';
  if (x >= 1e6) return (x / 1e6).toFixed(1) + 'M';
  if (x >= 1e3) return (x / 1e3).toFixed(1) + 'K';
  return Math.round(x).toString();
}

// ─── CHANNEL METRICS HELPER ──────────────────────────────────────────────────
// Fetches stats for the last 10 videos of a channel and returns computed metrics.
// No quality-filtering applied — caller decides what to do with the numbers.
async function fetchChannelMetrics(ch, km) {
  const plData = await ytGet('playlistItems', {
    part: 'contentDetails',
    playlistId: ch.uploadsPlaylistId,
    maxResults: 10,
  }, km);

  const plItems = plData?.items || [];
  const videoIds = plItems.map(i => i.contentDetails?.videoId).filter(Boolean);
  if (videoIds.length === 0) return null;

  const lastPostedAt = plItems[0]?.contentDetails?.videoPublishedAt || null;

  await sleep(150);

  const vidData = await ytGet('videos', {
    part: 'statistics',
    id: videoIds.join(','),
  }, km);

  const stats = vidData?.items || [];
  if (stats.length === 0) return null;

  const avgViews   = stats.reduce((s, v) => s + parseInt(v.statistics?.viewCount   || 0), 0) / stats.length;
  const avgLikes   = stats.reduce((s, v) => s + parseInt(v.statistics?.likeCount   || 0), 0) / stats.length;
  const avgComments= stats.reduce((s, v) => s + parseInt(v.statistics?.commentCount|| 0), 0) / stats.length;

  const likeRatio    = avgViews > 0 ? avgLikes    / avgViews : 0;
  const commentRatio = avgViews > 0 ? avgComments / avgViews : 0;

  const ageMs = Date.now() - new Date(ch.publishedAt || 0).getTime();
  const ageMonths = Math.max(ageMs / (1000 * 60 * 60 * 24 * 30.5), 1);
  const uploadFrequency = parseFloat((ch.videoCount / ageMonths).toFixed(2));

  const idealPrice = parseFloat((avgViews * 25 / 1000).toFixed(2));

  return {
    avg_views:        Math.round(avgViews),
    avg_likes:        Math.round(avgLikes),
    avg_comments:     Math.round(avgComments),
    like_ratio:       parseFloat(likeRatio.toFixed(4)),
    comment_ratio:    parseFloat(commentRatio.toFixed(4)),
    upload_frequency: uploadFrequency,
    last_posted_at:   lastPostedAt,
    ideal_price:      idealPrice,
    most_recent_date: lastPostedAt, // alias used for recency check in batch
  };
}

// ─── MAIN BATCH ───────────────────────────────────────────────────────────────
const TARGET = 300;

// Search depth. The top-50 relevance results for our query set have been fully mined
// after ~840 batches, so we now follow nextPageToken to reach results 51-150 — channels
// the finder has never fetched. search.list costs 100 units PER PAGE, so query count and
// page depth trade off directly against the same budget:
//   before: 180 queries x 1 page x 100 = 18,000 units
//   now:     60 queries x 3 pages x 100 = 18,000 units
// Sized for 10 API keys running 5 batches/day (~20,000 units/batch, 100,000/day).
// Do NOT raise PAGES_PER_QUERY without lowering QUERIES_PER_BATCH to match, or keys
// will exhaust mid-batch.
const PAGES_PER_QUERY   = 3;
const QUERIES_PER_BATCH = 60;

// App-launch campaign quality gate — high-engagement, closable mental-health creators.
// A creator must clear ALL thresholds AND fall in the mental-health niche cluster.
// View floor removed: this pipeline hunts micro/nano creators (best for commission
// deals), who rarely clear a 20K-view bar. Engagement is the real quality signal.
const QUALIFICATION = {
  minLikeRatio:    0.03,    // ≥ 3% like-to-view rate (healthy micro-creator engagement)
  minCommentRatio: 0.005,   // ≥ 0.5% comment-to-view rate
};
const CAMPAIGN_NICHES = new Set([
  'mental health', 'neurodivergent', 'emotional healing', 'chronic illness',
]);

// Shared campaign-membership predicate — mirrored client-side in public/index.html.
// A stored creator row belongs to the app-launch campaign segment if it clears every gate.
function isCampaignCreator(r) {
  if (!r) return false;
  return CAMPAIGN_NICHES.has(r.niche)
    && r.country === 'US'
    && Number(r.like_ratio)    >= QUALIFICATION.minLikeRatio
    && Number(r.comment_ratio) >= QUALIFICATION.minCommentRatio;
}

async function runBatch(km) {
  const batchNum = (liveState.batchNumber || 0) + 1;
  log(`=== Batch #${batchNum} START | ${km.keys.length} key(s) ===`);
  await persistState({ isRunning: true, batchNumber: batchNum });

  // Campaign mode: re-evaluate previously-seen channels too. saveCreator upserts
  // by channel ID, so re-finding a qualifier refreshes its row rather than duplicating.
  const discoveredIds = []; // ordered list of channel IDs (seen or not)
  const discoveredSet = new Set(); // O(1) dedupe — the id list now reaches ~9,000 entries

  // ── PHASE 1: SEARCH — collect channel IDs ─────────────────────────────────
  // Quota math lives on PAGES_PER_QUERY / QUERIES_PER_BATCH above.
  // Queries are shuffled so every batch explores a different subset.
  const queries = shuffle(QUERIES).slice(0, QUERIES_PER_BATCH);
  log(`Phase 1: ${queries.length} queries x ${PAGES_PER_QUERY} pages (${QUERIES.length} total available)`);
  liveState.progress = { phase: 'Searching', done: 0, total: queries.length, currentName: '', foundSoFar: 0 };

  for (let qi = 0; qi < queries.length; qi++) {
    if (!km.hasKeys()) { log('All keys exhausted in search phase'); break; }

    liveState.progress.done = qi;
    liveState.progress.currentName = queries[qi];

    let pageToken;
    let exhausted = false;

    for (let page = 0; page < PAGES_PER_QUERY; page++) {
      // Re-check per page: a single query now spends up to 300 units, so keys can run
      // out partway through one term.
      if (!km.hasKeys()) { log('All keys exhausted in search phase'); exhausted = true; break; }

      try {
        const data = await ytGet('search', {
          part: 'snippet',
          q: queries[qi],
          type: 'video',
          maxResults: 50,
          relevanceLanguage: 'en',
          order: 'relevance',
          ...(pageToken ? { pageToken } : {}),
        }, km);

        for (const item of data?.items || []) {
          const id = item.snippet?.channelId;
          if (id && !discoveredSet.has(id)) {
            discoveredSet.add(id);
            discoveredIds.push(id);
          }
        }

        pageToken = data?.nextPageToken;
        if (!pageToken) break; // no deeper results for this term
      } catch (e) {
        if (e.message === 'ALL_KEYS_EXHAUSTED') { exhausted = true; break; }
        log(`Search error "${queries[qi]}" p${page + 1}: ${e.message}`);
        break; // skip remaining pages for this term, keep going with the next term
      }

      await sleep(250);
    }

    if (exhausted) break;
  }

  log(`Phase 1 done: ${discoveredIds.length} new channel IDs`);

  // ── PHASE 2: CHANNEL DETAILS — batches of 50 ─────────────────────────────
  log(`Phase 2: Fetching channel details`);
  const channelBatches = chunk(discoveredIds, 50);
  const candidates = [];
  liveState.progress = { phase: 'Fetching channels', done: 0, total: channelBatches.length, currentName: '', foundSoFar: 0 };

  for (let bi = 0; bi < channelBatches.length; bi++) {
    if (!km.hasKeys()) break;
    liveState.progress.done = bi;

    try {
      const data = await ytGet('channels', {
        part: 'snippet,statistics,contentDetails,brandingSettings',
        id: channelBatches[bi].join(','),
        maxResults: 50,
      }, km);

      for (const ch of data?.items || []) {
        const subs = parseInt(ch.statistics?.subscriberCount || 0);
        const videoCount = parseInt(ch.statistics?.videoCount || 0);
        if (subs < 1000 || subs > 150000) continue; // micro/nano only — most likely to do commission

        const desc = ch.snippet?.description || '';
        const brandDesc = ch.brandingSettings?.channel?.description || '';
        const keywords = ch.brandingSettings?.channel?.keywords || '';
        const uploadsPlaylistId = ch.contentDetails?.relatedPlaylists?.uploads || '';
        if (!uploadsPlaylistId) continue;

        candidates.push({
          id: ch.id,
          title: ch.snippet?.title || '',
          customUrl: ch.snippet?.customUrl || '',
          publishedAt: ch.snippet?.publishedAt || '',
          thumbnail: ch.snippet?.thumbnails?.medium?.url || ch.snippet?.thumbnails?.default?.url || '',
          country: ch.snippet?.country || '',
          subscriberCount: subs,
          videoCount,
          totalViews: parseInt(ch.statistics?.viewCount || 0),
          uploadsPlaylistId,
          email: extractEmail(desc) || extractEmail(brandDesc),
          keywords,
          description: desc,
        });
      }
    } catch (e) {
      if (e.message === 'ALL_KEYS_EXHAUSTED') break;
      log(`Channel batch error: ${e.message}`);
    }

    await sleep(200);
  }

  log(`Phase 2 done: ${candidates.length} candidates pass subscriber filter`);

  // ── PHASE 3: VIDEO METRICS — playlistItems (1 unit) + videos batch ────────
  log(`Phase 3: Analyzing video metrics`);
  const creators = [];
  liveState.progress = { phase: 'Analyzing videos', done: 0, total: candidates.length, currentName: '', foundSoFar: 0 };

  for (let ci = 0; ci < candidates.length; ci++) {
    if (!km.hasKeys()) { log('All keys exhausted in video phase'); break; }
    if (creators.length >= TARGET) { log(`Hit target of ${TARGET}`); break; }

    const ch = candidates[ci];
    liveState.progress.done = ci;
    liveState.progress.currentName = ch.title;
    liveState.progress.foundSoFar = creators.length;

    try {
      // Recency pre-check via playlistItems (also used inside fetchChannelMetrics)
      const plData = await ytGet('playlistItems', {
        part: 'contentDetails',
        playlistId: ch.uploadsPlaylistId,
        maxResults: 10,
      }, km);

      const plItems = plData?.items || [];
      const videoIds = plItems.map(i => i.contentDetails?.videoId).filter(Boolean);
      if (videoIds.length < 1) {
        await markSeenBatch([ch.id]);
        await sleep(100);
        continue;
      }

      const mostRecentDate = plItems[0]?.contentDetails?.videoPublishedAt;

      // Fetch video stats + titles + durations (still 1 unit: videos.list cost ignores parts)
      const vidData = await ytGet('videos', {
        part: 'snippet,statistics,contentDetails',
        id: videoIds.join(','),
      }, km);

      const va = analyzeVideos(vidData?.items || []);
      if (!va) { await markSeenBatch([ch.id]); continue; }

      const avgViews    = va.avgViews;
      const avgLikes    = va.avgLikes;
      const avgComments = va.avgComments;

      const likeRatio    = avgViews > 0 ? avgLikes    / avgViews : 0;
      const commentRatio = avgViews > 0 ? avgComments / avgViews : 0;

      // Channel age & upload frequency
      const ageMs = Date.now() - new Date(ch.publishedAt || 0).getTime();
      const ageMonths = Math.max(ageMs / (1000 * 60 * 60 * 24 * 30.5), 1);
      const uploadFrequency = parseFloat((ch.videoCount / ageMonths).toFixed(2));

      const idealPrice = parseFloat((avgViews * 25 / 1000).toFixed(2));

      const handle = ch.customUrl || `channel/${ch.id}`;
      const channelUrl = ch.customUrl
        ? `https://youtube.com/${ch.customUrl}`
        : `https://youtube.com/channel/${ch.id}`;

      const detectedNiche = getNiche(ch.title, ch.description, ch.keywords);

      // App-launch campaign gate: mental-health niche + US audience + high engagement + reach.
      if (
        !CAMPAIGN_NICHES.has(detectedNiche) ||
        ch.country !== 'US' ||
        likeRatio   < QUALIFICATION.minLikeRatio ||
        commentRatio < QUALIFICATION.minCommentRatio
      ) {
        await markSeenBatch([ch.id]);
        await sleep(120);
        continue;
      }

      const commissionScore = computeCommissionScore({
        subscriberCount: ch.subscriberCount,
        commentRatio,
        likeRatio,
        email: ch.email,
        uploadFrequency,
        niche: detectedNiche,
        description: ch.description,
      });

      const creator = {
        first_name: ch.title.split(' ')[0] || ch.title,
        handle,
        email: ch.email || null,
        avg_views:        Math.round(avgViews),
        avg_likes:        Math.round(avgLikes),
        avg_comments:     Math.round(avgComments),
        like_ratio:       parseFloat(likeRatio.toFixed(4)),
        comment_ratio:    parseFloat(commentRatio.toFixed(4)),
        subscriber_count:  ch.subscriberCount,
        niche:             detectedNiche,
        channel_url:       channelUrl,
        date_found:        new Date().toISOString().split('T')[0],
        batch_number:      batchNum,
        video_count:       ch.videoCount,
        total_views:       ch.totalViews,
        country:           ch.country || null,
        upload_frequency:  uploadFrequency,
        thumbnail_url:     ch.thumbnail || null,
        ideal_price:       idealPrice,
        last_posted_at:    mostRecentDate || null,
        commission_score:  commissionScore,
        channel_id:         ch.id,
        median_views:       Math.round(va.medianViews),
        shorts_share:       va.shortsShare,
        latest_video_title: va.latestTitle,
        latest_video_url:   va.latestUrl,
        latest_video_at:    va.latestAt,
        top_video_title:    va.topTitle,
        top_video_url:      va.topUrl,
      };

      await saveCreator(creator);
      await markSeenBatch([ch.id]);
      creators.push(creator);

      log(`✓ [${creators.length}/${TARGET}] ${ch.title} | ${fmtNum(ch.subscriberCount)} subs | ${fmtNum(avgViews)} avg views | commission:${commissionScore} | ${creator.niche}${creator.email ? ' | 📧' : ''}`);

      if (creators.length % 50 === 0) {
        await persistState({ totalFound: (liveState.totalFound || 0) });
      }
    } catch (e) {
      if (e.message === 'ALL_KEYS_EXHAUSTED') break;
      log(`Video error ${ch.title}: ${e.message}`);
    }

    await sleep(150);
  }

  // ── DONE ──────────────────────────────────────────────────────────────────
  const newTotal = (liveState.totalFound || 0) + creators.length;
  await persistState({
    isRunning: false,
    totalFound: newTotal,
    lastRunAt: new Date().toISOString(),
  });
  liveState.progress = { phase: 'Complete', done: creators.length, total: TARGET, currentName: '', foundSoFar: creators.length };

  const emailCount = creators.filter(c => c.email).length;
  log(`=== Batch #${batchNum} DONE: ${creators.length} creators found, ${emailCount} with emails. Total: ${newTotal} ===`);
  log(`Key status: ${JSON.stringify(km.summary())}`);

  return creators;
}

// ─── EXCEL EXPORT ─────────────────────────────────────────────────────────────
// ─── PERSONALIZATION ──────────────────────────────────────────────────────────
async function generatePersonalization(rows) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey || !apiKey.trim()) { log('generatePersonalization: ANTHROPIC_API_KEY not set, skipping'); return rows; }

  const client = new Anthropic.default({ apiKey });

  const input = rows.map((r, i) => ({
    i,
    name: r.first_name || '',
    niche: r.niche || '',
    handle: r.handle || '',
    avg_views: r.avg_views || 0,
  }));

  const prompt = `You are writing personalized outreach data for a comfort plushie brand (Bemellou) targeting mental health / neurodivergent YouTube creators.

For each creator below, generate 3 fields:

- vibe: a single tone descriptor, all lowercase. ONE word or hyphenated word only (e.g. "grounded", "gentle", "soft-spoken", "warm"). NEVER use a comma. If you want to combine two words use "X and Y" format (e.g. "calm and direct"). Never more than 3 words total.
- praise: a short description of what makes their content approach unique, all lowercase, NO full stop at the end. Must be written in third person describing what they do (e.g. "talk about difficult topics without overdramatizing them", "make vulnerability feel safe rather than performative", "normalize conversations people usually avoid"). Never start with "you" or "your". No period at the end.
- looking_forward: a warm personalized sentence starting with "Looking forward to", first letter uppercase, ends with a full stop (e.g. "Looking forward to hearing your thoughts.", "Looking forward to seeing if this aligns.")

Creators:
${JSON.stringify(input, null, 2)}

Respond ONLY with a JSON array, no markdown, no explanation:
[{"i":0,"vibe":"...","praise":"...","looking_forward":"..."},...]`;

  try {
    log(`generatePersonalization: calling Claude for ${rows.length} creators`);
    const message = await client.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    });
    const raw = message.content[0].text.trim().replace(/^```json\s*/i, '').replace(/```\s*$/, '');
    log(`generatePersonalization: raw response length=${raw.length}, preview=${raw.slice(0, 100)}`);
    const parsed = JSON.parse(raw);
    log(`generatePersonalization: parsed ${parsed.length} entries`);
    const enriched = [...rows];
    for (const p of parsed) {
      if (enriched[p.i]) {
        enriched[p.i] = { ...enriched[p.i], vibe: p.vibe, praise: p.praise, looking_forward: p.looking_forward };
      }
    }
    return enriched;
  } catch (e) {
    log(`generatePersonalization error: ${e.message} | stack: ${e.stack}`);
    return rows;
  }
}

const EXCEL_COLS = [
  { key: 'first_name', header: 'Name', width: 18 },
  { key: 'handle', header: 'Handle', width: 25 },
  { key: 'email', header: 'Email', width: 32 },
  { key: 'niche', header: 'Niche', width: 20 },
  { key: 'subscriber_count', header: 'Subscribers', width: 14 },
  { key: 'avg_views', header: 'Avg Views', width: 12 },
  { key: 'avg_likes', header: 'Avg Likes', width: 12 },
  { key: 'avg_comments', header: 'Avg Comments', width: 14 },
  { key: 'like_ratio', header: 'Like Ratio', width: 12 },
  { key: 'comment_ratio', header: 'Comment Ratio', width: 14 },
  { key: 'country', header: 'Country', width: 10 },
  { key: 'upload_frequency', header: 'Uploads/Mo', width: 12 },
  { key: 'total_views', header: 'Total Views', width: 14 },
  { key: 'video_count', header: 'Videos', width: 10 },
  { key: 'channel_url', header: 'Channel URL', width: 45 },
  { key: 'thumbnail_url', header: 'Thumbnail URL', width: 45 },
  { key: 'date_found', header: 'Date Found', width: 12 },
  { key: 'batch_number', header: 'Batch', width: 8 },
  { key: 'commission_score', header: 'Commission Score', width: 16 },
  { key: 'fire_score', header: 'FIRE Score', width: 10 },
  { key: 'fire_label', header: 'FIRE', width: 8 },
  { key: 'us_status', header: 'US Status', width: 16 },
  { key: 'us_share_verified', header: 'US % (verified)', width: 14 },
  { key: 'contact_status', header: 'Contact Status', width: 16 },
  { key: 'opener', header: 'OPENER', width: 60 },
  { key: 'top_video_title', header: 'Video To Reference', width: 50 },
  { key: 'top_video_url', header: 'Video URL', width: 45 },
  { key: 'median_views', header: 'Median Views', width: 13 },
  { key: 'shorts_share', header: 'Shorts Share', width: 12 },
  { key: 'vibe', header: 'VIBE', width: 18 },
  { key: 'praise', header: 'PRAISE', width: 55 },
  { key: 'looking_forward', header: 'LOOKING FORWARD', width: 55 },
];

function buildSheet(rows, cols) {
  const headers = cols.map(c => c.header);
  const data = rows.map((r, i) => cols.map(c => {
    if (c.key === 'rank') return r.rank != null ? r.rank : i + 1;
    const v = r[c.key];
    if (c.key === 'like_ratio' || c.key === 'comment_ratio') return v != null ? parseFloat((v * 100).toFixed(2)) : '';
    return v != null ? v : '';
  }));
  const ws = XLSX.utils.aoa_to_sheet([headers, ...data]);
  ws['!cols'] = cols.map(c => ({ wch: c.width }));
  return ws;
}

function generateExcel(rows) {
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, buildSheet(rows, EXCEL_COLS), 'Creators');
  return wb;
}

// Ranked export: one sheet per group (e.g. Has Email / No Email), with Rank + Best Score
function generateRankedWorkbook(groups) {
  const cols = [
    { key: 'rank', header: 'Rank', width: 6 },
    { key: 'best_score', header: 'Best Score', width: 10 },
    ...EXCEL_COLS,
  ];
  const wb = XLSX.utils.book_new();
  for (const { name, rows } of groups) {
    if (!rows.length) continue;
    XLSX.utils.book_append_sheet(wb, buildSheet(rows, cols), name);
  }
  return wb;
}

// ─── SCHEDULER ────────────────────────────────────────────────────────────────
let batchRunning = false;
const RESULTS_PATH = path.join(__dirname, 'data', 'results.xlsx');

async function executeBatch(keys) {
  if (batchRunning) { log('Batch already running'); return { success: false, message: 'Already running' }; }
  if (!keys || keys.length === 0) return { success: false, message: 'No API keys' };

  batchRunning = true;
  try {
    const km = new KeyManager(keys);
    const creators = await runBatch(km);

    // Save Excel snapshot
    try {
      const all = await getLastResults();
      const wb = generateExcel(all);
      const dir = path.join(__dirname, 'data');
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      XLSX.writeFile(wb, RESULTS_PATH);
    } catch (e) { log('Excel snapshot error: ' + e.message); }

    // Claude personalization is off by default: it wrote generic niche flattery (never a
    // specific video) and costs credit. Video-specific openers are now built for free.
    if (process.env.ENRICH_WITH_CLAUDE === 'true') {
      enrichNewCreators().catch(e => log(`enrichNewCreators failed: ${e.message}`));
    }
    return { success: true, found: creators.length };
  } catch (e) {
    log('executeBatch error: ' + e.message);
    await persistState({ isRunning: false });
    liveState.progress = { phase: 'Error', done: 0, total: 0, currentName: e.message, foundSoFar: 0 };
    return { success: false, message: e.message };
  } finally {
    batchRunning = false;
  }
}

// Global key manager instance — shared across scheduled runs
let sharedKm = null;

function getSharedKm() {
  const keys = getApiKeys();
  if (!sharedKm || sharedKm.keys.join() !== keys.join()) {
    sharedKm = new KeyManager(keys);
  }
  return sharedKm;
}

async function testApiKey(key) {
  try {
    const res = await axios.get(`${YT}/search`, {
      params: { part: 'snippet', q: 'test', type: 'video', maxResults: 1, key },
      timeout: 10000,
    });
    return res.status === 200;
  } catch (e) {
    const status = e.response?.status;
    const reason = e.response?.data?.error?.errors?.[0]?.reason;
    if (reason === 'quotaExceeded') return 'quota'; // Valid key, just exhausted
    return false;
  }
}

function startScheduler() {
  initDb().then(async () => {
    await loadState();
    await persistState({ isRunning: false });

    const keys = getApiKeys();
    log(`Scheduler ready. ${keys.length} API key(s) loaded.`);
    if (keys.length === 0) { log('WARNING: No YouTube API keys found!'); return; }

    // Test keys on startup
    for (let i = 0; i < keys.length; i++) {
      const result = await testApiKey(keys[i]);
      log(`Key ${i + 1}: ${result === true ? 'OK' : result === 'quota' ? 'quota exhausted' : 'INVALID or error'}`);
    }

    // Reset key manager quota tracking daily at 08:00 UTC
    cron.schedule('0 8 * * *', () => {
      if (sharedKm) sharedKm.reset();
      log('Daily API key quota reset');
    });

    // Run 5x per day every 5 hours (~100 creators/run, ~500 creators/day)
    cron.schedule('0 0,5,10,15,20 * * *', () => {
      log('Scheduled batch — starting');
      executeBatch(getApiKeys());
    });

    log('Scheduled: 5x daily at 00:00, 05:00, 10:00, 15:00, 20:00 UTC (~600 creators/day)');

    // Smart startup: only run if no batch in the last 20 hours
    const lastRun = liveState.lastRunAt ? new Date(liveState.lastRunAt) : null;
    const hoursSinceLast = lastRun ? (Date.now() - lastRun.getTime()) / 3_600_000 : Infinity;
    if (hoursSinceLast > 4) {
      log(`Last run: ${lastRun ? Math.round(hoursSinceLast) + 'h ago' : 'never'} — running startup batch`);
      executeBatch(getApiKeys());
    } else {
      log(`Last run ${Math.round(hoursSinceLast)}h ago — skipping startup batch (next batch at 00/05/10/15/20 UTC)`);
    }
  }).catch(e => log('DB init error: ' + e.message));
}

// ─── INSTANTLY AI ─────────────────────────────────────────────────────────────
async function markInstantlySent(emails) {
  if (!emails.length) return;
  const p = getPool();
  if (!p) { emails.forEach(e => memoryInstantlySent.add(e)); return; }
  try {
    // Added to an Instantly campaign = queued, not sent: the campaign still has to be launched there.
    await p.query(`UPDATE creators SET instantly_sent_at = NOW(), contact_status = 'queued',
      contact_channel = 'instantly', contacted_at = COALESCE(contacted_at, NOW()), status_updated_at = NOW()
      WHERE email = ANY($1)`, [emails]);
    invalidateResultsCache();
  } catch (e) { log(`markInstantlySent error: ${e.message}`); }
}

async function resetSentLast2Days() {
  const p = getPool();
  const cutoff = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString();
  if (!p) {
    // In-memory mode: remove from memoryInstantlySent for recent creators
    const recent = memoryResults.filter(c => c.date_found && new Date(c.date_found) >= new Date(cutoff));
    recent.forEach(c => { if (c.email) memoryInstantlySent.delete(c.email); });
    return recent.length;
  }
  try {
    const result = await p.query(
      `UPDATE creators SET instantly_sent_at = NULL,
         contact_status = CASE WHEN contact_status IN ('legacy-contacted','exported','queued') THEN NULL ELSE contact_status END
       WHERE date_found >= $1 AND instantly_sent_at IS NOT NULL`,
      [cutoff]
    );
    log(`resetSentLast2Days: cleared ${result.rowCount} creators`);
    invalidateResultsCache();
    return result.rowCount;
  } catch (e) {
    log(`resetSentLast2Days error: ${e.message}`);
    throw e;
  }
}

async function savePersonalization(entries) {
  const p = getPool();
  if (!p) return;
  for (const e of entries) {
    try {
      await p.query(
        `UPDATE creators SET vibe=$1, praise=$2, looking_forward=$3 WHERE handle=$4`,
        [e.vibe, e.praise, e.looking_forward, e.handle]
      );
    } catch (err) { log(`savePersonalization error for ${e.handle}: ${err.message}`); }
  }
  invalidateResultsCache();
}

async function resetEnrichment() {
  const p = getPool();
  if (!p) return 0;
  const { rowCount } = await p.query(`UPDATE creators SET vibe=NULL, praise=NULL, looking_forward=NULL`);
  invalidateResultsCache();
  return rowCount;
}

async function enrichBatch(size = 10) {
  const p = getPool();
  if (!p) return 0;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey || !apiKey.trim()) return 0;
  const { rows: chunk } = await p.query(`SELECT * FROM creators WHERE vibe IS NULL LIMIT $1`, [size]);
  if (!chunk.length) return 0;
  const enriched = await generatePersonalization(chunk);
  await savePersonalization(enriched.filter(r => r.vibe));
  const { rows: remaining } = await p.query(`SELECT COUNT(*) FROM creators WHERE vibe IS NULL`);
  return parseInt(remaining[0].count);
}

async function enrichNewCreators() {
  const p = getPool();
  if (!p) return;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey || !apiKey.trim()) return;
  try {
    const { rows } = await p.query(`SELECT * FROM creators WHERE vibe IS NULL`);
    if (!rows.length) { log('enrichNewCreators: nothing to enrich'); return; }
    log(`enrichNewCreators: enriching ${rows.length} creators in batches of 10`);
    const BATCH = 10;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);
      log(`enrichNewCreators: batch ${Math.floor(i/BATCH)+1}/${Math.ceil(rows.length/BATCH)}`);
      const enriched = await generatePersonalization(chunk);
      await savePersonalization(enriched.filter(r => r.vibe));
      await new Promise(r => setTimeout(r, 500));
    }
    log(`enrichNewCreators: done`);
  } catch (e) { log(`enrichNewCreators error: ${e.message}`); }
}

async function pushToInstantly(creators, apiKey, batchLabel) {
  // Split already-sent from fresh
  const alreadySent = creators.filter(c => isContacted(c) || memoryInstantlySent.has(c.email));
  const fresh = creators.filter(c => !isContacted(c) && !memoryInstantlySent.has(c.email));
  const emailable = fresh.filter(c => c.email && c.email.trim());
  // Send rule (Critical Rule 2): no specific-video opener, no send. Held leads stay 'new'
  // and show on the dashboard with their hold reason for manual review.
  const held = emailable.filter(c => !c.opener || c.opener_hold);
  const withEmail = emailable.filter(c => c.opener && !c.opener_hold);
  if (held.length) log(`Instantly push: holding ${held.length} leads without a sendable opener (no title or safety hold)`);

  if (withEmail.length === 0) {
    return { sent: 0, skipped: fresh.length - emailable.length, held: held.length, alreadySent: alreadySent.length, failed: 0, campaignName: null };
  }

  // Create a new campaign for this push
  const campaignName = `Bemellou - ${batchLabel} - ${new Date().toISOString().slice(0, 10)}`;
  log(`Creating Instantly campaign: "${campaignName}"`);
  const createRes = await fetch('https://api.instantly.ai/api/v1/campaign/create', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ api_key: apiKey, name: campaignName }),
  });
  if (!createRes.ok) {
    const err = await createRes.text();
    throw new Error(`Failed to create Instantly campaign: ${createRes.status} ${err}`);
  }
  const { id: campaignId } = await createRes.json();
  log(`Campaign created: ${campaignId}`);

  const leads = withEmail.map(c => ({
    email: c.email.trim(),
    firstName: c.first_name || '',
    personalization: c.opener,
    custom_variables: {
      channel_url: c.channel_url || '',
      niche: c.niche || '',
      subscribers: String(c.subscriber_count || ''),
      avg_views: String(Math.round(c.avg_views || 0)),
      handle: c.handle || '',
      top_video_title: c.top_video_title || c.latest_video_title || '',
      top_video_url: c.top_video_url || c.latest_video_url || '',
      fire_score: String(c.fire_score ?? ''),
    },
  }));

  // Instantly allows max 100 leads per request — chunk it
  const chunks = [];
  for (let i = 0; i < leads.length; i += 100) chunks.push(leads.slice(i, i + 100));

  const sentEmails = [];
  let sent = 0, failed = 0;
  for (const chunk of chunks) {
    try {
      const res = await fetch('https://api.instantly.ai/api/v1/lead/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          api_key: apiKey,
          campaign_id: campaignId,
          skip_if_in_workspace: true,
          leads: chunk,
        }),
      });
      if (res.ok) {
        sent += chunk.length;
        chunk.forEach(l => sentEmails.push(l.email));
      } else { failed += chunk.length; log(`Instantly chunk failed: ${res.status} ${res.statusText}`); }
    } catch (e) {
      failed += chunk.length;
      log(`Instantly fetch error: ${e.message}`);
    }
  }

  await markInstantlySent(sentEmails);

  log(`Instantly push complete: ${sent} sent, ${alreadySent.length} already sent, ${fresh.length - withEmail.length} skipped (no email), ${failed} failed`);
  return { sent, skipped: fresh.length - emailable.length, held: held.length, alreadySent: alreadySent.length, failed, campaignName };
}

// ─── SINGLE CREATOR LOOKUP ───────────────────────────────────────────────────
// Resolves a YouTube channel URL / @handle / channel-ID to a full metric profile.
// Does NOT apply quality thresholds — returns raw metrics regardless of size.
async function lookupCreator(input) {
  const keys = getApiKeys();
  if (!keys.length) throw new Error('No YouTube API keys configured');
  const km = new KeyManager(keys);

  // Parse input into either a channelId ("UC...") or a handle/username string
  let channelId = null;
  let handle = null;

  const s = (input || '').trim().replace(/\/$/, '');

  // Full URLs
  const channelMatch = s.match(/youtube\.com\/channel\/(UC[\w-]+)/i);
  const handleMatch  = s.match(/youtube\.com\/@?([\w.-]+)/i);

  if (channelMatch) {
    channelId = channelMatch[1];
  } else if (handleMatch) {
    handle = handleMatch[1].replace(/^@/, '');
  } else if (/^UC[\w-]{20,}$/.test(s)) {
    channelId = s;
  } else {
    // Bare handle with or without @
    handle = s.replace(/^@/, '');
  }

  // Build channel query params
  const baseParams = { part: 'snippet,statistics,contentDetails,brandingSettings', maxResults: 1 };
  let chData = null;

  if (channelId) {
    chData = await ytGet('channels', { ...baseParams, id: channelId }, km);
  } else {
    // Try forHandle first (newer API), fall back to forUsername
    chData = await ytGet('channels', { ...baseParams, forHandle: handle }, km);
    if (!chData?.items?.length) {
      chData = await ytGet('channels', { ...baseParams, forUsername: handle }, km);
    }
  }

  const ch = chData?.items?.[0];
  if (!ch) return null;

  const desc        = ch.snippet?.description || '';
  const brandDesc   = ch.brandingSettings?.channel?.description || '';
  const keywords    = ch.brandingSettings?.channel?.keywords || '';
  const uploadsId   = ch.contentDetails?.relatedPlaylists?.uploads || '';
  if (!uploadsId) return null;

  const subs        = parseInt(ch.statistics?.subscriberCount || 0);
  const videoCount  = parseInt(ch.statistics?.videoCount || 0);
  const totalViews  = parseInt(ch.statistics?.viewCount || 0);
  const publishedAt = ch.snippet?.publishedAt || '';
  const customUrl   = ch.snippet?.customUrl || '';
  const title       = ch.snippet?.title || '';
  const country     = ch.snippet?.country || '';
  const thumbnail   = ch.snippet?.thumbnails?.medium?.url || ch.snippet?.thumbnails?.default?.url || '';

  // Build the ch object shape expected by fetchChannelMetrics
  const chObj = { uploadsPlaylistId: uploadsId, publishedAt, videoCount };

  const metrics = await fetchChannelMetrics(chObj, km);
  if (!metrics) return null;

  const channelUrl = customUrl
    ? `https://youtube.com/${customUrl}`
    : `https://youtube.com/channel/${ch.id}`;

  return {
    first_name:       title.split(' ')[0] || title,
    handle:           customUrl || `channel/${ch.id}`,
    email:            extractEmail(desc) || extractEmail(brandDesc) || null,
    avg_views:        metrics.avg_views,
    avg_likes:        metrics.avg_likes,
    avg_comments:     metrics.avg_comments,
    like_ratio:       metrics.like_ratio,
    comment_ratio:    metrics.comment_ratio,
    subscriber_count: subs,
    niche:            getNiche(title, desc, keywords),
    channel_url:      channelUrl,
    video_count:      videoCount,
    total_views:      totalViews,
    country:          country || null,
    upload_frequency: metrics.upload_frequency,
    thumbnail_url:    thumbnail || null,
    ideal_price:      metrics.ideal_price,
    last_posted_at:   metrics.last_posted_at,
  };
}

// ─── CONTACT STATUS ──────────────────────────────────────────────────────────
// The only way a creator becomes "contacted". Downloads never change status.
async function setContactStatus(handles, status, { channel = null, notes = null, usShare } = {}) {
  if (!Array.isArray(handles) || !handles.length) throw new Error('handles required');
  if (!CONTACT_STATUSES.includes(status)) throw new Error(`status must be one of: ${CONTACT_STATUSES.join(', ')}`);
  const p = getPool();
  const dbStatus = status === 'new' ? null : status;
  const touches = ['sent', 'queued', 'replied', 'negotiating', 'deal', 'declined'].includes(status);
  if (!p) {
    memoryResults.filter(r => handles.includes(r.handle)).forEach(r => {
      r.contact_status = dbStatus; if (channel) r.contact_channel = channel; if (notes) r.notes = notes;
      if (usShare !== undefined) r.us_share_verified = usShare;
    });
    return memoryResults.filter(r => handles.includes(r.handle)).length;
  }
  const res = await p.query(`
    UPDATE creators SET
      contact_status = $2,
      contact_channel = COALESCE($3, contact_channel),
      notes = COALESCE($4, notes),
      contacted_at = CASE WHEN $5 THEN COALESCE(contacted_at, NOW()) ELSE contacted_at END,
      us_share_verified = CASE WHEN $6 THEN $7::numeric ELSE us_share_verified END,
      status_updated_at = NOW()
    WHERE handle = ANY($1)`,
    [handles, dbStatus, channel, notes, touches, usShare !== undefined, usShare ?? null]);
  invalidateResultsCache();
  log(`setContactStatus: ${res.rowCount} creators -> ${status}`);
  return res.rowCount;
}

// ─── VIDEO BACKFILL ──────────────────────────────────────────────────────────
// Adds video titles/urls, median views and Shorts share to rows saved before they existed.
// Cost per creator: channels.list (1, only when the channel id is unknown) + playlistItems (1)
// + videos (1). Free quota, but it shares the daily budget with the batch, so it is chunked.
let backfillRunning = false;
async function backfillVideos({ limit = 500, campaignOnly = true } = {}) {
  if (backfillRunning) return { started: false, message: 'Backfill already running' };
  if (batchRunning) return { started: false, message: 'A discovery batch is running, try again after it finishes' };
  const p = getPool();
  if (!p) return { started: false, message: 'No database' };
  const keys = getApiKeys();
  if (!keys.length) return { started: false, message: 'No API keys' };

  const all = await getLastResults(Infinity, { fresh: true });
  let rows = all.filter(r => !r.videos_checked_at);
  if (campaignOnly) rows = rows.filter(isCampaignCreator);
  rows.sort((a, b) => (b.commission_score || 0) - (a.commission_score || 0));
  rows = rows.slice(0, Math.max(1, Math.min(Number(limit) || 500, 3000)));

  backfillRunning = true;
  (async () => {
    const km = new KeyManager(keys);
    let done = 0, failed = 0;
    log(`backfillVideos: starting ${rows.length} creators (campaignOnly=${campaignOnly})`);
    for (const r of rows) {
      if (!km.hasKeys()) { log('backfillVideos: keys exhausted, stopping'); break; }
      try {
        let channelId = r.channel_id || (String(r.handle).startsWith('channel/') ? r.handle.slice(8) : null);
        if (!channelId) {
          const h = String(r.handle).startsWith('@') ? r.handle : '@' + r.handle;
          const cd = await ytGet('channels', { part: 'id', forHandle: h }, km);
          channelId = cd?.items?.[0]?.id || null;
        }
        if (!channelId || !channelId.startsWith('UC')) { failed++; continue; }
        const pl = await ytGet('playlistItems', { part: 'contentDetails', playlistId: 'UU' + channelId.slice(2), maxResults: 10 }, km);
        const ids = (pl?.items || []).map(i => i.contentDetails?.videoId).filter(Boolean);
        if (!ids.length) { failed++; continue; }
        const vd = await ytGet('videos', { part: 'snippet,statistics,contentDetails', id: ids.join(',') }, km);
        const va = analyzeVideos(vd?.items || []);
        if (!va) { failed++; continue; }
        await p.query(`UPDATE creators SET channel_id = $2, median_views = $3, shorts_share = $4,
            latest_video_title = $5, latest_video_url = $6, latest_video_at = $7,
            top_video_title = $8, top_video_url = $9, videos_checked_at = NOW()
          WHERE handle = $1`,
          [r.handle, channelId, Math.round(va.medianViews), va.shortsShare, va.latestTitle, va.latestUrl,
           va.latestAt, va.topTitle, va.topUrl]);
        done++;
        if (done % 50 === 0) log(`backfillVideos: ${done}/${rows.length}`);
      } catch (e) {
        if (e.message === 'ALL_KEYS_EXHAUSTED') { log('backfillVideos: keys exhausted, stopping'); break; }
        failed++;
      }
      await sleep(120);
    }
    invalidateResultsCache();
    log(`backfillVideos: done. updated ${done}, failed ${failed}, of ${rows.length}`);
  })().catch(e => log(`backfillVideos error: ${e.message}`)).finally(() => { backfillRunning = false; });

  return { started: true, queued: rows.length };
}

// Ranked by FIRE first, then the older engagement/views percentile score.
function sortByFire(rows) {
  computeBestScores(rows);
  return [...rows].sort((a, b) =>
    ((b.fire_score || 0) - (a.fire_score || 0)) || (b.best_score - a.best_score));
}

module.exports = {
  startScheduler, executeBatch, getState, getLastResults, generateExcel,
  initDb, RESULTS_PATH, getApiKeys, getLogs, pushToInstantly,
  getManualSentBatches, toggleManualSent, markInstantlySent, resetSentLast2Days,
  generatePersonalization, enrichNewCreators, enrichBatch, resetEnrichment,
  lookupCreator, sortByBest, computeBestScores, generateRankedWorkbook,
  isCampaignCreator, CAMPAIGN_TARGET: TARGET,
  setContactStatus, backfillVideos, sortByFire, isContacted, CONTACT_STATUSES, computeFire, buildOpener,
};
