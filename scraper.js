require('dotenv').config({ path: require('path').resolve(__dirname, '.env') });
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const SEEN_PATH = path.join(__dirname, 'data', 'seen.json');
const LOG_PATH = path.join(__dirname, 'logs', 'scraper.log');

const SEARCH_QUERIES = [
  'mental health comfort',
  'anxiety relief vlog',
  'plushie collection',
  'cozy lifestyle',
  'emotional wellness',
  'kawaii unboxing',
  'self care routine',
  'loneliness support',
  'soft life aesthetic',
  'stuffed animals review',
  'comfort content creator',
  'social anxiety',
  'plush toy review',
  'mental health day in my life',
  'cozy room tour',
  'anxiety tips',
  'cute plush haul',
  'wellness vlog',
  'mental health awareness',
  'comfort zone lifestyle',
  'anxiety vlog',
  'depression storytime',
  'therapy talk',
  'neurodivergent adhd',
  'autism acceptance',
  'burnout recovery',
  'healing journey',
  'mental health check in',
  'coping with anxiety',
  'my mental health story',
  'living with depression',
  'panic attack help',
  'overstimulated adhd',
  'emotional support',
  'lonely gen z',
  'situationship spiral',
  'nervous system regulation',
  'inner child healing',
  'journaling for anxiety',
  'grounding techniques',
];

// App-launch marketing campaign — high-quality, closable mental-health creators
const TARGET = 300;
const QUALIFICATION = {
  minAvgViews: 20000,          // ≥ 20K average views
  minCommentRatio: 0.01,       // ≥ 1% comment-to-view rate
  minLikeRatio: 0.10,          // ≥ 10% like-to-view rate
};

const NICHE_MAP = {
  'mental health': ['mental health', 'anxiety', 'depression', 'therapy', 'emotional', 'loneliness', 'social anxiety', 'wellness'],
  'kawaii/plush': ['plushie', 'plush', 'kawaii', 'stuffed animal', 'cute', 'unboxing'],
  'cozy lifestyle': ['cozy', 'soft life', 'comfort', 'aesthetic', 'room tour'],
  'self-improvement': ['self care', 'self improvement', 'routine', 'habits', 'mindfulness'],
};

function log(message) {
  const line = `[${new Date().toISOString()}] ${message}\n`;
  fs.appendFileSync(LOG_PATH, line);
  console.log(line.trim());
}

function loadSeen() {
  try {
    if (fs.existsSync(SEEN_PATH)) {
      return new Set(JSON.parse(fs.readFileSync(SEEN_PATH, 'utf8')));
    }
  } catch (e) {
    log(`Error loading seen.json: ${e.message}`);
  }
  return new Set();
}

function saveSeen(seen) {
  fs.writeFileSync(SEEN_PATH, JSON.stringify([...seen], null, 2));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function detectNiche(query, channelTitle, channelDescription) {
  const text = `${query} ${channelTitle} ${channelDescription}`.toLowerCase();
  for (const [niche, keywords] of Object.entries(NICHE_MAP)) {
    for (const kw of keywords) {
      if (text.includes(kw)) return niche;
    }
  }
  return 'comfort content';
}

function extractFirstName(title) {
  if (!title) return 'Unknown';
  // Remove common suffixes
  const cleaned = title.replace(/\s*(official|tv|channel|vlog|asmr)$/i, '').trim();
  const parts = cleaned.split(/[\s_-]+/);
  if (parts.length > 0 && parts[0].length > 0) {
    return parts[0];
  }
  return 'Unknown';
}

async function apiCall(url, params, apiKey, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const resp = await axios.get(url, {
        params: { ...params, key: apiKey },
        timeout: 15000,
      });
      return resp.data;
    } catch (err) {
      const status = err.response?.status;
      if (status === 429 || status === 403) {
        const wait = Math.pow(2, attempt + 1) * 1000;
        log(`Rate limited (${status}), backing off ${wait}ms`);
        await sleep(wait);
        if (attempt === retries - 1) throw err;
      } else {
        throw err;
      }
    }
  }
}

async function searchChannels(query, apiKey, maxResults = 20) {
  const data = await apiCall('https://www.googleapis.com/youtube/v3/search', {
    part: 'snippet',
    q: query,
    type: 'video',
    maxResults,
    relevanceLanguage: 'en',
    order: 'relevance',
  }, apiKey);

  if (!data.items) return [];

  // Extract unique channel IDs from video results
  const channelIds = new Set();
  const channels = [];
  for (const item of data.items) {
    const cid = item.snippet.channelId;
    if (!channelIds.has(cid)) {
      channelIds.add(cid);
      channels.push({
        id: cid,
        title: item.snippet.channelTitle,
      });
    }
  }
  return channels;
}

async function getChannelDetails(channelId, apiKey) {
  const data = await apiCall('https://www.googleapis.com/youtube/v3/channels', {
    part: 'snippet,statistics,contentDetails,brandingSettings',
    id: channelId,
  }, apiKey);

  if (!data.items || data.items.length === 0) return null;

  const ch = data.items[0];
  const stats = ch.statistics;
  const snippet = ch.snippet;
  const uploadsPlaylistId = ch.contentDetails?.relatedPlaylists?.uploads;

  return {
    id: channelId,
    title: snippet.title,
    description: snippet.description || '',
    customUrl: snippet.customUrl || '',
    subscriberCount: parseInt(stats.subscriberCount) || 0,
    totalViews: parseInt(stats.viewCount) || 0,
    videoCount: parseInt(stats.videoCount) || 0,
    uploadsPlaylistId,
    email: extractEmail(snippet.description || ''),
  };
}

function extractEmail(text) {
  const match = text.match(/[\w.+-]+@[\w-]+\.[\w.-]+/);
  return match ? match[0] : 'Not listed';
}

async function getLastNVideos(uploadsPlaylistId, apiKey, n = 10) {
  const data = await apiCall('https://www.googleapis.com/youtube/v3/playlistItems', {
    part: 'contentDetails',
    playlistId: uploadsPlaylistId,
    maxResults: n,
  }, apiKey);

  if (!data.items || data.items.length === 0) return [];

  const videoIds = data.items.map(item => item.contentDetails.videoId);

  const videoData = await apiCall('https://www.googleapis.com/youtube/v3/videos', {
    part: 'statistics',
    id: videoIds.join(','),
  }, apiKey);

  if (!videoData.items) return [];

  return videoData.items.map(v => ({
    viewCount: parseInt(v.statistics.viewCount) || 0,
    likeCount: parseInt(v.statistics.likeCount) || 0,
    commentCount: parseInt(v.statistics.commentCount) || 0,
  }));
}

function computeMetrics(videos) {
  if (videos.length === 0) return null;
  const avg = (arr) => Math.round(arr.reduce((a, b) => a + b, 0) / arr.length);
  const avgViews = avg(videos.map(v => v.viewCount));
  const avgLikes = avg(videos.map(v => v.likeCount));
  const avgComments = avg(videos.map(v => v.commentCount));

  return {
    avg_views: avgViews,
    avg_likes: avgLikes,
    avg_comments: avgComments,
    like_ratio: avgViews > 0 ? avgLikes / avgViews : 0,
    comment_ratio: avgViews > 0 ? avgComments / avgViews : 0,
  };
}

function qualifies(metrics) {
  if (!metrics) return { pass: false, reasons: ['No video data'] };
  const reasons = [];

  if (metrics.avg_views < QUALIFICATION.minAvgViews) {
    reasons.push(`avg_views ${metrics.avg_views} < ${QUALIFICATION.minAvgViews}`);
  }
  if (metrics.comment_ratio < QUALIFICATION.minCommentRatio) {
    reasons.push(`comment_ratio ${metrics.comment_ratio.toFixed(4)} < ${QUALIFICATION.minCommentRatio.toFixed(4)}`);
  }
  if (metrics.like_ratio < QUALIFICATION.minLikeRatio) {
    reasons.push(`like_ratio ${metrics.like_ratio.toFixed(4)} < ${QUALIFICATION.minLikeRatio.toFixed(4)}`);
  }

  return { pass: reasons.length === 0, reasons };
}

async function runBatch(apiKey, onProgress) {
  const seen = loadSeen();
  const found = [];
  let queryIndex = 0;
  const maxIterations = SEARCH_QUERIES.length * 50;

  const batchState = {
    isRunning: true,
    processed: 0,
    qualified: 0,
    currentQuery: '',
  };

  log('--- Starting new batch ---');

  while (found.length < TARGET && queryIndex < maxIterations) {
    const query = SEARCH_QUERIES[queryIndex % SEARCH_QUERIES.length];
    batchState.currentQuery = query;
    log(`Searching: "${query}" (iteration ${queryIndex + 1})`);

    try {
      const candidates = await searchChannels(query, apiKey);

      for (const candidate of candidates) {
        if (found.length >= TARGET) break;
        if (seen.has(candidate.id)) continue;

        seen.add(candidate.id);
        batchState.processed++;

        try {
          await sleep(500); // Rate limiting delay

          const details = await getChannelDetails(candidate.id, apiKey);
          if (!details || !details.uploadsPlaylistId) {
            log(`Skipping ${candidate.id}: no uploads playlist`);
            continue;
          }

          await sleep(500);

          const videos = await getLastNVideos(details.uploadsPlaylistId, apiKey);
          if (videos.length === 0) {
            log(`Skipping ${candidate.id}: no videos`);
            continue;
          }

          const metrics = computeMetrics(videos);
          const qual = qualifies(metrics);

          if (qual.pass) {
            const row = {
              first_name: extractFirstName(details.title),
              handle: details.customUrl || candidate.id,
              email: details.email,
              avg_views: metrics.avg_views,
              avg_likes: metrics.avg_likes,
              avg_comments: metrics.avg_comments,
              like_ratio: parseFloat(metrics.like_ratio.toFixed(4)),
              comment_ratio: parseFloat(metrics.comment_ratio.toFixed(4)),
              subscriber_count: details.subscriberCount,
              niche: detectNiche(query, details.title, details.description),
              channel_url: details.customUrl
                ? `https://youtube.com/${details.customUrl}`
                : `https://youtube.com/channel/${candidate.id}`,
              date_found: new Date().toISOString().split('T')[0],
              batch_number: 0, // Will be set by caller
            };
            found.push(row);
            batchState.qualified = found.length;
            log(`QUALIFIED: ${details.title} (${found.length}/${TARGET})`);
          } else {
            log(`Disqualified ${details.title}: ${qual.reasons.join(', ')}`);
          }
        } catch (err) {
          log(`Error processing channel ${candidate.id}: ${err.message}`);
        }
      }
    } catch (err) {
      log(`Error searching "${query}": ${err.message}`);
      if (err.response?.status === 403) {
        log('API quota likely exhausted, stopping batch early');
        break;
      }
    }

    queryIndex++;
    if (onProgress) onProgress(batchState);
  }

  saveSeen(seen);
  batchState.isRunning = false;
  log(`Batch complete: ${found.length} channels found`);
  return found;
}

module.exports = {
  runBatch,
  loadSeen,
  saveSeen,
  SEARCH_QUERIES,
  QUALIFICATION,
  log,
};
