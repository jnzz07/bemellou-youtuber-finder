# Bemellou YouTuber Finder

Auto-discovers YouTube creators matching Bemellou brand criteria (mental health, cozy lifestyle, plush/kawaii culture). Finds 50 new qualifying channels every 24 hours and exports them to a downloadable spreadsheet.

## Setup

### 1. Get a YouTube Data API v3 Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Enable the **YouTube Data API v3** from the API Library
4. Go to **Credentials** > **Create Credentials** > **API Key**
5. Copy the API key

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and paste your API key:

```
YOUTUBE_API_KEY=your_key_here
PORT=3000
```

### 3. Install & Run

```bash
npm install
node server.js
```

### 4. Open the Dashboard

Navigate to **http://localhost:3000** in your browser.

## Features

- **Auto-discovery**: Scrapes YouTube for channels matching brand criteria every 24 hours at 02:00 UTC
- **Qualification checks**: Filters by avg views (>=1,000), comment ratio (>=1:90), like ratio (>=1:11)
- **Deduplication**: Tracks seen channels to never add duplicates
- **Dashboard**: Real-time status, paginated/sortable table, search/filter
- **Export**: Download results as .xlsx spreadsheet

## Changing the Schedule

Edit `scheduler.js` and modify the cron expression in `cron.schedule()`:

```js
cron.schedule('0 2 * * *', ...)  // Current: daily at 02:00 UTC
cron.schedule('0 */12 * * *', ...)  // Every 12 hours
cron.schedule('0 9 * * 1', ...)  // Every Monday at 09:00 UTC
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Current scraper status |
| GET | `/api/download` | Download results.xlsx |
| POST | `/api/trigger` | Manually trigger a scrape run |
| GET | `/api/results` | Last 50 results as JSON |
| GET | `/api/results/all` | All results as JSON |

## Docker

```bash
docker build -t bemellou-finder .
docker run -p 3000:3000 --env-file .env bemellou-finder
```

## API Quota Notes

YouTube Data API v3 free tier provides 10,000 units/day. Each batch of 50 channels uses approximately 250-500 units. The scraper includes exponential backoff on rate limit errors and 500ms delays between requests.
