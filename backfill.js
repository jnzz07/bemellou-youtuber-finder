'use strict';
require('dotenv').config();
const { Pool } = require('pg');
const Anthropic = require('@anthropic-ai/sdk');

const DATABASE_URL = 'postgresql://postgres:YejOzGoYBXIYYYOsWnuiNqFLqyXlUgmv@junction.proxy.rlwy.net:32525/railway';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY; // never hardcode keys

const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, connectionTimeoutMillis: 30000, idleTimeoutMillis: 60000, max: 1 });
const client = new Anthropic.default({ apiKey: ANTHROPIC_API_KEY });

async function generateBatch(rows) {
  const input = rows.map((r, i) => ({
    i, name: r.first_name || '', niche: r.niche || '', handle: r.handle || '',
  }));

  const prompt = `You are writing personalized outreach data for a comfort plushie brand (Bemellou) targeting mental health / neurodivergent YouTube creators.

For each creator, generate 3 fields:
- vibe: 1-2 word tone descriptor (all lowercase, e.g. "grounded", "gentle", "soft-spoken")
- praise: short specific compliment about their content approach (all lowercase, e.g. "talk about difficult topics without overdramatizing them")
- looking_forward: a "looking forward to..." sentence (first letter uppercase, ends with full stop, e.g. "Looking forward to hearing your thoughts.")

Creators:
${JSON.stringify(input)}

Respond ONLY with a JSON array, no markdown:
[{"i":0,"vibe":"...","praise":"...","looking_forward":"..."},...]`;

  const message = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 2048,
    messages: [{ role: 'user', content: prompt }],
  });
  const raw = message.content[0].text.trim().replace(/^```json\s*/i, '').replace(/```\s*$/, '');
  return JSON.parse(raw);
}

async function run() {
  const { rows } = await pool.query(`SELECT id, handle, first_name, niche FROM creators WHERE vibe IS NULL ORDER BY id`);
  console.log(`Found ${rows.length} creators to enrich`);

  const BATCH = 20;
  let done = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    try {
      const results = await generateBatch(chunk);
      for (const r of results) {
        const creator = chunk[r.i];
        if (!creator) continue;
        await pool.query(
          `UPDATE creators SET vibe=$1, praise=$2, looking_forward=$3 WHERE handle=$4`,
          [r.vibe, r.praise, r.looking_forward, creator.handle]
        );
      }
      done += chunk.length;
      console.log(`✓ ${done}/${rows.length} done`);
    } catch (e) {
      console.error(`Batch ${i}-${i+BATCH} failed: ${e.message}`);
    }
  }
  console.log('All done!');
  await pool.end();
}

run().catch(e => { console.error(e); process.exit(1); });
