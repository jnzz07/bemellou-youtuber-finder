'use strict';
const https = require('https');

const URL = 'https://bemellou-youtuber-finder-production.up.railway.app/api/enrich';

function post() {
  return new Promise((resolve, reject) => {
    const req = https.request(URL, { method: 'POST', headers: { 'Content-Type': 'application/json' } }, res => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

async function run() {
  let remaining = Infinity;
  let calls = 0;
  while (remaining > 0) {
    try {
      const res = await post();
      remaining = res.remaining ?? 0;
      calls++;
      process.stdout.write(`\r✓ call ${calls} — ${remaining} remaining   `);
      if (remaining === 0) break;
      await new Promise(r => setTimeout(r, 1000));
    } catch (e) {
      process.stdout.write(`\nRetrying after error: ${e.message}\n`);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  console.log('\n\nAll done! Download your CSV now.');
}

run();
