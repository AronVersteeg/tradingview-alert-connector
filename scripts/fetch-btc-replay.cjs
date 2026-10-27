// Read-only input capture. No credentials, application bootstrap, orders or mail.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { compactIntrusionHistory } = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const output = process.argv[2];
if (!output) throw new Error('Supply an output directory');
const base = 'https://arons-tradingview-alert-connector.onrender.com';
const HOUR = 3600000;
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const manifest = [];
async function read(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(60000), headers: { 'Accept-Encoding': 'gzip' } });
  if (!response.ok) throw new Error(`${response.status} ${url}; stopped without automatic rate-limit retries`);
  const text = await response.text();
  manifest.push({ url, retrievedAt: new Date().toISOString(), bytes: Buffer.byteLength(text),
    sha256: crypto.createHash('sha256').update(text).digest('hex') });
  return JSON.parse(text);
}
async function main() {
  fs.mkdirSync(output, { recursive: true });
  const { serverTime } = await read('https://api.binance.com/api/v3/time');
  const cutoff = Math.floor(serverTime / HOUR) * HOUR;
  const raw = await read(base + '/decentrader/liquidity-timelapse');
  const keys = ['signature', 'timestamp', 'intrusionTimestamp', 'direction', 'sideCounts', 'firstObservedAt',
    'candleClosedAt', 'smtpSentAt', 'emailType', 'id'];
  const compact = row => Object.fromEntries(keys.filter(key => row[key] !== undefined).map(key => [key, row[key]]));
  const intrusionPayload = { source: raw.source, intrusionDomStudy: { history: raw.intrusionDomStudy?.history?.map(compact) },
    delayHistory: { records: raw.delayHistory?.records?.map(compact) } };
  const history = compactIntrusionHistory(intrusionPayload, cutoff);
  if (!history.records.length) throw new Error('No saved intrusions; cannot backfill them from current active flags');
  const status = await read(base + '/decentrader/gap-status');
  const settings = Object.fromEntries(['tradeRiskUsd', 'tradeRiskUsdCapByPct', 'slMaxDistancePct',
    'dynamicSlEnabled', 'dynamicSlLiveUpdatesEnabled', 'dynamicSlFractalDelay'].map(key => [key, status[key]]));
  // Match the chart's maximum loaded history, extending further if saved signals require it.
  const from = Math.min(cutoff - 12000 * HOUR, history.records[0].time * 1000 - 96 * HOUR);
  const hourly = [];
  for (let cursor = from; cursor < cutoff;) {
    const rows = await read(`https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&startTime=${cursor}&endTime=${cutoff - 1}&limit=1000`);
    if (!Array.isArray(rows) || !rows.length) throw new Error('Hourly history ended before requested cutoff');
    hourly.push(...rows);
    const next = Number(rows.at(-1)[0]) + HOUR;
    if (!(next > cursor)) throw new Error('Non-advancing Binance history');
    cursor = next;
    console.log(`Spot 1H: ${hourly.length} candles`);
    await pause(700);
  }
  const daily = await read(`https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&endTime=${cutoff - 1}&limit=1000`);
  const capture = { version: 1, cutoff, capturedAt: new Date().toISOString(), source: 'Binance Spot BTCUSDT',
    settings, intrusionPayload, hourly, daily, manifest };
  fs.writeFileSync(path.join(output, 'inputs.json'), JSON.stringify(capture));
  console.log(JSON.stringify({ output, cutoff: new Date(cutoff).toISOString(), candles: hourly.length,
    intrusions: history.records.length, first: history.records[0], last: history.records.at(-1), settings }, null, 2));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
