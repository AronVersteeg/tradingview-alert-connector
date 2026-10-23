// Offline, public-data-only recovery. Never import the trading application.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
require('ts-node/register/transpile-only');
const { fetchBinanceSpotCandles, IncrementalReplicaReplay } = require('../src/services/openLiquidityV2Replica');
const [cacheFile, outputFile, from, through] = process.argv.slice(2);
if (!cacheFile || !outputFile || !from || !through) throw new Error('Supply candle cache, output, first candle ISO and exclusive end ISO');
const hour = 3600000, first = Date.parse(from), end = Date.parse(through), start = first - 8760 * hour;
if (!(first % hour === 0 && end % hour === 0 && first < end && end <= Date.now())) throw new Error('Invalid closed hourly range');
(async () => {
  let candles = fs.existsSync(cacheFile) ? JSON.parse(fs.readFileSync(cacheFile, 'utf8')) : [];
  if (!candles.length) candles = await fetchBinanceSpotCandles((end - start) / hour, end);
  else {
    if (candles[0].timestampMs > start) candles = [...await fetchBinanceSpotCandles((candles[0].timestampMs - start) / hour, candles[0].timestampMs), ...candles];
    if (candles.at(-1).timestampMs < end - hour) candles.push(...await fetchBinanceSpotCandles((end - candles.at(-1).timestampMs) / hour, end));
  }
  candles = [...new Map(candles.filter(c => c.timestampMs >= start && c.timestampMs < end).map(c => [c.timestampMs, c])).values()].sort((a, b) => a.timestampMs - b.timestampMs);
  if (candles.length !== (end - start) / hour || candles.some((c, i) => c.timestampMs !== start + i * hour || c.closeTimeMs > c.timestampMs + hour || ![c.open, c.high, c.low, c.close].every(p => Number.isFinite(p) && p > 0))) throw new Error('Missing or invalid source hours; recovery refused');
  fs.writeFileSync(cacheFile, JSON.stringify(candles));
  const replay = await IncrementalReplicaReplay.create(candles.slice(0, 8760), { frameLimit: 1 });
  const records = [];
  for (const candle of candles.slice(8760)) {
    const snapshot = replay.append(candle);
    records.push([candle.timestampMs / 1000, snapshot.gap?.left ?? null, snapshot.gap?.right ?? null]);
  }
  const recovery = { version: 1, symbol: 'BTCUSDT', modelVersion: 'binance-spot-liquidation-cohorts-v2.4',
    historicalAccuracy: 'causal-model-replay', generatedAt: new Date().toISOString(),
    source: { endpoint: 'https://api.binance.com/api/v3/klines', interval: '1h', cohortWindowHours: 8760,
      from: new Date(start).toISOString(), through: new Date(end).toISOString(), candles: candles.length,
      sha256: crypto.createHash('sha256').update(JSON.stringify(candles)).digest('hex') },
    timingNote: 'Recomputed after the fact, not original provider observations or evidence of historical alert availability.', records };
  fs.mkdirSync(path.dirname(outputFile), { recursive: true });
  fs.writeFileSync(outputFile, JSON.stringify(recovery) + '\n');
  console.log(JSON.stringify({ records: records.length, source: recovery.source, outputFile }));
})().catch(error => { console.error(error.message); process.exitCode = 1; });
