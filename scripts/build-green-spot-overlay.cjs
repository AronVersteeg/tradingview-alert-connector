const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { buildHourlyFractalBoxes } = require('../public/btc-spot-chart/hourly-fractal-boxes.js');
const { replayGreenPositions } = require('../public/btc-spot-chart/green-position-replay.js');
const { readReplay } = require('../public/btc-spot-chart/historical-replay.js');

const [asset, sourceFile] = process.argv.slice(2);
assert(/^(eth|inj|sol|zec|wld)$/.test(asset), 'Expected a supported alternative Spot asset.');
const root = path.join(__dirname, '..'), startAsset = asset.toUpperCase();
const raw = JSON.parse(fs.readFileSync(sourceFile, 'utf8'));
const archive = JSON.parse(fs.readFileSync(path.join(root, `research-data/${asset}-green-retest-history.json`), 'utf8'));
assert.equal(archive.market, `${startAsset}-USD`);
assert.equal(raw.ticker, archive.market);
assert.equal(raw.symbol, `${startAsset}USDT`);
assert(raw.through >= archive.through);
const HOUR = 3600;
const candles = raw.spot.filter(([at]) => at >= archive.from - 100 * HOUR * 1000 && at < archive.through)
  .map(([at, closeAt, open, high, low, close]) => ({ time: at / 1000, open, high, low, close, closed: closeAt === at + HOUR * 1000 - 1 }));
assert(candles.length && candles.every((row, i) => row.closed && (!i || candles[i - 1].time + HOUR === row.time)));
const byTime = new Map(candles.map(c => [c.time, c]));
const signals = archive.greens.map(([at, direction, edge, close]) => {
  const time = at / 1000 - HOUR;
  assert.equal(byTime.get(time)?.close, close);
  return { time, closedAt: at / 1000, direction, edge, close };
});
const fractals = buildHourlyFractalBoxes(candles, archive.through).fractals;
// Give the recorded live markers the most recent week; hand over only when flat.
let before = archive.through / 1000 - 168 * HOUR;
let result;
for (; before > archive.from / 1000; before -= HOUR) {
  result = replayGreenPositions(candles.filter(c => c.time < before), signals, fractals);
  if (!result.position && result.selected.length) break;
}
assert(result && !result.position && result.selected.length);
const payload = { source: 'binance-spot-green-reconstruction', symbol: raw.symbol, modelVersion: archive.model,
  before, positionAtCutoff: 'flat', signals: result.selected.map(s => [s.time, s.direction, s.close]) };
readReplay(payload, 'green', raw.symbol);
fs.writeFileSync(path.join(root, `public/btc-spot-chart/${asset}-green-replay.json`), JSON.stringify(payload) + '\n');
console.log(raw.symbol, payload.signals.length, 'historical Green markers, flat at', new Date(before * 1000).toISOString());
