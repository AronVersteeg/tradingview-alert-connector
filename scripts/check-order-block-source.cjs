const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const axios = require('axios');
const { buildOrderBlocks } = require('../public/btc-spot-chart/order-blocks.js');

// Public vendor reference only. Never imports the trading application or executes downloaded code.
(async () => {
  const url = 'https://www.luxalgo.com/library/indicator/order-block-detector/';
  const { data: html } = await axios.get(url, { timeout: 30000 });
  const match = html.match(/<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  assert(match, 'Missing structured vendor page data');
  function find(value) {
    if (!value || typeof value !== 'object') return;
    if (value.pineScriptCode && value.indicator?.tradingviewUuid === 'KvGhxGxY') return value;
    for (const child of Object.values(value)) { const found = find(child); if (found) return found; }
  }
  const source = find(JSON.parse(match[1])); assert(source, 'Wrong vendor script');
  const preview = source.indicator.chartPreviewData;
  assert(Array.isArray(preview.marketData));
  // Vendor's reference is a daily equities chart. Compact its sessions to hourly
  // indices solely to compare the timeframe-independent OHLCV formula, not market prices.
  const reference = preview.marketData.slice().sort((a, b) => a.time - b.time);
  const candles = reference.map((c, i) => ({ ...c, time: 3600 * (i + 1), closed: true }));
  const model = buildOrderBlocks(candles, {}, Number.MAX_SAFE_INTEGER);
  const expected = preview.plots.__boxes__.data.at(-1).value.filter(b => !b._deleted && b.left && b.top && b.bottom)
    .map(b => ({ time: b.left, top: b.top, bottom: b.bottom, direction: b.bgcolor.startsWith('#169400') ? 'bullish' : 'bearish' }));
  const actual = ['bullish', 'bearish'].flatMap(direction => model.blocks.filter(b => b.state === 'intact' && b.direction === direction).slice(-3))
    .map(b => ({ time: reference[b.originTime / 3600 - 1].time, top: b.top, bottom: b.bottom, direction: b.direction }));
  const sort = values => values.sort((a, b) => a.time - b.time || a.direction.localeCompare(b.direction));
  sort(actual); sort(expected); assert.equal(actual.length, expected.length);
  actual.forEach((block, i) => {
    assert.equal(block.time, expected[i].time); assert.equal(block.direction, expected[i].direction);
    for (const key of ['top', 'bottom']) assert(Math.abs(block[key] - expected[i][key]) < 1e-10, 'Vendor coordinate mismatch');
  });
  console.log(JSON.stringify({ source: url, sha256: crypto.createHash('sha256').update(source.pineScriptCode).digest('hex'),
    referenceTicker: preview.ticker, referenceTimeframe: preview.timeframe, candles: candles.length,
    matchingActiveBlocks: expected.length, priceTolerance: 1e-10,
    retainedBrokenBlocks: model.blocks.filter(b => b.state === 'broken').length,
    limitation: 'Compares vendor preview active coordinates; not a proof of complete TradingView event parity.' }, null, 2));
})().catch(error => { console.error(error); process.exitCode = 1; });
