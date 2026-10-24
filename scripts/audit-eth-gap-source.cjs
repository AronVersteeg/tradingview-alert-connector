const assert = require('node:assert/strict');
const axios = require('axios');
const HOUR = 3_600_000;
const key = zone => `${zone[0]}|${zone[1]}|${zone[2]}`;
const same = (a, b) => a === b || Number.isFinite(a) && Number.isFinite(b) && Math.abs(a - b) < .0001001;

function compareSavedLabel(record, gap, entrants) {
  const sides = new Set(entrants.map(zone => zone[2] - gap.left <= gap.right - zone[2] ? 'long' : 'short'));
  const direction = sides.size === 1 ? [...sides][0] : 'mixed';
  const facts = { at: record.timestamp, recordedDirection: record.direction, reconstructedDirection: direction,
    recordedGap: record.gap, reconstructedGap: gap && { left: gap.left, right: gap.right }, entrants: entrants.length,
    revisions: record.gapRevisions?.length || 0 };
  if (!entrants.length || direction !== record.direction) return { ...facts, kind: 'intrusion-mismatch' };
  if (!Number.isFinite(record.gap?.left) || !Number.isFinite(record.gap?.right)) return { ...facts, kind: 'original-gap-missing' };
  if (gap?.left === record.gap.left && gap?.right === record.gap.right) return { ...facts, kind: 'match' };
  if (record.gapRevisions?.some(revision => revision.gap.left === gap?.left && revision.gap.right === gap?.right)) return { ...facts, kind: 'saved-revision' };
  return { ...facts, kind: 'gap-mismatch' };
}

function auditPayload(payload, candles) {
  assert.equal(payload.source.market, 'ETH-USD');
  assert.equal(payload.modelVersion, 'binance-spot-eth-liquidation-cohorts-v2.1');
  assert.equal(payload.quality.priceStepUsd, 5);
  assert.equal(payload.quality.causalModel, true);
  assert(payload.source.sourceStatuses.some(source => source.name === 'Binance Spot ETHUSDT 1H' && source.ok));
  assert(!payload.source.sourceStatuses.some(source => /Futures|BTCUSDT/i.test(source.name)));
  const { frames, gaps, zoneDeltas, zoneSeed } = payload;
  assert(frames.length > 1 && frames.length === gaps.length && frames.length === zoneDeltas.length);
  const active = new Map(), raw = new Map(candles.map(c => [c.timestampMs, c]));
  const issues = [], expectedIntrusions = new Map(), leftValues = new Set(), rightValues = new Set();
  let candleMatches = 0, leftChanges = 0, rightChanges = 0;
  const apply = zone => {
    assert(Array.isArray(zone) && zone.length === 4 && ['L', 'S'].includes(zone[0]) && [3, 5, 10].includes(zone[1]) &&
      zone[2] > 0 && zone[2] % 5 === 0 && Number.isSafeInteger(zone[3]) && zone[3] >= 0, 'Invalid ETH cohort tuple');
    if (zone[3]) active.set(key(zone), zone); else active.delete(key(zone));
  };
  zoneSeed.forEach(apply);
  frames.forEach((frame, i) => {
    assert.equal(Date.parse(frame.t.replace(' ', 'T') + 'Z'), frame.startedAtMs);
    assert.equal(frame.startedAtMs % HOUR, 0);
    if (i) assert.equal(frame.startedAtMs - frames[i - 1].startedAtMs, HOUR);
    const previous = new Map(active);
    zoneDeltas[i].forEach(apply);
    let left = -Infinity, right = Infinity;
    for (const zone of active.values()) {
      if (zone[2] < frame.price) left = Math.max(left, zone[2]);
      if (zone[2] > frame.price) right = Math.min(right, zone[2]);
    }
    const gap = gaps[i];
    if (Number.isFinite(left) && Number.isFinite(right)) {
      if (!gap || gap.left !== left || gap.right !== right || gap.width !== right - left) issues.push({ type: 'nearest-edge-mismatch', at: frame.t, expected: [left, right], actual: gap && [gap.left, gap.right] });
    } else if (gap !== null) issues.push({ type: 'unexpected-gap', at: frame.t });
    if (gap) { leftValues.add(gap.left); rightValues.add(gap.right); }
    if (i && gap && gaps[i - 1]) {
      leftChanges += Number(gap.left !== gaps[i - 1].left); rightChanges += Number(gap.right !== gaps[i - 1].right);
    }
    if (i && gaps[i - 1]) {
      const previousGap = gaps[i - 1], entrants = [...active.values()].filter(zone => zone[3] > (previous.get(key(zone))?.[3] || 0) && zone[2] > previousGap.left && zone[2] < previousGap.right);
      if (entrants.length) {
        const sides = new Set(entrants.map(zone => zone[2] - previousGap.left <= previousGap.right - zone[2] ? 'long' : 'short'));
        expectedIntrusions.set(frame.t, { direction: sides.size === 1 ? [...sides][0] : 'mixed', gap: previousGap });
      }
    }
    const candle = raw.get(frame.startedAtMs);
    if (!candle || !['open', 'high', 'low', 'close'].every(field => same(frame[field], candle[field])) ||
        !same(frame.price, (candle.open + candle.high + candle.low + candle.close) / 4)) issues.push({ type: 'spot-ohlc-mismatch', at: frame.t });
    else candleMatches++;
  });
  let benchmarkOverlap = 0, benchmarkOutsideCoverage = 0, benchmarkOriginalGapMissing = 0;
  const first = frames[0].startedAtMs, last = frames.at(-1).startedAtMs;
  for (const record of payload.intrusionBenchmarks?.history || []) {
    const ms = Date.parse(record.timestamp.replace(' ', 'T') + 'Z');
    if (ms <= first || ms > last) { benchmarkOutsideCoverage++; continue; }
    benchmarkOverlap++;
    const expected = expectedIntrusions.get(record.timestamp);
    if (expected && expected.direction === record.direction && (!Number.isFinite(record.gap?.left) || !Number.isFinite(record.gap?.right))) benchmarkOriginalGapMissing++;
    else if (!expected || expected.direction !== record.direction || record.gap.left !== expected.gap.left || record.gap.right !== expected.gap.right) {
      issues.push({ type: 'saved-intrusion-mismatch', at: record.timestamp, originalGap: record.gap, expected,
        revisions: record.gapRevisions?.length || 0 });
    }
  }
  return { market: 'ETH-USD', symbol: 'ETHUSDT', source: 'Binance Spot', modelVersion: payload.modelVersion,
    from: frames[0].t, through: frames.at(-1).t, frames: frames.length, candleMatches,
    uniqueLeftEdges: leftValues.size, uniqueRightEdges: rightValues.size, leftChanges, rightChanges,
    reconstructedIntrusions: expectedIntrusions.size, benchmarkOverlap, benchmarkOutsideCoverage, benchmarkOriginalGapMissing, issues };
}

async function run() {
  const payload = (await axios.get('https://arons-tradingview-alert-connector.onrender.com/open-liquidity/v2/liquidity-timelapse',
    { params: { market: 'ETH-USD' }, timeout: 60_000 })).data;
  const allLabels = process.argv.includes('--all-labels'), deep = process.argv.includes('--deep') || allLabels;
  const labels = payload.intrusionBenchmarks?.history || [];
  const labelTimes = labels.map(row => Date.parse(row.timestamp.replace(' ', 'T') + 'Z'));
  labelTimes.forEach(ms => assert(Number.isFinite(ms) && ms % HOUR === 0, 'Invalid saved label time'));
  const firstTarget = allLabels ? Math.min(payload.frames[0].startedAtMs, ...labelTimes) : payload.frames[0].startedAtMs;
  const start = firstTarget - (deep ? 8760 * HOUR : 0);
  const end = payload.frames.at(-1).startedAtMs + HOUR - 1, candles = [];
  let pages = 0;
  for (let cursor = start; cursor < end;) {
    const rows = (await axios.get('https://api.binance.com/api/v3/klines', { timeout: 30_000,
      params: { symbol: 'ETHUSDT', interval: '1h', startTime: cursor, endTime: end, limit: 1000 } })).data;
    assert(Array.isArray(rows) && rows.length, 'Missing ETH Spot candle page');
    candles.push(...rows.map(row => ({ timestampMs: +row[0], closeTimeMs: +row[6], open: +row[1], high: +row[2], low: +row[3], close: +row[4] })));
    pages++; const next = +rows.at(-1)[0] + HOUR; assert(next > cursor); cursor = next;
    await new Promise(resolve => setTimeout(resolve, 300));
  }
  for (let i = 1; i < candles.length; i++) assert.equal(candles[i].timestampMs - candles[i - 1].timestampMs, HOUR);
  const report = auditPayload(payload, candles); report.binancePages = pages;
  if (deep) {
    // This module defines pure replay helpers and collectors, but starts nothing.
    require('ts-node/register/transpile-only');
    const { buildReplicaSnapshots, IncrementalReplicaReplay } = require('../src/services/openLiquidityV2Replica');
    const expected = new Map();
    if (allLabels) {
      const replay = await IncrementalReplicaReplay.create(candles.slice(0, 8760), { priceStepUsd: 5, frameLimit: 2, modelVersion: payload.modelVersion });
      const byTime = new Map();
      for (const record of labels) {
        const time = Date.parse(record.timestamp.replace(' ', 'T') + 'Z');
        byTime.set(time, [...(byTime.get(time) || []), record]);
      }
      const mismatches = [], revisions = [], missing = [];
      let checked = 0, signalsMatched = 0, originalGapsMatched = 0;
      for (let i = 8760; i < candles.length; i++) {
        const previous = replay.snapshots.at(-1), current = replay.append(candles[i]);
        expected.set(candles[i].timestampMs, current.gap);
        for (const record of byTime.get(candles[i].timestampMs) || []) {
          checked++;
          const priorCounts = new Map((replay.snapshots[0].zoneSeed || []).map(zone => [key(zone), zone[3]]));
          const gap = previous.gap;
          const entrants = gap ? current.zoneDeltas.filter(zone => zone[3] > (priorCounts.get(key(zone)) || 0) && zone[2] > gap.left && zone[2] < gap.right) : [];
          const result = compareSavedLabel(record, gap, entrants);
          if (result.kind !== 'intrusion-mismatch') signalsMatched++;
          if (result.kind === 'original-gap-missing') missing.push(record.timestamp);
          else if (result.kind === 'saved-revision') revisions.push(result);
          else if (result.kind === 'match') originalGapsMatched++;
          else mismatches.push(result);
        }
        if (i % 128 === 0) await new Promise(resolve => setImmediate(resolve));
      }
      report.allSavedLabels = { total: labels.length, checked, signalsMatched, unverified: labels.length - checked,
        originalGapsMatched, missingOriginalGaps: { count: missing.length, from: missing[0], through: missing.at(-1),
          note: 'Reconstructed values are not original saved observations; no history is overwritten.' },
        originalGapDifferencesExplainedBySavedRevision: revisions, mismatches };
    } else {
      for (const row of buildReplicaSnapshots(candles, { priceStepUsd: 5, frameLimit: 500, modelVersion: payload.modelVersion })) expected.set(Date.parse(row.effectiveAt), row.gap);
    }
    report.fullSpotReplay = { sourceCandles: candles.length, warmupHours: 8760, checked: payload.frames.length, mismatches: [] };
    payload.frames.forEach((frame, i) => {
      const a = payload.gaps[i], b = expected.get(frame.startedAtMs);
      if ((a?.left ?? null) !== (b?.left ?? null) || (a?.right ?? null) !== (b?.right ?? null)) report.fullSpotReplay.mismatches.push({ at: frame.t, actual: a && [a.left, a.right], rebuilt: b && [b.left, b.right] });
    });
  }
  console.log(JSON.stringify(report, null, 2));
  if (report.issues.length || report.fullSpotReplay?.mismatches.length || report.allSavedLabels?.mismatches.length || report.allSavedLabels?.unverified) process.exitCode = 1;
}
module.exports = { auditPayload, compareSavedLabel };
if (require.main === module) run().catch(error => { console.error(error.message); process.exitCode = 1; });
