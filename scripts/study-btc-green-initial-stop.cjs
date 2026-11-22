const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { main } = require('./run-top15-retest.cjs');
const { simulateGreenRetest } = require('../src/research/greenRetest');

const HOUR = 3_600_000;
const iso = at => new Date(at).toISOString();
const against = direction => direction === 'long' ? -1 : 1;
const body = (c, fraction) => c.openRef + fraction * (c.closeRef - c.openRef);
const wick = (c, direction) => direction === 'long' ? c.lowRef : c.highRef;
const baseVariants = [
  { id: 'open', label: 'Signal candle open (baseline)' },
  ...[0.25, 0.5, 0.75].map(f => ({ id: `body${100 * f}`, label: `${100 * f}% from open into body`, stop: c => body(c, f) })),
  ...[0.25, 0.5].map(f => ({ id: `beyondOpen${100 * f}`, label: `${100 * f}% of body beyond open on protective side`,
    stop: (c, direction) => c.openRef + against(direction) * f * Math.abs(c.closeRef - c.openRef) })),
  { id: 'wick', label: 'Signal wick extreme', stop: wick },
  { id: 'wick0.1pct', label: 'Signal wick buffered 0.1% of price',
    stop: (c, direction) => wick(c, direction) * (1 + against(direction) * 0.001) },
  { id: 'wick25range', label: 'Beyond wick by 25% of signal range',
    stop: (c, direction) => wick(c, direction) + against(direction) * 0.25 * (c.highRef - c.lowRef) },
  { id: 'sizingFractal', label: 'Confirmed sizing fractal with its existing buffer', stop: (_c, _direction, sizingStop) => sizingStop }
];

function preSignalAtr(known) {
  const sample = known.slice(-16, -1);
  if (sample.length < 15) return NaN;
  return sample.slice(1).reduce((sum, candle, index) => {
    const prior = sample[index].closeRef;
    return sum + Math.max(candle.highRef - candle.lowRef, Math.abs(candle.highRef - prior), Math.abs(candle.lowRef - prior));
  }, 0) / 14;
}

const structuralVariants = [
  ...[0.1, 0.25, 0.5].map(multiplier => ({ id: `breakAtr${100 * multiplier}`,
    label: `Beyond strictest broken Spot level by ${multiplier} pre-signal ATR`,
    stop: (c, direction, _sizingStop, { signal, known }) => {
      const spotLevels = [signal.edge, ...signal.dailyLevels];
      const strict = direction === 'long' ? Math.max(...spotLevels) : Math.min(...spotLevels);
      const level = strict * (c.closeRef / signal.close);
      return level + against(direction) * multiplier * preSignalAtr(known);
    } }))
];
const atrVariants = [0.75, 1, 1.25, 1.5].map(multiplier => ({ id: `signalCloseAtr${100 * multiplier}`,
  label: `${multiplier} pre-signal ATR from dYdX signal close`,
  stop: (c, direction, _sizingStop, { known }) => c.closeRef + against(direction) * multiplier * preSignalAtr(known) }));

function metrics(trades, skipped, skippedWhileOpen) {
  const closed = trades.filter(t => t.status === 'CLOSED');
  let gross = 0, peak = 0, maxDrawdown = 0, turnover = 0, riskSum = 0, initialExits = 0, wins = 0;
  for (const t of closed) {
    gross += t.grossPnl;
    peak = Math.max(peak, gross);
    maxDrawdown = Math.max(maxDrawdown, peak - gross);
    turnover += t.size * (t.entryPrice + t.exitPrice);
    riskSum += t.riskUsd;
    if (t.stops.filter(stop => stop.at <= t.closedAt).length === 1) initialExits++;
    if (t.grossPnl > 0) wins++;
  }
  const net = Object.fromEntries([2, 5, 10].map(bps => [bps, gross - turnover * bps / 10_000]));
  return { closed: closed.length, wins, gross, maxDrawdown, turnover, net,
    breakEvenBpsPerSide: turnover ? gross / turnover * 10_000 : null,
    avgInitialRisk: closed.length ? riskSum / closed.length : null, initialExits, trailingExits: closed.length - initialExits,
    skipped, skippedWhileOpen, incomplete: trades.filter(t => t.status === 'INCOMPLETE').length,
    open: trades.filter(t => t.status === 'OPEN').length,
    openGross: trades.reduce((total, t) => total + (t.markToMarket?.grossPnl ?? 0), 0) };
}

function runWindow(data, from, through, variants, signals) {
  const byVariant = variants.map(v => ({ v, trades: [], skipped: 0, skippedWhileOpen: 0 }));
  const known = new Set(data.rows.map(row => Date.parse(row.timestamp)));
  let start = from;
  const dataGaps = [];
  while (start < through) {
    const simulate = (v, end) => simulateGreenRetest(data.rows, { market: data.ticker, from: start, through: end,
      signals: v.signals ?? signals, yellows: data.yellows }, v.stop);
    let results = variants.map(v => simulate(v, through));
    const gap = Math.min(...results.map(r => r.incompleteAt ?? Infinity));
    if (Number.isFinite(gap)) results = variants.map(v => simulate(v, gap + HOUR));
    results.forEach((r, i) => {
      byVariant[i].trades.push(...r.trades);
      byVariant[i].skipped += r.skipped;
      byVariant[i].skippedWhileOpen += r.skippedWhileOpen;
    });
    if (!Number.isFinite(gap)) break;
    start = gap + HOUR;
    while (start < through && !known.has(start)) start += HOUR;
    dataGaps.push({ at: iso(gap), resumedAt: start < through ? iso(start) : null });
  }
  return { from: iso(from), through: iso(through), dataGaps, variants: byVariant.map(({ v, trades, skipped, skippedWhileOpen }) =>
    ({ id: v.id, label: v.label, ...metrics(trades, skipped, skippedWhileOpen), ...(v.audit ? v.audit(trades) : {}),
      yearly: Object.fromEntries([2023, 2024, 2025, 2026].map(y => {
        const left = Date.UTC(y - 1, 11, 31, 23), right = Date.UTC(y, 11, 31, 23);
        const yearTrades = trades.filter(t => t.closedAt >= left && t.closedAt < right);
        const result = metrics(yearTrades);
        return [y, { closed: result.closed, gross: result.gross, netAtFiveBpsPerSide: result.net[5] }];
      })) })) };
}

if (require.main === module) {
process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';
const [rawDir, out, strategy = 'green', selection] = process.argv.slice(2);
assert(['green', 'purple'].includes(strategy));
const variants = selection === 'atr75' ? [baseVariants[0], atrVariants[0]]
  : strategy === 'purple' ? [...baseVariants, ...structuralVariants, ...atrVariants] : baseVariants;
const split = Date.UTC(2024, 11, 31, 23);
const data = main('BTC', rawDir, data => {
  const signals = strategy === 'purple' ? data.purples : data.greens;
  const run = (from, through) => runWindow(data, from, through, variants, signals);
  return { full: run(data.from, data.through), development: run(data.from, split), holdout: run(split, data.through) };
});
const expected = strategy === 'purple' ? { closed: 88, gross: 203.71516134211564 } : { closed: 163, gross: 147.91573221358223 };
assert.equal(data.full.variants[0].closed, expected.closed);
assert(Math.abs(data.full.variants[0].gross - expected.gross) < 1e-9);
const report = { market: 'BTC-USD', strategy, riskBudgetUsd: 15,
  methodology: 'Initial stop placement only. Existing fractal sizing and $15 max initial modeled price risk, same-direction Yellow trailing, one position at a time. No TP. Later validation window starts flat; an unresolved gap restarts all variants flat. Fee sensitivity charges both entry and exit notional at hypothetical fees per side; no funding or additional slippage. ATR is the mean of 14 hourly true ranges before the signal candle. Structural stop uses the strictest broken Spot level, multiplied by the dYdX/Spot close ratio of the signal candle, then applies the ATR buffer in dYdX units.',
  ...data };
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(report, null, 2) + '\n');
const n = v => v === null ? v : Math.round(v * 100) / 100;
console.table(data.full.variants.map((v, i) => ({ id: v.id, closed: v.closed, gross: n(v.gross), net5: n(v.net[5]),
  beBps: n(v.breakEvenBpsPerSide), drawdown: n(v.maxDrawdown), nInitial: v.initialExits, openGross: n(v.openGross), gap: v.incomplete,
  train: n(data.development.variants[i].gross), priorNet5: n(data.development.variants[i].net[5]),
  holdout: n(data.holdout.variants[i].gross), holdoutNet5: n(data.holdout.variants[i].net[5]),
  y24: n(v.yearly[2024].gross), y25: n(v.yearly[2025].gross), y26: n(v.yearly[2026].gross) })));
}
module.exports = { runWindow, atr75: atrVariants[0] };
