const fs = require('node:fs');
const path = require('node:path');
const { projectReplicaIntrusionsForResearch } = require('../src/services/openLiquidityV2Replica');
const { buildDailyFractalHistory } = require('../src/services/binanceDailyFractalHistory');
const { dydxHourlyCandlesToFractalRows } = require('../src/services/decentraderGapMonitor');
const { simulateGreenRetest } = require('../src/research/greenRetest');

const HOUR = 3_600_000, DAY = 24 * HOUR;
const WARM = 8760, MAX_DAILY_DAYS = 999, MAX_HOURLY = 999;
const PRODUCTION_STEPS = { BTC: 100, ETH: 5, SOL: 0.1, INJ: 0.01, ZEC: 1 };
const VERIFIED_LISTING = { TAO: Date.UTC(2024, 3, 11, 12), HYPE: Date.UTC(2026, 8, 24, 11), WLD: Date.UTC(2023, 6, 24, 9) };
const MAINTENANCE = Date.UTC(2023, 2, 24, 12);
const iso = at => new Date(at).toISOString();
const utc = row => Date.parse(row.timestamp);
const validOhlc = (o, h, l, c) => [o, h, l, c].every(x => Number.isFinite(x) && x > 0) && h >= Math.max(o, c) && l <= Math.min(o, c);

function chooseStep(reference) {
  const x = reference * 0.0015;
  const base = 10 ** Math.floor(Math.log10(x));
  return Number([1, 2, 5, 10].map(n => base * n).sort((a, b) => Math.abs(a - x) - Math.abs(b - x))[0].toPrecision(8));
}

function dailySignal(records, hourByTime, timestamp, close, direction, priorClose) {
  const lastMidnight = Math.floor((timestamp + HOUR) / DAY) * DAY;
  const eligiblePivot = lastMidnight - (MAX_DAILY_DAYS - 2) * DAY;
  const floor = timestamp + (1 - MAX_HOURLY) * HOUR;
  const isLong = direction === 'long', matches = [];
  for (const level of records) {
    if (level.pivot < eligiblePivot || level.confirmed >= timestamp || level.type !== (isLong ? 'HIGH' : 'LOW')) continue;
    if (!(isLong ? priorClose <= level.price && close > level.price : priorClose >= level.price && close < level.price)) continue;
    const wickHint = level.firstWick < lastMidnight ? level.firstWick : undefined;
    const verifyFrom = Math.max(Math.floor(level.confirmed / HOUR) * HOUR + HOUR, wickHint ?? floor);
    if (verifyFrom < floor) continue;
    let valid = true;
    for (let t = verifyFrom; t < timestamp; t += HOUR) {
      const candle = hourByTime.get(t);
      if (!candle || (isLong ? candle.close > level.price : candle.close < level.price)) { valid = false; break; }
    }
    if (valid) matches.push(level.price);
  }
  return [...new Set(matches)].sort((a, b) => a - b);
}

function calcSignals(raw, candles, step, first, through, warm = WARM, includeIntrusionDaily = false) {
  const { ticker, symbol } = raw;
  // The replica expects a step of at least $0.01; choose equivalent price units for lower-priced research markets.
  const scale = Math.max(1, 10 ** Math.ceil(Math.log10(0.01 / step)));
  const input = scale === 1 ? candles : candles.map(c => ({ ...c, open: c.open * scale, high: c.high * scale,
    low: c.low * scale, close: c.close * scale }));
  const spotByHour = new Map(candles.map(row => [row.timestampMs, row]));
  const days = raw.daily.map(([openTime, closeTime, high, low]) => ({ openTime, closeTime, high, low }));
  if (days.some((d, i) => d.closeTime !== d.openTime + DAY - 1 || d.openTime % DAY || !(+d.low > 0) || +d.high < +d.low ||
      (i && d.openTime !== days[i - 1].openTime + DAY))) throw new Error('Spot daily data not contiguous.');
  const records = buildDailyFractalHistory(days, ticker, symbol).map(level => ({ ...level, pivot: Date.parse(level.pivotAt),
    confirmed: Date.parse(level.confirmedAt), firstWick: Date.parse(level.firstBrokenAt) }));
  const greens = [], purples = [], yellows = [], intrusionDaily = [];
  for (const intrusion of projectReplicaIntrusionsForResearch(input, step * scale, warm)) {
    const at = intrusion.timestampMs + HOUR;
    if (at < first || at > through) continue;
    const candle = spotByHour.get(intrusion.timestampMs), previous = spotByHour.get(intrusion.timestampMs - HOUR);
    const direction = candle.close > previous.close ? 'long' : 'short';
    const intrusionDirection = intrusion.imbalance > 0 ? 'long' : intrusion.imbalance < 0 ? 'short' : null;
    if (intrusionDirection) yellows.push({ at, direction: intrusionDirection });
    const edge = Number(((direction === 'long' ? intrusion.right : intrusion.left) / scale).toFixed(8));
    const crossed = direction === 'long' ? previous.close <= edge && candle.close > edge : previous.close >= edge && candle.close < edge;
    if (!crossed && !includeIntrusionDaily) continue;
    const signal = { time: intrusion.timestampMs / 1000, closedAt: at / 1000, direction, edge, close: candle.close, setup: 'green-line' };
    if (crossed) greens.push(signal);
    if (intrusionDirection !== direction) continue;
    const dailyLevels = dailySignal(records, spotByHour, intrusion.timestampMs, candle.close, direction, previous.close);
    if (!dailyLevels.length) continue;
    if (crossed) purples.push({ ...signal, dailyLevels });
    if (includeIntrusionDaily) intrusionDaily.push({ ...signal, dailyLevels, gapCloseBreak: crossed });
  }
  return { greens, purples, yellows, scale, intrusionDaily };
}

function quantify(trades) {
  let pnl = 0, highWater = 0, drawdown = 0;
  const closed = trades.filter(trade => trade.status === 'CLOSED');
  const exitStops = {
    initial: { closed: 0, wins: 0, realizedGrossUsd: 0 },
    trailing: { closed: 0, wins: 0, realizedGrossUsd: 0 }
  };
  for (const trade of closed) {
    const exitStop = trade.stops.findLast(stop => stop.at <= trade.closedAt);
    const group = exitStop === trade.stops[0] ? exitStops.initial : exitStops.trailing;
    group.closed++;
    if (trade.grossPnl > 0) group.wins++;
    group.realizedGrossUsd += trade.grossPnl;
  }
  for (const trade of closed) { pnl += trade.grossPnl; highWater = Math.max(highWater, pnl); drawdown = Math.max(drawdown, highWater - pnl); }
  return { closed: closed.length, wins: closed.filter(trade => trade.grossPnl > 0).length,
    realizedGrossUsd: pnl, open: trades.filter(trade => trade.status === 'OPEN').length,
    unrealizedGrossUsd: trades.reduce((n, tr) => n + (tr.markToMarket?.grossPnl ?? 0), 0),
    incomplete: trades.filter(trade => trade.status === 'INCOMPLETE').length, realizedMaxDrawdownUsd: drawdown, exitStops };
}

function byCalendarYear(trades, from, through, rows) {
  const localYear = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/Amsterdam', year: 'numeric' });
  const yearOf = at => Number(localYear.format(at));
  const grouped = new Map();
  for (const trade of trades) {
    if (trade.status !== 'CLOSED') continue;
    const year = yearOf(trade.closedAt);
    if (!grouped.has(year)) grouped.set(year, []);
    grouped.get(year).push(trade);
  }
  const result = {};
  for (let year = yearOf(from); year <= yearOf(through - 1); year++) {
    // Amsterdam is UTC+1 at the January boundary.
    const start = Date.UTC(year - 1, 11, 31, 23), end = Date.UTC(year, 11, 31, 23);
    const coveredFrom = Math.max(start, from), coveredThrough = Math.min(end, through);
    const closed = grouped.get(year) || [];
    const annual = quantify(closed);
    result[year] = { from: iso(coveredFrom), through: iso(coveredThrough), fullCalendarYear: coveredFrom === start && coveredThrough === end,
      missingHours: (coveredThrough - coveredFrom) / HOUR - rows.filter(row => utc(row) >= coveredFrom && utc(row) < coveredThrough).length,
      closed: annual.closed, wins: annual.wins, realizedGrossUsd: annual.realizedGrossUsd };
  }
  return result;
}

function main(asset, dir, research, { includeIntrusionDaily = false } = {}) {
  const raw = JSON.parse(fs.readFileSync(path.join(dir, `${asset}.json`), 'utf8'));
  if (raw.unavailable) return { asset, unavailable: raw.unavailable };
  const { ticker, symbol, start } = raw;
  const rows = dydxHourlyCandlesToFractalRows(raw.dydx.map(([startedAt, open, high, low, close, trades, orderbookMidPriceOpen, orderbookMidPriceClose]) =>
    ({ startedAt, open, high, low, close, trades, orderbookMidPriceOpen, orderbookMidPriceClose, resolution: '1HOUR' })), raw.through)
    .filter(r => validOhlc(r.openRef, r.highRef, r.lowRef, r.closeRef));
  if (!rows.length || new Set(rows.map(utc)).size !== rows.length) throw new Error('Empty or duplicate dYdX input.');
  const spot = raw.spot.map(([timestampMs, closeTimeMs, open, high, low, close]) => ({ timestampMs, closeTimeMs, open, high, low, close }))
    .filter(c => validOhlc(c.open, c.high, c.low, c.close) && c.timestampMs % HOUR === 0 &&
      (c.closeTimeMs === c.timestampMs + HOUR - 1 || c.timestampMs === MAINTENANCE &&
        c.closeTimeMs > MAINTENANCE && c.closeTimeMs < MAINTENANCE + HOUR));
  const runs = [];
  let first = 0;
  for (let i = 1; i <= spot.length; i++) {
    if (i < spot.length && (spot[i].timestampMs === spot[i - 1].timestampMs + HOUR ||
        spot[i - 1].timestampMs === MAINTENANCE && spot[i].timestampMs === MAINTENANCE + 2 * HOUR)) continue;
    const fromListing = VERIFIED_LISTING[asset] === spot[first]?.timestampMs;
    const warm = fromListing ? 1 : WARM;
    if (i - first > warm) runs.push({ candles: spot.slice(first, i), warm, fromListing });
    first = i;
  }
  const covered = run => Math.min(raw.through, utc(rows.at(-1)) + HOUR, run.candles.at(-1).timestampMs + HOUR) -
    Math.max(start, utc(rows[0]) + 72 * HOUR, run.candles[run.warm].timestampMs);
  const run = runs.sort((a, b) => covered(b) - covered(a))[0];
  if (!run) return { asset, unavailable: 'Insufficient continuous Spot history for the 8,760-hour model.' };
  const { candles, warm, fromListing } = run;
  const from = Math.max(start, utc(rows[0]) + 72 * HOUR, candles[warm].timestampMs);
  const through = Math.min(raw.through, utc(rows.at(-1)) + HOUR, candles.at(-1).timestampMs + HOUR);
  if (through <= from + HOUR) return { asset, unavailable: 'No viable overlap of Spot and dYdX after warmup.' };
  const step = PRODUCTION_STEPS[asset] ?? chooseStep(candles[0].close);
  const { greens, purples, yellows, scale, intrusionDaily } = calcSignals(raw, candles, step, from, through, warm, includeIntrusionDaily);
  if (research) return research({ ticker, rows, from, through, greens, purples, yellows, intrusionDaily });
  const recorded = new Set(rows.map(utc));
  const missingHours = (through - from) / HOUR - rows.filter(r => utc(r) >= from && utc(r) < through).length;
  const trades = { green: [], purple: [] };
  const restarts = [];
  let segment = from;
  while (segment < through) {
    const run = (signals, end) => simulateGreenRetest(rows, { market: ticker, from: segment, through: end, signals, yellows });
    let green = run(greens, through), purple = run(purples, through);
    const gap = Math.min(green.incompleteAt ?? Infinity, purple.incompleteAt ?? Infinity);
    if (Number.isFinite(gap)) {
      green = run(greens, gap + HOUR);
      purple = run(purples, gap + HOUR);
    }
    trades.green.push(...green.trades);
    trades.purple.push(...purple.trades);
    if (!Number.isFinite(gap)) break;
    segment = gap + HOUR;
    while (segment < through && !recorded.has(segment)) segment += HOUR;
    restarts.push({ gapAt: iso(gap), resumedAt: segment < through ? iso(segment) : null });
  }
  return { asset, ticker, symbol, from: iso(from), through: iso(through), days: (through - from) / DAY,
    earliestDydx: iso(utc(rows[0])), firstSpot: iso(candles[0].timestampMs), spotBinUsd: step,
    usesExistingBin: asset in PRODUCTION_STEPS, scale, fromListing, missingHours, restarts,
    green: { candidates: greens.length, ...quantify(trades.green), annual: byCalendarYear(trades.green, from, through, rows) },
    purple: { candidates: purples.length, ...quantify(trades.purple), annual: byCalendarYear(trades.purple, from, through, rows) },
    signals: { greens, purples, yellows } };
}

if (require.main === module) {
  const [asset, dir] = process.argv.slice(2);
  process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';
  let result;
  try { result = main(asset, dir); }
  catch (e) { result = { asset, unavailable: e.message }; process.exitCode = 1; }
  fs.writeFileSync(path.join(dir, `result-${asset}.json`), JSON.stringify(result));
  const { signals, ...summary } = result;
  console.log(JSON.stringify(summary));
}
module.exports = { main, calcSignals };
