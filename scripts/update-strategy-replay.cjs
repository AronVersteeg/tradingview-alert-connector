const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const axios = require('axios');

// Keep this historical study aligned with the established replay methodology.
process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';

const { replaySource } = require('./run-top15-retest.cjs');
const { runWindow } = require('./study-btc-green-initial-stop.cjs');

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
// dYdX Chain genesis: https://www.dydx.foundation/blog/dydx-token-mechanics
const DYDX_GENESIS = Date.UTC(2023, 9, 26, 17);
const GENESIS_DAY = Math.floor(DYDX_GENESIS / DAY) * DAY;
const ATR_PERIOD = 14;
const ATR_THRESHOLD = 2;
const FEE_BPS_PER_SIDE = 5;
const RISK_USD = 15;
const ACTIVE_ASSETS = ['BTC', 'ETH', 'INJ', 'SOL', 'ZEC', 'WLD'];
const BINANCE_MAINTENANCE = Date.UTC(2023, 2, 24, 12);
const LEGACY_CACHE = path.join(os.tmpdir(), 'codex-top15-retest-20261120');
const DEFAULT_CACHE = fs.existsSync(path.join(LEGACY_CACHE, 'BTC.json'))
  ? LEGACY_CACHE
  : path.join(os.tmpdir(), 'tradingview-alert-connector-strategy-replay');
const DEFAULT_REPORT = path.resolve(__dirname, '../research-output/active-pair-strategy-replay');
const BINANCE = 'https://data-api.binance.vision/api/v3/klines';
const DYDX = 'https://indexer.dydx.trade/v4/candles/perpetualMarkets';

const args = process.argv.slice(2);
const option = (name) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : undefined;
};
const offline = args.includes('--offline');
const requestedAsset = option('--asset')?.toUpperCase();
if (args.includes('--asset') && !ACTIVE_ASSETS.includes(requestedAsset)) throw new Error('Unknown active --asset.');
const ASSETS = requestedAsset ? [requestedAsset] : ACTIVE_ASSETS;
const dataDir = path.resolve(option('--data-dir') || process.env.STRATEGY_REPLAY_DATA_DIR || DEFAULT_CACHE);
const reportBase = path.resolve(option('--report') || DEFAULT_REPORT);
const requestedThrough = option('--through');
const targetThrough = requestedThrough
  ? Date.parse(requestedThrough)
  : Math.floor(Date.now() / HOUR) * HOUR;

const iso = (at) => new Date(at).toISOString();
const replayDate = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', dateStyle: 'short' });
const localDate = (at) => replayDate.format(new Date(at));
const money = (value) => `${value >= 0 ? '+$' : '-$'}${Math.abs(value).toFixed(2)}`;
const pause = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const queue = new Map();
const nextRequest = new Map();

if (!Number.isSafeInteger(targetThrough) || targetThrough % HOUR || targetThrough <= DYDX_GENESIS || targetThrough > Date.now()) {
  throw new Error('--through must be a closed-hour boundary after dYdX Chain genesis and not in the future.');
}

async function get(url, params) {
  const host = new URL(url).hostname;
  for (let attempt = 0; attempt < 4; attempt++) {
    const previous = queue.get(host) || Promise.resolve();
    const gate = previous.then(async () => {
      await pause(Math.max(0, (nextRequest.get(host) || 0) - Date.now()));
      nextRequest.set(host, Date.now() + 200);
    });
    queue.set(host, gate);
    await gate;
    try {
      return (await axios.get(url, { params, timeout: 25_000 })).data;
    } catch (error) {
      const status = error.response?.status;
      if (attempt === 3 || status && status < 500 && status !== 418 && status !== 429) throw error;
      const delay = Math.max(Number(error.response?.headers?.['retry-after']) * 1000 || 0, (attempt + 1) * 2_000);
      nextRequest.set(host, Date.now() + delay);
      await pause(delay);
    }
  }
}

async function binanceRows(symbol, interval, from, through, width) {
  const rows = [];
  for (let at = from; at < through; at += 1000 * width) {
    const end = Math.min(through, at + 1000 * width);
    const batch = await get(BINANCE, { symbol, interval, startTime: at, endTime: end - 1, limit: 1000 });
    for (const row of batch) if (+row[0] >= at && +row[0] < end) rows.push(row);
  }
  return rows;
}

async function dydxRows(ticker, from, through) {
  const rows = [];
  for (let at = from; at < through; at += 750 * HOUR) {
    const end = Math.min(through, at + 750 * HOUR);
    const data = await get(`${DYDX}/${ticker}`, {
      resolution: '1HOUR', fromISO: iso(at), toISO: iso(end), limit: 1000
    });
    for (const row of data.candles || []) {
      const startedAt = Date.parse(row.startedAt);
      if (row.ticker === ticker && row.resolution === '1HOUR' && startedAt >= at && startedAt < end) rows.push(row);
    }
  }
  return rows;
}

function uniqueSorted(rows, timestamp) {
  return [...new Map(rows.map(row => [timestamp(row), row])).entries()]
    .sort((left, right) => left[0] - right[0]).map(([, row]) => row);
}

function validateSpot(asset, rows, through) {
  assert(rows.length, `${asset}: no Binance Spot candles`);
  for (let i = 0; i < rows.length; i++) {
    const [at, closeAt, open, high, low, close] = rows[i];
    const maintenance = at === BINANCE_MAINTENANCE && closeAt > at && closeAt < at + HOUR;
    assert(Number.isSafeInteger(at) && at % HOUR === 0 && (closeAt === at + HOUR - 1 || maintenance),
      `${asset}: malformed Spot timestamp at ${iso(at)}`);
    assert([open, high, low, close].every(value => Number.isFinite(value) && value > 0) &&
      high >= Math.max(open, close) && low <= Math.min(open, close), `${asset}: malformed Spot OHLC at ${iso(at)}`);
    const maintenanceGap = i > 0 && at === BINANCE_MAINTENANCE + 2 * HOUR &&
      (rows[i - 1][0] === BINANCE_MAINTENANCE || rows[i - 1][0] === BINANCE_MAINTENANCE - HOUR);
    if (i && at !== rows[i - 1][0] + HOUR && !maintenanceGap) {
      throw new Error(`${asset}: missing Spot hour before ${iso(at)}`);
    }
  }
  assert.equal(rows.at(-1)[0] + HOUR, through, `${asset}: latest closed Spot hour is missing`);
}

function validateDaily(asset, rows, through) {
  assert(rows.length, `${asset}: no Binance Daily candles`);
  for (let i = 0; i < rows.length; i++) {
    const [at, closeAt, high, low] = rows[i];
    assert(Number.isSafeInteger(at) && at % DAY === 0 && closeAt === at + DAY - 1 && +high >= +low && +low > 0,
      `${asset}: malformed Daily candle at ${iso(at)}`);
    if (i && at !== rows[i - 1][0] + DAY) throw new Error(`${asset}: missing Daily candle before ${iso(at)}`);
  }
  assert.equal(rows.at(-1)[0] + DAY, through, `${asset}: latest closed Daily candle is missing`);
}

const dydxTuple = row => [row.startedAt, row.open, row.high, row.low, row.close, row.trades,
  row.orderbookMidPriceOpen, row.orderbookMidPriceClose];

async function bootstrap(asset, through) {
  const symbol = `${asset}USDT`;
  const ticker = `${asset}-USD`;
  const first = await get(BINANCE, { symbol, interval: '1h', startTime: DYDX_GENESIS - 365 * DAY, limit: 1 });
  if (!first[0]) throw new Error(`${asset}: Binance Spot symbol unavailable`);
  const spotStart = +first[0][0];
  const dailyThrough = Math.floor(through / DAY) * DAY;
  const [spotSource, dailySource, dydxSource] = await Promise.all([
    binanceRows(symbol, '1h', spotStart, through, HOUR),
    binanceRows(symbol, '1d', GENESIS_DAY - 1000 * DAY, dailyThrough, DAY),
    dydxRows(ticker, DYDX_GENESIS, through)
  ]);
  const spot = spotSource.map(row => [+row[0], +row[6], +row[1], +row[2], +row[3], +row[4]]);
  const daily = dailySource.map(row => [+row[0], +row[6], row[2], row[3]]);
  validateSpot(asset, spot, through);
  validateDaily(asset, daily, dailyThrough);
  return { asset, symbol, ticker, start: DYDX_GENESIS, through, spot, daily,
    dydx: uniqueSorted(dydxSource, row => Date.parse(row.startedAt)).map(dydxTuple) };
}

async function extend(asset, raw, through) {
  if (through <= raw.through) return raw;
  assert.equal(raw.asset, asset);
  assert.equal(raw.symbol, `${asset}USDT`);
  assert.equal(raw.ticker, `${asset}-USD`);
  const overlapFrom = Math.max(raw.start, raw.through - 24 * HOUR);
  const dailyThrough = Math.floor(through / DAY) * DAY;
  const dailyFrom = Math.max(raw.daily[0][0], Math.floor(raw.through / DAY) * DAY - DAY);
  const [spotSource, dailySource, dydxSource] = await Promise.all([
    binanceRows(raw.symbol, '1h', overlapFrom, through, HOUR),
    binanceRows(raw.symbol, '1d', dailyFrom, dailyThrough, DAY),
    dydxRows(raw.ticker, overlapFrom, through)
  ]);
  const spot = uniqueSorted([
    ...raw.spot.filter(row => row[0] < overlapFrom),
    ...spotSource.map(row => [+row[0], +row[6], +row[1], +row[2], +row[3], +row[4]])
  ], row => row[0]);
  const daily = uniqueSorted([
    ...raw.daily.filter(row => row[0] < dailyFrom),
    ...dailySource.map(row => [+row[0], +row[6], row[2], row[3]])
  ], row => row[0]);
  const dydx = uniqueSorted([
    ...raw.dydx.filter(row => Date.parse(row[0]) < overlapFrom),
    ...dydxSource.map(dydxTuple)
  ], row => Date.parse(row[0]));
  validateSpot(asset, spot, through);
  validateDaily(asset, daily, dailyThrough);
  return { ...raw, through, spot, daily, dydx };
}

async function refresh(asset) {
  const file = path.join(dataDir, `${asset}.json`);
  let raw;
  if (fs.existsSync(file)) raw = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (offline) {
    if (!raw) throw new Error(`${asset}: no cached replay source at ${file}`);
    return { raw, changed: false, file };
  }
  const updated = raw ? await extend(asset, raw, targetThrough) : await bootstrap(asset, targetThrough);
  return { raw: updated, changed: updated !== raw, file };
}

function writeRaw({ raw, changed, file }) {
  if (!changed) return;
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(raw));
  fs.renameSync(temporary, file);
}

function atrSignals(raw, data, signals) {
  const spot = new Map(raw.spot.map(row => [row[0], row]));
  const dydx = new Map(data.rows.map(row => [Date.parse(row.timestamp), row]));
  let unscorable = 0;
  const selected = signals.filter(signal => {
    const at = signal.time * 1000;
    let previous = spot.get(at - (ATR_PERIOD + 1) * HOUR);
    let trueRangeSum = 0;
    for (let hoursBack = ATR_PERIOD; hoursBack > 0; hoursBack--) {
      const startedAt = at - hoursBack * HOUR;
      const row = spot.get(startedAt);
      if (!row || !previous || row[1] !== startedAt + HOUR - 1 || previous[1] !== startedAt - 1) {
        unscorable++;
        return false;
      }
      trueRangeSum += Math.max(row[3] - row[4], Math.abs(row[3] - previous[5]), Math.abs(row[4] - previous[5]));
      previous = row;
    }
    const candle = dydx.get(at);
    if (!candle || !(trueRangeSum > 0)) {
      unscorable++;
      return false;
    }
    const direction = signal.direction === 'long' ? 1 : -1;
    return direction * (candle.closeRef - candle.openRef) / (trueRangeSum / ATR_PERIOD) >= ATR_THRESHOLD;
  });
  return { signals: selected, unscorable };
}

function replay(asset, raw) {
  if (requestedThrough && targetThrough > raw.through) throw new Error(`${asset}: requested cutoff is not in the cache.`);
  const through = requestedThrough ? targetThrough : raw.through;
  const source = through === raw.through ? raw : { ...raw, through,
    spot: raw.spot.filter(row => row[1] < through), daily: raw.daily.filter(row => row[1] < through),
    dydx: raw.dydx.filter(row => Date.parse(row[0]) < through) };
  const data = replaySource(asset, { ...source, start: DYDX_GENESIS }, value => value);
  if (data.unavailable) throw new Error(data.unavailable);
  const greenAtr = atrSignals(source, data, data.greens);
  const purpleAtr = atrSignals(source, data, data.purples);
  const variants = [
    { id: 'green', signals: data.greens },
    { id: 'purple', signals: data.purples },
    { id: 'atr', signals: greenAtr.signals },
    { id: 'atr-purple', signals: purpleAtr.signals }
  ];
  const result = runWindow(data, data.from, data.through, variants);
  const strategies = Object.fromEntries(result.variants.map((row, index) => [row.id, {
    candidates: variants[index].signals.length,
    closed: row.closed,
    wins: row.wins,
    realizedGrossUsd: row.gross,
    realizedNetUsd: row.net[FEE_BPS_PER_SIDE],
    maxDrawdownUsd: row.maxDrawdown,
    open: row.open,
    openGrossUsd: row.openGross,
    incomplete: row.incomplete,
    unscorable: row.id === 'atr' ? greenAtr.unscorable : row.id === 'atr-purple' ? purpleAtr.unscorable : 0
  }]));
  const best = Object.entries(strategies).sort((left, right) => right[1].realizedNetUsd - left[1].realizedNetUsd)[0][0];
  return { asset, market: data.ticker, from: result.from, through: result.through,
    missingDydxHours: (data.through - data.from) / HOUR - data.rows.filter(row => {
      const at = Date.parse(row.timestamp);
      return at >= data.from && at < data.through;
    }).length,
    replayRestarts: result.dataGaps, best, strategies };
}

function markdown(report) {
  const modes = ['green', 'purple', 'atr', 'atr-purple'];
  const labels = { green: 'Green', purple: 'Purple', atr: 'ATR', 'atr-purple': 'ATR Purple' };
  const lines = [
    '# Active Pair Strategy Replay', '',
    `Updated: ${report.updatedAt}`, '',
    `Assumptions: $${RISK_USD} risk per entry, ${FEE_BPS_PER_SIDE} bps hypothetical fees per side, no funding or extra slippage. ` +
      'Every strategy is replayed separately with at most one open position per pair. Open P&L is excluded from the ranking.', '',
    'Simulated fills, stops and P&L use dYdX v4 hourly candles. Entry signals and the ATR reference use Binance Spot.',
    'Dates in the ranking are local to Europe/Amsterdam. Exact timestamps below are UTC.', '',
    '| Pair | Replay from | Replay through | Green | Purple | ATR | ATR Purple | Historical best |',
    '| --- | --- | --- | ---: | ---: | ---: | ---: | --- |'
  ];
  for (const pair of report.pairs) lines.push(`| ${pair.asset} | ${localDate(pair.from)} | ${localDate(pair.through)} | ${modes.map(mode => money(pair.strategies[mode].realizedNetUsd)).join(' | ')} | ${labels[pair.best]} |`);
  lines.push('', '## Coverage', '', '| Pair | From | Through | Missing dYdX hours | Replay restarts |',
    '| --- | --- | --- | ---: | ---: |');
  for (const pair of report.pairs) lines.push(`| ${pair.asset} | ${pair.from} | ${pair.through} | ${pair.missingDydxHours} | ${pair.replayRestarts.length} |`);
  lines.push('', '## Interpretation', '',
    '- “Historical best” means the highest realized net result inside that pair’s own tested period; periods differ by listing and usable dYdX coverage.',
    '- Incomplete trades around missing dYdX candles are excluded from realized P&L and remain visible in the JSON report.',
    '- Historical ranking is descriptive, not a guarantee of future profitability.', '',
    'Run `npm run update` to refresh market data and rebuild this report. Use `npm run update -- --offline` to recalculate only from the local cache.', '');
  return lines.join('\n');
}

async function main() {
  fs.mkdirSync(dataDir, { recursive: true });
  const raw = {};
  const refreshed = [];
  for (const asset of ASSETS) {
    const result = await refresh(asset);
    raw[asset] = result.raw;
    refreshed.push(result);
    console.log(`${asset}: source through ${iso(raw[asset].through)}`);
  }
  // Do not leave mixed pair cutoffs behind when one source request or validation fails.
  refreshed.forEach(writeRaw);
  const pairs = ASSETS.map(asset => replay(asset, raw[asset]));
  const report = {
    version: 1,
    updatedAt: new Date().toISOString(),
    riskBudgetUsd: RISK_USD,
    hypotheticalFeeBpsPerSide: FEE_BPS_PER_SIDE,
    atr: { period: ATR_PERIOD, threshold: ATR_THRESHOLD, source: 'dYdX signal body / prior Binance Spot mean true range' },
    pairs
  };
  fs.mkdirSync(path.dirname(reportBase), { recursive: true });
  fs.writeFileSync(`${reportBase}.json`, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(`${reportBase}.md`, markdown(report));
  console.table(pairs.flatMap(pair => Object.entries(pair.strategies).map(([strategy, row]) => ({
    pair: pair.asset, strategy, from: localDate(pair.from), through: localDate(pair.through),
    candidates: row.candidates, closed: row.closed, net: +row.realizedNetUsd.toFixed(2),
    drawdown: +row.maxDrawdownUsd.toFixed(2), incomplete: row.incomplete,
    best: strategy === pair.best ? 'YES' : ''
  }))));
  console.log(`Reports written to ${reportBase}.md and ${reportBase}.json`);
}

main().catch(error => {
  console.error('Strategy replay update failed:', error.response?.status || '', error.response?.data || error.message);
  process.exitCode = 1;
});
