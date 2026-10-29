import { binanceDailyFractalHistory, latestExpectedFractalClose } from './binanceDailyFractalHistory';
import { ethSpotChartFeed, SpotChartPage } from './binanceSpotChart';
import { ethIntrusionFuturesDeltaFeed } from './intrusionFuturesDelta';

// These are the same pure evaluators used by the purple chart overlay.
const { projectDailyFractals } = require('../../public/btc-spot-chart/fractals.js');
const { compactIntrusionHistory, buildIntrusionDailyBreakouts } = require('../../public/btc-spot-chart/intrusion-daily-breakouts.js');
const { readDeltaResponse, deltaLabel } = require('../../public/btc-spot-chart/futures-delta.js');

export const ETH_GAP_DAILY_ENTRY_ENV = 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED';
export const ETH_GAP_DAILY_MAX_AGE_MS = 5 * 60_000;
const HOUR = 3_600_000;

export type EthGapDailySignal = {
  time: number;
  closedAt: number;
  direction: 'long' | 'short';
  close: number;
  dailyLevels: { id: string; type: 'HIGH' | 'LOW'; price: number; confirmedAt: string }[];
};

export function withEthDydxBodyMidpointStop(activePlan: any, candle: any, signal: EthGapDailySignal, referencePrice: number): any {
  const sizingStop = activePlan?.stop;
  if (!sizingStop?.valid || !Number.isFinite(Number(sizingStop.price)) || !(Number(sizingStop.price) > 0) ||
    !sizingStop.fractal || activePlan.status === 'invalid-stop') {
    throw new Error(sizingStop?.reason || 'ETH requires a valid original dYdX fractal for order sizing.');
  }
  const open = Number(candle?.openRef), close = Number(candle?.closeRef);
  const high = Number(candle?.highRef), low = Number(candle?.lowRef);
  if (candle?.fractalCandleSource !== 'dydx-1h' || Date.parse(candle.timestamp) !== signal.time * 1000 ||
    signal.closedAt !== signal.time + 3600 || signal.closedAt * 1000 > Date.now() ||
    ![open, close, high, low, referencePrice].every(value => Number.isFinite(value) && value > 0) ||
    low > Math.min(open, close) || high < Math.max(open, close)) {
    throw new Error('ETH midpoint SL requires valid closed dYdX OHLC from the exact Binance signal hour.');
  }
  if (activePlan.direction !== signal.direction) throw new Error('ETH midpoint SL direction does not match the entry signal.');
  const midpoint = (open + close) / 2;
  if (signal.direction === 'long' ? midpoint >= referencePrice : midpoint <= referencePrice) {
    throw new Error('ETH dYdX body-midpoint SL is not on the protective side of the current oracle price.');
  }
  const distance = Math.abs(referencePrice - midpoint), riskPct = distance / Math.max(1, referencePrice);
  if (Number.isFinite(sizingStop.maxDistancePct) && riskPct > sizingStop.maxDistancePct) {
    throw new Error('ETH dYdX body-midpoint SL exceeds the existing maximum stop distance.');
  }
  // Keep sizing and the confirmed dYdX fractal anchor; replace only the execution stop price.
  return { ...activePlan, sizingStop: { ...sizingStop, fractal: { ...sizingStop.fractal } },
    stop: { ...sizingStop, price: midpoint, source: 'dydx-signal-body-midpoint', distance, riskPct,
      buffer: 0, adjustedToMinDistance: false, wickGuardPrice: undefined, wickGuardSource: undefined,
      reason: 'dYdX signal-body midpoint SL; original confirmed fractal retained as the trailing anchor.' },
    initialStopPolicy: { mode: 'signal-body-midpoint', candleSource: 'dydx-1h',
      candleStartedAt: new Date(signal.time * 1000).toISOString(), candleClosedAt: new Date(signal.closedAt * 1000).toISOString(),
      open, close, midpoint, sizingStopPrice: sizingStop.price, sizingSource: 'dydx-fractal',
      fractalPlannedStopRiskUsd: activePlan.sizing?.plannedStopRiskUsd,
      midpointPlannedStopRiskUsd: Number(activePlan.sizing?.size) * distance } };
}

export type EthEntryFuturesDelta = {
  symbol: 'ETHUSDT'; venue: 'Binance USD-M Futures'; interval: '5m';
  signalTime: number; startedAt: number; closedAt: number; fetchedAt: string;
  rawDeltaPct: number; directionalDeltaPct: number; passed: boolean;
};

export async function readEthEntryFuturesDelta(signal: EthGapDailySignal): Promise<EthEntryFuturesDelta> {
  const response = await ethIntrusionFuturesDeltaFeed.snapshot([signal.time]);
  const record = readDeltaResponse(response, 'ETHUSDT').find((row: any) => row.hour === signal.time);
  if (!record || record.status !== 'ok') {
    throw new Error(`ETH final-5m Futures delta unavailable (${record?.status || 'missing'}); entry remains pending.`);
  }
  // Reuse the chart's direction normalization and exact signal-candle identity checks.
  const label = deltaLabel(signal, record);
  const directionalDeltaPct = label.directionalDeltaPct;
  if (!Number.isFinite(directionalDeltaPct)) throw new Error('Invalid ETH directional Futures delta.');
  return { symbol: 'ETHUSDT', venue: 'Binance USD-M Futures', interval: '5m', signalTime: signal.time,
    startedAt: record.candle.startedAt, closedAt: record.candle.closedAt, fetchedAt: record.candle.fetchedAt,
    rawDeltaPct: record.candle.rawDeltaPct, directionalDeltaPct, passed: directionalDeltaPct > 0 };
}

export function ethGapDailyEntryEnabled(): boolean {
  return ['true', '1', 'yes', 'on'].includes(String(process.env[ETH_GAP_DAILY_ENTRY_ENV] || '').trim().toLowerCase());
}

export function freshEthGapDailySignal(signal: EthGapDailySignal, activatedAt: string, now = Date.now()): boolean {
  const close = signal.closedAt * 1000;
  return Number.isSafeInteger(close) && close % HOUR === 0 && signal.time * 1000 + HOUR === close &&
    close === Math.floor(now / HOUR) * HOUR && close > Date.parse(activatedAt) &&
    now >= close && now - close <= ETH_GAP_DAILY_MAX_AGE_MS;
}

export function evaluateEthGapDailySignal(payload: any, intrusions: any[], page: SpotChartPage, daily: any,
  now = Date.now()): EthGapDailySignal | undefined {
  const through = Math.floor(now / HOUR) * HOUR;
  if (payload?.source?.market !== 'ETH-USD' || payload.modelVersion !== 'binance-spot-eth-liquidation-cohorts-v2.1' ||
    payload.quality?.causalModel !== true || payload.quality?.priceStepUsd !== 5 ||
    !payload.source.sourceStatuses?.some((source: any) => source.name === 'Binance Spot ETHUSDT 1H' && source.ok) ||
    page.symbol !== 'ETHUSDT' || page.venue !== 'Binance Spot' || page.interval !== '1h' ||
    daily?.interval !== '1d' || daily.stale ||
    !(Date.parse(daily.latestClosedCandleAt) >= latestExpectedFractalClose('1d', now))) {
    throw new Error('ETH gap/Daily entry requires current Binance Spot ETHUSDT sources.');
  }
  const frame = payload.frames?.at(-1);
  const candle = page.candles.find(row => row.closed && row.time * 1000 === through - HOUR);
  if (!candle || frame?.startedAtMs !== through - HOUR ||
    Date.parse(String(frame.t).replace(' ', 'T') + 'Z') !== through - HOUR ||
    !['open', 'high', 'low', 'close'].every(key => Number(frame[key]) === Number((candle as any)[key]))) {
    throw new Error('ETH replica and Spot candles do not cover the same latest closed hour.');
  }
  const bundle = { symbol: 'ETHUSDT', venue: 'Binance Spot', daily, evaluatedThrough: new Date(through).toISOString() };
  const projection = projectDailyFractals(bundle, page.candles, 'ETHUSDT');
  const history = compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } }, now, 'ETH-USD');
  return buildIntrusionDailyBreakouts(page.candles, history, projection, now, 'ETH-USD').signals
    .find((signal: EthGapDailySignal) => signal.closedAt * 1000 === through);
}

export async function readEthGapDailySignal(payload: any, intrusions: any[]): Promise<EthGapDailySignal | undefined> {
  const now = Date.now();
  let daily = await binanceDailyFractalHistory('ETH-USD', false, 'Binance Spot');
  if (!daily.stale && Date.parse(daily.latestClosedCandleAt || '') < latestExpectedFractalClose('1d', now)) {
    daily = await binanceDailyFractalHistory('ETH-USD', true, 'Binance Spot');
  }
  const page = await ethSpotChartFeed.page();
  return evaluateEthGapDailySignal(payload, intrusions, page, daily, now);
}
