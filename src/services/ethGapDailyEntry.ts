import { binanceDailyFractalHistory, latestExpectedFractalClose } from './binanceDailyFractalHistory';
import { ethSpotChartFeed, solSpotChartFeed, SpotChartPage } from './binanceSpotChart';
import { PurpleLineMarket, purpleLineEntryEnabled, purpleLineSetupForMarket } from './purpleLineSetup';
import { ethIntrusionFuturesDeltaFeed } from './intrusionFuturesDelta';

// These are the same pure evaluators used by the purple chart overlay.
const { projectDailyFractals } = require('../../public/btc-spot-chart/fractals.js');
const { compactIntrusionHistory, buildIntrusionDailyBreakouts } = require('../../public/btc-spot-chart/intrusion-daily-breakouts.js');
const { readDeltaResponse, deltaLabel } = require('../../public/btc-spot-chart/futures-delta.js');

export const ETH_GAP_DAILY_ENTRY_ENV = 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED';
export const ETH_GAP_DAILY_MAX_AGE_MS = 5 * 60_000;
const HOUR = 3_600_000;

export type PurpleLineSignal = {
  time: number;
  closedAt: number;
  direction: 'long' | 'short';
  close: number;
  dailyLevels: { id: string; type: 'HIGH' | 'LOW'; price: number; confirmedAt: string }[];
};

export type EthGapDailySignal = PurpleLineSignal;

export const PURPLE_BODY_STOP_FRACTION = 0.25;

export function withPurpleDydxBodyQuarterStop(activePlan: any, candle: any, signal: EthGapDailySignal, referencePrice: number, asset = 'ETH'): any {
  const sizingStop = activePlan?.stop;
  if (!sizingStop?.valid || !Number.isFinite(Number(sizingStop.price)) || !(Number(sizingStop.price) > 0) ||
    !sizingStop.fractal || activePlan.status === 'invalid-stop') {
    throw new Error(sizingStop?.reason || `${asset} requires a valid original dYdX fractal for order sizing.`);
  }
  const open = Number(candle?.openRef), close = Number(candle?.closeRef);
  const high = Number(candle?.highRef), low = Number(candle?.lowRef);
  if (candle?.fractalCandleSource !== 'dydx-1h' || Date.parse(candle.timestamp) !== signal.time * 1000 ||
    signal.closedAt !== signal.time + 3600 || signal.closedAt * 1000 > Date.now() ||
    ![open, close, high, low, referencePrice].every(value => Number.isFinite(value) && value > 0) ||
    low > Math.min(open, close) || high < Math.max(open, close)) {
    throw new Error(`${asset} quarter-body SL requires valid closed dYdX OHLC from the exact Binance signal hour.`);
  }
  if (activePlan.direction !== signal.direction) throw new Error(`${asset} quarter-body SL direction does not match the entry signal.`);
  const bodyStopPrice = open + PURPLE_BODY_STOP_FRACTION * (close - open);
  if (signal.direction === 'long' ? bodyStopPrice >= referencePrice : bodyStopPrice <= referencePrice) {
    throw new Error(`${asset} dYdX quarter-body SL is not on the protective side of the current oracle price.`);
  }
  const distance = Math.abs(referencePrice - bodyStopPrice), riskPct = distance / Math.max(1, referencePrice);
  if (Number.isFinite(sizingStop.maxDistancePct) && riskPct > sizingStop.maxDistancePct) {
    throw new Error(`${asset} dYdX quarter-body SL exceeds the existing maximum stop distance.`);
  }
  // Keep sizing and the confirmed dYdX fractal anchor; replace only the execution stop price.
  return { ...activePlan, sizingStop: { ...sizingStop, fractal: { ...sizingStop.fractal } },
    stop: { ...sizingStop, price: bodyStopPrice, source: 'dydx-signal-body-quarter', distance, riskPct,
      buffer: 0, adjustedToMinDistance: false, wickGuardPrice: undefined, wickGuardSource: undefined,
      reason: 'dYdX signal-body SL at 25% from open; original confirmed fractal retained as the trailing anchor.' },
    initialStopPolicy: { mode: 'signal-body-quarter', bodyFraction: PURPLE_BODY_STOP_FRACTION, candleSource: 'dydx-1h',
      candleStartedAt: new Date(signal.time * 1000).toISOString(), candleClosedAt: new Date(signal.closedAt * 1000).toISOString(),
      open, close, bodyStopPrice, sizingStopPrice: sizingStop.price, sizingSource: 'dydx-fractal',
      fractalPlannedStopRiskUsd: activePlan.sizing?.plannedStopRiskUsd,
      bodyStopPlannedStopRiskUsd: Number(activePlan.sizing?.size) * distance } };
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
  return purpleLineEntryEnabled('ETH-USD');
}

export function freshEthGapDailySignal(signal: EthGapDailySignal, activatedAt: string, now = Date.now()): boolean {
  const close = signal.closedAt * 1000;
  return Number.isSafeInteger(close) && close % HOUR === 0 && signal.time * 1000 + HOUR === close &&
    close === Math.floor(now / HOUR) * HOUR && close > Date.parse(activatedAt) &&
    now >= close && now - close <= ETH_GAP_DAILY_MAX_AGE_MS;
}

export function evaluateEthGapDailySignal(payload: any, intrusions: any[], page: SpotChartPage, daily: any,
  now = Date.now()): EthGapDailySignal | undefined {
  return evaluatePurpleLineSignal('ETH-USD', payload, intrusions, page, daily, now);
}

export function evaluatePurpleLineSignal(market: PurpleLineMarket, payload: any, intrusions: any[], page: SpotChartPage, daily: any,
  now = Date.now()): EthGapDailySignal | undefined {
  const config = purpleLineSetupForMarket(market);
  if (!config) throw new Error('Unsupported purple line market.');
  const through = Math.floor(now / HOUR) * HOUR;
  if (payload?.source?.market !== market || payload.modelVersion !== config.modelVersion ||
    payload.quality?.causalModel !== true || payload.quality?.priceStepUsd !== config.priceStepUsd ||
    !payload.source.sourceStatuses?.some((source: any) => source.name === `Binance Spot ${config.symbol} 1H` && source.ok) ||
    page.symbol !== config.symbol || page.venue !== 'Binance Spot' || page.interval !== '1h' ||
    daily?.interval !== '1d' || daily.stale ||
    !(Date.parse(daily.latestClosedCandleAt) >= latestExpectedFractalClose('1d', now))) {
    throw new Error(`${config.asset} gap/Daily entry requires current Binance Spot ${config.symbol} sources.`);
  }
  const frame = payload.frames?.at(-1);
  const candle = page.candles.find(row => row.closed && row.time * 1000 === through - HOUR);
  if (!candle || frame?.startedAtMs !== through - HOUR ||
    Date.parse(String(frame.t).replace(' ', 'T') + 'Z') !== through - HOUR ||
    !['open', 'high', 'low', 'close'].every(key => Number(frame[key]) === Number((candle as any)[key]))) {
    throw new Error(`${config.asset} replica and Spot candles do not cover the same latest closed hour.`);
  }
  const bundle = { symbol: config.symbol, venue: 'Binance Spot', daily, evaluatedThrough: new Date(through).toISOString() };
  const projection = projectDailyFractals(bundle, page.candles, config.symbol);
  const history = compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } }, now, market);
  return buildIntrusionDailyBreakouts(page.candles, history, projection, now, market).signals
    .find((signal: EthGapDailySignal) => signal.closedAt * 1000 === through);
}

export async function readEthGapDailySignal(payload: any, intrusions: any[]): Promise<EthGapDailySignal | undefined> {
  return readPurpleLineSignal('ETH-USD', payload, intrusions);
}

export async function readPurpleLineSignal(market: PurpleLineMarket, payload: any, intrusions: any[]): Promise<EthGapDailySignal | undefined> {
  if (!purpleLineSetupForMarket(market)) throw new Error('Unsupported purple line market.');
  const now = Date.now();
  let daily = await binanceDailyFractalHistory(market, false, 'Binance Spot');
  if (!daily.stale && Date.parse(daily.latestClosedCandleAt || '') < latestExpectedFractalClose('1d', now)) {
    daily = await binanceDailyFractalHistory(market, true, 'Binance Spot');
  }
  const page = await (market === 'SOL-USD' ? solSpotChartFeed : ethSpotChartFeed).page();
  return evaluatePurpleLineSignal(market, payload, intrusions, page, daily, now);
}
