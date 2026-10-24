import { binanceDailyFractalHistory, latestExpectedFractalClose } from './binanceDailyFractalHistory';
import { ethSpotChartFeed, SpotChartPage } from './binanceSpotChart';

// These are the same pure evaluators used by the purple chart overlay.
const { projectDailyFractals } = require('../../public/btc-spot-chart/fractals.js');
const { compactIntrusionHistory, buildIntrusionDailyBreakouts } = require('../../public/btc-spot-chart/intrusion-daily-breakouts.js');

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
