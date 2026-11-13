import { binanceSpotChartFeed, ethSpotChartFeed, solSpotChartFeed, zecSpotChartFeed, SpotChartPage } from './binanceSpotChart';
import type { GreenLineSignal } from './ethGapDailyEntry';
import { PurpleLineMarket, purpleLineSetupForMarket } from './purpleLineSetup';

const { timelineFromPayload } = require('../../public/btc-spot-chart/gap-boxes.js');
const { compactIntrusionHistory, buildGapIntrusionMarkers } = require('../../public/btc-spot-chart/intrusion-daily-breakouts.js');
const HOUR = 3600;
const feeds = { 'BTC-USD': binanceSpotChartFeed, 'ETH-USD': ethSpotChartFeed,
  'SOL-USD': solSpotChartFeed, 'ZEC-USD': zecSpotChartFeed };

export function evaluateGreenLineSignal(market: PurpleLineMarket, payload: any, intrusions: any[],
  page: SpotChartPage, now = Date.now()): GreenLineSignal | undefined {
  const config = purpleLineSetupForMarket(market);
  if (!config || payload?.source?.market !== market || payload.modelVersion !== config.modelVersion ||
      payload.quality?.causalModel !== true || payload.quality?.priceStepUsd !== config.priceStepUsd ||
      !payload.source.sourceStatuses?.some((source: any) => source.name === `Binance Spot ${config.symbol} 1H` && source.ok) ||
      page.symbol !== config.symbol || page.venue !== 'Binance Spot' || page.interval !== '1h') {
    throw new Error(`${market} Green entry requires current matching Binance Spot sources.`);
  }
  const closedAt = Math.floor(now / (HOUR * 1000)) * HOUR;
  const frames = payload.frames?.slice(-2);
  const candles = [closedAt - 2 * HOUR, closedAt - HOUR].map((time, index) => {
    const frame = frames?.[index], candle = page.candles.find(c => c.time === time && c.closed);
    if (!candle || frame?.startedAtMs !== time * 1000 ||
        frame.t !== new Date(time * 1000).toISOString().slice(0, 19).replace('T', ' ') ||
        !['open', 'high', 'low', 'close'].every(key => Number(frame[key]) === candle[key as keyof typeof candle])) {
      throw new Error(`${market} Green entry is awaiting matching consecutive closed Binance Spot candles.`);
    }
    return candle;
  });
  if (!Array.isArray(payload.gaps) || payload.gaps.length !== payload.frames.length) {
    throw new Error(`${market} Green entry is awaiting matching gap frames.`);
  }
  const timeline = timelineFromPayload({ ...payload, frames, gaps: payload.gaps.slice(-2) }, market);
  const history = compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } },
    now, market, { includeGapHours: true });
  const marker = buildGapIntrusionMarkers(candles, history, timeline, now, market)
    .find((row: GreenLineSignal) => row.closedAt === closedAt);
  return marker ? { ...marker, setup: 'green-line', close: candles[1].close } : undefined;
}

export async function readGreenLineSignal(market: PurpleLineMarket, payload: any, intrusions: any[]): Promise<GreenLineSignal | undefined> {
  if (!purpleLineSetupForMarket(market)) throw new Error('Unsupported Green line market.');
  const now = Date.now();
  const page = await feeds[market].page();
  return evaluateGreenLineSignal(market, payload, intrusions, page, now);
}
