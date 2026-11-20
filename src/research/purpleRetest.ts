import { latestExpectedFractalClose, type DailyFractalHistorySnapshot } from '../services/binanceDailyFractalHistory';
import { binanceSpotChartFeed, ethSpotChartFeed, solSpotChartFeed, zecSpotChartFeed } from '../services/binanceSpotChart';
import { readSpotDailyFractals } from '../services/ethGapDailyEntry';
import { PURPLE_LINE_SETUPS } from '../services/purpleLineSetup';
import type { RetestContext } from './greenRetest';

const { projectDailyFractals } = require('../../public/btc-spot-chart/fractals.js');
const { buildDailyFractalBreakouts } = require('../../public/btc-spot-chart/gap-weekly-breakouts.js');
const HOUR = 3600;
const DAY_MS = 86_400_000;
const SPOT_CLOSED_HOURS = 999;
const DAILY_CLOSED_DAYS = 999;
const feeds = { 'BTC-USD': binanceSpotChartFeed, 'ETH-USD': ethSpotChartFeed,
  'SOL-USD': solSpotChartFeed, 'ZEC-USD': zecSpotChartFeed };

type Hour = { time: number; closed: boolean; open: number; high: number; low: number; close: number };
type Daily = Pick<DailyFractalHistorySnapshot, 'symbol' | 'venue' | 'market' | 'interval' | 'stale' | 'latestClosedCandleAt' | 'records'>;

export function purpleRetestContext(green: RetestContext, hours: Hour[], daily: Daily): RetestContext {
  const { symbol } = PURPLE_LINE_SETUPS[green.market];
  if (daily.market !== green.market || daily.symbol !== symbol || daily.venue !== 'Binance Spot' || daily.interval !== '1d' ||
      daily.stale || !(Date.parse(daily.latestClosedCandleAt || '') >= latestExpectedFractalClose('1d', green.through))) {
    throw new Error(`Unverified ${green.market} Spot Daily history.`);
  }
  const yellowKeys = new Set(green.yellows.map(row => `${row.at}|${row.direction}`));
  const byHour = new Map(hours.filter(row => row.closed).map(row => [row.time, row]));
  return { ...green, signals: green.signals.flatMap(signal => {
    const cutoff = signal.closedAt * 1000;
    // Green can use a mixed intrusion; Purple must match the actual intrusion direction.
    if (!yellowKeys.has(`${cutoff}|${signal.direction}`)) return [];
    const window: Hour[] = [];
    for (let time = signal.closedAt - SPOT_CLOSED_HOURS * HOUR; time < signal.closedAt; time += HOUR) {
      const candle = byHour.get(time);
      if (!candle) throw new Error(`Missing ${green.market} Spot history for Purple retest.`);
      window.push(candle);
    }
    if (window.at(-1)?.close !== signal.close) throw new Error(`Mismatched ${green.market} Spot signal close.`);

    const lastDailyClose = latestExpectedFractalClose('1d', cutoff);
    const firstEligiblePivot = lastDailyClose + 1 - (DAILY_CLOSED_DAYS - 2) * DAY_MS;
    // Recreate the Daily snapshot as it existed at this signal, including the
    // absence of future wick hints when the Daily bar had not closed yet.
    const records = daily.records.filter(record => Date.parse(record.confirmedAt) <= lastDailyClose &&
      Date.parse(record.pivotAt) >= firstEligiblePivot).map(record =>
      record.firstBrokenAt && Date.parse(record.firstBrokenAt) >= lastDailyClose + 1
        ? { ...record, firstBrokenAt: undefined } : record);
    const projection = projectDailyFractals({ symbol, venue: daily.venue, evaluatedThrough: new Date(cutoff).toISOString(),
      daily: { ...daily, latestClosedCandleAt: new Date(lastDailyClose).toISOString(), records } }, window, symbol);
    const breakout = buildDailyFractalBreakouts(window.slice(-2), projection, cutoff, symbol).signals[0];
    return breakout?.direction === signal.direction
      ? [{ ...signal, dailyLevels: [...new Set<number>(breakout.dailyLevels.map((level: { price: number }) => level.price))].sort((a, b) => a - b) }]
      : [];
  }) };
}

export async function recentPurpleRetest(green: RetestContext, now: number) {
  const feed = feeds[green.market];
  const latest = await feed.page();
  if (!latest.candles.length) throw new Error('Spot chart history is empty.');
  const previous = await feed.page(latest.candles[0].time * 1000);
  const daily = await readSpotDailyFractals(green.market, now);
  return purpleRetestContext(green, [...previous.candles, ...latest.candles], daily);
}
