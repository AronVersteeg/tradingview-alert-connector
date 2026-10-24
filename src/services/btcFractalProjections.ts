import {
  binanceDailyFractalHistory, binanceWeeklyFractalHistory, binanceMonthlyFractalHistory,
  DailyFractalHistorySnapshot, latestExpectedFractalClose
} from './binanceDailyFractalHistory';

const HOUR_MS = 3_600_000;

export type BtcFractalProjectionSnapshot = {
  symbol: 'BTCUSDT' | 'ETHUSDT';
  venue: 'Binance Spot';
  checkedAt: string;
  evaluatedThrough: string;
  daily: DailyFractalHistorySnapshot;
  weekly: DailyFractalHistorySnapshot;
  monthly: DailyFractalHistorySnapshot;
};

export class BtcFractalProjectionFeed {
  private cached: BtcFractalProjectionSnapshot | undefined;
  private pending: Promise<BtcFractalProjectionSnapshot> | undefined;
  private daily: (force?: boolean) => Promise<DailyFractalHistorySnapshot>;
  private weekly: (force?: boolean) => Promise<DailyFractalHistorySnapshot>;
  private monthly: (force?: boolean) => Promise<DailyFractalHistorySnapshot>;
  private symbol: 'BTCUSDT' | 'ETHUSDT';

  constructor(
    daily?: (force?: boolean) => Promise<DailyFractalHistorySnapshot>,
    weekly?: (force?: boolean) => Promise<DailyFractalHistorySnapshot>,
    monthly?: (force?: boolean) => Promise<DailyFractalHistorySnapshot>,
    market: 'BTC-USD' | 'ETH-USD' = 'BTC-USD'
  ) {
    if (!['BTC-USD', 'ETH-USD'].includes(market)) throw new Error('Unsupported Spot fractal market');
    this.symbol = market === 'ETH-USD' ? 'ETHUSDT' : 'BTCUSDT';
    this.daily = daily || ((force = false) => binanceDailyFractalHistory(market, force, 'Binance Spot'));
    this.weekly = weekly || ((force = false) => binanceWeeklyFractalHistory(market, force));
    this.monthly = monthly || ((force = false) => binanceMonthlyFractalHistory(market, force));
  }

  async snapshot(): Promise<BtcFractalProjectionSnapshot> {
    const hour = Math.floor(Date.now() / HOUR_MS) * HOUR_MS;
    if (this.cached?.evaluatedThrough === new Date(hour).toISOString()) return this.cached;
    if (this.pending) return this.pending;
    this.pending = this.fetch().then((snapshot) => {
      this.cached = snapshot;
      return snapshot;
    }).finally(() => { this.pending = undefined; });
    return this.pending;
  }

  private async fetch(): Promise<BtcFractalProjectionSnapshot> {
    const evaluatedAt = Date.now();
    const snapshots: DailyFractalHistorySnapshot[] = [];
    for (const [interval, read] of [['1d', this.daily], ['1w', this.weekly], ['1M', this.monthly]] as const) {
      let snapshot = await read();
      // Higher-timeframe caches can straddle the UTC close by less than their TTL.
      if (!snapshot.stale && Date.parse(snapshot.latestClosedCandleAt || '') < latestExpectedFractalClose(interval, evaluatedAt)) {
        snapshot = await read(true);
      }
      if (snapshot.venue !== 'Binance Spot' || snapshot.symbol !== this.symbol || snapshot.interval !== interval ||
        snapshot.stale || !Number.isFinite(Date.parse(snapshot.latestClosedCandleAt || '')) ||
        Date.parse(snapshot.latestClosedCandleAt!) < latestExpectedFractalClose(interval, evaluatedAt)) {
        throw new Error(`${this.symbol} Spot ${interval} fractals are not current; projections will not be advanced.`);
      }
      snapshots.push(snapshot);
    }
    const now = Date.now();
    return {
      symbol: this.symbol, venue: 'Binance Spot', checkedAt: new Date(now).toISOString(),
      evaluatedThrough: new Date(Math.floor(evaluatedAt / HOUR_MS) * HOUR_MS).toISOString(),
      daily: snapshots[0], weekly: snapshots[1], monthly: snapshots[2]
    };
  }
}

export const btcFractalProjectionFeed = new BtcFractalProjectionFeed();
export const ethFractalProjectionFeed = new BtcFractalProjectionFeed(undefined, undefined, undefined, 'ETH-USD');
