import {
  binanceDailyFractalHistory, binanceWeeklyFractalHistory,
  DailyFractalHistorySnapshot, latestExpectedFractalClose
} from './binanceDailyFractalHistory';

const HOUR_MS = 3_600_000;

export type BtcFractalProjectionSnapshot = {
  symbol: 'BTCUSDT';
  venue: 'Binance Spot';
  checkedAt: string;
  evaluatedThrough: string;
  daily: DailyFractalHistorySnapshot;
  weekly: DailyFractalHistorySnapshot;
};

export class BtcFractalProjectionFeed {
  private cached: BtcFractalProjectionSnapshot | undefined;
  private pending: Promise<BtcFractalProjectionSnapshot> | undefined;

  constructor(
    private daily = (force = false) => binanceDailyFractalHistory('BTC-USD', force, 'Binance Spot'),
    private weekly = (force = false) => binanceWeeklyFractalHistory('BTC-USD', force)
  ) {}

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
    for (const [interval, read] of [['1d', this.daily], ['1w', this.weekly]] as const) {
      let snapshot = await read();
      // Daily/Weekly caches can straddle the UTC close by less than their TTL.
      if (!snapshot.stale && Date.parse(snapshot.latestClosedCandleAt || '') < latestExpectedFractalClose(interval, evaluatedAt)) {
        snapshot = await read(true);
      }
      if (snapshot.venue !== 'Binance Spot' || snapshot.symbol !== 'BTCUSDT' || snapshot.interval !== interval ||
        snapshot.stale || !Number.isFinite(Date.parse(snapshot.latestClosedCandleAt || '')) ||
        Date.parse(snapshot.latestClosedCandleAt!) < latestExpectedFractalClose(interval, evaluatedAt)) {
        throw new Error(`BTC Spot ${interval} fractals are not current; projections will not be advanced.`);
      }
      snapshots.push(snapshot);
    }
    const now = Date.now();
    return {
      symbol: 'BTCUSDT', venue: 'Binance Spot', checkedAt: new Date(now).toISOString(),
      evaluatedThrough: new Date(Math.floor(evaluatedAt / HOUR_MS) * HOUR_MS).toISOString(),
      daily: snapshots[0], weekly: snapshots[1]
    };
  }
}

export const btcFractalProjectionFeed = new BtcFractalProjectionFeed();
