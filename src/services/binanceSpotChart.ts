import { binanceGet } from './binanceHttp';

const URL = 'https://api.binance.com/api/v3/klines';
const HOUR_MS = 3_600_000;
const PAGE_SIZE = 1000;
const MAX_CACHE_PAGES = 8;

export type SpotChartCandle = {
  time: number;
  closeTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  quoteVolume: number;
  closed: boolean;
};

export function parseSpotChartCandles(rows: unknown[], nowMs = Date.now()): SpotChartCandle[] {
  const candles = new Map<number, SpotChartCandle>();
  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    const [start, open, high, low, close, volume, end, quoteVolume] = row.map(Number);
    if (![start, open, high, low, close, volume, end, quoteVolume].every(Number.isFinite) ||
      start <= 0 || start % HOUR_MS !== 0 || start > nowMs || end !== start + HOUR_MS - 1 ||
      Math.min(open, high, low, close) <= 0 || high < Math.max(open, close) ||
      low > Math.min(open, close) || high < low || volume < 0 || quoteVolume < 0) continue;
    candles.set(start, {
      time: start / 1000, closeTime: end, open, high, low, close, volume, quoteVolume,
      closed: end < nowMs
    });
  }
  return [...candles.values()].sort((a, b) => a.time - b.time);
}

export type SpotChartPage = {
  symbol: 'BTCUSDT' | 'ETHUSDT' | 'SOLUSDT';
  venue: 'Binance Spot';
  interval: '1h';
  fetchedAt: string;
  candles: SpotChartCandle[];
  hasMore: boolean;
};

// Shared read-only Spot data: bounded memory, no disk writes or trading callbacks.
export class BinanceSpotChartFeed {
  constructor(readonly symbol: 'BTCUSDT' | 'ETHUSDT' | 'SOLUSDT' = 'BTCUSDT') {
    if (!['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].includes(symbol)) throw new Error('Unsupported Spot chart symbol');
  }
  private cache = new Map<number, { expiresAt: number; page: SpotChartPage }>();
  private pending = new Map<number, Promise<SpotChartPage>>();

  async page(before?: number): Promise<SpotChartPage> {
    if (before !== undefined && (!Number.isSafeInteger(before) || before <= 0 || before > Date.now())) {
      throw new Error('before must be a past Unix timestamp in milliseconds.');
    }
    const endTime = before === undefined ? undefined : Math.floor(before / HOUR_MS) * HOUR_MS - 1;
    const key = endTime ?? 0;
    const cached = this.cache.get(key);
    if (cached && cached.expiresAt > Date.now()) return cached.page;
    const pending = this.pending.get(key);
    if (pending) return pending;
    if (this.pending.size >= MAX_CACHE_PAGES) throw new Error('Chart history is busy; retry shortly.');
    const run = this.fetch(endTime).then((page) => {
      this.cache.delete(key);
      this.cache.set(key, { expiresAt: Date.now() + (key ? 300_000 : 15_000), page });
      while (this.cache.size > MAX_CACHE_PAGES) this.cache.delete(this.cache.keys().next().value);
      return page;
    }).finally(() => this.pending.delete(key));
    this.pending.set(key, run);
    return run;
  }

  private async fetch(endTime?: number): Promise<SpotChartPage> {
    const fetchedAt = new Date().toISOString();
    const response = await binanceGet<unknown[]>(URL, {
      timeout: 15_000,
      params: { symbol: this.symbol, interval: '1h', limit: PAGE_SIZE, ...(endTime === undefined ? {} : { endTime }) }
    });
    if (!Array.isArray(response.data)) throw new Error('Binance returned invalid Spot candle data.');
    const candles = parseSpotChartCandles(response.data);
    if (response.data.length && !candles.length) throw new Error(`Binance returned no valid ${this.symbol} Spot candles.`);
    return {
      symbol: this.symbol, venue: 'Binance Spot', interval: '1h', fetchedAt, candles,
      hasMore: candles.length === PAGE_SIZE
    };
  }
}

export const binanceSpotChartFeed = new BinanceSpotChartFeed();
export const ethSpotChartFeed = new BinanceSpotChartFeed('ETHUSDT');
export const solSpotChartFeed = new BinanceSpotChartFeed('SOLUSDT');
