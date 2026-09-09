import { binanceGet } from './binanceHttp';

const BINANCE_FUTURES_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';
const WILLIAMS_WINDOW = 2;
const HISTORY_LIMIT = 1000;
const CACHE_TTL_MS = 15 * 60_000;

export const BINANCE_DAILY_FRACTAL_MARKETS = {
  'BTC-USD': { symbol: 'BTCUSDT' },
  'ETH-USD': { symbol: 'ETHUSDT' },
  'INJ-USD': { symbol: 'INJUSDT' },
  'SOL-USD': { symbol: 'SOLUSDT' },
  'ZEC-USD': { symbol: 'ZECUSDT' },
  'PAXG-USD': { symbol: 'XAUUSDT' },
  'XAG-USD': { symbol: 'XAGUSDT' }
} as const;

export type DailyFractalMarket = keyof typeof BINANCE_DAILY_FRACTAL_MARKETS;

export type BinanceDailyCandle = {
  openTime: number;
  closeTime: number;
  high: string;
  low: string;
};

export type DailyFractalHistoryItem = {
  id: string;
  market: DailyFractalMarket;
  symbol: string;
  type: 'HIGH' | 'LOW';
  price: number;
  priceExact: string;
  pivotAt: string;
  confirmedAt: string;
  firstBrokenAt?: string;
  supersededAt?: string;
  isLatestForType: boolean;
};

export type DailyFractalHistorySnapshot = {
  ok: true;
  market: DailyFractalMarket;
  symbol: string;
  venue: 'Binance Futures';
  interval: '1d';
  window: 2;
  fetchedAt: string;
  latestClosedCandleAt?: string;
  cached: boolean;
  records: DailyFractalHistoryItem[];
};

type CachedSnapshot = {
  storedAt: number;
  snapshot: DailyFractalHistorySnapshot;
};

const cachedSnapshots = new Map<DailyFractalMarket, CachedSnapshot>();
const pendingSnapshots = new Map<DailyFractalMarket, Promise<DailyFractalHistorySnapshot>>();

function finiteNumber(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function exactDecimal(value: unknown): string | undefined {
  const text = String(value ?? '').trim();
  return text && finiteNumber(text) !== undefined ? text : undefined;
}

export function parseBinanceDailyCandles(rows: unknown[], nowMs = Date.now()): BinanceDailyCandle[] {
  return rows
    .map((row) => {
      if (!Array.isArray(row)) return undefined;
      const openTime = finiteNumber(row[0]);
      const high = exactDecimal(row[2]);
      const low = exactDecimal(row[3]);
      const closeTime = finiteNumber(row[6]);
      if (openTime === undefined || closeTime === undefined || !high || !low || closeTime > nowMs) {
        return undefined;
      }
      return { openTime, closeTime, high, low };
    })
    .filter((row): row is BinanceDailyCandle => Boolean(row))
    .sort((left, right) => left.openTime - right.openTime);
}

export function buildDailyFractalHistory(
  candles: BinanceDailyCandle[],
  market: DailyFractalMarket = 'BTC-USD',
  symbol = BINANCE_DAILY_FRACTAL_MARKETS[market].symbol
): DailyFractalHistoryItem[] {
  const records: DailyFractalHistoryItem[] = [];

  for (let index = WILLIAMS_WINDOW; index < candles.length - WILLIAMS_WINDOW; index += 1) {
    const pivot = candles[index];
    const pivotHigh = Number(pivot.high);
    const pivotLow = Number(pivot.low);
    let isHigh = true;
    let isLow = true;

    for (let offset = -WILLIAMS_WINDOW; offset <= WILLIAMS_WINDOW; offset += 1) {
      if (offset === 0) continue;
      const neighbor = candles[index + offset];
      const neighborHigh = Number(neighbor.high);
      const neighborLow = Number(neighbor.low);

      // Match the live Williams implementation: equality against older
      // candles is allowed, while the newest equal pivot wins.
      if (offset < 0 ? pivotHigh < neighborHigh : pivotHigh <= neighborHigh) isHigh = false;
      if (offset < 0 ? pivotLow > neighborLow : pivotLow >= neighborLow) isLow = false;
    }

    const confirmedAt = new Date(candles[index + WILLIAMS_WINDOW].closeTime).toISOString();
    const pivotAt = new Date(pivot.openTime).toISOString();
    if (isHigh) {
      records.push({
        id: `${symbol}|HIGH|${pivot.openTime}`,
        market,
        symbol,
        type: 'HIGH',
        price: pivotHigh,
        priceExact: pivot.high,
        pivotAt,
        confirmedAt,
        isLatestForType: false
      });
    }
    if (isLow) {
      records.push({
        id: `${symbol}|LOW|${pivot.openTime}`,
        market,
        symbol,
        type: 'LOW',
        price: pivotLow,
        priceExact: pivot.low,
        pivotAt,
        confirmedAt,
        isLatestForType: false
      });
    }
  }

  for (const type of ['HIGH', 'LOW'] as const) {
    const typed = records.filter((record) => record.type === type);
    const latest = typed[typed.length - 1];
    if (latest) latest.isLatestForType = true;

    for (let index = 0; index < typed.length; index += 1) {
      const record = typed[index];
      const successor = typed[index + 1];
      if (successor) record.supersededAt = successor.confirmedAt;
      const confirmationMs = Date.parse(record.confirmedAt);
      const breakCandle = candles.find((candle) => {
        if (candle.closeTime <= confirmationMs) return false;
        return type === 'HIGH'
          ? Number(candle.high) > record.price
          : Number(candle.low) < record.price;
      });
      if (breakCandle) record.firstBrokenAt = new Date(breakCandle.openTime).toISOString();
    }
  }

  return records.sort((left, right) => Date.parse(right.confirmedAt) - Date.parse(left.confirmedAt));
}

async function fetchSnapshot(market: DailyFractalMarket): Promise<DailyFractalHistorySnapshot> {
  const symbol = BINANCE_DAILY_FRACTAL_MARKETS[market].symbol;
  const fetchedAt = new Date().toISOString();
  const response = await binanceGet<unknown[]>(BINANCE_FUTURES_KLINES_URL, {
    params: { symbol, interval: '1d', limit: HISTORY_LIMIT },
    timeout: 20_000
  });
  const candles = parseBinanceDailyCandles(Array.isArray(response.data) ? response.data : []);
  if (candles.length < WILLIAMS_WINDOW * 2 + 1) {
    throw new Error(`Binance ${symbol} returned only ${candles.length} closed daily candles.`);
  }

  return {
    ok: true,
    market,
    symbol,
    venue: 'Binance Futures',
    interval: '1d',
    window: WILLIAMS_WINDOW,
    fetchedAt,
    latestClosedCandleAt: new Date(candles[candles.length - 1].closeTime).toISOString(),
    cached: false,
    records: buildDailyFractalHistory(candles, market, symbol)
  };
}

export function isDailyFractalMarket(value: string): value is DailyFractalMarket {
  return Object.prototype.hasOwnProperty.call(BINANCE_DAILY_FRACTAL_MARKETS, value);
}

export async function binanceDailyFractalHistory(
  market: DailyFractalMarket,
  forceRefresh = false
): Promise<DailyFractalHistorySnapshot> {
  const now = Date.now();
  const cachedSnapshot = cachedSnapshots.get(market);
  if (!forceRefresh && cachedSnapshot && now - cachedSnapshot.storedAt < CACHE_TTL_MS) {
    return { ...cachedSnapshot.snapshot, cached: true };
  }
  const pendingSnapshot = pendingSnapshots.get(market);
  if (pendingSnapshot) return pendingSnapshot;

  const request = fetchSnapshot(market)
    .then((snapshot) => {
      cachedSnapshots.set(market, { storedAt: Date.now(), snapshot });
      return snapshot;
    })
    .catch((error) => {
      const fallback = cachedSnapshots.get(market);
      if (fallback) {
        console.warn('Binance daily fractal refresh failed; using cached snapshot.', {
          market,
          symbol: BINANCE_DAILY_FRACTAL_MARKETS[market].symbol,
          error: error instanceof Error ? error.message : String(error),
          fetchedAt: fallback.snapshot.fetchedAt
        });
        return { ...fallback.snapshot, cached: true };
      }
      throw error;
    })
    .finally(() => {
      pendingSnapshots.delete(market);
    });

  pendingSnapshots.set(market, request);
  return request;
}

export async function binanceBtcDailyFractalHistory(
  forceRefresh = false
): Promise<DailyFractalHistorySnapshot> {
  return binanceDailyFractalHistory('BTC-USD', forceRefresh);
}
