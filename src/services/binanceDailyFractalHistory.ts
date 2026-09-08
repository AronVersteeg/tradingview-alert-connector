import { binanceGet } from './binanceHttp';

const BINANCE_FUTURES_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';
const WILLIAMS_WINDOW = 2;
const HISTORY_LIMIT = 1000;
const CACHE_TTL_MS = 15 * 60_000;

export type BinanceDailyCandle = {
  openTime: number;
  closeTime: number;
  high: string;
  low: string;
};

export type DailyFractalHistoryItem = {
  id: string;
  market: 'BTC-USD';
  symbol: 'BTCUSDT';
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
  market: 'BTC-USD';
  symbol: 'BTCUSDT';
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

let cachedSnapshot: CachedSnapshot | undefined;
let pendingSnapshot: Promise<DailyFractalHistorySnapshot> | undefined;

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
  candles: BinanceDailyCandle[]
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
        id: `BTCUSDT|HIGH|${pivot.openTime}`,
        market: 'BTC-USD',
        symbol: 'BTCUSDT',
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
        id: `BTCUSDT|LOW|${pivot.openTime}`,
        market: 'BTC-USD',
        symbol: 'BTCUSDT',
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

async function fetchSnapshot(): Promise<DailyFractalHistorySnapshot> {
  const fetchedAt = new Date().toISOString();
  const response = await binanceGet<unknown[]>(BINANCE_FUTURES_KLINES_URL, {
    params: { symbol: 'BTCUSDT', interval: '1d', limit: HISTORY_LIMIT },
    timeout: 20_000
  });
  const candles = parseBinanceDailyCandles(Array.isArray(response.data) ? response.data : []);
  if (candles.length < WILLIAMS_WINDOW * 2 + 1) {
    throw new Error(`Binance BTCUSDT returned only ${candles.length} closed daily candles.`);
  }

  return {
    ok: true,
    market: 'BTC-USD',
    symbol: 'BTCUSDT',
    venue: 'Binance Futures',
    interval: '1d',
    window: WILLIAMS_WINDOW,
    fetchedAt,
    latestClosedCandleAt: new Date(candles[candles.length - 1].closeTime).toISOString(),
    cached: false,
    records: buildDailyFractalHistory(candles)
  };
}

export async function binanceBtcDailyFractalHistory(
  forceRefresh = false
): Promise<DailyFractalHistorySnapshot> {
  const now = Date.now();
  if (!forceRefresh && cachedSnapshot && now - cachedSnapshot.storedAt < CACHE_TTL_MS) {
    return { ...cachedSnapshot.snapshot, cached: true };
  }
  if (pendingSnapshot) return pendingSnapshot;

  pendingSnapshot = fetchSnapshot()
    .then((snapshot) => {
      cachedSnapshot = { storedAt: Date.now(), snapshot };
      return snapshot;
    })
    .catch((error) => {
      if (cachedSnapshot) {
        console.warn('Binance BTC daily fractal refresh failed; using cached snapshot.', {
          error: error instanceof Error ? error.message : String(error),
          fetchedAt: cachedSnapshot.snapshot.fetchedAt
        });
        return { ...cachedSnapshot.snapshot, cached: true };
      }
      throw error;
    })
    .finally(() => {
      pendingSnapshot = undefined;
    });

  return pendingSnapshot;
}
