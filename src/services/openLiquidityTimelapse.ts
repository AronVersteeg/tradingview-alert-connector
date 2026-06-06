import axios from 'axios';

const DYDX_INDEXER_URL = 'https://indexer.dydx.trade/v4';
const LEVERAGES = [3, 5, 10];

type DydxCandle = {
  startedAt: string;
  ticker: string;
  resolution: string;
  low: string;
  high: string;
  open: string;
  close: string;
  baseTokenVolume?: string;
  usdVolume?: string;
  trades?: number;
  startingOpenInterest?: string;
  orderbookMidPriceOpen?: string;
  orderbookMidPriceClose?: string;
};

type TimelapseEvent = {
  i: number;
  s: 'L' | 'S';
  l: number;
  p: number;
  a: 1;
  n: 0 | 1;
};

let cachedPayload: any | undefined;
let cachedAt = 0;

function parseNumber(value: any): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function median(values: number[]): number {
  const filtered = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!filtered.length) return 0;
  const middle = Math.floor(filtered.length / 2);
  return filtered.length % 2 ? filtered[middle] : (filtered[middle - 1] + filtered[middle]) / 2;
}

function priceKey(price: number): string {
  return price.toFixed(8).replace(/0+$/, '').replace(/\.$/, '');
}

function roundToStep(value: number, step: number): number {
  return Math.round(value / step) * step;
}

function bucketStepForPrice(price: number): number {
  if (price >= 80000) return 100;
  if (price >= 20000) return 50;
  if (price >= 5000) return 25;
  return 10;
}

function timestampForDydx(startedAt: string): string {
  const date = new Date(startedAt);
  if (!Number.isFinite(date.getTime())) return startedAt;
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

async function fetchDydxCandles(market: string, limit: number): Promise<DydxCandle[]> {
  const response = await axios.get(`${DYDX_INDEXER_URL}/candles/perpetualMarkets/${encodeURIComponent(market)}`, {
    timeout: 30000,
    params: {
      resolution: '1HOUR',
      limit
    }
  });

  const candles = response.data?.candles;
  if (!Array.isArray(candles)) {
    throw new Error('dYdX candle response did not contain candles.');
  }

  return candles
    .slice()
    .sort((a: DydxCandle, b: DydxCandle) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime());
}

function repeatCount(activity: number, sideWeight: number, leverage: number): number {
  const leverageWeight = leverage === 10 ? 1.35 : leverage === 5 ? 1.15 : 1;
  return Math.round(clamp(activity * sideWeight * leverageWeight, 1, 8));
}

export async function getOpenLiquidityTimelapsePayload(market = 'BTC-USD'): Promise<any> {
  const normalizedMarket = market.replace(/_/g, '-').toUpperCase();
  const now = Date.now();
  if (cachedPayload && now - cachedAt < 60_000 && cachedPayload.source?.market === normalizedMarket) {
    return cachedPayload;
  }

  const candles = await fetchDydxCandles(normalizedMarket, 500);
  const parsed = candles
    .map((candle) => {
      const open = parseNumber(candle.open);
      const high = parseNumber(candle.high);
      const low = parseNumber(candle.low);
      const close = parseNumber(candle.close);
      const volume = parseNumber(candle.usdVolume) || 0;
      const trades = Number(candle.trades || 0);
      const oi = parseNumber(candle.startingOpenInterest) || 0;
      if (
        open === undefined ||
        high === undefined ||
        low === undefined ||
        close === undefined ||
        close <= 0
      ) {
        return undefined;
      }
      return {
        candle,
        open,
        high,
        low,
        close,
        volume,
        trades,
        oi,
        rangePct: Math.max(0, high - low) / close
      };
    })
    .filter(Boolean) as Array<{
      candle: DydxCandle;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
      trades: number;
      oi: number;
      rangePct: number;
    }>;

  if (!parsed.length) {
    throw new Error(`No usable dYdX candles for ${normalizedMarket}.`);
  }

  const medianVolume = Math.max(1, median(parsed.map((item) => item.volume)));
  const medianTrades = Math.max(1, median(parsed.map((item) => item.trades)));
  const medianRangePct = Math.max(0.001, median(parsed.map((item) => item.rangePct)));
  const events: TimelapseEvent[] = [];
  const prices: number[] = [];
  const firstSeen = new Set<string>();
  const currentZoneCounts = new Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>();

  function addZone(frameIndex: number, side: 'L' | 'S', leverage: number, price: number, count: number): void {
    if (!Number.isFinite(price) || price <= 0 || count <= 0) return;

    const key = `${side}|${leverage}|${priceKey(price)}`;
    const isFirstSeen = !firstSeen.has(key);
    firstSeen.add(key);
    prices.push(price);

    for (let repeat = 0; repeat < count; repeat += 1) {
      events.push({
        i: frameIndex,
        s: side,
        l: leverage,
        p: price,
        a: 1,
        n: isFirstSeen && repeat === 0 ? 1 : 0
      });
    }

    const existing =
      currentZoneCounts.get(key) ||
      ({
        s: side,
        l: leverage,
        p: price,
        c: 0
      } as { s: 'L' | 'S'; l: number; p: number; c: number });
    existing.c += count;
    currentZoneCounts.set(key, existing);
  }

  parsed.forEach((item, index) => {
    prices.push(item.close);
    const previous = index > 0 ? parsed[index - 1] : undefined;
    const oiChangePct = previous?.oi ? (item.oi - previous.oi) / Math.max(1, previous.oi) : 0;
    const body = item.close - item.open;
    const candleRange = Math.max(1, item.high - item.low);
    const trend = clamp(body / candleRange, -1, 1);
    const volumeFactor = clamp(item.volume / medianVolume, 0.25, 3.5);
    const tradesFactor = clamp(item.trades / medianTrades, 0.25, 3.5);
    const rangeFactor = clamp(item.rangePct / medianRangePct, 0.35, 3.5);
    const oiFactor = clamp(1 + Math.abs(oiChangePct) * 35, 0.8, 3);
    const activity = clamp(
      (volumeFactor * 0.42 + tradesFactor * 0.2 + rangeFactor * 0.18 + oiFactor * 0.2),
      0.75,
      5.5
    );

    const longBuildWeight = clamp(1 + Math.max(0, trend) * 0.45 + Math.max(0, oiChangePct) * 18, 0.65, 1.85);
    const shortBuildWeight = clamp(1 + Math.max(0, -trend) * 0.45 + Math.max(0, oiChangePct) * 18, 0.65, 1.85);
    const anchors = [item.open, item.close];
    const mid = (item.open + item.close + item.high + item.low) / 4;
    if (Math.abs(mid - item.close) > bucketStepForPrice(item.close) * 0.5 || activity > 2.2) {
      anchors.push(mid);
    }

    for (const leverage of LEVERAGES) {
      const baseDistancePct = 0.86 / leverage;
      const volatilityBuffer = clamp(item.rangePct * 1.6, 0, 0.035);
      const step = bucketStepForPrice(item.close);

      for (const anchor of anchors) {
        const longLiq = roundToStep(anchor * (1 - baseDistancePct - volatilityBuffer), step);
        const shortLiq = roundToStep(anchor * (1 + baseDistancePct + volatilityBuffer), step);
        addZone(index, 'L', leverage, longLiq, repeatCount(activity, longBuildWeight, leverage));
        addZone(index, 'S', leverage, shortLiq, repeatCount(activity, shortBuildWeight, leverage));
      }
    }
  });

  const payload = {
    source: {
      name: 'Open data study',
      market: normalizedMarket,
      url: 'https://indexer.dydx.trade',
      api: `${DYDX_INDEXER_URL}/candles/perpetualMarkets/${normalizedMarket}`,
      method: 'public dYdX 1H candles + startingOpenInterest proxy',
      params: ['resolution=1HOUR', `limit=${candles.length}`],
      note:
        'Study-only open-data liquidity proxy built from public dYdX candles, volume, trades and starting open interest. It does not use Decentrader and does not place or size trades.'
    },
    range: {
      minPrice: Math.min(...prices),
      maxPrice: Math.max(...prices)
    },
    frames: parsed.map((item, index) => ({
      i: index,
      t: timestampForDydx(item.candle.startedAt),
      price: item.close
    })),
    events,
    topCurrentZones: Array.from(currentZoneCounts.values())
      .sort((a, b) => b.c - a.c || a.p - b.p)
      .slice(0, 40)
  };

  cachedPayload = payload;
  cachedAt = now;
  return payload;
}
