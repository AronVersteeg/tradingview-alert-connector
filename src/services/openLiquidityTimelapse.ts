import axios from 'axios';

const DYDX_INDEXER_URL = 'https://indexer.dydx.trade/v4';
const GMX_GRAPHQL_URL = 'https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql';
const GMX_BTC_MARKETS = [
  '0x47c031236e19d024b42f8AE6780E44A573170703',
  '0x7C11F78Ce78768518D743E81Fdfa2F860C6b9A77'
];

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

type GmxPosition = {
  id: string;
  isLong: boolean;
  sizeInUsd: string;
  entryPrice: string;
  leverage: string;
  openedAt: number;
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

async function fetchGmxBtcPositions(): Promise<GmxPosition[]> {
  const positions: GmxPosition[] = [];

  for (const market of GMX_BTC_MARKETS) {
    const query = `
      query GmxBtcPositions($market: String!) {
        positions(
          limit: 1000
          where: { market_eq: $market, isSnapshot_eq: false }
          orderBy: openedAt_DESC
        ) {
          id
          isLong
          sizeInUsd
          entryPrice
          leverage
          openedAt
        }
      }
    `;
    const response = await axios.post(
      GMX_GRAPHQL_URL,
      {
        query,
        variables: { market }
      },
      {
        timeout: 30000,
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json'
        }
      }
    );

    const rows = response.data?.data?.positions;
    if (Array.isArray(rows)) {
      positions.push(...(rows as GmxPosition[]));
    }
  }

  return positions;
}

function parseGmxUsd(value: string): number | undefined {
  const parsed = parseNumber(value);
  if (parsed === undefined) return undefined;
  return parsed / 1e30;
}

function parseGmxPrice(value: string): number | undefined {
  const parsed = parseNumber(value);
  if (parsed === undefined) return undefined;
  return parsed / 1e22;
}

function parseGmxLeverage(value: string): number | undefined {
  const parsed = parseNumber(value);
  if (parsed === undefined) return undefined;
  return parsed / 10000;
}

function leverageBucket(leverage: number): number {
  if (leverage <= 4) return 3;
  if (leverage <= 7.5) return 5;
  return 10;
}

function approximateLiquidationPrice(entryPrice: number, leverage: number, isLong: boolean): number | undefined {
  if (!Number.isFinite(entryPrice) || entryPrice <= 0 || !Number.isFinite(leverage) || leverage <= 1) {
    return undefined;
  }

  const maintenanceBuffer = 0.006;
  const liquidationDistance = clamp((1 / leverage) - maintenanceBuffer, 0.0035, 0.45);
  return isLong
    ? entryPrice * (1 - liquidationDistance)
    : entryPrice * (1 + liquidationDistance);
}

function positionWeight(sizeUsd: number, leverage: number): number {
  const sizeWeight = Math.log10(Math.max(10, sizeUsd)) - 0.7;
  const leverageWeight = leverage >= 15 ? 1.35 : leverage >= 8 ? 1.15 : 1;
  return Math.round(clamp(sizeWeight * leverageWeight, 1, 12));
}

function frameIndexForOpenedAt(frames: Array<{ startedAtMs: number }>, openedAtSeconds: number): number {
  const openedAtMs = openedAtSeconds * 1000;
  if (!frames.length || !Number.isFinite(openedAtMs)) return 0;
  let bestIndex = 0;

  for (let index = 0; index < frames.length; index += 1) {
    if (frames[index].startedAtMs <= openedAtMs) {
      bestIndex = index;
    } else {
      break;
    }
  }

  return bestIndex;
}

export async function getOpenLiquidityTimelapsePayload(market = 'BTC-USD'): Promise<any> {
  const normalizedMarket = market.replace(/_/g, '-').toUpperCase();
  const now = Date.now();
  if (cachedPayload && now - cachedAt < 60_000 && cachedPayload.source?.market === normalizedMarket) {
    return cachedPayload;
  }

  const [candles, gmxPositions] = await Promise.all([
    fetchDydxCandles(normalizedMarket, 500),
    fetchGmxBtcPositions()
  ]);
  const parsed = candles
    .map((candle) => {
      const close = parseNumber(candle.close);
      const startedAtMs = new Date(candle.startedAt).getTime();
      if (close === undefined || close <= 0 || !Number.isFinite(startedAtMs)) {
        return undefined;
      }
      return {
        candle,
        close,
        startedAtMs
      };
    })
    .filter(Boolean) as Array<{
      candle: DydxCandle;
      close: number;
      startedAtMs: number;
    }>;

  if (!parsed.length) {
    throw new Error(`No usable dYdX candles for ${normalizedMarket}.`);
  }

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

  for (const item of parsed) {
    prices.push(item.close);
  }

  for (const position of gmxPositions) {
    const sizeUsd = parseGmxUsd(position.sizeInUsd);
    const entryPrice = parseGmxPrice(position.entryPrice);
    const leverage = parseGmxLeverage(position.leverage);
    if (
      sizeUsd === undefined ||
      entryPrice === undefined ||
      leverage === undefined ||
      sizeUsd <= 0 ||
      entryPrice <= 0 ||
      leverage <= 1
    ) {
      continue;
    }

    const liquidationPrice = approximateLiquidationPrice(entryPrice, leverage, position.isLong);
    if (liquidationPrice === undefined) continue;

    const referencePrice = parsed[parsed.length - 1]?.close || entryPrice;
    const step = bucketStepForPrice(referencePrice);
    const roundedPrice = roundToStep(liquidationPrice, step);
    const frameIndex = frameIndexForOpenedAt(parsed, position.openedAt);
    const side = position.isLong ? 'L' : 'S';
    const leverageLevel = leverageBucket(leverage);
    addZone(frameIndex, side, leverageLevel, roundedPrice, positionWeight(sizeUsd, leverage));
  }

  const payload = {
    source: {
      name: 'Open data study',
      market: normalizedMarket,
      url: 'https://docs.gmx.io/docs/api/graphql/',
      api: GMX_GRAPHQL_URL,
      method: 'GMX active BTC positions -> approximate liquidation buckets; dYdX candles only provide the price/time axis',
      params: [`gmxPositions=${gmxPositions.length}`, `dydxCandles=${candles.length}`],
      note:
        'Study-only open-data liquidity map built from public GMX active BTC positions and approximate liquidation buckets. It does not use Decentrader and does not place or size trades.'
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
