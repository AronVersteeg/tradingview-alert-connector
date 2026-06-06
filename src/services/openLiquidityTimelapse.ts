import axios from 'axios';

const DYDX_INDEXER_URL = 'https://indexer.dydx.trade/v4';
const GMX_GRAPHQL_URL = 'https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql';
const BINANCE_FAPI_URL = 'https://fapi.binance.com';
const BYBIT_API_URL = 'https://api.bybit.com';
const HYPERLIQUID_INFO_URL = 'https://api.hyperliquid.xyz/info';
const GMX_BTC_MARKETS = [
  '0x47c031236e19d024b42f8AE6780E44A573170703',
  '0x7C11F78Ce78768518D743E81Fdfa2F860C6b9A77'
];
const STUDY_FRAME_LIMIT = 500;

type DydxCandle = {
  startedAt: string;
  close: string;
};

type BinanceOi = {
  sumOpenInterestValue: string;
  timestamp: number;
};

type BinanceRatio = {
  longAccount: string;
  shortAccount: string;
  timestamp: number;
};

type BybitOi = {
  openInterest: string;
  timestamp: string;
};

type GmxPosition = {
  id: string;
  isLong: boolean;
  sizeInUsd: string;
  entryPrice: string;
  leverage: string;
  openedAt: number;
};

type StudyFrame = {
  i: number;
  t: string;
  startedAtMs: number;
  price: number;
};

type TimelapseEvent = {
  i: number;
  s: 'L' | 'S';
  l: number;
  p: number;
  a: 1;
  n: 0 | 1;
};

type StudySourceResult<T> = {
  ok: boolean;
  label: string;
  data?: T;
  error?: string;
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

function timestampForMs(ms: number): string {
  const date = new Date(ms);
  if (!Number.isFinite(date.getTime())) return '';
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

function timestampForDydx(startedAt: string): string {
  const date = new Date(startedAt);
  if (!Number.isFinite(date.getTime())) return startedAt;
  return timestampForMs(date.getTime());
}

async function safeFetch<T>(label: string, fn: () => Promise<T>): Promise<StudySourceResult<T>> {
  try {
    return { ok: true, label, data: await fn() };
  } catch (error) {
    return {
      ok: false,
      label,
      error: error instanceof Error ? error.message : String(error)
    };
  }
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

async function fetchBinanceOpenInterest(): Promise<BinanceOi[]> {
  const response = await axios.get(`${BINANCE_FAPI_URL}/futures/data/openInterestHist`, {
    timeout: 30000,
    params: {
      symbol: 'BTCUSDT',
      period: '1h',
      limit: STUDY_FRAME_LIMIT
    }
  });
  if (!Array.isArray(response.data)) throw new Error('Binance OI response is not an array.');
  return response.data;
}

async function fetchBinanceRatios(): Promise<BinanceRatio[]> {
  const response = await axios.get(`${BINANCE_FAPI_URL}/futures/data/globalLongShortAccountRatio`, {
    timeout: 30000,
    params: {
      symbol: 'BTCUSDT',
      period: '1h',
      limit: STUDY_FRAME_LIMIT
    }
  });
  if (!Array.isArray(response.data)) throw new Error('Binance ratio response is not an array.');
  return response.data;
}

async function fetchBinanceDepth(): Promise<any> {
  const response = await axios.get(`${BINANCE_FAPI_URL}/fapi/v1/depth`, {
    timeout: 20000,
    params: {
      symbol: 'BTCUSDT',
      limit: 100
    }
  });
  return {
    bids: Array.isArray(response.data?.bids) ? response.data.bids.length : 0,
    asks: Array.isArray(response.data?.asks) ? response.data.asks.length : 0,
    eventTime: response.data?.E
  };
}

async function fetchBybitOpenInterest(): Promise<BybitOi[]> {
  const response = await axios.get(`${BYBIT_API_URL}/v5/market/open-interest`, {
    timeout: 30000,
    params: {
      category: 'linear',
      symbol: 'BTCUSDT',
      intervalTime: '1h',
      limit: 200
    }
  });
  const rows = response.data?.result?.list;
  if (!Array.isArray(rows)) throw new Error('Bybit OI response did not contain list.');
  return rows.slice().sort((a: BybitOi, b: BybitOi) => Number(a.timestamp) - Number(b.timestamp));
}

async function fetchBybitDepth(): Promise<any> {
  const response = await axios.get(`${BYBIT_API_URL}/v5/market/orderbook`, {
    timeout: 20000,
    params: {
      category: 'linear',
      symbol: 'BTCUSDT',
      limit: 200
    }
  });
  return {
    bids: Array.isArray(response.data?.result?.b) ? response.data.result.b.length : 0,
    asks: Array.isArray(response.data?.result?.a) ? response.data.result.a.length : 0,
    time: response.data?.time
  };
}

async function fetchHyperliquidCandles(frames: StudyFrame[]): Promise<any[]> {
  const startTime = frames[0]?.startedAtMs || Date.now() - STUDY_FRAME_LIMIT * 3600_000;
  const endTime = (frames[frames.length - 1]?.startedAtMs || Date.now()) + 3600_000;
  const response = await axios.post(
    HYPERLIQUID_INFO_URL,
    {
      type: 'candleSnapshot',
      req: {
        coin: 'BTC',
        interval: '1h',
        startTime,
        endTime
      }
    },
    {
      timeout: 30000,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    }
  );
  if (!Array.isArray(response.data)) throw new Error('Hyperliquid candle response is not an array.');
  return response.data;
}

async function fetchHyperliquidContext(): Promise<any> {
  const response = await axios.post(
    HYPERLIQUID_INFO_URL,
    { type: 'metaAndAssetCtxs' },
    {
      timeout: 30000,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    }
  );
  return response.data;
}

async function fetchHyperliquidBook(): Promise<any> {
  const response = await axios.post(
    HYPERLIQUID_INFO_URL,
    {
      type: 'l2Book',
      coin: 'BTC',
      nSigFigs: 5
    },
    {
      timeout: 20000,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    }
  );
  return {
    bids: Array.isArray(response.data?.levels?.[0]) ? response.data.levels[0].length : 0,
    asks: Array.isArray(response.data?.levels?.[1]) ? response.data.levels[1].length : 0,
    time: response.data?.time
  };
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

function liquidationPriceFromEntry(entryPrice: number, leverage: number, isLong: boolean): number | undefined {
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

function frameIndexForTimestamp(frames: StudyFrame[], timestampMs: number): number {
  if (!frames.length || !Number.isFinite(timestampMs)) return 0;
  let bestIndex = 0;

  for (let index = 0; index < frames.length; index += 1) {
    if (frames[index].startedAtMs <= timestampMs) {
      bestIndex = index;
    } else {
      break;
    }
  }

  return bestIndex;
}

function valueAtOrBefore<T extends { timestampMs: number }>(rows: T[], timestampMs: number): T | undefined {
  let value: T | undefined;
  for (const row of rows) {
    if (row.timestampMs <= timestampMs) value = row;
    else break;
  }
  return value;
}

function addRepeatedEvent(
  events: TimelapseEvent[],
  prices: number[],
  firstSeen: Set<string>,
  currentZoneCounts: Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>,
  frameIndex: number,
  side: 'L' | 'S',
  leverage: number,
  price: number,
  count: number
): void {
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

function addEstimatedLiquidationBuild(
  events: TimelapseEvent[],
  prices: number[],
  firstSeen: Set<string>,
  currentZoneCounts: Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>,
  frameIndex: number,
  referencePrice: number,
  deltaOiUsd: number,
  longShare: number,
  sourceWeight: number
): void {
  if (!Number.isFinite(referencePrice) || referencePrice <= 0 || !Number.isFinite(deltaOiUsd) || deltaOiUsd <= 0) {
    return;
  }

  const step = bucketStepForPrice(referencePrice);
  const baseCount = Math.round(clamp(Math.log10(Math.max(10_000, deltaOiUsd)) - 3.6, 1, 18) * sourceWeight);
  const share = clamp(longShare, 0.15, 0.85);

  for (const leverage of [3, 5, 10]) {
    const leverageWeight = leverage === 10 ? 1.25 : leverage === 5 ? 1.05 : 0.75;
    const longCount = Math.max(1, Math.round(baseCount * share * leverageWeight));
    const shortCount = Math.max(1, Math.round(baseCount * (1 - share) * leverageWeight));
    const longLiq = liquidationPriceFromEntry(referencePrice, leverage, true);
    const shortLiq = liquidationPriceFromEntry(referencePrice, leverage, false);

    if (longLiq) {
      addRepeatedEvent(
        events,
        prices,
        firstSeen,
        currentZoneCounts,
        frameIndex,
        'L',
        leverage,
        roundToStep(longLiq, step),
        longCount
      );
    }

    if (shortLiq) {
      addRepeatedEvent(
        events,
        prices,
        firstSeen,
        currentZoneCounts,
        frameIndex,
        'S',
        leverage,
        roundToStep(shortLiq, step),
        shortCount
      );
    }
  }
}

function buildFrames(candles: DydxCandle[]): StudyFrame[] {
  return candles
    .map((candle, index) => {
      const price = parseNumber(candle.close);
      const startedAtMs = new Date(candle.startedAt).getTime();
      if (price === undefined || price <= 0 || !Number.isFinite(startedAtMs)) return undefined;
      return {
        i: index,
        t: timestampForDydx(candle.startedAt),
        startedAtMs,
        price
      } as StudyFrame;
    })
    .filter(Boolean) as StudyFrame[];
}

function normalizedBinanceOi(rows: BinanceOi[]): Array<{ timestampMs: number; oiUsd: number }> {
  return rows
    .map((row) => ({
      timestampMs: Number(row.timestamp),
      oiUsd: Number(row.sumOpenInterestValue)
    }))
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.oiUsd))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function normalizedBinanceRatios(rows: BinanceRatio[]): Array<{ timestampMs: number; longShare: number }> {
  return rows
    .map((row) => ({
      timestampMs: Number(row.timestamp),
      longShare: Number(row.longAccount)
    }))
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.longShare))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function normalizedBybitOi(rows: BybitOi[], frames: StudyFrame[]): Array<{ timestampMs: number; oiUsd: number }> {
  return rows
    .map((row) => {
      const timestampMs = Number(row.timestamp);
      const btcOi = Number(row.openInterest);
      const frame = valueAtOrBefore(frames, timestampMs) || frames[frames.length - 1];
      return {
        timestampMs,
        oiUsd: btcOi * (frame?.price || 0)
      };
    })
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.oiUsd))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function hyperliquidContextForBtc(context: any): any | undefined {
  const universe = context?.[0]?.universe;
  const assetContexts = context?.[1];
  if (!Array.isArray(universe) || !Array.isArray(assetContexts)) return undefined;
  const index = universe.findIndex((asset: any) => String(asset?.name || '').toUpperCase() === 'BTC');
  return index >= 0 ? assetContexts[index] : undefined;
}

export async function getOpenLiquidityTimelapsePayload(market = 'BTC-USD'): Promise<any> {
  const normalizedMarket = market.replace(/_/g, '-').toUpperCase();
  const now = Date.now();
  if (cachedPayload && now - cachedAt < 60_000 && cachedPayload.source?.market === normalizedMarket) {
    return cachedPayload;
  }

  const candles = await fetchDydxCandles(normalizedMarket, STUDY_FRAME_LIMIT);
  const frames = buildFrames(candles);
  if (!frames.length) {
    throw new Error(`No usable dYdX candles for ${normalizedMarket}.`);
  }

  const [
    gmxPositionsResult,
    binanceOiResult,
    binanceRatioResult,
    binanceDepthResult,
    bybitOiResult,
    bybitDepthResult,
    hyperCandlesResult,
    hyperContextResult,
    hyperBookResult
  ] = await Promise.all([
    safeFetch('GMX positions', fetchGmxBtcPositions),
    safeFetch('Binance OI', fetchBinanceOpenInterest),
    safeFetch('Binance ratios', fetchBinanceRatios),
    safeFetch('Binance depth', fetchBinanceDepth),
    safeFetch('Bybit OI', fetchBybitOpenInterest),
    safeFetch('Bybit depth', fetchBybitDepth),
    safeFetch('Hyperliquid candles', () => fetchHyperliquidCandles(frames)),
    safeFetch('Hyperliquid context', fetchHyperliquidContext),
    safeFetch('Hyperliquid book', fetchHyperliquidBook)
  ]);

  const events: TimelapseEvent[] = [];
  const prices: number[] = frames.map((frame) => frame.price);
  const firstSeen = new Set<string>();
  const currentZoneCounts = new Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>();

  const binanceOi = normalizedBinanceOi(binanceOiResult.data || []);
  const binanceRatios = normalizedBinanceRatios(binanceRatioResult.data || []);
  for (let index = 1; index < binanceOi.length; index += 1) {
    const current = binanceOi[index];
    const previous = binanceOi[index - 1];
    const deltaOiUsd = current.oiUsd - previous.oiUsd;
    const frameIndex = frameIndexForTimestamp(frames, current.timestampMs);
    const frame = frames[frameIndex];
    const ratio = valueAtOrBefore(binanceRatios, current.timestampMs);
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      frame?.price || 0,
      deltaOiUsd,
      ratio?.longShare ?? 0.5,
      1.4
    );
  }

  const bybitOi = normalizedBybitOi(bybitOiResult.data || [], frames);
  for (let index = 1; index < bybitOi.length; index += 1) {
    const current = bybitOi[index];
    const previous = bybitOi[index - 1];
    const deltaOiUsd = current.oiUsd - previous.oiUsd;
    const frameIndex = frameIndexForTimestamp(frames, current.timestampMs);
    const frame = frames[frameIndex];
    const previousFrame = frames[Math.max(0, frameIndex - 1)];
    const trendLongShare = frame && previousFrame && frame.price > previousFrame.price ? 0.57 : 0.43;
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      frame?.price || 0,
      deltaOiUsd,
      trendLongShare,
      1.15
    );
  }

  const hyperCandles = Array.isArray(hyperCandlesResult.data) ? hyperCandlesResult.data : [];
  for (const candle of hyperCandles) {
    const volumeBtc = Number(candle?.v || 0);
    const close = Number(candle?.c || 0);
    const open = Number(candle?.o || 0);
    const timestampMs = Number(candle?.t || 0);
    const frameIndex = frameIndexForTimestamp(frames, timestampMs);
    const longShare = close > open ? 0.56 : close < open ? 0.44 : 0.5;
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      close || frames[frameIndex]?.price || 0,
      volumeBtc * close * 0.06,
      longShare,
      0.55
    );
  }

  const gmxPositions = gmxPositionsResult.data || [];
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

    const liquidationPrice = liquidationPriceFromEntry(entryPrice, leverage, position.isLong);
    if (liquidationPrice === undefined) continue;

    const referencePrice = frames[frames.length - 1]?.price || entryPrice;
    const step = bucketStepForPrice(referencePrice);
    const roundedPrice = roundToStep(liquidationPrice, step);
    const frameIndex = frameIndexForTimestamp(frames, position.openedAt * 1000);
    addRepeatedEvent(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      position.isLong ? 'L' : 'S',
      leverageBucket(leverage),
      roundedPrice,
      Math.max(1, Math.round(positionWeight(sizeUsd, leverage) * 0.75))
    );
  }

  const hyperContext = hyperliquidContextForBtc(hyperContextResult.data);
  const latestFrame = frames[frames.length - 1];
  const hyperOiUsd = Number(hyperContext?.openInterest || 0) * (latestFrame?.price || 0);
  if (Number.isFinite(hyperOiUsd) && hyperOiUsd > 0 && latestFrame) {
    const premium = Number(hyperContext?.premium || 0);
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      latestFrame.i,
      latestFrame.price,
      hyperOiUsd * 0.008,
      premium >= 0 ? 0.55 : 0.45,
      0.75
    );
  }

  const sourceStatuses = [
    gmxPositionsResult,
    binanceOiResult,
    binanceRatioResult,
    binanceDepthResult,
    bybitOiResult,
    bybitDepthResult,
    hyperCandlesResult,
    hyperContextResult,
    hyperBookResult
  ].map((source) => ({
    label: source.label,
    ok: source.ok,
    error: source.error
  }));

  const payload = {
    source: {
      name: 'Public perp study',
      market: normalizedMarket,
      url: 'https://docs.gmx.io/docs/api/graphql/',
      api: 'GMX GraphQL + Binance Futures public REST + Bybit public REST + Hyperliquid public info + dYdX indexer',
      method:
        'Multi-source public perp liquidity estimate: CEX OI buildups -> estimated liquidation buckets, GMX active positions -> approximate liquidation buckets, dYdX candles -> price/time axis.',
      params: [
        `frames=${frames.length}`,
        `events=${events.length}`,
        `gmxPositions=${gmxPositions.length}`,
        `binanceOi=${binanceOi.length}`,
        `bybitOi=${bybitOi.length}`,
        `hyperCandles=${hyperCandles.length}`
      ],
      sourceStatuses,
      marketDepth: {
        binance: binanceDepthResult.data || null,
        bybit: bybitDepthResult.data || null,
        hyperliquid: hyperBookResult.data || null
      },
      note:
        'Study-only public perp liquidity estimate from GMX positions, Binance/Bybit open-interest buildups and Hyperliquid public context. Orderbooks are collected as diagnostics but are not drawn as gap histograms. It does not use Decentrader and does not place or size trades.'
    },
    range: {
      minPrice: Math.min(...prices),
      maxPrice: Math.max(...prices)
    },
    frames: frames.map((frame, index) => ({
      i: index,
      t: frame.t,
      price: frame.price
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
