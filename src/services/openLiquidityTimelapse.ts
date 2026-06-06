import axios from 'axios';

const DYDX_INDEXER_URL = 'https://indexer.dydx.trade/v4';
const GMX_GRAPHQL_URL = 'https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql';
const BINANCE_FAPI_URL = 'https://fapi.binance.com';
const BYBIT_API_URL = 'https://api.bybit.com';
const HYPERLIQUID_INFO_URL = 'https://api.hyperliquid.xyz/info';
const OKX_API_URL = 'https://www.okx.com';
const BITGET_API_URL = 'https://api.bitget.com';
const DEFAULT_ACTIVE_STUDY_SOURCES = ['bybit-oi', 'hyper-oi', 'bitget-oi'];
const GMX_BTC_MARKETS = [
  '0x47c031236e19d024b42f8AE6780E44A573170703',
  '0x7C11F78Ce78768518D743E81Fdfa2F860C6b9A77'
];
const STUDY_FRAME_LIMIT = 500;

type DydxCandle = {
  startedAt: string;
  low?: string;
  high?: string;
  usdVolume?: string;
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

type BybitRatio = {
  buyRatio: string;
  sellRatio: string;
  timestamp: string;
};

type OkxOpenInterestVolume = [string, string, string];
type OkxLongShortRatio = [string, string];

type BitgetOpenInterest = {
  symbol: string;
  openInterest: string;
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
  low: number;
  high: number;
  volumeUsd: number;
};

type TimelapseEvent = {
  i: number;
  s: 'L' | 'S';
  l: number;
  p: number;
  a: 0 | 1;
  n: 0 | 1;
  z?: string;
};

type MarketStateFrame = {
  i: number;
  t: number;
  price: number;
  oiUsd?: number;
  oiDropFromPrevious?: number;
  rangePct: number;
  impulse: number;
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

async function fetchBinanceTopAccountRatios(): Promise<BinanceRatio[]> {
  const response = await axios.get(`${BINANCE_FAPI_URL}/futures/data/topLongShortAccountRatio`, {
    timeout: 30000,
    params: {
      symbol: 'BTCUSDT',
      period: '1h',
      limit: STUDY_FRAME_LIMIT
    }
  });
  if (!Array.isArray(response.data)) throw new Error('Binance top account ratio response is not an array.');
  return response.data;
}

async function fetchBinanceTopPositionRatios(): Promise<BinanceRatio[]> {
  const response = await axios.get(`${BINANCE_FAPI_URL}/futures/data/topLongShortPositionRatio`, {
    timeout: 30000,
    params: {
      symbol: 'BTCUSDT',
      period: '1h',
      limit: STUDY_FRAME_LIMIT
    }
  });
  if (!Array.isArray(response.data)) throw new Error('Binance top position ratio response is not an array.');
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

async function fetchBybitAccountRatio(): Promise<BybitRatio[]> {
  const response = await axios.get(`${BYBIT_API_URL}/v5/market/account-ratio`, {
    timeout: 30000,
    params: {
      category: 'linear',
      symbol: 'BTCUSDT',
      period: '1h',
      limit: 200
    }
  });
  const rows = response.data?.result?.list;
  if (!Array.isArray(rows)) throw new Error('Bybit account ratio response did not contain list.');
  return rows.slice().sort((a: BybitRatio, b: BybitRatio) => Number(a.timestamp) - Number(b.timestamp));
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

async function fetchOkxOpenInterestVolume(): Promise<OkxOpenInterestVolume[]> {
  const response = await axios.get(`${OKX_API_URL}/api/v5/rubik/stat/contracts/open-interest-volume`, {
    timeout: 30000,
    params: {
      ccy: 'BTC',
      period: '1H'
    }
  });
  const rows = response.data?.data;
  if (!Array.isArray(rows)) throw new Error('OKX open-interest response did not contain data.');
  return rows.slice().sort((a: OkxOpenInterestVolume, b: OkxOpenInterestVolume) => Number(a[0]) - Number(b[0]));
}

async function fetchOkxLongShortRatio(): Promise<OkxLongShortRatio[]> {
  const response = await axios.get(`${OKX_API_URL}/api/v5/rubik/stat/contracts/long-short-account-ratio`, {
    timeout: 30000,
    params: {
      ccy: 'BTC',
      period: '1H'
    }
  });
  const rows = response.data?.data;
  if (!Array.isArray(rows)) throw new Error('OKX long-short ratio response did not contain data.');
  return rows.slice().sort((a: OkxLongShortRatio, b: OkxLongShortRatio) => Number(a[0]) - Number(b[0]));
}

async function fetchBitgetOpenInterest(): Promise<BitgetOpenInterest[]> {
  const response = await axios.get(`${BITGET_API_URL}/api/v3/market/open-interest`, {
    timeout: 20000,
    params: {
      category: 'USDT-FUTURES',
      symbol: 'BTCUSDT'
    }
  });
  const rows = response.data?.data?.list;
  if (!Array.isArray(rows)) throw new Error('Bitget open-interest response did not contain list.');
  return rows;
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

function frameAtOrBefore(frames: StudyFrame[], timestampMs: number): StudyFrame | undefined {
  let value: StudyFrame | undefined;
  for (const frame of frames) {
    if (frame.startedAtMs <= timestampMs) value = frame;
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
  count: number,
  source = 'derived'
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
      n: isFirstSeen && repeat === 0 ? 1 : 0,
      z: source
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
  sourceWeight: number,
  source = 'cex-oi'
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
        longCount,
        source
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
        shortCount,
        source
      );
    }
  }
}

function buildFrames(candles: DydxCandle[]): StudyFrame[] {
  return candles
    .map((candle, index) => {
      const price = parseNumber(candle.close);
      const low = parseNumber(candle.low);
      const high = parseNumber(candle.high);
      const volumeUsd = parseNumber(candle.usdVolume);
      const startedAtMs = new Date(candle.startedAt).getTime();
      if (price === undefined || price <= 0 || !Number.isFinite(startedAtMs)) return undefined;
      return {
        i: index,
        t: timestampForDydx(candle.startedAt),
        startedAtMs,
        price,
        low: low !== undefined && low > 0 ? low : price,
        high: high !== undefined && high > 0 ? high : price,
        volumeUsd: volumeUsd !== undefined && volumeUsd > 0 ? volumeUsd : 0
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

function normalizedBybitRatios(rows: BybitRatio[]): Array<{ timestampMs: number; longShare: number }> {
  return rows
    .map((row) => ({
      timestampMs: Number(row.timestamp),
      longShare: Number(row.buyRatio)
    }))
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.longShare))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function normalizedBybitOi(rows: BybitOi[], frames: StudyFrame[]): Array<{ timestampMs: number; oiUsd: number }> {
  return rows
    .map((row) => {
      const timestampMs = Number(row.timestamp);
      const btcOi = Number(row.openInterest);
      const frame = frameAtOrBefore(frames, timestampMs) || frames[frames.length - 1];
      return {
        timestampMs,
        oiUsd: btcOi * (frame?.price || 0)
      };
    })
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.oiUsd))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function normalizedOkxOpenInterest(rows: OkxOpenInterestVolume[]): Array<{ timestampMs: number; oiUsd: number }> {
  return rows
    .map((row) => ({
      timestampMs: Number(row[0]),
      oiUsd: Number(row[1])
    }))
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.oiUsd))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function normalizedOkxRatios(rows: OkxLongShortRatio[]): Array<{ timestampMs: number; longShare: number }> {
  return rows
    .map((row) => {
      const ratio = Number(row[1]);
      return {
        timestampMs: Number(row[0]),
        longShare: ratio > 0 ? ratio / (1 + ratio) : 0.5
      };
    })
    .filter((row) => Number.isFinite(row.timestampMs) && Number.isFinite(row.longShare))
    .sort((a, b) => a.timestampMs - b.timestampMs);
}

function weightedLongShareAt(
  timestampMs: number,
  sources: Array<{ rows: Array<{ timestampMs: number; longShare: number }>; weight: number }>
): number | undefined {
  let weighted = 0;
  let totalWeight = 0;

  for (const source of sources) {
    const value = valueAtOrBefore(source.rows, timestampMs);
    if (value && Number.isFinite(value.longShare)) {
      weighted += clamp(value.longShare, 0.15, 0.85) * source.weight;
      totalWeight += source.weight;
    }
  }

  return totalWeight > 0 ? weighted / totalWeight : undefined;
}

function buildMarketStateFrames(
  frames: StudyFrame[],
  oiSources: Array<Array<{ timestampMs: number; oiUsd: number }>>
): MarketStateFrame[] {
  const stateFrames: MarketStateFrame[] = [];
  const ranges = frames.map((frame) => Math.max(0, (frame.high - frame.low) / Math.max(1, frame.price)));

  for (const frame of frames) {
    const oiValues = oiSources
      .map((source) => valueAtOrBefore(source, frame.startedAtMs)?.oiUsd)
      .filter((value): value is number => value !== undefined && Number.isFinite(value) && value > 0);
    const oiUsd = oiValues.length ? oiValues.reduce((sum, value) => sum + value, 0) : undefined;
    const start = Math.max(0, frame.i - 24);
    const recentRanges = ranges.slice(start, frame.i + 1).filter((value) => value > 0);
    const averageRange =
      recentRanges.length > 0 ? recentRanges.reduce((sum, value) => sum + value, 0) / recentRanges.length : ranges[frame.i] || 0;
    const rangePct = ranges[frame.i] || 0;
    const volumeFactor = frame.volumeUsd > 0 ? Math.log10(Math.max(10_000, frame.volumeUsd)) / 7 : 0.6;
    const rangeFactor = averageRange > 0 ? rangePct / averageRange : 1;

    stateFrames.push({
      i: frame.i,
      t: frame.startedAtMs,
      price: frame.price,
      oiUsd,
      rangePct,
      impulse: clamp(rangeFactor * volumeFactor, 0, 4)
    });
  }

  for (let index = 1; index < stateFrames.length; index += 1) {
    const previous = stateFrames[index - 1].oiUsd;
    const current = stateFrames[index].oiUsd;
    stateFrames[index].oiDropFromPrevious =
      previous && current && previous > 0 ? Math.max(0, (previous - current) / previous) : 0;
  }

  return stateFrames;
}

function deterministicUnit(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) / 4294967295;
}

function liquidationHalfLifeHours(event: TimelapseEvent): number {
  if (event.z === 'gmx-position') return 720;
  if (event.l >= 10) return 120;
  if (event.l >= 5) return 216;
  return 336;
}

function sourceFloor(event: TimelapseEvent): number {
  if (event.z === 'gmx-position') return 0.55;
  if (event.z === 'hyper-volume') return 0.18;
  return 0.25;
}

function activeStudySources(): Set<string> {
  const configured = String(process.env.OPEN_LIQUIDITY_ACTIVE_SOURCES || '').trim();
  const sources = configured
    ? configured
        .split(',')
        .map((source) => source.trim())
        .filter(Boolean)
    : DEFAULT_ACTIVE_STUDY_SOURCES;
  return new Set(sources);
}

function markLifecycleLiquidationEvents(
  events: TimelapseEvent[],
  frames: StudyFrame[],
  marketStateFrames: MarketStateFrame[]
): { events: TimelapseEvent[]; inactive: Record<string, number> } {
  if (!events.length || !frames.length) {
    return {
      events,
      inactive: {
        swept: 0,
        impulse: 0,
        oiDecay: 0,
        ageDecay: 0,
        probabilistic: 0
      }
    };
  }

  const futureLow = Array(frames.length).fill(Infinity);
  const futureHigh = Array(frames.length).fill(-Infinity);
  const futureImpulseLow = Array(frames.length).fill(Infinity);
  const futureImpulseHigh = Array(frames.length).fill(-Infinity);
  let minLow = Infinity;
  let maxHigh = -Infinity;
  let impulseLow = Infinity;
  let impulseHigh = -Infinity;

  for (let index = frames.length - 1; index >= 0; index -= 1) {
    const frame = frames[index];
    const state = marketStateFrames[index];
    minLow = Math.min(minLow, frame.low || frame.price);
    maxHigh = Math.max(maxHigh, frame.high || frame.price);
    futureLow[index] = minLow;
    futureHigh[index] = maxHigh;

    if ((state?.impulse || 0) >= 1.35 || (state?.oiDropFromPrevious || 0) >= 0.025) {
      impulseLow = Math.min(impulseLow, frame.low || frame.price);
      impulseHigh = Math.max(impulseHigh, frame.high || frame.price);
    }

    futureImpulseLow[index] = impulseLow;
    futureImpulseHigh[index] = impulseHigh;
  }

  const touchBufferPct = 0.003;
  const impulseBufferPct = 0.006;
  const latestFrameIndex = frames.length - 1;
  const inactive: Record<string, number> = {
    swept: 0,
    impulse: 0,
    oiDecay: 0,
    ageDecay: 0,
    probabilistic: 0
  };

  const nextEvents = events.map((event, eventIndex) => {
    const nextIndex = Math.min(frames.length - 1, event.i + 1);
    const swept =
      event.s === 'L'
        ? futureLow[nextIndex] <= event.p * (1 + touchBufferPct)
        : futureHigh[nextIndex] >= event.p * (1 - touchBufferPct);

    if (swept) {
      inactive.swept += 1;
      return { ...event, a: 0 as 0 };
    }

    const impulseCleared =
      event.s === 'L'
        ? futureImpulseLow[nextIndex] <= event.p * (1 + impulseBufferPct)
        : futureImpulseHigh[nextIndex] >= event.p * (1 - impulseBufferPct);
    const stateAtBirth = marketStateFrames[Math.min(event.i, marketStateFrames.length - 1)];
    const latestState = marketStateFrames[marketStateFrames.length - 1];
    const oiDrop =
      stateAtBirth?.oiUsd && latestState?.oiUsd && stateAtBirth.oiUsd > 0
        ? Math.max(0, (stateAtBirth.oiUsd - latestState.oiUsd) / stateAtBirth.oiUsd)
        : 0;
    const ageHours = Math.max(0, latestFrameIndex - event.i);
    const halfLife = liquidationHalfLifeHours(event);
    const ageSurvival = Math.pow(0.5, ageHours / halfLife);
    const oiSurvival = oiDrop <= 0.08 ? 1 : clamp(1 - (oiDrop - 0.08) / 0.32, sourceFloor(event), 1);
    const impulseSurvival = impulseCleared ? (event.z === 'gmx-position' ? 0.65 : 0.35) : 1;
    const survival = clamp(ageSurvival * oiSurvival * impulseSurvival, 0, 1);

    if (impulseCleared && impulseSurvival < 1) inactive.impulse += 1;
    if (oiDrop > 0.08 && oiSurvival < 1) inactive.oiDecay += 1;
    if (ageSurvival < 0.75) inactive.ageDecay += 1;

    const unit = deterministicUnit(`${event.z || ''}|${event.s}|${event.l}|${event.p}|${event.i}|${eventIndex}`);
    if (unit > survival) {
      inactive.probabilistic += 1;
      return { ...event, a: 0 as 0 };
    }

    return event;
  });

  return { events: nextEvents, inactive };
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
    binanceTopAccountRatioResult,
    binanceTopPositionRatioResult,
    binanceDepthResult,
    bybitOiResult,
    bybitRatioResult,
    bybitDepthResult,
    okxOiResult,
    okxRatioResult,
    bitgetOiResult,
    hyperCandlesResult,
    hyperContextResult,
    hyperBookResult
  ] = await Promise.all([
    safeFetch('GMX positions', fetchGmxBtcPositions),
    safeFetch('Binance OI', fetchBinanceOpenInterest),
    safeFetch('Binance ratios', fetchBinanceRatios),
    safeFetch('Binance top account ratios', fetchBinanceTopAccountRatios),
    safeFetch('Binance top position ratios', fetchBinanceTopPositionRatios),
    safeFetch('Binance depth', fetchBinanceDepth),
    safeFetch('Bybit OI', fetchBybitOpenInterest),
    safeFetch('Bybit account ratio', fetchBybitAccountRatio),
    safeFetch('Bybit depth', fetchBybitDepth),
    safeFetch('OKX OI', fetchOkxOpenInterestVolume),
    safeFetch('OKX ratios', fetchOkxLongShortRatio),
    safeFetch('Bitget OI', fetchBitgetOpenInterest),
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
  const binanceTopAccountRatios = normalizedBinanceRatios(binanceTopAccountRatioResult.data || []);
  const binanceTopPositionRatios = normalizedBinanceRatios(binanceTopPositionRatioResult.data || []);
  for (let index = 1; index < binanceOi.length; index += 1) {
    const current = binanceOi[index];
    const previous = binanceOi[index - 1];
    const deltaOiUsd = current.oiUsd - previous.oiUsd;
    const frameIndex = frameIndexForTimestamp(frames, current.timestampMs);
    const frame = frames[frameIndex];
    const longShare =
      weightedLongShareAt(current.timestampMs, [
        { rows: binanceRatios, weight: 0.45 },
        { rows: binanceTopAccountRatios, weight: 0.25 },
        { rows: binanceTopPositionRatios, weight: 0.3 }
      ]) ?? 0.5;
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      frame?.price || 0,
      deltaOiUsd,
      longShare,
      1.4,
      'binance-oi'
    );
  }

  const bybitOi = normalizedBybitOi(bybitOiResult.data || [], frames);
  const bybitRatios = normalizedBybitRatios(bybitRatioResult.data || []);
  for (let index = 1; index < bybitOi.length; index += 1) {
    const current = bybitOi[index];
    const previous = bybitOi[index - 1];
    const deltaOiUsd = current.oiUsd - previous.oiUsd;
    const frameIndex = frameIndexForTimestamp(frames, current.timestampMs);
    const frame = frames[frameIndex];
    const previousFrame = frames[Math.max(0, frameIndex - 1)];
    const trendLongShare = frame && previousFrame && frame.price > previousFrame.price ? 0.57 : 0.43;
    const ratio = valueAtOrBefore(bybitRatios, current.timestampMs);
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      frame?.price || 0,
      deltaOiUsd,
      ratio?.longShare ?? trendLongShare,
      1.15,
      'bybit-oi'
    );
  }

  const okxOi = normalizedOkxOpenInterest(okxOiResult.data || []);
  const okxRatios = normalizedOkxRatios(okxRatioResult.data || []);
  for (let index = 1; index < okxOi.length; index += 1) {
    const current = okxOi[index];
    const previous = okxOi[index - 1];
    const deltaOiUsd = current.oiUsd - previous.oiUsd;
    const frameIndex = frameIndexForTimestamp(frames, current.timestampMs);
    const frame = frames[frameIndex];
    const ratio = valueAtOrBefore(okxRatios, current.timestampMs);
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      frameIndex,
      frame?.price || 0,
      deltaOiUsd,
      ratio?.longShare ?? 0.5,
      1.05,
      'okx-oi'
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
      0.55,
      'hyper-volume'
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
      Math.max(1, Math.round(positionWeight(sizeUsd, leverage) * 0.75)),
      'gmx-position'
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
      0.75,
      'hyper-oi'
    );
  }

  const bitgetOiRows = bitgetOiResult.data || [];
  const bitgetBtcOi = bitgetOiRows
    .map((row) => Number(row.openInterest))
    .filter((value) => Number.isFinite(value) && value > 0)
    .reduce((sum, value) => sum + value, 0);
  if (bitgetBtcOi > 0 && latestFrame) {
    addEstimatedLiquidationBuild(
      events,
      prices,
      firstSeen,
      currentZoneCounts,
      latestFrame.i,
      latestFrame.price,
      bitgetBtcOi * latestFrame.price * 0.006,
      0.5,
      0.55,
      'bitget-oi'
    );
  }

  const marketStateFrames = buildMarketStateFrames(frames, [binanceOi, bybitOi, okxOi]);
  const lifecycleResult = markLifecycleLiquidationEvents(events, frames, marketStateFrames);
  const activeSources = activeStudySources();
  const lifecycleEvents = lifecycleResult.events.map((event) => {
    if (!event.a || !event.z || activeSources.has(event.z)) return event;
    return { ...event, a: 0 as 0 };
  });
  const inactiveCount = lifecycleEvents.filter((event) => event.a === 0).length;
  const sourceFilteredCount = lifecycleResult.events.filter(
    (event) => event.a && event.z && !activeSources.has(event.z)
  ).length;
  const activeZoneCounts = new Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>();
  for (const event of lifecycleEvents) {
    if (!event.a || event.i > latestFrame.i) continue;
    const key = `${event.s}|${event.l}|${priceKey(event.p)}`;
    const existing =
      activeZoneCounts.get(key) ||
      ({
        s: event.s,
        l: event.l,
        p: event.p,
        c: 0
      } as { s: 'L' | 'S'; l: number; p: number; c: number });
    existing.c += 1;
    activeZoneCounts.set(key, existing);
  }

  const sourceStatuses = [
    gmxPositionsResult,
    binanceOiResult,
    binanceRatioResult,
    binanceTopAccountRatioResult,
    binanceTopPositionRatioResult,
    binanceDepthResult,
    bybitOiResult,
    bybitRatioResult,
    bybitDepthResult,
    okxOiResult,
    okxRatioResult,
    bitgetOiResult,
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
      api: 'GMX GraphQL + Binance Futures public REST + Bybit public REST + OKX public REST + Bitget public REST + Hyperliquid public info + dYdX indexer',
      method:
        'Multi-source public perp liquidity estimate: CEX OI buildups plus trader ratios -> estimated liquidation buckets, GMX active positions -> approximate liquidation buckets, dYdX candles -> price/time axis, then a survivor model removes zones after sweep, OI decay, age decay and impulse cleanup.',
      params: [
        `frames=${frames.length}`,
        `events=${events.length}`,
        `gmxPositions=${gmxPositions.length}`,
        `binanceOi=${binanceOi.length}`,
        `bybitOi=${bybitOi.length}`,
        `okxOi=${okxOi.length}`,
        `bitgetOi=${bitgetOiRows.length}`,
        `hyperCandles=${hyperCandles.length}`,
        `inactive=${inactiveCount}`,
        `inactiveSwept=${lifecycleResult.inactive.swept}`,
        `inactiveImpulse=${lifecycleResult.inactive.impulse}`,
        `inactiveOiDecay=${lifecycleResult.inactive.oiDecay}`,
        `inactiveAgeDecay=${lifecycleResult.inactive.ageDecay}`,
        `inactiveLifecycle=${lifecycleResult.inactive.probabilistic}`,
        `inactiveSourceFiltered=${sourceFilteredCount}`,
        `activeSources=${Array.from(activeSources).join('+')}`
      ],
      sourceStatuses,
      marketDepth: {
        binance: binanceDepthResult.data || null,
        bybit: bybitDepthResult.data || null,
        hyperliquid: hyperBookResult.data || null
      },
      note:
        'Study-only public perp liquidity estimate from GMX positions, Binance/Bybit/OKX open-interest buildups, Binance/Bybit/OKX trader ratios, Bitget current OI and Hyperliquid public context. It applies a lifecycle model so stale, swept, decayed or impulse-cleared zones stop being active, then renders a calibrated active-source subset. It does not use Decentrader and does not place or size trades.'
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
    events: lifecycleEvents,
    topCurrentZones: Array.from(activeZoneCounts.values())
      .sort((a, b) => b.c - a.c || a.p - b.p)
      .slice(0, 40)
  };

  cachedPayload = payload;
  cachedAt = now;
  return payload;
}
