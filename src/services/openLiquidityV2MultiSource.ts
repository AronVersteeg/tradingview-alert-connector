import fs from 'fs';
import path from 'path';

import axios from 'axios';
import { binanceGet } from './binanceHttp';

const MARKET = 'BTC-USD' as const;
const MODEL_VERSION = 'public-multi-venue-cohorts-v2.2';
const HOUR_MS = 3_600_000;
const FRAME_LIMIT = 500;
const BINANCE_URL = 'https://fapi.binance.com';
const BYBIT_URL = 'https://api.bybit.com';
const OKX_URL = 'https://www.okx.com';
const HYPERLIQUID_URL = 'https://api.hyperliquid.xyz/info';
const DYDX_URL = 'https://indexer.dydx.trade/v4';
const GMX_GRAPHQL_URL = 'https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql';
const GMX_BTC_MARKETS = [
  '0x47c031236e19d024b42f8AE6780E44A573170703',
  '0x7C11F78Ce78768518D743E81Fdfa2F860C6b9A77'
];

type Side = 'L' | 'S';
type Leverage = 3 | 5 | 10;

type Candle = {
  timestampMs: number;
  open: number;
  high: number;
  low: number;
  close: number;
  quoteVolumeUsd: number;
  takerBuyUsd?: number;
};

type VenueHour = Candle & {
  venue: string;
  oiUsd: number;
  flowPrecision: 'venue-taker' | 'market-proxy' | 'candle-only';
};

type Cohort = {
  venue: string;
  side: Side;
  leverage: Leverage;
  liquidationPrice: number;
  remainingUsd: number;
  confidence: number;
  bornAtMs: number;
  exactInventory: boolean;
};

export type MultiVenueLiquidityZone = {
  side: Side;
  leverage: Leverage;
  price: number;
  positionCount: number;
  notionalUsd: number;
  weightedUsd: number;
  confidence: number;
  uncertaintyUsd: number;
  sourceCount: number;
  sources: string[];
  exactInventoryUsd: number;
  inferredInventoryUsd: number;
};

type MultiVenueGap = {
  left: number;
  right: number;
  width: number;
  leftEdge: MultiVenueLiquidityZone;
  rightEdge: MultiVenueLiquidityZone;
  interiorWeightedUsd: number;
  interiorToEdgeRatio: number;
  peakToEdgeRatio: number;
  cleanliness: number;
  confidence: number;
  sourceAgreement: number;
  status: 'candidate' | 'confirmed';
  method: 'strict-contiguous-low-density';
};

type Snapshot = {
  version: 2;
  modelVersion: string;
  effectiveAt: string;
  observedAt: string;
  referencePrice: number;
  high: number;
  low: number;
  sourceHours: number;
  availableSources: string[];
  totalNotionalUsd: number;
  zones: MultiVenueLiquidityZone[];
  gap: MultiVenueGap | null;
};

type SourceResult<T> = {
  label: string;
  ok: boolean;
  data: T;
  error?: string;
};

function finite(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function positive(value: unknown): number {
  return Math.max(0, finite(value));
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.max(minimum, Math.min(maximum, value));
}

function rounded(value: number, decimals = 4): number {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function enabled(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return String(raw).trim().toLowerCase() === 'true';
}

function boundedInteger(value: unknown, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed)
    ? Math.max(minimum, Math.min(maximum, Math.floor(parsed)))
    : fallback;
}

function timestampForMs(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

function priceStep(referencePrice: number): number {
  const configured = Number(process.env.OPEN_LIQUIDITY_V2_PRICE_STEP_USD);
  if (Number.isFinite(configured) && configured >= 10 && configured <= 1_000) return configured;
  return referencePrice >= 20_000 ? 50 : 25;
}

async function safeFetch<T>(label: string, fetcher: () => Promise<T>, fallback: T): Promise<SourceResult<T>> {
  try {
    return { label, ok: true, data: await fetcher() };
  } catch (error) {
    return {
      label,
      ok: false,
      data: fallback,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

async function fetchDydxCandles(): Promise<Candle[]> {
  const response = await axios.get(`${DYDX_URL}/candles/perpetualMarkets/${MARKET}`, {
    timeout: 30_000,
    params: { resolution: '1HOUR', limit: FRAME_LIMIT }
  });
  const rows = response.data?.candles;
  if (!Array.isArray(rows)) throw new Error('dYdX candle response did not contain candles.');
  return rows
    .map((row: any) => ({
      timestampMs: Date.parse(row.startedAt),
      open: positive(row.open),
      high: positive(row.high),
      low: positive(row.low),
      close: positive(row.close),
      quoteVolumeUsd: positive(row.usdVolume)
    }))
    .filter((row: Candle) => Number.isFinite(row.timestampMs) && row.close > 0)
    .sort((a: Candle, b: Candle) => a.timestampMs - b.timestampMs);
}

async function fetchBinanceHours(): Promise<VenueHour[]> {
  const [candleResponse, oiResponse] = await Promise.all([
    binanceGet(`${BINANCE_URL}/fapi/v1/klines`, {
      timeout: 30_000,
      params: { symbol: 'BTCUSDT', interval: '1h', limit: FRAME_LIMIT }
    }),
    binanceGet(`${BINANCE_URL}/futures/data/openInterestHist`, {
      timeout: 30_000,
      params: { symbol: 'BTCUSDT', period: '1h', limit: FRAME_LIMIT }
    })
  ]);
  if (!Array.isArray(candleResponse.data) || !Array.isArray(oiResponse.data)) {
    throw new Error('Binance historical response was incomplete.');
  }
  const oiByTime = new Map<number, number>(
    oiResponse.data.map((row: any) => [finite(row.timestamp), positive(row.sumOpenInterestValue)])
  );
  return candleResponse.data
    .map((row: any[]) => ({
      venue: 'binance',
      timestampMs: finite(row[0]),
      open: positive(row[1]),
      high: positive(row[2]),
      low: positive(row[3]),
      close: positive(row[4]),
      quoteVolumeUsd: positive(row[7]),
      takerBuyUsd: positive(row[10]),
      oiUsd: oiByTime.get(finite(row[0])) || 0,
      flowPrecision: 'venue-taker' as const
    }))
    .filter((row: VenueHour) => row.timestampMs > 0 && row.close > 0 && row.oiUsd > 0)
    .sort((a: VenueHour, b: VenueHour) => a.timestampMs - b.timestampMs);
}

async function fetchBybitHours(): Promise<VenueHour[]> {
  const [candleResponse, oiResponse] = await Promise.all([
    axios.get(`${BYBIT_URL}/v5/market/kline`, {
      timeout: 30_000,
      params: { category: 'linear', symbol: 'BTCUSDT', interval: '60', limit: 200 }
    }),
    axios.get(`${BYBIT_URL}/v5/market/open-interest`, {
      timeout: 30_000,
      params: { category: 'linear', symbol: 'BTCUSDT', intervalTime: '1h', limit: 200 }
    })
  ]);
  const candles = candleResponse.data?.result?.list;
  const openInterest = oiResponse.data?.result?.list;
  if (!Array.isArray(candles) || !Array.isArray(openInterest)) {
    throw new Error('Bybit historical response was incomplete.');
  }
  const oiByTime = new Map<number, number>(
    openInterest.map((row: any) => [finite(row.timestamp), positive(row.openInterest)])
  );
  return candles
    .map((row: any[]) => {
      const timestampMs = finite(row[0]);
      const close = positive(row[4]);
      return {
        venue: 'bybit',
        timestampMs,
        open: positive(row[1]),
        high: positive(row[2]),
        low: positive(row[3]),
        close,
        quoteVolumeUsd: positive(row[6]),
        oiUsd: (oiByTime.get(timestampMs) || 0) * close,
        flowPrecision: 'market-proxy' as const
      };
    })
    .filter((row: VenueHour) => row.timestampMs > 0 && row.close > 0 && row.oiUsd > 0)
    .sort((a: VenueHour, b: VenueHour) => a.timestampMs - b.timestampMs);
}

async function fetchOkxHours(): Promise<VenueHour[]> {
  const [candleResponse, oiResponse] = await Promise.all([
    axios.get(`${OKX_URL}/api/v5/market/history-candles`, {
      timeout: 30_000,
      params: { instId: 'BTC-USDT-SWAP', bar: '1H', limit: 500 }
    }),
    axios.get(`${OKX_URL}/api/v5/rubik/stat/contracts/open-interest-volume`, {
      timeout: 30_000,
      params: { ccy: 'BTC', period: '1H', limit: 500 }
    })
  ]);
  const candles = candleResponse.data?.data;
  const openInterest = oiResponse.data?.data;
  if (!Array.isArray(candles) || !Array.isArray(openInterest)) {
    throw new Error('OKX historical response was incomplete.');
  }
  const oiByTime = new Map<number, number>(
    openInterest.map((row: any[]) => [finite(row[0]), positive(row[1])])
  );
  return candles
    .map((row: any[]) => {
      const timestampMs = finite(row[0]);
      return {
        venue: 'okx',
        timestampMs,
        open: positive(row[1]),
        high: positive(row[2]),
        low: positive(row[3]),
        close: positive(row[4]),
        quoteVolumeUsd: positive(row[7]),
        oiUsd: oiByTime.get(timestampMs) || 0,
        flowPrecision: 'market-proxy' as const
      };
    })
    .filter((row: VenueHour) => row.timestampMs > 0 && row.close > 0 && row.oiUsd > 0)
    .sort((a: VenueHour, b: VenueHour) => a.timestampMs - b.timestampMs);
}

async function fetchHyperliquidCandles(startTime: number, endTime: number): Promise<Candle[]> {
  const response = await axios.post(
    HYPERLIQUID_URL,
    {
      type: 'candleSnapshot',
      req: { coin: 'BTC', interval: '1h', startTime, endTime }
    },
    {
      timeout: 30_000,
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' }
    }
  );
  if (!Array.isArray(response.data)) throw new Error('Hyperliquid candle response was not an array.');
  return response.data
    .map((row: any) => ({
      timestampMs: finite(row.t),
      open: positive(row.o),
      high: positive(row.h),
      low: positive(row.l),
      close: positive(row.c),
      quoteVolumeUsd: positive(row.v) * positive(row.c)
    }))
    .filter((row: Candle) => row.timestampMs > 0 && row.close > 0)
    .sort((a: Candle, b: Candle) => a.timestampMs - b.timestampMs);
}

type GmxPosition = {
  positionKey: string;
  isLong: boolean;
  sizeInUsd: string;
  leverage: string;
};

async function fetchGmxPositions(): Promise<GmxPosition[]> {
  const rows: GmxPosition[] = [];
  const query = `
    query PublicV2Positions($market: String!, $offset: Int!, $limit: Int!) {
      positions(
        limit: $limit
        offset: $offset
        where: { market_eq: $market, isSnapshot_eq: false }
        orderBy: id_ASC
      ) {
        positionKey isLong sizeInUsd leverage
      }
    }
  `;
  for (const market of GMX_BTC_MARKETS) {
    for (let offset = 0; offset < 10_000; offset += 1_000) {
      const response = await axios.post(
        GMX_GRAPHQL_URL,
        { query, variables: { market, offset, limit: 1_000 } },
        {
          timeout: 30_000,
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' }
        }
      );
      if (Array.isArray(response.data?.errors) && response.data.errors.length) {
        throw new Error(response.data.errors.map((item: any) => item?.message).filter(Boolean).join('; '));
      }
      const page = response.data?.data?.positions;
      if (!Array.isArray(page)) throw new Error('GMX position response did not contain positions.');
      rows.push(...page);
      if (page.length < 1_000) break;
    }
  }
  return rows;
}

function bodyPressure(candle: Candle): number {
  const range = Math.max(1, candle.high - candle.low);
  return clamp((candle.close - candle.open) / range, -1, 1);
}

function takerPressure(candle: Candle): number | undefined {
  if (!Number.isFinite(candle.takerBuyUsd) || candle.quoteVolumeUsd <= 0) return undefined;
  const takerBuy = clamp(Number(candle.takerBuyUsd), 0, candle.quoteVolumeUsd);
  const takerSell = Math.max(0, candle.quoteVolumeUsd - takerBuy);
  return clamp((takerBuy - takerSell) / Math.max(1, takerBuy + takerSell), -1, 1);
}

export function inferredLongShare(
  candle: Candle,
  marketFlowCandle?: Candle
): { longShare: number; confidence: number } {
  const directTaker = takerPressure(candle);
  const proxyTaker = directTaker === undefined ? takerPressure(marketFlowCandle || candle) : directTaker;
  const body = bodyPressure(candle);
  const pressure = proxyTaker === undefined
    ? body
    : directTaker === undefined
      ? proxyTaker * 0.55 + body * 0.45
      : proxyTaker * 0.7 + body * 0.3;
  const longShare = clamp(0.5 + pressure * 0.38, 0.18, 0.82);
  const precisionBase = directTaker !== undefined ? 0.72 : proxyTaker !== undefined ? 0.61 : 0.48;
  return {
    longShare,
    confidence: clamp(precisionBase + Math.abs(pressure) * 0.16, precisionBase, 0.9)
  };
}

function liquidationPrice(entryPrice: number, side: Side, leverage: Leverage): number {
  const reserve = 0.0075;
  const distance = Math.max(0.01, 1 / leverage - reserve);
  return side === 'L'
    ? entryPrice * (1 - distance)
    : entryPrice * (1 + distance);
}

function reduceVenueCohorts(cohorts: Cohort[], venue: string, fraction: number): void {
  const reduction = clamp(fraction, 0, 1);
  if (reduction <= 0) return;
  for (const cohort of cohorts) {
    if (cohort.venue === venue) cohort.remainingUsd *= 1 - reduction;
  }
}

function sweepCohorts(cohorts: Cohort[], candle: Candle): void {
  for (const cohort of cohorts) {
    if (cohort.bornAtMs >= candle.timestampMs) continue;
    const swept = cohort.side === 'L'
      ? candle.low <= cohort.liquidationPrice
      : candle.high >= cohort.liquidationPrice;
    if (swept) cohort.remainingUsd = 0;
  }
}

function addInferredCohorts(
  cohorts: Cohort[],
  hour: VenueHour,
  deltaOiUsd: number,
  marketFlowCandle?: Candle
): void {
  if (deltaOiUsd <= Math.max(100_000, hour.oiUsd * 0.00005)) return;
  const inference = inferredLongShare(hour, marketFlowCandle);
  const leverageWeights: Array<{ leverage: Leverage; weight: number }> = [
    { leverage: 3, weight: 0.12 },
    { leverage: 5, weight: 0.28 },
    { leverage: 10, weight: 0.6 }
  ];
  for (const side of ['L', 'S'] as Side[]) {
    const sideShare = side === 'L' ? inference.longShare : 1 - inference.longShare;
    for (const leverageWeight of leverageWeights) {
      const remainingUsd = deltaOiUsd * sideShare * leverageWeight.weight;
      if (remainingUsd < 25_000) continue;
      cohorts.push({
        venue: hour.venue,
        side,
        leverage: leverageWeight.leverage,
        liquidationPrice: liquidationPrice(hour.close, side, leverageWeight.leverage),
        remainingUsd,
        confidence: inference.confidence,
        bornAtMs: hour.timestampMs,
        exactInventory: false
      });
    }
  }
}

function aggregateCohorts(cohorts: Cohort[], referencePrice: number): MultiVenueLiquidityZone[] {
  const step = priceStep(referencePrice);
  const bins = new Map<string, {
    side: Side;
    leverage: Leverage;
    price: number;
    positionCount: number;
    notionalUsd: number;
    weightedUsd: number;
    confidenceWeight: number;
    sources: Set<string>;
    exactInventoryUsd: number;
    inferredInventoryUsd: number;
  }>();
  for (const cohort of cohorts) {
    if (cohort.remainingUsd < 10_000) continue;
    const price = Math.round(cohort.liquidationPrice / step) * step;
    if (price < referencePrice * 0.35 || price > referencePrice * 2.2) continue;
    const key = `${cohort.side}|${cohort.leverage}|${price}`;
    const bin = bins.get(key) || {
      side: cohort.side,
      leverage: cohort.leverage,
      price,
      positionCount: 0,
      notionalUsd: 0,
      weightedUsd: 0,
      confidenceWeight: 0,
      sources: new Set<string>(),
      exactInventoryUsd: 0,
      inferredInventoryUsd: 0
    };
    bin.positionCount += 1;
    bin.notionalUsd += cohort.remainingUsd;
    bin.weightedUsd += cohort.remainingUsd * cohort.confidence;
    bin.confidenceWeight += cohort.remainingUsd * cohort.confidence;
    bin.sources.add(cohort.venue);
    if (cohort.exactInventory) bin.exactInventoryUsd += cohort.remainingUsd;
    else bin.inferredInventoryUsd += cohort.remainingUsd;
    bins.set(key, bin);
  }
  return [...bins.values()].map((bin) => ({
    side: bin.side,
    leverage: bin.leverage,
    price: bin.price,
    positionCount: bin.positionCount,
    notionalUsd: rounded(bin.notionalUsd, 2),
    weightedUsd: rounded(bin.weightedUsd, 2),
    confidence: rounded(bin.notionalUsd > 0 ? bin.confidenceWeight / bin.notionalUsd : 0, 3),
    uncertaintyUsd: rounded(step * (bin.sources.size > 1 ? 1.5 : 2.5), 2),
    sourceCount: bin.sources.size,
    sources: [...bin.sources].sort(),
    exactInventoryUsd: rounded(bin.exactInventoryUsd, 2),
    inferredInventoryUsd: rounded(bin.inferredInventoryUsd, 2)
  }))
    .sort((a, b) => a.price - b.price || a.side.localeCompare(b.side) || a.leverage - b.leverage);
}

function selectDisplayZones(
  zones: MultiVenueLiquidityZone[],
  referencePrice: number,
  gap: MultiVenueGap | undefined
): MultiVenueLiquidityZone[] {
  const selected = new Map<string, MultiVenueLiquidityZone>();
  const add = (zone: MultiVenueLiquidityZone | undefined): void => {
    if (!zone) return;
    selected.set(`${zone.side}|${zone.leverage}|${zone.price}`, zone);
  };
  zones
    .slice()
    .sort((a, b) => b.weightedUsd - a.weightedUsd)
    .slice(0, 75)
    .forEach(add);
  zones
    .slice()
    .sort(
      (a, b) =>
        Math.abs(a.price - referencePrice) - Math.abs(b.price - referencePrice) ||
        b.weightedUsd - a.weightedUsd
    )
    .slice(0, 25)
    .forEach(add);
  add(gap?.leftEdge);
  add(gap?.rightEdge);
  return [...selected.values()]
    .sort((a, b) => a.price - b.price || a.side.localeCompare(b.side) || a.leverage - b.leverage)
    .slice(0, 102);
}

function collapseByPrice(zones: MultiVenueLiquidityZone[]): MultiVenueLiquidityZone[] {
  const bins = new Map<string, MultiVenueLiquidityZone>();
  for (const zone of zones) {
    const key = `${zone.side}|${zone.price}`;
    const existing = bins.get(key);
    if (!existing) {
      bins.set(key, { ...zone, sources: zone.sources.slice() });
      continue;
    }
    const sources = new Set([...existing.sources, ...zone.sources]);
    const totalNotional = existing.notionalUsd + zone.notionalUsd;
    existing.confidence = totalNotional > 0
      ? (
          existing.confidence * existing.notionalUsd +
          zone.confidence * zone.notionalUsd
        ) / totalNotional
      : 0;
    if (zone.weightedUsd > existing.weightedUsd) existing.leverage = zone.leverage;
    existing.positionCount += zone.positionCount;
    existing.notionalUsd = totalNotional;
    existing.weightedUsd += zone.weightedUsd;
    existing.sourceCount = sources.size;
    existing.sources = [...sources].sort();
    existing.exactInventoryUsd += zone.exactInventoryUsd;
    existing.inferredInventoryUsd += zone.inferredInventoryUsd;
  }
  return [...bins.values()];
}

export function detectStrictMultiVenueGap(
  zones: MultiVenueLiquidityZone[],
  currentPrice: number,
  options: { minClusterUsd?: number; minCleanliness?: number } = {}
): MultiVenueGap | undefined {
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) return undefined;
  const collapsed = collapseByPrice(zones);
  const configuredMinimum = Number(options.minClusterUsd ?? process.env.OPEN_LIQUIDITY_V2_MIN_CLUSTER_USD);
  const minimumCluster = Number.isFinite(configuredMinimum) && configuredMinimum > 0
    ? configuredMinimum
    : 1_000_000;
  // A gap edge is the nearest materially occupied bin, not the strongest peak
  // anywhere on that side. Using a high percentile here widened corridors past
  // their actual boundary and allowed dense interior bars into the "gap".
  const strongThreshold = minimumCluster;
  const leftCandidates = collapsed
    .filter((zone) => zone.side === 'L' && zone.price < currentPrice && zone.weightedUsd >= strongThreshold)
    .sort((a, b) => b.price - a.price);
  const rightCandidates = collapsed
    .filter((zone) => zone.side === 'S' && zone.price > currentPrice && zone.weightedUsd >= strongThreshold)
    .sort((a, b) => a.price - b.price);
  const leftEdge = leftCandidates[0];
  const rightEdge = rightCandidates[0];
  if (!leftEdge || !rightEdge || rightEdge.price - leftEdge.price < 500) return undefined;

  const interior = collapsed.filter((zone) => zone.price > leftEdge.price && zone.price < rightEdge.price);
  const interiorWeightedUsd = interior.reduce((sum, zone) => sum + zone.weightedUsd, 0);
  const interiorPeak = interior.reduce((peak, zone) => Math.max(peak, zone.weightedUsd), 0);
  const edgeFloor = Math.max(1, Math.min(leftEdge.weightedUsd, rightEdge.weightedUsd));
  const edgeTotal = Math.max(1, leftEdge.weightedUsd + rightEdge.weightedUsd);
  const peakToEdgeRatio = interiorPeak / edgeFloor;
  const interiorToEdgeRatio = interiorWeightedUsd / edgeTotal;
  const cleanliness = clamp(1 - Math.max(peakToEdgeRatio, interiorToEdgeRatio), 0, 1);
  const minimumCleanliness = clamp(
    Number.isFinite(Number(options.minCleanliness))
      ? Number(options.minCleanliness)
      : Number(process.env.OPEN_LIQUIDITY_V2_MIN_GAP_CLEANLINESS || 0.72),
    0.5,
    0.98
  );
  if (
    cleanliness < minimumCleanliness ||
    peakToEdgeRatio > 0.3 ||
    interiorToEdgeRatio > 0.28
  ) {
    return undefined;
  }

  const sourceAgreement = Math.min(leftEdge.sourceCount, rightEdge.sourceCount);
  const confidence = clamp(
    cleanliness * 0.55 +
    Math.min(leftEdge.confidence, rightEdge.confidence) * 0.25 +
    clamp(sourceAgreement / 3, 0, 1) * 0.2,
    0,
    1
  );
  return {
    left: leftEdge.price,
    right: rightEdge.price,
    width: rightEdge.price - leftEdge.price,
    leftEdge,
    rightEdge,
    interiorWeightedUsd: rounded(interiorWeightedUsd, 2),
    interiorToEdgeRatio: rounded(interiorToEdgeRatio, 4),
    peakToEdgeRatio: rounded(peakToEdgeRatio, 4),
    cleanliness: rounded(cleanliness, 3),
    confidence: rounded(confidence, 3),
    sourceAgreement,
    status: sourceAgreement >= 2 ? 'confirmed' : 'candidate',
    method: 'strict-contiguous-low-density'
  };
}

export function buildCausalSnapshots(input: {
  referenceCandles: Candle[];
  venueHours: VenueHour[][];
  marketFlowCandles: Candle[];
  observedAt?: string;
}): Snapshot[] {
  const venueMaps = input.venueHours.map((hours) => new Map(hours.map((hour) => [hour.timestampMs, hour])));
  const previousByVenue = new Map<string, VenueHour>();
  const flowByTime = new Map(input.marketFlowCandles.map((candle) => [candle.timestampMs, candle]));
  const cohorts: Cohort[] = [];
  const snapshots: Snapshot[] = [];
  const observedAt = input.observedAt || new Date().toISOString();

  for (let frameIndex = 0; frameIndex < input.referenceCandles.length; frameIndex += 1) {
    const frame = input.referenceCandles[frameIndex];
    sweepCohorts(cohorts, frame);
    const availableSources: string[] = [];

    for (const venueMap of venueMaps) {
      const hour = venueMap.get(frame.timestampMs);
      if (!hour) continue;
      availableSources.push(hour.venue);
      const previous = previousByVenue.get(hour.venue);
      if (previous && previous.oiUsd > 0) {
        const deltaOiUsd = hour.oiUsd - previous.oiUsd;
        if (deltaOiUsd < 0) {
          reduceVenueCohorts(cohorts, hour.venue, Math.abs(deltaOiUsd) / previous.oiUsd);
        } else {
          addInferredCohorts(cohorts, hour, deltaOiUsd, flowByTime.get(frame.timestampMs));
        }
      }
      previousByVenue.set(hour.venue, hour);
    }

    for (let index = cohorts.length - 1; index >= 0; index -= 1) {
      if (cohorts[index].remainingUsd < 10_000) cohorts.splice(index, 1);
    }
    const allZones = aggregateCohorts(cohorts, frame.close);
    const gap = detectStrictMultiVenueGap(allZones, frame.close);
    const zones = selectDisplayZones(allZones, frame.close, gap);
    snapshots.push({
      version: 2,
      modelVersion: MODEL_VERSION,
      effectiveAt: new Date(frame.timestampMs).toISOString(),
      observedAt,
      referencePrice: rounded(frame.close, 2),
      high: rounded(frame.high, 2),
      low: rounded(frame.low, 2),
      sourceHours: availableSources.length,
      availableSources: [...new Set(availableSources)].sort(),
      totalNotionalUsd: rounded(allZones.reduce((sum, zone) => sum + zone.notionalUsd, 0), 2),
      zones,
      gap: gap || null
    });
  }
  return snapshots;
}

export class OpenLiquidityV2MultiSourceCollector {
  private interval: NodeJS.Timeout | undefined;
  private refreshPromise: Promise<void> | undefined;
  private snapshots: Snapshot[] = [];
  private loaded = false;
  private lastSuccessAt: string | undefined;
  private lastErrorAt: string | undefined;
  private lastError: string | undefined;
  private sourceStatuses: SourceResult<unknown>[] = [];
  private payloadCache: { at: number; payload: any } | undefined;

  private enabled(): boolean {
    return enabled('OPEN_LIQUIDITY_V2_ENABLED', true);
  }

  private pollMinutes(): number {
    return boundedInteger(process.env.OPEN_LIQUIDITY_V2_POLL_MINUTES, 60, 10, 1_440);
  }

  historyDirectory(): string {
    const explicit = String(process.env.OPEN_LIQUIDITY_V2_HISTORY_DIR || '').trim();
    if (explicit) return explicit;
    const domDirectory = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
    if (domDirectory) return path.join(path.dirname(domDirectory), 'open-liquidity-v2');
    const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
    const base = fs.existsSync(renderDisk) ? renderDisk : path.join(process.cwd(), 'data');
    return path.join(base, 'open-liquidity-v2');
  }

  private historyFile(): string {
    return path.join(this.historyDirectory(), `${MODEL_VERSION}.json`);
  }

  start(): void {
    if (!this.enabled() || this.interval) return;
    this.loadHistory();
    this.refresh().catch((error) => console.error('Initial multi-source Open Liquidity V2 refresh failed:', error));
    this.interval = setInterval(() => {
      this.refresh().catch((error) => console.error('Multi-source Open Liquidity V2 refresh failed:', error));
    }, this.pollMinutes() * 60_000);
    console.log('Multi-source Open Liquidity V2 collector started:', {
      market: MARKET,
      modelVersion: MODEL_VERSION,
      pollMinutes: this.pollMinutes(),
      historyDirectory: this.historyDirectory(),
      readOnly: true
    });
  }

  stop(): void {
    if (this.interval) clearInterval(this.interval);
    this.interval = undefined;
  }

  private loadHistory(): void {
    if (this.loaded) return;
    this.loaded = true;
    const file = this.historyFile();
    if (!fs.existsSync(file)) return;
    try {
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (Array.isArray(parsed)) {
        this.snapshots = parsed
          .filter((snapshot) => snapshot?.modelVersion === MODEL_VERSION && snapshot?.effectiveAt)
          .sort((a, b) => Date.parse(a.effectiveAt) - Date.parse(b.effectiveAt));
      }
    } catch (error) {
      console.warn('Multi-source Open Liquidity V2 history could not be read:', {
        file,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private persistHistory(): void {
    fs.mkdirSync(this.historyDirectory(), { recursive: true });
    const file = this.historyFile();
    const temporary = `${file}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify(this.snapshots));
    fs.renameSync(temporary, file);
  }

  async refresh(): Promise<void> {
    if (this.refreshPromise) return this.refreshPromise;
    this.refreshPromise = this.refreshInternal().finally(() => {
      this.refreshPromise = undefined;
    });
    return this.refreshPromise;
  }

  private async refreshInternal(): Promise<void> {
    this.loadHistory();
    try {
      const dydx = await fetchDydxCandles();
      if (!dydx.length) throw new Error('No dYdX reference candles available.');
      const startTime = dydx[0].timestampMs;
      const endTime = dydx[dydx.length - 1].timestampMs + HOUR_MS;
      const [binance, bybit, okx, hyperliquid, gmx] = await Promise.all([
        safeFetch('Binance futures OI + taker flow', fetchBinanceHours, [] as VenueHour[]),
        safeFetch('Bybit linear OI', fetchBybitHours, [] as VenueHour[]),
        safeFetch('OKX swap OI', fetchOkxHours, [] as VenueHour[]),
        safeFetch(
          'Hyperliquid public candles',
          () => fetchHyperliquidCandles(startTime, endTime),
          [] as Candle[]
        ),
        safeFetch('GMX protocol positions', fetchGmxPositions, [] as GmxPosition[])
      ]);
      this.sourceStatuses = [binance, bybit, okx, hyperliquid, gmx];
      const successfulOiSources = [binance, bybit, okx].filter((source) => source.ok && source.data.length);
      if (successfulOiSources.length < 2) {
        throw new Error(
          `Only ${successfulOiSources.length} historical OI source(s) available; at least two are required.`
        );
      }
      const marketFlowCandles = binance.ok && binance.data.length
        ? binance.data
        : hyperliquid.data;
      const rebuilt = buildCausalSnapshots({
        referenceCandles: dydx,
        venueHours: successfulOiSources.map((source) => source.data),
        marketFlowCandles
      });
      const byEffectiveAt = new Map(this.snapshots.map((snapshot) => [snapshot.effectiveAt, snapshot]));
      for (const snapshot of rebuilt) byEffectiveAt.set(snapshot.effectiveAt, snapshot);
      this.snapshots = [...byEffectiveAt.values()]
        .sort((a, b) => Date.parse(a.effectiveAt) - Date.parse(b.effectiveAt))
        .slice(-2_160);
      this.persistHistory();
      this.payloadCache = undefined;
      this.lastSuccessAt = new Date().toISOString();
      this.lastError = undefined;
      console.log('Multi-source Open Liquidity V2 refreshed:', {
        snapshots: this.snapshots.length,
        from: this.snapshots[0]?.effectiveAt,
        to: this.snapshots[this.snapshots.length - 1]?.effectiveAt,
        sources: this.sourceStatuses.map((source) => ({
          label: source.label,
          ok: source.ok,
          rows: Array.isArray(source.data) ? source.data.length : 0,
          error: source.error
        }))
      });
    } catch (error) {
      this.lastErrorAt = new Date().toISOString();
      this.lastError = error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  getStatus(): any {
    this.loadHistory();
    return {
      enabled: this.enabled(),
      running: Boolean(this.interval),
      readOnly: true,
      market: MARKET,
      modelVersion: MODEL_VERSION,
      pollMinutes: this.pollMinutes(),
      historyDirectory: this.historyDirectory(),
      observations: this.snapshots.length,
      historicalObservations: Math.max(0, this.snapshots.length - 1),
      liveObservations: this.snapshots.length ? 1 : 0,
      coverage: this.snapshots.length
        ? {
            from: this.snapshots[0].effectiveAt,
            to: this.snapshots[this.snapshots.length - 1].effectiveAt
          }
        : undefined,
      bootstrap: {
        running: Boolean(this.refreshPromise),
        requestedDays: Math.ceil(FRAME_LIMIT / 24),
        completedDays: Math.floor(this.snapshots.length / 24)
      },
      sources: this.sourceStatuses.map((source) => ({
        name: source.label,
        ok: source.ok,
        requiresApiKey: false,
        rows: Array.isArray(source.data) ? source.data.length : 0,
        error: source.error
      })),
      lastSuccessAt: this.lastSuccessAt,
      lastErrorAt: this.lastErrorAt,
      lastError: this.lastError
    };
  }

  async getPayload(): Promise<any> {
    this.loadHistory();
    if (!this.snapshots.length) await this.refresh();
    if (this.payloadCache && Date.now() - this.payloadCache.at < 60_000) {
      return this.payloadCache.payload;
    }
    const snapshots = this.snapshots.slice(-FRAME_LIMIT).map((snapshot, index) => ({
      ...snapshot,
      i: index,
      kind: index === this.snapshots.slice(-FRAME_LIMIT).length - 1
        ? 'live-observation'
        : 'historical-backfill',
      source: 'multi-venue',
      positionCount: snapshot.zones.reduce((sum, zone) => sum + zone.positionCount, 0),
      acceptedPositionCount: snapshot.zones.length
    }));
    const frames = snapshots.map((snapshot, index) => ({
      i: index,
      t: timestampForMs(Date.parse(snapshot.effectiveAt)),
      startedAtMs: Date.parse(snapshot.effectiveAt),
      price: snapshot.referencePrice,
      low: snapshot.low,
      high: snapshot.high,
      snapshot: index
    }));
    const gaps = snapshots.map((snapshot) => snapshot.gap || null);
    const prices = snapshots.flatMap((snapshot) => [
      snapshot.referencePrice,
      ...snapshot.zones.map((zone) => zone.price)
    ]);
    const latestSnapshot = snapshots[snapshots.length - 1];
    const eventCount = snapshots.reduce((sum, snapshot) => sum + snapshot.zones.length, 0);
    let minimumPrice = Number.POSITIVE_INFINITY;
    let maximumPrice = Number.NEGATIVE_INFINITY;
    for (const price of prices) {
      if (!Number.isFinite(price)) continue;
      minimumPrice = Math.min(minimumPrice, price);
      maximumPrice = Math.max(maximumPrice, price);
    }
      const sourceStatuses = this.sourceStatuses.map((source) => ({
      label: source.label,
      ok: source.ok,
      rows: Array.isArray(source.data) ? source.data.length : 0,
      requiresApiKey: false,
      role: source.label.includes('OI')
        ? 'historical cohort inventory'
        : source.label.includes('GMX')
          ? 'monitored context; excluded from density until liquidation prices are exact'
          : 'independent price/flow confirmation',
      error: source.error
    }));
    const payload = {
      version: 2,
      modelVersion: MODEL_VERSION,
      snapshotZones: true,
      weightUnit: 'confidence-weighted USD',
      eventCount,
      source: {
        name: 'Public perp V2 multi-venue',
        market: MARKET,
        url: '/open-liquidity/v2/status',
        api:
          'Binance futures + Bybit linear + OKX swaps + Hyperliquid + dYdX + GMX public endpoints',
        method:
          'Causal multi-venue cohort reconstruction. Positive OI deltas form leveraged position cohorts; venue taker flow, market taker flow and candle body infer direction. Negative OI deltas reduce only existing venue cohorts, and later candle sweeps remove crossed liquidation levels. GMX contributes direct current inventory with an explicitly model-estimated liquidation price.',
        params: [
          `model=${MODEL_VERSION}`,
          `frames=${frames.length}`,
          `snapshots=${snapshots.length}`,
          `zones=${eventCount}`,
          `activeSources=${latestSnapshot?.availableSources?.join(',') || 'none'}`,
          'gap=min 72% clean; interior <=28% of edge support'
        ],
        sourceStatuses,
        note:
          'Observe-only multi-venue study. A green gap is rendered only when the combined Binance, Bybit and OKX cohort histograms themselves leave a strict low-density corridor. GMX is monitored as context but does not alter density while its liquidation prices are model estimates. Missing or contradictory sources lower confidence or remove the gap; no gap is painted by target coordinates. This study never places, sizes or manages dYdX orders.'
      },
      quality: {
        causalModel: true,
        persistentObservations: true,
        usesFuturePriceData: false,
        exactPositionInventory: false,
        exactLiquidationPrices: false,
        sourceAgreement: latestSnapshot?.availableSources?.length || 0,
        requiredSourceAgreement: 2,
        gapMethod: 'strict-contiguous-low-density',
        minimumGapCleanliness: Number(process.env.OPEN_LIQUIDITY_V2_MIN_GAP_CLEANLINESS || 0.72)
      },
      status: this.getStatus(),
      range: {
        minPrice: Number.isFinite(minimumPrice) ? minimumPrice : 0,
        maxPrice: Number.isFinite(maximumPrice) ? maximumPrice : 0
      },
      frames,
      snapshots,
      gaps,
      events: [],
      contextEvents: [],
      topCurrentZones: latestSnapshot
        ? latestSnapshot.zones
            .slice()
            .sort((a, b) => b.weightedUsd - a.weightedUsd)
            .slice(0, 40)
        : []
    };
    this.payloadCache = { at: Date.now(), payload };
    return payload;
  }
}

export const openLiquidityV2MultiSourceCollector = new OpenLiquidityV2MultiSourceCollector();
