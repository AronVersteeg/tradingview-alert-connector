import fs from 'fs';
import path from 'path';

import { binanceGet } from './binanceHttp';

const BINANCE_SPOT_URL = 'https://api.binance.com/api/v3/klines';
const BINANCE_FUTURES_URL = 'https://fapi.binance.com/fapi/v1/klines';
const HOUR_MS = 3_600_000;
const COHORT_WINDOW_HOURS = 8_760;
const FRAME_LIMIT = 500;
// Keep the rendered replay bounded while the live cohort state advances hourly.
const HISTORY_LIMIT = FRAME_LIMIT;
const DISPLAY_ZONE_LIMIT = 150;
const PAYLOAD_CACHE_TTL_MS = 15_000;

// Cold replays temporarily hold source candles and old/new histories. Serialize
// these bootstraps; warmed closes must not wait in this queue.
let replicaRefreshQueue: Promise<void> = Promise.resolve();
const binanceCandleCache = new Map<string, SpotCandle[]>();
let globalReplicaPayloadCache: { token: symbol; clear: () => void } | undefined;

function enqueueReplicaRefresh<T>(work: () => Promise<T>): Promise<T> {
  const run = replicaRefreshQueue.then(work, work);
  replicaRefreshQueue = run.then(() => undefined, () => undefined);
  return run;
}

function memoryUsageMb(): { rss: number; heapUsed: number; heapTotal: number } {
  const usage = process.memoryUsage();
  const mb = (bytes: number) => Math.round((bytes / 1024 / 1024) * 10) / 10;
  return {
    rss: mb(usage.rss),
    heapUsed: mb(usage.heapUsed),
    heapTotal: mb(usage.heapTotal)
  };
}

type ReplicaMarket = 'BTC-USD' | 'ETH-USD' | 'INJ-USD' | 'SOL-USD' | 'ZEC-USD' | 'PAXG-USD' | 'XAG-USD';
type ReplicaSymbol = 'BTCUSDT' | 'ETHUSDT' | 'INJUSDT' | 'SOLUSDT' | 'ZECUSDT' | 'XAUUSDT' | 'PAXGUSDT' | 'XAGUSDT';
type ReplicaVenue = 'spot' | 'futures';

type ReplicaMarketConfig = {
  market: ReplicaMarket;
  symbol: ReplicaSymbol;
  asset: 'BTC' | 'ETH' | 'INJ' | 'SOL' | 'ZEC' | 'GOLD' | 'SILVER';
  modelVersion: string;
  priceStepUsd: number;
  historyDirectoryName: string;
  historyEnv: string;
  enabledEnv: string;
  venue?: ReplicaVenue;
  confirmationSymbol?: ReplicaSymbol;
  minimumSourceHours?: number;
};

const BTC_CONFIG: ReplicaMarketConfig = {
  market: 'BTC-USD',
  symbol: 'BTCUSDT',
  asset: 'BTC',
  modelVersion: 'binance-spot-liquidation-cohorts-v2.4',
  priceStepUsd: 100,
  historyDirectoryName: 'open-liquidity-v2',
  historyEnv: 'OPEN_LIQUIDITY_V2_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_ENABLED'
};

const ETH_CONFIG: ReplicaMarketConfig = {
  market: 'ETH-USD',
  symbol: 'ETHUSDT',
  asset: 'ETH',
  modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1',
  priceStepUsd: 5,
  historyDirectoryName: 'open-liquidity-v2-eth',
  historyEnv: 'OPEN_LIQUIDITY_V2_ETH_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_ETH_ENABLED'
};

const INJ_CONFIG: ReplicaMarketConfig = {
  market: 'INJ-USD',
  symbol: 'INJUSDT',
  asset: 'INJ',
  modelVersion: 'binance-spot-inj-liquidation-cohorts-v2.1',
  priceStepUsd: 0.01,
  historyDirectoryName: 'open-liquidity-v2-inj',
  historyEnv: 'OPEN_LIQUIDITY_V2_INJ_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_INJ_ENABLED'
};

const SOL_CONFIG: ReplicaMarketConfig = {
  market: 'SOL-USD',
  symbol: 'SOLUSDT',
  asset: 'SOL',
  modelVersion: 'binance-spot-sol-liquidation-cohorts-v2.1',
  priceStepUsd: 0.1,
  historyDirectoryName: 'open-liquidity-v2-sol',
  historyEnv: 'OPEN_LIQUIDITY_V2_SOL_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_SOL_ENABLED'
};

const ZEC_CONFIG: ReplicaMarketConfig = {
  market: 'ZEC-USD',
  symbol: 'ZECUSDT',
  asset: 'ZEC',
  modelVersion: 'binance-spot-zec-liquidation-cohorts-v2.1',
  priceStepUsd: 1,
  historyDirectoryName: 'open-liquidity-v2-zec',
  historyEnv: 'OPEN_LIQUIDITY_V2_ZEC_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_ZEC_ENABLED'
};

const GOLD_CONFIG: ReplicaMarketConfig = {
  market: 'PAXG-USD',
  symbol: 'XAUUSDT',
  asset: 'GOLD',
  modelVersion: 'binance-futures-xau-liquidation-cohorts-v2.1',
  priceStepUsd: 5,
  historyDirectoryName: 'open-liquidity-v2-gold',
  historyEnv: 'OPEN_LIQUIDITY_V2_GOLD_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_GOLD_ENABLED',
  venue: 'futures',
  confirmationSymbol: 'PAXGUSDT',
  // XAUUSDT started trading in December 2025, so a full 8,760-hour
  // bootstrap cannot exist yet. The rolling model uses every available hour.
  minimumSourceHours: 720
};

const SILVER_CONFIG: ReplicaMarketConfig = {
  market: 'XAG-USD',
  symbol: 'XAGUSDT',
  asset: 'SILVER',
  modelVersion: 'binance-futures-xag-liquidation-cohorts-v2.1',
  priceStepUsd: 0.1,
  historyDirectoryName: 'open-liquidity-v2-silver',
  historyEnv: 'OPEN_LIQUIDITY_V2_SILVER_HISTORY_DIR',
  enabledEnv: 'OPEN_LIQUIDITY_V2_SILVER_ENABLED',
  venue: 'futures',
  // Binance XAGUSDT launched in January 2026. Use all available closed
  // hourly candles while retaining the same rolling cohort mechanics.
  minimumSourceHours: 720
};

type Side = 'L' | 'S';
type Leverage = 3 | 5 | 10;

export type SpotCandle = {
  timestampMs: number;
  closeTimeMs: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

export type ReplicaLiquidityZone = {
  side: Side;
  leverage: Leverage;
  price: number;
  positionCount: number;
  relativeCount: number;
  weightedUsd: number;
  notionalUsd: number;
  confidence: number;
  uncertaintyUsd: number;
  sourceCount: number;
  sources: string[];
};

export type ReplicaGap = {
  left: number;
  right: number;
  width: number;
  leftEdge: ReplicaLiquidityZone;
  rightEdge: ReplicaLiquidityZone;
  interiorRelativeCount: number;
  cleanliness: number;
  confidence: number;
  sourceAgreement: number;
  status: 'replica';
  method: 'nearest-active-cohort-edges';
};

// [side, leverage, rounded price, active cohort count]. The first replay
// frame carries a complete seed; later frames only carry changed bins.
export type CompactReplicaZone = [Side, Leverage, number, number];

export type ReplicaSnapshot = {
  version: 2;
  modelVersion: string;
  effectiveAt: string;
  observedAt: string;
  referencePrice: number;
  open: number;
  close: number;
  high: number;
  low: number;
  sourceHours: number;
  availableSources: string[];
  activeCohortCount: number;
  displayZoneCount?: number;
  zones: ReplicaLiquidityZone[];
  zoneSeed?: CompactReplicaZone[];
  zoneDeltas: CompactReplicaZone[];
  gap: ReplicaGap | null;
};

function compactReplicaHistoryInPlace(snapshots: ReplicaSnapshot[]): ReplicaSnapshot[] {
  const latestIndex = snapshots.length - 1;
  snapshots.forEach((snapshot, index) => {
    snapshot.displayZoneCount = Math.max(
      0,
      Math.trunc(finite(snapshot.displayZoneCount) || snapshot.zones?.length || 0)
    );
    // The compact seed/delta timeline already contains every historical bin.
    // Only the latest frame needs expanded zones for current TP/map summaries.
    if (index !== latestIndex) snapshot.zones = [];
  });
  return snapshots;
}

type ActiveBin = {
  side: Side;
  leverage: Leverage;
  price: number;
  cohorts: Array<{ birthIndex: number; rawPrice: number }>;
};

type CohortLevel = {
  side: Side;
  leverage: Leverage;
  price: number;
  rawPrice: number;
};

const MULTIPLIERS: Array<{ side: Side; leverage: Leverage; multiplier: number }> = [
  { side: 'L', leverage: 3, multiplier: 0.75 },
  { side: 'S', leverage: 3, multiplier: 1.5 },
  { side: 'L', leverage: 5, multiplier: 0.833 },
  { side: 'S', leverage: 5, multiplier: 1.244 },
  { side: 'L', leverage: 10, multiplier: 0.913294 },
  { side: 'S', leverage: 10, multiplier: 1.104823 }
];

function finite(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function rounded(value: number, decimals = 4): number {
  const factor = 10 ** decimals;
  return Number.isFinite(value) ? Math.round(value * factor) / factor : 0;
}

function roundedToStep(value: number, step: number): number {
  const inverse = 1 / step;
  if (Number.isInteger(inverse)) return Math.round(value * inverse) / inverse;
  return rounded(Math.round(value / step) * step, 8);
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

function binKey(side: Side, leverage: Leverage, price: number): string {
  return `${side}|${leverage}|${price}`;
}

export function ohlc4(candle: Pick<SpotCandle, 'open' | 'high' | 'low' | 'close'>): number {
  return (candle.open + candle.high + candle.low + candle.close) / 4;
}

export function cohortLevelsForOhlc4(
  referencePrice: number,
  priceStepUsd = BTC_CONFIG.priceStepUsd
): CohortLevel[] {
  return MULTIPLIERS.map(({ side, leverage, multiplier }) => {
    const rawPrice = referencePrice * multiplier;
    return {
      side,
      leverage,
      rawPrice,
      price: roundedToStep(rawPrice, priceStepUsd)
    };
  });
}

function zoneFromBin(
  bin: ActiveBin,
  priceStepUsd: number,
  sourceLabel = 'binance-spot'
): ReplicaLiquidityZone {
  const relativeCount = bin.cohorts.length;
  return {
    side: bin.side,
    leverage: bin.leverage,
    price: bin.price,
    positionCount: relativeCount,
    relativeCount,
    // Retained only for backwards-compatible clients. It is a count, never USD.
    weightedUsd: relativeCount,
    notionalUsd: 0,
    confidence: 1,
    uncertaintyUsd: priceStepUsd / 2,
    sourceCount: 1,
    sources: [sourceLabel]
  };
}

function strongestAtPrice(zones: ReplicaLiquidityZone[], price: number): ReplicaLiquidityZone {
  return zones
    .filter((zone) => zone.price === price)
    .reduce((best, zone) => zone.relativeCount > best.relativeCount ? zone : best);
}

export function detectReplicaGap(
  zones: ReplicaLiquidityZone[],
  currentPrice: number
): ReplicaGap | undefined {
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) return undefined;
  const prices = [...new Set(zones.map((zone) => zone.price))];
  const leftPrice = prices.filter((price) => price < currentPrice).sort((a, b) => b - a)[0];
  const rightPrice = prices.filter((price) => price > currentPrice).sort((a, b) => a - b)[0];
  if (!Number.isFinite(leftPrice) || !Number.isFinite(rightPrice) || leftPrice >= rightPrice) {
    return undefined;
  }
  const leftEdge = strongestAtPrice(zones, leftPrice);
  const rightEdge = strongestAtPrice(zones, rightPrice);
  const interiorRelativeCount = zones
    .filter((zone) => zone.price > leftPrice && zone.price < rightPrice)
    .reduce((sum, zone) => sum + zone.relativeCount, 0);
  return {
    left: leftPrice,
    right: rightPrice,
    width: rightPrice - leftPrice,
    leftEdge,
    rightEdge,
    interiorRelativeCount,
    cleanliness: interiorRelativeCount === 0 ? 1 : 0,
    confidence: 1,
    sourceAgreement: 1,
    status: 'replica',
    method: 'nearest-active-cohort-edges'
  };
}

function selectDisplayZones(
  zones: ReplicaLiquidityZone[],
  referencePrice: number,
  gap: ReplicaGap | undefined
): ReplicaLiquidityZone[] {
  const selected = new Map<string, ReplicaLiquidityZone>();
  const add = (zone: ReplicaLiquidityZone | undefined): void => {
    if (!zone) return;
    selected.set(binKey(zone.side, zone.leverage, zone.price), zone);
  };
  zones
    .slice()
    .sort((a, b) => b.relativeCount - a.relativeCount || a.price - b.price)
    .slice(0, 100)
    .forEach(add);
  zones
    .slice()
    .sort((a, b) =>
      Math.abs(a.price - referencePrice) - Math.abs(b.price - referencePrice) ||
      b.relativeCount - a.relativeCount
    )
    .slice(0, 48)
    .forEach(add);
  add(gap?.leftEdge);
  add(gap?.rightEdge);
  return [...selected.values()]
    .sort((a, b) => a.price - b.price || a.side.localeCompare(b.side) || a.leverage - b.leverage)
    .slice(0, DISPLAY_ZONE_LIMIT);
}

function sweepBins(bins: Map<string, ActiveBin>, candle: SpotCandle, priceStepUsd: number): void {
  for (const [key, bin] of bins) {
    if (bin.side === 'L') {
      if (candle.low <= bin.price - priceStepUsd / 2) {
        bins.delete(key);
      } else if (candle.low <= bin.price + priceStepUsd / 2) {
        bin.cohorts = bin.cohorts.filter((cohort) => candle.low > cohort.rawPrice);
      }
    } else if (candle.high >= bin.price + priceStepUsd / 2) {
      bins.delete(key);
    } else if (candle.high >= bin.price - priceStepUsd / 2) {
      bin.cohorts = bin.cohorts.filter((cohort) => candle.high < cohort.rawPrice);
    }
    if (!bin.cohorts.length) bins.delete(key);
  }
}

function expireBirths(
  bins: Map<string, ActiveBin>,
  birthKeysByIndex: Map<number, string[]>,
  expiredIndex: number
): void {
  if (expiredIndex < 0) return;
  for (const key of birthKeysByIndex.get(expiredIndex) || []) {
    const bin = bins.get(key);
    if (!bin || bin.cohorts[0]?.birthIndex !== expiredIndex) continue;
    bin.cohorts.shift();
    if (!bin.cohorts.length) bins.delete(key);
  }
  birthKeysByIndex.delete(expiredIndex);
}

function addCandleCohorts(
  bins: Map<string, ActiveBin>,
  birthKeysByIndex: Map<number, string[]>,
  candle: SpotCandle,
  frameIndex: number,
  priceStepUsd: number
): void {
  const keys: string[] = [];
  for (const level of cohortLevelsForOhlc4(ohlc4(candle), priceStepUsd)) {
    const key = binKey(level.side, level.leverage, level.price);
    const bin = bins.get(key) || {
      side: level.side,
      leverage: level.leverage,
      price: level.price,
      cohorts: []
    };
    bin.cohorts.push({ birthIndex: frameIndex, rawPrice: level.rawPrice });
    bins.set(key, bin);
    keys.push(key);
  }
  birthKeysByIndex.set(frameIndex, keys);
}

type ReplayOptions = {
  observedAt?: string;
  cohortWindowHours?: number;
  frameLimit?: number;
  priceStepUsd?: number;
  modelVersion?: string;
  sourceLabel?: string;
};

type ReplicaReplayState = {
  bins: Map<string, ActiveBin>;
  birthKeysByIndex: Map<number, string[]>;
  previousZoneCounts: Map<string, CompactReplicaZone>;
  nextIndex: number;
  lastCandle?: SpotCandle;
};

type ReplicaRefreshTiming = {
  mode: 'bootstrap' | 'incremental';
  queuedMs: number;
  fetchMs: number;
  calculationMs: number;
  totalMs: number;
  processedCandles: number;
  candleStartedAt?: string;
  candleClosedAt?: string;
  afterCloseDelayMs?: number;
};

function replicaSnapshot(
  candle: SpotCandle,
  state: ReplicaReplayState,
  options: ReplayOptions,
  first: boolean
): ReplicaSnapshot {
  const priceStepUsd = Math.max(0.01, finite(options.priceStepUsd) || BTC_CONFIG.priceStepUsd);
  const sourceLabel = options.sourceLabel || 'binance-spot';
  const allZones = [...state.bins.values()].map((bin) => zoneFromBin(bin, priceStepUsd, sourceLabel));
  const referencePrice = ohlc4(candle);
  const gap = detectReplicaGap(allZones, referencePrice);
  const zones = selectDisplayZones(allZones, referencePrice, gap);
  const currentZoneCounts = new Map<string, CompactReplicaZone>(allZones.map((zone) => [
    binKey(zone.side, zone.leverage, zone.price),
    [zone.side, zone.leverage, zone.price, zone.relativeCount]
  ]));
  const zoneDeltas: CompactReplicaZone[] = [];
  if (!first) {
    for (const key of new Set([...state.previousZoneCounts.keys(), ...currentZoneCounts.keys()])) {
      const previous = state.previousZoneCounts.get(key);
      const current = currentZoneCounts.get(key);
      if ((previous?.[3] || 0) === (current?.[3] || 0)) continue;
      zoneDeltas.push(current || [previous![0], previous![1], previous![2], 0]);
    }
  }
  state.previousZoneCounts = currentZoneCounts;
  return {
    version: 2,
    modelVersion: options.modelVersion || BTC_CONFIG.modelVersion,
    effectiveAt: new Date(candle.timestampMs).toISOString(),
    observedAt: options.observedAt || new Date().toISOString(),
    referencePrice: rounded(referencePrice, 4),
    open: rounded(candle.open, 4),
    close: rounded(candle.close, 4),
    high: rounded(candle.high, 4),
    low: rounded(candle.low, 4),
    sourceHours: Math.min(state.nextIndex, options.cohortWindowHours || COHORT_WINDOW_HOURS),
    availableSources: [sourceLabel],
    activeCohortCount: allZones.reduce((sum, zone) => sum + zone.relativeCount, 0),
    zones,
    zoneSeed: first ? [...currentZoneCounts.values()] : undefined,
    zoneDeltas,
    gap: gap || null
  };
}

function advanceReplicaState(state: ReplicaReplayState, candle: SpotCandle, options: ReplayOptions): void {
  const step = Math.max(0.01, finite(options.priceStepUsd) || BTC_CONFIG.priceStepUsd);
  sweepBins(state.bins, candle, step);
  expireBirths(state.bins, state.birthKeysByIndex, state.nextIndex - options.cohortWindowHours!);
  addCandleCohorts(state.bins, state.birthKeysByIndex, candle, state.nextIndex, step);
  state.nextIndex += 1;
  state.lastCandle = candle;
}

function* replayReplicaSnapshots(
  candles: SpotCandle[], options: ReplayOptions
): Generator<void, { snapshots: ReplicaSnapshot[]; state: ReplicaReplayState }> {
  const ordered = candles
    .filter((candle) => candle.timestampMs > 0 && candle.close > 0)
    .slice()
    .sort((a, b) => a.timestampMs - b.timestampMs);
  const observedAt = options.observedAt || new Date().toISOString();
  const cohortWindowHours = boundedInteger(
    options.cohortWindowHours,
    COHORT_WINDOW_HOURS,
    1,
    COHORT_WINDOW_HOURS
  );
  const frameLimit = boundedInteger(options.frameLimit, FRAME_LIMIT, 1, HISTORY_LIMIT);
  const snapshotStart = Math.max(0, ordered.length - frameLimit);
  const state: ReplicaReplayState = {
    bins: new Map(), birthKeysByIndex: new Map(), previousZoneCounts: new Map(), nextIndex: 0
  };
  const snapshots: ReplicaSnapshot[] = [];
  const replayOptions = { ...options, observedAt, cohortWindowHours };

  for (let frameIndex = 0; frameIndex < ordered.length; frameIndex += 1) {
    const candle = ordered[frameIndex];
    // Existing cohorts can be liquidated by this candle. Cohorts born on this
    // candle are added afterwards, so they can only be swept by a later hour.
    advanceReplicaState(state, candle, replayOptions);
    if (frameIndex >= snapshotStart) snapshots.push(replicaSnapshot(candle, state, replayOptions, !snapshots.length));
    if (frameIndex % 128 === 127) yield;
  }
  return { snapshots, state };
}

export function buildReplicaSnapshots(candles: SpotCandle[], options: ReplayOptions = {}): ReplicaSnapshot[] {
  const replay = replayReplicaSnapshots(candles, options);
  let step = replay.next();
  while (!step.done) step = replay.next();
  return step.value.snapshots;
}

// Raw cohort prices and birth indices are retained; rounded snapshot counts
// alone cannot reproduce later sweeps or rolling-window expiry exactly.
export class IncrementalReplicaReplay {
  private state: ReplicaReplayState;
  readonly snapshots: ReplicaSnapshot[];
  private constructor(replayed: { snapshots: ReplicaSnapshot[]; state: ReplicaReplayState }, private options: ReplayOptions) {
    this.state = replayed.state;
    this.snapshots = replayed.snapshots;
  }

  static async create(candles: SpotCandle[], options: ReplayOptions = {}): Promise<IncrementalReplicaReplay> {
    const normalized = {
      ...options,
      cohortWindowHours: boundedInteger(options.cohortWindowHours, COHORT_WINDOW_HOURS, 1, COHORT_WINDOW_HOURS),
      frameLimit: boundedInteger(options.frameLimit, FRAME_LIMIT, 1, HISTORY_LIMIT)
    };
    const replay = replayReplicaSnapshots(candles, normalized);
    let step = replay.next();
    while (!step.done) {
      await new Promise<void>((resolve) => setImmediate(resolve));
      step = replay.next();
    }
    return new IncrementalReplicaReplay(step.value, normalized);
  }

  get lastCandle(): SpotCandle | undefined { return this.state.lastCandle; }

  append(candle: SpotCandle, observedAt = new Date().toISOString()): ReplicaSnapshot {
    if (this.lastCandle && candle.timestampMs !== this.lastCandle.timestampMs + HOUR_MS) {
      throw new Error('Public V2 live replay requires consecutive closed 1H candles.');
    }
    if (candle.closeTimeMs > Date.now() || !(candle.close > 0)) {
      throw new Error('Public V2 live replay refuses an open or invalid candle.');
    }
    advanceReplicaState(this.state, candle, this.options);
    const snapshot = replicaSnapshot(candle, this.state, { ...this.options, observedAt }, !this.snapshots.length);
    this.snapshots.push(snapshot);
    while (this.snapshots.length > this.options.frameLimit!) {
      const seed = new Map((this.snapshots[0].zoneSeed || []).map((zone) => [binKey(zone[0], zone[1], zone[2]), zone]));
      for (const zone of this.snapshots[1].zoneDeltas) {
        const key = binKey(zone[0], zone[1], zone[2]);
        if (zone[3] > 0) seed.set(key, zone);
        else seed.delete(key);
      }
      this.snapshots.shift();
      this.snapshots[0].zoneSeed = [...seed.values()];
      this.snapshots[0].zoneDeltas = [];
    }
    return snapshot;
  }
}

async function fetchBinanceCandles(
  url: string,
  sourceHours = COHORT_WINDOW_HOURS + FRAME_LIMIT + 24,
  nowMs = Date.now(),
  symbol: ReplicaSymbol = BTC_CONFIG.symbol
): Promise<SpotCandle[]> {
  const startTime = Math.floor((nowMs - sourceHours * HOUR_MS) / HOUR_MS) * HOUR_MS;
  const cacheKey = `${url}|${symbol}`;
  const cached = (binanceCandleCache.get(cacheKey) || [])
    .filter((candle) => candle.timestampMs >= startTime && candle.timestampMs <= nowMs);
  const candles: SpotCandle[] = [...cached];
  const cachedFirst = cached[0]?.timestampMs;
  const cachedLast = cached[cached.length - 1]?.timestampMs;
  const cacheCoversStart = cachedFirst !== undefined && cachedFirst <= startTime;
  let cursor = cacheCoversStart && cachedLast !== undefined
    ? Math.max(startTime, cachedLast - HOUR_MS)
    : startTime;
  while (cursor < nowMs) {
    const response = await binanceGet(url, {
      timeout: 30_000,
      params: {
        symbol,
        interval: '1h',
        startTime: cursor,
        endTime: nowMs,
        limit: 1_000
      }
    });
    if (!Array.isArray(response.data) || !response.data.length) break;
    const page = response.data
      .map((row: any[]) => ({
        timestampMs: finite(row[0]),
        closeTimeMs: finite(row[6]),
        open: finite(row[1]),
        high: finite(row[2]),
        low: finite(row[3]),
        close: finite(row[4])
      }))
      .filter((candle: SpotCandle) =>
        candle.timestampMs >= startTime &&
        candle.closeTimeMs <= nowMs &&
        candle.close > 0
      );
    candles.push(...page);
    const nextCursor = finite(response.data[response.data.length - 1]?.[0]) + HOUR_MS;
    if (nextCursor <= cursor) break;
    cursor = nextCursor;
    if (response.data.length < 1_000) break;
  }
  const merged = [...new Map(candles.map((candle) => [candle.timestampMs, candle])).values()]
    .sort((a, b) => a.timestampMs - b.timestampMs);
  binanceCandleCache.set(cacheKey, merged);
  return merged;
}

export async function fetchBinanceSpotCandles(
  sourceHours = COHORT_WINDOW_HOURS + FRAME_LIMIT + 24,
  nowMs = Date.now(),
  symbol: ReplicaSymbol = BTC_CONFIG.symbol
): Promise<SpotCandle[]> {
  return fetchBinanceCandles(BINANCE_SPOT_URL, sourceHours, nowMs, symbol);
}

export async function fetchBinanceFuturesCandles(
  sourceHours = COHORT_WINDOW_HOURS + FRAME_LIMIT + 24,
  nowMs = Date.now(),
  symbol: ReplicaSymbol = GOLD_CONFIG.symbol
): Promise<SpotCandle[]> {
  return fetchBinanceCandles(BINANCE_FUTURES_URL, sourceHours, nowMs, symbol);
}

export type GoldConfirmationSummary = {
  primarySymbol: 'XAUUSDT';
  confirmationSymbol: 'PAXGUSDT';
  alignedHours: number;
  directionAgreementPct: number;
  latest: {
    timestamp: string;
    xauPrice: number;
    paxgPrice: number;
    basisUsd: number;
    basisPct: number;
    directionMatch: boolean;
  };
};

export function summarizeGoldConfirmation(
  primaryCandles: SpotCandle[],
  confirmationCandles: SpotCandle[]
): GoldConfirmationSummary | undefined {
  const confirmationByTime = new Map(
    confirmationCandles.map((candle) => [candle.timestampMs, candle])
  );
  const aligned = primaryCandles
    .map((primary) => ({ primary, confirmation: confirmationByTime.get(primary.timestampMs) }))
    .filter((pair): pair is { primary: SpotCandle; confirmation: SpotCandle } => Boolean(pair.confirmation));
  if (!aligned.length) return undefined;

  const directionMatches = aligned.filter(({ primary, confirmation }) =>
    Math.sign(primary.close - primary.open) === Math.sign(confirmation.close - confirmation.open)
  ).length;
  const latest = aligned[aligned.length - 1];
  const xauPrice = ohlc4(latest.primary);
  const paxgPrice = ohlc4(latest.confirmation);
  const basisUsd = paxgPrice - xauPrice;

  return {
    primarySymbol: 'XAUUSDT',
    confirmationSymbol: 'PAXGUSDT',
    alignedHours: aligned.length,
    directionAgreementPct: rounded((directionMatches / aligned.length) * 100, 1),
    latest: {
      timestamp: new Date(latest.primary.timestampMs).toISOString(),
      xauPrice: rounded(xauPrice, 4),
      paxgPrice: rounded(paxgPrice, 4),
      basisUsd: rounded(basisUsd, 4),
      basisPct: rounded((basisUsd / xauPrice) * 100, 4),
      directionMatch:
        Math.sign(latest.primary.close - latest.primary.open) ===
        Math.sign(latest.confirmation.close - latest.confirmation.open)
    }
  };
}

export class OpenLiquidityV2ReplicaCollector {
  private interval: NodeJS.Timeout | undefined;
  private initialTimer: NodeJS.Timeout | undefined;
  private refreshPromise: Promise<void> | undefined;
  private liveReplay: IncrementalReplicaReplay | undefined;
  private persistenceTimer: NodeJS.Timeout | undefined;
  private persistencePromise: Promise<void> = Promise.resolve();
  private lastRefreshTiming: ReplicaRefreshTiming | undefined;
  private snapshots: ReplicaSnapshot[] = [];
  private loaded = false;
  private lastSuccessAt: string | undefined;
  private lastErrorAt: string | undefined;
  private lastError: string | undefined;
  private sourceRows = 0;
  private confirmationRows = 0;
  private confirmationError: string | undefined;
  private confirmationCandles = new Map<number, SpotCandle>();
  private goldConfirmation: GoldConfirmationSummary | undefined;
  private payloadCache: { at: number; payload: any } | undefined;
  private payloadCacheTimer: NodeJS.Timeout | undefined;
  private readonly payloadCacheToken = Symbol('replica-payload-cache');

  constructor(private readonly config: ReplicaMarketConfig = BTC_CONFIG) {}

  private enabled(): boolean {
    return enabled(this.config.enabledEnv, true);
  }

  private pollMinutes(): number {
    return boundedInteger(process.env.OPEN_LIQUIDITY_V2_POLL_MINUTES, 60, 10, 1_440);
  }

  historyDirectory(): string {
    const explicit = String(process.env[this.config.historyEnv] || '').trim();
    if (explicit) return explicit;
    const domDirectory = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
    if (domDirectory) return path.join(path.dirname(domDirectory), this.config.historyDirectoryName);
    const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
    const base = fs.existsSync(renderDisk) ? renderDisk : path.join(process.cwd(), 'data');
    return path.join(base, this.config.historyDirectoryName);
  }

  private historyFile(): string {
    return path.join(this.historyDirectory(), `${this.config.modelVersion}.json`);
  }

  start(initialDelayMs = 0): void {
    if (!this.enabled() || this.interval || this.initialTimer) return;
    const begin = () => {
      this.initialTimer = undefined;
      this.refresh().catch((error) => console.error(`Initial ${this.config.asset} Public Perp V2 replica refresh failed:`, error));
      this.interval = setInterval(() => {
        this.refresh().catch((error) => console.error(`${this.config.asset} Public Perp V2 replica refresh failed:`, error));
      }, this.pollMinutes() * 60_000);
    };
    if (initialDelayMs > 0) {
      this.initialTimer = setTimeout(begin, initialDelayMs);
    } else {
      begin();
    }
    console.log('Public Perp V2 replica collector started:', {
      market: this.config.market,
      modelVersion: this.config.modelVersion,
      pollMinutes: this.pollMinutes(),
      historyDirectory: this.historyDirectory(),
      readOnly: true
    });
  }

  stop(): void {
    if (this.interval) clearInterval(this.interval);
    if (this.initialTimer) clearTimeout(this.initialTimer);
    if (this.payloadCacheTimer) clearTimeout(this.payloadCacheTimer);
    if (this.persistenceTimer) {
      clearTimeout(this.persistenceTimer);
      this.persistenceTimer = undefined;
      this.persistHistoryLater();
    }
    this.interval = undefined;
    this.initialTimer = undefined;
    this.payloadCacheTimer = undefined;
    this.payloadCache = undefined;
  }

  private loadHistory(): void {
    if (this.loaded) return;
    this.loaded = true;
    const file = this.historyFile();
    if (!fs.existsSync(file)) return;
    try {
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (Array.isArray(parsed)) {
        this.snapshots = compactReplicaHistoryInPlace(parsed
          .filter((snapshot) => snapshot?.modelVersion === this.config.modelVersion && snapshot?.effectiveAt)
          .sort((a, b) => Date.parse(a.effectiveAt) - Date.parse(b.effectiveAt))
          .slice(-HISTORY_LIMIT));
      }
    } catch (error) {
      console.warn('Public Perp V2 replica history could not be read:', {
        file,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private persistHistoryLater(): void {
    const file = this.historyFile();
    const temporary = `${file}.tmp`;
    const serialized = JSON.stringify(this.snapshots);
    this.persistencePromise = this.persistencePromise.then(async () => {
      await fs.promises.mkdir(this.historyDirectory(), { recursive: true });
      await fs.promises.writeFile(temporary, serialized);
      await fs.promises.rename(temporary, file);
    }).catch((error) => console.error('Public V2 replay persistence failed:', {
      market: this.config.market, error: error instanceof Error ? error.message : String(error)
    }));
  }

  private schedulePersistence(): void {
    if (this.persistenceTimer) return;
    // Alerts consume the in-memory close first; disk work is not an entry gate.
    this.persistenceTimer = setTimeout(() => {
      this.persistenceTimer = undefined;
      this.persistHistoryLater();
    }, 10_000);
    this.persistenceTimer.unref?.();
  }

  async refresh(): Promise<void> {
    if (this.refreshPromise) return this.refreshPromise;
    const requestedAt = Date.now();
    const work = () => this.refreshInternal(requestedAt);
    // Only cold bootstraps share the memory-protection queue. A warmed market
    // advances a handful of candles without waiting for another pair's replay.
    this.refreshPromise = (this.liveReplay ? work() : enqueueReplicaRefresh(work)).finally(() => {
      this.refreshPromise = undefined;
    });
    return this.refreshPromise;
  }

  async refreshForLatestClosedHour(nowMs = Date.now()): Promise<boolean> {
    this.loadHistory();
    const latestClosedHour = Math.floor(nowMs / HOUR_MS) * HOUR_MS - HOUR_MS;
    const latestSnapshotHour = Date.parse(this.snapshots[this.snapshots.length - 1]?.effectiveAt || '');
    if (Number.isFinite(latestSnapshotHour) && latestSnapshotHour >= latestClosedHour) return false;
    await this.refresh();
    const refreshedHour = Date.parse(this.snapshots[this.snapshots.length - 1]?.effectiveAt || '');
    if (!Number.isFinite(refreshedHour) || refreshedHour < latestClosedHour) {
      throw new Error(`Binance ${this.config.symbol} has not published the latest closed 1H candle for Public V2.`);
    }
    return true;
  }

  private clearPayloadCache(): void {
    if (this.payloadCacheTimer) clearTimeout(this.payloadCacheTimer);
    this.payloadCacheTimer = undefined;
    this.payloadCache = undefined;
    if (globalReplicaPayloadCache?.token === this.payloadCacheToken) {
      globalReplicaPayloadCache = undefined;
    }
  }

  private async refreshInternal(requestedAt = Date.now()): Promise<void> {
    const startedAt = Date.now();
    this.loadHistory();
    this.clearPayloadCache();
    const memoryBefore = memoryUsageMb();
    try {
      const sourceHours = COHORT_WINDOW_HOURS + FRAME_LIMIT + 24;
      const nowMs = Date.now();
      const fetchPrimary = this.config.venue === 'futures'
        ? fetchBinanceFuturesCandles
        : fetchBinanceSpotCandles;
      const confirmationPromise: Promise<SpotCandle[]> = this.config.confirmationSymbol
        ? fetchBinanceFuturesCandles(sourceHours, nowMs, this.config.confirmationSymbol)
            .catch((error) => {
              this.confirmationError = error instanceof Error ? error.message : String(error);
              console.warn(`${this.config.asset} confirmation feed refresh failed; primary map will continue:`, {
                symbol: this.config.confirmationSymbol,
                error: this.confirmationError
              });
              return [] as SpotCandle[];
            })
        : Promise.resolve([] as SpotCandle[]);
      const [candles, confirmationCandles] = await Promise.all([
        fetchPrimary(sourceHours, nowMs, this.config.symbol),
        confirmationPromise
      ]);
      const fetchedAt = Date.now();
      const minimumSourceHours = this.config.minimumSourceHours || COHORT_WINDOW_HOURS;
      if (candles.length < minimumSourceHours) {
        const venueName = this.config.venue === 'futures' ? 'Futures' : 'Spot';
        throw new Error(
          `Only ${candles.length} Binance ${venueName} hours received; ${minimumSourceHours} required.`
        );
      }
      this.sourceRows = candles.length;
      this.confirmationRows = confirmationCandles.length;
      this.confirmationCandles = new Map(
        confirmationCandles.map((candle) => [candle.timestampMs, candle])
      );
      this.goldConfirmation = this.config.asset === 'GOLD'
        ? summarizeGoldConfirmation(candles, confirmationCandles)
        : undefined;
      if (confirmationCandles.length) this.confirmationError = undefined;
      const sourceLabel = this.config.venue === 'futures'
        ? `binance-futures-${this.config.symbol.toLowerCase()}`
        : 'binance-spot';
      const replayOptions = {
        frameLimit: FRAME_LIMIT,
        priceStepUsd: this.config.priceStepUsd,
        modelVersion: this.config.modelVersion,
        sourceLabel
      };
      const previous = this.liveReplay?.lastCandle;
      const overlap = previous && candles.find((candle) => candle.timestampMs === previous.timestampMs);
      const corrected = previous && (!overlap || ['open', 'high', 'low', 'close'].some((key) => overlap[key] !== previous[key]));
      const newCandles = previous ? candles.filter((candle) => candle.timestampMs > previous.timestampMs) : candles;
      if (previous && !corrected) {
        // Validate the whole catch-up before mutating state. Never skip an hour.
        newCandles.forEach((candle, index) => {
          if (candle.timestampMs !== previous.timestampMs + (index + 1) * HOUR_MS) {
            throw new Error(`Binance ${this.config.symbol} live replay is missing a closed 1H candle.`);
          }
        });
      }
      const replayMode = !this.liveReplay || corrected ? 'bootstrap' : 'incremental';
      if (replayMode === 'bootstrap') {
        const build = () => IncrementalReplicaReplay.create(candles, replayOptions);
        this.liveReplay = await (corrected ? enqueueReplicaRefresh(build) : build());
      } else {
        for (const candle of newCandles) this.liveReplay!.append(candle);
      }
      this.snapshots = compactReplicaHistoryInPlace(this.liveReplay!.snapshots);
      // A dashboard read may have cached the old replay while fetch was pending.
      this.clearPayloadCache();
      if (replayMode === 'bootstrap' || newCandles.length) this.schedulePersistence();
      this.lastSuccessAt = new Date().toISOString();
      const latest = this.liveReplay!.lastCandle;
      this.lastRefreshTiming = {
        mode: replayMode,
        queuedMs: startedAt - requestedAt,
        fetchMs: fetchedAt - startedAt,
        calculationMs: Date.now() - fetchedAt,
        totalMs: Date.now() - requestedAt,
        processedCandles: replayMode === 'bootstrap' ? candles.length : newCandles.length,
        candleStartedAt: latest ? new Date(latest.timestampMs).toISOString() : undefined,
        candleClosedAt: latest ? new Date(latest.timestampMs + HOUR_MS).toISOString() : undefined,
        afterCloseDelayMs: latest ? Date.now() - latest.timestampMs - HOUR_MS : undefined
      };
      this.lastError = undefined;
      console.log('Public Perp V2 replica refreshed:', {
        snapshots: this.snapshots.length,
        from: this.snapshots[0]?.effectiveAt,
        to: this.snapshots[this.snapshots.length - 1]?.effectiveAt,
        source: `Binance ${this.config.venue === 'futures' ? 'Futures' : 'Spot'} ${this.config.symbol} 1H`,
        sourceRows: candles.length,
        confirmationSymbol: this.config.confirmationSymbol,
        confirmationRows: confirmationCandles.length,
        cohortWindowHours: COHORT_WINDOW_HOURS,
        timing: this.lastRefreshTiming,
        memoryMb: {
          before: memoryBefore,
          after: memoryUsageMb()
        }
      });
    } catch (error) {
      this.lastErrorAt = new Date().toISOString();
      this.lastError = error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  getStatus(): any {
    this.loadHistory();
    const venueName = this.config.venue === 'futures' ? 'Futures' : 'Spot';
    const minimumSourceHours = this.config.minimumSourceHours || COHORT_WINDOW_HOURS;
    const sources = [{
      name: `Binance ${venueName} ${this.config.symbol} 1H`,
      ok: this.sourceRows >= minimumSourceHours,
      requiresApiKey: false,
      rows: this.sourceRows,
      role: 'OHLC4 cohort creation and subsequent liquidation sweep source'
    }];
    if (this.config.confirmationSymbol) {
      sources.push({
        name: `Binance Futures ${this.config.confirmationSymbol} 1H`,
        ok: this.confirmationRows > 0,
        requiresApiKey: false,
        rows: this.confirmationRows,
        role: 'Independent tokenized-gold price and candle-direction confirmation source'
      });
    }
    return {
      enabled: this.enabled(),
      running: Boolean(this.interval || this.initialTimer),
      readOnly: true,
      market: this.config.market,
      modelVersion: this.config.modelVersion,
      pollMinutes: this.pollMinutes(),
      historyDirectory: this.historyDirectory(),
      observations: this.snapshots.length,
      coverage: this.snapshots.length ? {
        from: this.snapshots[0].effectiveAt,
        to: this.snapshots[this.snapshots.length - 1].effectiveAt
      } : undefined,
      bootstrap: {
        running: Boolean(this.refreshPromise),
        requestedDays: Math.ceil((COHORT_WINDOW_HOURS + FRAME_LIMIT) / 24),
        completedDays: Math.floor(this.sourceRows / 24)
      },
      sources,
      goldConfirmation: this.goldConfirmation,
      confirmationError: this.confirmationError,
      lastSuccessAt: this.lastSuccessAt,
      lastErrorAt: this.lastErrorAt,
      lastError: this.lastError,
      liveReplayReady: Boolean(this.liveReplay),
      lastRefreshTiming: this.lastRefreshTiming
    };
  }

  async getPayload(): Promise<any> {
    this.loadHistory();
    if (!this.snapshots.length) await this.refresh();
    if (this.payloadCache && Date.now() - this.payloadCache.at < 60_000) {
      return this.payloadCache.payload;
    }
    const recent = this.snapshots.slice(-FRAME_LIMIT);
    const venueName = this.config.venue === 'futures' ? 'Futures' : 'Spot';
    const sourceKey = this.config.venue === 'futures'
      ? `binance-futures-${this.config.symbol.toLowerCase()}-replica`
      : 'binance-spot-replica';
    const zoneSeed = recent[0]?.zoneSeed || [];
    const zoneDeltas = recent.map((snapshot) => snapshot.zoneDeltas || []);
    const snapshots = recent.map((snapshot, index) => {
      const { zoneSeed: _zoneSeed, zoneDeltas: _zoneDeltas, ...compactSnapshot } = snapshot;
      return {
        ...compactSnapshot,
        // Full histogram state is reconstructed from the compact timeline.
        zones: [],
        i: index,
        kind: index === recent.length - 1 ? 'live-observation' : 'historical-backfill',
        source: sourceKey,
        positionCount: snapshot.activeCohortCount,
        acceptedPositionCount: snapshot.displayZoneCount || snapshot.zones.length
      };
    });
    const frames = snapshots.map((snapshot, index) => {
      const timestampMs = Date.parse(snapshot.effectiveAt);
      const confirmation = this.confirmationCandles.get(timestampMs);
      const confirmationPrice = confirmation ? ohlc4(confirmation) : undefined;
      return {
        i: index,
        t: timestampForMs(timestampMs),
        startedAtMs: timestampMs,
        price: snapshot.referencePrice,
        open: snapshot.open,
        close: snapshot.close,
        low: snapshot.low,
        high: snapshot.high,
        snapshot: index,
        ...(confirmation && confirmationPrice
          ? {
              goldConfirmation: {
                symbol: this.config.confirmationSymbol,
                price: rounded(confirmationPrice, 4),
                open: rounded(confirmation.open, 4),
                close: rounded(confirmation.close, 4),
                basisUsd: rounded(confirmationPrice - snapshot.referencePrice, 4),
                basisPct: rounded(
                  ((confirmationPrice - snapshot.referencePrice) / snapshot.referencePrice) * 100,
                  4
                ),
                directionMatch:
                  Math.sign(snapshot.close - snapshot.open) ===
                  Math.sign(confirmation.close - confirmation.open)
              }
            }
          : {})
      };
    });
    const prices = [
      ...snapshots.map((snapshot) => snapshot.referencePrice),
      ...zoneSeed.map((zone) => zone[2]),
      ...zoneDeltas.flatMap((deltas) => deltas.map((zone) => zone[2]))
    ].filter(Number.isFinite);
    const latestInternalSnapshot = recent[recent.length - 1];
    const eventCount = zoneSeed.length + zoneDeltas.reduce((sum, deltas) => sum + deltas.length, 0);
    const payload = {
      version: 2,
      modelVersion: this.config.modelVersion,
      snapshotZones: true,
      compactZoneTimeline: true,
      zoneSeed,
      zoneDeltas,
      weightUnit: 'relative active cohort count',
      eventCount,
      goldConfirmation: this.goldConfirmation,
      source: {
        name: this.config.asset === 'GOLD'
          ? 'Gold/USD Public Perp V2 XAU replica'
          : this.config.asset === 'SILVER'
            ? 'Silver/USD Public Perp V2 XAG replica'
            : `${this.config.asset}/USD Public Perp V2 Binance Spot replica`,
        market: this.config.market,
        url: `/open-liquidity/v2/status?market=${this.config.market}`,
        api: [
          `Binance ${venueName} ${this.config.symbol} public 1H klines`,
          this.config.confirmationSymbol
            ? `Binance Futures ${this.config.confirmationSymbol} confirmation`
            : '',
          'no API key'
        ].filter(Boolean).join('; '),
        method:
          `Each closed 1H Binance ${venueName} ${this.config.symbol} candle creates six 3x, 5x and 10x long/short cohorts from OHLC4. Exact reconstructed multipliers are applied, prices are rounded to $${this.config.priceStepUsd}, later highs/lows remove crossed cohorts, and up to the latest 8,760 birth hours remain active.`,
        params: [
          `model=${this.config.modelVersion}`,
          `cohortWindowHours=${COHORT_WINDOW_HOURS}`,
          `priceStepUsd=${this.config.priceStepUsd}`,
          `frames=${frames.length}`,
          `zones=${eventCount}`,
          'multipliers=L3 .75,S3 1.5,L5 .833,S5 1.244,L10 .913294,S10 1.104823'
        ],
        sourceStatuses: this.getStatus().sources,
        note:
          this.config.asset === 'GOLD'
            ? 'Gold reconstruction. XAUUSDT Futures drives the causal cohort map and PAXGUSDT independently confirms price basis and hourly direction. Histogram height is a relative count, not USD volume or account inventory. Server-side intrusion monitoring and optional dYdX PAXG-USD execution are handled by the separate Gold trade monitor.'
            : this.config.asset === 'SILVER'
              ? 'Silver reconstruction. XAGUSDT Futures drives the causal cohort map. Histogram height is a relative count, not USD volume or account inventory. Server-side filtered-intrusion monitoring and dYdX XAG-USD execution are handled by the separate Silver trade monitor.'
              : 'Observe-only Decentrader-compatible reconstruction. Histogram height is a relative count of still-active hourly cohorts, not USD volume, open interest or account inventory. Replay is causal: a cohort can only disappear on a later candle. This source never sends alerts and never places, sizes or manages dYdX orders.'
      },
      quality: {
        causalModel: true,
        persistentObservations: true,
        usesFuturePriceData: false,
        exactPositionInventory: false,
        exactLiquidationPrices: false,
        decentraderFormulaParity: true,
        sourceAgreement: 1,
        requiredSourceAgreement: 1,
        gapMethod: 'nearest-active-cohort-edges',
        priceStepUsd: this.config.priceStepUsd,
        cohortWindowHours: COHORT_WINDOW_HOURS
      },
      status: this.getStatus(),
      range: {
        minPrice: prices.length ? Math.min(...prices) : 0,
        maxPrice: prices.length ? Math.max(...prices) : 0
      },
      frames,
      snapshots,
      gaps: snapshots.map((snapshot) => snapshot.gap || null),
      events: [],
      contextEvents: [],
      topCurrentZones: latestInternalSnapshot
        ? latestInternalSnapshot.zones.slice().sort((a, b) => b.relativeCount - a.relativeCount).slice(0, 40)
        : []
    };
    if (globalReplicaPayloadCache?.token !== this.payloadCacheToken) {
      globalReplicaPayloadCache?.clear();
    }
    this.clearPayloadCache();
    this.payloadCache = { at: Date.now(), payload };
    globalReplicaPayloadCache = {
      token: this.payloadCacheToken,
      clear: () => this.clearPayloadCache()
    };
    this.payloadCacheTimer = setTimeout(() => {
      this.payloadCache = undefined;
      this.payloadCacheTimer = undefined;
      if (globalReplicaPayloadCache?.token === this.payloadCacheToken) {
        globalReplicaPayloadCache = undefined;
      }
    }, PAYLOAD_CACHE_TTL_MS);
    this.payloadCacheTimer.unref?.();
    return payload;
  }
}

export const openLiquidityV2BtcCollector = new OpenLiquidityV2ReplicaCollector(BTC_CONFIG);
export const openLiquidityV2EthCollector = new OpenLiquidityV2ReplicaCollector(ETH_CONFIG);
export const openLiquidityV2InjCollector = new OpenLiquidityV2ReplicaCollector(INJ_CONFIG);
export const openLiquidityV2SolCollector = new OpenLiquidityV2ReplicaCollector(SOL_CONFIG);
export const openLiquidityV2ZecCollector = new OpenLiquidityV2ReplicaCollector(ZEC_CONFIG);
export const openLiquidityV2GoldCollector = new OpenLiquidityV2ReplicaCollector(GOLD_CONFIG);
export const openLiquidityV2SilverCollector = new OpenLiquidityV2ReplicaCollector(SILVER_CONFIG);
