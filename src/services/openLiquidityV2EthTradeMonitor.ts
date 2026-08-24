import fs from 'fs';
import path from 'path';

import { AlertObject } from '../types';

import {
  coinGlassEthWhaleCollector,
  coinGlassInjWhaleCollector
} from './coinGlassEthWhaleCollector';
import { decentralizedDomCollectorForMarket } from './decentralizedDomCollector';
import {
  IntrusionImpulseQuality,
  evaluateIntrusionImpulseQuality
} from './intrusionImpulseQuality';
import {
  AlertState,
  DecentraderRow,
  DecentraderTradeExecutor,
  DydxOpenPosition,
  DydxSizingAccountSnapshot,
  Gap,
  GapAlert,
  LiquidityBar,
  TradePlanDirection,
  TradeZone,
  buildDecentraderDynamicSlAlert,
  buildDecentraderDynamicTpAlert,
  buildDecentraderFlatCleanupAlert,
  buildDecentraderOrderAlert,
  buildDecentraderStopBreachFlatAlert,
  buildDirectionalPlan,
  buildFractalStop,
  decentraderRegularIntrusionEmailEnabled,
  dydxHourlyCandlesToFractalRows,
  fetchBinanceFuturesHourlyCandlesForSymbol,
  fetchDydxHourlyCandlesForMarket,
  filteredAlertBody,
  gapFibonacciConfluenceForZone,
  intrusionCandleReview,
  mapDirectionFromAlert,
  nlTime,
  nowNlIso,
  sendEmailBestEffort,
  stabilizeManagedTakeProfits,
  smtpSettingsFromEnv
} from './decentraderGapMonitor';
import {
  CompactReplicaZone,
  OpenLiquidityV2ReplicaCollector,
  openLiquidityV2EthCollector,
  openLiquidityV2GoldCollector,
  openLiquidityV2InjCollector,
  openLiquidityV2SilverCollector
} from './openLiquidityV2Replica';

const MARKET = 'ETH-USD';
const SYMBOL = 'ETHUSDT';
const PRICE_STEP = 5;
const HOUR_MS = 3_600_000;

type WhaleSnapshotProvider = {
  snapshot(): any;
};

export type OpenLiquidityV2TradeMonitorConfig = {
  market: string;
  symbol: string;
  asset: 'ETH' | 'INJ' | 'GOLD' | 'SILVER';
  priceStep: number;
  tradeCapable: boolean;
  enabledEnv: string;
  autoTradeEnv: string;
  inheritDecentraderAutoTrade: boolean;
  stateFileEnv: string;
  stateFileName: string;
  strategyPrefix: string;
  edgeBufferEnv: string;
  coinGlassMinUsdEnv: string;
  coinGlassMaxDistanceEnv: string;
  coinGlassMaxDistanceUsd: number;
  coinGlass?: WhaleSnapshotProvider;
};

const ETH_MONITOR_CONFIG: OpenLiquidityV2TradeMonitorConfig = {
  market: MARKET,
  symbol: SYMBOL,
  asset: 'ETH',
  priceStep: PRICE_STEP,
  tradeCapable: true,
  enabledEnv: 'OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED',
  autoTradeEnv: 'OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED',
  inheritDecentraderAutoTrade: true,
  stateFileEnv: 'OPEN_LIQUIDITY_V2_ETH_TRADE_STATE_FILE',
  stateFileName: 'open-liquidity-v2-eth-trade-state.json',
  strategyPrefix: 'open_liquidity_v2_eth',
  edgeBufferEnv: 'DECENTRADER_TP1_EDGE_FRONT_RUN_USD',
  coinGlassMinUsdEnv: 'COINGLASS_WHALE_ETH_LEVEL_MIN_USD',
  coinGlassMaxDistanceEnv: 'COINGLASS_TP_CONFLUENCE_ETH_MAX_DISTANCE_USD',
  coinGlassMaxDistanceUsd: 15,
  coinGlass: coinGlassEthWhaleCollector
};

const INJ_MONITOR_CONFIG: OpenLiquidityV2TradeMonitorConfig = {
  market: 'INJ-USD',
  symbol: 'INJUSDT',
  asset: 'INJ',
  priceStep: 0.01,
  tradeCapable: true,
  enabledEnv: 'OPEN_LIQUIDITY_V2_INJ_INTRUSION_MONITOR_ENABLED',
  autoTradeEnv: 'OPEN_LIQUIDITY_V2_INJ_AUTO_TRADE_ENABLED',
  inheritDecentraderAutoTrade: false,
  stateFileEnv: 'OPEN_LIQUIDITY_V2_INJ_TRADE_STATE_FILE',
  stateFileName: 'open-liquidity-v2-inj-trade-state.json',
  strategyPrefix: 'open_liquidity_v2_inj',
  edgeBufferEnv: 'OPEN_LIQUIDITY_V2_INJ_TP1_EDGE_FRONT_RUN_USD',
  coinGlassMinUsdEnv: 'COINGLASS_WHALE_INJ_LEVEL_MIN_USD',
  coinGlassMaxDistanceEnv: 'COINGLASS_TP_CONFLUENCE_INJ_MAX_DISTANCE_USD',
  coinGlassMaxDistanceUsd: 0.05,
  coinGlass: coinGlassInjWhaleCollector
};

const GOLD_MONITOR_CONFIG: OpenLiquidityV2TradeMonitorConfig = {
  market: 'PAXG-USD',
  symbol: 'XAUUSDT',
  asset: 'GOLD',
  priceStep: 1,
  tradeCapable: true,
  enabledEnv: 'OPEN_LIQUIDITY_V2_GOLD_INTRUSION_MONITOR_ENABLED',
  autoTradeEnv: 'OPEN_LIQUIDITY_V2_GOLD_AUTO_TRADE_ENABLED',
  inheritDecentraderAutoTrade: false,
  stateFileEnv: 'OPEN_LIQUIDITY_V2_GOLD_TRADE_STATE_FILE',
  stateFileName: 'open-liquidity-v2-gold-intrusion-state.json',
  strategyPrefix: 'open_liquidity_v2_gold',
  edgeBufferEnv: 'DECENTRADER_TP1_EDGE_FRONT_RUN_USD',
  coinGlassMinUsdEnv: 'COINGLASS_WHALE_GOLD_LEVEL_MIN_USD',
  coinGlassMaxDistanceEnv: 'COINGLASS_TP_CONFLUENCE_GOLD_MAX_DISTANCE_USD',
  coinGlassMaxDistanceUsd: 25
};

const SILVER_MONITOR_CONFIG: OpenLiquidityV2TradeMonitorConfig = {
  market: 'XAG-USD',
  symbol: 'XAGUSDT',
  asset: 'SILVER',
  priceStep: 0.1,
  tradeCapable: true,
  enabledEnv: 'OPEN_LIQUIDITY_V2_SILVER_INTRUSION_MONITOR_ENABLED',
  autoTradeEnv: 'OPEN_LIQUIDITY_V2_SILVER_AUTO_TRADE_ENABLED',
  // Silver joins the live portfolio when the shared Decentrader switch is on,
  // while retaining a dedicated per-market kill switch.
  inheritDecentraderAutoTrade: true,
  stateFileEnv: 'OPEN_LIQUIDITY_V2_SILVER_TRADE_STATE_FILE',
  stateFileName: 'open-liquidity-v2-silver-intrusion-state.json',
  strategyPrefix: 'open_liquidity_v2_silver',
  edgeBufferEnv: 'OPEN_LIQUIDITY_V2_SILVER_TP1_EDGE_FRONT_RUN_USD',
  coinGlassMinUsdEnv: 'COINGLASS_WHALE_SILVER_LEVEL_MIN_USD',
  coinGlassMaxDistanceEnv: 'COINGLASS_TP_CONFLUENCE_SILVER_MAX_DISTANCE_USD',
  coinGlassMaxDistanceUsd: 0.5
};

type PendingEthAlert = {
  alert: GapAlert;
  firstObservedAt: string;
  normalSmtpSentAt?: string;
};

type EthDelayRecord = {
  signature: string;
  emailType: 'normal' | 'filtered';
  sideCounts: string;
  intrusionTimestamp: string;
  smtpSentAt: string;
  delayMinutes: number;
  completedCandles1h: number;
};

type EthBenchmarkRecord = {
  signature: string;
  timestamp: string;
  timestampNl: string;
  sideCounts: string;
  direction: TradePlanDirection | 'mixed';
  filtered: boolean;
  candleReview?: any;
  impulseQuality?: IntrusionImpulseQuality;
  coinGlass: any;
  tradeOutcome?: string;
  observedAt: string;
};

type EthMonitorState = AlertState & {
  pendingAlerts?: Record<string, PendingEthAlert>;
  normalSentSignatures?: string[];
  filteredSentSignatures?: string[];
  delayRecords?: EthDelayRecord[];
  benchmarkRecords?: EthBenchmarkRecord[];
};

function boolEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || String(raw).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function numberEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function positiveIntegerEnv(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.max(min, Math.min(max, Math.floor(parsed)))
    : fallback;
}

function fractionEnv(name: string, fallback: number): number {
  const value = numberEnv(name, fallback);
  return value > 1 ? value / 100 : value;
}

function finite(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundedToStep(value: number, step: number): number {
  const inverse = 1 / step;
  if (Number.isInteger(inverse)) return Math.round(value * inverse) / inverse;
  return Number((Math.round(value / step) * step).toFixed(8));
}

function normalizedMarket(value: unknown): string {
  return String(value || '').replace(/_/g, '-').toUpperCase();
}

function existingPosition(account: DydxSizingAccountSnapshot, market = MARKET): DydxOpenPosition | undefined {
  return account.openPositions?.find((position) =>
    normalizedMarket(position.market) === market && Math.abs(finite(position.size)) > 0
  );
}

function directionForPosition(position: DydxOpenPosition | undefined): TradePlanDirection | undefined {
  if (finite(position?.size) > 0) return 'long';
  if (finite(position?.size) < 0) return 'short';
  return undefined;
}

export function capTakeProfitsForStatefulOrderCapacity(
  takeProfits: any[],
  capacity: {
    limit: number;
    openOrders: number;
    marketOpenOrders: number;
    marketTakeProfitLimit?: number;
    marketReservedStopSlots?: number;
  }
): { takeProfits: any[]; requested: number; allowed: number; reservedStopSlots: number } {
  const requested = Array.isArray(takeProfits) ? takeProfits.length : 0;
  const otherMarketOrders = Math.max(0, capacity.openOrders - capacity.marketOpenOrders);
  const reservedStopSlots = Number.isFinite(capacity.marketReservedStopSlots)
    ? Math.max(0, Number(capacity.marketReservedStopSlots))
    : 1;
  const allowed = Number.isFinite(capacity.marketTakeProfitLimit)
    ? Math.max(0, Math.floor(Number(capacity.marketTakeProfitLimit)))
    : Math.max(0, capacity.limit - otherMarketOrders - reservedStopSlots);

  return {
    takeProfits: (takeProfits || []).slice(0, allowed),
    requested,
    allowed,
    reservedStopSlots
  };
}

function priceKey(price: number): string {
  return price.toFixed(8).replace(/0+$/, '').replace(/\.$/, '');
}

function sideCounts(alert: GapAlert): string {
  const parts: string[] = [];
  if (alert.left.length) parts.push(`${alert.left.length} left edge`);
  if (alert.right.length) parts.push(`${alert.right.length} right edge`);
  return parts.join(' + ') || `${alert.entrants.length} inside gap`;
}

function signatureForAlert(alert: GapAlert, market = MARKET): string {
  return `${market}|${alert.timestamp}|${alert.entrants
    .map((bar) => `${bar.key}:${bar.count}`)
    .sort()
    .join('|')}`;
}

function timestampMs(timestamp: string): number {
  return Date.parse(`${String(timestamp).replace(' ', 'T')}Z`);
}

function replicaRows(payload: any): DecentraderRow[] {
  return (payload?.frames || []).map((frame: any) => ({
    timestamp: frame.t,
    ohlc4: finite(frame.price),
    openRef: finite(frame.open) || finite(frame.price),
    closeRef: finite(frame.close) || finite(frame.price),
    highRef: finite(frame.high) || finite(frame.price),
    lowRef: finite(frame.low) || finite(frame.price)
  }));
}

function replicaBar(zone: CompactReplicaZone): LiquidityBar {
  return {
    key: `${zone[0]}|${zone[1]}|${priceKey(zone[2])}`,
    side: zone[0],
    leverage: zone[1],
    price: zone[2],
    count: zone[3]
  };
}

function normalizedGap(raw: any, referencePrice: number): Gap | undefined {
  const left = finite(raw?.left);
  const right = finite(raw?.right);
  if (!(left > 0 && right > left)) return undefined;
  const leftEdge: LiquidityBar = {
    key: `L|${finite(raw?.leftEdge?.leverage) || 10}|${priceKey(left)}`,
    side: 'L',
    leverage: finite(raw?.leftEdge?.leverage) || 10,
    price: left,
    count: finite(raw?.leftEdge?.positionCount ?? raw?.leftEdge?.relativeCount) || 1
  };
  const rightEdge: LiquidityBar = {
    key: `S|${finite(raw?.rightEdge?.leverage) || 10}|${priceKey(right)}`,
    side: 'S',
    leverage: finite(raw?.rightEdge?.leverage) || 10,
    price: right,
    count: finite(raw?.rightEdge?.positionCount ?? raw?.rightEdge?.relativeCount) || 1
  };
  return {
    left,
    right,
    width: right - left,
    price: referencePrice,
    leftEdge,
    rightEdge,
    leftToPrice: referencePrice - left,
    rightToPrice: right - referencePrice
  };
}

export function reconstructReplicaIntrusions(payload: any, afterTimestamp?: string): {
  alerts: GapAlert[];
  latestBars: LiquidityBar[];
} {
  const frames = Array.isArray(payload?.frames) ? payload.frames : [];
  const deltas: CompactReplicaZone[][] = Array.isArray(payload?.zoneDeltas) ? payload.zoneDeltas : [];
  const active = new Map<string, CompactReplicaZone>();
  for (const zone of (payload?.zoneSeed || []) as CompactReplicaZone[]) {
    active.set(`${zone[0]}|${zone[1]}|${priceKey(zone[2])}`, zone);
  }
  const afterIndex = afterTimestamp
    ? frames.findIndex((frame: any) => String(frame.t) === afterTimestamp)
    : frames.length - 1;
  const detectionStart = afterIndex >= 0 ? afterIndex + 1 : Math.max(1, frames.length - 1);
  const alerts: GapAlert[] = [];

  for (let frameIndex = 0; frameIndex < frames.length; frameIndex += 1) {
    const previousCounts = new Map(
      [...active.entries()].map(([key, zone]) => [key, zone[3]])
    );
    for (const zone of deltas[frameIndex] || []) {
      const key = `${zone[0]}|${zone[1]}|${priceKey(zone[2])}`;
      if (zone[3] > 0) active.set(key, zone);
      else active.delete(key);
    }
    if (frameIndex < detectionStart || frameIndex <= 0) continue;
    const frame = frames[frameIndex];
    const previousFrame = frames[frameIndex - 1];
    const previousGap = normalizedGap(payload?.gaps?.[frameIndex - 1], finite(previousFrame?.price));
    if (!previousGap) continue;
    const currentPrice = finite(frame?.price);
    const entrants = [...active.entries()]
      .filter(([key, zone]) => zone[3] > (previousCounts.get(key) || 0))
      .map(([, zone]) => replicaBar(zone))
      .filter((bar) => bar.price > previousGap.left && bar.price < previousGap.right)
      .map((bar) => ({
        ...bar,
        newCount: bar.count - (previousCounts.get(bar.key) || 0),
        gapSide: bar.price - previousGap.left <= previousGap.right - bar.price ? 'left' : 'right',
        sideOfPrice: bar.price < currentPrice ? 'left' : bar.price > currentPrice ? 'right' : 'price'
      } as LiquidityBar));
    if (!entrants.length) continue;
    alerts.push({
      frameIndex,
      timestamp: String(frame.t || ''),
      timestampNl: nlTime(frame.t),
      price: currentPrice,
      previousGap,
      entrants,
      left: entrants.filter((bar) => bar.gapSide === 'left'),
      right: entrants.filter((bar) => bar.gapSide === 'right')
    });
  }
  return { alerts, latestBars: [...active.values()].map(replicaBar) };
}

function clusterStep(bars: LiquidityBar[], price: number, minimumStep = PRICE_STEP): number {
  const prices = [price, ...bars.map((bar) => bar.price)];
  const span = Math.max(...prices) - Math.min(...prices);
  if (price < 100) return Math.max(minimumStep, span > 20 ? 0.1 : span > 5 ? 0.05 : minimumStep);
  if (span > 6_000) return 100;
  if (span > 3_000) return 50;
  if (span > 1_200) return 25;
  if (span > 600) return 10;
  return minimumStep;
}

function coinGlassConfluence(
  direction: TradePlanDirection,
  price: number,
  gap: Gap | undefined,
  options: {
    coinGlass?: WhaleSnapshotProvider;
    minUsdEnv?: string;
    maxDistanceEnv?: string;
    maxDistanceUsd?: number;
  } = {}
): any {
  if (!boolEnv('COINGLASS_TP_CONFLUENCE_ENABLED', true)) return undefined;
  const snapshot = options.coinGlass?.snapshot();
  if (!snapshot) return undefined;
  const minUsd = numberEnv(
    options.minUsdEnv || 'COINGLASS_TP_CONFLUENCE_MIN_USD',
    snapshot.minUsd || 10_000_000
  );
  const maxDistance = numberEnv(options.maxDistanceEnv || 'COINGLASS_TP_CONFLUENCE_ETH_MAX_DISTANCE_USD', options.maxDistanceUsd ?? 15);
  const longDurationHours = numberEnv('COINGLASS_TP_CONFLUENCE_LONG_DURATION_HOURS', 14 * 24);
  const expectedSide = direction === 'long' ? 'sell' : 'buy';
  let best: any;
  for (const level of snapshot.levels || []) {
    if (level.side !== expectedSide || finite(level.volumeUsd) < minUsd) continue;
    if (gap && level.price > gap.left && level.price < gap.right) continue;
    const distance = Math.abs(finite(level.price) - price);
    if (distance > maxDistance) continue;
    const startedAt = finite(level.startedAt);
    const startMs = startedAt > 0 ? (startedAt < 1e12 ? startedAt * 1000 : startedAt) : Date.now();
    const durationHours = Math.max(0, (Date.now() - startMs) / HOUR_MS);
    const volumeMultiplier = Math.min(0.3, Math.max(0, finite(level.volumeUsd) - minUsd) / 50_000_000);
    const durationMultiplier = durationHours >= longDurationHours ? 0.15 : 0;
    const candidate = {
      price: level.price,
      distance,
      volumeUsd: level.volumeUsd,
      durationHours,
      durationDays: durationHours / 24,
      side: level.side,
      multiplier: 1 + volumeMultiplier + durationMultiplier,
      longDuration: durationHours >= longDurationHours
    };
    if (!best || candidate.multiplier > best.multiplier || candidate.volumeUsd > best.volumeUsd) best = candidate;
  }
  return best;
}

export function buildReplicaTradeZones(
  bars: LiquidityBar[],
  price: number,
  gap: Gap | undefined,
  options: {
    priceStep?: number;
    edgeBufferEnv?: string;
    coinGlass?: WhaleSnapshotProvider;
    coinGlassMinUsdEnv?: string;
    coinGlassMaxDistanceEnv?: string;
    coinGlassMaxDistanceUsd?: number;
  } = {}
): { longTp: TradeZone[]; shortTp: TradeZone[] } {
  const priceStep = options.priceStep || PRICE_STEP;
  const step = clusterStep(bars, price, priceStep);
  const leverageWeight = new Map([[3, 1], [5, 1.35], [10, 1.7]]);
  const clusters = new Map<string, any>();
  for (const direction of ['long', 'short'] as TradePlanDirection[]) {
    for (const bar of bars) {
      const correct = direction === 'long'
        ? bar.side === 'S' && bar.price > price
        : bar.side === 'L' && bar.price < price;
      if (!correct) continue;
      const bucket = roundedToStep(bar.price, step);
      const key = `${direction}|${bucket}`;
      const cluster = clusters.get(key) || {
        direction, priceSum: 0, weightSum: 0, weighted: 0, count: 0, leverages: new Set<number>()
      };
      const weight = leverageWeight.get(bar.leverage) || 1;
      cluster.priceSum += bar.price * weight;
      cluster.weightSum += weight;
      cluster.weighted += bar.count * weight;
      cluster.count += bar.count;
      cluster.leverages.add(bar.leverage);
      clusters.set(key, cluster);
    }
  }
  const candidates = [...clusters.values()].map((cluster) => {
    const zonePrice = roundedToStep(cluster.priceSum / cluster.weightSum, priceStep);
    const distance = Math.abs(zonePrice - price);
    const distanceFactor = Math.max(0.35, 1 - Math.min(distance / Math.max(1, price * 0.22), 1) * 0.65);
    const overlapFactor = 1 + Math.max(0, cluster.leverages.size - 1) * 0.22;
    const score = Math.max(1, Math.round(cluster.weighted * overlapFactor * distanceFactor));
    return {
      direction: cluster.direction,
      rank: 0,
      price: zonePrice,
      count: cluster.count,
      score,
      distance,
      leverages: [...cluster.leverages].sort((a, b) => a - b),
      fresh: 0
    } as TradeZone;
  });
  const maxLevels = positiveIntegerEnv('DECENTRADER_TP_MAX_LEVELS', 6, 1, 6);
  const minimumSpacing = Math.max(step * 2, price * fractionEnv('DECENTRADER_TP_MIN_SPACING_PCT', 0.025));

  function edgeTp(direction: TradePlanDirection): TradeZone | undefined {
    if (!gap) return undefined;
    const edge = direction === 'long' ? gap.rightEdge : gap.leftEdge;
    const edgePrice = direction === 'long' ? gap.right : gap.left;
    const configuredBuffer = Number(process.env[options.edgeBufferEnv || 'DECENTRADER_TP1_EDGE_FRONT_RUN_USD']);
    const buffer = Number.isFinite(configuredBuffer) && configuredBuffer >= 0
      ? configuredBuffer
      : Math.max(priceStep, price * fractionEnv('DECENTRADER_TP1_EDGE_FRONT_RUN_PCT', 0.0005));
    const tpPrice = direction === 'long'
      ? roundedToStep(Math.floor((edgePrice - buffer) / priceStep) * priceStep, priceStep)
      : roundedToStep(Math.ceil((edgePrice + buffer) / priceStep) * priceStep, priceStep);
    if (direction === 'long' ? tpPrice <= price : tpPrice >= price) return undefined;
    return {
      direction,
      rank: 1,
      price: tpPrice,
      count: edge.count,
      score: Math.max(1, edge.count),
      selectionScore: Math.max(1, edge.count),
      peak: false,
      edge: true,
      edgePrice,
      frontRunBuffer: buffer,
      continuation: false,
      distance: Math.abs(tpPrice - price),
      leverages: [edge.leverage],
      fresh: 0
    };
  }

  function ranked(direction: TradePlanDirection): TradeZone[] {
    const ordered = candidates
      .filter((zone) => zone.direction === direction)
      .sort((a, b) => direction === 'long' ? a.price - b.price : b.price - a.price);
    const eligible = ordered.map((zone, index) => {
      const previous = ordered[index - 1];
      const next = ordered[index + 1];
      const peak = (!previous || zone.score >= previous.score) && (!next || zone.score >= next.score);
      const cgConfluence = coinGlassConfluence(direction, zone.price, gap, {
        coinGlass: options.coinGlass,
        minUsdEnv: options.coinGlassMinUsdEnv,
        maxDistanceEnv: options.coinGlassMaxDistanceEnv,
        maxDistanceUsd: options.coinGlassMaxDistanceUsd
      });
      const fibConfluence = gapFibonacciConfluenceForZone(direction, zone.price, gap, step);
      const overlapBoost = 1 + Math.max(0, zone.leverages.length - 1) * 0.08;
      const peakBoost = peak ? 1.18 : 1;
      const fibBoost = fibConfluence ? 1 + 0.12 * fibConfluence.proximity : 1;
      return {
        ...zone,
        peak,
        cgConfluence,
        fibConfluence,
        selectionScore: Math.round(zone.score * overlapBoost * peakBoost * fibBoost * (cgConfluence?.multiplier || 1))
      } as TradeZone;
    });
    const edge = edgeTp(direction);
    const selected: TradeZone[] = [];
    for (const zone of eligible.sort((a, b) =>
      finite(b.selectionScore) - finite(a.selectionScore) || a.distance - b.distance
    )) {
      if (edge && boolEnv('DECENTRADER_TP_BEYOND_EDGE_ONLY', true)) {
        if (direction === 'long' ? zone.price <= finite(edge.edgePrice) : zone.price >= finite(edge.edgePrice)) continue;
      }
      if (edge && Math.abs(zone.price - edge.price) < minimumSpacing) continue;
      if (selected.some((other) => Math.abs(other.price - zone.price) < minimumSpacing)) continue;
      selected.push({ ...zone, continuation: Boolean(edge) });
      if (selected.length >= maxLevels - (edge ? 1 : 0)) break;
    }
    const result = [...(edge ? [edge] : []), ...selected]
      .sort((a, b) => direction === 'long' ? a.price - b.price : b.price - a.price)
      .slice(0, maxLevels);
    return result.map((zone, index) => ({ ...zone, rank: index + 1 }));
  }
  return { longTp: ranked('long'), shortTp: ranked('short') };
}

function coinGlassBenchmark(alert: GapAlert, provider?: WhaleSnapshotProvider): any {
  const snapshot = provider?.snapshot();
  if (!snapshot) return { source: null, levelsInsideGap: 0, buyUsd: 0, sellUsd: 0, bias: 'UNAVAILABLE' };
  const inside = (snapshot.levels || []).filter((level) =>
    finite(level.volumeUsd) >= snapshot.minUsd &&
    finite(level.price) > alert.previousGap.left &&
    finite(level.price) < alert.previousGap.right
  );
  const buyUsd = inside.filter((level) => level.side === 'buy').reduce((sum, level) => sum + finite(level.volumeUsd), 0);
  const sellUsd = inside.filter((level) => level.side === 'sell').reduce((sum, level) => sum + finite(level.volumeUsd), 0);
  return {
    source: snapshot.symbol,
    minUsd: snapshot.minUsd,
    levelsInsideGap: inside.length,
    buyUsd,
    sellUsd,
    bias: buyUsd > sellUsd * 1.08 ? 'BUY_SUPPORT' : sellUsd > buyUsd * 1.08 ? 'SELL_FRICTION' : 'BALANCED',
    fetchedAt: snapshot.fetchedAt || null
  };
}

function rawAlertBody(alert: GapAlert, asset = 'ETH', symbol = 'ETHUSDT'): string {
  const bars = alert.entrants.map((bar) =>
    `- ${bar.side === 'L' ? 'Long' : 'Short'} ${bar.leverage}x $${bar.price.toLocaleString('en-US')} (${bar.gapSide} edge)`
  );
  return [
    `Public Perp V2 ${symbol} liquidity gap alert`,
    '',
    `Time: ${alert.timestampNl} (${alert.timestamp} UTC)`,
    `Price: $${alert.price.toLocaleString('en-US')}`,
    `Previous clean gap: $${alert.previousGap.left.toLocaleString('en-US')} -> $${alert.previousGap.right.toLocaleString('en-US')}`,
    `Gap width: $${alert.previousGap.width.toLocaleString('en-US')}`,
    '',
    `New or expanded histos inside previous gap: ${sideCounts(alert)}`,
    ...bars,
    '',
    `Source: Public Perp V2 ${asset} replica (Binance ${asset === 'GOLD' || asset === 'SILVER' ? 'Futures' : 'Spot'} causal liquidation cohorts).`
  ].join('\n');
}

function trimList(values: string[] | undefined, max = 500): string[] {
  return [...new Set(values || [])].slice(-max);
}

function intrusionHistoryMaxRecords(): number {
  return positiveIntegerEnv('OPEN_LIQUIDITY_V2_INTRUSION_HISTORY_MAX_RECORDS', 10_000, 500, 50_000);
}

function intrusionHistoryPayloadMaxRecords(): number {
  return positiveIntegerEnv('OPEN_LIQUIDITY_V2_INTRUSION_HISTORY_PAYLOAD_MAX_RECORDS', 1_000, 100, 5_000);
}

function stateFile(config: OpenLiquidityV2TradeMonitorConfig = ETH_MONITOR_CONFIG): string {
  const explicit = String(process.env[config.stateFileEnv] || '').trim();
  if (explicit) return explicit;
  const btcState = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim();
  if (btcState) return path.join(path.dirname(btcState), config.stateFileName);
  const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
  const base = fs.existsSync(renderDisk) ? renderDisk : path.join(process.cwd(), 'data');
  return path.join(base, config.stateFileName);
}

function readState(config: OpenLiquidityV2TradeMonitorConfig = ETH_MONITOR_CONFIG): EthMonitorState {
  try {
    return JSON.parse(fs.readFileSync(stateFile(config), 'utf8'));
  } catch {
    return {};
  }
}

function writeState(state: EthMonitorState, config: OpenLiquidityV2TradeMonitorConfig = ETH_MONITOR_CONFIG): void {
  const file = stateFile(config);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(state));
  fs.renameSync(temporary, file);
}

export class OpenLiquidityV2EthTradeMonitor {
  private interval: NodeJS.Timeout | undefined;
  private initialTimer: NodeJS.Timeout | undefined;
  private running = false;
  private executor: DecentraderTradeExecutor | undefined;
  private lastResult: any;
  private lastStartedAt: string | undefined;
  private lastFinishedAt: string | undefined;

  constructor(
    private readonly collector: OpenLiquidityV2ReplicaCollector,
    private readonly config: OpenLiquidityV2TradeMonitorConfig = ETH_MONITOR_CONFIG
  ) {}

  configureTradeExecutor(executor: DecentraderTradeExecutor): void {
    this.executor = executor;
  }

  private enabled(): boolean {
    return boolEnv(this.config.enabledEnv, true);
  }

  private autoTradeEnabled(): boolean {
    if (!this.config.tradeCapable) return false;
    return boolEnv(
      this.config.autoTradeEnv,
      this.config.inheritDecentraderAutoTrade
        ? boolEnv('DECENTRADER_AUTO_TRADE_ENABLED', false)
        : false
    );
  }

  private pollMinutes(): number {
    return positiveIntegerEnv('DECENTRADER_GAP_POLL_MINUTES', 10, 1, 1_440);
  }

  start(initialDelayMs = 75_000): void {
    if (!this.enabled() || this.interval || this.initialTimer) return;
    const begin = () => {
      this.initialTimer = undefined;
      this.check().catch((error) => console.error(`${this.config.asset} V2 intrusion monitor initial check failed:`, error));
      this.interval = setInterval(() => {
        this.check().catch((error) => console.error(`${this.config.asset} V2 intrusion monitor check failed:`, error));
      }, this.pollMinutes() * 60_000);
    };
    this.initialTimer = setTimeout(begin, initialDelayMs);
    console.log(`${this.config.asset} Public Perp V2 intrusion/trade monitor scheduled:`, {
      market: this.config.market,
      pollMinutes: this.pollMinutes(),
      autoTradeEnabled: this.autoTradeEnabled(),
      observeOnly: !this.config.tradeCapable,
      stateFile: stateFile(this.config),
      inheritsDecentraderRiskAndOrderEnvs: true
    });
  }

  getStatus(): any {
    const state = readState(this.config);
    return {
      enabled: this.enabled(),
      running: this.running,
      market: this.config.market,
      autoTradeEnabled: this.autoTradeEnabled(),
      observeOnly: !this.config.tradeCapable,
      hasTradeExecutor: Boolean(this.executor),
      pollMinutes: this.pollMinutes(),
      stateFile: stateFile(this.config),
      lastStartedAt: this.lastStartedAt,
      lastFinishedAt: this.lastFinishedAt,
      lastResult: this.lastResult,
      lastTradeDecision: state.lastTradeDecision,
      managedPosition: state.managedPosition || null,
      pendingAlerts: Object.keys(state.pendingAlerts || {}).length,
      intrusionCandleFilter: {
        enabled: boolEnv('DECENTRADER_INTRUSION_CANDLE_FILTER_ENABLED', false),
        regularEmailEnabled: decentraderRegularIntrusionEmailEnabled(),
        source: 'binance-futures',
        symbol: this.config.symbol,
        volumeDeltaEnabled: boolEnv('DECENTRADER_INTRUSION_VOLUME_DELTA_ENABLED', true),
        rule: 'all fully closed 1H Delay candles: price color and taker delta must match direction'
      }
    };
  }

  snapshot(): any {
    const state = readState(this.config);
    const payloadMaxRecords = intrusionHistoryPayloadMaxRecords();
    const storedDelayRecords = state.delayRecords || [];
    const storedBenchmarkRecords = state.benchmarkRecords || [];
    const records = storedDelayRecords.slice(-payloadMaxRecords).reverse();
    const completed = records.map((record) => record.completedCandles1h);
    const average = completed.length ? completed.reduce((sum, value) => sum + value, 0) / completed.length : 0;
    return {
      status: this.getStatus(),
      delayHistory: {
        totalRecords: storedDelayRecords.length,
        records,
        recent: records.slice(0, 12),
        stats: {
          all: {
            count: records.length,
            minCompletedCandles1h: completed.length ? Math.min(...completed) : null,
            averageCompletedCandles1h: completed.length ? average : null,
            maxCompletedCandles1h: completed.length ? Math.max(...completed) : null
          }
        }
      },
      intrusionBenchmarks: {
        totalRecords: storedBenchmarkRecords.length,
        history: storedBenchmarkRecords.slice(-payloadMaxRecords).reverse(),
        recent: storedBenchmarkRecords.slice(-12).reverse()
      },
      tradePlan: this.lastResult?.tradePlan || null
    };
  }

  private addDelayRecord(state: EthMonitorState, alert: GapAlert, signature: string, emailType: 'normal' | 'filtered', sentAt: string): void {
    const delayMinutes = Math.max(0, (Date.parse(sentAt) - timestampMs(alert.timestamp)) / 60_000);
    const record: EthDelayRecord = {
      signature,
      emailType,
      sideCounts: sideCounts(alert),
      intrusionTimestamp: alert.timestamp,
      smtpSentAt: sentAt,
      delayMinutes,
      completedCandles1h: Math.floor(delayMinutes / 60 + 1e-9)
    };
    state.delayRecords = [...(state.delayRecords || []), record].slice(-intrusionHistoryMaxRecords());
  }

  private addBenchmark(state: EthMonitorState, alert: GapAlert, signature: string, details: Partial<EthBenchmarkRecord>): void {
    const existing = (state.benchmarkRecords || []).findIndex((record) => record.signature === signature);
    const existingRecord = existing >= 0 ? (state.benchmarkRecords || [])[existing] : undefined;
    const base: EthBenchmarkRecord = {
      signature,
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      sideCounts: sideCounts(alert),
      direction: mapDirectionFromAlert(alert) || 'mixed',
      filtered: false,
      coinGlass: existingRecord?.coinGlass || coinGlassBenchmark(alert, this.config.coinGlass),
      observedAt: nowNlIso(),
      ...details
    };
    const records = [...(state.benchmarkRecords || [])];
    if (existing >= 0) records[existing] = { ...records[existing], ...base };
    else records.push(base);
    state.benchmarkRecords = records.slice(-intrusionHistoryMaxRecords());
  }

  private impulseQuality(
    alert: GapAlert,
    review: any,
    coinGlass: any
  ): IntrusionImpulseQuality {
    const collector = decentralizedDomCollectorForMarket(this.config.market);
    const from = new Date(timestampMs(alert.timestamp)).toISOString();
    const to = String(review?.delayCutoffAt || nowNlIso());
    const domRecords = collector
      ? collector.getHistory({ from, to, maxPoints: 5_000 }).records
      : [];
    return evaluateIntrusionImpulseQuality({
      direction: mapDirectionFromAlert(alert),
      alertTimestamp: alert.timestamp,
      gapWidth: alert.previousGap?.width,
      review,
      domRecords,
      coinGlass,
      evaluatedAt: to
    });
  }

  async getTradePlan(account: DydxSizingAccountSnapshot, signalAlert?: GapAlert): Promise<any> {
    const payload = await this.collector.getPayload();
    const rows = replicaRows(payload);
    const frameIndex = rows.length - 1;
    const frame = payload.frames?.[frameIndex];
    if (!frame || frameIndex < 0) throw new Error(`No ${this.config.asset} V2 replica frame available for trade planning.`);
    const reconstructed = reconstructReplicaIntrusions(payload, frame.t);
    const bars = reconstructed.latestBars;
    const gap = normalizedGap(payload.gaps?.[frameIndex], finite(frame.price));
    const zones = buildReplicaTradeZones(bars, finite(frame.price), gap, {
      priceStep: this.config.priceStep,
      edgeBufferEnv: this.config.edgeBufferEnv,
      coinGlass: this.config.coinGlass,
      coinGlassMinUsdEnv: this.config.coinGlassMinUsdEnv,
      coinGlassMaxDistanceEnv: this.config.coinGlassMaxDistanceEnv,
      coinGlassMaxDistanceUsd: this.config.coinGlassMaxDistanceUsd
    });
    const marketInfo = account.markets?.[this.config.market] || account.markets?.[this.config.market.replace('-', '_')];
    if (!marketInfo) throw new Error(`No dYdX market info available for ${this.config.market}.`);
    const mode = String(process.env.DECENTRADER_TRADE_SIZING_MODE || 'growth').trim().toLowerCase();
    const fractalRows = dydxHourlyCandlesToFractalRows(
      await fetchDydxHourlyCandlesForMarket(this.config.market)
    );
    const fractalFrameIndex = fractalRows.length - 1;
    if (fractalFrameIndex < 4) {
      throw new Error(`Not enough closed dYdX 1H candles available for ${this.config.market} fractal stops.`);
    }
    const longPlan = buildDirectionalPlan('long', account, marketInfo, fractalRows, fractalFrameIndex, gap, signalAlert, zones, finite(frame.price), mode);
    const shortPlan = buildDirectionalPlan('short', account, marketInfo, fractalRows, fractalFrameIndex, gap, signalAlert, zones, finite(frame.price), mode);
    const activeDirection = mapDirectionFromAlert(signalAlert);
    return {
      ok: true,
      symbol: this.config.symbol,
      market: this.config.market,
      fractalCandleSource: 'dydx-1h',
      timestamp: frame.t,
      timestampNl: nlTime(frame.t),
      price: finite(frame.price),
      signal: {
        direction: activeDirection || 'none',
        reason: activeDirection ? `${activeDirection} bias from ${this.config.asset} V2 gap intrusion` : `No fresh ${this.config.asset} intrusion`
      },
      account: {
        equity: account.equity,
        freeCollateral: account.freeCollateral,
        openPositionsCount: account.openPositionsCount,
        updatedAt: account.updatedAt
      },
      gap: gap || null,
      marketInfo,
      activePlan: activeDirection === 'long' ? longPlan : activeDirection === 'short' ? shortPlan : null,
      plans: { long: longPlan, short: shortPlan },
      note: `${this.config.asset} V2 planning uses the shared Decentrader risk, fractal SL, managed TP and dYdX execution rules.`
    };
  }

  private async applyStatefulOrderCapacity(orderAlert: AlertObject, result: any): Promise<void> {
    if (!this.executor?.getStatefulOrderCapacity) return;

    const capacity = await this.executor.getStatefulOrderCapacity(this.config.market);
    const requestedLevels = Array.isArray((orderAlert as any).take_profits)
      ? (orderAlert as any).take_profits
      : [];
    const capped = capTakeProfitsForStatefulOrderCapacity(requestedLevels, capacity);
    const otherMarketOrders = Math.max(0, capacity.openOrders - capacity.marketOpenOrders);
    const slotsForMarket = Number.isFinite(capacity.marketOrderLimit)
      ? Math.max(0, Number(capacity.marketOrderLimit))
      : Math.max(0, capacity.limit - otherMarketOrders);

    if (slotsForMarket < capped.reservedStopSlots) {
      throw new Error(
        `No dYdX stateful-order slot is available for the required ${this.config.market} protective stop. ` +
        `Open=${capacity.openOrders}/${capacity.limit}, existing ${this.config.market} orders=${capacity.marketOpenOrders}.`
      );
    }

    (orderAlert as any).take_profits = capped.takeProfits;
    result.statefulOrderCapacity = {
      ...capacity,
      requestedTakeProfits: capped.requested,
      allowedTakeProfits: capped.takeProfits.length,
      reservedStopSlots: capped.reservedStopSlots
    };

    if (capped.takeProfits.length < capped.requested) {
      console.warn(`${this.config.asset} V2 TP ladder reduced to fit the dYdX stateful-order limit:`, {
        market: this.config.market,
        ...result.statefulOrderCapacity
      });
    }
  }

  private registerManagedPosition(
    state: EthMonitorState,
    signature: string,
    direction: TradePlanDirection,
    position: DydxOpenPosition,
    oraclePrice: number,
    plan: any,
    orderAlert: AlertObject,
    outcome: 'PLACED' | 'ADOPTED' | 'RECOVERED_AFTER_PARTIAL_EXECUTION'
  ): void {
    const stop = plan.activePlan?.stop;
    const registeredAt = nowNlIso();
    const takeProfits = Array.isArray((orderAlert as any).take_profits)
      ? (orderAlert as any).take_profits.map((level: any) => ({ ...level }))
      : [];

    state.lastTradeExecutedSignature = signature;
    state.lastTradeExecutedAt = registeredAt;
    state.managedPosition = {
      market: this.config.market,
      direction,
      openedAt: registeredAt,
      entrySignature: signature,
      initialSize: Math.abs(finite(position.size)),
      entryPrice: finite(position.entryPrice) || oraclePrice,
      currentStop: finite(stop?.price),
      currentStopUpdatedAt: registeredAt,
      currentStopFractalIndex: stop?.fractal?.index,
      currentStopFractalTimestamp: stop?.fractal?.timestamp,
      currentStopFractalPrice: stop?.fractal?.price,
      currentStopFractalSource: stop?.fractal?.source,
      currentStopFractalCandleSource: 'dydx-1h',
      takeProfits
    };
    stabilizeManagedTakeProfits(
      state.managedPosition,
      takeProfits,
      Math.abs(finite(position.size)),
      registeredAt,
      [],
      0,
      { currentPrice: finite(position.entryPrice) || oraclePrice }
    );
    state.lastTradeDecision = {
      at: registeredAt,
      outcome,
      market: this.config.market,
      direction,
      signature,
      size: Math.abs(finite(position.size)),
      stop: finite(stop?.price),
      takeProfits
    };
  }

  private async executeAlert(state: EthMonitorState, alert: GapAlert, signature: string, result: any): Promise<void> {
    if (!this.autoTradeEnabled()) {
      result.tradeSkipped = `${this.config.asset} V2 auto-trading is disabled.`;
      return;
    }
    if (!this.executor) {
      result.tradeSkipped = `No dYdX executor is configured for ${this.config.asset} V2.`;
      return;
    }
    if (state.lastTradeExecutedSignature === signature) {
      result.tradeSkipped = `Duplicate ${this.config.asset} V2 trade signature.`;
      return;
    }
    const direction = mapDirectionFromAlert(alert);
    if (!direction) {
      result.tradeSkipped = `Mixed ${this.config.asset} V2 intrusion has no trade direction.`;
      return;
    }
    const delayHours = (Date.now() - timestampMs(alert.timestamp)) / HOUR_MS;
    const maxDelayHours = numberEnv('DECENTRADER_INTRUSION_MAX_EXECUTION_DELAY_HOURS', 8);
    if (delayHours > maxDelayHours) {
      result.tradeSkipped = `${this.config.asset} V2 Delay ${delayHours.toFixed(2)}h exceeds ${maxDelayHours}h.`;
      return;
    }
    const account = await this.executor.getAccountSnapshot([this.config.market]);
    const openPosition = existingPosition(account, this.config.market);
    if (openPosition && directionForPosition(openPosition) === direction) {
      if (state.managedPosition) {
        result.tradeSkipped = `Existing ${this.config.market} ${direction} position detected; new intrusion skipped.`;
        return;
      }

      const recoveryPlan = await this.getTradePlan(account, alert);
      const recoveryAlert = buildDecentraderOrderAlert(recoveryPlan, signature);
      (recoveryAlert as any).strategy = this.config.strategyPrefix;
      await this.applyStatefulOrderCapacity(recoveryAlert, result);
      const recoveryOraclePrice = finite(recoveryPlan.marketInfo?.oraclePrice) || finite(recoveryPlan.price);
      this.registerManagedPosition(
        state,
        signature,
        direction,
        openPosition,
        recoveryOraclePrice,
        recoveryPlan,
        recoveryAlert,
        'ADOPTED'
      );
      result.tradePlaced = true;
      result.tradeRecovered = true;
      result.tradePlan = recoveryPlan;
      result.tradeDecision = state.lastTradeDecision;
      await this.syncManagedOrders(state, result);
      console.warn(`${this.config.asset} V2 adopted an existing matching position after an interrupted entry flow:`, state.lastTradeDecision);
      return;
    }
    const plan = await this.getTradePlan(account, alert);
    const marketStatus = String(plan.marketInfo?.status || '').trim().toUpperCase();
    if (marketStatus && marketStatus !== 'ACTIVE') {
      result.tradeSkipped = `${this.config.market} is ${marketStatus}; no live order was submitted.`;
      return;
    }
    const oraclePrice = finite(plan.marketInfo?.oraclePrice) || finite(plan.price);
    const drift = Math.abs(oraclePrice - alert.price) / Math.max(1, alert.price);
    const maxDrift = fractionEnv('DECENTRADER_INTRUSION_MAX_PRICE_DRIFT_PCT', 0.04);
    if (drift > maxDrift) {
      result.tradeSkipped = `${this.config.asset} price drift ${(drift * 100).toFixed(2)}% exceeds ${(maxDrift * 100).toFixed(2)}%.`;
      return;
    }
    const orderAlert = buildDecentraderOrderAlert(plan, signature);
    (orderAlert as any).strategy = this.config.strategyPrefix;
    await this.applyStatefulOrderCapacity(orderAlert, result);
    try {
      await this.executor.placeOrder(orderAlert);
    } catch (error) {
      const partialSnapshot = await this.executor.getAccountSnapshot([this.config.market]);
      const partialPosition = existingPosition(partialSnapshot, this.config.market);
      if (!partialPosition || directionForPosition(partialPosition) !== direction) throw error;

      this.registerManagedPosition(
        state,
        signature,
        direction,
        partialPosition,
        oraclePrice,
        plan,
        orderAlert,
        'RECOVERED_AFTER_PARTIAL_EXECUTION'
      );
      result.tradePlaced = true;
      result.tradeRecovered = true;
      result.tradeWarning = error instanceof Error ? error.message : String(error);
      result.tradePlan = plan;
      result.tradeDecision = state.lastTradeDecision;
      await this.syncManagedOrders(state, result);
      console.warn(`${this.config.asset} V2 recovered a position after partial dYdX order execution:`, {
        decision: state.lastTradeDecision,
        warning: result.tradeWarning
      });
      return;
    }
    const after = await this.executor.getAccountSnapshot([this.config.market]);
    const placedPosition = existingPosition(after, this.config.market);
    if (!placedPosition || directionForPosition(placedPosition) !== direction) {
      throw new Error(`dYdX did not report the requested ${this.config.market} ${direction} position after execution.`);
    }
    this.registerManagedPosition(state, signature, direction, placedPosition, oraclePrice, plan, orderAlert, 'PLACED');
    result.tradePlaced = true;
    result.tradePlan = plan;
    result.tradeDecision = state.lastTradeDecision;
    console.log(`${this.config.asset} V2 filtered intrusion trade placed:`, state.lastTradeDecision);
  }

  private async recoverUnmanagedFilteredPosition(state: EthMonitorState, payload: any, result: any): Promise<void> {
    if (!this.executor || !this.autoTradeEnabled() || state.managedPosition) return;

    const account = await this.executor.getAccountSnapshot([this.config.market]);
    const position = existingPosition(account, this.config.market);
    if (!position) return;

    const positionDirection = directionForPosition(position);
    const filteredSignatures = new Set(state.filteredSentSignatures || []);
    if (!filteredSignatures.size) return;

    const firstTimestamp = String(payload?.frames?.[0]?.t || '');
    const historicalAlerts = reconstructReplicaIntrusions(payload, firstTimestamp).alerts;
    const candidate = historicalAlerts
      .slice()
      .reverse()
      .find((alert) => {
        const signature = signatureForAlert(alert, this.config.market);
        return filteredSignatures.has(signature) && mapDirectionFromAlert(alert) === positionDirection;
      });
    if (!candidate) return;

    const signature = signatureForAlert(candidate, this.config.market);
    const maxDelayHours = numberEnv('DECENTRADER_INTRUSION_MAX_EXECUTION_DELAY_HOURS', 8);
    const ageHours = (Date.now() - timestampMs(candidate.timestamp)) / HOUR_MS;
    if (ageHours > maxDelayHours) return;

    await this.executeAlert(state, candidate, signature, result);
    if (result.tradeRecovered) {
      console.warn(`${this.config.asset} V2 recovered persisted filtered execution state:`, {
        market: this.config.market,
        signature,
        direction: positionDirection,
        positionSize: position.size,
        ageHours
      });
    }
  }

  private async syncManagedOrders(state: EthMonitorState, result: any): Promise<void> {
    const executor = this.executor;
    const managed = state.managedPosition;
    if (!executor || !managed || !this.autoTradeEnabled()) return;
    const account = await executor.getAccountSnapshot([this.config.market]);
    const position = existingPosition(account, this.config.market);
    if (!position) {
      await executor.placeOrder(buildDecentraderFlatCleanupAlert(this.config.market, managed));
      delete state.managedPosition;
      result.managedOrderSync = { outcome: 'FLAT_CLEANED_UP', market: this.config.market };
      return;
    }
    const direction = directionForPosition(position);
    const grew = Math.abs(position.size) > managed.initialSize + 1e-9;
    const entryMismatch = Boolean(
      managed.entryPrice && position.entryPrice &&
      Math.abs(position.entryPrice - managed.entryPrice) / managed.entryPrice > 0.005
    );
    if (direction !== managed.direction || grew || entryMismatch) {
      result.managedOrderSync = { outcome: 'SKIPPED', reason: `${this.config.asset} position no longer matches the monitor-owned entry.` };
      return;
    }
    const plan = await this.getTradePlan(account);
    result.tradePlan = plan;
    if (boolEnv('DECENTRADER_DYNAMIC_TP_ENABLED', true) && executor.syncTakeProfits) {
      const tpAlert = buildDecentraderDynamicTpAlert(plan, position);
      (tpAlert as any).strategy = `${this.config.strategyPrefix}_dynamic_tps`;
      await this.applyStatefulOrderCapacity(tpAlert, result);
      const directionalPlan = plan?.plans?.[direction];
      const minimumOrderSize =
        finite(directionalPlan?.sizing?.minimumOrderSize) ||
        finite(plan?.marketInfo?.stepSize);
      const stabilized = stabilizeManagedTakeProfits(
        managed,
        (tpAlert as any).take_profits || [],
        Math.abs(finite(position.size)),
        nowNlIso(),
        Array.isArray(state.lastTradeDecision?.takeProfits)
          ? state.lastTradeDecision.takeProfits
          : [],
        minimumOrderSize,
        { currentPrice: finite(plan.marketInfo?.oraclePrice) || finite(plan.price) }
      );
      (tpAlert as any).take_profits = stabilized.takeProfits;
      if (stabilized.takeProfits.length) {
        result.dynamicTpSync = await executor.syncTakeProfits(tpAlert);
        result.dynamicTpSync = {
          ...result.dynamicTpSync,
          tp1Lifecycle: stabilized.lifecycle || null,
          tpRatchetLifecycle: stabilized.ratchetLifecycle,
          tp1ConsumedNow: stabilized.consumedNow,
          takeProfits: stabilized.takeProfits
        };
      }
    }
    if (!boolEnv('DECENTRADER_DYNAMIC_SL_ENABLED', true) || !executor.syncTrailingStop || !managed.currentStop) return;
    const currentPrice = finite(plan.marketInfo?.oraclePrice) || finite(plan.price);
    const breached = direction === 'long'
      ? currentPrice <= finite(managed.currentStop)
      : currentPrice >= finite(managed.currentStop);
    if (breached) {
      await executor.placeOrder(buildDecentraderStopBreachFlatAlert(this.config.market, currentPrice, position, managed.currentStop));
      const after = await executor.getAccountSnapshot([this.config.market]);
      if (!existingPosition(after, this.config.market)) delete state.managedPosition;
      result.dynamicSlSync = { outcome: 'FLATTENED_AFTER_STOP_BREACH', currentPrice, stop: managed.currentStop };
      return;
    }
    const rows = dydxHourlyCandlesToFractalRows(
      await fetchDydxHourlyCandlesForMarket(this.config.market)
    );
    const fractalDelay = Math.max(0, Math.min(10, Math.floor(numberEnv(
      'DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY',
      numberEnv('DECENTRADER_TRAIL_FRACTAL_DELAY', 0)
    ))));
    // Existing V2 positions may still carry a pre-dYdX or OHLC4 pivot anchor.
    const needsDydxFractalRebase =
      managed.currentStopFractalCandleSource !== 'dydx-1h' ||
      managed.currentStopFractalSource === 'ohlc4';
    const candidate = buildFractalStop(
      rows,
      rows.length - 1,
      direction,
      currentPrice,
      needsDydxFractalRebase
        ? {
            fractalDelay,
            enforceMinDistance: false,
            missingReason: `Waiting for a confirmed dYdX fractal before rebasing the legacy ${this.config.asset} trailing stop.`
          }
        : {
            afterFractalIndex: managed.currentStopFractalIndex,
            afterFractalTimestamp: managed.currentStopFractalTimestamp,
            fractalDelay,
            enforceMinDistance: false,
            missingReason: `Waiting for ${fractalDelay + 1} confirmed newer ${this.config.asset} fractal(s).`
          }
    );
    const candidateStop = finite(candidate.price);
    const correctSide = candidateStop > 0 && (direction === 'long' ? candidateStop < currentPrice : candidateStop > currentPrice);
    const improves = needsDydxFractalRebase || (direction === 'long'
      ? candidateStop > managed.currentStop
      : candidateStop < managed.currentStop);
    if (!candidate.valid || !correctSide || !improves) {
      const coverageSyncEnabled = boolEnv('DECENTRADER_DYNAMIC_SL_COVERAGE_SYNC_ENABLED', true);
      if (!coverageSyncEnabled) {
        result.dynamicSlSync = {
          outcome: 'UNCHANGED',
          currentStop: managed.currentStop,
          candidateStop: candidateStop || null,
          needsDydxFractalRebase,
          fractalDelay,
          coverageSyncEnabled: false
        };
        return;
      }

      const coverageAlert = buildDecentraderDynamicSlAlert(plan, position, managed.currentStop);
      (coverageAlert as any).strategy = `${this.config.strategyPrefix}_dynamic_sl_coverage`;
      const coverageSync = await executor.syncTrailingStop(coverageAlert);
      result.dynamicSlSync = {
        ...coverageSync,
        currentStop: managed.currentStop,
        candidateStop: candidateStop || null,
        needsDydxFractalRebase,
        fractalDelay,
        coverageSyncEnabled: true
      };
      if (coverageSync?.outcome === 'UPDATED') managed.currentStopUpdatedAt = nowNlIso();
      return;
    }
    if (!boolEnv('DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED', false)) {
      result.dynamicSlSync = {
        outcome: 'READY',
        currentStop: managed.currentStop,
        candidateStop,
        needsDydxFractalRebase
      };
      return;
    }
    const slAlert = buildDecentraderDynamicSlAlert(plan, position, candidateStop, candidate);
    (slAlert as any).strategy = `${this.config.strategyPrefix}_dynamic_sl`;
    const sync = await executor.syncTrailingStop(slAlert);
    result.dynamicSlSync = { ...sync, needsDydxFractalRebase };
    if (sync?.outcome === 'UPDATED' || sync?.outcome === 'UNCHANGED') {
      managed.currentStop = candidateStop;
      managed.currentStopUpdatedAt = nowNlIso();
      managed.currentStopFractalIndex = candidate.fractal?.index;
      managed.currentStopFractalTimestamp = candidate.fractal?.timestamp;
      managed.currentStopFractalPrice = candidate.fractal?.price;
      managed.currentStopFractalSource = candidate.fractal?.source;
      managed.currentStopFractalCandleSource = 'dydx-1h';
    }
  }

  async check(): Promise<any> {
    if (this.running) return this.lastResult;
    this.running = true;
    this.lastStartedAt = nowNlIso();
    const state = readState(this.config);
    const result: any = {
      ok: true,
      market: this.config.market,
      alerts: [],
      emailSentCount: 0,
      tradePlaced: false
    };
    try {
      const payload = await this.collector.getPayload();
      const frames = payload.frames || [];
      const latestTimestamp = String(frames[frames.length - 1]?.t || '');
      if (!state.lastDataTimestamp) {
        state.lastDataTimestamp = String(frames[Math.max(0, frames.length - 2)]?.t || latestTimestamp);
        result.initializedAtLatestFrame = latestTimestamp;
      }
      {
        const reconstructed = reconstructReplicaIntrusions(payload, state.lastDataTimestamp);
        result.alerts = reconstructed.alerts;
        const smtp = smtpSettingsFromEnv();
        const regularIntrusionEmailEnabled = decentraderRegularIntrusionEmailEnabled();
        state.pendingAlerts = state.pendingAlerts || {};
        const normalSent = new Set(state.normalSentSignatures || []);
        const filteredSent = new Set(state.filteredSentSignatures || []);

        for (const alert of reconstructed.alerts) {
          const signature = signatureForAlert(alert, this.config.market);
          if (!state.pendingAlerts[signature]) {
            state.pendingAlerts[signature] = { alert, firstObservedAt: nowNlIso() };
            this.addBenchmark(state, alert, signature, {});
          }
        }

        let intrusionCandles: any[] | undefined;
        const filterEnabled = boolEnv('DECENTRADER_INTRUSION_CANDLE_FILTER_ENABLED', false);
        for (const [signature, pending] of Object.entries(state.pendingAlerts)) {
          const currentFrameIndex = frames.findIndex((frame: any) => String(frame.t) === pending.alert.timestamp);
          const alert = currentFrameIndex >= 0
            ? { ...pending.alert, frameIndex: currentFrameIndex }
            : pending.alert;
          pending.alert = alert;
          if (!normalSent.has(signature) && regularIntrusionEmailEnabled && smtp) {
            const sent = await sendEmailBestEffort(
              smtp,
              `${this.config.asset} ${sideCounts(alert)} | ${alert.timestampNl}`,
              rawAlertBody(alert, this.config.asset, this.config.symbol)
            );
            if (sent.sent) {
              pending.normalSmtpSentAt = nowNlIso();
              normalSent.add(signature);
              this.addDelayRecord(state, alert, signature, 'normal', pending.normalSmtpSentAt);
              result.emailSentCount += 1;
            }
          }
          if (filterEnabled && !regularIntrusionEmailEnabled && !pending.normalSmtpSentAt) {
            // Keep a stable Delay cutoff while suppressing the raw intrusion
            // email. Only a passing FILTERED alert is delivered.
            pending.normalSmtpSentAt = nowNlIso();
            console.log(`${this.config.asset} V2 regular intrusion email suppressed; Delay cutoff fixed internally:`, {
              signature,
              timestamp: alert.timestamp,
              timestampNl: alert.timestampNl,
              delayCutoffAt: pending.normalSmtpSentAt
            });
          }
          if (!filterEnabled) {
            await this.executeAlert(state, alert, signature, result);
            delete state.pendingAlerts[signature];
            this.addBenchmark(state, alert, signature, { tradeOutcome: result.tradePlaced ? 'PLACED' : result.tradeSkipped || 'SKIPPED' });
            continue;
          }
          if (!pending.normalSmtpSentAt) continue;
          if (!intrusionCandles) {
            intrusionCandles = await fetchBinanceFuturesHourlyCandlesForSymbol(
              this.config.symbol,
              String(frames[0]?.t || alert.timestamp)
            );
          }
          const review = intrusionCandleReview(
            replicaRows(payload),
            alert,
            true,
            intrusionCandles,
            pending.normalSmtpSentAt,
            boolEnv('DECENTRADER_INTRUSION_VOLUME_DELTA_ENABLED', true)
          );
          const storedBenchmark = (state.benchmarkRecords || []).find((record) => record.signature === signature);
          const causalCoinGlass = storedBenchmark?.coinGlass || coinGlassBenchmark(alert, this.config.coinGlass);
          const impulseQuality = this.impulseQuality(alert, review, causalCoinGlass);
          this.addBenchmark(state, alert, signature, {
            filtered: review.status === 'PASS',
            candleReview: review,
            coinGlass: causalCoinGlass,
            impulseQuality
          });
          if (review.status === 'PENDING') continue;
          if (review.status === 'PASS') {
            if (!filteredSent.has(signature) && smtp) {
              const sent = await sendEmailBestEffort(
                smtp,
                `FILTERED ${this.config.asset} ${sideCounts(alert)} | ${impulseQuality.label} | ${alert.timestampNl}`,
                filteredAlertBody(alert, this.config.symbol, review).replace(
                  /^FILTERED Decentrader/,
                  `FILTERED ${this.config.asset} Public Perp V2`
                )
              );
              if (sent.sent) {
                const sentAt = nowNlIso();
                filteredSent.add(signature);
                this.addDelayRecord(state, alert, signature, 'filtered', sentAt);
                result.emailSentCount += 1;
              }
            }
            await this.executeAlert(state, alert, signature, result);
            this.addBenchmark(state, alert, signature, {
              filtered: true,
              candleReview: review,
              coinGlass: causalCoinGlass,
              impulseQuality,
              tradeOutcome: result.tradePlaced ? 'PLACED' : result.tradeSkipped || 'SKIPPED'
            });
          }
          delete state.pendingAlerts[signature];
        }
        state.normalSentSignatures = trimList([...normalSent]);
        state.filteredSentSignatures = trimList([...filteredSent]);
        state.lastDataTimestamp = latestTimestamp;
      }
      if (!state.managedPosition && !result.tradePlaced) {
        await this.recoverUnmanagedFilteredPosition(state, payload, result);
      }
      // The managed entry flow already submits its initial SL and TPs. Waiting
      // until the next poll avoids resubmitting while the indexer catches up.
      if (!result.tradePlaced) {
        await this.syncManagedOrders(state, result);
      }
      this.lastResult = result;
      return result;
    } catch (error) {
      result.ok = false;
      result.error = error instanceof Error ? error.message : String(error);
      this.lastResult = result;
      console.error(`${this.config.asset} Public Perp V2 intrusion monitor failed:`, error);
      return result;
    } finally {
      state.lastCheckedAt = nowNlIso();
      writeState(state, this.config);
      this.lastFinishedAt = nowNlIso();
      this.running = false;
    }
  }
}

export const openLiquidityV2EthTradeMonitor = new OpenLiquidityV2EthTradeMonitor(
  openLiquidityV2EthCollector,
  ETH_MONITOR_CONFIG
);

export const openLiquidityV2InjTradeMonitor = new OpenLiquidityV2EthTradeMonitor(
  openLiquidityV2InjCollector,
  INJ_MONITOR_CONFIG
);

export const openLiquidityV2GoldIntrusionMonitor = new OpenLiquidityV2EthTradeMonitor(
  openLiquidityV2GoldCollector,
  GOLD_MONITOR_CONFIG
);
export const openLiquidityV2SilverIntrusionMonitor = new OpenLiquidityV2EthTradeMonitor(
  openLiquidityV2SilverCollector,
  SILVER_MONITOR_CONFIG
);
