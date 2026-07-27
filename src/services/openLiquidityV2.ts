import fs from 'fs';
import path from 'path';

import axios from 'axios';

const DYDX_INDEXER_URL = 'https://indexer.dydx.trade/v4';
const DEFAULT_GMX_GRAPHQL_URL = 'https://gmx.squids.live/gmx-synthetics-arbitrum:prod/api/graphql';
const GMX_BTC_MARKETS = [
  '0x47c031236e19d024b42f8AE6780E44A573170703',
  '0x7C11F78Ce78768518D743E81Fdfa2F860C6b9A77'
];
const MARKET = 'BTC-USD' as const;
const MODEL_VERSION = 'gmx-position-density-v2.1';
const FRAME_LIMIT = 500;

type PositionSide = 'L' | 'S';
type ObservationKind = 'historical-backfill' | 'live-observation';

type DydxCandle = {
  startedAt: string;
  close: string;
  low?: string;
  high?: string;
};

export type GmxV2PositionInput = {
  positionKey: string;
  market: string;
  collateralToken: string;
  isLong: boolean;
  sizeInUsd: string;
  entryPrice: string;
  leverage: string;
  openedAt: number;
  snapshotTimestamp?: number | null;
  unrealizedPnl?: string;
  unrealizedFees?: string;
  unrealizedPriceImpact?: string;
};

export type LiquidationEstimate = {
  side: PositionSide;
  leverage: number;
  leverageBucket: 3 | 5 | 10;
  liquidationPrice: number;
  uncertaintyLow: number;
  uncertaintyHigh: number;
  sizeUsd: number;
  confidence: number;
};

export type V2LiquidityZone = {
  side: PositionSide;
  leverage: 3 | 5 | 10;
  price: number;
  positionCount: number;
  notionalUsd: number;
  weightedUsd: number;
  confidence: number;
  uncertaintyUsd: number;
  sourceCount: number;
};

export type V2Gap = {
  left: number;
  right: number;
  width: number;
  leftEdge: V2LiquidityZone;
  rightEdge: V2LiquidityZone;
  interiorWeightedUsd: number;
  cleanliness: number;
  confidence: number;
  sourceAgreement: number;
  status: 'candidate' | 'confirmed';
  method: 'contiguous-low-density';
};

export type OpenLiquidityV2Observation = {
  version: 2;
  modelVersion: string;
  source: 'gmx';
  kind: ObservationKind;
  effectiveAt: string;
  observedAt: string;
  receivedAt: string;
  referencePrice: number;
  positionCount: number;
  acceptedPositionCount: number;
  totalNotionalUsd: number;
  zones: V2LiquidityZone[];
};

type V2Status = {
  enabled: boolean;
  running: boolean;
  readOnly: true;
  market: typeof MARKET;
  modelVersion: string;
  pollMinutes: number;
  historyDirectory: string;
  observations: number;
  historicalObservations: number;
  liveObservations: number;
  coverage?: { from?: string; to?: string };
  bootstrap: {
    running: boolean;
    requestedDays: number;
    completedDays: number;
    lastError?: string;
  };
  source: {
    name: 'GMX';
    decentralizedProtocol: true;
    publicIndexer: true;
    requiresApiKey: false;
    lastSuccessAt?: string;
    lastErrorAt?: string;
    lastError?: string;
  };
};

function finiteNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function positiveNumber(value: unknown): number {
  return Math.max(0, finiteNumber(value));
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function rounded(value: number, decimals = 4): number {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function boundedInteger(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed)
    ? Math.max(min, Math.min(max, Math.floor(parsed)))
    : fallback;
}

function envEnabled(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return String(raw).trim().toLowerCase() === 'true';
}

function gmxGraphqlUrl(): string {
  return String(process.env.OPEN_LIQUIDITY_V2_GMX_GRAPHQL_URL || DEFAULT_GMX_GRAPHQL_URL).trim();
}

function compareObservations(
  a: OpenLiquidityV2Observation,
  b: OpenLiquidityV2Observation
): number {
  const timeDifference = Date.parse(a.effectiveAt) - Date.parse(b.effectiveAt);
  if (timeDifference !== 0) return timeDifference;
  if (a.kind === b.kind) return Date.parse(a.observedAt) - Date.parse(b.observedAt);
  return a.kind === 'historical-backfill' ? -1 : 1;
}

function timestampForMs(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

function priceStep(referencePrice: number): number {
  const configured = Number(process.env.OPEN_LIQUIDITY_V2_PRICE_STEP_USD);
  if (Number.isFinite(configured) && configured >= 10 && configured <= 1000) return configured;
  if (referencePrice >= 80_000) return 100;
  if (referencePrice >= 20_000) return 50;
  return 25;
}

function leverageBucket(leverage: number): 3 | 5 | 10 {
  if (leverage <= 4) return 3;
  if (leverage <= 7.5) return 5;
  return 10;
}

function collateralConfidence(collateralToken: string): number {
  const normalized = String(collateralToken || '').toLowerCase();
  // USDC and USDC.e on Arbitrum. Token-collateral positions have additional
  // price covariance, so they deliberately receive a wider uncertainty band.
  if (
    normalized === '0xaf88d065e77c8cc2239327c5edb3a432268e5831' ||
    normalized === '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
  ) {
    return 0.78;
  }
  return 0.64;
}

export function calculateGmxLiquidationEstimate(
  position: GmxV2PositionInput,
  referencePrice: number
): LiquidationEstimate | undefined {
  const sizeUsd = positiveNumber(position.sizeInUsd) / 1e30;
  const leverage = positiveNumber(position.leverage) / 10_000;
  if (sizeUsd <= 0 || leverage <= 1 || !Number.isFinite(referencePrice) || referencePrice <= 0) {
    return undefined;
  }

  // GMX reports current leverage after PnL and pending fees. Converting that
  // remaining equity into a price distance is materially better than applying
  // leverage to the entry price. Exact Reader/SDK output remains the target;
  // this estimate carries an explicit uncertainty band until then.
  const maintenanceAndCloseReserve = 0.0075;
  const distancePct = clamp((1 / leverage) - maintenanceAndCloseReserve, 0.0025, 0.48);
  const liquidationPrice = position.isLong
    ? referencePrice * (1 - distancePct)
    : referencePrice * (1 + distancePct);
  if (
    !Number.isFinite(liquidationPrice) ||
    liquidationPrice < referencePrice * 0.4 ||
    liquidationPrice > referencePrice * 1.8
  ) {
    return undefined;
  }

  const confidence = collateralConfidence(position.collateralToken);
  const uncertaintyPct = confidence < 0.7 ? 0.012 : 0.0075;

  return {
    side: position.isLong ? 'L' : 'S',
    leverage: rounded(leverage, 3),
    leverageBucket: leverageBucket(leverage),
    liquidationPrice: rounded(liquidationPrice, 2),
    uncertaintyLow: rounded(liquidationPrice * (1 - uncertaintyPct), 2),
    uncertaintyHigh: rounded(liquidationPrice * (1 + uncertaintyPct), 2),
    sizeUsd: rounded(sizeUsd, 2),
    confidence
  };
}

export function aggregateGmxPositionZones(
  positions: GmxV2PositionInput[],
  referencePrice: number,
  step = priceStep(referencePrice)
): { zones: V2LiquidityZone[]; accepted: number; totalNotionalUsd: number } {
  const bins = new Map<string, {
    side: PositionSide;
    leverage: 3 | 5 | 10;
    price: number;
    positionCount: number;
    notionalUsd: number;
    weightedUsd: number;
    confidenceWeight: number;
    uncertaintyWeight: number;
  }>();
  let accepted = 0;
  let totalNotionalUsd = 0;

  for (const position of positions) {
    const estimate = calculateGmxLiquidationEstimate(position, referencePrice);
    if (!estimate) continue;
    accepted += 1;
    totalNotionalUsd += estimate.sizeUsd;
    const bucketPrice = Math.round(estimate.liquidationPrice / step) * step;
    const key = `${estimate.side}|${estimate.leverageBucket}|${bucketPrice}`;
    const bin = bins.get(key) || {
      side: estimate.side,
      leverage: estimate.leverageBucket,
      price: bucketPrice,
      positionCount: 0,
      notionalUsd: 0,
      weightedUsd: 0,
      confidenceWeight: 0,
      uncertaintyWeight: 0
    };
    const weightedUsd = estimate.sizeUsd * estimate.confidence;
    bin.positionCount += 1;
    bin.notionalUsd += estimate.sizeUsd;
    bin.weightedUsd += weightedUsd;
    bin.confidenceWeight += estimate.confidence * estimate.sizeUsd;
    bin.uncertaintyWeight +=
      ((estimate.uncertaintyHigh - estimate.uncertaintyLow) / 2) * estimate.sizeUsd;
    bins.set(key, bin);
  }

  return {
    accepted,
    totalNotionalUsd: rounded(totalNotionalUsd, 2),
    zones: [...bins.values()]
      .map((bin) => ({
        side: bin.side,
        leverage: bin.leverage,
        price: bin.price,
        positionCount: bin.positionCount,
        notionalUsd: rounded(bin.notionalUsd, 2),
        weightedUsd: rounded(bin.weightedUsd, 2),
        confidence: rounded(bin.notionalUsd > 0 ? bin.confidenceWeight / bin.notionalUsd : 0, 3),
        uncertaintyUsd: rounded(bin.notionalUsd > 0 ? bin.uncertaintyWeight / bin.notionalUsd : 0, 2),
        sourceCount: 1
      }))
      .sort((a, b) => a.price - b.price || a.side.localeCompare(b.side) || a.leverage - b.leverage)
  };
}

function percentile(values: number[], fraction: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * fraction)));
  return sorted[index];
}

function collapseZonesByPrice(zones: V2LiquidityZone[]): V2LiquidityZone[] {
  const bins = new Map<string, V2LiquidityZone>();
  for (const zone of zones) {
    const key = `${zone.side}|${zone.price}`;
    const existing = bins.get(key);
    if (!existing) {
      bins.set(key, { ...zone });
      continue;
    }
    const dominantLeverage = zone.weightedUsd > existing.weightedUsd
      ? zone.leverage
      : existing.leverage;
    const totalNotional = existing.notionalUsd + zone.notionalUsd;
    existing.confidence = totalNotional > 0
      ? (
          existing.confidence * existing.notionalUsd +
          zone.confidence * zone.notionalUsd
        ) / totalNotional
      : 0;
    existing.positionCount += zone.positionCount;
    existing.notionalUsd = totalNotional;
    existing.weightedUsd += zone.weightedUsd;
    existing.uncertaintyUsd = Math.max(existing.uncertaintyUsd, zone.uncertaintyUsd);
    existing.sourceCount = Math.max(existing.sourceCount, zone.sourceCount);
    existing.leverage = dominantLeverage;
  }
  return [...bins.values()];
}

export function detectV2LiquidityGap(
  zones: V2LiquidityZone[],
  currentPrice: number,
  options: { minClusterUsd?: number; sourceAgreement?: number } = {}
): V2Gap | undefined {
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) return undefined;
  const collapsed = collapseZonesByPrice(zones);
  const longBelow = collapsed.filter((zone) => zone.side === 'L' && zone.price < currentPrice);
  const shortAbove = collapsed.filter((zone) => zone.side === 'S' && zone.price > currentPrice);
  if (!longBelow.length || !shortAbove.length) return undefined;

  const nonZero = collapsed.map((zone) => zone.weightedUsd).filter((value) => value > 0);
  const configuredFloor = Number(options.minClusterUsd ?? process.env.OPEN_LIQUIDITY_V2_MIN_CLUSTER_USD);
  const minimumClusterUsd = Number.isFinite(configuredFloor) && configuredFloor > 0
    ? configuredFloor
    : 250_000;
  const strongThreshold = Math.max(minimumClusterUsd, percentile(nonZero, 0.6));
  const leftCandidates = longBelow.filter((zone) => zone.weightedUsd >= strongThreshold);
  const rightCandidates = shortAbove.filter((zone) => zone.weightedUsd >= strongThreshold);
  if (!leftCandidates.length || !rightCandidates.length) return undefined;

  const leftEdge = leftCandidates.sort((a, b) => b.price - a.price)[0];
  const rightEdge = rightCandidates.sort((a, b) => a.price - b.price)[0];
  if (!leftEdge || !rightEdge || leftEdge.price >= rightEdge.price) return undefined;

  const interior = collapsed.filter((zone) => zone.price > leftEdge.price && zone.price < rightEdge.price);
  const interiorWeightedUsd = interior.reduce((sum, zone) => sum + zone.weightedUsd, 0);
  const interiorPeak = interior.reduce((peak, zone) => Math.max(peak, zone.weightedUsd), 0);
  const edgeFloor = Math.max(1, Math.min(leftEdge.weightedUsd, rightEdge.weightedUsd));
  const cleanliness = clamp(1 - interiorPeak / edgeFloor, 0, 1);
  const sourceAgreement = Math.max(1, Math.floor(options.sourceAgreement || 1));
  const sourceFactor = clamp(sourceAgreement / 2, 0.5, 1);
  const confidence = rounded(
    clamp((cleanliness * 0.65 + Math.min(leftEdge.confidence, rightEdge.confidence) * 0.35) * sourceFactor, 0, 1),
    3
  );

  return {
    left: leftEdge.price,
    right: rightEdge.price,
    width: rightEdge.price - leftEdge.price,
    leftEdge,
    rightEdge,
    interiorWeightedUsd: rounded(interiorWeightedUsd, 2),
    cleanliness: rounded(cleanliness, 3),
    confidence,
    sourceAgreement,
    status: sourceAgreement >= 2 && cleanliness >= 0.65 ? 'confirmed' : 'candidate',
    method: 'contiguous-low-density'
  };
}

async function fetchDydxCandles(limit = FRAME_LIMIT): Promise<DydxCandle[]> {
  const response = await axios.get(`${DYDX_INDEXER_URL}/candles/perpetualMarkets/${MARKET}`, {
    timeout: 30_000,
    params: { resolution: '1HOUR', limit }
  });
  const candles = response.data?.candles;
  if (!Array.isArray(candles)) throw new Error('dYdX candle response did not contain candles.');
  return candles
    .slice()
    .sort((a: DydxCandle, b: DydxCandle) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
}

async function fetchGmxPositions(input: {
  market: string;
  snapshotTimestamp?: number;
}): Promise<GmxV2PositionInput[]> {
  const rows: GmxV2PositionInput[] = [];
  const pageSize = 1_000;
  const historical = Number.isFinite(input.snapshotTimestamp);
  for (let offset = 0; offset < 10_000; offset += pageSize) {
    const query = historical
      ? `
        query GmxV2Positions($market: String!, $snapshotTimestamp: Int!, $offset: Int!, $limit: Int!) {
          positions(
            limit: $limit
            offset: $offset
            where: {
              market_eq: $market
              isSnapshot_eq: true
              snapshotTimestamp_eq: $snapshotTimestamp
            }
            orderBy: id_ASC
          ) {
            positionKey market collateralToken isLong sizeInUsd entryPrice leverage openedAt
            snapshotTimestamp unrealizedPnl unrealizedFees unrealizedPriceImpact
          }
        }
      `
      : `
        query GmxV2Positions($market: String!, $offset: Int!, $limit: Int!) {
          positions(
            limit: $limit
            offset: $offset
            where: { market_eq: $market, isSnapshot_eq: false }
            orderBy: id_ASC
          ) {
            positionKey market collateralToken isLong sizeInUsd entryPrice leverage openedAt
            snapshotTimestamp unrealizedPnl unrealizedFees unrealizedPriceImpact
          }
        }
      `;
    const response = await axios.post(
      gmxGraphqlUrl(),
      {
        query,
        variables: {
          market: input.market,
          snapshotTimestamp: input.snapshotTimestamp,
          offset,
          limit: pageSize
        }
      },
      {
        timeout: 30_000,
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' }
      }
    );
    if (Array.isArray(response.data?.errors) && response.data.errors.length) {
      throw new Error(response.data.errors.map((error: any) => error?.message).filter(Boolean).join('; '));
    }
    const page = response.data?.data?.positions;
    if (!Array.isArray(page)) throw new Error('GMX position response did not contain positions.');
    rows.push(...page);
    if (page.length < pageSize) break;
  }
  return rows;
}

function candlePriceAt(candles: DydxCandle[], timestampMs: number): number {
  let result = positiveNumber(candles[0]?.close);
  for (const candle of candles) {
    const candleMs = Date.parse(candle.startedAt);
    if (candleMs > timestampMs) break;
    const close = positiveNumber(candle.close);
    if (close > 0) result = close;
  }
  return result;
}

function buildObservation(input: {
  positions: GmxV2PositionInput[];
  effectiveAtMs: number;
  observedAtMs: number;
  referencePrice: number;
  kind: ObservationKind;
}): OpenLiquidityV2Observation {
  const aggregated = aggregateGmxPositionZones(input.positions, input.referencePrice);
  return {
    version: 2,
    modelVersion: MODEL_VERSION,
    source: 'gmx',
    kind: input.kind,
    effectiveAt: new Date(input.effectiveAtMs).toISOString(),
    observedAt: new Date(input.observedAtMs).toISOString(),
    receivedAt: new Date().toISOString(),
    referencePrice: rounded(input.referencePrice, 2),
    positionCount: input.positions.length,
    acceptedPositionCount: aggregated.accepted,
    totalNotionalUsd: aggregated.totalNotionalUsd,
    zones: aggregated.zones
  };
}

export class OpenLiquidityV2Collector {
  private interval: NodeJS.Timeout | undefined;
  private refreshPromise: Promise<void> | undefined;
  private bootstrapPromise: Promise<void> | undefined;
  private observations: OpenLiquidityV2Observation[] = [];
  private loaded = false;
  private bootstrapCompletedDays = 0;
  private bootstrapError: string | undefined;
  private lastSuccessAt: string | undefined;
  private lastErrorAt: string | undefined;
  private lastError: string | undefined;
  private payloadCache: { at: number; payload: any } | undefined;

  private enabled(): boolean {
    return envEnabled('OPEN_LIQUIDITY_V2_ENABLED', true);
  }

  private pollMinutes(): number {
    return boundedInteger(process.env.OPEN_LIQUIDITY_V2_POLL_MINUTES, 60, 10, 1_440);
  }

  private backfillDays(): number {
    return boundedInteger(process.env.OPEN_LIQUIDITY_V2_BACKFILL_DAYS, 21, 3, 120);
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
    return path.join(this.historyDirectory(), 'gmx-position-density.ndjson');
  }

  start(): void {
    if (!this.enabled() || this.interval) return;
    this.loadHistory();
    this.refresh().catch((error) => console.error('Initial Open Liquidity V2 refresh failed:', error));
    this.interval = setInterval(() => {
      this.refresh().catch((error) => console.error('Open Liquidity V2 refresh failed:', error));
    }, this.pollMinutes() * 60_000);
    console.log('Open Liquidity V2 collector started:', {
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
      const byKey = new Map<string, OpenLiquidityV2Observation>();
      for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean)) {
        const record = JSON.parse(line) as OpenLiquidityV2Observation;
        if (record?.version !== 2 || record?.modelVersion !== MODEL_VERSION || !record?.effectiveAt) continue;
        byKey.set(`${record.kind}|${record.effectiveAt}`, record);
      }
      this.observations = [...byKey.values()].sort(compareObservations);
    } catch (error) {
      console.warn('Open Liquidity V2 history could not be read:', {
        file,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private storeObservation(record: OpenLiquidityV2Observation): boolean {
    const key = `${record.kind}|${record.effectiveAt}`;
    if (this.observations.some((candidate) => `${candidate.kind}|${candidate.effectiveAt}` === key)) {
      return false;
    }
    fs.mkdirSync(this.historyDirectory(), { recursive: true });
    fs.appendFileSync(this.historyFile(), `${JSON.stringify(record)}\n`);
    this.observations.push(record);
    this.observations.sort(compareObservations);
    this.payloadCache = undefined;
    return true;
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
      const candles = await fetchDydxCandles();
      const positionsByMarket = await Promise.all(
        GMX_BTC_MARKETS.map((market) => fetchGmxPositions({ market }))
      );
      const now = Date.now();
      const effectiveAtMs = Math.floor(now / 3_600_000) * 3_600_000;
      const referencePrice = candlePriceAt(candles, effectiveAtMs);
      const record = buildObservation({
        positions: positionsByMarket.reduce(
          (all, marketPositions) => all.concat(marketPositions),
          [] as GmxV2PositionInput[]
        ),
        effectiveAtMs,
        observedAtMs: now,
        referencePrice,
        kind: 'live-observation'
      });
      this.storeObservation(record);
      this.lastSuccessAt = new Date().toISOString();
      this.lastError = undefined;
      if (!this.bootstrapPromise) {
        this.bootstrapPromise = this.bootstrapHistory(candles).finally(() => {
          this.bootstrapPromise = undefined;
        });
      }
    } catch (error) {
      this.lastErrorAt = new Date().toISOString();
      this.lastError = error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  private async bootstrapHistory(candles: DydxCandle[]): Promise<void> {
    const latestCandleMs = Date.parse(candles[candles.length - 1]?.startedAt || '');
    if (!Number.isFinite(latestCandleMs)) return;
    const latestDayMs = Math.floor(latestCandleMs / 86_400_000) * 86_400_000;
    const existing = new Set(
      this.observations
        .filter((record) => record.kind === 'historical-backfill')
        .map((record) => record.effectiveAt)
    );
    this.bootstrapCompletedDays = existing.size;
    this.bootstrapError = undefined;

    for (let age = this.backfillDays() - 1; age >= 0; age -= 1) {
      const effectiveAtMs = latestDayMs - age * 86_400_000;
      const effectiveAt = new Date(effectiveAtMs).toISOString();
      if (existing.has(effectiveAt)) continue;
      try {
        const positionsByMarket = await Promise.all(
          GMX_BTC_MARKETS.map((market) =>
            fetchGmxPositions({ market, snapshotTimestamp: Math.floor(effectiveAtMs / 1000) })
          )
        );
        const positions = positionsByMarket.reduce(
          (all, marketPositions) => all.concat(marketPositions),
          [] as GmxV2PositionInput[]
        );
        if (!positions.length) continue;
        const referencePrice = candlePriceAt(candles, effectiveAtMs);
        this.storeObservation(buildObservation({
          positions,
          effectiveAtMs,
          observedAtMs: Date.now(),
          referencePrice,
          kind: 'historical-backfill'
        }));
        this.bootstrapCompletedDays += 1;
      } catch (error) {
        this.bootstrapError = error instanceof Error ? error.message : String(error);
        console.warn('Open Liquidity V2 historical day could not be backfilled:', {
          effectiveAt,
          error: this.bootstrapError
        });
      }
    }
  }

  getStatus(): V2Status {
    this.loadHistory();
    const historical = this.observations.filter((record) => record.kind === 'historical-backfill');
    const live = this.observations.filter((record) => record.kind === 'live-observation');
    return {
      enabled: this.enabled(),
      running: Boolean(this.interval),
      readOnly: true,
      market: MARKET,
      modelVersion: MODEL_VERSION,
      pollMinutes: this.pollMinutes(),
      historyDirectory: this.historyDirectory(),
      observations: this.observations.length,
      historicalObservations: historical.length,
      liveObservations: live.length,
      coverage: this.observations.length
        ? {
            from: this.observations[0].effectiveAt,
            to: this.observations[this.observations.length - 1].effectiveAt
          }
        : undefined,
      bootstrap: {
        running: Boolean(this.bootstrapPromise),
        requestedDays: this.backfillDays(),
        completedDays: this.bootstrapCompletedDays,
        lastError: this.bootstrapError
      },
      source: {
        name: 'GMX',
        decentralizedProtocol: true,
        publicIndexer: true,
        requiresApiKey: false,
        lastSuccessAt: this.lastSuccessAt,
        lastErrorAt: this.lastErrorAt,
        lastError: this.lastError
      }
    };
  }

  async getPayload(): Promise<any> {
    this.loadHistory();
    if (!this.observations.length) await this.refresh();
    if (this.payloadCache && Date.now() - this.payloadCache.at < 60_000) {
      return this.payloadCache.payload;
    }

    const candles = await fetchDydxCandles();
    const frames = candles.map((candle, index) => ({
      i: index,
      t: timestampForMs(Date.parse(candle.startedAt)),
      startedAtMs: Date.parse(candle.startedAt),
      price: positiveNumber(candle.close),
      low: positiveNumber(candle.low) || positiveNumber(candle.close),
      high: positiveNumber(candle.high) || positiveNumber(candle.close)
    })).filter((frame) => frame.price > 0 && Number.isFinite(frame.startedAtMs));
    const usableObservations = this.observations
      .filter((record) => record.zones.length > 0)
      .sort(compareObservations);
    const snapshots = usableObservations.map((record, index) => ({
      i: index,
      effectiveAt: record.effectiveAt,
      observedAt: record.observedAt,
      receivedAt: record.receivedAt,
      kind: record.kind,
      source: record.source,
      referencePrice: record.referencePrice,
      positionCount: record.positionCount,
      acceptedPositionCount: record.acceptedPositionCount,
      totalNotionalUsd: record.totalNotionalUsd,
      zones: record.zones
    }));
    const gaps: Array<V2Gap | null> = [];
    let snapshotIndex = -1;
    const responseFrames = frames.map((frame) => {
      while (
        snapshotIndex + 1 < snapshots.length &&
        Date.parse(snapshots[snapshotIndex + 1].effectiveAt) <= frame.startedAtMs
      ) {
        snapshotIndex += 1;
      }
      const snapshot = snapshotIndex >= 0 ? snapshots[snapshotIndex] : undefined;
      gaps.push(snapshot ? detectV2LiquidityGap(snapshot.zones, frame.price) || null : null);
      return {
        i: frame.i,
        t: frame.t,
        price: frame.price,
        snapshot: snapshotIndex
      };
    });
    const prices = snapshots.flatMap((snapshot) => snapshot.zones.map((zone) => zone.price));
    prices.push(...frames.map((frame) => frame.price));
    const latestSnapshot = snapshots[snapshots.length - 1];
    const eventCount = snapshots.reduce((sum, snapshot) => sum + snapshot.zones.length, 0);
    const payload = {
      version: 2,
      modelVersion: MODEL_VERSION,
      snapshotZones: true,
      weightUnit: 'confidence-weighted USD',
      eventCount,
      source: {
        name: 'Public perp V2',
        market: MARKET,
        url: 'https://docs.gmx.io/docs/api/graphql/',
        api: 'GMX public protocol indexer snapshots + dYdX public candles',
        method:
          'Protocol-position density. GMX current leverage is converted into a liquidation-price estimate, aggregated in USD price bins and stored as immutable observations. Historical frames use only the protocol snapshot effective at that frame; no future candle sweep or survivor model is applied.',
        params: [
          `model=${MODEL_VERSION}`,
          `frames=${responseFrames.length}`,
          `snapshots=${snapshots.length}`,
          `zones=${eventCount}`,
          `positions=${latestSnapshot?.positionCount || 0}`,
          `accepted=${latestSnapshot?.acceptedPositionCount || 0}`,
          'sourceAgreement=1/2'
        ],
        sourceStatuses: [
          {
            label: 'GMX protocol positions',
            ok: Boolean(latestSnapshot),
            exactInventory: true,
            liquidationPrecision: 'model-estimated-with-uncertainty',
            decentralizedProtocol: true,
            requiresApiKey: false
          },
          {
            label: 'Drift protocol positions',
            ok: false,
            exactInventory: false,
            status: 'adapter reserved; direct Solana UserMap collection is the next source'
          }
        ],
        note:
          'Observe-only V2. The inventory is based on real GMX positions and persistent protocol snapshots, but liquidation prices remain model estimates until the GMX Reader/SDK parameters are wired in. A gap stays a candidate while only one independent position source agrees. It never places, sizes or manages dYdX orders.'
      },
      quality: {
        causalModel: true,
        persistentObservations: true,
        usesFuturePriceData: false,
        exactPositionInventory: true,
        exactLiquidationPrices: false,
        sourceAgreement: 1,
        requiredSourceAgreement: 2,
        gapMethod: 'contiguous-low-density'
      },
      status: this.getStatus(),
      range: {
        minPrice: Math.min(...prices),
        maxPrice: Math.max(...prices)
      },
      frames: responseFrames,
      snapshots,
      gaps,
      events: [],
      contextEvents: [],
      topCurrentZones: latestSnapshot
        ? latestSnapshot.zones
            .slice()
            .sort((a: V2LiquidityZone, b: V2LiquidityZone) => b.weightedUsd - a.weightedUsd)
            .slice(0, 40)
        : []
    };
    this.payloadCache = { at: Date.now(), payload };
    return payload;
  }
}

export const openLiquidityV2Collector = new OpenLiquidityV2Collector();
