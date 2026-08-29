import { binanceGet } from './binanceHttp';

type Direction = 'long' | 'short';
type HurdleSide = 'ask' | 'bid';
type HurdleSource = 'binance' | 'coinglass' | 'combined';

type DepthPayload = {
  lastUpdateId?: number;
  bids?: Array<[string, string]>;
  asks?: Array<[string, string]>;
};

type HurdleCandidate = {
  price: number;
  distancePct: number;
  side: HurdleSide;
  source: HurdleSource;
  binanceUsd: number;
  coinGlassUsd: number;
  effectiveUsd: number;
  persistenceHours?: number;
};

export type IntrusionForwardHurdle = {
  version: 1;
  observeOnly: true;
  causal: true;
  source: 'binance-futures-depth+coinglass' | 'binance-futures-depth' | 'coinglass' | 'none';
  symbol: string;
  direction: Direction;
  observedAt: string;
  dataCutoffAt?: string;
  referencePrice: number;
  referenceSource: 'binance-depth-mid' | 'delay-close';
  horizonPct: number;
  binPct: number;
  closeDistancePct: number;
  status: 'CLOSE' | 'AHEAD' | 'CLEAR' | 'DATA_GAP';
  headline: string;
  firstHurdle?: HurdleCandidate;
  binance?: {
    snapshotAt: string;
    lastUpdateId?: number;
    levelsConsidered: number;
    binCount: number;
    medianBinUsd: number;
    materialThresholdUsd: number;
    nearestMaterial?: HurdleCandidate;
  };
  coinGlass?: {
    fetchedAt?: string;
    ageMinutes?: number;
    maxAgeMinutes: number;
    stale: boolean;
    freshnessError?: string;
    levelsConsidered: number;
    nearest?: HurdleCandidate;
  };
  directionalDelayFlowUsd: number;
  flowToHurdleRatio?: number;
  caveat: string;
  error?: string;
};

type CacheEntry = { fetchedAt: number; payload: DepthPayload };
const depthCache = new Map<string, CacheEntry>();
const depthInFlight = new Map<string, Promise<DepthPayload>>();

function numberEnv(name: string, fallback: number, min: number, max: number): number {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? Math.max(min, Math.min(max, value)) : fallback;
}

function boolEnv(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined || String(value).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function finite(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function percentile(values: number[], fraction: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * fraction)))];
}

function compactUsd(value: number): string {
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (absolute >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (absolute >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value.toFixed(0)}`;
}

function priceLabel(value: number): string {
  const decimals = value >= 1_000 ? 0 : value >= 100 ? 1 : value >= 1 ? 2 : 4;
  return value.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: decimals });
}

function depthReferencePrice(payload: DepthPayload, fallback: number): {
  price: number;
  source: IntrusionForwardHurdle['referenceSource'];
} {
  const bestBid = finite(payload.bids?.[0]?.[0]);
  const bestAsk = finite(payload.asks?.[0]?.[0]);
  if (bestBid > 0 && bestAsk > 0 && bestAsk >= bestBid) {
    return { price: (bestBid + bestAsk) / 2, source: 'binance-depth-mid' };
  }
  if (bestBid > 0 || bestAsk > 0) {
    return { price: bestBid || bestAsk, source: 'binance-depth-mid' };
  }
  return { price: fallback, source: 'delay-close' };
}

function directionalDelayFlow(review: any, direction: Direction): number {
  const sign = direction === 'short' ? -1 : 1;
  const values = Array.isArray(review?.volumeDeltaQuote)
    ? review.volumeDeltaQuote.map(Number).filter(Number.isFinite)
    : [];
  return Math.max(0, values.reduce((sum: number, value: number) => sum + value * sign, 0));
}

async function fetchDepth(symbol: string): Promise<DepthPayload> {
  const normalized = symbol.toUpperCase();
  const cacheMs = numberEnv('INTRUSION_FORWARD_HURDLE_CACHE_SECONDS', 30, 5, 300) * 1_000;
  const cached = depthCache.get(normalized);
  if (cached && Date.now() - cached.fetchedAt <= cacheMs) return cached.payload;
  const active = depthInFlight.get(normalized);
  if (active) return active;

  const limit = Math.round(numberEnv('INTRUSION_FORWARD_HURDLE_DEPTH_LIMIT', 500, 100, 500));
  const request = binanceGet<DepthPayload>('https://fapi.binance.com/fapi/v1/depth', {
    params: { symbol: normalized, limit },
    timeout: 10_000
  }).then((response) => {
    depthCache.set(normalized, { fetchedAt: Date.now(), payload: response.data });
    return response.data;
  }).finally(() => depthInFlight.delete(normalized));
  depthInFlight.set(normalized, request);
  return request;
}

function binanceCandidate(input: {
  payload: DepthPayload;
  referencePrice: number;
  direction: Direction;
  horizonPct: number;
  binPct: number;
  observedAt: string;
}): IntrusionForwardHurdle['binance'] {
  const side: HurdleSide = input.direction === 'long' ? 'ask' : 'bid';
  const rawLevels = (side === 'ask' ? input.payload.asks : input.payload.bids) || [];
  const bins = new Map<number, { usd: number; weightedPrice: number; levels: number }>();
  let levelsConsidered = 0;

  for (const raw of rawLevels) {
    const price = finite(raw?.[0]);
    const quantity = finite(raw?.[1]);
    if (!(price > 0) || !(quantity > 0)) continue;
    const directionalDistance = input.direction === 'long'
      ? price - input.referencePrice
      : input.referencePrice - price;
    if (directionalDistance <= 0) continue;
    const distancePct = directionalDistance / input.referencePrice * 100;
    if (distancePct > input.horizonPct) continue;
    const index = Math.floor(distancePct / input.binPct);
    const usd = price * quantity;
    const bin = bins.get(index) || { usd: 0, weightedPrice: 0, levels: 0 };
    bin.usd += usd;
    bin.weightedPrice += price * usd;
    bin.levels += 1;
    bins.set(index, bin);
    levelsConsidered += 1;
  }

  const values = [...bins.values()].map((bin) => bin.usd).filter((value) => value > 0);
  const medianBinUsd = percentile(values, 0.5);
  const upperQuartileUsd = percentile(values, 0.75);
  const materialThresholdUsd = Math.max(medianBinUsd * 2, upperQuartileUsd * 1.25);
  const candidates: HurdleCandidate[] = [];
  for (const bin of bins.values()) {
    if (bin.usd < materialThresholdUsd || !(bin.weightedPrice > 0)) continue;
    const price = bin.weightedPrice / bin.usd;
    const distancePct = Math.abs(price - input.referencePrice) / input.referencePrice * 100;
    candidates.push({
      price,
      distancePct,
      side,
      source: 'binance',
      binanceUsd: bin.usd,
      coinGlassUsd: 0,
      effectiveUsd: bin.usd
    });
  }

  return {
    snapshotAt: input.observedAt,
    lastUpdateId: input.payload.lastUpdateId,
    levelsConsidered,
    binCount: bins.size,
    medianBinUsd,
    materialThresholdUsd,
    nearestMaterial: candidates.sort((left, right) => left.distancePct - right.distancePct)[0]
  };
}

function coinGlassCandidate(input: {
  snapshot?: any;
  referencePrice: number;
  direction: Direction;
  horizonPct: number;
  observedAt: string;
}): IntrusionForwardHurdle['coinGlass'] | undefined {
  const snapshot = input.snapshot;
  if (!snapshot || snapshot.enabled === false || !Array.isArray(snapshot.levels)) return undefined;
  const maxAgeMinutes = numberEnv('INTRUSION_FORWARD_HURDLE_COINGLASS_MAX_AGE_MINUTES', 15, 1, 120);
  const observedAtMs = Date.parse(input.observedAt);
  const fetchedAtMs = Date.parse(String(snapshot.fetchedAt || ''));
  const ageMinutes = Number.isFinite(observedAtMs) && Number.isFinite(fetchedAtMs)
    ? (observedAtMs - fetchedAtMs) / 60_000
    : undefined;
  const freshnessError = ageMinutes === undefined
    ? 'CoinGlass snapshot has no valid fetchedAt timestamp.'
    : ageMinutes < -1
      ? 'CoinGlass snapshot was fetched after the hurdle observation time.'
      : ageMinutes > maxAgeMinutes
        ? `CoinGlass snapshot is ${ageMinutes.toFixed(1)} minutes old; maximum is ${maxAgeMinutes.toFixed(1)} minutes.`
        : undefined;
  if (freshnessError) {
    return {
      fetchedAt: snapshot.fetchedAt,
      ageMinutes,
      maxAgeMinutes,
      stale: true,
      freshnessError,
      levelsConsidered: 0
    };
  }

  const expectedSide = input.direction === 'long' ? 'sell' : 'buy';
  const now = observedAtMs;
  const candidates: HurdleCandidate[] = snapshot.levels
    .filter((level: any) => String(level?.side || '').toLowerCase() === expectedSide)
    .map((level: any) => {
      const price = finite(level?.price);
      const volumeUsd = finite(level?.volumeUsd);
      const directionalDistance = input.direction === 'long'
        ? price - input.referencePrice
        : input.referencePrice - price;
      const distancePct = input.referencePrice > 0 ? directionalDistance / input.referencePrice * 100 : 0;
      const startedAtRaw = finite(level?.startedAt);
      const startedAt = startedAtRaw > 0 && startedAtRaw < 10_000_000_000 ? startedAtRaw * 1_000 : startedAtRaw;
      return {
        price,
        distancePct,
        side: (input.direction === 'long' ? 'ask' : 'bid') as HurdleSide,
        source: 'coinglass' as HurdleSource,
        binanceUsd: 0,
        coinGlassUsd: volumeUsd,
        effectiveUsd: volumeUsd,
        persistenceHours: startedAt > 0 ? Math.max(0, (now - startedAt) / 3_600_000) : undefined
      };
    })
    .filter((candidate: HurdleCandidate) =>
      candidate.price > 0 &&
      candidate.effectiveUsd >= finite(snapshot.minUsd) &&
      candidate.distancePct > 0 &&
      candidate.distancePct <= input.horizonPct
    )
    .sort((left: HurdleCandidate, right: HurdleCandidate) => left.distancePct - right.distancePct);
  return {
    fetchedAt: snapshot.fetchedAt,
    ageMinutes,
    maxAgeMinutes,
    stale: false,
    levelsConsidered: candidates.length,
    nearest: candidates[0]
  };
}

export function classifyForwardHurdle(input: {
  symbol: string;
  direction: Direction;
  referencePrice: number;
  review?: any;
  depthPayload?: DepthPayload;
  coinGlassSnapshot?: any;
  observedAt?: string;
  dataCutoffAt?: string;
  depthError?: string;
  referenceSource?: IntrusionForwardHurdle['referenceSource'];
}): IntrusionForwardHurdle {
  const observedAt = input.observedAt || new Date().toISOString();
  const horizonPct = numberEnv('INTRUSION_FORWARD_HURDLE_HORIZON_PCT', 2, 0.25, 10);
  const binPct = numberEnv('INTRUSION_FORWARD_HURDLE_BIN_PCT', 0.1, 0.02, 1);
  const closeDistancePct = numberEnv('INTRUSION_FORWARD_HURDLE_CLOSE_DISTANCE_PCT', 0.75, 0.05, 5);
  const delayFlowUsd = directionalDelayFlow(input.review, input.direction);
  const binance = input.depthPayload
    ? binanceCandidate({
        payload: input.depthPayload,
        referencePrice: input.referencePrice,
        direction: input.direction,
        horizonPct,
        binPct,
        observedAt
      })
    : undefined;
  const coinGlass = coinGlassCandidate({
    snapshot: input.coinGlassSnapshot,
    referencePrice: input.referencePrice,
    direction: input.direction,
    horizonPct,
    observedAt
  });
  const usableCoinGlass = coinGlass && !coinGlass.stale ? coinGlass : undefined;
  const candidates = [binance?.nearestMaterial, usableCoinGlass?.nearest].filter(Boolean) as HurdleCandidate[];
  let firstHurdle = candidates.sort((left, right) => left.distancePct - right.distancePct)[0];

  if (binance?.nearestMaterial && usableCoinGlass?.nearest) {
    const distanceBetweenPct = Math.abs(binance.nearestMaterial.price - usableCoinGlass.nearest.price) / input.referencePrice * 100;
    if (distanceBetweenPct <= binPct) {
      firstHurdle = {
        price: usableCoinGlass.nearest.price,
        distancePct: Math.min(binance.nearestMaterial.distancePct, usableCoinGlass.nearest.distancePct),
        side: usableCoinGlass.nearest.side,
        source: 'combined',
        binanceUsd: binance.nearestMaterial.binanceUsd,
        coinGlassUsd: usableCoinGlass.nearest.coinGlassUsd,
        effectiveUsd: Math.max(binance.nearestMaterial.binanceUsd, usableCoinGlass.nearest.coinGlassUsd),
        persistenceHours: usableCoinGlass.nearest.persistenceHours
      };
    }
  }

  const source: IntrusionForwardHurdle['source'] = binance && usableCoinGlass
    ? 'binance-futures-depth+coinglass'
    : binance ? 'binance-futures-depth' : usableCoinGlass ? 'coinglass' : 'none';
  const status: IntrusionForwardHurdle['status'] = firstHurdle
    ? firstHurdle.distancePct <= closeDistancePct ? 'CLOSE' : 'AHEAD'
    : source === 'none' ? 'DATA_GAP' : 'CLEAR';
  const baseHeadline = status === 'CLOSE'
    ? `HURDLE CLOSE @${priceLabel(firstHurdle!.price)}`
    : status === 'AHEAD'
      ? `HURDLE @${priceLabel(firstHurdle!.price)}`
      : status === 'CLEAR'
        ? 'NO CLOSE HURDLE'
        : 'HURDLE DATA GAP';
  const headline = coinGlass?.stale ? `${baseHeadline} | CG STALE` : baseHeadline;

  return {
    version: 1,
    observeOnly: true,
    causal: true,
    source,
    symbol: input.symbol.toUpperCase(),
    direction: input.direction,
    observedAt,
    dataCutoffAt: input.dataCutoffAt,
    referencePrice: input.referencePrice,
    referenceSource: input.referenceSource || 'delay-close',
    horizonPct,
    binPct,
    closeDistancePct,
    status,
    headline,
    firstHurdle,
    binance,
    coinGlass,
    directionalDelayFlowUsd: delayFlowUsd,
    flowToHurdleRatio: firstHurdle?.effectiveUsd
      ? delayFlowUsd / firstHurdle.effectiveUsd
      : undefined,
    caveat: 'Resting orders can be cancelled or spoofed. Flow-to-hurdle is an observe-only absorption proxy, not guaranteed executable capacity.',
    error: input.depthError
  };
}

export async function evaluateIntrusionForwardHurdle(input: {
  symbol: string;
  direction?: Direction;
  referencePrice: number;
  review?: any;
  coinGlassSnapshot?: any;
  dataCutoffAt?: string;
}): Promise<IntrusionForwardHurdle> {
  const direction = input.direction;
  if (!direction || !(input.referencePrice > 0) || !boolEnv('INTRUSION_FORWARD_HURDLE_ENABLED', true)) {
    return classifyForwardHurdle({
      symbol: input.symbol,
      direction: direction || 'long',
      referencePrice: input.referencePrice,
      review: input.review,
      coinGlassSnapshot: input.coinGlassSnapshot,
      dataCutoffAt: input.dataCutoffAt,
      depthError: direction ? 'Forward hurdle scanner disabled or reference price unavailable.' : 'Direction unavailable.'
    });
  }
  try {
    const payload = await fetchDepth(input.symbol);
    const reference = depthReferencePrice(payload, input.referencePrice);
    return classifyForwardHurdle({
      ...input,
      direction,
      referencePrice: reference.price,
      referenceSource: reference.source,
      depthPayload: payload,
      observedAt: new Date().toISOString()
    });
  } catch (error) {
    return classifyForwardHurdle({
      ...input,
      direction,
      observedAt: new Date().toISOString(),
      depthError: error instanceof Error ? error.message : String(error)
    });
  }
}

export function forwardHurdleBody(hurdle: IntrusionForwardHurdle): string {
  const level = hurdle.firstHurdle;
  const coinGlassFreshness = !hurdle.coinGlass
    ? 'not configured'
    : hurdle.coinGlass.stale
      ? `STALE (${hurdle.coinGlass.freshnessError || 'snapshot rejected'})`
      : `${hurdle.coinGlass.ageMinutes?.toFixed(1) || '0.0'} min old`;
  return [
    'Forward hurdle scan (observe-only)',
    `Result: ${hurdle.headline}`,
    `Reference price: ${priceLabel(hurdle.referencePrice)} (${hurdle.referenceSource})`,
    `First hurdle: ${level ? `${priceLabel(level.price)} (${level.distancePct.toFixed(2)}% ahead)` : '-'}`,
    `Visible Binance depth: ${level ? compactUsd(level.binanceUsd) : '-'}`,
    `CoinGlass wall: ${level ? compactUsd(level.coinGlassUsd) : '-'}`,
    `CoinGlass freshness: ${coinGlassFreshness}`,
    `Directional Delay flow: ${compactUsd(hurdle.directionalDelayFlowUsd)}`,
    `Flow / hurdle: ${hurdle.flowToHurdleRatio === undefined ? '-' : `${hurdle.flowToHurdleRatio.toFixed(2)}x`}`,
    `Source: ${hurdle.source}`,
    `Measured at: ${hurdle.observedAt}`,
    `Note: ${hurdle.caveat}`
  ].join('\n');
}
