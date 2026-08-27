import { DomMinuteRecord } from './decentralizedDomCollector';
import { aggregateIntrusionDomWindow, IntrusionDomWindow } from './decentraderIntrusionDomStudy';
import type { BinanceOpenInterestWindow } from './binanceOpenInterestHistory';

type Direction = 'long' | 'short';

type CandleReviewInput = {
  status?: string;
  delayCutoffAt?: string;
  candleOpens?: number[];
  candleCloses?: number[];
  quoteVolume?: number[];
  volumeDeltaQuote?: number[];
};

type CoinGlassInput = {
  source?: string | null;
  buyUsd?: number;
  sellUsd?: number;
  fetchedAt?: string | null;
};

export type IntrusionImpulseQualityLabel = 'IQ STRONG' | 'IQ MIXED' | 'IQ WEAK' | 'IQ DATA GAP';

export type IntrusionImpulseQuality = {
  version: 2;
  observeOnly: true;
  label: IntrusionImpulseQualityLabel;
  headline: string;
  score: number;
  availableSignals: number;
  evaluatedAt: string;
  dataCutoffAt?: string;
  selectedMetric: 'OI_FLUSH_PCT';
  strongThresholdPct: number;
  reasons: string[];
  candle: {
    count: number;
    directionalReturnPct?: number;
    gapDisplacementRatio?: number;
    directionalDeltaRatio?: number;
    weakestDirectionalDeltaRatio?: number;
  };
  dom: {
    requiredCoverageMinutes: number;
    window?: IntrusionDomWindow;
  };
  coinGlass: {
    source?: string | null;
    directionalSupportUsd?: number;
    opposingUsd?: number;
    directionalRatio?: number;
    fetchedAt?: string | null;
  };
  openInterest?: BinanceOpenInterestWindow;
};

const OI_FLUSH_STRONG_THRESHOLD_PCT = -1.8;

function finite(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function timestampMs(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Date.parse(value.includes('T') ? value : `${value.replace(' ', 'T')}Z`);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function directional(value: number | undefined, direction: Direction): number | undefined {
  return value === undefined ? undefined : value * (direction === 'long' ? 1 : -1);
}

export function evaluateIntrusionImpulseQuality(input: {
  direction?: Direction;
  alertTimestamp: string;
  gapWidth?: number;
  review: CandleReviewInput;
  domRecords: DomMinuteRecord[];
  coinGlass?: CoinGlassInput;
  openInterest?: BinanceOpenInterestWindow;
  evaluatedAt?: string;
}): IntrusionImpulseQuality {
  const evaluatedAt = input.evaluatedAt || new Date().toISOString();
  const fromMs = timestampMs(input.alertTimestamp);
  const cutoffMs = timestampMs(input.review.delayCutoffAt);
  const direction = input.direction;
  const durationMinutes = fromMs !== undefined && cutoffMs !== undefined
    ? Math.max(0, Math.floor((cutoffMs - fromMs) / 60_000))
    : 0;
  const requiredCoverageMinutes = Math.max(3, Math.min(30, Math.floor(durationMinutes * 0.25)));
  const causalDomRecords = fromMs === undefined || cutoffMs === undefined
    ? []
    : input.domRecords.filter((record) => {
        const start = Date.parse(record.bucketStart);
        const end = Date.parse(record.bucketEnd);
        return Number.isFinite(start) && Number.isFinite(end) && start >= fromMs && end <= cutoffMs;
      });
  const domWindow = direction && fromMs !== undefined && cutoffMs !== undefined
    ? aggregateIntrusionDomWindow(causalDomRecords, fromMs, cutoffMs, direction)
    : undefined;

  const opens = (input.review.candleOpens || []).map(finite).filter((value): value is number => value !== undefined);
  const closes = (input.review.candleCloses || []).map(finite).filter((value): value is number => value !== undefined);
  const quoteVolumes = (input.review.quoteVolume || []).map(finite).filter((value): value is number => value !== undefined);
  const deltas = (input.review.volumeDeltaQuote || []).map(finite).filter((value): value is number => value !== undefined);
  const completeCandleMetrics = opens.length > 0 && opens.length === closes.length && opens.length === quoteVolumes.length && opens.length === deltas.length;
  const rawReturnPct = completeCandleMetrics && opens[0] > 0
    ? ((closes[closes.length - 1] / opens[0]) - 1) * 100
    : undefined;
  const directionalReturnPct = direction ? directional(rawReturnPct, direction) : undefined;
  const gapWidth = finite(input.gapWidth);
  const gapDisplacementRatio = directionalReturnPct !== undefined && gapWidth !== undefined && gapWidth > 0 && opens[0] > 0
    ? Math.max(0, directionalReturnPct / 100 * opens[0] / gapWidth)
    : undefined;
  const totalQuoteVolume = quoteVolumes.reduce((sum, value) => sum + Math.abs(value), 0);
  const rawDeltaRatio = totalQuoteVolume > 0
    ? deltas.reduce((sum, value) => sum + value, 0) / totalQuoteVolume
    : undefined;
  const directionalDeltaRatio = direction ? directional(rawDeltaRatio, direction) : undefined;
  const perCandleDirectionalDeltaRatios = completeCandleMetrics && direction
    ? deltas.map((value, index) => directional(quoteVolumes[index] > 0 ? value / quoteVolumes[index] : undefined, direction))
        .filter((value): value is number => value !== undefined)
    : [];
  const weakestDirectionalDeltaRatio = perCandleDirectionalDeltaRatios.length
    ? Math.min(...perCandleDirectionalDeltaRatios)
    : undefined;

  const buyUsd = finite(input.coinGlass?.buyUsd);
  const sellUsd = finite(input.coinGlass?.sellUsd);
  const coinGlassFetchedMs = timestampMs(input.coinGlass?.fetchedAt || undefined);
  const causalCoinGlass = coinGlassFetchedMs === undefined || cutoffMs === undefined || coinGlassFetchedMs <= cutoffMs;
  const hasCoinGlassLiquidity = causalCoinGlass && buyUsd !== undefined && sellUsd !== undefined && buyUsd + sellUsd > 0;
  const directionalSupportUsd = hasCoinGlassLiquidity
    ? direction === 'long' ? buyUsd : direction === 'short' ? sellUsd : undefined
    : undefined;
  const opposingUsd = hasCoinGlassLiquidity
    ? direction === 'long' ? sellUsd : direction === 'short' ? buyUsd : undefined
    : undefined;
  const directionalCgRatio = directionalSupportUsd !== undefined && opposingUsd !== undefined
    ? directionalSupportUsd / Math.max(1, opposingUsd)
    : undefined;

  const oiChange = finite(input.openInterest?.contractChangePct);
  const hasCausalOi = Boolean(
    input.openInterest &&
    input.openInterest.samples >= 2 &&
    oiChange !== undefined &&
    cutoffMs !== undefined &&
    timestampMs(input.openInterest.to) !== undefined &&
    (timestampMs(input.openInterest.to) || 0) <= cutoffMs
  );
  const strong = hasCausalOi && (oiChange as number) <= OI_FLUSH_STRONG_THRESHOLD_PCT;
  const label: IntrusionImpulseQualityLabel = !hasCausalOi
    ? 'IQ DATA GAP'
    : strong ? 'IQ STRONG' : 'IQ WEAK';
  const headline = hasCausalOi
    ? `${label} OI ${(oiChange as number).toFixed(2)}%`
    : 'IQ DATA GAP OI';
  const reasons = hasCausalOi
    ? [
        `Contract OI changed ${(oiChange as number).toFixed(2)}% inside The Delay.`,
        strong
          ? `The provisional strong flush threshold of ${OI_FLUSH_STRONG_THRESHOLD_PCT.toFixed(2)}% was reached.`
          : `The provisional strong flush threshold of ${OI_FLUSH_STRONG_THRESHOLD_PCT.toFixed(2)}% was not reached.`
      ]
    : ['At least two causal Binance Futures Open Interest samples are required inside The Delay.'];
  const score = strong ? 1 : 0;
  const availableSignals = hasCausalOi ? 1 : 0;

  return {
    version: 2,
    observeOnly: true,
    label,
    headline,
    score,
    availableSignals,
    evaluatedAt,
    dataCutoffAt: input.review.delayCutoffAt,
    selectedMetric: 'OI_FLUSH_PCT',
    strongThresholdPct: OI_FLUSH_STRONG_THRESHOLD_PCT,
    reasons,
    candle: {
      count: opens.length,
      directionalReturnPct,
      gapDisplacementRatio,
      directionalDeltaRatio,
      weakestDirectionalDeltaRatio
    },
    dom: {
      requiredCoverageMinutes,
      window: domWindow
    },
    coinGlass: {
      source: input.coinGlass?.source,
      directionalSupportUsd,
      opposingUsd,
      directionalRatio: directionalCgRatio,
      fetchedAt: causalCoinGlass ? input.coinGlass?.fetchedAt : null
    },
    openInterest: input.openInterest
  };
}
