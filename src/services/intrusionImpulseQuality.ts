import { DomMinuteRecord } from './decentralizedDomCollector';
import { aggregateIntrusionDomWindow, IntrusionDomWindow } from './decentraderIntrusionDomStudy';

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
  version: 1;
  observeOnly: true;
  label: IntrusionImpulseQualityLabel;
  score: number;
  availableSignals: number;
  evaluatedAt: string;
  dataCutoffAt?: string;
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
};

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

  const reasons: string[] = [];
  const checks: Array<{ available: boolean; passed: boolean; pass: string; fail: string }> = [
    {
      available: input.review.status !== undefined,
      passed: input.review.status === 'PASS',
      pass: 'Delay candle filter passed.',
      fail: `Delay candle filter is ${input.review.status || 'unavailable'}.`
    },
    {
      available: directionalReturnPct !== undefined,
      passed: (directionalReturnPct || 0) >= 0.20,
      pass: 'Directional candle displacement is material.',
      fail: 'Directional candle displacement is limited.'
    },
    {
      available: gapDisplacementRatio !== undefined,
      passed: (gapDisplacementRatio || 0) >= 0.08,
      pass: 'Delay displacement is meaningful relative to the gap.',
      fail: 'Delay displacement is small relative to the gap.'
    },
    {
      available: directionalDeltaRatio !== undefined,
      passed: (directionalDeltaRatio || 0) >= 0.05,
      pass: 'Net taker delta supports the intrusion direction.',
      fail: 'Net taker delta support is weak.'
    },
    {
      available: weakestDirectionalDeltaRatio !== undefined,
      passed: (weakestDirectionalDeltaRatio || 0) >= 0.01,
      pass: 'Every Delay candle has directional taker participation.',
      fail: 'At least one Delay candle has weak taker participation.'
    },
    {
      available: domWindow?.directionalTakerDeltaUsd !== undefined,
      passed: (domWindow?.directionalTakerDeltaUsd || 0) > 0,
      pass: 'Live DOM taker flow supports the direction.',
      fail: 'Live DOM taker flow opposes the direction.'
    },
    {
      available: domWindow?.directionalImbalance25Bps !== undefined || domWindow?.directionalBookPressureUsd !== undefined,
      passed: (domWindow?.directionalImbalance25Bps || 0) > 0.02 || (domWindow?.directionalBookPressureUsd || 0) > 0,
      pass: 'Live order-book pressure supports the direction.',
      fail: 'Live order-book pressure does not confirm the direction.'
    },
    {
      available: directionalCgRatio !== undefined,
      passed: (directionalCgRatio || 0) >= 1.05,
      pass: 'CoinGlass gap liquidity supports the direction.',
      fail: 'CoinGlass gap liquidity is neutral or opposing.'
    }
  ];
  for (const check of checks) if (check.available) reasons.push(check.passed ? check.pass : check.fail);
  const score = checks.filter((check) => check.available && check.passed).length;
  const availableSignals = checks.filter((check) => check.available).length;
  const hasCausalCoverage = Boolean(
    direction &&
    fromMs !== undefined &&
    cutoffMs !== undefined &&
    completeCandleMetrics &&
    domWindow &&
    domWindow.coverageMinutes >= requiredCoverageMinutes
  );
  const label: IntrusionImpulseQualityLabel = !hasCausalCoverage
    ? 'IQ DATA GAP'
    : score >= 6
      ? 'IQ STRONG'
      : score >= 4
        ? 'IQ MIXED'
        : 'IQ WEAK';

  if (!hasCausalCoverage) {
    if (!completeCandleMetrics) reasons.unshift('Complete causal candle volume metrics are not available.');
    if (!domWindow || domWindow.coverageMinutes < requiredCoverageMinutes) {
      reasons.unshift(`Causal DOM coverage is ${domWindow?.coverageMinutes || 0}/${requiredCoverageMinutes} required minutes.`);
    }
  }

  return {
    version: 1,
    observeOnly: true,
    label,
    score,
    availableSignals,
    evaluatedAt,
    dataCutoffAt: input.review.delayCutoffAt,
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
    }
  };
}
