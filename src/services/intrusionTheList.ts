import fs from 'fs';
import path from 'path';

import type { IntrusionImpulseQuality } from './intrusionImpulseQuality';
import type { IntrusionForwardHurdle } from './intrusionForwardHurdle';

export type IntrusionUserLabel = 'STRONG' | 'WEAK';

export type BinanceDelayAnalytics = {
  source: 'binance-futures';
  causal: true;
  symbol: string;
  alertTimestamp: string;
  delayCutoffAt?: string;
  candleTimestamps: string[];
  candleOpens: number[];
  candleCloses: number[];
  quoteVolume: number[];
  takerDeltaQuote: number[];
  closedCandles: number;
  priceChangePct?: number;
  directionalPriceChangePct?: number;
  totalQuoteVolume: number;
  cumulativeTakerDeltaQuote: number;
  takerDeltaRatio?: number;
  directionalTakerDeltaRatio?: number;
  alignedTakerDeltaCandles: number;
  takerDeltaPersistencePct?: number;
  oiContractChangePct?: number;
  oiUsdChangePct?: number;
  oiSamples: number;
  oiPriceRegime: 'POSITION_FLUSH_WITH_MOVE' | 'NEW_POSITION_BUILDUP' | 'OI_FLAT_OR_MIXED' | 'DATA_GAP';
};

export type IntrusionTheListRecord = {
  version: 1;
  key: string;
  market: string;
  symbol: string;
  asset: string;
  alertTimestamp: string;
  timestampNl?: string;
  direction?: 'long' | 'short';
  delayCutoffAt?: string;
  filteredStatus?: string;
  userLabel?: IntrusionUserLabel;
  userLabelNote?: string;
  automaticLabel: IntrusionImpulseQuality['label'];
  selectedMetric: 'OI_FLUSH_PCT';
  impulseQuality: IntrusionImpulseQuality;
  forwardHurdle?: IntrusionForwardHurdle;
  binance?: BinanceDelayAnalytics;
  candleReview?: any;
  coinGlass?: any;
  observedAt: string;
  updatedAt: string;
};

type IntrusionTheListFile = {
  version: 1;
  methodology: {
    selectedMetric: 'OI_FLUSH_PCT';
    strongWhenContractChangePctLte: number;
    observeOnly: true;
    labeledSampleSize: number;
  };
  records: IntrusionTheListRecord[];
};

const STRONG_THRESHOLD_PCT = -1.8;

function finiteArray(value: unknown): number[] {
  return Array.isArray(value)
    ? value.map(Number).filter((item) => Number.isFinite(item))
    : [];
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

export function buildBinanceDelayAnalytics(input: {
  symbol: string;
  alertTimestamp: string;
  delayCutoffAt?: string;
  direction?: 'long' | 'short';
  candleReview?: any;
  impulseQuality: IntrusionImpulseQuality;
}): BinanceDelayAnalytics {
  const candleTimestamps = stringArray(input.candleReview?.candleTimestamps);
  const candleOpens = finiteArray(input.candleReview?.candleOpens);
  const candleCloses = finiteArray(input.candleReview?.candleCloses);
  const quoteVolume = finiteArray(input.candleReview?.quoteVolume);
  const takerDeltaQuote = finiteArray(input.candleReview?.volumeDeltaQuote);
  const closedCandles = Math.min(
    Number(input.candleReview?.closedCandlesChecked) || Number.POSITIVE_INFINITY,
    candleOpens.length,
    candleCloses.length
  );
  const normalizedClosedCandles = Number.isFinite(closedCandles) ? closedCandles : 0;
  const firstOpen = candleOpens[0];
  const lastClose = candleCloses[normalizedClosedCandles - 1];
  const priceChangePct = firstOpen > 0 && Number.isFinite(lastClose)
    ? ((lastClose / firstOpen) - 1) * 100
    : undefined;
  const directionSign = input.direction === 'short' ? -1 : 1;
  const directionalPriceChangePct = priceChangePct === undefined
    ? undefined
    : priceChangePct * directionSign;
  const totalQuoteVolume = quoteVolume.reduce((sum, value) => sum + Math.abs(value), 0);
  const cumulativeTakerDeltaQuote = takerDeltaQuote.reduce((sum, value) => sum + value, 0);
  const takerDeltaRatio = totalQuoteVolume > 0
    ? cumulativeTakerDeltaQuote / totalQuoteVolume
    : undefined;
  const directionalTakerDeltaRatio = takerDeltaRatio === undefined
    ? undefined
    : takerDeltaRatio * directionSign;
  const alignedTakerDeltaCandles = takerDeltaQuote.filter((value) => value * directionSign > 0).length;
  const takerDeltaPersistencePct = takerDeltaQuote.length
    ? alignedTakerDeltaCandles / takerDeltaQuote.length * 100
    : undefined;
  const oiContractChangePct = input.impulseQuality.openInterest?.contractChangePct;
  const oiUsdChangePct = input.impulseQuality.openInterest?.usdChangePct;
  const oiSamples = input.impulseQuality.openInterest?.samples || 0;
  const oiPriceRegime: BinanceDelayAnalytics['oiPriceRegime'] =
    oiSamples < 2 || oiContractChangePct === undefined || directionalPriceChangePct === undefined
      ? 'DATA_GAP'
      : oiContractChangePct <= STRONG_THRESHOLD_PCT && directionalPriceChangePct > 0
        ? 'POSITION_FLUSH_WITH_MOVE'
        : oiContractChangePct > 0 && directionalPriceChangePct > 0
          ? 'NEW_POSITION_BUILDUP'
          : 'OI_FLAT_OR_MIXED';

  return {
    source: 'binance-futures',
    causal: true,
    symbol: input.symbol,
    alertTimestamp: input.alertTimestamp,
    delayCutoffAt: input.delayCutoffAt,
    candleTimestamps,
    candleOpens,
    candleCloses,
    quoteVolume,
    takerDeltaQuote,
    closedCandles: normalizedClosedCandles,
    priceChangePct,
    directionalPriceChangePct,
    totalQuoteVolume,
    cumulativeTakerDeltaQuote,
    takerDeltaRatio,
    directionalTakerDeltaRatio,
    alignedTakerDeltaCandles,
    takerDeltaPersistencePct,
    oiContractChangePct,
    oiUsdChangePct,
    oiSamples,
    oiPriceRegime
  };
}

function filePath(): string {
  const configured = String(process.env.INTRUSION_THE_LIST_FILE || '').trim();
  if (configured) return configured;
  const domDirectory = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  const base = domDirectory ? path.dirname(domDirectory) : path.join(process.cwd(), 'data');
  return path.join(base, 'intrusion-the-list.json');
}

function seededQuality(symbol: string, from: string, to: string, contractChangePct: number, usdChangePct: number, samples: number): IntrusionImpulseQuality {
  const strong = contractChangePct <= STRONG_THRESHOLD_PCT;
  return {
    version: 2,
    observeOnly: true,
    label: strong ? 'IQ STRONG' : 'IQ WEAK',
    headline: `${strong ? 'IQ STRONG' : 'IQ WEAK'} OI ${contractChangePct.toFixed(2)}%`,
    score: strong ? 1 : 0,
    availableSignals: 1,
    evaluatedAt: to,
    dataCutoffAt: to,
    selectedMetric: 'OI_FLUSH_PCT',
    strongThresholdPct: STRONG_THRESHOLD_PCT,
    reasons: [`Contract OI changed ${contractChangePct.toFixed(2)}% inside The Delay.`],
    candle: { count: 0 },
    dom: { requiredCoverageMinutes: 0 },
    coinGlass: {},
    openInterest: {
      source: 'binance-futures-open-interest',
      symbol,
      from,
      to,
      fetchedAt: to,
      samples,
      contractChangePct,
      usdChangePct
    }
  };
}

function seedRecord(input: {
  market: string;
  symbol: string;
  asset: string;
  timestamp: string;
  timestampNl: string;
  cutoff: string;
  direction: 'long' | 'short';
  userLabel: IntrusionUserLabel;
  contractChangePct: number;
  usdChangePct: number;
  samples: number;
  note: string;
}): IntrusionTheListRecord {
  const quality = seededQuality(input.symbol, `${input.timestamp.replace(' ', 'T')}Z`, input.cutoff, input.contractChangePct, input.usdChangePct, input.samples);
  return {
    version: 1,
    key: `${input.market}|${input.timestamp}`,
    market: input.market,
    symbol: input.symbol,
    asset: input.asset,
    alertTimestamp: input.timestamp,
    timestampNl: input.timestampNl,
    direction: input.direction,
    delayCutoffAt: input.cutoff,
    filteredStatus: 'PASS',
    userLabel: input.userLabel,
    userLabelNote: input.note,
    automaticLabel: quality.label,
    selectedMetric: 'OI_FLUSH_PCT',
    impulseQuality: quality,
    observedAt: input.cutoff,
    updatedAt: input.cutoff
  };
}

const SEED_RECORDS: IntrusionTheListRecord[] = [
  seedRecord({
    market: 'BTC-USD', symbol: 'BTCUSDT', asset: 'BTC', timestamp: '2026-08-19 20:00:00',
    timestampNl: '19-08-2026 22:00 NL', cutoff: '2026-08-19T22:32:57.517Z', direction: 'long',
    userLabel: 'STRONG', contractChangePct: -2.2595, usdChangePct: -1.2063, samples: 31,
    note: 'User-confirmed true impulse.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-08-23 13:00:00',
    timestampNl: '23-08-2026 15:00 NL', cutoff: '2026-08-23T14:54:00.742Z', direction: 'long',
    userLabel: 'STRONG', contractChangePct: -2.3362, usdChangePct: -0.3592, samples: 23,
    note: 'User-confirmed true impulse.'
  }),
  seedRecord({
    market: 'BTC-USD', symbol: 'BTCUSDT', asset: 'BTC', timestamp: '2026-07-21 05:00:00',
    timestampNl: '21-07-2026 07:00 NL', cutoff: '2026-07-21T07:33:00.000Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: 1.2590, usdChangePct: 1.9922, samples: 31,
    note: 'User-confirmed bounce/false impulse; OI reconstructed from the official Binance Vision archive.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-08-26 13:00:00',
    timestampNl: '26-08-2026 15:00 NL', cutoff: '2026-08-26T14:08:13.439Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: -0.6188, usdChangePct: -1.8838, samples: 14,
    note: 'User-confirmed false impulse.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-08-25 19:00:00',
    timestampNl: '25-08-2026 21:00 NL', cutoff: '2026-08-25T21:00:56.096Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: -1.3886, usdChangePct: -4.3645, samples: 25,
    note: 'User-confirmed false impulse.'
  }),
  seedRecord({
    market: 'SOL-USD', symbol: 'SOLUSDT', asset: 'SOL', timestamp: '2026-08-27 08:00:00',
    timestampNl: '27-08-2026 10:00 NL', cutoff: '2026-08-27T09:52:24.813Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: 2.92136, usdChangePct: 0, samples: 23,
    note: 'User-confirmed false impulse.'
  }),
  seedRecord({
    market: 'ETH-USD', symbol: 'ETHUSDT', asset: 'ETH', timestamp: '2026-08-27 09:00:00',
    timestampNl: '27-08-2026 11:00 NL', cutoff: '2026-08-27T10:49:29.010Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: -2.36159, usdChangePct: 0, samples: 22,
    note: 'User-confirmed false impulse.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-08-28 09:00:00',
    timestampNl: '28-08-2026 11:00 NL', cutoff: '2026-08-28T10:23:46.578Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.371768, usdChangePct: 0.479232, samples: 17,
    note: 'User-confirmed false impulse.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-08-28 16:00:00',
    timestampNl: '28-08-2026 18:00 NL', cutoff: '2026-08-28T17:47:52.761Z', direction: 'short',
    userLabel: 'STRONG', contractChangePct: -1.846121, usdChangePct: -4.089121, samples: 22,
    note: 'User-confirmed true impulse; strong but not extremely strong. Stopped out 29-08-2026 16:00 NL with a small profit.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-08-29 14:00:00',
    timestampNl: '29-08-2026 16:00 NL', cutoff: '2026-08-29T15:04:21.006Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: 2.6769214175664358, usdChangePct: 6.241772732132067, samples: 13,
    note: 'User-confirmed false impulse. Original stored Delay assessment: candle/delta PASS and IQ WEAK with rising contract OI.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-08-30 12:00:00',
    timestampNl: '30-08-2026 14:00 NL', cutoff: '2026-08-30T13:14:17.895Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: 0.4456443352116146, usdChangePct: 1.3486931446338835, samples: 15,
    note: 'User-confirmed false impulse. Original stored Delay assessment: candle/delta PASS and IQ WEAK with rising contract OI.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-08-31 01:00:00',
    timestampNl: '31-08-2026 03:00 NL', cutoff: '2026-08-31T02:10:19.015Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 1.0504007381189506, usdChangePct: -0.428976218842414, samples: 15,
    note: 'User-confirmed false impulse. Original stored Delay assessment: candle/delta PASS and IQ WEAK with rising contract OI.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-09-01 08:00:00',
    timestampNl: '01-09-2026 10:00 NL', cutoff: '2026-09-01T09:12:42.302Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 1.0059355189300012, usdChangePct: -1.2832794469722186, samples: 15,
    note: 'User-confirmed false impulse. Original stored Delay assessment: candle/delta PASS and IQ WEAK with rising contract OI.'
  }),
  seedRecord({
    market: 'SOL-USD', symbol: 'SOLUSDT', asset: 'SOL', timestamp: '2026-09-01 18:00:00',
    timestampNl: '01-09-2026 20:00 NL', cutoff: '2026-09-01T19:15:16.580Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.0720722355503911, usdChangePct: -0.6944203101710178, samples: 16,
    note: 'User-confirmed false impulse. Original stored Delay assessment: candle/delta PASS and IQ WEAK with slightly rising contract OI.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-09-01 20:00:00',
    timestampNl: '01-09-2026 22:00 NL', cutoff: '2026-09-01T21:12:42.294Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.7572342804394117, usdChangePct: 0.22363972345176286, samples: 15,
    note: 'User-confirmed complete false impulse ("hele false impulse"). Original stored Delay assessment: candle/delta PASS, IQ WEAK with rising contract OI, and a close hurdle.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-09-02 00:00:00',
    timestampNl: '02-09-2026 02:00 NL', cutoff: '2026-09-02T01:12:41.591Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.9776061498412592, usdChangePct: 0.08965996655878605, samples: 15,
    note: 'User-confirmed complete false impulse ("hele false impulse"). Original stored Delay assessment: candle/delta PASS, IQ WEAK with rising contract OI, and a close hurdle.'
  }),
  seedRecord({
    market: 'BTC-USD', symbol: 'BTCUSDT', asset: 'BTC', timestamp: '2026-09-03 18:00:00',
    timestampNl: '03-09-2026 20:00 NL', cutoff: '2026-09-03T21:38:51.554Z', direction: 'long',
    userLabel: 'WEAK', contractChangePct: -0.487456221422522, usdChangePct: 0.8612793171132838, samples: 44,
    note: 'User-confirmed false impulse. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'SOL-USD', symbol: 'SOLUSDT', asset: 'SOL', timestamp: '2026-09-02 09:00:00',
    timestampNl: '02-09-2026 11:00 NL', cutoff: '2026-09-02T10:15:16.214Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.49889688740754057, usdChangePct: 0.049397619406033044, samples: 16,
    note: 'User-confirmed false impulse. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'SOL-USD', symbol: 'SOLUSDT', asset: 'SOL', timestamp: '2026-09-02 10:00:00',
    timestampNl: '02-09-2026 12:00 NL', cutoff: '2026-09-02T11:15:16.276Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 1.564014295545335, usdChangePct: 1.0039195340886042, samples: 16,
    note: 'User-confirmed false impulse. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-09-02 10:00:00',
    timestampNl: '02-09-2026 12:00 NL', cutoff: '2026-09-02T11:16:42.591Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: -1.297872996637106, usdChangePct: -3.163631453283078, samples: 16,
    note: 'User-confirmed false impulse. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-09-02 11:00:00',
    timestampNl: '02-09-2026 13:00 NL', cutoff: '2026-09-02T12:48:40.957Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 1.4691702303321952, usdChangePct: 1.3048619203551715, samples: 22,
    note: 'User-confirmed false impulse. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'INJ-USD', symbol: 'INJUSDT', asset: 'INJ', timestamp: '2026-09-02 22:00:00',
    timestampNl: '03-09-2026 00:00 NL', cutoff: '2026-09-02T23:46:32.5Z', direction: 'short',
    userLabel: 'WEAK', contractChangePct: 0.5564487181533462, usdChangePct: 0.17561459947232905, samples: 22,
    note: 'User-confirmed strong false impulse ("strong false impulse"). This describes the user outcome, not an automatic IQ STRONG label. Original stored Delay assessment: IQ WEAK.'
  }),
  seedRecord({
    market: 'ZEC-USD', symbol: 'ZECUSDT', asset: 'ZEC', timestamp: '2026-09-03 14:00:00',
    timestampNl: '03-09-2026 16:00 NL', cutoff: '2026-09-03T15:59:31.241Z', direction: 'long',
    userLabel: 'STRONG', contractChangePct: 7.790200965905969, usdChangePct: 19.345429756979037, samples: 24,
    note: 'User-observed impulse with potential profit ("impulse met potentiele winst"); realized profit is not confirmed. Original stored Delay assessment: IQ WEAK with rising contract OI.'
  })
];

function readRecords(): IntrusionTheListRecord[] {
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath(), 'utf8')) as IntrusionTheListFile;
    return Array.isArray(parsed.records) ? parsed.records : [];
  } catch {
    return [];
  }
}

function mergeWithSeeds(records: IntrusionTheListRecord[]): IntrusionTheListRecord[] {
  const byKey = new Map<string, IntrusionTheListRecord>();
  for (const seed of SEED_RECORDS) byKey.set(seed.key, seed);
  for (const record of records) {
    const seed = byKey.get(record.key);
    byKey.set(record.key, {
      ...seed,
      ...record,
      userLabel: record.userLabel || seed?.userLabel,
      userLabelNote: record.userLabelNote || seed?.userLabelNote
    });
  }
  return [...byKey.values()].sort((left, right) => left.alertTimestamp.localeCompare(right.alertTimestamp));
}

function writeRecords(records: IntrusionTheListRecord[]): void {
  const file = filePath();
  const merged = mergeWithSeeds(records).slice(-5_000);
  const payload: IntrusionTheListFile = {
    version: 1,
    methodology: {
      selectedMetric: 'OI_FLUSH_PCT',
      strongWhenContractChangePctLte: STRONG_THRESHOLD_PCT,
      observeOnly: true,
      labeledSampleSize: merged.filter((record) => record.userLabel).length
    },
    records: merged
  };
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(payload, null, 2));
  fs.renameSync(temporary, file);
}

export function recordIntrusionTheList(input: Omit<IntrusionTheListRecord, 'version' | 'key' | 'automaticLabel' | 'selectedMetric' | 'observedAt' | 'updatedAt'>): IntrusionTheListRecord {
  const records = mergeWithSeeds(readRecords());
  const key = `${input.market}|${input.alertTimestamp}`;
  const existing = records.find((record) => record.key === key);
  const now = new Date().toISOString();
  const record: IntrusionTheListRecord = {
    ...existing,
    ...input,
    version: 1,
    key,
    userLabel: existing?.userLabel,
    userLabelNote: existing?.userLabelNote,
    automaticLabel: input.impulseQuality.label,
    selectedMetric: 'OI_FLUSH_PCT',
    binance: buildBinanceDelayAnalytics({
      symbol: input.symbol,
      alertTimestamp: input.alertTimestamp,
      delayCutoffAt: input.delayCutoffAt,
      direction: input.direction,
      candleReview: input.candleReview,
      impulseQuality: input.impulseQuality
    }),
    observedAt: existing?.observedAt || now,
    updatedAt: now
  };
  writeRecords([...records.filter((candidate) => candidate.key !== key), record]);
  return record;
}

export function intrusionTheListSnapshot(): IntrusionTheListFile {
  const records = mergeWithSeeds(readRecords());
  writeRecords(records);
  return {
    version: 1,
    methodology: {
      selectedMetric: 'OI_FLUSH_PCT',
      strongWhenContractChangePctLte: STRONG_THRESHOLD_PCT,
      observeOnly: true,
      labeledSampleSize: records.filter((record) => record.userLabel).length
    },
    records: records.slice().reverse()
  };
}
