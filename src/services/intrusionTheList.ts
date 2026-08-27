import fs from 'fs';
import path from 'path';

import type { IntrusionImpulseQuality } from './intrusionImpulseQuality';

export type IntrusionUserLabel = 'STRONG' | 'WEAK';

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
