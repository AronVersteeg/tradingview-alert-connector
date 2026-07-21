import fs from 'fs';
import path from 'path';

export type DecentraderDelayEmailType = 'normal' | 'filtered';

export type DecentraderDelayRecord = {
  id: string;
  signature: string;
  emailType: DecentraderDelayEmailType;
  sideCounts: string;
  intrusionTimestamp: string;
  intrusionTimestampNl: string;
  smtpSentAt: string;
  delayMinutes: number;
  delayCandles1h: number;
  completedCandles1h: number;
};

export type DecentraderDelayStats = {
  count: number;
  minMinutes?: number;
  averageMinutes?: number;
  maxMinutes?: number;
  minCompletedCandles1h?: number;
  averageCompletedCandles1h?: number;
  maxCompletedCandles1h?: number;
};

export type DecentraderDelaySnapshot = {
  updatedAt?: string;
  totalRecords: number;
  firstSmtpSentAt?: string;
  lastSmtpSentAt?: string;
  stats: {
    all: DecentraderDelayStats;
    normal: DecentraderDelayStats;
    filtered: DecentraderDelayStats;
  };
  recent: DecentraderDelayRecord[];
  records: DecentraderDelayRecord[];
};

type DelayHistoryFile = {
  updatedAt?: string;
  records: DecentraderDelayRecord[];
};

const DEFAULT_MAX_RECORDS = 10_000;
const RECENT_RECORDS = 100;

function finiteNumber(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function intrusionTimestampMs(timestamp: string): number | undefined {
  const parsed = Date.parse(String(timestamp || '').replace(' ', 'T') + 'Z');
  return Number.isFinite(parsed) ? parsed : undefined;
}

function delayHistoryMaxRecords(): number {
  const parsed = Number(process.env.DECENTRADER_DELAY_HISTORY_MAX_RECORDS || DEFAULT_MAX_RECORDS);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.max(100, Math.min(50_000, Math.floor(parsed)))
    : DEFAULT_MAX_RECORDS;
}

export function decentraderDelayHistoryFile(): string {
  const monitorStateFile = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim();
  return (
    String(process.env.DECENTRADER_DELAY_HISTORY_FILE || '').trim() ||
    (monitorStateFile ? path.join(path.dirname(monitorStateFile), 'decentrader-delay-history.json') : '') ||
    path.join(process.cwd(), 'data', 'decentrader-delay-history.json')
  );
}

function normalizeRecord(value: any): DecentraderDelayRecord | undefined {
  const delayMinutes = finiteNumber(value?.delayMinutes);
  const delayCandles1h = finiteNumber(value?.delayCandles1h);
  const completedCandles1h = finiteNumber(value?.completedCandles1h);
  if (
    typeof value?.id !== 'string' ||
    typeof value?.signature !== 'string' ||
    !['normal', 'filtered'].includes(value?.emailType) ||
    typeof value?.intrusionTimestamp !== 'string' ||
    typeof value?.smtpSentAt !== 'string' ||
    delayMinutes === undefined ||
    delayCandles1h === undefined ||
    completedCandles1h === undefined
  ) {
    return undefined;
  }

  return {
    id: value.id,
    signature: value.signature,
    emailType: value.emailType,
    sideCounts: String(value.sideCounts || ''),
    intrusionTimestamp: value.intrusionTimestamp,
    intrusionTimestampNl: String(value.intrusionTimestampNl || ''),
    smtpSentAt: value.smtpSentAt,
    delayMinutes,
    delayCandles1h,
    completedCandles1h
  };
}

export function readDecentraderDelayHistory(): DelayHistoryFile {
  try {
    const parsed = JSON.parse(fs.readFileSync(decentraderDelayHistoryFile(), 'utf8'));
    return {
      updatedAt: typeof parsed?.updatedAt === 'string' ? parsed.updatedAt : undefined,
      records: Array.isArray(parsed?.records)
        ? parsed.records.map(normalizeRecord).filter(Boolean) as DecentraderDelayRecord[]
        : []
    };
  } catch {
    return { records: [] };
  }
}

function writeDecentraderDelayHistory(history: DelayHistoryFile): void {
  const file = decentraderDelayHistoryFile();
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporaryFile = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporaryFile, JSON.stringify(history));
  fs.renameSync(temporaryFile, file);
}

export function buildDecentraderDelayRecord(input: {
  signature: string;
  emailType: DecentraderDelayEmailType;
  sideCounts: string;
  intrusionTimestamp: string;
  intrusionTimestampNl: string;
  smtpSentAt: string;
}): DecentraderDelayRecord | undefined {
  const intrusionMs = intrusionTimestampMs(input.intrusionTimestamp);
  const smtpSentMs = Date.parse(input.smtpSentAt);
  if (!Number.isFinite(intrusionMs) || !Number.isFinite(smtpSentMs)) return undefined;

  const rawDelayMinutes = ((smtpSentMs as number) - (intrusionMs as number)) / 60_000;
  if (rawDelayMinutes < -1) return undefined;

  const delayMinutes = Math.max(0, rawDelayMinutes);
  const delayCandles1h = delayMinutes / 60;
  return {
    id: `${input.emailType}|${input.signature}|${input.smtpSentAt}`,
    signature: input.signature,
    emailType: input.emailType,
    sideCounts: input.sideCounts,
    intrusionTimestamp: input.intrusionTimestamp,
    intrusionTimestampNl: input.intrusionTimestampNl,
    smtpSentAt: input.smtpSentAt,
    delayMinutes,
    delayCandles1h,
    completedCandles1h: Math.floor(delayCandles1h + 1e-9)
  };
}

export function appendDecentraderDelayRecord(record: DecentraderDelayRecord): void {
  const history = readDecentraderDelayHistory();
  const records = [...history.records, record].slice(-delayHistoryMaxRecords());
  writeDecentraderDelayHistory({ updatedAt: record.smtpSentAt, records });
}

export function summarizeDecentraderDelayRecords(
  records: DecentraderDelayRecord[]
): DecentraderDelayStats {
  if (!records.length) return { count: 0 };

  const minutes = records.map((record) => record.delayMinutes);
  const completedCandles = records.map((record) => record.completedCandles1h);
  return {
    count: records.length,
    minMinutes: Math.min(...minutes),
    averageMinutes: minutes.reduce((sum, value) => sum + value, 0) / minutes.length,
    maxMinutes: Math.max(...minutes),
    minCompletedCandles1h: Math.min(...completedCandles),
    averageCompletedCandles1h:
      completedCandles.reduce((sum, value) => sum + value, 0) / completedCandles.length,
    maxCompletedCandles1h: Math.max(...completedCandles)
  };
}

export function decentraderDelaySnapshot(): DecentraderDelaySnapshot {
  const history = readDecentraderDelayHistory();
  const normal = history.records.filter((record) => record.emailType === 'normal');
  const filtered = history.records.filter((record) => record.emailType === 'filtered');
  return {
    updatedAt: history.updatedAt,
    totalRecords: history.records.length,
    firstSmtpSentAt: history.records[0]?.smtpSentAt,
    lastSmtpSentAt: history.records[history.records.length - 1]?.smtpSentAt,
    stats: {
      all: summarizeDecentraderDelayRecords(history.records),
      normal: summarizeDecentraderDelayRecords(normal),
      filtered: summarizeDecentraderDelayRecords(filtered)
    },
    recent: history.records.slice(-RECENT_RECORDS).reverse(),
    records: history.records.slice().reverse()
  };
}
