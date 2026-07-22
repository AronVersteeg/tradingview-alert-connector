import fs from 'fs';
import path from 'path';

import {
  decentralizedDomCollector,
  DomMinuteRecord
} from './decentralizedDomCollector';
import { DecentraderDelayRecord } from './decentraderDelayHistory';

type Direction = 'long' | 'short';
type CandleColor = 'green' | 'red' | 'flat' | 'unknown';

type StudyFrame = {
  t: string;
  price?: number;
  candleColor?: CandleColor;
};

type StudyGap = {
  left: number;
  right: number;
  width?: number;
};

type StudyAlert = {
  signature: string;
  timestamp: string;
  timestampNl?: string;
  price?: number;
  sideCounts?: string;
  gap?: StudyGap;
  previousGap?: StudyGap;
  intrusionCandleReview?: any;
};

type CoinGlassObservation = {
  observedAt: string;
  frameTimestamp?: string;
  currentPrice: number;
  gap?: StudyGap;
  levels: Array<{
    key?: string;
    side: 'buy' | 'sell' | 'unknown';
    price: number;
    volumeUsd: number;
    startedAt?: number;
  }>;
};

export type IntrusionDomWindow = {
  from: string;
  to: string;
  coverageMinutes: number;
  startPrice?: number;
  endPrice?: number;
  rawPriceReturnPct?: number;
  directionalPriceReturnPct?: number;
  rawTakerDeltaUsd: number;
  directionalTakerDeltaUsd: number;
  rawBookPressureUsd: number;
  directionalBookPressureUsd: number;
  averageImbalance25Bps?: number;
  directionalImbalance25Bps?: number;
  largestTradeUsd: number;
  bidNetChangeUsd: number;
  askNetChangeUsd: number;
};

type CoinGlassStudySnapshot = {
  observedAt: string;
  ageMinutes: number;
  price: number;
  gap?: StudyGap;
  levelCount: number;
  supportCount: number;
  frictionCount: number;
  supportUsd: number;
  frictionUsd: number;
};

type EvidenceComponent = {
  key: string;
  label: string;
  available: boolean;
  passed: boolean;
  value?: number;
};

export type IntrusionDomStudyRecord = {
  version: 1;
  researchVersion?: number;
  signature: string;
  timestamp: string;
  timestampNl: string;
  sideCounts: string;
  direction: Direction;
  alertPrice?: number;
  gap?: StudyGap;
  firstObservedAt: string;
  lastUpdatedAt: string;
  normalEmailSentAt?: string;
  filteredEmailSentAt?: string;
  filtered: boolean;
  candleReview: {
    status: 'PASS' | 'FAIL' | 'PENDING' | 'UNKNOWN';
    expectedColor: CandleColor;
    intrusionColor: CandleColor;
    nextColor: CandleColor;
  };
  dom: {
    pre1h?: IntrusionDomWindow;
    pre2h?: IntrusionDomWindow;
    pre4h?: IntrusionDomWindow;
    pre12h?: IntrusionDomWindow;
    pre24h?: IntrusionDomWindow;
    intrusion1h?: IntrusionDomWindow;
    confirmation1h?: IntrusionDomWindow;
    signal2h?: IntrusionDomWindow;
    delay?: IntrusionDomWindow;
  };
  coinGlass: {
    pre1h?: CoinGlassStudySnapshot;
    pre2h?: CoinGlassStudySnapshot;
    pre4h?: CoinGlassStudySnapshot;
    pre12h?: CoinGlassStudySnapshot;
    pre24h?: CoinGlassStudySnapshot;
    event?: CoinGlassStudySnapshot;
    review?: CoinGlassStudySnapshot;
    frictionRemovedUsd?: number;
    frictionRemovalPct?: number;
    supportRetentionPct?: number;
  };
  outcome: {
    observedHours: number;
    currentDirectionalReturnPct?: number;
    maxFavorablePct?: number;
    maxAdversePct?: number;
    gapEdgeCrossed?: boolean;
  };
  evidence: {
    ruleVersion: 'benchmark-v1';
    score: number;
    available: number;
    validDomPattern: boolean;
    classification: 'INSUFFICIENT' | 'REJECTION' | 'BUILDUP' | 'IMPULSE_CANDIDATE';
    components: EvidenceComponent[];
  };
  hypothesisEntryValid: boolean;
};

type StudyFile = {
  updatedAt?: string;
  historyStartTimestamp?: string;
  records: IntrusionDomStudyRecord[];
};

const MAX_RECORDS = 10_000;
const RECENT_RECORDS = 8;
const RESEARCH_VERSION = 2;
const HOUR_MS = 60 * 60 * 1000;

function finiteNumber(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function timestampMs(timestamp: string): number | undefined {
  const parsed = Date.parse(String(timestamp || '').replace(' ', 'T') + 'Z');
  return Number.isFinite(parsed) ? parsed : undefined;
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

function baseSignature(signature: string): string {
  return String(signature || '').replace(/^FILTERED\|/, '');
}

function directionFromText(sideCounts: string, signature: string): Direction | undefined {
  const text = `${sideCounts} ${signature}`.toLowerCase();
  const hasLeft = text.includes('left edge') || /\|l\|/i.test(signature);
  const hasRight = text.includes('right edge') || /\|s\|/i.test(signature);
  if (hasLeft === hasRight) return undefined;
  if (hasLeft) return 'long';
  if (hasRight) return 'short';
  return undefined;
}

function expectedColor(direction: Direction): CandleColor {
  return direction === 'long' ? 'green' : 'red';
}

function directional(value: number | undefined, direction: Direction): number | undefined {
  if (value === undefined) return undefined;
  return direction === 'long' ? value : -value;
}

function studyFile(): string {
  const explicit = String(process.env.DECENTRADER_INTRUSION_DOM_STUDY_FILE || '').trim();
  if (explicit) return explicit;

  const domDirectory = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  if (domDirectory) {
    return path.join(path.dirname(domDirectory), 'decentrader-intrusion-dom-study.json');
  }

  const stateFile = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim();
  if (stateFile) {
    return path.join(path.dirname(stateFile), 'decentrader-intrusion-dom-study.json');
  }

  return path.join(process.cwd(), 'data', 'decentrader-intrusion-dom-study.json');
}

function readStudy(): StudyFile {
  try {
    const parsed = JSON.parse(fs.readFileSync(studyFile(), 'utf8'));
    return {
      updatedAt: typeof parsed?.updatedAt === 'string' ? parsed.updatedAt : undefined,
      historyStartTimestamp: typeof parsed?.historyStartTimestamp === 'string'
        ? parsed.historyStartTimestamp
        : undefined,
      records: Array.isArray(parsed?.records) ? parsed.records : []
    };
  } catch {
    return { records: [] };
  }
}

function writeStudy(study: StudyFile): void {
  const file = studyFile();
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporaryFile = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporaryFile, JSON.stringify(study));
  fs.renameSync(temporaryFile, file);
}

function recordMid(record: DomMinuteRecord): number | undefined {
  const mids = Object.values(record.venues || {})
    .map((venue) => finiteNumber(venue?.mid))
    .filter((value): value is number => value !== undefined && value > 0);
  if (!mids.length) return undefined;
  return mids.reduce((sum, value) => sum + value, 0) / mids.length;
}

export function aggregateIntrusionDomWindow(
  records: DomMinuteRecord[],
  fromMs: number,
  toMs: number,
  direction: Direction
): IntrusionDomWindow | undefined {
  const selected = records.filter((record) => {
    const at = Date.parse(record.bucketStart);
    return Number.isFinite(at) && at >= fromMs && at < toMs;
  });
  if (!selected.length) return undefined;

  const mids = selected.map(recordMid).filter((value): value is number => value !== undefined);
  const venues = selected.flatMap((record) => Object.values(record.venues || {}).filter(Boolean));
  const sumVenue = (key: keyof NonNullable<DomMinuteRecord['venues']['dydx']>): number =>
    venues.reduce((sum, venue) => sum + (finiteNumber(venue?.[key]) || 0), 0);
  const imbalances = selected
    .map((record) => finiteNumber(record.crossVenue?.consensusImbalance25Bps))
    .filter((value): value is number => value !== undefined);
  const rawTakerDeltaUsd = selected.reduce(
    (sum, record) => sum + (finiteNumber(record.crossVenue?.consensusTakerDeltaUsd) || 0),
    0
  );
  const bidNetChangeUsd = sumVenue('bidAddedUsd') - sumVenue('bidRemovedUsd');
  const askNetChangeUsd = sumVenue('askAddedUsd') - sumVenue('askRemovedUsd');
  const rawBookPressureUsd = bidNetChangeUsd - askNetChangeUsd;
  const rawPriceReturnPct = mids.length > 1 && mids[0] > 0
    ? ((mids[mids.length - 1] / mids[0]) - 1) * 100
    : undefined;
  const averageImbalance25Bps = imbalances.length
    ? imbalances.reduce((sum, value) => sum + value, 0) / imbalances.length
    : undefined;

  return {
    from: iso(fromMs),
    to: iso(toMs),
    coverageMinutes: selected.length,
    startPrice: mids[0],
    endPrice: mids[mids.length - 1],
    rawPriceReturnPct,
    directionalPriceReturnPct: directional(rawPriceReturnPct, direction),
    rawTakerDeltaUsd,
    directionalTakerDeltaUsd: directional(rawTakerDeltaUsd, direction) || 0,
    rawBookPressureUsd,
    directionalBookPressureUsd: directional(rawBookPressureUsd, direction) || 0,
    averageImbalance25Bps,
    directionalImbalance25Bps: directional(averageImbalance25Bps, direction),
    largestTradeUsd: Math.max(0, ...venues.map((venue) => finiteNumber(venue?.largestTradeUsd) || 0)),
    bidNetChangeUsd,
    askNetChangeUsd
  };
}

function nearestObservation(
  observations: CoinGlassObservation[],
  targetMs: number,
  maxAgeMinutes = 120
): CoinGlassObservation | undefined {
  let nearest: CoinGlassObservation | undefined;
  let nearestDistance = Number.POSITIVE_INFINITY;
  for (const observation of observations) {
    const at = Date.parse(observation.observedAt);
    const distance = Math.abs(at - targetMs);
    if (Number.isFinite(at) && distance < nearestDistance) {
      nearest = observation;
      nearestDistance = distance;
    }
  }
  return nearestDistance <= maxAgeMinutes * 60_000 ? nearest : undefined;
}

function summarizeCoinGlass(
  observation: CoinGlassObservation | undefined,
  targetMs: number,
  direction: Direction,
  fallbackGap?: StudyGap
): CoinGlassStudySnapshot | undefined {
  if (!observation) return undefined;
  const gap = fallbackGap || observation.gap;
  if (!gap) return undefined;
  const levels = (observation.levels || []).filter(
    (level) => level.price >= gap.left && level.price <= gap.right
  );
  const supportSide = direction === 'long' ? 'buy' : 'sell';
  const frictionSide = direction === 'long' ? 'sell' : 'buy';
  const support = levels.filter((level) => level.side === supportSide);
  const friction = levels.filter((level) => level.side === frictionSide);
  return {
    observedAt: observation.observedAt,
    ageMinutes: Math.abs(Date.parse(observation.observedAt) - targetMs) / 60_000,
    price: finiteNumber(observation.currentPrice) || 0,
    gap,
    levelCount: levels.length,
    supportCount: support.length,
    frictionCount: friction.length,
    supportUsd: support.reduce((sum, level) => sum + (finiteNumber(level.volumeUsd) || 0), 0),
    frictionUsd: friction.reduce((sum, level) => sum + (finiteNumber(level.volumeUsd) || 0), 0)
  };
}

function candleReviewFor(
  direction: Direction,
  timestamp: string,
  frames: StudyFrame[],
  supplied?: any
): IntrusionDomStudyRecord['candleReview'] {
  const expected = expectedColor(direction);
  const frameIndex = frames.findIndex((frame) => frame.t === timestamp);
  const intrusion = supplied?.intrusionColor || frames[frameIndex]?.candleColor || 'unknown';
  const next = supplied?.nextColor || frames[frameIndex + 1]?.candleColor || 'unknown';
  const status = supplied?.status === 'PASS' || supplied?.status === 'FAIL' || supplied?.status === 'PENDING'
    ? supplied.status
    : intrusion === 'unknown' || next === 'unknown'
      ? 'PENDING'
      : intrusion === expected && next === expected
        ? 'PASS'
        : 'FAIL';
  return {
    status,
    expectedColor: expected,
    intrusionColor: intrusion,
    nextColor: next
  };
}

export function buildIntrusionDomEvidence(
  dom: IntrusionDomStudyRecord['dom'],
  coinGlass: IntrusionDomStudyRecord['coinGlass'],
  direction: Direction = 'long'
): IntrusionDomStudyRecord['evidence'] {
  const event = dom.intrusion1h;
  const confirmation = dom.confirmation1h;
  const cgEvent = coinGlass.event;
  const cgReview = coinGlass.review;
  const rawPriceAtReview = event?.startPrice && cgReview?.price
    ? ((cgReview.price / event.startPrice) - 1) * 100
    : undefined;
  const priceAtReview = directional(rawPriceAtReview, direction);
  const frictionRemoved = coinGlass.frictionRemovalPct;
  const supportRetention = coinGlass.supportRetentionPct;
  const components: EvidenceComponent[] = [
    {
      key: 'event-price',
      label: 'Intrusion price progresses in signal direction',
      available: event?.directionalPriceReturnPct !== undefined && (event.coverageMinutes || 0) >= 30,
      passed: (event?.directionalPriceReturnPct || 0) > 0,
      value: event?.directionalPriceReturnPct
    },
    {
      key: 'event-taker-flow',
      label: 'Aggressive flow agrees with signal direction',
      available: Boolean(event && event.coverageMinutes >= 30),
      passed: (event?.directionalTakerDeltaUsd || 0) > 0,
      value: event?.directionalTakerDeltaUsd
    },
    {
      key: 'event-book-pressure',
      label: 'Visible book change agrees with signal direction',
      available: Boolean(event && event.coverageMinutes >= 30),
      passed: (event?.directionalBookPressureUsd || 0) > 0,
      value: event?.directionalBookPressureUsd
    },
    {
      key: 'confirmation-price',
      label: 'Following hour confirms price direction',
      available: confirmation?.directionalPriceReturnPct !== undefined && (confirmation.coverageMinutes || 0) >= 30,
      passed: (confirmation?.directionalPriceReturnPct || 0) > 0,
      value: confirmation?.directionalPriceReturnPct
    },
    {
      key: 'cg-friction-removal',
      label: 'CoinGlass friction clears while price advances',
      available: Boolean(cgEvent && cgReview && frictionRemoved !== undefined && priceAtReview !== undefined),
      passed: (frictionRemoved || 0) >= 0.5 && (priceAtReview || 0) > 0,
      value: frictionRemoved
    },
    {
      key: 'cg-support-retention',
      label: 'CoinGlass support remains present',
      available: Boolean(cgEvent && cgReview && supportRetention !== undefined),
      passed: (supportRetention || 0) >= 0.75,
      value: supportRetention
    }
  ];
  const available = components.filter((component) => component.available).length;
  const score = components.filter((component) => component.available && component.passed).length;
  const validDomPattern = available >= 5 && score / available >= 5 / 6;
  const classification = available < 4
    ? 'INSUFFICIENT'
    : validDomPattern
      ? 'IMPULSE_CANDIDATE'
      : score / available >= 0.5
        ? 'BUILDUP'
        : 'REJECTION';
  return {
    ruleVersion: 'benchmark-v1',
    score,
    available,
    validDomPattern,
    classification,
    components
  };
}

function outcomeFor(
  records: DomMinuteRecord[],
  timestamp: number,
  direction: Direction,
  alertPrice: number | undefined,
  gap: StudyGap | undefined,
  nowMs: number
): IntrusionDomStudyRecord['outcome'] {
  const mids = records
    .filter((record) => Date.parse(record.bucketStart) >= timestamp)
    .map(recordMid)
    .filter((value): value is number => value !== undefined);
  const base = alertPrice || mids[0];
  if (!base || !mids.length) {
    return { observedHours: Math.max(0, (nowMs - timestamp) / HOUR_MS) };
  }
  const returns = mids.map((mid) => directional(((mid / base) - 1) * 100, direction) || 0);
  const edge = direction === 'long' ? gap?.right : gap?.left;
  const edgeCrossed = edge === undefined
    ? undefined
    : direction === 'long'
      ? mids.some((mid) => mid >= edge)
      : mids.some((mid) => mid <= edge);
  return {
    observedHours: Math.max(0, (Math.min(nowMs, timestamp + 24 * HOUR_MS) - timestamp) / HOUR_MS),
    currentDirectionalReturnPct: returns[returns.length - 1],
    maxFavorablePct: Math.max(...returns),
    maxAdversePct: Math.min(...returns),
    gapEdgeCrossed: edgeCrossed
  };
}

function initialRecord(input: {
  signature: string;
  timestamp: string;
  timestampNl?: string;
  sideCounts?: string;
  direction: Direction;
  now: string;
}): IntrusionDomStudyRecord {
  return {
    version: 1,
    researchVersion: RESEARCH_VERSION,
    signature: input.signature,
    timestamp: input.timestamp,
    timestampNl: input.timestampNl || input.timestamp,
    sideCounts: input.sideCounts || `${input.direction === 'long' ? 'left' : 'right'} edge`,
    direction: input.direction,
    firstObservedAt: input.now,
    lastUpdatedAt: input.now,
    filtered: false,
    candleReview: {
      status: 'UNKNOWN',
      expectedColor: expectedColor(input.direction),
      intrusionColor: 'unknown',
      nextColor: 'unknown'
    },
    dom: {},
    coinGlass: {},
    outcome: { observedHours: 0 },
    evidence: {
      ruleVersion: 'benchmark-v1',
      score: 0,
      available: 0,
      validDomPattern: false,
      classification: 'INSUFFICIENT',
      components: []
    },
    hypothesisEntryValid: false
  };
}

export function refreshIntrusionDomStudy(input: {
  alerts?: StudyAlert[];
  delayRecords?: DecentraderDelayRecord[];
  frames?: StudyFrame[];
  coinGlassObservations?: CoinGlassObservation[];
  historyStartTimestamp?: string;
  now?: string;
}): void {
  const now = input.now || new Date().toISOString();
  const nowMs = Date.parse(now);
  const frames = input.frames || [];
  const observations = input.coinGlassObservations || [];
  const study = readStudy();
  const bySignature = new Map(study.records.map((record) => [record.signature, record]));
  const refreshSignatures = new Set<string>();

  for (const alert of input.alerts || []) {
    const signature = baseSignature(alert.signature);
    const direction = directionFromText(alert.sideCounts || '', signature);
    if (!direction || !alert.timestamp) continue;
    const existing = bySignature.get(signature);
    const record = existing || initialRecord({
      signature,
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      sideCounts: alert.sideCounts,
      direction,
      now
    });
    record.timestampNl = alert.timestampNl || record.timestampNl;
    record.sideCounts = alert.sideCounts || record.sideCounts;
    record.alertPrice = finiteNumber(alert.price) || record.alertPrice;
    record.gap = alert.gap || alert.previousGap || record.gap;
    record.candleReview = candleReviewFor(direction, record.timestamp, frames, alert.intrusionCandleReview);
    bySignature.set(signature, record);
    if (!existing) refreshSignatures.add(signature);
  }

  for (const delay of input.delayRecords || []) {
    const signature = baseSignature(delay.signature);
    const direction = directionFromText(delay.sideCounts, signature);
    if (!direction || !delay.intrusionTimestamp) continue;
    const existing = bySignature.get(signature);
    const record = existing || initialRecord({
      signature,
      timestamp: delay.intrusionTimestamp,
      timestampNl: delay.intrusionTimestampNl,
      sideCounts: delay.sideCounts,
      direction,
      now
    });
    if (delay.emailType === 'normal') {
      if (!record.normalEmailSentAt || Date.parse(delay.smtpSentAt) < Date.parse(record.normalEmailSentAt)) {
        record.normalEmailSentAt = delay.smtpSentAt;
        refreshSignatures.add(signature);
      }
    } else {
      record.filtered = true;
      if (!record.filteredEmailSentAt || Date.parse(delay.smtpSentAt) < Date.parse(record.filteredEmailSentAt)) {
        record.filteredEmailSentAt = delay.smtpSentAt;
        refreshSignatures.add(signature);
      }
    }
    record.candleReview = candleReviewFor(direction, record.timestamp, frames, record.filtered ? {
      status: 'PASS',
      intrusionColor: expectedColor(direction),
      nextColor: expectedColor(direction)
    } : undefined);
    bySignature.set(signature, record);
    if (!existing) refreshSignatures.add(signature);
  }

  const records = [...bySignature.values()].sort((a, b) => a.timestamp.localeCompare(b.timestamp));
  const refreshCutoff = nowMs - 30 * HOUR_MS;
  for (const record of records) {
    const at = timestampMs(record.timestamp);
    if (at === undefined) continue;
    const shouldRefresh = at >= refreshCutoff
      || refreshSignatures.has(record.signature)
      || record.researchVersion !== RESEARCH_VERSION;
    if (!shouldRefresh) continue;
    const historyTo = Math.min(nowMs, at + 24 * HOUR_MS);
    const domHistory = decentralizedDomCollector.getHistory({
      from: iso(at - 24 * HOUR_MS),
      to: iso(historyTo),
      maxPoints: 5_000
    }).records;
    const reviewAt = Date.parse(record.filteredEmailSentAt || record.normalEmailSentAt || now);
    const pre1hObservation = nearestObservation(observations, at - HOUR_MS);
    const pre2hObservation = nearestObservation(observations, at - 2 * HOUR_MS);
    const pre4hObservation = nearestObservation(observations, at - 4 * HOUR_MS);
    const pre12hObservation = nearestObservation(observations, at - 12 * HOUR_MS);
    const pre24hObservation = nearestObservation(observations, at - 24 * HOUR_MS);
    const eventObservation = nearestObservation(observations, at);
    const reviewObservation = nearestObservation(observations, reviewAt);
    record.gap = record.gap || eventObservation?.gap || reviewObservation?.gap;
    record.alertPrice = record.alertPrice || frames.find((frame) => frame.t === record.timestamp)?.price || eventObservation?.currentPrice;
    record.candleReview = candleReviewFor(record.direction, record.timestamp, frames, record.filtered ? {
      status: 'PASS',
      intrusionColor: expectedColor(record.direction),
      nextColor: expectedColor(record.direction)
    } : undefined);
    record.dom = {
      pre1h: aggregateIntrusionDomWindow(domHistory, at - HOUR_MS, at, record.direction),
      pre2h: aggregateIntrusionDomWindow(domHistory, at - 2 * HOUR_MS, at, record.direction),
      pre4h: aggregateIntrusionDomWindow(domHistory, at - 4 * HOUR_MS, at, record.direction),
      pre12h: aggregateIntrusionDomWindow(domHistory, at - 12 * HOUR_MS, at, record.direction),
      pre24h: aggregateIntrusionDomWindow(domHistory, at - 24 * HOUR_MS, at, record.direction),
      intrusion1h: aggregateIntrusionDomWindow(domHistory, at, at + HOUR_MS, record.direction),
      confirmation1h: aggregateIntrusionDomWindow(domHistory, at + HOUR_MS, at + 2 * HOUR_MS, record.direction),
      signal2h: aggregateIntrusionDomWindow(domHistory, at, at + 2 * HOUR_MS, record.direction),
      delay: aggregateIntrusionDomWindow(domHistory, at, Math.max(at + 60_000, reviewAt), record.direction)
    };
    const pre1hCg = summarizeCoinGlass(pre1hObservation, at - HOUR_MS, record.direction, record.gap);
    const pre2hCg = summarizeCoinGlass(pre2hObservation, at - 2 * HOUR_MS, record.direction, record.gap);
    const pre4hCg = summarizeCoinGlass(pre4hObservation, at - 4 * HOUR_MS, record.direction, record.gap);
    const pre12hCg = summarizeCoinGlass(pre12hObservation, at - 12 * HOUR_MS, record.direction, record.gap);
    const pre24hCg = summarizeCoinGlass(pre24hObservation, at - 24 * HOUR_MS, record.direction, record.gap);
    const eventCg = summarizeCoinGlass(eventObservation, at, record.direction, record.gap);
    const reviewCg = summarizeCoinGlass(reviewObservation, reviewAt, record.direction, record.gap);
    const frictionRemovedUsd = eventCg && reviewCg
      ? Math.max(0, eventCg.frictionUsd - reviewCg.frictionUsd)
      : undefined;
    record.coinGlass = {
      pre1h: pre1hCg,
      pre2h: pre2hCg,
      pre4h: pre4hCg,
      pre12h: pre12hCg,
      pre24h: pre24hCg,
      event: eventCg,
      review: reviewCg,
      frictionRemovedUsd,
      frictionRemovalPct: eventCg && eventCg.frictionUsd > 0 && frictionRemovedUsd !== undefined
        ? frictionRemovedUsd / eventCg.frictionUsd
        : eventCg && reviewCg && eventCg.frictionUsd === 0
          ? 0
          : undefined,
      supportRetentionPct: eventCg && reviewCg && eventCg.supportUsd > 0
        ? reviewCg.supportUsd / eventCg.supportUsd
        : undefined
    };
    record.outcome = outcomeFor(domHistory, at, record.direction, record.alertPrice, record.gap, nowMs);
    record.evidence = buildIntrusionDomEvidence(record.dom, record.coinGlass, record.direction);
    record.hypothesisEntryValid = record.candleReview.status === 'PASS' && record.evidence.validDomPattern;
    record.researchVersion = RESEARCH_VERSION;
    record.lastUpdatedAt = now;
  }

  writeStudy({
    updatedAt: now,
    historyStartTimestamp: [input.historyStartTimestamp, study.historyStartTimestamp]
      .filter((value): value is string => Boolean(value))
      .sort()[0],
    records: records.slice(-MAX_RECORDS)
  });
}

function average(records: IntrusionDomStudyRecord[], getter: (record: IntrusionDomStudyRecord) => number | undefined): number | undefined {
  const values = records.map(getter).filter((value): value is number => value !== undefined);
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : undefined;
}

export function intrusionDomStudySnapshot(): any {
  const study = readStudy();
  const classified = study.records.filter((record) => record.evidence.classification !== 'INSUFFICIENT');
  const filtered = classified.filter((record) => record.filtered);
  const normalOnly = classified.filter((record) => !record.filtered);
  return {
    enabled: true,
    observeOnly: true,
    entryGateActive: false,
    hypothesis: 'valid entry = candle filter PASS + valid DOM pattern',
    ruleVersion: 'benchmark-v1',
    file: studyFile(),
    updatedAt: study.updatedAt,
    historyStartTimestamp: study.historyStartTimestamp,
    totalRecords: study.records.length,
    classifiedRecords: classified.length,
    comparison: {
      filtered: {
        count: filtered.length,
        averageScore: average(filtered, (record) => record.evidence.score),
        averageDirectionalReturnPct: average(filtered, (record) => record.outcome.currentDirectionalReturnPct),
        validDomPatterns: filtered.filter((record) => record.evidence.validDomPattern).length
      },
      normalOnly: {
        count: normalOnly.length,
        averageScore: average(normalOnly, (record) => record.evidence.score),
        averageDirectionalReturnPct: average(normalOnly, (record) => record.outcome.currentDirectionalReturnPct),
        validDomPatterns: normalOnly.filter((record) => record.evidence.validDomPattern).length
      }
    },
    recent: study.records.slice(-RECENT_RECORDS).reverse(),
    history: study.records.slice().reverse().map((record) => ({
      signature: record.signature,
      timestamp: record.timestamp,
      timestampNl: record.timestampNl,
      sideCounts: record.sideCounts,
      direction: record.direction,
      alertPrice: record.alertPrice,
      gap: record.gap,
      filtered: record.filtered,
      normalEmailSentAt: record.normalEmailSentAt,
      filteredEmailSentAt: record.filteredEmailSentAt,
      candleReview: record.candleReview,
      pre1h: record.dom.pre1h,
      pre2h: record.dom.pre2h,
      pre4h: record.dom.pre4h,
      pre12h: record.dom.pre12h,
      pre24h: record.dom.pre24h,
      event: record.dom.intrusion1h,
      confirmation: record.dom.confirmation1h,
      signal2h: record.dom.signal2h,
      delayWindow: record.dom.delay,
      coinGlass: {
        pre1h: record.coinGlass.pre1h,
        pre2h: record.coinGlass.pre2h,
        pre4h: record.coinGlass.pre4h,
        pre12h: record.coinGlass.pre12h,
        pre24h: record.coinGlass.pre24h,
        eventObservedAt: record.coinGlass.event?.observedAt,
        reviewObservedAt: record.coinGlass.review?.observedAt,
        eventLevelCount: record.coinGlass.event?.levelCount,
        reviewLevelCount: record.coinGlass.review?.levelCount,
        eventFrictionUsd: record.coinGlass.event?.frictionUsd,
        reviewFrictionUsd: record.coinGlass.review?.frictionUsd,
        frictionRemovalPct: record.coinGlass.frictionRemovalPct,
        eventSupportUsd: record.coinGlass.event?.supportUsd,
        reviewSupportUsd: record.coinGlass.review?.supportUsd,
        supportRetentionPct: record.coinGlass.supportRetentionPct
      },
      outcome: record.outcome,
      evidence: record.evidence,
      hypothesisEntryValid: record.hypothesisEntryValid
    }))
  };
}

export function intrusionDomStudyResumeTimestamp(): string | undefined {
  const records = readStudy().records;
  const latest = records[records.length - 1];
  const latestMs = latest ? timestampMs(latest.timestamp) : undefined;
  return latestMs === undefined ? undefined : iso(Math.max(0, latestMs - HOUR_MS));
}

export function intrusionDomStudyHistoryStartTimestamp(): string | undefined {
  return readStudy().historyStartTimestamp;
}
