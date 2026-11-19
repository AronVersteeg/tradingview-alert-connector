import fs from 'node:fs';
import path from 'node:path';
import type { DecentraderRow } from '../services/decentraderGapMonitor';
import { PURPLE_LINE_SETUPS } from '../services/purpleLineSetup';
import { btcGreenRetestSignals, buildBtcGreenRetestFromRows } from './btcGreenRetest';

const HOUR = 3_600_000;
const MAX_HOURS = 26_304;
const MIN_OVERLAP_HOURS = 24;
const ARCHIVE_PATH = path.resolve(__dirname, '../../research-data/btc-green-retest-history.json');
export const BTC_GREEN_HISTORY_VERSION = 1;
export const BTC_GREEN_HISTORY_WARMUP_HOURS = 8760;
type Context = ReturnType<typeof btcGreenRetestSignals>;
type Series = { context: Context; rows: DecentraderRow[] };
type Direction = 'long' | 'short';
type Candle = [open: number, high: number, low: number, close: number];
export type BtcGreenHistoryArchive = {
  version: number; model: string; priceStepUsd: number; cohortWindowHours: number;
  from: number; through: number; dydxFrom: number;
  dydx: (Candle | null)[]; greens: [closedAt: number, direction: Direction, edge: number, close: number][];
  yellows: [closedAt: number, direction: Direction][];
};

const isHour = (at: number) => Number.isSafeInteger(at) && at > 0 && at % HOUR === 0;
const isDirection = (value: unknown) => value === 'long' || value === 'short';
const isPrice = (value: number) => Number.isFinite(value) && value > 0;

export function decodeBtcGreenHistory(file: BtcGreenHistoryArchive, now = Date.now()): Series {
  const setup = PURPLE_LINE_SETUPS['BTC-USD'];
  if (file.version !== BTC_GREEN_HISTORY_VERSION || file.model !== setup.modelVersion || file.priceStepUsd !== setup.priceStepUsd ||
      file.cohortWindowHours !== BTC_GREEN_HISTORY_WARMUP_HOURS || ![file.from, file.through, file.dydxFrom].every(isHour) ||
      file.through > now || file.through <= file.from + HOUR || file.dydxFrom > file.from ||
      (file.through - file.dydxFrom) / HOUR > MAX_HOURS || file.dydx.length !== (file.through - file.dydxFrom) / HOUR) {
    throw new Error('BTC Green history archive metadata or coverage is invalid.');
  }
  for (const events of [file.greens, file.yellows]) {
    let previous = file.from + HOUR;
    for (const [at, direction] of events) {
      if (!isHour(at) || at <= previous || at > file.through || !isDirection(direction)) throw new Error('Invalid BTC Green event history.');
      previous = at;
    }
  }
  const rows = file.dydx.flatMap((candle, i) => {
    if (candle === null) return [];
    const [openRef, highRef, lowRef, closeRef] = candle;
    if (![openRef, highRef, lowRef, closeRef].every(isPrice) || highRef < Math.max(openRef, closeRef) || lowRef > Math.min(openRef, closeRef)) {
      throw new Error('Invalid dYdX candle in BTC Green history.');
    }
    return [{ timestamp: new Date(file.dydxFrom + i * HOUR).toISOString(), openRef, highRef, lowRef, closeRef,
      ohlc4: (openRef + highRef + lowRef + closeRef) / 4, fractalCandleSource: 'dydx-1h' }];
  });
  const signals = file.greens.map(([at, direction, edge, close]) => {
    if (![edge, close].every(isPrice)) throw new Error('Invalid BTC Green signal prices.');
    return { time: (at - HOUR) / 1000, closedAt: at / 1000, direction, edge, close, setup: 'green-line' as const };
  });
  return { rows, context: { from: file.from, through: file.through, signals,
    yellows: file.yellows.map(([at, direction]) => ({ at, direction })) } };
}

let archive: Series | null | undefined;
export function readBtcGreenHistory(): Series | undefined {
  if (archive === undefined) {
    try { archive = decodeBtcGreenHistory(JSON.parse(fs.readFileSync(ARCHIVE_PATH, 'utf8'))); }
    catch (error) { archive = null; console.warn('BTC Green retest archive unavailable:', (error as Error).message); }
  }
  return archive || undefined;
}

function extendHistory(history: Series, recent: Series): Series | undefined {
  const { context: past } = history, { context: live } = recent;
  // The first recent frame is only a baseline; no predecessor was loaded for it.
  const firstKnown = Math.max(past.from, live.from) + 2 * HOUR;
  if (live.through <= past.through || past.through - firstKnown < (MIN_OVERLAP_HOURS - 1) * HOUR) return;
  const agrees = (a: string[], b: string[]) => a.length === b.length && a.every((value, i) => value === b[i]);
  const inOverlap = (at: number) => at >= firstKnown && at <= past.through;
  const greenKeys = (c: Context) => c.signals.filter(s => inOverlap(s.closedAt * 1000))
    .map(s => `${s.closedAt}|${s.direction}|${s.edge}|${s.close}`);
  const yellowKeys = (c: Context) => c.yellows.filter(y => inOverlap(y.at)).map(y => `${y.at}|${y.direction}`);
  if (!agrees(greenKeys(past), greenKeys(live)) || !agrees(yellowKeys(past), yellowKeys(live))) return;

  const byHour = new Map(recent.rows.map(row => [Date.parse(row.timestamp), row]));
  const newRows: DecentraderRow[] = [];
  let through = past.through;
  while (through < live.through) {
    const row = byHour.get(through);
    if (!row) break;
    newRows.push(row);
    through += HOUR;
  }
  if (!newRows.length) return;
  const inExtension = (at: number) => at > past.through && at <= through;
  return { rows: [...history.rows, ...newRows], context: { from: past.from, through,
    signals: [...past.signals, ...live.signals.filter(s => inExtension(s.closedAt * 1000))],
    yellows: [...past.yellows, ...live.yellows.filter(y => inExtension(y.at))] } };
}

export function buildBtcGreenRetestWithHistory(history: Series | undefined, recent: Series | undefined) {
  if (!history && !recent) throw new Error('No BTC Green retest data available.');
  const extended = history && recent && extendHistory(history, recent);
  const selected = extended || history || recent!;
  const result = buildBtcGreenRetestFromRows(selected.context, selected.rows);
  const lastCheckedHour = result.incompleteAt ?? result.through;
  const covered = new Set(result.candles.map(row => row.time * 1000)
    .filter(at => at >= result.from && at < lastCheckedHour));
  return { ...result, missingDydxHours: (lastCheckedHour - result.from) / HOUR - covered.size,
    historyCoverage: extended ? 'archive-plus-recent' : history ? 'archive-only' : 'recent-only',
    reconstructedThrough: history?.context.through };
}
