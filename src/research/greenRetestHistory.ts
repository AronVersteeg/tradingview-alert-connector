import fs from 'node:fs';
import path from 'node:path';
import type { DecentraderRow } from '../services/decentraderGapMonitor';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../services/purpleLineSetup';
import { buildGreenRetestFromRows, type RetestContext } from './greenRetest';
import { ATR_ENTRY_PERIOD, atrEntryReading, atrEntryPasses, priorSpotAtr, type SpotAtrRange } from '../services/atrEntry';
import { filterGreenContinuation } from '../services/greenContinuationRule';

const HOUR = 3_600_000;
const MAX_HOURS = 26_304;
const MIN_OVERLAP_HOURS = 24;
export const GREEN_HISTORY_VERSION = 1;
export const GREEN_HISTORY_WARMUP_HOURS = 8760;
type Context = RetestContext;
export type RetestMode = 'green' | 'purple' | 'atr' | 'atr-purple';
type LossCandidate = { signalAt: number; status: string; grossPnl?: number };

const closedRetestLoss = (trade: LossCandidate) => trade.status === 'CLOSED' && typeof trade.grossPnl === 'number' &&
  Number.isFinite(trade.grossPnl) && trade.grossPnl < 0;

export function closedRetestLossSignalTimes(trades: readonly LossCandidate[]): number[] {
  return trades.filter(closedRetestLoss).map(trade => trade.signalAt / 1000);
}

export function retestTradeMarkers(trades: readonly (LossCandidate & { direction: 'long' | 'short' })[]) {
  // Marker closedAt is the signal candle's close (entry boundary), not the trade exit.
  return trades.map(trade => ({ time: trade.signalAt / 1000, closedAt: (trade.signalAt + HOUR) / 1000,
    direction: trade.direction, loss: closedRetestLoss(trade) }));
}

type AtrMarker = { time: number; ratio: number };
type Series = { context: Context; rows: DecentraderRow[]; atrMarkers?: AtrMarker[] };
type Direction = 'long' | 'short';
type Candle = [open: number, high: number, low: number, close: number];
export type GreenHistoryArchive = {
  market?: PurpleLineMarket;
  version: number; model: string; priceStepUsd: number; cohortWindowHours: number;
  from: number; through: number; dydxFrom: number;
  dydx: (Candle | null)[]; greens: [closedAt: number, direction: Direction, edge: number, close: number][];
  yellows: [closedAt: number, direction: Direction][];
};

const isHour = (at: number) => Number.isSafeInteger(at) && at > 0 && at % HOUR === 0;
const isDirection = (value: unknown) => value === 'long' || value === 'short';
const isPrice = (value: number) => Number.isFinite(value) && value > 0;

export function greenHistoryPath(market: PurpleLineMarket): string {
  return path.resolve(__dirname, `../../research-data/${PURPLE_LINE_SETUPS[market].asset.toLowerCase()}-green-retest-history.json`);
}

export function purpleHistoryPath(market: PurpleLineMarket): string {
  return path.resolve(__dirname, `../../research-data/${PURPLE_LINE_SETUPS[market].asset.toLowerCase()}-purple-retest-signals.json`);
}

export function atrHistoryPath(market: PurpleLineMarket): string {
  return path.resolve(__dirname, `../../research-data/${PURPLE_LINE_SETUPS[market].asset.toLowerCase()}-atr-retest-ranges.json`);
}

type AtrHistory = { version: 1; market: PurpleLineMarket; model: string; from: number; through: number;
  ranges: [closedAt: number, spotAtr: number][] };

function filterAtrSeries(series: Series, atrAt: (signalAt: number) => number | undefined): Series {
  const byHour = new Map(series.rows.map(row => [Date.parse(row.timestamp), row]));
  const atrMarkers: AtrMarker[] = [];
  const signals = series.context.signals.filter(signal => {
    const at = signal.time * 1000;
    const reading = atrEntryReading(at, signal.direction, atrAt(at), byHour.get(at));
    if (!reading || !atrEntryPasses(reading)) return false;
    atrMarkers.push({ time: signal.time, ratio: reading.ratio });
    return true;
  });
  return { ...series, context: { ...series.context, signals }, atrMarkers };
}

export function decodeAtrHistory(green: Series, file: AtrHistory, purple?: PurpleHistory): Series {
  const { context } = green;
  if (file.version !== 1 || file.market !== context.market || file.model !== PURPLE_LINE_SETUPS[context.market].modelVersion ||
      file.from !== context.from || file.through !== context.through || !Array.isArray(file.ranges) ||
      file.ranges.length !== context.signals.length || file.ranges.some(([at, range], i) =>
        at !== context.signals[i].closedAt * 1000 || !isPrice(range))) {
    throw new Error(`Incompatible ${context.market} ATR archive.`);
  }
  const ranges = new Map(file.ranges);
  return filterAtrSeries(purple ? decodePurpleHistory(green, purple) : green, at => ranges.get(at + HOUR));
}

export function recentAtrRetest(series: Series, frames: SpotAtrRange[]): Series {
  const frameByHour = new Map(frames.map(frame => [frame.startedAtMs, frame]));
  const from = series.context.from + (ATR_ENTRY_PERIOD + 1) * HOUR;
  return filterAtrSeries({ ...series, context: { ...series.context, from,
    signals: series.context.signals.filter(signal => signal.time * 1000 >= from) } }, at => priorSpotAtr(frameByHour, at));
}

export type PurpleHistory = { version: 1; market: PurpleLineMarket; model: string; from: number; through: number;
  entries: [closedAt: number, dailyLevels: number[]][] };

export function decodePurpleHistory(green: Series, file: PurpleHistory): Series {
  const { context } = green;
  if (file.version !== 1 || file.market !== context.market || file.model !== PURPLE_LINE_SETUPS[context.market].modelVersion ||
      file.from !== context.from || file.through !== context.through || !Array.isArray(file.entries)) {
    throw new Error(`Incompatible ${context.market} Purple archive.`);
  }
  const greenEntries = new Map(context.signals.map(signal => [signal.closedAt * 1000, signal]));
  const yellows = new Set(context.yellows.map(row => `${row.at}|${row.direction}`));
  let previous = context.from;
  const signals = file.entries.map(([at, dailyLevels]) => {
    const signal = greenEntries.get(at);
    if (at <= previous || !signal || !yellows.has(`${at}|${signal.direction}`) || !Array.isArray(dailyLevels) ||
        !dailyLevels.length || dailyLevels.some((price, index) => !isPrice(price) || index > 0 && price <= dailyLevels[index - 1])) {
      throw new Error(`Invalid ${context.market} Purple archive signal.`);
    }
    previous = at;
    return { ...signal, dailyLevels };
  });
  return { rows: green.rows, context: { ...context, signals } };
}

export function decodeGreenHistory(market: PurpleLineMarket, file: GreenHistoryArchive, now = Date.now()): Series {
  const setup = PURPLE_LINE_SETUPS[market];
  if ((file.market ?? 'BTC-USD') !== market || file.version !== GREEN_HISTORY_VERSION ||
      file.model !== setup.modelVersion || file.priceStepUsd !== setup.priceStepUsd ||
      file.cohortWindowHours !== GREEN_HISTORY_WARMUP_HOURS || ![file.from, file.through, file.dydxFrom].every(isHour) ||
      file.through > now || file.through <= file.from + HOUR || file.dydxFrom > file.from ||
      (file.through - file.dydxFrom) / HOUR > MAX_HOURS || file.dydx.length !== (file.through - file.dydxFrom) / HOUR) {
    throw new Error(`${market} Green history archive metadata or coverage is invalid.`);
  }
  for (const events of [file.greens, file.yellows]) {
    let previous = file.from + HOUR;
    for (const [at, direction] of events) {
      if (!isHour(at) || at <= previous || at > file.through || !isDirection(direction)) throw new Error(`Invalid ${market} Green event history.`);
      previous = at;
    }
  }
  const rows = file.dydx.flatMap((candle, i) => {
    if (candle === null) return [];
    const [openRef, highRef, lowRef, closeRef] = candle;
    if (![openRef, highRef, lowRef, closeRef].every(isPrice) || highRef < Math.max(openRef, closeRef) || lowRef > Math.min(openRef, closeRef)) {
      throw new Error(`Invalid dYdX candle in ${market} Green history.`);
    }
    return [{ timestamp: new Date(file.dydxFrom + i * HOUR).toISOString(), openRef, highRef, lowRef, closeRef,
      ohlc4: (openRef + highRef + lowRef + closeRef) / 4, fractalCandleSource: 'dydx-1h' }];
  });
  const signals = file.greens.map(([at, direction, edge, close]) => {
    if (![edge, close].every(isPrice)) throw new Error(`Invalid ${market} Green signal prices.`);
    return { time: (at - HOUR) / 1000, closedAt: at / 1000, direction, edge, close, setup: 'green-line' as const };
  });
  return { rows, context: { market, from: file.from, through: file.through, signals,
    yellows: file.yellows.map(([at, direction]) => ({ at, direction })) } };
}

let archive: { market: PurpleLineMarket; series: Series } | undefined;
const unavailableMarkets = new Set<PurpleLineMarket>();
export function readGreenHistory(market: PurpleLineMarket): Series | undefined {
  if (archive?.market === market) return archive.series;
  if (unavailableMarkets.has(market)) return;
  try {
    const series = decodeGreenHistory(market, JSON.parse(fs.readFileSync(greenHistoryPath(market), 'utf8')));
    archive = { market, series };
    return series;
  } catch (error) {
    unavailableMarkets.add(market);
    console.warn(`${market} Green retest archive unavailable:`, (error as Error).message);
  }
}

export function readRetestHistory(market: PurpleLineMarket, strategy: RetestMode): Series | undefined {
  const green = readGreenHistory(market);
  if (strategy === 'green' || !green) return green;
  try {
    const usesPurple = strategy === 'purple' || strategy === 'atr-purple';
    const purple = usesPurple ? JSON.parse(fs.readFileSync(purpleHistoryPath(market), 'utf8')) as PurpleHistory : undefined;
    if (usesPurple && !purple) throw new Error(`Missing ${market} Purple archive.`);
    if (strategy === 'purple') return decodePurpleHistory(green, purple!);
    return decodeAtrHistory(green, JSON.parse(fs.readFileSync(atrHistoryPath(market), 'utf8')), purple);
  } catch (error) { console.warn(`${market} ${strategy} retest archive unavailable:`, (error as Error).message); }
}

function extendHistory(history: Series, recent: Series): Series | undefined {
  const { context: past } = history, { context: live } = recent;
  if (past.market !== live.market) throw new Error('Cannot combine Green retest history for different markets.');
  // The first recent frame is only a baseline; no predecessor was loaded for it.
  const firstKnown = Math.max(past.from, live.from) + 2 * HOUR;
  if (live.through <= past.through || past.through - firstKnown < (MIN_OVERLAP_HOURS - 1) * HOUR) return;
  const agrees = (a: string[], b: string[]) => a.length === b.length && a.every((value, i) => value === b[i]);
  const inOverlap = (at: number) => at >= firstKnown && at <= past.through;
  const greenKeys = (c: Context) => c.signals.filter(s => inOverlap(s.closedAt * 1000))
    .map(s => `${s.closedAt}|${s.direction}|${s.edge}|${s.close}|${s.dailyLevels?.join(',') ?? ''}`);
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
  return { rows: [...history.rows, ...newRows], ...(history.atrMarkers && { atrMarkers: [...history.atrMarkers,
    ...(recent.atrMarkers ?? []).filter(marker => inExtension(marker.time * 1000 + HOUR))] }),
    context: { market: past.market, from: past.from, through,
    signals: [...past.signals, ...live.signals.filter(s => inExtension(s.closedAt * 1000))],
    yellows: [...past.yellows, ...live.yellows.filter(y => inExtension(y.at))] } };
}

export function buildGreenRetestWithHistory(history: Series | undefined, recent: Series | undefined, strategy: RetestMode = 'green',
  continuation?: { archive?: Context; recent?: Context; bridge?: boolean }) {
  if (!history && !recent) throw new Error('No Green retest data available.');
  const extended = history && recent && extendHistory(history, recent);
  const selected = extended || history || recent!;
  let { context, atrMarkers } = selected;
  if (continuation && (strategy === 'green' || strategy === 'atr')) {
    const archived = continuation.archive?.signals ?? [];
    const latest = continuation.recent?.signals ?? [];
    const cutoff = history ? history.context.through / 1000 : 0;
    const signals = extended
      ? [...filterGreenContinuation(context.signals.filter(row => row.closedAt <= cutoff), archived),
        ...filterGreenContinuation(context.signals.filter(row => row.closedAt > cutoff),
          continuation.bridge ? [...archived, ...latest.filter(row => row.closedAt > cutoff)] : latest)]
      : filterGreenContinuation(context.signals, history ? archived : latest);
    const hours = new Set(signals.map(signal => signal.time));
    context = { ...context, signals };
    atrMarkers = atrMarkers?.filter(marker => hours.has(marker.time));
  }
  const result = buildGreenRetestFromRows(context, selected.rows);
  const lastCheckedHour = result.incompleteAt ?? result.through;
  const covered = new Set(result.candles.map(row => row.time * 1000)
    .filter(at => at >= result.from && at < lastCheckedHour));
  return { ...result, strategy, atrMarkers, missingDydxHours: (lastCheckedHour - result.from) / HOUR - covered.size,
    historyCoverage: extended ? 'archive-plus-recent' : history ? 'archive-only' : 'recent-only',
    reconstructedThrough: history?.context.through };
}
