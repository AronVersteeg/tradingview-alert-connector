import type { DecentraderRow } from './decentraderGapMonitor';

const HOUR = 3_600_000;
export const ATR_ENTRY_PERIOD = 14;
export const ATR_ENTRY_MULTIPLIER = 2;

export type SpotAtrRange = { startedAtMs: number; high: number; low: number; close: number };
export type AtrEntryReading = { spotAtr: number; directionalBody: number; ratio: number };
const positive = (price: number) => Number.isFinite(price) && price > 0;

export function priorSpotAtr(byHour: ReadonlyMap<number, SpotAtrRange>, signalAt: number): number | undefined {
  if (!Number.isSafeInteger(signalAt) || signalAt % HOUR !== 0) return;
  let previous = byHour.get(signalAt - (ATR_ENTRY_PERIOD + 1) * HOUR);
  let total = 0;
  for (let at = signalAt - ATR_ENTRY_PERIOD * HOUR; at < signalAt; at += HOUR) {
    const candle = byHour.get(at);
    if (!previous || previous.startedAtMs !== at - HOUR || !positive(previous.close) || !candle ||
        candle.startedAtMs !== at || ![candle.high, candle.low, candle.close].every(positive) ||
        candle.high < candle.close || candle.low > candle.close) return;
    total += Math.max(candle.high - candle.low, Math.abs(candle.high - previous.close), Math.abs(candle.low - previous.close));
    previous = candle;
  }
  const atr = total / ATR_ENTRY_PERIOD;
  return positive(atr) ? atr : undefined;
}

export function atrEntryReading(signalAt: number, direction: 'long' | 'short', spotAtr: number | undefined,
  candle: DecentraderRow | undefined): AtrEntryReading | undefined {
  if (!candle || candle.fractalCandleSource !== 'dydx-1h' || Date.parse(candle.timestamp) !== signalAt ||
      spotAtr === undefined || !positive(spotAtr) || !positive(candle.openRef) || !positive(candle.closeRef)) return;
  const directionalBody = direction === 'long' ? candle.closeRef - candle.openRef : candle.openRef - candle.closeRef;
  return { spotAtr, directionalBody, ratio: directionalBody / spotAtr };
}

export function atrEntryPasses(reading: AtrEntryReading): boolean {
  return Number.isFinite(reading.ratio) && reading.ratio >= ATR_ENTRY_MULTIPLIER;
}
