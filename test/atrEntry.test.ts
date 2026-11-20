import { atrEntryPasses, atrEntryReading, priorSpotAtr } from '../src/services/atrEntry';
import { buildGreenRetestWithHistory, readRetestHistory, recentAtrRetest } from '../src/research/greenRetestHistory';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const HOUR = 3_600_000, signalAt = Date.UTC(2026, 10, 20, 6);

test('pre-signal ATR uses 14 complete ranges, previous closes and no signal wick', () => {
  const rows = new Map(Array.from({ length: 16 }, (_, i) => {
    const startedAtMs = signalAt + (i - 15) * HOUR;
    return [startedAtMs, { startedAtMs, high: i === 15 ? 1000 : 103, low: 101, close: i === 0 ? 100 : 102 }];
  }));
  expect(priorSpotAtr(rows, signalAt)).toBe(29 / 14);
  rows.delete(signalAt - 6 * HOUR);
  expect(priorSpotAtr(rows, signalAt)).toBeUndefined();
});

test.each([['long', 100, 110], ['short', 110, 100]] as const)('2x is inclusive, directional and exact-hour for %s', (direction, openRef, closeRef) => {
  const candle = { timestamp: new Date(signalAt).toISOString(), fractalCandleSource: 'dydx-1h', openRef, closeRef };
  expect(atrEntryPasses(atrEntryReading(signalAt, direction, 5, candle)!)).toBe(true);
  expect(atrEntryPasses(atrEntryReading(signalAt, direction === 'long' ? 'short' : 'long', 5, candle)!)).toBe(false);
  expect(atrEntryReading(signalAt + HOUR, direction, 5, candle)).toBeUndefined();
  expect(atrEntryReading(signalAt, direction, 5, { ...candle, fractalCandleSource: 'binance-spot' })).toBeUndefined();
});

test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s ATR markers and retest use exactly the same archived Green subset', market => {
  const green = readRetestHistory(market, 'green')!;
  const atr = readRetestHistory(market, 'atr')!;
  expect(atr.context.yellows).toEqual(green.context.yellows);
  expect(atr.context.signals).toHaveLength(atr.atrMarkers!.length);
  expect(atr.context.signals.length).toBeGreaterThan(0);
  expect(atr.context.signals.length).toBeLessThan(green.context.signals.length);
  expect(atr.atrMarkers!.every((marker, i) => marker.ratio >= 2 && marker.time === atr.context.signals[i].time)).toBe(true);
  const replay = buildGreenRetestWithHistory(atr, undefined, 'atr');
  expect(replay.strategy).toBe('atr');
  expect(replay.trades.length).toBeGreaterThan(0);
  expect(replay.trades.every(t => atr.atrMarkers!.some(mark => mark.time === t.signalAt / 1000))).toBe(true);
});

test('recent ATR scores exactly the Green candle and preserves later Yellow events', () => {
  const frame = (i: number) => ({ startedAtMs: signalAt + i * HOUR, low: 99, high: 101, close: 100 });
  const frames = Array.from({ length: 19 }, (_, i) => frame(i - 16));
  const signal = (i: number) => ({ time: (signalAt + i * HOUR) / 1000, closedAt: (signalAt + (i + 1) * HOUR) / 1000,
    direction: 'long' as const, setup: 'green-line' as const, edge: 99, close: 100 });
  const series = { context: { market: 'BTC-USD' as const, from: frames[0].startedAtMs, through: signalAt + 3 * HOUR,
    signals: [-2, 0, 1].map(signal), yellows: [{ at: signalAt + 3 * HOUR, direction: 'long' as const }] },
    rows: [[-2, 180], [0, 104], [1, 102]].map(([i, closeRef]) => ({ timestamp: new Date(signalAt + i * HOUR).toISOString(),
      fractalCandleSource: 'dydx-1h', openRef: 100, closeRef })) };
  const scored = recentAtrRetest(series, frames);
  expect(scored.context.signals).toEqual([signal(0)]);
  expect(scored.atrMarkers).toEqual([{ time: signalAt / 1000, ratio: 2 }]);
  expect(scored.context.yellows).toBe(series.context.yellows);
  expect(recentAtrRetest(series, frames.filter(row => row.startedAtMs !== signalAt - HOUR)).atrMarkers).toEqual([]);
});
