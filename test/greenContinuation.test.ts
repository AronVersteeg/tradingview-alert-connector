import { archivedContinuation, canBridgeContinuation, greenContinuationMarkers, hasGreenContinuation } from '../src/research/greenContinuation';
import { buildGreenRetestWithHistory, readRetestHistory } from '../src/research/greenRetestHistory';
import type { RetestContext, RetestSignal } from '../src/research/greenRetest';
import * as greenRetest from '../src/research/greenRetest';
import * as purpleRetest from '../src/research/purpleRetest';
import { filterGreenContinuation } from '../src/services/greenContinuationRule';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const HOUR = 3600, BASE = Date.UTC(2026, 0, 1) / 1000;
const at = (hour: number) => BASE + hour * HOUR;
function row(hour: number, direction: 'long' | 'short'): RetestSignal {
  return { time: at(hour - 1), closedAt: at(hour), direction, edge: 100, close: 102, setup: 'green-line' };
}
const context = (from: number, through: number, signals: RetestSignal[], market: PurpleLineMarket = 'BTC-USD'): RetestContext =>
  ({ market, from: at(from) * 1000, through: at(through) * 1000, signals, yellows: [] });

test('a Purple only unlocks later candles; a later opposite Purple changes the direction', () => {
  const candidates = [row(5, 'long'), row(9, 'short'), row(12, 'long'), row(13, 'short'), row(15, 'long'),
    row(17, 'short'), row(3, 'long'), row(18, 'long')];
  const purples = [row(5, 'long'), row(11, 'long'), row(13, 'short'), row(18, 'long')];
  expect(filterGreenContinuation(candidates, [])).toEqual([]);
  expect(filterGreenContinuation(candidates, purples)).toEqual([row(12, 'long'), row(17, 'short')]);
});

test('bridge an older Purple only across verified overlapping history for the same pair', () => {
  const archive = { green: context(0, 50, [row(15, 'long')]), purple: context(0, 50, [row(10, 'long')]) };
  const live = { green: context(24, 65, [row(55, 'long'), row(61, 'short')]), purple: context(24, 65, [row(60, 'short')]) };
  expect(greenContinuationMarkers(archive, live).signals).toEqual([row(15, 'long'), row(55, 'long'), row(61, 'short')]);
  expect(canBridgeContinuation(archive.purple, { ...live.purple, market: 'ETH-USD' })).toBe(false);
  const gapped = { green: { ...live.green, from: at(51) * 1000 }, purple: { ...live.purple, from: at(51) * 1000 } };
  expect(greenContinuationMarkers(archive, gapped).signals).toEqual([row(15, 'long'), row(61, 'short')]);
  const conflicting = { ...live, purple: { ...live.purple, signals: [row(39, 'short'), row(60, 'short')] } };
  expect(canBridgeContinuation(archive.purple, conflicting.purple)).toBe(false);
  expect(greenContinuationMarkers(archive, conflicting).signals).toEqual([row(15, 'long'), row(61, 'short')]);
});

test('live gate requires both the verified candidate and the prior Purple, sharing the chronology read', async () => {
  const market = 'WLD-USD';
  const last = row(61, 'short');
  const exact = context(20, 61, [last], market);
  const raw = jest.spyOn(greenRetest, 'greenRetestSignals').mockReturnValue(exact);
  const purple = jest.spyOn(purpleRetest, 'recentPurpleRetest').mockResolvedValue(context(20, 61, [row(60, 'short')], market));
  try {
    const payload = { modelVersion: 'test', frames: [{ startedAtMs: at(20) * 1000 }, { startedAtMs: at(60) * 1000 }] };
    await expect(hasGreenContinuation(market, payload, last, Date.now())).resolves.toBe(true);
    await expect(hasGreenContinuation(market, payload, { ...last, close: last.close + 1 }, Date.now())).resolves.toBe(false);
    expect(raw).toHaveBeenCalledTimes(1);
    expect(purple).toHaveBeenCalledTimes(1);
  } finally {
    raw.mockRestore();
    purple.mockRestore();
  }
});

test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s overlay and retest use the same archived direction gate', market => {
  const archive = archivedContinuation(market)!;
  const qualified = greenContinuationMarkers(archive);
  expect(qualified.signals.length).toBeGreaterThan(0);
  expect(qualified.signals.length).toBeLessThan(archive.green.signals.length);
  const green = buildGreenRetestWithHistory(readRetestHistory(market, 'green'), undefined, 'green', { archive: archive.purple });
  expect(green.candidateCount).toBe(qualified.signals.length);
  expect(green.trades.every(trade => qualified.signals.some(row => row.time * 1000 === trade.signalAt))).toBe(true);
  const atr = buildGreenRetestWithHistory(readRetestHistory(market, 'atr'), undefined, 'atr', { archive: archive.purple });
  expect(atr.atrMarkers).toHaveLength(atr.candidateCount);
  expect(atr.atrMarkers!.every(marker => qualified.signals.some(row => row.time === marker.time))).toBe(true);
  for (const mode of ['purple', 'atr-purple'] as const) {
    const original = readRetestHistory(market, mode)!;
    const replay = buildGreenRetestWithHistory(original, undefined, mode, {});
    expect(replay.candidateCount).toBe(original.context.signals.length);
  }
});
