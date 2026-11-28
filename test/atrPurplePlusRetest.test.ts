import { atrPurplePlusSignals, buildAtrPurplePlusWithHistory, buildGreenRetestWithHistory, readRetestHistory } from '../src/research/greenRetestHistory';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const HOUR = 3_600_000, base = Date.UTC(2026, 9, 1);
const at = (offset: number) => base + offset * HOUR;
const signal = (offset: number, direction: 'long' | 'short') => ({ closedAt: at(offset) / 1000, direction });
const loss = (offset: number, direction: 'long' | 'short') =>
  ({ signalAt: at(offset - 2), closedAt: at(offset), status: 'CLOSED', grossPnl: -2, direction });

test('takes the first Purple of the same direction only when the losing exit candle has closed', () => {
  const purples = [signal(2, 'long'), signal(5, 'short'), signal(6, 'long'), signal(7, 'long'), signal(8, 'long'),
    signal(10, 'long'), signal(11, 'short'), signal(12, 'short'), signal(13, 'long')];
  const baseline = [loss(6, 'long'), loss(8, 'short'), loss(9, 'short'),
    { ...loss(9, 'long'), status: 'OPEN' }, { ...loss(11, 'long'), grossPnl: 2 }];
  expect(atrPurplePlusSignals(purples, baseline)).toEqual([signal(7, 'long'), signal(11, 'short')]);
  expect(atrPurplePlusSignals(purples.slice(0, 3), baseline)).toEqual([]);
});

test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s ATR Purple + selects from Binance Purple only and keeps the usual trade controls', market => {
  const purple = readRetestHistory(market, 'purple')!;
  const atrPurple = readRetestHistory(market, 'atr-purple')!;
  const parent = buildGreenRetestWithHistory(atrPurple, undefined, 'atr-purple');
  const result = buildAtrPurplePlusWithHistory(purple, atrPurple);
  const candidates = atrPurplePlusSignals(purple.context.signals.filter(signal => signal.closedAt * 1000 < result.through), parent.trades);
  const eligible = new Map(candidates.map(signal => [signal.time * 1000, signal]));
  expect(result).toMatchObject({ market, strategy: 'atr-purple-plus', historyCoverage: 'archive-only', from: parent.from,
    through: parent.incompleteAt ?? parent.through, candidateCount: eligible.size, riskBudgetUsd: 15 });
  expect(result.trades.length).toBeLessThanOrEqual(eligible.size);
  if (market === 'BTC-USD') expect(result.trades.length).toBeGreaterThan(0);
  if (!parent.trades.some(trade => trade.status === 'CLOSED' && (trade.grossPnl ?? 0) < 0)) {
    expect(result.candidateCount).toBe(0);
    expect(result.trades).toEqual([]);
  }
  expect(result.trades.every((trade, i) => {
    const spot = eligible.get(trade.signalAt);
    const previous = result.trades[i - 1];
    return spot?.direction === trade.direction && trade.initialStop === trade.stops[0].price &&
      (!previous || previous.closedAt !== undefined && trade.openedAt > previous.closedAt);
  })).toBe(true);
  expect(result.spotMarkers).toHaveLength(result.trades.length);
  expect(result.spotMarkers.every(marker => eligible.get(marker.time * 1000)?.close === marker.close)).toBe(true);
});

test('extends both source replays together without resetting their previous losses or open positions', () => {
  const purple = readRetestHistory('BTC-USD', 'purple')!;
  const parent = readRetestHistory('BTC-USD', 'atr-purple')!;
  const through = purple.context.through, cutoff = through - 30 * HOUR;
  const archive = (series: typeof purple) => ({ ...series, rows: series.rows.filter(row => Date.parse(row.timestamp) < cutoff),
    context: { ...series.context, through: cutoff,
      signals: series.context.signals.filter(signal => signal.closedAt * 1000 <= cutoff),
      yellows: series.context.yellows.filter(yellow => yellow.at <= cutoff) } });
  const recent = (series: typeof purple) => ({ ...series, context: { ...series.context, from: through - 120 * HOUR } });
  const full = buildAtrPurplePlusWithHistory(purple, parent);
  const joined = buildAtrPurplePlusWithHistory(archive(purple), archive(parent), recent(purple), recent(parent));
  expect(joined).toMatchObject({ through, reconstructedThrough: cutoff, historyCoverage: 'archive-plus-recent',
    trades: full.trades, spotMarkers: full.spotMarkers });
  const unverified = buildAtrPurplePlusWithHistory(archive(purple), archive(parent), recent(purple));
  expect(unverified).toMatchObject({ through: cutoff, historyCoverage: 'archive-only' });
});
