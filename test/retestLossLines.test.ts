import { archivedContinuation } from '../src/research/greenContinuation';
import { buildGreenRetestWithHistory, closedRetestLossSignalTimes, readRetestHistory, type RetestMode } from '../src/research/greenRetestHistory';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const { GapWeeklyBreakoutPrimitive } = require('../public/btc-spot-chart/gap-weekly-breakouts.js');
const hour = 3600;
const start = Date.UTC(2026, 10, 19) / 1000;

test('returns the entry signal candle only for closed losses, not open losses, breakeven or incomplete trades', () => {
  const trade = (offset: number, status: string, grossPnl: number | undefined) =>
    ({ signalAt: (start + offset * hour) * 1000, status, grossPnl, closedAt: (start + 8 * hour) * 1000 });
  expect(closedRetestLossSignalTimes([
    trade(0, 'CLOSED', -1), trade(1, 'OPEN', -80), trade(2, 'INCOMPLETE', undefined),
    trade(3, 'CLOSED', 0), trade(4, 'CLOSED', 1), trade(5, 'CLOSED', -Infinity), trade(6, 'CLOSED', -2)
  ])).toEqual([start, start + 6 * hour]);
});

test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s loss hours match the retest for all four strategies', market => {
  for (const mode of ['green', 'purple', 'atr', 'atr-purple'] as RetestMode[]) {
    const series = readRetestHistory(market, mode)!;
    const archive = mode === 'green' || mode === 'atr' ? archivedContinuation(market)?.purple : undefined;
    const { trades } = buildGreenRetestWithHistory(series, undefined, mode, { archive });
    const marked = new Set(closedRetestLossSignalTimes(trades));
    expect(trades.length).toBeGreaterThan(0);
    for (const trade of trades) {
      expect(marked.has(trade.signalAt / 1000)).toBe(trade.status === 'CLOSED' && trade.grossPnl! < 0);
    }
  }
});

test.each(['#00ff00', '#ba68c8', '#238554', '#f18ebc'])('%s replaces only the losing strokes and keeps the strategy color', color => {
  const base = new GapWeeklyBreakoutPrimitive(color, 2);
  const losses = new GapWeeklyBreakoutPrimitive(color, 2, [7, 4, 1, 4]);
  const times = new Set([start + hour, start + 2 * hour]);
  const chart = { timeScale: () => ({ timeToCoordinate: (time: number) => (time - start) / hour * 24 + 8 }) };
  const target = { useMediaCoordinateSpace: (render: (scope: any) => void) => render({ context: ctx, mediaSize: { width: 150, height: 120 } }) };
  const ctx: any = {};
  for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke']) ctx[name] = jest.fn();
  base.attached({ chart }); losses.attached({ chart });
  base.setData([{ time: start }, { time: start + hour }]);
  base.setOmittedLineTimes(times);
  losses.setData([...times].map(time => ({ time })));

  base.draw(target);
  expect(ctx.setLineDash).toHaveBeenLastCalledWith([]);
  expect(ctx.moveTo.mock.calls).toEqual([[8.5, 0]]);
  expect(base.signals).toHaveLength(2);
  ctx.moveTo.mockClear();
  losses.draw(target);
  expect(ctx.moveTo.mock.calls).toEqual([[32.5, 0], [56.5, 0]]);
  expect(ctx.setLineDash).toHaveBeenLastCalledWith([7, 4, 1, 4]);
  expect(ctx.strokeStyle).toBe(color);
  losses.setVisible(false);
  ctx.stroke.mockClear();
  losses.draw(target);
  expect(ctx.stroke).not.toHaveBeenCalled();
});
