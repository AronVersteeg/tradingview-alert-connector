const breakouts = require('../public/btc-spot-chart/gap-weekly-breakouts.js');
const HOUR = 3600, start = Date.parse('2026-10-19T00:00:00Z') / 1000;
const iso = (seconds: number) => new Date(seconds * 1000).toISOString();
const source = { symbol: 'BTCUSDT', venue: 'Binance Spot' };
const candles = () => [
  [100, 101, 99, 100], [100, 101, 99, 100], [100, 128, 99, 125], [125, 132, 123, 130],
  [130, 132, 72, 75], [75, 77, 65, 70], [70, 102, 68, 100], [100, 155, 99, 150]
].map(([open, high, low, close], index) => ({ time: start + index * HOUR, open, high, low, close, closed: true }));
const timeline = () => ({ ...source, records: Array.from({ length: 9 }, (_, index) => {
  const i = index - 1;
  return { candleTime: start + i * HOUR, availableAt: start + (i + 1) * HOUR,
    lower: i >= 4 ? 60 : 85, upper: i >= 2 ? 140 : 115 };
}) });
const weeklyModel = () => ({ ...source, evaluatedThrough: iso(start + 8 * HOUR), levels: [
  { id: 'W|high', timeframe: 'W', type: 'HIGH', price: 120, confirmedAt: iso(start - .001), broken: true, brokenTime: start + 2 * HOUR },
  { id: 'W|low', timeframe: 'W', type: 'LOW', price: 80, confirmedAt: iso(start - .001), broken: true, brokenTime: start + 4 * HOUR },
  { id: 'W|future', timeframe: 'W', type: 'HIGH', price: 145, confirmedAt: iso(start + 8 * HOUR - .001), broken: false }
] });
const now = (start + 8 * HOUR) * 1000;

describe.each(['Weekly', 'Daily'])('Joint closed-candle gap and %s breakout markers, display only', (name) => {
  const timeframe = name === 'Weekly' ? 'W' : 'D', levelsKey = name === 'Weekly' ? 'weeklyLevels' : 'dailyLevels';
  const buildGapWeeklyBreakouts = breakouts[`buildGap${name}Breakouts`];
  const GapWeeklyBreakoutPrimitive = breakouts[`Gap${name}BreakoutPrimitive`];
  const model = () => ({ ...weeklyModel(), levels: weeklyModel().levels.map(level => ({ ...level,
    id: level.id.replace('W|', `${timeframe}|`), timeframe })) });
  test('marks one long and one short, using the pre-candle box rather than its expanded replacement', () => {
    const result = buildGapWeeklyBreakouts(candles(), timeline(), model(), now);
    expect(result.signals.map((signal: any) => [signal.time, signal.direction])).toEqual([
      [start + 2 * HOUR, 'long'], [start + 4 * HOUR, 'short']
    ]);
    expect(result.signals[0]).toMatchObject({ closedAt: start + 3 * HOUR, gap: { upper: 115 }, [levelsKey]: [{ price: 120 }] });
    expect(result.signals[1].gap.lower).toBe(85);
  });

  test('requires both fresh close crossings, not merely being beyond an already-broken fractal line', () => {
    const levels = model(); levels.levels[0].brokenTime = start;
    expect(buildGapWeeklyBreakouts(candles(), timeline(), levels, now).signals.map((s: any) => s.direction)).toEqual(['short']);
    const rows = candles(); rows[1] = { ...rows[1], high: 123, close: 122 };
    expect(buildGapWeeklyBreakouts(rows, timeline(), model(), now).signals.some((s: any) => s.time === start + 2 * HOUR)).toBe(false);
  });

  test('wick-only excursions, exact equality and open candles do not produce signals', () => {
    for (const [close, closed] of [[120, true], [114, true], [125, false]] as const) {
      const rows = candles(); rows[2] = { ...rows[2], close, closed };
      expect(buildGapWeeklyBreakouts(rows, timeline(), model(), now).signals.some((s: any) => s.time === rows[2].time)).toBe(false);
    }
    const equal = timeline(); equal.records[2].upper = 125;
    expect(buildGapWeeklyBreakouts(candles(), equal, model(), now).signals.some((s: any) => s.direction === 'long')).toBe(false);
  });

  test('a fractal must already be confirmed when the signal candle opens', () => {
    const input = model();
    input.levels[0].confirmedAt = iso(start + 2 * HOUR);
    expect(buildGapWeeklyBreakouts(candles(), timeline(), input, now).signals.some((s: any) => s.direction === 'long')).toBe(false);
    input.levels[0].confirmedAt = iso(start + 2 * HOUR - .001);
    expect(buildGapWeeklyBreakouts(candles(), timeline(), input, now).signals.some((s: any) => s.direction === 'long')).toBe(true);
  });

  test('does not span missing candle/gap hours, null gaps or a stale fractal evaluation cutoff', () => {
    const rows = candles().filter((_, index) => index !== 1);
    expect(buildGapWeeklyBreakouts(rows, timeline(), model(), now).signals.map((s: any) => s.direction)).toEqual(['short']);
    const gaps = timeline(); gaps.records.splice(2, 1);
    expect(buildGapWeeklyBreakouts(candles(), gaps, model(), now).signals.map((s: any) => s.direction)).toEqual(['short']);
    const noGap: any = timeline(); noGap.records[4].lower = noGap.records[4].upper = null;
    expect(buildGapWeeklyBreakouts(candles(), noGap, model(), now).signals.map((s: any) => s.direction)).toEqual(['long']);
    expect(buildGapWeeklyBreakouts(candles(), timeline(), { ...model(), evaluatedThrough: iso(start + 2 * HOUR) }, now).signals).toEqual([]);
  });

  test('earlier markers are unchanged by future candles, future fractals, or current-level labels', () => {
    const early = buildGapWeeklyBreakouts(candles().slice(0, 4), timeline(), model(), (start + 4 * HOUR) * 1000).signals;
    const later = buildGapWeeklyBreakouts(candles(), timeline(), model(), now).signals;
    expect(later.filter((s: any) => s.closedAt <= start + 4 * HOUR)).toEqual(early);
    expect(later).toHaveLength(2); // No marker from the fractal confirmed at the future candle's own close.
  });

  test('uses only the selected timeframe highs for longs and lows for shorts, with one line for multiple levels', () => {
    const input = model();
    input.levels.push({ ...input.levels[0], id: 'W|another-high', price: 121 });
    input.levels.push({ ...input.levels[0], id: 'M|high', timeframe: 'M' });
    input.levels.push({ ...input.levels[0], id: 'other|high', timeframe: timeframe === 'W' ? 'D' : 'W' });
    const result = buildGapWeeklyBreakouts(candles(), timeline(), input, now);
    expect(result.signals).toHaveLength(2); expect(result.signals[0][levelsKey]).toHaveLength(2);
  });

  test('rejects different venues/markets and malformed input instead of drawing guessed markers', () => {
    expect(() => buildGapWeeklyBreakouts(candles(), { ...timeline(), symbol: 'ETHUSDT' }, model(), now)).toThrow('source');
    expect(() => buildGapWeeklyBreakouts(candles(), timeline(), { ...model(), venue: 'Binance Futures' }, now)).toThrow('source');
    const gaps = timeline(); gaps.records[0].availableAt += 1;
    expect(() => buildGapWeeklyBreakouts(candles(), gaps, model(), now)).toThrow('history');
    const rows = candles(); rows[1] = rows[0];
    expect(() => buildGapWeeklyBreakouts(rows, timeline(), model(), now)).toThrow('candle');
  });

  test('draws thin vertical pane lines in the timeframe color, supports toggling and skips off-screen candles', () => {
    const layer = new GapWeeklyBreakoutPrimitive(), ctx: any = {};
    for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke']) ctx[name] = jest.fn();
    const requestUpdate = jest.fn();
    layer.attached({ chart: { timeScale: () => ({ timeToCoordinate: (time: number) => time === start + 2 * HOUR ? 22.3 : null }) }, requestUpdate });
    layer.setData(buildGapWeeklyBreakouts(candles(), timeline(), model(), now).signals);
    const draw = () => layer.paneViews()[0].renderer().draw({ useMediaCoordinateSpace: (render: any) => render({ context: ctx, mediaSize: { width: 300, height: 400 } }) });
    draw(); expect(ctx.lineWidth).toBe(1); expect(ctx.strokeStyle).toBe(name === 'Weekly' ? '#ff9800' : '#fff59d');
    expect(ctx.moveTo).toHaveBeenCalledWith(22.5, 0); expect(ctx.lineTo).toHaveBeenCalledWith(22.5, 400);
    expect(ctx.stroke).toHaveBeenCalledTimes(1);
    layer.setVisible(false); ctx.stroke.mockClear(); draw(); expect(ctx.stroke).not.toHaveBeenCalled();
    layer.setVisible(true); draw(); expect(ctx.stroke).toHaveBeenCalledTimes(1);
    layer.detached(); ctx.stroke.mockClear(); draw(); expect(ctx.stroke).not.toHaveBeenCalled();
    expect(requestUpdate).toHaveBeenCalled();
  });
});
