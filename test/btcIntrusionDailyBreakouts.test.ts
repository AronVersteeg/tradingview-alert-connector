export {};
const intrusionBreakouts = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const { compactIntrusionHistory, buildGapIntrusionMarkers, buildIntrusionDailyBreakouts, buildIntrusionWeeklyBreakouts, IntrusionDailyBreakoutPrimitive } = intrusionBreakouts;
const HOUR = 3600, start = Date.parse('2026-10-19T00:00:00Z') / 1000;
const iso = (seconds: number) => new Date(seconds * 1000).toISOString();
const now = (start + 8 * HOUR) * 1000;
const alert = (hour: number) => iso(start + hour * HOUR).slice(0, 19).replace('T', ' ');
const source = { url: 'https://www.decentrader.com/liquidity-maps/?coin=btc', method: 'getOHLCHourlyCalculations' };
const payload = () => ({ source, intrusionDomStudy: { history: [
  { timestamp: alert(2), direction: 'long', normalEmailSentAt: iso(start + 7 * HOUR) },
  { timestamp: alert(4), direction: 'short', filteredEmailSentAt: iso(start + 7 * HOUR) }
] }, delayHistory: { records: [
  { intrusionTimestamp: alert(2), signature: `${alert(2)}|L|10|100:1`, smtpSentAt: iso(start + 7 * HOUR) }
] } });
const candles = () => [
  [100, 101, 99, 100], [100, 101, 99, 100], [100, 128, 99, 125], [125, 132, 123, 130],
  [130, 132, 72, 75], [75, 77, 65, 70], [70, 102, 68, 100], [100, 155, 99, 150]
].map(([open, high, low, close], i) => ({ time: start + i * HOUR, open, high, low, close, closed: true }));
const model = () => ({ symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: iso(start + 8 * HOUR), levels: [
  { id: 'D|high', timeframe: 'D', type: 'HIGH', price: 120, confirmedAt: iso(start - .001), broken: true, brokenTime: start + 2 * HOUR },
  { id: 'D|low', timeframe: 'D', type: 'LOW', price: 80, confirmedAt: iso(start - .001), broken: true, brokenTime: start + 4 * HOUR }
] });

describe('Standalone Binance Spot gap intrusion markers', () => {
  test.each(['BTC', 'ETH', 'SOL', 'ZEC'])('%s marks a saved intrusion only on a fresh same-hour gap edge close break', asset => {
    const market = `${asset}-USD`;
    const data = {
      source: { market, sourceStatuses: [{ name: `Binance Spot ${asset}USDT 1H` }] },
      intrusionBenchmarks: { history: [
        { timestamp: alert(0), direction: 'short' },
        { timestamp: alert(1), direction: 'mixed' },
        { timestamp: alert(2), direction: 'long' },
        { timestamp: alert(4), direction: 'short' },
        { timestamp: alert(5), direction: 'short' },
        { timestamp: alert(6) },
        { timestamp: alert(7), direction: 'long' },
        { smtpSentAt: iso(start + 6 * HOUR), direction: 'long' }
      ] },
      delayHistory: { records: [
        { intrusionTimestamp: alert(2), signature: `${market}|${alert(2)}|L|10|100:1` },
        { intrusionTimestamp: alert(3), signature: `${market}|${alert(3)}|S|10|130:1`, smtpSentAt: iso(start + 6 * HOUR) }
      ] }
    };
    const history = compactIntrusionHistory(data, now, market, { includeGapHours: true });
    const { gapHours, ...directionalHistory } = history;
    expect(directionalHistory).toEqual(compactIntrusionHistory(data, now, market));
    const chartCandles = candles().map(c => c.time === start + HOUR ? { ...c, close: 103, high: 104 } : c);
    const timeline = {symbol: `${asset}USDT`, venue: 'Binance Spot',
      records: [[90,102], [90,120], [85,140], [80,150], [80,130], [65,120], [60,140]].map(([lower,upper], i) =>
        ({candleTime:start+i*HOUR, availableAt:start+(i+1)*HOUR, lower, upper}))};
    const expected = [[1, 'long', 102], [2, 'long', 120], [4, 'short', 80]].map(([hour, direction, edge]) =>
      ({time:start+Number(hour)*HOUR, closedAt:start+(Number(hour)+1)*HOUR, direction, edge}));
    expect(buildGapIntrusionMarkers(chartCandles, history, timeline, now - 1, market)).toEqual(expected);
    expect(buildGapIntrusionMarkers(chartCandles.filter(c => c.time !== start + 3*HOUR), history, timeline, now - 1, market)).toEqual(expected.slice(0, 2));
    expect(buildGapIntrusionMarkers(chartCandles, history, undefined, now, market)).toEqual([]);
    expect(buildGapIntrusionMarkers(chartCandles, history, timeline, now, 'OTHER-USD')).toEqual([]);
    expect(buildGapIntrusionMarkers(chartCandles, { ...history, source: 'decentrader-recorded-intrusions' }, timeline, now, market)).toEqual([]);
    expect(() => buildGapIntrusionMarkers(chartCandles, history, {...timeline, symbol:'OTHERUSDT'}, now, market)).toThrow('source');
    const wickOnly = chartCandles.map(c => c.time === start + 2*HOUR ? {...c, close:120} : c);
    expect(buildGapIntrusionMarkers(wickOnly, history, timeline, now - 1, market)).toEqual([expected[0], expected[2]]);
  });
});

describe('Decentrader intrusion alert time and same-candle Daily close break, display only', () => {
  test('normalizes UTC alert-open times and deduplicates normal, filtered and study records without using mail time', () => {
    const history = compactIntrusionHistory(payload(), now);
    expect(history.records).toEqual([
      { time: start + 2 * HOUR, alertTime: iso(start + 2 * HOUR), direction: 'long' },
      { time: start + 4 * HOUR, alertTime: iso(start + 4 * HOUR), direction: 'short' }
    ]);
    expect(JSON.stringify(history)).not.toContain('smtpSentAt');
    expect(history.timeBasis).toBe('alert-candle-open-utc');
  });

  test('accepts native BTC Public V2 recorded intrusions without reading Decentrader or SMTP timestamps', () => {
    const native = { source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H', ok: true }] },
      intrusionBenchmarks: { history: payload().intrusionDomStudy.history } };
    const history = compactIntrusionHistory(native, now, 'BTC-USD');
    expect(history.source).toBe('binance-spot-recorded-intrusions');
    const result = buildIntrusionDailyBreakouts(candles(), history, model(), now, 'BTC-USD');
    expect(result.signals.map((s: any) => [s.time, s.direction])).toEqual([[start + 2 * HOUR, 'long'], [start + 4 * HOUR, 'short']]);
    expect(result.intrusionSource).toBe('binance-spot-recorded-intrusions');
  });

  test('ignores ambiguous sides, missing/non-hour/zone-less times, future records and mail-only records', () => {
    const data: any = payload(); data.intrusionDomStudy.history = [
      { timestamp: alert(2), sideCounts: '1 left edge, 1 right edge' },
      { timestamp: '2026-10-19T02:30:00Z', direction: 'long' },
      { timestamp: '2026-10-19T02:00:00', direction: 'long' },
      { timestamp: alert(9), direction: 'long' },
      { smtpSentAt: iso(start + 2 * HOUR), direction: 'long' }
    ]; data.delayHistory.records = [];
    const history = compactIntrusionHistory(data, now);
    expect(history.records).toEqual([]); expect(history.ignoredRecords).toBe(5);
  });

  test('rejects Public V2 or missing history instead of treating current histograms as saved intrusions', () => {
    expect(() => compactIntrusionHistory({ ...payload(), source: { name: 'Binance Spot replica' } }, now)).toThrow('source');
    expect(() => compactIntrusionHistory({ source }, now)).toThrow('missing');
    expect(compactIntrusionHistory({ source, delayHistory: { records: [] } }, now).records).toEqual([]);
  });

  test('marks both directions at alert time, not a later mail candle, without requiring a green-box gap break', () => {
    const result = buildIntrusionDailyBreakouts(candles(), compactIntrusionHistory(payload(), now), model(), now);
    expect(result.signals.map((s: any) => [s.time, s.direction])).toEqual([
      [start + 2 * HOUR, 'long'], [start + 4 * HOUR, 'short']
    ]);
    expect(result.signals[0]).toMatchObject({ intrusion: { alertTime: iso(start + 2 * HOUR) }, dailyLevels: [{ price: 120 }] });
    expect(result.signals[0].gap).toBeUndefined();
  });

  test('requires matching direction on the exact candle, not a neighboring intrusion or Weekly-only break', () => {
    const data: any = payload(); data.intrusionDomStudy.history[0].direction = 'short'; data.delayHistory.records = [];
    expect(buildIntrusionDailyBreakouts(candles(), compactIntrusionHistory(data, now), model(), now).signals).toHaveLength(1);
    data.intrusionDomStudy.history[0] = { timestamp: alert(1), direction: 'long' };
    expect(buildIntrusionDailyBreakouts(candles(), compactIntrusionHistory(data, now), model(), now).signals).toHaveLength(1);
    const fractals = model(); fractals.levels[0].timeframe = 'W';
    expect(buildIntrusionDailyBreakouts(candles(), compactIntrusionHistory(payload(), now), fractals, now).signals).toHaveLength(1);
  });

  test('ignores open candles, equality/wicks, prior wick breaks, late confirmations and missing preceding candles', () => {
    const history = compactIntrusionHistory(payload(), now);
    for (const [close, closed] of [[120, true], [119, true], [125, false]] as const) {
      const rows = candles(); rows[2] = { ...rows[2], close, closed };
      expect(buildIntrusionDailyBreakouts(rows, history, model(), now).signals.some((s: any) => s.direction === 'long')).toBe(false);
    }
    const broken = model(); broken.levels[0].brokenTime = start;
    expect(buildIntrusionDailyBreakouts(candles(), history, broken, now).signals).toHaveLength(1);
    const late = model(); late.levels[0].confirmedAt = iso(start + 3 * HOUR - .001);
    expect(buildIntrusionDailyBreakouts(candles(), history, late, now).signals).toHaveLength(1);
    expect(buildIntrusionDailyBreakouts(candles().filter((_, i) => i !== 1), history, model(), now).signals).toHaveLength(1);
  });

  test('later SMTP times do not move markers or create new ones after a previously broken Daily level', () => {
    const data: any = payload(); data.intrusionDomStudy.history.push({ timestamp: alert(3), direction: 'long' });
    const history = compactIntrusionHistory(data, now);
    const early = buildIntrusionDailyBreakouts(candles().slice(0, 4), history, model(), (start + 4 * HOUR) * 1000).signals;
    const later = buildIntrusionDailyBreakouts(candles(), history, model(), now).signals;
    expect(later.filter((s: any) => s.closedAt <= start + 4 * HOUR)).toEqual(early);
    expect(later).toHaveLength(2);
  });

  test('validates provenance and exact candle identity', () => {
    const history = compactIntrusionHistory(payload(), now);
    expect(() => buildIntrusionDailyBreakouts(candles(), { ...history, market: 'ETH-USD' }, model(), now)).toThrow('history');
    history.records[0].alertTime = iso(start + 3 * HOUR);
    expect(() => buildIntrusionDailyBreakouts(candles(), history, model(), now)).toThrow('alert time');
  });

  test('renders a thin purple pane-height line and toggles independently', () => {
    const layer = new IntrusionDailyBreakoutPrimitive(), ctx: any = {};
    for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke']) ctx[name] = jest.fn();
    layer.attached({ chart: { timeScale: () => ({ timeToCoordinate: () => 20 }) }, requestUpdate: jest.fn() });
    layer.setData([{ time: start }]);
    const draw = () => layer.paneViews()[0].renderer().draw({ useMediaCoordinateSpace: (render: any) => render({ context: ctx, mediaSize: { width: 300, height: 400 } }) });
    draw(); expect(ctx.strokeStyle).toBe('#ba68c8'); expect(ctx.lineWidth).toBe(1);
    expect(ctx.moveTo).toHaveBeenCalledWith(20.5, 0); expect(ctx.lineTo).toHaveBeenCalledWith(20.5, 400);
    layer.setVisible(false); ctx.stroke.mockClear(); draw(); expect(ctx.stroke).not.toHaveBeenCalled();
  });
});

describe('recorded intrusion and same-candle Weekly close break, offline replay', () => {
  const weeklyModel = () => ({ ...model(), levels: model().levels.map((level: any) => ({ ...level,
    id: level.id.replace('D|', 'W|'), timeframe: 'W' })) });

  test('requires a Weekly level, not merely a Daily break, and does not require a gap edge', () => {
    const history = compactIntrusionHistory(payload(), now);
    expect(buildIntrusionWeeklyBreakouts(candles(), history, model(), now).signals).toEqual([]);
    const result = buildIntrusionWeeklyBreakouts(candles(), history, weeklyModel(), now);
    expect(result.signals.map((s: any) => [s.time, s.direction])).toEqual([[start + 2 * HOUR, 'long'], [start + 4 * HOUR, 'short']]);
    expect(result.signals[0]).toMatchObject({ weeklyLevels: [{ price: 120 }], intrusion: { alertTime: iso(start + 2 * HOUR) } });
    expect(result.signals[0]).not.toHaveProperty('gap'); expect(result.signals[0]).not.toHaveProperty('dailyLevels');
  });

  test('does not exclude a Weekly break when the same candle also breaks a Daily fractal', () => {
    const history = compactIntrusionHistory(payload(), now), weekly = weeklyModel();
    const combined = { ...weekly, levels: [...weekly.levels, ...model().levels] };
    expect(buildIntrusionWeeklyBreakouts(candles(), history, combined, now)).toEqual(buildIntrusionWeeklyBreakouts(candles(), history, weekly, now));
  });

  test('groups multiple Weekly levels broken on one intrusion candle into one entry signal', () => {
    const weekly = weeklyModel(); weekly.levels.push({ ...weekly.levels[0], id: 'W|second-high', price: 123 });
    const result = buildIntrusionWeeklyBreakouts(candles(), compactIntrusionHistory(payload(), now), weekly, now);
    expect(result.signals).toHaveLength(2); expect(result.signals[0].weeklyLevels).toHaveLength(2);
  });

  test('requires fresh, confirmed levels, the exact intrusion candle/direction and only closed candles', () => {
    const history = compactIntrusionHistory(payload(), now), weekly = weeklyModel();
    const priorBreak = weeklyModel(); priorBreak.levels[0].brokenTime = start + HOUR;
    const late = weeklyModel(); late.levels[0].confirmedAt = iso(start + 3 * HOUR - .001);
    for (const fractals of [priorBreak, late]) {
      expect(buildIntrusionWeeklyBreakouts(candles(), history, fractals, now).signals.map((s: any) => s.direction)).toEqual(['short']);
    }
    const rows = candles(); rows[2].closed = false;
    expect(buildIntrusionWeeklyBreakouts(rows, history, weekly, now).signals.map((s: any) => s.direction)).toEqual(['short']);
    const wrong = { ...history, records: history.records.map((r: any) => ({ ...r, direction: r.direction === 'long' ? 'short' : 'long' })) };
    expect(buildIntrusionWeeklyBreakouts(candles(), wrong, weekly, now).signals).toEqual([]);
    const early = buildIntrusionWeeklyBreakouts(candles().slice(0, 4), history, weekly, (start + 4 * HOUR) * 1000).signals;
    expect(buildIntrusionWeeklyBreakouts(candles(), history, weekly, now).signals.filter((s: any) => s.closedAt <= start + 4 * HOUR)).toEqual(early);
  });
});
