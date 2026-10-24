export {};
const intrusionBreakouts = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const { compactIntrusionHistory, buildIntrusionDailyBreakouts, IntrusionDailyBreakoutPrimitive } = intrusionBreakouts;
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
