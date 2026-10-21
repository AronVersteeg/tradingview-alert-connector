import { confirmedWilliamsFractals } from '../src/services/binanceDailyFractalHistory';
const { buildHourlyFractalBoxes, HourlyFractalBoxPrimitive } = require('../public/btc-spot-chart/hourly-fractal-boxes.js');

const HOUR = 3600;
const start = Date.parse('2026-10-20T00:00:00Z') / 1000;
const rows = () => [
  [10, 6], [11, 5], [15, 7], [12, 6], [11, 4], [10, 2], [11, 4], [12, 5],
  [16, 6], [13, 5], [12, 4], [11, 3], [12, 4], [13, 5]
].map(([high, low], index) => ({ time: start + index * HOUR, high, low, closed: true }));
const now = (start + 100 * HOUR) * 1000;

describe('read-only 1H Williams fractal boxes', () => {
  test('matches the existing Williams engine, including pivot and confirmation times', () => {
    const input = rows();
    const expected = confirmedWilliamsFractals(input.map((row) => ({ openTime: row.time * 1000,
      closeTime: (row.time + HOUR) * 1000 - 1, high: String(row.high), low: String(row.low) })));
    const actual = buildHourlyFractalBoxes(input, now).fractals;
    expect(actual).toEqual(expected.map((fractal) => ({ type: fractal.type, price: fractal.price,
      pivotTime: Date.parse(fractal.pivotAt) / 1000, confirmedAt: (Date.parse(fractal.confirmedAt) + 1) / 1000 })));
  });

  test('builds stepped ranges from the exact pivot, only after both high and low are confirmed', () => {
    const result = buildHourlyFractalBoxes(rows(), now);
    expect(result.boxes).toEqual([
      { fromTime: start + 5 * HOUR, toTime: start + 8 * HOUR, confirmedAt: start + 8 * HOUR, high: 15, low: 2, openEnded: false },
      { fromTime: start + 8 * HOUR, toTime: start + 11 * HOUR, confirmedAt: start + 11 * HOUR, high: 16, low: 2, openEnded: false },
      { fromTime: start + 11 * HOUR, toTime: start + 14 * HOUR, confirmedAt: start + 14 * HOUR, high: 16, low: 3, openEnded: true }
    ]);
  });

  test('ignores unfinished right candles and live excursions while extending the last known range', () => {
    const input = rows().slice(0, 11);
    input[10].closed = false;
    const pending = buildHourlyFractalBoxes(input, now);
    expect(pending.fractals.some((fractal: any) => fractal.price === 16)).toBe(false);
    expect(pending.boxes.at(-1)).toMatchObject({ high: 15, low: 2, toTime: start + 11 * HOUR });
    input[10].closed = true;
    expect(buildHourlyFractalBoxes(input, now).boxes.at(-1)).toMatchObject({ high: 16, low: 2, confirmedAt: start + 11 * HOUR });
    expect(buildHourlyFractalBoxes(input, (start + 10 * HOUR) * 1000).fractals.some((fractal: any) => fractal.price === 16)).toBe(false);
  });

  test('newest equal-price pivot wins and no boxes are invented without both confirmed sides', () => {
    const input = [10, 11, 15, 15, 12, 11, 10].map((high, index) => ({ time: start + index * HOUR, high, low: 1, closed: true }));
    const result = buildHourlyFractalBoxes(input, now);
    expect(result.fractals).toEqual([{ type: 'HIGH', price: 15, pivotTime: start + 3 * HOUR, confirmedAt: start + 6 * HOUR }]);
    expect(result.boxes).toEqual([]);
    expect(buildHourlyFractalBoxes([], now)).toEqual({ boxes: [], fractals: [], evaluatedThrough: null });
  });

  test('resets at missing hours instead of spanning unknown price history', () => {
    const input = rows();
    const initial = input.slice(0, 8);
    const afterGap = input.slice(8).map((row) => ({ ...row, time: row.time + 5 * HOUR }));
    const result = buildHourlyFractalBoxes([...initial, ...afterGap], now);
    expect(result.boxes[0]).toMatchObject({ toTime: start + 8 * HOUR, openEnded: false });
    expect(result.boxes).toHaveLength(1);
  });

  test('renderer draws gray boxes behind candles, clips them and supports its own toggle', () => {
    const primitive = new HourlyFractalBoxPrimitive();
    const requestUpdate = jest.fn();
    const ctx: any = {};
    for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'fillRect']) ctx[name] = jest.fn();
    primitive.attached({ chart: { timeScale: () => ({ logicalToCoordinate: (index: number) => index * 10 }) },
      series: { priceToCoordinate: (price: number) => 100 - price }, requestUpdate });
    const input = rows();
    primitive.setData(buildHourlyFractalBoxes(input, now).boxes, input);
    const render = () => primitive.paneViews()[0].renderer().draw({ useMediaCoordinateSpace: (draw: any) => draw({ context: ctx, mediaSize: { width: 120, height: 100 } }) });
    expect(primitive.paneViews()[0].zOrder()).toBe('bottom');
    render();
    expect(ctx.fillStyle).toBe('rgba(210, 214, 224, 0.26)');
    expect(ctx.fillRect.mock.calls).toEqual([[50, 85, 30, 13], [80, 84, 30, 14], [110, 84, 10, 13]]);
    primitive.setVisible(false); render();
    expect(ctx.fillRect).toHaveBeenCalledTimes(3);
    expect(requestUpdate).toHaveBeenCalledTimes(2);
    primitive.detached(); render();
    expect(ctx.fillRect).toHaveBeenCalledTimes(3);
  });
});
