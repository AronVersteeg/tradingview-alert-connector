export {};
const { filterGreenPositions } = require('../public/btc-spot-chart/green-position-replay.js');
const { buildHourlyFractalBoxes } = require('../public/btc-spot-chart/hourly-fractal-boxes.js');
const H = 3600, start = Date.UTC(2026, 8, 1) / 1000;
const candle = (i: number, o: number, h: number, l: number, c: number, closed = true) =>
  ({ time: start + i*H, open:o, high:h, low:l, close:c, closed });
const signal = (i: number, direction = 'long') => ({ time:start+i*H, closedAt:start+(i+1)*H, direction });
const rows = [
  candle(0, 105, 108, 100, 106), candle(1, 106, 109, 101, 107),
  candle(2, 107, 109, 95, 108), candle(3, 108, 111, 103, 110),
  candle(4, 110, 113, 107, 112), candle(5, 112, 115, 109, 114),
  candle(6, 114, 118, 112.5, 117), candle(7, 117, 119, 115, 118),
  candle(8, 118, 120, 116, 119), candle(9, 119, 121, 117, 120),
  candle(10, 120, 122, 113, 121), candle(11, 121, 124, 120, 123),
  candle(12, 123, 126, 122, 125), candle(13, 125, 128, 120, 126),
  candle(14, 126, 129, 124, 128)
];
const filter = (input: any[], signals: any[]) =>
  filterGreenPositions(input, signals, buildHourlyFractalBoxes(input, (start+20*H)*1000).fractals);

test('suppresses both same and opposite green signals until the earlier position stops', () => {
  const input = rows.map((c,i) => i === 10 ? { ...c, low:110 } : c);
  expect(filter(input, [signal(5),signal(6),signal(7,'short'),signal(9),signal(10)]))
    .toEqual([signal(5),signal(10)]);
});

test('trailing uses a confirmed higher fractal and never the earlier intrabar range', () => {
  const input = rows.map((c,i) => i === 13 ? { ...c, low:112.5 } : c);
  expect(filter(input, [signal(5),signal(12),signal(13)])).toEqual([signal(5),signal(13)]);
});

test('ignores an unfilled last signal and a stop on the wrong side of the entry', () => {
  const input = rows.slice(0,8);
  expect(filter(input, [signal(7)])).toEqual([]);
  expect(filter(input.map((c,i) => i === 6 ? { ...c, open:111, low:110 } : c), [signal(5)])).toEqual([]);
});
