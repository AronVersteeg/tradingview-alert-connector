import { BtcFractalProjectionFeed } from '../src/services/btcFractalProjections';
import { DailyFractalHistorySnapshot, latestExpectedFractalClose } from '../src/services/binanceDailyFractalHistory';
const { projectFractals, FractalPrimitive } = require('../public/btc-spot-chart/fractals.js');

const HOUR = 3600;
const date = (value: string) => Date.parse(value) / 1000;
const iso = (seconds: number) => new Date(seconds * 1000).toISOString();
const through = date('2026-10-21T16:00:00Z');
const start = date('2026-09-01T00:00:00Z');
const candles = () => Array.from({ length: (through - start) / HOUR }, (_, index) => ({
  time: start + index * HOUR, high: 90, low: 20, closed: true
}));
const record = (overrides: any = {}) => ({
  id: 'test', type: 'HIGH', price: 100, priceExact: '100.00000000',
  pivotAt: '2026-10-10T00:00:00Z', confirmedAt: '2026-10-12T23:59:59.999Z',
  isLatestForType: true, ...overrides
});
const snapshot = (interval: '1d' | '1w' | '1M', records: any[] = []): DailyFractalHistorySnapshot => ({
  ok: true, market: 'BTC-USD', symbol: 'BTCUSDT', venue: 'Binance Spot', interval, window: 2,
  fetchedAt: iso(through), cached: false,
  latestClosedCandleAt: new Date(latestExpectedFractalClose(interval, through * 1000)).toISOString(), records
});
const bundle = (daily = [record()], weekly: any[] = [], monthly: any[] = []) => ({
  symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: iso(through),
  daily: snapshot('1d', daily), weekly: snapshot('1w', weekly), monthly: snapshot('1M', monthly)
});

describe('BTC Spot fractal projection read cache', () => {
  beforeEach(() => { jest.useFakeTimers(); jest.setSystemTime(through * 1000 + 5000); });
  afterEach(() => jest.useRealTimers());

  test('shares concurrent reads, reuses the closed hour and refreshes next hour', async () => {
    const daily = jest.fn().mockResolvedValue(snapshot('1d'));
    const weekly = jest.fn().mockResolvedValue(snapshot('1w'));
    const monthly = jest.fn().mockResolvedValue(snapshot('1M'));
    const feed = new BtcFractalProjectionFeed(daily, weekly, monthly);
    const [a, b] = await Promise.all([feed.snapshot(), feed.snapshot()]);
    expect(a).toBe(b); expect(await feed.snapshot()).toBe(a);
    expect(daily).toHaveBeenCalledTimes(1); expect(weekly).toHaveBeenCalledTimes(1);
    expect(monthly).toHaveBeenCalledTimes(1); expect(a.monthly.interval).toBe('1M');
    jest.setSystemTime((through + HOUR) * 1000 + 5000);
    expect((await feed.snapshot()).evaluatedThrough).toBe(iso(through + HOUR));
    expect(daily).toHaveBeenCalledTimes(2);
  });

  test('forces a lagging midnight cache once, without accepting stale or Futures history', async () => {
    const current = snapshot('1d');
    const daily = jest.fn().mockResolvedValueOnce({ ...current, latestClosedCandleAt: '2026-10-19T23:59:59.999Z' }).mockResolvedValue(current);
    await new BtcFractalProjectionFeed(daily, async () => snapshot('1w'), async () => snapshot('1M')).snapshot();
    expect(daily.mock.calls).toEqual([[], [true]]);
    for (const bad of [{ ...current, stale: true }, { ...current, venue: 'Binance Futures' }, { ...current, latestClosedCandleAt: undefined }]) {
      await expect(new BtcFractalProjectionFeed(async () => bad as any).snapshot()).rejects.toThrow('not current');
    }
  });

  test('does not cache failures or advance an hour crossed during a source read', async () => {
    const daily = jest.fn().mockRejectedValueOnce(new Error('cooldown')).mockImplementation(async () => {
      jest.setSystemTime((through + HOUR) * 1000 + 5000); return snapshot('1d');
    });
    const feed = new BtcFractalProjectionFeed(daily, async () => snapshot('1w'), async () => snapshot('1M'));
    await expect(feed.snapshot()).rejects.toThrow('cooldown');
    expect((await feed.snapshot()).evaluatedThrough).toBe(iso(through));
    expect((await feed.snapshot()).evaluatedThrough).toBe(iso(through + HOUR));
  });

  test('refreshes a lagging Monthly boundary and rejects stale Monthly data', async () => {
    const monthly = jest.fn().mockResolvedValueOnce({ ...snapshot('1M'), latestClosedCandleAt: '2026-08-31T23:59:59.999Z' }).mockResolvedValue(snapshot('1M'));
    const feed = new BtcFractalProjectionFeed(async () => snapshot('1d'), async () => snapshot('1w'), monthly);
    expect((await feed.snapshot()).monthly.latestClosedCandleAt).toBe('2026-09-30T23:59:59.999Z');
    expect(monthly.mock.calls).toEqual([[], [true]]);
    await expect(new BtcFractalProjectionFeed(async () => snapshot('1d'), async () => snapshot('1w'),
      async () => ({ ...snapshot('1M'), stale: true })).snapshot()).rejects.toThrow('1M fractals are not current');
  });
});

describe('closed-hour D/W/M projection geometry', () => {
  test('anchors to the first exact intraday wick, not midnight, and preserves exact prices/current labels', () => {
    const rows = candles();
    const origin = date('2026-10-10T07:00:00Z');
    rows.find((row) => row.time === origin)!.high = 100;
    rows.find((row) => row.time === origin + HOUR)!.high = 100;
    const result = projectFractals(bundle(), rows).levels[0];
    expect(result).toMatchObject({ originTime: origin, price: 100, priceExact: '100.00000000', label: 'DC', broken: false });
    expect(projectFractals(bundle([record({ isLatestForType: false })]), rows).levels[0].label).toBe('D');
  });

  test('exact touches and open-candle excursions do not break; a closed wick strictly through does', () => {
    const rows = candles();
    rows[rows.length - 1].high = 100;
    rows.push({ time: through, high: 110, low: 20, closed: false });
    expect(projectFractals(bundle(), rows).levels[0].broken).toBe(false);
    rows[rows.length - 2].high = 100.01;
    expect(projectFractals(bundle(), rows).levels[0]).toMatchObject({ broken: true, brokenTime: through - HOUR });
  });

  test('resolves a historical D break to its first exact closed 1H candle', () => {
    const rows = candles();
    const brokenTime = date('2026-10-16T13:00:00Z');
    rows.find((row) => row.time === brokenTime)!.high = 101;
    rows.find((row) => row.time === brokenTime + HOUR)!.high = 102;
    const result = projectFractals(bundle([record({ firstBrokenAt: '2026-10-16T00:00:00Z' })]), rows).levels[0];
    expect(result).toMatchObject({ broken: true, brokenTime });
  });

  test('resolves a Weekly low and current WC label, ignoring pre-confirmation excursions', () => {
    const rows = candles();
    const origin = date('2026-09-15T06:00:00Z');
    rows.find((row) => row.time === origin)!.low = 10;
    const weekly = record({ type: 'LOW', price: 10, priceExact: '10.00', pivotAt: '2026-09-14T00:00:00Z', confirmedAt: '2026-10-04T23:59:59.999Z' });
    rows.find((row) => row.time === date('2026-10-01T00:00:00Z'))!.low = 9;
    const result = projectFractals(bundle([], [weekly]), rows).levels[0];
    expect(result).toMatchObject({ label: 'WC', originTime: origin, broken: false });
    rows[rows.length - 1].low = 9;
    expect(projectFractals(bundle([], [weekly]), rows).levels[0]).toMatchObject({ broken: true, brokenTime: through - HOUR });
  });

  test('never invents an origin when the pivot period is only partially loaded', () => {
    const rows = candles().filter((row) => row.time >= date('2026-10-10T08:00:00Z'));
    rows[1].high = 100;
    const partial = projectFractals(bundle(), rows).levels[0];
    expect(partial).toMatchObject({ originTime: null, originBeforeHistory: false });
    const old = projectFractals(bundle([record({ pivotAt: '2026-10-01T00:00:00Z' })]), rows).levels[0];
    expect(old).toMatchObject({ originTime: null, originBeforeHistory: true });
  });

  test('keeps a clipped Monthly line when a complete loaded month-tail proves its wick is older', () => {
    const first = date('2026-07-29T08:00:00Z');
    const rows = Array.from({ length: (through - first) / HOUR }, (_, i) => ({ time: first + i * HOUR, high: 90, low: 20, closed: true }));
    const monthly = record({ pivotAt: '2026-07-01T00:00:00Z', confirmedAt: '2026-09-30T23:59:59.999Z' });
    const result = projectFractals(bundle([], [], [monthly]), rows).levels[0];
    expect(result).toMatchObject({ originTime: null, originBeforeHistory: true });
    rows[0].high = 100;
    expect(projectFractals(bundle([], [], [monthly]), rows).levels[0]).toMatchObject({ originTime: null, originBeforeHistory: false });
  });

  test.each([
    ['2024-02-01T00:00:00Z', '2024-02-29T23:00:00Z', '2024-05-01T00:00:00Z'],
    ['2025-02-01T00:00:00Z', '2025-02-28T23:00:00Z', '2025-05-01T00:00:00Z'],
    ['2025-12-01T00:00:00Z', '2025-12-31T23:00:00Z', '2026-03-01T00:00:00Z']
  ])('resolves the exact last-hour Monthly wick for %s without treating a month as 30 days', (pivotAt, wickAt, cutoffAt) => {
    const first = date(pivotAt), cutoff = date(cutoffAt);
    const rows = Array.from({ length: (cutoff - first) / HOUR }, (_, i) => ({ time: first + i * HOUR, high: 90, low: 20, closed: true }));
    rows.find((row) => row.time === date(wickAt))!.high = 100;
    const data = bundle([], [], [record({ pivotAt, confirmedAt: iso(cutoff - .001) })]);
    data.evaluatedThrough = iso(cutoff);
    for (const [key, interval] of [['daily', '1d'], ['weekly', '1w'], ['monthly', '1M']] as const) {
      data[key].latestClosedCandleAt = new Date(latestExpectedFractalClose(interval, cutoff * 1000)).toISOString();
    }
    expect(projectFractals(data, rows).levels[0]).toMatchObject({ timeframe: 'M', label: 'MC', originTime: date(wickAt), broken: false });
  });

  test('resolves Monthly historical and current-month breaks to closed 1H wicks', () => {
    const rows = candles();
    const breakingHour = date('2026-10-16T13:00:00Z');
    rows.find((row) => row.time === breakingHour)!.high = 101;
    const monthly = record({ pivotAt: '2026-07-01T00:00:00Z', confirmedAt: '2026-09-30T23:59:59.999Z' });
    expect(projectFractals(bundle([], [], [monthly]), rows).levels[0]).toMatchObject({ broken: true, brokenTime: breakingHour });
    expect(projectFractals(bundle([], [], [{ ...monthly, firstBrokenAt: '2026-10-01T00:00:00Z' }]), rows).levels[0]).toMatchObject({ broken: true, brokenTime: breakingHour });
  });

  test('refuses stale, incomplete or not-yet-closed coverage instead of falsely marking levels intact', () => {
    const rows = candles();
    expect(() => projectFractals(bundle(), rows.slice(0, -1))).toThrow('latest closed');
    expect(() => projectFractals(bundle(), rows.filter((row) => row.time !== through - 2 * HOUR))).toThrow('Missing closed');
    const stale = bundle(); stale.daily.stale = true;
    expect(() => projectFractals(stale, rows)).toThrow('stale');
    expect(() => projectFractals({ ...bundle(), venue: 'Binance Futures' }, rows)).toThrow('source');
    expect(projectFractals(bundle([record({ confirmedAt: iso(through + HOUR) })]), rows).levels).toEqual([]);
  });

  test('renderer clips old origins, keeps broken endpoints and respects independent visibility toggles', () => {
    const primitive = new FractalPrimitive();
    const requestUpdate = jest.fn();
    primitive.attached({ chart: {}, series: {}, requestUpdate });
    primitive.setData([
      { timeframe: 'D', price: 100, originTime: null, originBeforeHistory: true, broken: false },
      { timeframe: 'W', price: 80, originTime: 1, broken: true, brokenTime: 2 },
      { timeframe: 'D', originTime: null, originBeforeHistory: false, broken: false },
      { timeframe: 'W', originTime: 1, broken: true, brokenTime: null }
    ]);
    expect(primitive.visibleLevels()).toHaveLength(2);
    primitive.setOptions({ broken: false }); expect(primitive.visibleLevels()).toHaveLength(1);
    primitive.setOptions({ daily: false }); expect(primitive.visibleLevels()).toHaveLength(0);
    expect(requestUpdate).toHaveBeenCalledTimes(3);
    primitive.detached();
  });

  test('Weekly takes display precedence at the exact same price without removing Daily data', () => {
    const primitive = new FractalPrimitive();
    const daily = { timeframe: 'D', label: 'DC', price: 100, originTime: 1, broken: false };
    const weekly = { ...daily, timeframe: 'W', label: 'WC' };
    const nearbyDaily = { ...daily, price: 100.01 };
    const levels = [daily, weekly, nearbyDaily];
    primitive.setData(levels);
    expect(primitive.visibleLevels()).toEqual([weekly, nearbyDaily]);
    expect(primitive.levels).toEqual([daily, weekly, nearbyDaily]);
    primitive.setOptions({ weekly: false });
    expect(primitive.visibleLevels()).toEqual([daily, nearbyDaily]);
    primitive.setOptions({ weekly: true });
    expect(primitive.visibleLevels()).toEqual([weekly, nearbyDaily]);
  });

  test('broken Weekly segments also take precedence, but unresolved Weekly origins do not hide Daily', () => {
    const primitive = new FractalPrimitive();
    const daily = { timeframe: 'D', price: 100, originTime: 1, broken: true, brokenTime: 3 };
    const weekly = { ...daily, timeframe: 'W' };
    primitive.setData([daily, weekly]);
    expect(primitive.visibleLevels()).toEqual([weekly]);
    primitive.setData([daily, { ...weekly, originTime: null, originBeforeHistory: false }]);
    expect(primitive.visibleLevels()).toEqual([daily]);
  });

  test('Monthly has priority over Weekly and Daily, with independent visibility fallbacks', () => {
    const primitive = new FractalPrimitive();
    const daily = { timeframe: 'D', price: 100, originTime: 1, broken: false };
    const weekly = { ...daily, timeframe: 'W' };
    const monthly = { ...daily, timeframe: 'M' };
    primitive.setData([daily, weekly, monthly]);
    expect(primitive.visibleLevels()).toEqual([monthly]);
    primitive.setOptions({ monthly: false }); expect(primitive.visibleLevels()).toEqual([weekly]);
    primitive.setOptions({ weekly: false }); expect(primitive.visibleLevels()).toEqual([daily]);
    primitive.setOptions({ monthly: true, daily: false }); expect(primitive.visibleLevels()).toEqual([monthly]);
  });

  test('canvas renderer actually draws the dashed segment only between origin and breaking hour', () => {
    const primitive = new FractalPrimitive();
    const ctx: any = {};
    for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke', 'fillRect', 'fillText']) ctx[name] = jest.fn();
    ctx.measureText = () => ({ width: 10 });
    primitive.attached({ chart: { timeScale: () => ({ timeToCoordinate: (time: number) => time }) },
      series: { priceToCoordinate: () => 80 }, requestUpdate: jest.fn() });
    primitive.setData([{ timeframe: 'D', label: 'D', price: 100, originTime: 20,
      originBeforeHistory: false, broken: true, brokenTime: 150 }]);
    primitive.paneViews()[0].renderer().draw({ useMediaCoordinateSpace: (render: any) => render({ context: ctx, mediaSize: { width: 500, height: 300 } }) });
    expect(ctx.setLineDash).toHaveBeenCalledWith([5, 5]);
    expect(ctx.moveTo).toHaveBeenCalledWith(20, 80);
    expect(ctx.lineTo.mock.calls).toEqual([[150, 80]]);
  });
});
