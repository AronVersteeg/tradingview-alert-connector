import { BinanceSpotChartFeed, parseSpotChartCandles } from '../src/services/binanceSpotChart';
import { binanceGet } from '../src/services/binanceHttp';
const browserData = require('../public/btc-spot-chart/data.js');

jest.mock('../src/services/binanceHttp', () => ({ binanceGet: jest.fn() }));

const hour = 3_600_000;
const start = Date.parse('2026-10-01T12:00:00Z');
const row = (time = start) => [time, '80000.01', '81000.99', '79000.02', '80500.13', '12.345678', time + hour - 1, '990000.1234'];
const stream = (overrides: any = {}) => ({
  e: 'kline', s: 'BTCUSDT', E: start + 120_000,
  k: { s: 'BTCUSDT', i: '1h', t: start, T: start + hour - 1,
    o: '80000.01', h: '81000.99', l: '79000.02', c: '80500.13', v: '12.345678', q: '990000.1234', x: false, ...overrides }
});

describe('read-only BTCUSDT Spot chart data', () => {
  beforeEach(() => (binanceGet as jest.Mock).mockReset());

  test('preserves exact OHLC and volumes, including the explicitly open candle', () => {
    const candles = parseSpotChartCandles([row(start - hour), row()], start + 120_000);
    expect(candles).toHaveLength(2);
    expect(candles[0].closed).toBe(true);
    expect(candles[1]).toMatchObject({
      time: start / 1000, open: 80000.01, high: 81000.99, low: 79000.02, close: 80500.13,
      volume: 12.345678, quoteVolume: 990000.1234, closed: false
    });
  });

  test('rejects corrupt, future, wrong-duration and impossible OHLC candles', () => {
    const invalid = [
      ['bad'], row(start + 2 * hour), [...row().slice(0, 6), start + 12, 10],
      [start, '81001', '81000', '79000', '80500', 1, start + hour - 1, 100],
      [start, '80000', '81000', '79000', '80500', -1, start + hour - 1, 100]
    ];
    expect(parseSpotChartCandles(invalid, start + hour)).toEqual([]);
  });

  test('deduplicates and orders source candles without resampling or averaging', () => {
    const updated = row(); updated[4] = '80600.14';
    const candles = parseSpotChartCandles([row(), row(start - hour), updated], start + hour);
    expect(candles.map((candle) => candle.time)).toEqual([(start - hour) / 1000, start / 1000]);
    expect(candles[1].close).toBe(80600.14);
  });

  test('uses only Spot BTCUSDT 1h and shares recent reads', async () => {
    const get = binanceGet as jest.Mock;
    get.mockResolvedValue({ data: [row()] });
    const feed = new BinanceSpotChartFeed();
    const [first, second] = await Promise.all([feed.page(), feed.page()]);
    expect(first).toBe(second);
    expect(await feed.page()).toBe(first);
    expect(get).toHaveBeenCalledTimes(1);
    expect(get).toHaveBeenCalledWith('https://api.binance.com/api/v3/klines', expect.objectContaining({
      params: { symbol: 'BTCUSDT', interval: '1h', limit: 1000 }
    }));
    expect(first).toMatchObject({ symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h' });
  });

  test('paginates exclusively before the earliest candle without overlap', async () => {
    (binanceGet as jest.Mock).mockResolvedValue({ data: [row(start - hour)] });
    await new BinanceSpotChartFeed().page(start);
    expect(binanceGet).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({
      params: expect.objectContaining({ endTime: start - 1 })
    }));
  });

  test('bounds cache pages and refuses invalid history cursors', async () => {
    (binanceGet as jest.Mock).mockResolvedValue({ data: [row()] });
    const feed = new BinanceSpotChartFeed();
    for (let i = 1; i <= 12; i += 1) await feed.page(start - i * hour);
    expect((feed as any).cache.size).toBe(8);
    await expect(feed.page(-1)).rejects.toThrow('past Unix');
    await expect(feed.page(Date.now() + hour)).rejects.toThrow('past Unix');
    await expect(feed.page(NaN)).rejects.toThrow('past Unix');
  });

  test('does not cache a failed read as fresh data', async () => {
    const get = binanceGet as jest.Mock;
    get.mockRejectedValueOnce(new Error('cooldown')).mockResolvedValue({ data: [row()] });
    const feed = new BinanceSpotChartFeed();
    await expect(feed.page()).rejects.toThrow('cooldown');
    expect((await feed.page()).candles).toHaveLength(1);
    expect(get).toHaveBeenCalledTimes(2);
  });

  test('validates browser stream venue, symbol, timeframe, timestamp and OHLC', () => {
    expect(browserData.streamCandle(stream())).toMatchObject({ time: start / 1000, closed: false, close: 80500.13 });
    expect(browserData.streamCandle(stream({ x: true })).closed).toBe(true);
    for (const overrides of [{ s: 'ETHUSDT' }, { i: '1m' }, { T: start + hour }, { h: '1' }]) {
      expect(browserData.streamCandle(stream(overrides))).toBeNull();
    }
    expect(browserData.streamCandle({ ...stream(), s: 'ETHUSDT' })).toBeNull();
  });

  test('a delayed REST response cannot replace a newer websocket tick', () => {
    const live = browserData.streamCandle(stream());
    const rest = { ...live, close: 80001, version: live.version - 1000 };
    expect(browserData.mergeCandles([live], [rest])[0].close).toBe(80500.13);
    const next = { ...live, time: live.time + 3600, version: live.version + 3600000 };
    expect(browserData.mergeCandles([next, live], [rest], 1)).toEqual([next]);
  });
});
