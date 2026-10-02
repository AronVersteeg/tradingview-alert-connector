import {
  BINANCE_DAILY_FRACTAL_MARKETS,
  BinanceDailyCandle,
  binanceWeeklyFractalHistory,
  binanceDailyFractalHistory,
  latestExpectedFractalClose,
  buildDailyFractalHistory,
  isDailyFractalMarket,
  parseBinanceDailyCandles
} from '../src/services/binanceDailyFractalHistory';
import { binanceGet, binanceRetryAt } from '../src/services/binanceHttp';

jest.mock('../src/services/binanceHttp', () => ({
  binanceGet: jest.fn(),
  binanceRetryAt: jest.fn(() => 0)
}));

function candle(day: number, high: string, low: string): BinanceDailyCandle {
  const openTime = Date.UTC(2026, 0, day);
  return {
    openTime,
    closeTime: openTime + 86_400_000 - 1,
    high,
    low
  };
}

describe('Binance daily Williams fractal history', () => {
  test('preserves the cooldown error on repeated cold-cache reads without another transport request', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(Date.parse('2026-10-02T12:00:00.000Z'));
    const { BinanceCooldownError } = jest.requireActual('../src/services/binanceHttp');
    const until = Date.now() + 3_600_000;
    const error = new BinanceCooldownError('fapi.binance.com', until);
    const get = binanceGet as jest.Mock;
    get.mockClear().mockRejectedValue(error);
    (binanceRetryAt as jest.Mock).mockReturnValue(until);
    try {
      await expect(binanceDailyFractalHistory('SOL-USD')).rejects.toBe(error);
      await expect(binanceDailyFractalHistory('SOL-USD')).rejects.toBe(error);
      expect(get).toHaveBeenCalledTimes(1);
    } finally {
      (binanceRetryAt as jest.Mock).mockReturnValue(0);
      jest.useRealTimers();
    }
  });
  test('computes Daily and Monday UTC Weekly close boundaries', () => {
    const friday = Date.parse('2026-10-02T12:00:00.000Z');
    expect(latestExpectedFractalClose('1d', friday)).toBe(Date.parse('2026-10-01T23:59:59.999Z'));
    expect(latestExpectedFractalClose('1w', friday)).toBe(Date.parse('2026-09-27T23:59:59.999Z'));
  });

  test('reuses confirmed daily data throughout the day and refreshes after the next daily close', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(Date.parse('2026-10-02T12:00:00.000Z'));
    const rows = Array.from({ length: 7 }, (_, index) => {
      const open = Date.UTC(2026, 8, 25 + index);
      return [open, '10', '12', '8', '11', '1', open + 86_400_000 - 1];
    });
    (binanceGet as jest.Mock).mockClear().mockResolvedValue({ data: rows });
    try {
      await binanceDailyFractalHistory('ETH-USD', true);
      jest.setSystemTime(Date.parse('2026-10-02T23:30:00.000Z'));
      await binanceDailyFractalHistory('ETH-USD');
      expect(binanceGet).toHaveBeenCalledTimes(1);
      jest.setSystemTime(Date.parse('2026-10-03T00:00:20.000Z'));
      await binanceDailyFractalHistory('ETH-USD');
      expect(binanceGet).toHaveBeenCalledTimes(2);
    } finally { jest.useRealTimers(); }
  });

  test('marks failed refreshes stale and suppresses repeated attempts until the cooldown ends', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(Date.parse('2026-10-02T12:00:00.000Z'));
    const rows = Array.from({ length: 7 }, (_, index) => {
      const open = Date.UTC(2026, 8, 25 + index);
      return [open, '10', '12', '8', '11', '1', open + 86_400_000 - 1];
    });
    const get = binanceGet as jest.Mock;
    get.mockClear().mockResolvedValue({ data: rows });
    try {
      const original = await binanceDailyFractalHistory('INJ-USD', true);
      const retryAt = Date.now() + 3_600_000;
      (binanceRetryAt as jest.Mock).mockReturnValue(retryAt);
      get.mockRejectedValue(new Error('cooldown active'));
      const fallback = await binanceDailyFractalHistory('INJ-USD', true);
      expect(fallback).toMatchObject({ stale: true, fetchedAt: original.fetchedAt, retryAt: new Date(retryAt + 1000).toISOString() });
      await binanceDailyFractalHistory('INJ-USD', true);
      expect(get).toHaveBeenCalledTimes(2);
      jest.setSystemTime(retryAt + 1001);
      get.mockResolvedValue({ data: rows });
      expect((await binanceDailyFractalHistory('INJ-USD', true)).stale).toBeUndefined();
      expect(get).toHaveBeenCalledTimes(3);
    } finally {
      (binanceRetryAt as jest.Mock).mockReturnValue(0);
      jest.useRealTimers();
    }
  });
  test('maps every dashboard pair to its explicit Binance Futures source', () => {
    expect(BINANCE_DAILY_FRACTAL_MARKETS).toEqual({
      'BTC-USD': { symbol: 'BTCUSDT' },
      'ETH-USD': { symbol: 'ETHUSDT' },
      'INJ-USD': { symbol: 'INJUSDT' },
      'SOL-USD': { symbol: 'SOLUSDT' },
      'ZEC-USD': { symbol: 'ZECUSDT' },
      'PAXG-USD': { symbol: 'XAUUSDT' },
      'XAG-USD': { symbol: 'XAGUSDT' }
    });
    expect(isDailyFractalMarket('SOL-USD')).toBe(true);
    expect(isDailyFractalMarket('DOGE-USD')).toBe(false);
  });

  test('uses closed candles and preserves Binance decimal values', () => {
    const nowMs = Date.UTC(2026, 0, 3);
    const rows = [
      [Date.UTC(2026, 0, 1), '1', '12.0', '9.10', '11', '1', Date.UTC(2026, 0, 2) - 1],
      [Date.UTC(2026, 0, 2), '1', '13.0', '10.0', '12', '1', Date.UTC(2026, 0, 3) - 1],
      [Date.UTC(2026, 0, 3), '1', '99.0', '1.0', '50', '1', Date.UTC(2026, 0, 4) - 1]
    ];

    expect(parseBinanceDailyCandles(rows, nowMs)).toEqual([
      expect.objectContaining({ high: '12.0', low: '9.10' }),
      expect.objectContaining({ high: '13.0', low: '10.0' })
    ]);
  });

  test('confirms after two right candles and resolves equal plateaus to the newest pivot', () => {
    const candles = [
      candle(1, '10', '8'),
      candle(2, '12', '7'),
      candle(3, '15.00', '6'),
      candle(4, '15.00', '5.50'),
      candle(5, '13', '6'),
      candle(6, '12', '7'),
      candle(7, '16', '8'),
      candle(8, '14', '7'),
      candle(9, '13', '6')
    ];

    const records = buildDailyFractalHistory(candles);
    const plateauHigh = records.find((record) => record.type === 'HIGH' && record.price === 15);
    const plateauLow = records.find((record) => record.type === 'LOW' && record.price === 5.5);

    expect(plateauHigh?.pivotAt).toBe(new Date(Date.UTC(2026, 0, 4)).toISOString());
    expect(plateauHigh?.priceExact).toBe('15.00');
    expect(plateauHigh?.confirmedAt).toBe(new Date(candles[5].closeTime).toISOString());
    expect(plateauHigh?.firstBrokenAt).toBe(new Date(candles[6].openTime).toISOString());
    expect(plateauLow?.pivotAt).toBe(new Date(Date.UTC(2026, 0, 4)).toISOString());
  });

  test('marks the newest confirmed high and low independently', () => {
    const candles = [
      candle(1, '10', '8'), candle(2, '11', '7'), candle(3, '14', '6'),
      candle(4, '12', '5'), candle(5, '11', '6'), candle(6, '15', '7'),
      candle(7, '13', '6'), candle(8, '12', '4'), candle(9, '14', '6'),
      candle(10, '13', '7'), candle(11, '12', '8')
    ];
    const records = buildDailyFractalHistory(candles);
    const latest = records.filter((record) => record.isLatestForType);

    expect(latest).toHaveLength(2);
    expect(latest.find((record) => record.type === 'HIGH')?.price).toBe(14);
    expect(latest.find((record) => record.type === 'LOW')?.price).toBe(4);
  });

  test('labels records with the requested dashboard market and Binance symbol', () => {
    const candles = [
      candle(1, '10', '8'), candle(2, '11', '7'), candle(3, '14.50', '6'),
      candle(4, '12', '5'), candle(5, '11', '6')
    ];
    const records = buildDailyFractalHistory(candles, 'INJ-USD', 'INJUSDT');

    expect(records).toEqual(expect.arrayContaining([
      expect.objectContaining({
        id: `INJUSDT|HIGH|${Date.UTC(2026, 0, 3)}`,
        market: 'INJ-USD',
        symbol: 'INJUSDT',
        priceExact: '14.50'
      })
    ]));
  });

  test('requests closed Binance Futures weekly candles for the weekly snapshot', async () => {
    const rows = Array.from({ length: 7 }, (_, index) => {
      const openTime = Date.UTC(2026, 0, 5 + index * 7);
      return [
        openTime,
        '10',
        String(12 + (index === 2 ? 5 : 0)),
        String(8 - (index === 4 ? 2 : 0)),
        '11',
        '1',
        openTime + 7 * 86_400_000 - 1
      ];
    });
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: rows });

    const snapshot = await binanceWeeklyFractalHistory('ZEC-USD', true);

    expect(binanceGet).toHaveBeenCalledWith(
      'https://fapi.binance.com/fapi/v1/klines',
      expect.objectContaining({ params: { symbol: 'ZECUSDT', interval: '1w', limit: 1000 } })
    );
    expect(snapshot).toEqual(expect.objectContaining({
      market: 'ZEC-USD',
      symbol: 'ZECUSDT',
      venue: 'Binance Futures',
      interval: '1w',
      window: 2,
      cached: false
    }));
    expect(snapshot.records.length).toBeGreaterThan(0);
  });
});
