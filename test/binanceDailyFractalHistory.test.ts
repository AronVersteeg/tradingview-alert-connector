import {
  BINANCE_DAILY_FRACTAL_MARKETS,
  BinanceDailyCandle,
  buildDailyFractalHistory,
  isDailyFractalMarket,
  parseBinanceDailyCandles
} from '../src/services/binanceDailyFractalHistory';

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
});
