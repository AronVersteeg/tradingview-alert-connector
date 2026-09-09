import { DailyFractalHistoryItem } from '../src/services/binanceDailyFractalHistory';
import {
  ShadowHourlyCandle,
  evaluateShadowFractalBreakout,
  parseBinanceHourlyCandles
} from '../src/services/shadowFractalMonitor';

function hourly(
  hour: number,
  high: string,
  low: string,
  close: string
): ShadowHourlyCandle {
  const openTime = Date.UTC(2026, 8, 9, hour);
  return {
    openTime,
    closeTime: openTime + 60 * 60_000 - 1,
    open: close,
    high,
    low,
    close
  };
}

function daily(
  type: 'HIGH' | 'LOW',
  priceExact: string,
  confirmedAt: string
): DailyFractalHistoryItem {
  return {
    id: `BTCUSDT|${type}|1`,
    market: 'BTC-USD',
    symbol: 'BTCUSDT',
    type,
    price: Number(priceExact),
    priceExact,
    pivotAt: '2026-09-01T00:00:00.000Z',
    confirmedAt,
    isLatestForType: true
  };
}

describe('read-only Shadow Williams breakout monitor', () => {
  test('ignores the still-open Binance 1H candle and preserves exact decimals', () => {
    const nowMs = Date.UTC(2026, 8, 9, 2);
    const rows = [
      [Date.UTC(2026, 8, 9, 0), '10.10', '12.20', '9.90', '11.30', '1', Date.UTC(2026, 8, 9, 1) - 1],
      [Date.UTC(2026, 8, 9, 1), '11.30', '99', '1', '50', '1', Date.UTC(2026, 8, 9, 3) - 1]
    ];

    expect(parseBinanceHourlyCandles(rows, nowMs)).toEqual([
      expect.objectContaining({ open: '10.10', high: '12.20', low: '9.90', close: '11.30' })
    ]);
  });

  test('signals long only on the first close above both causally known high fractals', () => {
    const candles = [
      hourly(0, '8', '5', '7'),
      hourly(1, '9', '6', '8'),
      hourly(2, '10.00', '7', '9'),
      hourly(3, '9', '6', '8'),
      hourly(4, '8', '5', '8'),
      hourly(5, '12', '7', '11')
    ];
    const records = [
      daily('HIGH', '100', '2026-09-09T06:30:00.000Z'),
      daily('HIGH', '10.50', '2026-09-08T23:59:59.999Z')
    ];

    const signal = evaluateShadowFractalBreakout(candles, records);
    expect(signal?.direction).toBe('LONG');
    expect(signal?.hourlyFractal.priceExact).toBe('10.00');
    expect(signal?.dailyFractal.priceExact).toBe('10.50');
  });

  test('signals short on the first close below both confirmed low fractals', () => {
    const candles = [
      hourly(0, '15', '12', '14'),
      hourly(1, '14', '11', '13'),
      hourly(2, '13', '10.00', '11'),
      hourly(3, '14', '11', '12'),
      hourly(4, '15', '12', '12'),
      hourly(5, '12', '8', '9')
    ];
    const signal = evaluateShadowFractalBreakout(
      candles,
      [daily('LOW', '9.50', '2026-09-08T23:59:59.999Z')]
    );

    expect(signal?.direction).toBe('SHORT');
    expect(signal?.hourlyFractal.priceExact).toBe('10.00');
    expect(signal?.dailyFractal.priceExact).toBe('9.50');
  });

  test('does not repeat while closes remain beyond the same two levels', () => {
    const candles = [
      hourly(0, '8', '5', '7'),
      hourly(1, '9', '6', '8'),
      hourly(2, '10', '7', '9'),
      hourly(3, '9', '6', '8'),
      hourly(4, '8', '5', '8'),
      hourly(5, '12', '7', '11'),
      hourly(6, '13', '8', '12')
    ];

    expect(evaluateShadowFractalBreakout(
      candles,
      [daily('HIGH', '10.50', '2026-09-08T23:59:59.999Z')]
    )).toBeUndefined();
  });
});
