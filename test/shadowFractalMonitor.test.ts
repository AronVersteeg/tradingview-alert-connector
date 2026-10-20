import { DailyFractalHistoryItem } from '../src/services/binanceDailyFractalHistory';
import * as fractalHistory from '../src/services/binanceDailyFractalHistory';
import * as binanceHttp from '../src/services/binanceHttp';
import {
  ShadowHourlyCandle,
  evaluateShadowFractalBreakout,
  parseBinanceHourlyCandles,
  shadowFractalEmailEnabled,
  shadowFractalEntryIsFresh,
  ShadowFractalMonitor
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

describe('Shadow Williams breakout monitor', () => {
  test('retains a consistent Futures source for both entry-model timeframes despite the dashboard Spot switch', async () => {
    const hourMs = 3_600_000;
    const boundary = Math.floor(Date.now() / hourMs) * hourMs;
    const rows = Array.from({ length: 7 }, (_, index) => {
      const open = boundary - (7 - index) * hourMs;
      return [open, '10', '12', '8', '11', '1', open + hourMs - 1];
    });
    const get = jest.spyOn(binanceHttp, 'binanceGet').mockResolvedValue({ data: rows } as any);
    const daily = jest.spyOn(fractalHistory, 'binanceDailyFractalHistory').mockResolvedValue({
      venue: 'Binance Futures', records: []
    } as any);
    try {
      const monitor = new ShadowFractalMonitor() as any;
      await expect(monitor.checkMarket({ market: 'BTC-USD', asset: 'BTC', symbol: 'BTCUSDT' }))
        .resolves.toMatchObject({ signal: 'NONE' });
      expect(get).toHaveBeenCalledWith('https://fapi.binance.com/fapi/v1/klines', expect.anything());
      expect(daily).toHaveBeenCalledWith('BTC-USD', false, 'Binance Futures');
    } finally {
      get.mockRestore();
      daily.mockRestore();
    }
  });

  const originalShadowEmailEnabled = process.env.SHADOW_FRACTAL_EMAIL_ENABLED;

  afterEach(() => {
    if (originalShadowEmailEnabled === undefined) delete process.env.SHADOW_FRACTAL_EMAIL_ENABLED;
    else process.env.SHADOW_FRACTAL_EMAIL_ENABLED = originalShadowEmailEnabled;
  });

  test('keeps Shadow email notifications off by default and permits explicit opt-in', () => {
    delete process.env.SHADOW_FRACTAL_EMAIL_ENABLED;
    expect(shadowFractalEmailEnabled()).toBe(false);
    process.env.SHADOW_FRACTAL_EMAIL_ENABLED = 'true';
    expect(shadowFractalEmailEnabled()).toBe(true);
    process.env.SHADOW_FRACTAL_EMAIL_ENABLED = 'false';
    expect(shadowFractalEmailEnabled()).toBe(false);
  });

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

  test('only permits live entries during the first 15 minutes after the 1H close', () => {
    const closedAt = Date.UTC(2026, 8, 9, 12);
    expect(shadowFractalEntryIsFresh(closedAt, closedAt + 15 * 60_000)).toBe(true);
    expect(shadowFractalEntryIsFresh(closedAt, closedAt + 15 * 60_000 + 1)).toBe(false);
    expect(shadowFractalEntryIsFresh(closedAt, closedAt - 1)).toBe(false);
  });
});
