import {
  BtcManualTradeOverrideState,
  buildManualTakeProfitOrderLevels,
  matchingManualOverrideCandle,
  normalizeBtcManualTradeOverrideRequest
} from '../src/services/btcManualTradeOverride';

describe('BTC manual entry and TP override', () => {
  const now = Date.parse('2026-09-10T10:30:00.000Z');

  test('arms a validated one-shot long override', () => {
    const state = normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 80000,
      expiresInHours: 12,
      takeProfits: [
        { price: 81000, allocationPct: 60 },
        { price: 82500, allocationPct: 40 }
      ]
    }, now);

    expect(state).toMatchObject({
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-10T22:30:00.000Z'
    });
  });

  test('rejects TP allocations that do not total 100 percent', () => {
    expect(() => normalizeBtcManualTradeOverrideRequest({
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [
        { price: 81000, allocationPct: 50 },
        { price: 82000, allocationPct: 25 }
      ]
    }, now)).toThrow('must total 100%');
  });

  test('rejects take profits on the wrong side of the trigger', () => {
    expect(() => normalizeBtcManualTradeOverrideRequest({
      direction: 'short',
      closeTrigger: 78000,
      takeProfits: [{ price: 79000, allocationPct: 100 }]
    }, now)).toThrow('Every short TP must be below');
  });

  test('only accepts a matching candle that closed after arming', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-11T10:30:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T09:00:00.000Z'), closeTime: Date.parse('2026-09-10T09:59:59.999Z'), close: '80500' },
      { openTime: Date.parse('2026-09-10T10:00:00.000Z'), closeTime: Date.parse('2026-09-10T10:59:59.999Z'), close: '79900' },
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T11:30:00.000Z'))).toBeUndefined();
    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T12:00:20.000Z'))?.close).toBe('80100');
  });

  test('does not execute an old matching close after service downtime', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-11T10:30:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T12:20:00.000Z'))).toBeUndefined();
  });

  test('only evaluates the newest closed candle', () => {
    const state: BtcManualTradeOverrideState = {
      version: 1,
      market: 'BTC-USD',
      status: 'ARMED',
      direction: 'long',
      closeTrigger: 80000,
      takeProfits: [],
      armedAt: '2026-09-10T10:30:00.000Z',
      expiresAt: '2026-09-11T10:30:00.000Z',
      updatedAt: '2026-09-10T10:30:00.000Z'
    };
    const candles = [
      { openTime: Date.parse('2026-09-10T11:00:00.000Z'), closeTime: Date.parse('2026-09-10T11:59:59.999Z'), close: '80100' },
      { openTime: Date.parse('2026-09-10T12:00:00.000Z'), closeTime: Date.parse('2026-09-10T12:59:59.999Z'), close: '79900' }
    ];

    expect(matchingManualOverrideCandle(state, candles, Date.parse('2026-09-10T13:00:20.000Z'))).toBeUndefined();
  });

  test('allocates the exact manual TP ladder at dYdX step size', () => {
    const levels = buildManualTakeProfitOrderLevels('long', [
      { price: 81000, allocationPct: 50 },
      { price: 82000, allocationPct: 30 },
      { price: 83000, allocationPct: 20 }
    ], 0.01, 0.0001, 80050);

    expect(levels.map((level) => [level.price, level.size, level.manual_locked])).toEqual([
      [81000, 0.005, true],
      [82000, 0.003, true],
      [83000, 0.002, true]
    ]);
  });

  test('rejects a manual TP already behind the live entry price', () => {
    expect(() => buildManualTakeProfitOrderLevels(
      'long',
      [{ price: 80100, allocationPct: 100 }],
      0.01,
      0.0001,
      80200
    )).toThrow('not beyond the current long entry price');
  });
});
