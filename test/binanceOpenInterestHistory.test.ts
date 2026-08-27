import { summarizeOpenInterestWindow } from '../src/services/binanceOpenInterestHistory';

describe('Binance Open Interest history', () => {
  test('summarizes only samples inside the causal Delay window', () => {
    const fromMs = Date.parse('2026-08-19T20:00:00.000Z');
    const toMs = Date.parse('2026-08-19T20:15:00.000Z');
    const result = summarizeOpenInterestWindow({
      symbol: 'BTCUSDT', fromMs, toMs,
      points: [
        { timestamp: fromMs - 300_000, contractOpenInterest: 150, openInterestUsd: 150_000 },
        { timestamp: fromMs, contractOpenInterest: 100, openInterestUsd: 100_000 },
        { timestamp: fromMs + 300_000, contractOpenInterest: 99, openInterestUsd: 99_500 },
        { timestamp: toMs, contractOpenInterest: 97.5, openInterestUsd: 98_000 },
        { timestamp: toMs + 300_000, contractOpenInterest: 50, openInterestUsd: 50_000 }
      ]
    });

    expect(result.samples).toBe(3);
    expect(result.contractChangePct).toBeCloseTo(-2.5);
    expect(result.usdChangePct).toBeCloseTo(-2);
    expect(result.firstTimestamp).toBe('2026-08-19T20:00:00.000Z');
    expect(result.lastTimestamp).toBe('2026-08-19T20:15:00.000Z');
  });
});
