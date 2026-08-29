import {
  coinGlassFailureBackoffMs,
  coinGlassRefreshWaitMs
} from '../src/services/coinGlassRefreshPolicy';

describe('CoinGlass refresh policy', () => {
  test('backs off exponentially after consecutive failures and caps the delay', () => {
    expect(coinGlassFailureBackoffMs(1, 60_000, 600_000)).toBe(60_000);
    expect(coinGlassFailureBackoffMs(2, 60_000, 600_000)).toBe(120_000);
    expect(coinGlassFailureBackoffMs(4, 60_000, 600_000)).toBe(480_000);
    expect(coinGlassFailureBackoffMs(5, 60_000, 600_000)).toBe(600_000);
    expect(coinGlassFailureBackoffMs(20, 60_000, 600_000)).toBe(600_000);
  });

  test('uses last attempt time after a failure instead of stale fetchedAt', () => {
    const waitMs = coinGlassRefreshWaitMs({
      now: 1_030_000,
      fetchedAt: 100_000,
      lastAttemptAt: 1_000_000,
      consecutiveFailures: 2,
      pollMs: 600_000,
      failureBackoffBaseMs: 60_000,
      failureBackoffMaxMs: 600_000
    });

    expect(waitMs).toBe(90_000);
  });

  test('returns to normal polling after a successful refresh', () => {
    const waitMs = coinGlassRefreshWaitMs({
      now: 1_030_000,
      fetchedAt: 1_000_000,
      lastAttemptAt: 1_000_000,
      consecutiveFailures: 0,
      pollMs: 600_000,
      failureBackoffBaseMs: 60_000,
      failureBackoffMaxMs: 600_000
    });

    expect(waitMs).toBe(570_000);
  });
});
