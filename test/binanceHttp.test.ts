import { binanceRateLimitUntil, isBinanceRateLimitError } from '../src/services/binanceHttp';

describe('Binance REST rate-limit handling', () => {
  test('recognizes Binance code -1003 and honors the longest cooldown', () => {
    const now = Date.parse('2026-08-26T14:13:14.000Z');
    const bannedUntil = Date.parse('2026-08-26T14:29:57.922Z');
    const error = {
      response: {
        status: 418,
        headers: { 'retry-after': '1004' },
        data: {
          code: -1003,
          msg: `Way too many requests; IP banned until ${bannedUntil}.`
        }
      }
    };

    expect(isBinanceRateLimitError(error)).toBe(true);
    expect(binanceRateLimitUntil(error, now)).toBe(now + 1_004_000);
  });

  test('recognizes HTTP 429 without a Binance response body', () => {
    const now = Date.parse('2026-08-26T14:13:14.000Z');
    const error = { response: { status: 429, headers: { 'retry-after': '60' } } };

    expect(isBinanceRateLimitError(error)).toBe(true);
    expect(binanceRateLimitUntil(error, now)).toBe(now + 60_000);
  });
});
