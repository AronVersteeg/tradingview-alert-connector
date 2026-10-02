import { binanceRateLimitUntil, isBinanceRateLimitError, binanceRequestWeight } from '../src/services/binanceHttp';

describe('Binance REST rate-limit handling', () => {
  test('accounts for weighted Futures candle and depth requests', () => {
    expect(binanceRequestWeight('https://fapi.binance.com/fapi/v1/klines', { params: { limit: 12 } })).toBe(1);
    expect(binanceRequestWeight('https://fapi.binance.com/fapi/v1/klines', { params: { limit: 1500 } })).toBe(10);
    expect(binanceRequestWeight('https://fapi.binance.com/fapi/v1/depth', { params: { limit: 500 } })).toBe(10);
  });
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
