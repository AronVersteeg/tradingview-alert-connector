import { intrusionCandleReview } from '../src/services/decentraderGapMonitor';

describe('Decentrader intrusion candle filter', () => {
  test('passes from two closed dYdX candles without waiting for the next Decentrader frame', () => {
    const rows = [
      { timestamp: '2026-07-14 21:00:00', ohlc4: '64000' },
      { timestamp: '2026-07-14 22:00:00', ohlc4: '64500' }
    ] as any;
    const alert = {
      frameIndex: 1,
      timestamp: '2026-07-14 22:00:00',
      timestampNl: '15-07-2026 00:00 NL',
      price: 64788,
      previousGap: {},
      entrants: [{}],
      left: [{}],
      right: []
    } as any;
    const dydxCandles = [
      {
        startedAt: '2026-07-14T22:00:00.000Z',
        open: '64000',
        close: '64500'
      },
      {
        startedAt: '2026-07-14T23:00:00.000Z',
        open: '64500',
        close: '64900'
      }
    ] as any;

    const review = intrusionCandleReview(rows, alert, true, dydxCandles);

    expect(rows).toHaveLength(2);
    expect(review.status).toBe('PASS');
    expect(review.intrusionColor).toBe('green');
    expect(review.nextColor).toBe('green');
    expect(review.nextTimestamp).toBe('2026-07-14T23:00:00.000Z');
    expect(review.source).toBe('dydx');
  });

  test('keeps the review pending while the following dYdX candle is still open', () => {
    const now = jest.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-07-15T00:30:00.000Z'));
    const rows = [
      { timestamp: '2026-07-14 22:00:00', ohlc4: '64000' },
      { timestamp: '2026-07-14 23:00:00', ohlc4: '64500' }
    ] as any;
    const alert = {
      frameIndex: 1,
      timestamp: '2026-07-14 23:00:00',
      timestampNl: '15-07-2026 01:00 NL',
      price: 64788,
      previousGap: {},
      entrants: [{}],
      left: [{}],
      right: []
    } as any;
    const dydxCandles = [
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-15T00:00:00.000Z', open: '64500', close: '64900' }
    ] as any;

    try {
      const review = intrusionCandleReview(rows, alert, true, dydxCandles);
      expect(review.status).toBe('PENDING');
      expect(review.reason).toContain('following 1H candle to close');
    } finally {
      now.mockRestore();
    }
  });
});
