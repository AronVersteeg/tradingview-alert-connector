import { intrusionCandleReview } from '../src/services/decentraderGapMonitor';

function leftEdgeAlert(timestamp = '2026-07-14 22:00:00') {
  return {
    frameIndex: 1,
    timestamp,
    timestampNl: '15-07-2026 00:00 NL',
    price: 64788,
    previousGap: {},
    entrants: [{}],
    left: [{}],
    right: []
  } as any;
}

const rows = [
  { timestamp: '2026-07-14 21:00:00', ohlc4: '64000' },
  { timestamp: '2026-07-14 22:00:00', ohlc4: '64500' }
] as any;

describe('Decentrader intrusion candle filter', () => {
  test('passes when every fully closed dYdX candle in The Delay has the expected color', () => {
    const dydxCandles = [
      { startedAt: '2026-07-14T22:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64500', close: '64900' },
      { startedAt: '2026-07-15T00:00:00.000Z', open: '64900', close: '65100' },
      { startedAt: '2026-07-15T01:00:00.000Z', open: '65100', close: '65300' }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      dydxCandles,
      '2026-07-15T01:42:00.000Z'
    );

    expect(review.status).toBe('PASS');
    expect(review.closedCandlesChecked).toBe(3);
    expect(review.candleColors).toEqual(['green', 'green', 'green']);
    expect(review.candleTimestamps).toEqual([
      '2026-07-14T22:00:00.000Z',
      '2026-07-14T23:00:00.000Z',
      '2026-07-15T00:00:00.000Z'
    ]);
    expect(review.nextTimestamp).toBe('2026-07-14T23:00:00.000Z');
    expect(review.source).toBe('dydx');
  });

  test('fails when any fully closed candle in The Delay has the wrong color', () => {
    const dydxCandles = [
      { startedAt: '2026-07-14T22:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64900', close: '64600' },
      { startedAt: '2026-07-15T00:00:00.000Z', open: '64600', close: '65100' }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      dydxCandles,
      '2026-07-15T01:05:00.000Z'
    );

    expect(review.status).toBe('FAIL');
    expect(review.candleColors).toEqual(['green', 'red', 'green']);
    expect(review.firstMismatchTimestamp).toBe('2026-07-14T23:00:00.000Z');
  });

  test('excludes the candle that is still open at SMTP acceptance', () => {
    const dydxCandles = [
      { startedAt: '2026-07-14T22:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64500', close: '64900' },
      { startedAt: '2026-07-15T00:00:00.000Z', open: '65000', close: '64000' }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      dydxCandles,
      '2026-07-15T00:30:00.000Z'
    );

    expect(review.status).toBe('PASS');
    expect(review.closedCandlesChecked).toBe(2);
    expect(review.candleColors).toEqual(['green', 'green']);
  });

  test('keeps the review pending until the normal intrusion email has an SMTP timestamp', () => {
    const dydxCandles = [
      { startedAt: '2026-07-14T22:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64500', close: '64900' }
    ] as any;

    const review = intrusionCandleReview(rows, leftEdgeAlert(), true, dydxCandles);

    expect(review.status).toBe('PENDING');
    expect(review.reason).toContain('accepted by SMTP');
  });

  test('fails when SMTP arrives before two candles have closed', () => {
    const dydxCandles = [
      { startedAt: '2026-07-14T22:00:00.000Z', open: '64000', close: '64500' },
      { startedAt: '2026-07-14T23:00:00.000Z', open: '64500', close: '64900' }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      dydxCandles,
      '2026-07-14T23:30:00.000Z'
    );

    expect(review.status).toBe('FAIL');
    expect(review.reason).toContain('at least 2');
  });

  test('passes Binance Futures confirmation when both price candles and taker deltas match', () => {
    const binanceCandles = [
      {
        startedAt: '2026-07-14T22:00:00.000Z',
        open: '64000',
        close: '64500',
        source: 'binance-futures',
        volumeDeltaQuote: 1_500_000
      },
      {
        startedAt: '2026-07-14T23:00:00.000Z',
        open: '64500',
        close: '64900',
        source: 'binance-futures',
        volumeDeltaQuote: 750_000
      }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      binanceCandles,
      '2026-07-15T00:30:00.000Z',
      true
    );

    expect(review.status).toBe('PASS');
    expect(review.source).toBe('binance-futures');
    expect(review.candleColors).toEqual(['green', 'green']);
    expect(review.volumeDeltaColors).toEqual(['green', 'green']);
    expect(review.volumeDeltaQuote).toEqual([1_500_000, 750_000]);
  });

  test('fails closed when a Binance taker delta opposes otherwise matching price candles', () => {
    const binanceCandles = [
      {
        startedAt: '2026-07-14T22:00:00.000Z',
        open: '64000',
        close: '64500',
        source: 'binance-futures',
        volumeDeltaQuote: 1_500_000
      },
      {
        startedAt: '2026-07-14T23:00:00.000Z',
        open: '64500',
        close: '64900',
        source: 'binance-futures',
        volumeDeltaQuote: -250_000
      }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      binanceCandles,
      '2026-07-15T00:30:00.000Z',
      true
    );

    expect(review.status).toBe('FAIL');
    expect(review.candleColors).toEqual(['green', 'green']);
    expect(review.volumeDeltaColors).toEqual(['green', 'red']);
    expect(review.firstVolumeDeltaMismatchTimestamp).toBe('2026-07-14T23:00:00.000Z');
  });

  test('keeps Binance review pending when taker delta data is incomplete', () => {
    const binanceCandles = [
      {
        startedAt: '2026-07-14T22:00:00.000Z',
        open: '64000',
        close: '64500',
        source: 'binance-futures',
        volumeDeltaQuote: 1_500_000
      },
      {
        startedAt: '2026-07-14T23:00:00.000Z',
        open: '64500',
        close: '64900',
        source: 'binance-futures'
      }
    ] as any;

    const review = intrusionCandleReview(
      rows,
      leftEdgeAlert(),
      true,
      binanceCandles,
      '2026-07-15T00:30:00.000Z',
      true
    );

    expect(review.status).toBe('PENDING');
    expect(review.reason).toContain('taker-volume delta');
  });
});
