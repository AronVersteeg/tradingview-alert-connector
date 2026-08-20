import {
  buildFractalStop,
  dydxHourlyCandlesToFractalRows
} from '../src/services/decentraderGapMonitor';

describe('dynamic SL fractal ordering', () => {
  const previousWindow = process.env.DECENTRADER_SL_FRACTAL_WINDOW;
  const previousLookback = process.env.DECENTRADER_SL_LOOKBACK_BARS;
  const previousMaxDistance = process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
  const previousBuffer = process.env.DECENTRADER_SL_BUFFER_PCT;
  const previousMinDistance = process.env.DECENTRADER_SL_MIN_DISTANCE_PCT;
  const previousRangeBuffer = process.env.DECENTRADER_SL_BUFFER_RANGE_MULTIPLIER;

  beforeAll(() => {
    process.env.DECENTRADER_SL_FRACTAL_WINDOW = '1';
    process.env.DECENTRADER_SL_LOOKBACK_BARS = '72';
    process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';
    process.env.DECENTRADER_SL_BUFFER_PCT = '0.001';
    process.env.DECENTRADER_SL_MIN_DISTANCE_PCT = '0.0025';
    process.env.DECENTRADER_SL_BUFFER_RANGE_MULTIPLIER = '100';
  });

  afterAll(() => {
    if (previousWindow === undefined) delete process.env.DECENTRADER_SL_FRACTAL_WINDOW;
    else process.env.DECENTRADER_SL_FRACTAL_WINDOW = previousWindow;
    if (previousLookback === undefined) delete process.env.DECENTRADER_SL_LOOKBACK_BARS;
    else process.env.DECENTRADER_SL_LOOKBACK_BARS = previousLookback;
    if (previousMaxDistance === undefined) delete process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
    else process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = previousMaxDistance;
    if (previousBuffer === undefined) delete process.env.DECENTRADER_SL_BUFFER_PCT;
    else process.env.DECENTRADER_SL_BUFFER_PCT = previousBuffer;
    if (previousMinDistance === undefined) delete process.env.DECENTRADER_SL_MIN_DISTANCE_PCT;
    else process.env.DECENTRADER_SL_MIN_DISTANCE_PCT = previousMinDistance;
    if (previousRangeBuffer === undefined) delete process.env.DECENTRADER_SL_BUFFER_RANGE_MULTIPLIER;
    else process.env.DECENTRADER_SL_BUFFER_RANGE_MULTIPLIER = previousRangeBuffer;
  });

  test('uses the stable timestamp when rolling history reuses an old row index', () => {
    const lows = [100, 96, 90, 97, 101, 99, 94, 100, 102];
    const rows = lows.map((lowRef, hour) => ({
      timestamp: `2026-08-20 ${String(hour).padStart(2, '0')}:00:00`,
      lowRef,
      highRef: lowRef + 10,
      ohlc4: lowRef + 5
    }));

    const stop = buildFractalStop(rows as any, rows.length - 1, 'long', 110, {
      // Index 6 can refer to an older fractal after the fixed-size history window rolls.
      afterFractalIndex: 6,
      afterFractalTimestamp: '2026-08-20 02:00:00',
      fractalDelay: 0
    });

    expect(stop.valid).toBe(true);
    expect(stop.fractal?.index).toBe(6);
    expect(stop.fractal?.timestamp).toBe('2026-08-20 06:00:00');
    expect(stop.newerFractalCount).toBe(1);
  });

  test('maps only closed dYdX 1H candles and preserves their true wicks', () => {
    const candles = [
      { startedAt: '2026-08-20T03:00:00.000Z', resolution: '1HOUR', open: '103', high: '108', low: '101', close: '106' },
      { startedAt: '2026-08-20T01:00:00.000Z', resolution: '1HOUR', open: '100', high: '105', low: '95', close: '102' },
      { startedAt: '2026-08-20T02:00:00.000Z', resolution: '1HOUR', open: '102', high: '106', low: '99', close: '103' }
    ];

    const rows = dydxHourlyCandlesToFractalRows(
      candles,
      Date.parse('2026-08-20T03:30:00.000Z')
    );

    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.timestamp)).toEqual([
      '2026-08-20T01:00:00.000Z',
      '2026-08-20T02:00:00.000Z'
    ]);
    expect(rows[0]).toMatchObject({ highRef: 105, lowRef: 95, fractalCandleSource: 'dydx-1h' });
  });

  test('uses the fixed 0.1% buffer without widening a trailing stop to 0.25%', () => {
    const rows = [100, 90, 100].map((lowRef, hour) => ({
      timestamp: `2026-08-20 ${String(hour).padStart(2, '0')}:00:00`,
      lowRef,
      highRef: lowRef + 10,
      ohlc4: lowRef + 5
    }));

    const stop = buildFractalStop(rows as any, rows.length - 1, 'long', 110, {
      enforceMinDistance: false
    });

    expect(stop.valid).toBe(true);
    expect(stop.buffer).toBeCloseTo(0.09, 8);
    expect(stop.price).toBeCloseTo(89.91, 8);
    expect(stop.adjustedToMinDistance).toBe(false);
  });
});
