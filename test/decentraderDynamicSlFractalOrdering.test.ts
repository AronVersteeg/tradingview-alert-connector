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

  test('never substitutes an OHLC4 pivot for a missing Williams low fractal', () => {
    process.env.DECENTRADER_SL_FRACTAL_WINDOW = '2';
    const candles = [
      ['2026-08-20T23:00:00.000Z', 72613, 73356, 72575, 72950],
      ['2026-08-21T00:00:00.000Z', 72984, 73948, 72979, 73618],
      ['2026-08-21T01:00:00.000Z', 73616, 75677, 73616, 75031],
      ['2026-08-21T02:00:00.000Z', 75023, 75089, 74229, 74406],
      ['2026-08-21T03:00:00.000Z', 74383, 74770, 74174, 74517],
      ['2026-08-21T04:00:00.000Z', 74617, 74988, 74613, 74937],
      ['2026-08-21T05:00:00.000Z', 74935, 75433, 74819, 75398]
    ].map(([startedAt, open, high, low, close]) => ({
      startedAt: String(startedAt),
      resolution: '1HOUR',
      open: String(open),
      high: String(high),
      low: String(low),
      close: String(close)
    }));
    const rows = dydxHourlyCandlesToFractalRows(
      candles,
      Date.parse('2026-08-21T06:30:00.000Z')
    );

    const stop = buildFractalStop(rows as any, rows.length - 1, 'long', 76_200, {
      enforceMinDistance: false
    });

    // OHLC4 has a local pivot around 03:00 whose wick guard would yield
    // 73,616 (and a 73,542.384 stop), but no Williams low fractal exists.
    expect(stop.valid).toBe(false);
    expect(stop.source).toBe('missing-fractal');
  });

  test('selects the latest confirmed Williams low from the live BTC sequence', () => {
    process.env.DECENTRADER_SL_FRACTAL_WINDOW = '2';
    const candles = [
      ['2026-08-20T11:00:00.000Z', 71891, 72232, 71778, 71808],
      ['2026-08-20T12:00:00.000Z', 71853, 71921, 71461, 71835],
      ['2026-08-20T13:00:00.000Z', 71793, 72117, 71060, 71552],
      ['2026-08-20T14:00:00.000Z', 71538, 71879, 71300, 71708],
      ['2026-08-20T15:00:00.000Z', 71714, 72775, 71714, 72445],
      ['2026-08-20T16:00:00.000Z', 72460, 72864, 72176, 72799],
      ['2026-08-20T17:00:00.000Z', 72775, 73021, 72540, 72790],
      ['2026-08-20T18:00:00.000Z', 72802, 72802, 72310, 72349],
      ['2026-08-20T19:00:00.000Z', 72369, 72734, 72284, 72690],
      ['2026-08-20T20:00:00.000Z', 72610, 72979, 72412, 72602],
      ['2026-08-20T21:00:00.000Z', 72585, 72879, 72510, 72596],
      ['2026-08-20T22:00:00.000Z', 72677, 72778, 72460, 72638],
      ['2026-08-20T23:00:00.000Z', 72613, 73356, 72575, 72950],
      ['2026-08-21T00:00:00.000Z', 72984, 73948, 72979, 73618],
      ['2026-08-21T01:00:00.000Z', 73616, 75677, 73616, 75031],
      ['2026-08-21T02:00:00.000Z', 75023, 75089, 74229, 74406],
      ['2026-08-21T03:00:00.000Z', 74383, 74770, 74174, 74517],
      ['2026-08-21T04:00:00.000Z', 74617, 74988, 74613, 74937],
      ['2026-08-21T05:00:00.000Z', 74935, 75433, 74819, 75398]
    ].map(([startedAt, open, high, low, close]) => ({
      startedAt: String(startedAt),
      resolution: '1HOUR',
      open: String(open),
      high: String(high),
      low: String(low),
      close: String(close)
    }));
    const rows = dydxHourlyCandlesToFractalRows(
      candles,
      Date.parse('2026-08-21T06:30:00.000Z')
    );

    const stop = buildFractalStop(rows as any, rows.length - 1, 'long', 76_200, {
      fractalDelay: 0,
      enforceMinDistance: false
    });

    expect(stop.valid).toBe(true);
    expect(stop.fractal).toMatchObject({
      kind: 'bottom',
      price: 72_284,
      timestamp: '2026-08-20T19:00:00.000Z',
      source: 'lowRef'
    });
    expect(stop.price).toBeCloseTo(72_211.716, 6);
  });
});
