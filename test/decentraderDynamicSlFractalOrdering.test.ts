import { buildFractalStop } from '../src/services/decentraderGapMonitor';

describe('dynamic SL fractal ordering', () => {
  const previousWindow = process.env.DECENTRADER_SL_FRACTAL_WINDOW;
  const previousLookback = process.env.DECENTRADER_SL_LOOKBACK_BARS;
  const previousMaxDistance = process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;

  beforeAll(() => {
    process.env.DECENTRADER_SL_FRACTAL_WINDOW = '1';
    process.env.DECENTRADER_SL_LOOKBACK_BARS = '72';
    process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';
  });

  afterAll(() => {
    if (previousWindow === undefined) delete process.env.DECENTRADER_SL_FRACTAL_WINDOW;
    else process.env.DECENTRADER_SL_FRACTAL_WINDOW = previousWindow;
    if (previousLookback === undefined) delete process.env.DECENTRADER_SL_LOOKBACK_BARS;
    else process.env.DECENTRADER_SL_LOOKBACK_BARS = previousLookback;
    if (previousMaxDistance === undefined) delete process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
    else process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = previousMaxDistance;
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
});
