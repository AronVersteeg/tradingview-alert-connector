const { RIGHT_BUFFER, rightBoundary, keepRightSpace, presetRange, zoomRange } = require('../public/btc-spot-chart/viewport.js');

describe('BTC live projection viewport', () => {
  test('presets keep fifty logical candle slots beyond the projected right edge', () => {
    expect(RIGHT_BUFFER).toBe(50);
    expect(presetRange(1000, 10, 120)).toEqual({ from: 880, to: 1059 });
    expect(presetRange(1000, 10, 1000)).toEqual({ from: 0, to: 1059 });
    expect(rightBoundary(1000, 72) - (999 + 72)).toBe(50);
  });
  test('repeated live zoom preserves the edge and twelve actual candles at minimum', () => {
    let range = presetRange(1000, 10, 120);
    for (let i = 0; i < 30; i++) {
      range = zoomRange(range, 1000, 10, .75);
      expect(range.to - (999 + 10)).toBeGreaterThanOrEqual(50);
      expect(999 - range.from).toBeGreaterThanOrEqual(12);
    }
    expect(range).toEqual({ from: 987, to: 1059 });
    expect(zoomRange(range, 1000, 10, 1.33).from).toBeLessThan(range.from);
  });
  test('repairs pointer-anchored native zoom and adapts to longer edge offsets', () => {
    expect(keepRightSpace({ from: 940, to: 1005 }, 1000, 10)).toEqual({ from: 987, to: 1059 });
    const range = keepRightSpace({ from: 940, to: 1005 }, 1000, 72);
    expect(range.to).toBe(1121);
    expect(range.from).toBe(987);
    expect(keepRightSpace(range, 1000, 72)).toEqual(range);
  });
  test('keeps historical zoom centred instead of jumping back to live', () => {
    expect(zoomRange({ from: 100, to: 200 }, 1000, 10, .5)).toEqual({ from: 125, to: 175 });
  });
});
