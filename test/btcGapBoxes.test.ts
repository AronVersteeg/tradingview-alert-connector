const { timelineFromPayload, withDecentraderHistory, buildGapBoxes, GapBoxPrimitive } = require('../public/btc-spot-chart/gap-boxes.js');

const HOUR = 3600;
const start = Date.parse('2026-10-20T00:00:00Z') / 1000;
const source = { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H' }] };
const payload = (gaps: any[]) => ({ source, modelVersion: 'test', gaps,
  frames: gaps.map((_, index) => ({ startedAtMs: (start + index * HOUR) * 1000 })) });
const gap = (left = 80, right = 110) => ({ left, right });
const now = (start + 10 * HOUR) * 1000;

describe('BTC Public V2 gap rectangles', () => {
  const history = (records: any[]) => ({ symbol: 'BTCUSDT', venue: 'Decentrader map', historicalOnly: true,
    historicalAccuracy: 'observed-snapshots', records });
  const row = (index: number, lower = 80, upper = 110) => ({ candleTime: start + index * HOUR,
    availableAt: start + (index + 1) * HOUR, lower, upper });

  test('prepends older map boxes, verifies the handover and keeps Public V2 leading at every overlapping hour', () => {
    const live = timelineFromPayload(payload([gap(), gap(), gap(85)]));
    const archive = history([row(-3, 70, 120), row(-2), row(-1), row(0), row(1), row(2), row(3, 99, 200)]);
    const before = JSON.stringify({ live, archive });
    const combined = withDecentraderHistory(live, archive);
    expect(combined.records).toHaveLength(6);
    expect(combined.records.at(-1).lower).toBe(85); // Never extend the live feed with a newer Decentrader frame.
    expect(combined.history).toMatchObject({ records: 3, overlapCompared: 3, mismatches: 1, boundaryMatches: true });
    const boxes = buildGapBoxes(combined, now).boxes;
    expect(boxes[1]).toMatchObject({ fromTime: start - HOUR, lower: 80, startKnown: true, startSource: 'decentrader-map' });
    expect(boxes.at(-1)).toMatchObject({ lower: 85, openEnded: true, startSource: 'histomap' });
    expect(JSON.stringify({ live, archive })).toBe(before);
  });

  test('refuses to join mismatching or unverified source boundaries and retains missing-hour breaks', () => {
    const live = timelineFromPayload(payload([gap(), gap()]));
    for (const archive of [history([row(-2), row(-1), row(0, 81)]), history([row(-2), row(-1)])]) {
      const model = buildGapBoxes(withDecentraderHistory(live, archive), now);
      expect(model.boxes).toHaveLength(2);
      expect(model.boxes[1]).toMatchObject({ fromTime: start + HOUR, startKnown: false, startSource: 'unknown' });
    }
    expect(buildGapBoxes(withDecentraderHistory(live, history([row(-4), row(-1), row(0)])), now).boxes).toHaveLength(2);
  });

  test('rejects malformed historical sources, preserving the standalone live model as fallback', () => {
    const live = timelineFromPayload(payload([gap()]));
    for (const archive of [history([]), { ...history([row(-1)]), symbol: 'ETHUSDT' },
      { ...history([row(-1)]), historicalAccuracy: 'current-active-reconstruction' },
      { ...history([row(-1)]), historicalAccuracy: undefined },
      history([row(-1), row(-1)]), history([{ ...row(-1), lower: '80' }]), history([{ ...row(-1), availableAt: start + 1 }])]) {
      expect(() => withDecentraderHistory(live, archive)).toThrow();
    }
    expect(buildGapBoxes(live, now).boxes).toHaveLength(1);
  });

  test('reuses exact histomap edges, converting candle-open to actual close availability', () => {
    const original = payload([gap(79400, 87400), gap(79400, 87400)]);
    const timeline = timelineFromPayload(original);
    expect(timeline.records[0]).toEqual({ candleTime: start, availableAt: start + HOUR, lower: 79400, upper: 87400 });
    expect(original.gaps[0]).toEqual(gap(79400, 87400));
  });

  test('merges equal edges and starts a new rectangle at either edge change, never the browser fetch time', () => {
    const timeline = timelineFromPayload(payload([gap(), gap(), gap(81), gap(81, 112)]));
    const result = buildGapBoxes(timeline, now);
    expect(result.boxes.map((box: any) => [box.fromTime, box.toTime, box.lower, box.upper, box.startKnown, box.openEnded])).toEqual([
      [start + HOUR, start + 3 * HOUR, 80, 110, false, false],
      [start + 3 * HOUR, start + 4 * HOUR, 81, 110, true, false],
      [start + 4 * HOUR, start + 4 * HOUR, 81, 112, true, true]
    ]);
  });

  test('ignores unclosed source frames and handles null gaps without bridging them', () => {
    const timeline = timelineFromPayload(payload([gap(), null, gap(82), gap(85)]));
    const result = buildGapBoxes(timeline, (start + 3 * HOUR) * 1000);
    expect(result.boxes).toHaveLength(2);
    expect(result.boxes[0]).toMatchObject({ toTime: start + 2 * HOUR, openEnded: false });
    expect(result.boxes[1]).toMatchObject({ fromTime: start + 3 * HOUR, lower: 82, startKnown: true });
    expect(buildGapBoxes(timelineFromPayload(payload([gap(), null])), now).boxes[0].openEnded).toBe(false);
    expect(() => buildGapBoxes(timeline, start * 1000)).toThrow('Awaiting closed');
  });

  test('does not claim an exact birth after missing historical frames', () => {
    const data = payload([gap(), gap(83)]);
    data.frames[1].startedAtMs += 3 * HOUR * 1000;
    const boxes = buildGapBoxes(timelineFromPayload(data), now).boxes;
    expect(boxes[0]).toMatchObject({ toTime: start + 2 * HOUR, openEnded: false });
    expect(boxes[1]).toMatchObject({ startKnown: false, startSource: 'unknown' });
  });

  test('does not backdate a gap to a manual anchor when its earlier continuity cannot be verified', () => {
    const data = payload([gap(79400, 87400)]);
    data.frames[0].startedAtMs = Date.parse('2026-09-30T12:00:00Z');
    const box = buildGapBoxes(timelineFromPayload(data), Date.parse('2026-10-21T08:00:00Z')).boxes[0];
    expect(box).toMatchObject({ fromTime: Date.parse('2026-09-30T13:00:00Z') / 1000, startKnown: false, startSource: 'unknown' });
    data.frames[0].startedAtMs = Date.parse('2026-11-21T08:00:00Z');
    expect(buildGapBoxes(timelineFromPayload(data), Date.parse('2026-11-21T10:00:00Z')).boxes[0].startSource).toBe('unknown');
  });

  test('rejects other markets/venues and corrupt edges/timestamps rather than drawing false rectangles', () => {
    expect(() => timelineFromPayload({ ...payload([gap()]), source: { ...source, market: 'ETH-USD' } })).toThrow('source');
    expect(() => timelineFromPayload({ ...payload([gap()]), source: { ...source, sourceStatuses: [{ name: 'Binance Futures BTCUSDT 1H' }] } })).toThrow('source');
    for (const bad of [gap(0), gap(120, 100), gap(NaN), gap(80, Infinity), { left: '80', right: 100 }]) {
      expect(() => timelineFromPayload(payload([bad]))).toThrow('edges');
    }
    const duplicate = payload([gap(), gap()]); duplicate.frames[1] = duplicate.frames[0];
    expect(() => timelineFromPayload(duplicate)).toThrow('timestamp');
  });

  test('renderer draws a green outline, floats its fresh right cap and keeps stale caps at verified time', () => {
    jest.useFakeTimers(); jest.setSystemTime((start + 4 * HOUR) * 1000);
    try {
      const primitive = new GapBoxPrimitive();
      const ctx: any = {};
      for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'moveTo', 'lineTo', 'stroke', 'setLineDash']) ctx[name] = jest.fn();
      primitive.attached({ chart: { timeScale: () => ({ logicalToCoordinate: (index: number) => index * 10 }) },
        series: { priceToCoordinate: (value: number) => value }, requestUpdate: jest.fn() });
      const candles = Array.from({ length: 5 }, (_, i) => ({ time: start + i * HOUR }));
      const model = buildGapBoxes(timelineFromPayload(payload([gap(), gap(), gap(82), gap(82)])), Date.now());
      primitive.setData(model, candles);
      const draw = () => primitive.paneViews()[0].renderer().draw({ useMediaCoordinateSpace: (render: any) => render({ context: ctx, mediaSize: { width: 500, height: 300 } }) });
      draw();
      expect(ctx.strokeStyle).toBe('#00ff00');
      expect(primitive.lead).toBe(10);
      expect(ctx.lineTo.mock.calls).toContainEqual([140, 110]);
      expect(ctx.lineTo.mock.calls).toContainEqual([30, 82]);
      expect(ctx.lineTo.mock.calls).not.toContainEqual([10, 110]); // Unknown first birth stays open on the left.
      primitive.setOptions({ lead: 6 }); ctx.lineTo.mockClear(); draw();
      expect(ctx.lineTo.mock.calls).toContainEqual([100, 110]);
      jest.setSystemTime((start + 5 * HOUR) * 1000); ctx.lineTo.mockClear(); draw();
      expect(ctx.setLineDash).toHaveBeenCalledWith([5, 4]);
      expect(ctx.lineTo.mock.calls).toContainEqual([40, 110]);
      primitive.setOptions({ visible: false }); ctx.stroke.mockClear(); draw();
      expect(ctx.stroke).not.toHaveBeenCalled();
      primitive.detached();
    } finally { jest.useRealTimers(); }
  });

  test('right-axis labels follow only the active exact edges, hide with the overlay and never label stale history as active', () => {
    jest.useFakeTimers(); jest.setSystemTime((start + 4 * HOUR) * 1000);
    try {
      const primitive = new GapBoxPrimitive();
      primitive.attached({ chart: {}, series: { priceToCoordinate: (price: number) => price * 2,
        priceFormatter: () => ({ format: (price: number) => price.toFixed(2) }) }, requestUpdate: jest.fn() });
      const candles = Array.from({ length: 5 }, (_, i) => ({ time: start + i * HOUR }));
      primitive.setData(buildGapBoxes(timelineFromPayload(payload([gap(), gap(), gap(82, 112), gap(82, 112)])), Date.now()), candles);
      const labels = primitive.priceAxisViews();
      expect(labels.map((label: any) => label.text())).toEqual(['112.00', '82.00']);
      expect(labels.map((label: any) => label.coordinate())).toEqual([224, 164]);
      expect(labels.every((label: any) => label.visible() && label.tickVisible())).toBe(true);
      expect(labels[0].backColor()).toBe('#00ff00');
      primitive.setOptions({ visible: false });
      expect(labels.every((label: any) => !label.visible())).toBe(true);
      primitive.setOptions({ visible: true });
      primitive.setData(buildGapBoxes(timelineFromPayload(payload([gap(), gap(), gap(83, 114), gap(83, 114)])), Date.now()), candles);
      expect(labels.map((label: any) => label.text())).toEqual(['114.00', '83.00']);
      jest.setSystemTime((start + 5 * HOUR) * 1000);
      expect(labels.every((label: any) => !label.visible())).toBe(true);
      primitive.setData({ boxes: [], evaluatedThrough: start + 5 * HOUR }, candles);
      expect(labels.every((label: any) => !label.visible())).toBe(true);
      primitive.detached();
      expect(labels.every((label: any) => !label.visible())).toBe(true);
    } finally { jest.useRealTimers(); }
  });
});
