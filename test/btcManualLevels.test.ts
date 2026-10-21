const { fromSnapshot, buildBands, ManualLevelLines } = require('../public/btc-spot-chart/manual-levels.js');

const plan = (changes: any = {}) => ({ id: 'long-a', market: 'BTC-USD', status: 'ARMED', direction: 'long',
  closeTrigger: 100, armedAt: '2026-10-21T10:00:00Z', takeProfits: [{ price: 110, allocationPct: 50 }, { price: 120, allocationPct: 30 }, { price: 130, allocationPct: 20 }], ...changes });
const snapshot = (overrides: any[]) => ({ market: 'BTC-USD', enabled: true, overrides });

describe('read-only manual entry and TP overlays', () => {
  test('extracts only planned BTC entries and their explicit TPs, never SLs or secrets', () => {
    const input = snapshot([plan({ stopLoss: 90, result: { secret: true } }),
      plan({ id: 'short-b', direction: 'short', closeTrigger: 140, status: 'EXECUTING', takeProfits: [{ price: 135 }] }),
      ...['TRIGGERED', 'CANCELLED', 'EXPIRED', 'SKIPPED', 'ERROR', 'IDLE'].map((status) => plan({ status }))]);
    const model = fromSnapshot(input);
    expect(model.planCount).toBe(2);
    expect(model.levels.map((level: any) => level.price)).toEqual([100, 110, 120, 130, 140, 135]);
    expect(model.levels[0].fromTime).toBe(Date.parse('2026-10-21T10:00:00Z') / 1000);
    expect(JSON.stringify(model)).not.toMatch(/stopLoss|secret|result/);
    expect(input.overrides[0].takeProfits).toHaveLength(3);
  });

  test('supports legacy plans, disabled feature and dynamic TP plans without inventing targets', () => {
    const legacy = fromSnapshot({ market: 'BTC-USD', enabled: false, override: plan({ takeProfits: [] }) });
    expect(legacy.enabled).toBe(false);
    expect(legacy.levels).toHaveLength(1);
    expect(buildBands(legacy.levels)).toEqual([]);
    expect(fromSnapshot(snapshot([])).levels).toEqual([]);
    expect(() => fromSnapshot({ market: 'ETH-USD', overrides: [] })).toThrow();
    expect(() => fromSnapshot({ market: 'BTC-USD' })).toThrow();
  });

  test('rejects malformed or other-market prices without rendering zero/NaN levels', () => {
    const model = fromSnapshot(snapshot([plan({ closeTrigger: 0 }), plan({ closeTrigger: Infinity }),
      plan({ market: 'ETH-USD' }), plan({ direction: 'sideways' }), plan({ takeProfits: [null, { price: -1 }, { price: NaN }, { price: '110' }] })]));
    expect(model.levels.map((level: any) => level.price)).toEqual([100]);
  });

  test('builds consecutive progressively lighter target bands for longs and shorts', () => {
    const model = fromSnapshot(snapshot([plan({ takeProfits: [{ price: 130 }, { price: 110 }, { price: 120 }, { price: 110 }] }),
      plan({ id: 'short', direction: 'short', closeTrigger: 150, takeProfits: [{ price: 120 }, { price: 140 }, { price: 130 }] })]));
    const bands = buildBands(model.levels);
    expect(bands.map((band: any) => [band.fromPrice, band.toPrice])).toEqual([[100, 110], [110, 120], [120, 130], [150, 140], [140, 130], [130, 120]]);
    expect(bands.slice(0, 3).map((band: any) => band.color)).toEqual(['22, 76, 166', '55, 125, 218', '110, 175, 245']);
    expect(buildBands(fromSnapshot(snapshot([plan({ takeProfits: [{ price: 99 }] })])).levels)).toEqual([]);
  });

  function fakeSeries() {
    const series = { createPriceLine: jest.fn((options) => ({ options, applyOptions: jest.fn() })),
      removePriceLine: jest.fn(), priceToCoordinate: (price: number) => 200 - price };
    return series;
  }

  test('reconciles edits, TP removals, same-price plans, cancellation and visibility', () => {
    const series = fakeSeries(), layer = new ManualLevelLines(series);
    layer.setData(fromSnapshot(snapshot([plan(), plan({ id: 'long-b', takeProfits: [] })])));
    expect(series.createPriceLine).toHaveBeenCalledTimes(4);
    expect(series.createPriceLine.mock.calls[0][0]).toMatchObject({ price: 100, lineStyle: 0, axisLabelVisible: true });
    expect(series.createPriceLine.mock.calls[1][0]).toMatchObject({ price: 110, lineStyle: 2 });
    layer.setData(fromSnapshot(snapshot([plan({ closeTrigger: 105, takeProfits: [{ price: 110 }] })])));
    expect(series.removePriceLine).toHaveBeenCalledTimes(3);
    expect(layer.visiblePrices()).toEqual([105, 110]);
    expect(series.createPriceLine.mock.results[1].value.applyOptions).toHaveBeenLastCalledWith(expect.objectContaining({ title: 'L 105 TP1' }));
    layer.setVisible(false);
    expect(layer.visiblePrices()).toEqual([]);
    expect(series.createPriceLine.mock.results[1].value.applyOptions).toHaveBeenLastCalledWith(expect.objectContaining({ lineVisible: false, axisLabelVisible: false }));
    layer.setVisible(true);
    layer.setData(fromSnapshot(snapshot([plan({ status: 'CANCELLED' })])));
    expect(layer.lines.size).toBe(0);
    expect(layer.bands).toEqual([]);
    layer.destroy();
  });

  test('draws real independent blue bands behind candles and clears them with their plans', () => {
    const series = fakeSeries(), layer = new ManualLevelLines(series);
    const ctx: any = {};
    expect(layer.lead).toBe(10);
    layer.setLead(3);
    for (const name of ['save', 'restore', 'beginPath', 'rect', 'clip', 'fillRect', 'moveTo', 'lineTo', 'stroke']) ctx[name] = jest.fn();
    layer.attached({ chart: { timeScale: () => ({ logicalToCoordinate: (index: number) => index * 10 }) }, requestUpdate: jest.fn() });
    const start = Date.parse(plan().armedAt) / 1000;
    layer.setCandles(Array.from({ length: 12 }, (_, i) => ({ time: start + i * 3600 })));
    layer.setData(fromSnapshot(snapshot([plan()])));
    const target = { useMediaCoordinateSpace: (fn: any) => fn({ context: ctx, mediaSize: { width: 200, height: 200 } }) };
    layer.draw(target);
    expect(layer.paneViews()[0].zOrder()).toBe('bottom');
    expect(ctx.fillRect.mock.calls).toEqual([[0, 90, 140, 10], [0, 80, 140, 10], [0, 70, 140, 10]]);
    // Interpolate sub-hour plan starts; the vendored scale accepts integer indices.
    layer.setData(fromSnapshot(snapshot([plan({ armedAt: new Date((start + 1800) * 1000).toISOString() })])));
    layer.draw(target);
    expect(ctx.fillRect.mock.calls[3]).toEqual([5, 90, 135, 10]);
    layer.setVisible(false); layer.draw(target);
    expect(ctx.fillRect).toHaveBeenCalledTimes(6);
    layer.setVisible(true); layer.setData(fromSnapshot(snapshot([]))); layer.draw(target);
    expect(ctx.fillRect).toHaveBeenCalledTimes(6);
  });
});
