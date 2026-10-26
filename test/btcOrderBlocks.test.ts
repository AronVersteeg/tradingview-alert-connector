const { buildOrderBlocks, clusterActiveBlocks, placeClusterLabels, brokenRankGroups, OrderBlockPrimitive } = require('../public/btc-spot-chart/order-blocks.js');
const HOUR = 3600, start = Date.parse('2026-10-01T00:00:00Z') / 1000;
const bar = (i: number, changes: any = {}) => ({ time: start + i * HOUR, open: 100, high: 115, low: 95,
  close: 100, volume: 1, closed: true, ...changes });
const bull = () => [bar(0), bar(1, { high: 110, low: 90, volume: 10 }), bar(2)];
const bear = () => [bar(0), bar(1, { high: 110, low: 90, volume: 10 }), bar(2, { high: 105, low: 85 })];
const build = (candles: any[], options: any = { length: 1 }, now = (start + 1000 * HOUR) * 1000) => buildOrderBlocks(candles, options, now);

describe('LuxAlgo volume-pivot order block reconstruction', () => {
  test('matches bullish low-to-hl2 and bearish hl2-to-high coordinates, including the average', () => {
    expect(build(bull()).blocks).toEqual([expect.objectContaining({ direction: 'bullish', bottom: 90, top: 100, average: 95,
      originTime: start + HOUR, confirmedAt: start + 3 * HOUR, state: 'intact' })]);
    expect(build(bear()).blocks).toEqual([expect.objectContaining({ direction: 'bearish', bottom: 100, top: 110, average: 105 })]);
  });
  test('confirms a default pivot only after five closed right candles, anchored five candles earlier', () => {
    const candles = Array.from({ length: 11 }, (_, i) => bar(i, i === 5 ? { high: 110, low: 90, volume: 100 } : {}));
    expect(buildOrderBlocks(candles.slice(0, 10)).blocks).toHaveLength(0);
    expect(buildOrderBlocks(candles).blocks).toEqual([expect.objectContaining({ originTime: start + 5 * HOUR,
      confirmedAt: start + 11 * HOUR, direction: 'bullish' })]);
    candles[10].closed = false;
    expect(buildOrderBlocks(candles).blocks).toHaveLength(0);
  });
  test('uses actual base volume, accepts older volume ties and rejects newer equal peaks or zero peaks', () => {
    const olderTie = bull(); olderTie[0].volume = 10;
    expect(build(olderTie).blocks).toHaveLength(1);
    const newerTie = bull(); newerTie[2].volume = 10;
    expect(build(newerTie).blocks).toHaveLength(0);
    expect(build(bull().map(c => ({ ...c, volume: 0 }))).blocks).toHaveLength(0);
  });
  test('gives the high-price branch precedence and otherwise retains the previous direction', () => {
    const both = bull(); both[2].high = 105;
    expect(build(both).blocks[0].direction).toBe('bearish');
    const retained = [...bull(), bar(3, { high: 115, low: 95, volume: 20 }), bar(4)];
    expect(build(retained).blocks.at(-1).direction).toBe('bullish');
  });
  test('retains broken blocks, with strict Wick/Close mitigation and exact causal break timestamps', () => {
    const wick = [...bull(), bar(3, { low: 89 })];
    expect(build(wick).blocks[0]).toMatchObject({ state: 'broken', brokenTime: start + 3 * HOUR, brokenAt: start + 4 * HOUR });
    expect(build(wick, { length: 1, mitigation: 'Close' }).blocks[0].state).toBe('intact');
    const close = [...wick, bar(4, { low: 88, close: 89 })];
    expect(build(close, { length: 1, mitigation: 'Close' }).blocks[0].brokenAt).toBe(start + 5 * HOUR);
    expect(build([...bull(), bar(3, { low: 90 })]).blocks[0].state).toBe('intact');
    expect(build([...bear(), bar(3, { high: 111 })]).blocks[0].state).toBe('broken');
    expect(build([...bear(), bar(3, { high: 110 })]).blocks[0].state).toBe('intact');
  });
  test('does not use open/future candles or leak later breaks into earlier replay', () => {
    const candles = [...bull(), bar(3, { low: 89 }), bar(4, { volume: 1000, low: 80 })];
    for (let n = 1; n <= candles.length; n++) {
      const now = (start + n * HOUR) * 1000;
      expect(build(candles, { length: 1 }, now)).toEqual(build(candles.slice(0, n), { length: 1 }, now));
    }
    expect(build([...bull(), bar(3, { closed: false, low: 1 })]).blocks[0].state).toBe('intact');
  });
  test('does not invent mitigation or carry state across missing hours', () => {
    const model = build([...bull(), bar(7), bar(8, { volume: 10, high: 110, low: 90 }), bar(9)]);
    expect(model.gaps).toBe(1);
    expect(model.blocks[0]).toMatchObject({ state: 'coverage-gap', brokenAt: null, coverageEndedAt: start + 3 * HOUR });
    expect(model.blocks[1]).toMatchObject({ state: 'intact', originTime: start + 8 * HOUR });
    expect(model.blocks[0]).not.toHaveProperty('volumeRankAtBreak');
  });
  test('keeps historical blocks beyond LuxAlgo display caps, and stable IDs after older bars are prepended', () => {
    const candles = Array.from({ length: 100 }, (_, i) => bar(i, i % 4 === 1 ? { volume: 10, high: 110, low: 90 } : {}));
    const all = build(candles), tail = build(candles.slice(20));
    expect(all.blocks.length).toBeGreaterThan(3);
    for (const block of tail.blocks) expect(all.blocks).toContainEqual(block);
  });
  test('rejects malformed data, duplicates and invalid controls instead of inventing zero volume', () => {
    for (const changes of [{ volume: undefined }, { volume: -1 }, { high: 80 }, { low: 101 }, { time: start + .1 }]) {
      expect(() => build([bar(0, changes)])).toThrow('Invalid Spot');
    }
    expect(() => build([bar(0), bar(0)])).toThrow('Invalid Spot');
    for (const options of [{ length: 0 }, { length: 101 }, { length: 1.5 }, { mitigation: 'Guess' }]) {
      expect(() => build([], options)).toThrow('settings');
    }
  });
  test('renders broken fill and border at precisely half intensity, stops at the break, and honors the checkbox', () => {
    const model = build([...bull(), bar(3, { low: 89 })]), primitive = new OrderBlockPrimitive();
    const fills: any[] = [], strokes: any[] = [];
    const ctx: any = { save() {}, restore() {}, beginPath() {}, rect() {}, clip() {}, setLineDash() {}, moveTo() {}, lineTo() {}, stroke() {},
      fillRect(...args: number[]) { fills.push({ alpha: this.globalAlpha, args }); },
      strokeRect(...args: number[]) { strokes.push({ alpha: this.globalAlpha, args }); } };
    primitive.attached({ chart: { timeScale: () => ({ logicalToCoordinate: (i: number) => i * 10 }) },
      series: { priceToCoordinate: (price: number) => 150 - price }, requestUpdate: jest.fn() });
    primitive.setData({ blocks: [ { ...model.blocks[0], state: 'intact', brokenAt: null }, model.blocks[0] ] }, [...bull(), bar(3), bar(4)]);
    const target = { useMediaCoordinateSpace: (draw: any) => draw({ context: ctx, mediaSize: { width: 200, height: 100 } }) };
    primitive.draw(target);
    expect(fills.map(row => row.alpha)).toEqual([.2, .1]);
    expect(strokes.map(row => row.alpha)).toEqual([.3, .15]);
    expect(fills[1].args[0] + fills[1].args[2]).toBe(40);
    primitive.setVisible(false); primitive.draw(target); expect(fills).toHaveLength(2);
  });
});

describe('historical order block volume ranks', () => {
  const twoClusters = (firstVolume = 30) => [bar(0), bar(1, { high: 110, low: 90, volume: firstVolume }), bar(2),
    bar(3, { open: 140, high: 150, low: 130, close: 140, volume: 20 }),
    bar(4, { open: 145, high: 155, low: 140, close: 145 })];

  test('ranks simultaneous breaks against one common active-cluster snapshot', () => {
    const model = build([...twoClusters(), bar(5, { high: 155, low: 89 })]);
    expect(model.blocks).toHaveLength(2);
    expect(model.blocks.map((b: any) => [b.state, b.volumeRankAtBreak])).toEqual([['broken', 1], ['broken', 2]]);
    expect(model.blocks.every((b: any) => b.brokenAt === start + 6 * HOUR)).toBe(true);
  });

  test('freezes each rank at its own break, never against future blocks or already broken clusters', () => {
    const firstBreak = [...twoClusters(10), bar(5, { open: 140, high: 155, low: 129, close: 135 })];
    const before = build(firstBreak), after = build([...firstBreak, bar(6, { high: 120, low: 89 }),
      bar(7, { high: 110, low: 80, volume: 1000 }), bar(8)]);
    expect(before.blocks[1]).toMatchObject({ state: 'broken', volumeRankAtBreak: 1 });
    expect(after.blocks[1]).toEqual(before.blocks[1]);
    expect(after.blocks[0]).toMatchObject({ state: 'broken', volumeRankAtBreak: 1, brokenAt: start + 7 * HOUR });
    expect(after.blocks[2]).toMatchObject({ state: 'intact', volume: 1000 });
  });

  test('includes newly confirmed clusters before mitigation at the same close, matching detection order', () => {
    const candles = [...bull(), bar(3, { open: 140, high: 150, low: 130, close: 140, volume: 20 }), bar(4, { high: 151, low: 89 })];
    const blocks = build(candles).blocks;
    expect(blocks[0]).toMatchObject({ state: 'broken', volumeRankAtBreak: 2 });
    expect(blocks[1]).toMatchObject({ state: 'broken', volumeRankAtBreak: 1, confirmedAt: start + 5 * HOUR });
  });

  test('deduplicates one cluster breaking on one candle, but keeps separate break events and excludes unknown ranks', () => {
    const broken = (id: string, changes: any = {}) => ({ id, state: 'broken', direction: 'bullish', originTime: start,
      brokenAt: start + 10 * HOUR, top: 110, bottom: 90, volumeRankAtBreak: 2, breakClusterId: 'a,b', ...changes });
    const groups = brokenRankGroups([broken('a'), broken('b', { bottom: 95, top: 120 }),
      broken('c', { brokenAt: start + 11 * HOUR }), broken('unknown', { volumeRankAtBreak: null }),
      broken('gap', { state: 'coverage-gap' })]);
    expect(groups).toHaveLength(2);
    expect(groups[1]).toMatchObject({ rank: 2, count: 2, bottom: 90, top: 120 });
  });

  test('renders only #rank inside the historical block without a gap anchor and hides with the orderblock checkbox', () => {
    const primitive = new OrderBlockPrimitive(), texts: string[] = [];
    const ctx: any = { save() {}, restore() {}, beginPath() {}, rect() {}, clip() {}, fillRect() {},
      measureText: (value: string) => ({ width: value.length * 5 }), fillText: (text: string) => texts.push(text) };
    primitive.attached({ chart: { timeScale: () => ({ logicalToCoordinate: (i: number) => i * 20 }) },
      series: { priceToCoordinate: (price: number) => 150 - price }, requestUpdate: jest.fn() });
    primitive.setLabelAnchor(() => null);
    primitive.setData(build([...bull(), bar(3, { low: 89 })]), [...bull(), bar(3)]);
    const draw = (width = 200) => primitive.drawLabels({ useMediaCoordinateSpace: (fn: any) => fn({ context: ctx, mediaSize: { width, height: 100 } }) });
    draw();
    expect(texts).toEqual(['#1']); expect(primitive.getLabels()).toEqual([]);
    expect(primitive.getBrokenRankLabels()).toEqual([expect.objectContaining({ text: '#1', rank: 1, x: 66, fontSize: 10 })]);
    const copy = primitive.getBrokenRankLabels(); copy[0].rank = 99;
    expect(primitive.getBrokenRankLabels()[0].rank).toBe(1);
    draw(70); expect(primitive.getBrokenRankLabels()).toEqual([]);
    primitive.setVisible(false); draw(); expect(texts).toEqual(['#1']);
    expect(primitive.getBrokenRankLabels()).toEqual([]);
  });
});

describe('active order block cluster labels', () => {
  const block = (id: string, bottom: number, top: number, direction = 'bullish', state = 'intact') => ({ id, bottom, top, direction, state });
  test('merges nested, touching and transitively overlapping ranges without including broken or unknown blocks', () => {
    const input = [block('c', 108, 120, 'bearish'), block('b', 104, 110), block('a', 100, 105),
      block('nested', 101, 102), block('touch', 120, 125), block('separate', 140, 150),
      block('broken', 1, 200, 'bearish', 'broken'), block('unknown', 1, 200, 'bullish', 'coverage-gap')];
    const before = JSON.stringify(input), clusters = clusterActiveBlocks(input);
    expect(clusters).toHaveLength(2);
    expect(clusters[0]).toMatchObject({ bottom: 100, top: 125, count: 5, direction: 'mixed' });
    expect(clusters[1]).toMatchObject({ bottom: 140, top: 150, count: 1, direction: 'bullish' });
    expect(JSON.stringify(input)).toBe(before);
    expect(clusterActiveBlocks(input.slice().reverse())).toEqual(clusters);
  });
  test('keeps visible labels inside the pane, with no vertical collisions, even when prices are very close', () => {
    const items = Array.from({ length: 20 }, (_, i) => ({ id: i, anchorY: 45 + i * .01, count: i + 1, height: 24 }));
    const labels = placeClusterLabels(items, 120);
    expect(labels.length).toBeGreaterThan(0); expect(labels.length).toBeLessThan(items.length);
    labels.forEach((label: any, i: number) => {
      expect(label.y).toBeGreaterThanOrEqual(4); expect(label.y + label.height).toBeLessThanOrEqual(116);
      if (i) expect(label.y).toBeGreaterThanOrEqual(labels[i - 1].y + labels[i - 1].height + 3);
    });
    expect(items[0]).not.toHaveProperty('y');
  });
  test('renders only active cluster ranges right of the gap anchor, wraps in a narrow column and respects visibility', () => {
    const primitive = new OrderBlockPrimitive(), texts: any[] = [];
    const ctx: any = { save() {}, restore() {}, beginPath() {}, rect() {}, clip() {}, setLineDash() {},
      fillRect() {}, moveTo() {}, lineTo() {}, stroke() {}, measureText: (value: string) => ({ width: value.length * 5 }),
      fillText(text: string, x: number, y: number) { texts.push({ text, x, y }); } };
    let anchor: any = 120;
    primitive.attached({ chart: {}, series: { priceToCoordinate: (price: number) => 300 - price }, requestUpdate: jest.fn() });
    primitive.setLabelAnchor(() => anchor);
    primitive.setData({ blocks: [block('a', 100000.005, 100100), block('b', 100050, 100200),
      block('old', 1, 200000, 'bearish', 'broken')] }, []);
    primitive.series.priceToCoordinate = (price: number) => 50 + (100200 - price) / 10;
    const draw = () => primitive.drawLabels({ useMediaCoordinateSpace: (fn: any) => fn({ context: ctx, mediaSize: { width: 200, height: 120 } }) });
    draw();
    expect(primitive.getLabels()).toHaveLength(1);
    expect(primitive.getLabels()[0]).toMatchObject({ x: 130, bottom: 100000.005, top: 100200, lines: ['100,200', '100,000.005', 'PV n/a'] });
    expect(texts.every(text => text.x === 130)).toBe(true);
    expect(primitive.getLabels()[0].fontSize).toBe(10);
    primitive.setLabelFontSize(15); draw();
    expect(primitive.getLabels()[0]).toMatchObject({ fontSize: 15, height: 51 });
    primitive.setLabelFontSize(0); draw(); expect(primitive.getLabels()[0].fontSize).toBe(15);
    const copy = primitive.getClusters(); copy[0].blockIds.push('external');
    expect(primitive.getClusters()[0].blockIds).not.toContain('external');
    anchor = null; draw(); expect(primitive.getLabels()).toEqual([]);
    anchor = 120; primitive.setVisible(false); draw(); expect(primitive.getLabels()).toEqual([]);
  });
  test('sums unique origin-candle BTC volume and ranks active clusters by it, independently from their price order', () => {
    const origin = (id: string, bottom: number, top: number, volume: number, hour: number, state = 'intact') => ({
      ...block(id, bottom, top, 'bullish', state), originTime: start + hour * HOUR, volume
    });
    const clusters = clusterActiveBlocks([origin('a', 100, 110, 200, 1), origin('a-copy', 100, 110, 200, 1),
      origin('b', 105, 115, 300, 2), origin('large', 150, 160, 1500, 3), origin('broken', 100, 110, 9000, 4, 'broken')]);
    expect(clusters[0]).toMatchObject({ bottom: 100, top: 115, pivotVolumeBtc: 500, volumeCandleCount: 2, volumeRank: 2, volumeSharePct: 25 });
    expect(clusters[1]).toMatchObject({ bottom: 150, top: 160, pivotVolumeBtc: 1500, volumeRank: 1, volumeSharePct: 75 });
    expect(clusters[0].volumeSource).toBe('origin-candle-base-volume');
    const remaining = clusterActiveBlocks([origin('a', 100, 110, 200, 1), origin('b', 105, 115, 300, 2, 'broken')]);
    expect(remaining[0]).toMatchObject({ pivotVolumeBtc: 200, volumeRank: 1, volumeSharePct: 100 });
  });
  test('shares ranks for equal volumes and never silently treats absent or contradictory volume as zero', () => {
    const known = (id: string, bottom: number, volume: number, hour: number) => ({ ...block(id, bottom, bottom + 1), volume, originTime: start + hour * HOUR });
    const clusters = clusterActiveBlocks([known('a', 100, 10, 1), known('b', 200, 10, 2), known('c', 300, 5, 3), block('missing', 400, 410)]);
    expect(clusters.map((c: any) => c.volumeRank)).toEqual([1, 1, 3, null]);
    expect(clusters[3]).toMatchObject({ pivotVolumeBtc: null, volumeComplete: false, volumeSharePct: null });
    expect(clusterActiveBlocks([known('a', 100, 10, 1), known('b', 100, 20, 1)])[0]).toMatchObject({ volumeComplete: false, pivotVolumeBtc: null });
    const crowded = placeClusterLabels([{ anchorY: 30, height: 24, count: 10, pivotVolumeBtc: 1 },
      { anchorY: 30, height: 24, count: 1, pivotVolumeBtc: 100 }], 36);
    expect(crowded).toHaveLength(1); expect(crowded[0].pivotVolumeBtc).toBe(100);
  });
});
