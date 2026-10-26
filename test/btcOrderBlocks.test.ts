const { buildOrderBlocks, OrderBlockPrimitive } = require('../public/btc-spot-chart/order-blocks.js');
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
