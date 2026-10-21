const { transformView, install } = require('../public/btc-spot-chart/interactions.js');

const view = () => ({ time: { from: 0, to: 100 }, price: { from: 100, to: 200 }, width: 100,
  height: 200, priceTopY: 20, pricePixels: 100 });

describe('histomap-style Spot pan and zoom', () => {
  test('pans both axes directly with the pointer without changing scale', () => {
    expect(transformView(view(), { dx: 10, dy: 20 })).toEqual({ time: { from: -10, to: 90 }, price: { from: 120, to: 220 } });
  });
  test('zooms around the pointed candle and price and supports pinch-centre movement', () => {
    expect(transformView(view(), { factor: .5, x: 25, y: 70 })).toEqual({ time: { from: 12.5, to: 62.5 }, price: { from: 125, to: 175 } });
    expect(transformView(view(), { factor: .5, x: 25, y: 70, dx: 10, dy: 10 })).toEqual({ time: { from: 7.5, to: 57.5 }, price: { from: 130, to: 180 } });
  });
  test('keeps axis gestures independent and clamps extreme price scales', () => {
    expect(transformView(view(), { factor: .5, price: false })).not.toHaveProperty('price');
    expect(transformView(view(), { factor: .5, time: false })).not.toHaveProperty('time');
    const result = transformView(view(), { factor: 1e-8, dy: -1e6 });
    expect(result.price.from).toBeGreaterThan(0);
    expect(result.price.to - result.price.from).toBeCloseTo(1);
  });
  test('preserves future space in live zoom but allows historical pans and zooms', () => {
    const live = { ...view(), time: { from: 0, to: 159 } };
    expect(transformView(live, { factor: .5, pinRight: true, count: 100, lead: 10 }).time.to).toBe(159);
    expect(transformView(live, { dx: 100 }).time.to).toBe(0);
    expect(transformView(view(), { factor: .5, pinRight: true, count: 1000, lead: 10 }).time).toEqual({ from: 25, to: 75 });
  });

  function harness() {
    let state: any = view();
    const listeners = new Map(), captures = new Set(), classes = new Set();
    const element: any = {
      getBoundingClientRect: () => ({ left: 0, top: 0 }),
      addEventListener: (name: string, fn: any) => listeners.set(name, fn),
      removeEventListener: (name: string) => listeners.delete(name),
      classList: { add: (name: string) => classes.add(name), remove: (name: string) => classes.delete(name) },
      setPointerCapture: (id: number) => captures.add(id), hasPointerCapture: (id: number) => captures.has(id),
      releasePointerCapture: (id: number) => captures.delete(id)
    };
    const adapter = { read: () => state, apply: jest.fn((next: any) => { state = { ...state, ...next }; }),
      constraints: () => ({ count: 1000, lead: 10 }), onChange: jest.fn(), reset: jest.fn(), onTap: jest.fn(), onEnd: jest.fn() };
    const control = install(element, adapter);
    const fire = (type: string, x: number, y: number, id = 1, extra = {}) => {
      const event = { type, pointerId: id, pointerType: 'touch', button: 0, clientX: x, clientY: y,
        preventDefault: jest.fn(), stopPropagation: jest.fn(), ...extra };
      listeners.get(type)?.(event); return event;
    };
    return { adapter, control, fire, classes, captures, listeners, state: () => state };
  }

  const raf = global.requestAnimationFrame, caf = global.cancelAnimationFrame;
  beforeEach(() => {
    jest.useFakeTimers();
    global.requestAnimationFrame = ((fn: any) => setTimeout(fn, 16)) as any;
    global.cancelAnimationFrame = ((id: any) => clearTimeout(id)) as any;
  });
  afterEach(() => { global.requestAnimationFrame = raf; global.cancelAnimationFrame = caf; jest.useRealTimers(); });

  test('captures touch, pans on one animation frame, then releases without inertia or phantom tap', () => {
    const h = harness();
    h.fire('pointerdown', 30, 50); h.fire('pointermove', 40, 60); h.fire('pointermove', 50, 70);
    expect(h.adapter.apply).not.toHaveBeenCalled();
    jest.advanceTimersByTime(16);
    expect(h.adapter.apply).toHaveBeenCalledTimes(1);
    expect(h.state().time).toEqual({ from: -20, to: 80 });
    h.fire('pointerup', 50, 70);
    expect(h.captures.size).toBe(0); expect(h.classes.size).toBe(0);
    expect(h.adapter.onTap).not.toHaveBeenCalled();
    jest.advanceTimersByTime(500); expect(h.adapter.apply).toHaveBeenCalledTimes(1);
    h.control.destroy(); expect(h.listeners.size).toBe(0);
  });
  test('transitions from two fingers to one without jumping or activating a tap', () => {
    const h = harness();
    h.fire('pointerdown', 20, 50, 1); h.fire('pointerdown', 80, 50, 2);
    h.fire('pointermove', 90, 50, 2); jest.advanceTimersByTime(16);
    expect(h.state().time.to - h.state().time.from).toBeLessThan(100);
    h.fire('pointerup', 90, 50, 2);
    const before = { ...h.state().time };
    h.fire('pointermove', 25, 50, 1); jest.advanceTimersByTime(16);
    expect(h.state().time.to - h.state().time.from).toBeCloseTo(before.to - before.from);
    h.fire('pointerup', 25, 50, 1); expect(h.adapter.onTap).not.toHaveBeenCalled();
  });
  test('cancel discards queued movement and wheel matches histomap zoom factors', () => {
    const h = harness();
    h.fire('pointerdown', 20, 50); h.fire('pointermove', 40, 50);
    h.control.cancel(); jest.advanceTimersByTime(16); expect(h.adapter.apply).not.toHaveBeenCalled();
    const wheel = h.fire('wheel', 50, 70, 1, { deltaY: -100 });
    expect(wheel.preventDefault).toHaveBeenCalled();
    expect(h.state().time.to - h.state().time.from).toBeCloseTo(84);
    expect(h.state().price.to - h.state().price.from).toBeCloseTo(84);
    h.control.destroy();
  });
  test('axis dragging changes only the intended scale and a double tap resets', () => {
    const h = harness();
    h.fire('pointerdown', 110, 70); h.fire('pointermove', 110, 90); h.fire('pointerup', 110, 90);
    expect(h.state().time).toEqual(view().time);
    expect(h.state().price.to - h.state().price.from).toBeGreaterThan(100);
    const price = { ...h.state().price };
    h.fire('pointerdown', 40, 210); h.fire('pointermove', 20, 210); h.fire('pointerup', 20, 210);
    expect(h.state().price).toEqual(price);
    expect(h.state().time.to - h.state().time.from).toBeLessThan(100);
    h.fire('pointerdown', 40, 70); h.fire('pointerup', 40, 70);
    h.fire('pointerdown', 40, 70); h.fire('pointerup', 40, 70);
    expect(h.adapter.reset).toHaveBeenCalledTimes(1);
    h.control.destroy();
  });
  test('history prepends discard stale queued indices and rebase without a jump or phantom tap', () => {
    const h = harness();
    h.fire('pointerdown', 30, 50); h.fire('pointermove', 40, 50);
    h.adapter.apply({ time: { from: 1000, to: 1100 } });
    h.control.rebase(); jest.advanceTimersByTime(16);
    expect(h.state().time).toEqual({ from: 1000, to: 1100 });
    h.fire('pointermove', 45, 50); h.fire('pointerup', 45, 50);
    expect(h.state().time).toEqual({ from: 995, to: 1095 });
    expect(h.adapter.onTap).not.toHaveBeenCalled(); h.control.destroy();
  });
});
