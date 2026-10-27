import { replayFractalStrategy, summarizeReplay, ReplayConfig, StopSelector } from '../src/research/fractalStrategyReplay';
import { buildFractalStop } from '../src/services/decentraderGapMonitor';
const HOUR = 3600, start = Date.parse('2026-01-01T00:00:00Z') / 1000;
const bar = (i: number, changes: any = {}) => ({ time: start + i * HOUR, open: 100, high: 105, low: 96, close: 101, closed: true, ...changes });
const signal = (i = 4, direction: 'long' | 'short' = 'long') => ({ time: start + i * HOUR, closedAt: start + (i + 1) * HOUR, direction });
const config: ReplayConfig = { riskUsd: 15, feeBps: 0, slippageBps: 0, fractalDelay: 0, warmupBars: 4 };
const fixed: StopSelector = (_rows, _index, direction) => ({ valid: true, price: direction === 'long' ? 90 : 110,
  fractal: { timestamp: new Date(start * 1000).toISOString(), index: 0, price: direction === 'long' ? 90 : 110 } });

describe('read-only fractal strategy replay', () => {
  test('enters next open, sizes exact $15 initial price risk, and does not stop out on the signal candle', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i, i === 4 ? { low: 80 } : i === 7 ? { low: 89 } : {}));
    const result = replayFractalStrategy(candles, [signal()], config, fixed);
    expect(result.trades[0]).toMatchObject({ entryTime: start + 5 * HOUR, entryPrice: 100, quantityBtc: 1.5,
      initialStop: 90, exitPrice: 90, grossPnl: -15, netPnl: -15, exitCandleTime: start + 7 * HOUR });
  });

  test('treats an adverse opening gap as worse than the stop, and applies taker costs on both sides', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i, i === 6 ? { open: 85, low: 80, close: 90 } : {}));
    const trade = replayFractalStrategy(candles, [signal()], { ...config, feeBps: 10 }, fixed).trades[0];
    expect(trade).toMatchObject({ exitPrice: 85, exitReason: 'gap-stop', grossPnl: -22.5, exitWindowEnd: start + 6 * HOUR });
    expect(trade.fees).toBeCloseTo((100 + 85) * 1.5 * .001);
    expect(trade.netPnl).toBeCloseTo(-22.7775);
  });

  test('supports symmetric synthetic shorts with adverse entry and exit slippage', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i, i === 6 ? { high: 111 } : {}));
    const trade = replayFractalStrategy(candles, [signal(4, 'short')], { ...config, slippageBps: 10 }, fixed).trades[0];
    expect(trade.entryPrice).toBeCloseTo(99.9); expect(trade.exitPrice).toBeCloseTo(110.11);
    expect(trade.quantityBtc).toBeCloseTo(15 / 10.1);
    expect(trade.grossPnl).toBeLessThan(-15);
  });

  test('activates trailing only on the next candle, never applies a new stop to earlier wicks in its confirmation candle', () => {
    const candles = Array.from({ length: 9 }, (_, i) => bar(i, [6, 8].includes(i) ? { low: 94 } : {}));
    const select: StopSelector = (rows, index, direction, price, options) => index >= 6 && options.afterFractalTimestamp
      ? { valid: true, price: 95, fractal: { timestamp: new Date((start + 3 * HOUR) * 1000).toISOString(), price: 95, index: 3 } }
      : fixed(rows, index, direction, price, options);
    const trade = replayFractalStrategy(candles, [signal()], config, select).trades[0];
    expect(trade.stops).toHaveLength(2);
    expect(trade.stops[1].effectiveAt).toBe(start + 7 * HOUR);
    expect(trade.exitCandleTime).toBe(start + 8 * HOUR);
    expect(trade.netPnl).toBe(-7.5);
  });

  test('passes no future rows to the shared live stop function and forwards anchor/delay without minimum widening', () => {
    const calls: any[] = [];
    const select: StopSelector = (rows, index, direction, price, options) => {
      calls.push({ count: rows.length, index, last: rows.at(-1)?.timestamp, options });
      return fixed(rows, index, direction, price, options);
    };
    replayFractalStrategy(Array.from({ length: 9 }, (_, i) => bar(i)), [signal()], { ...config, fractalDelay: 2 }, select);
    expect(calls[0]).toMatchObject({ count: 5, index: 4, last: new Date((start + 4 * HOUR) * 1000).toISOString(), options: { enforceMinDistance: false } });
    expect(calls[1]).toMatchObject({ count: 6, index: 5, options: { afterFractalTimestamp: new Date(start * 1000).toISOString(), fractalDelay: 2, enforceMinDistance: false } });
    expect(calls.every(c => c.count === c.index + 1)).toBe(true);
  });

  test('never widens a protective stop or reuses an older pivot', () => {
    const candles = Array.from({ length: 9 }, (_, i) => bar(i));
    const select: StopSelector = (rows, index, direction, price, options) => !options.afterFractalTimestamp
      ? fixed(rows, index, direction, price, options)
      : { valid: true, price: index === 6 ? 80 : 98, fractal: { timestamp: new Date((start - HOUR) * 1000).toISOString(), index: 0, price: 98 } };
    expect(replayFractalStrategy(candles, [signal()], config, select).trades[0].stops).toHaveLength(1);
  });

  test('does not use a stop learned after an old stop was hit earlier in the candle', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i, i === 6 ? { low: 89, close: 104 } : {}));
    const select = jest.fn(fixed);
    const result = replayFractalStrategy(candles, [signal()], config, select);
    expect(result.trades[0].exitCandleTime).toBe(start + 6 * HOUR);
    expect(select.mock.calls.map(call => call[1])).not.toContain(6);
  });

  test('skips overlapping entries before the current candle path is known; independent mode keeps them separate', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i, i === 6 ? { low: 89 } : {}));
    const signals = [signal(), signal(5)];
    const single = replayFractalStrategy(candles, signals, config, fixed);
    const all = replayFractalStrategy(candles, signals, config, fixed, true);
    expect(single.trades).toHaveLength(1); expect(single.skipped[0].reason).toBe('Position already open at entry time');
    expect(all.trades).toHaveLength(2);
  });

  test('deduplicates identical signals, rejects conflicting directions and preserves last-candle signals as pending', () => {
    const candles = Array.from({ length: 8 }, (_, i) => bar(i));
    expect(replayFractalStrategy(candles, [signal(), signal()], config, fixed).trades).toHaveLength(1);
    expect(replayFractalStrategy(candles, [signal(), signal(4, 'short')], config, fixed).skipped).toHaveLength(2);
    const last = replayFractalStrategy(candles, [signal(7)], config, fixed);
    expect(last.trades).toHaveLength(0); expect(last.skipped[0].reason).toContain('outside closed history');
  });

  test('does not invent exits or count open mark-to-market P&L as realized', () => {
    const result = replayFractalStrategy(Array.from({ length: 8 }, (_, i) => bar(i)), [signal()], config, fixed);
    expect(result.trades[0]).toMatchObject({ status: 'open', unrealizedGross: 1.5 });
    expect(result.trades[0]).not.toHaveProperty('exitPrice');
    expect(result.summary).toMatchObject({ closed: 0, open: 1, netPnlUsd: 0, unrealizedGrossUsd: 1.5, profitFactor: null, winRatePct: null });
  });

  test('rejects invalid history, settings and unavailable protective stops', () => {
    for (const candles of [[bar(0), bar(2)], [bar(0), bar(0)], [bar(0, { high: 90 })]]) {
      expect(() => replayFractalStrategy(candles, [], config, fixed)).toThrow('contiguous');
    }
    expect(() => replayFractalStrategy([bar(0)], [], { ...config, riskUsd: 0 }, fixed)).toThrow('settings');
    const missing = replayFractalStrategy(Array.from({ length: 8 }, (_, i) => bar(i)), [signal()], config, () => ({ valid: false, reason: 'No fractal' }));
    expect(missing.skipped[0].reason).toBe('No fractal'); expect(missing.trades).toHaveLength(0);
  });

  test('uses the real live fractal selector causally, including the two closed right candles', () => {
    const names = ['DECENTRADER_SL_FRACTAL_WINDOW', 'DECENTRADER_SL_LOOKBACK_BARS', 'DECENTRADER_SL_BUFFER_PCT', 'DECENTRADER_SL_MAX_DISTANCE_PCT'];
    const before = names.map(name => process.env[name]);
    try {
      Object.assign(process.env, { DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_LOOKBACK_BARS: '72',
        DECENTRADER_SL_BUFFER_PCT: '.001', DECENTRADER_SL_MAX_DISTANCE_PCT: '.3' });
      const candles = Array.from({ length: 10 }, (_, i) => bar(i, { low: i === 2 ? 90 : i === 6 ? 95 : 97 }));
      const configReal = { ...config, warmupBars: 5 };
      const prefix = replayFractalStrategy(candles.slice(0, 8), [signal()], configReal, buildFractalStop);
      const full = replayFractalStrategy(candles, [signal()], configReal, buildFractalStop);
      expect(prefix.trades[0].initialStop).toBeCloseTo(89.91);
      expect(prefix.trades[0].stops).toHaveLength(1);
      expect(full.trades[0].stops[1]).toMatchObject({ effectiveAt: start + 9 * HOUR, price: 94.905 });
      expect(full.trades[0].stops[0]).toEqual(prefix.trades[0].stops[0]);
    } finally { names.forEach((name, i) => before[i] === undefined ? delete process.env[name] : process.env[name] = before[i]); }
  });

  test('reports realized drawdown in exit order, not signal order', () => {
    const rows: any[] = [{ id: 1, status: 'closed', netPnl: 50, grossPnl: 50, fees: 0, exitWindowEnd: 4 },
      { id: 2, status: 'closed', netPnl: -15, grossPnl: -15, fees: 0, exitWindowEnd: 2 },
      { id: 3, status: 'closed', netPnl: -10, grossPnl: -10, fees: 0, exitWindowEnd: 5 }];
    expect(summarizeReplay(rows)).toMatchObject({ netPnlUsd: 25, maxRealizedDrawdownUsd: 15, wins: 1, losses: 2, profitFactor: 2 });
  });
});
