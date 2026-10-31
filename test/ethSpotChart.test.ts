import { BinanceSpotChartFeed } from '../src/services/binanceSpotChart';
import { BtcFractalProjectionFeed } from '../src/services/btcFractalProjections';
import * as higherTimeframes from '../src/services/binanceDailyFractalHistory';
import { binanceGet } from '../src/services/binanceHttp';
const data = require('../public/btc-spot-chart/data.js');
const fractals = require('../public/btc-spot-chart/fractals.js');
const gaps = require('../public/btc-spot-chart/gap-boxes.js');
const breaks = require('../public/btc-spot-chart/gap-weekly-breakouts.js');
const intrusions = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const manuals = require('../public/btc-spot-chart/manual-levels.js');
jest.mock('../src/services/binanceHttp', () => ({ binanceGet: jest.fn() }));
describe.each(['ETH', 'SOL'] as const)('%s Spot parity', asset => {
  const market = asset === 'SOL' ? 'SOL-USD' : 'ETH-USD';
  const symbol = asset === 'SOL' ? 'SOLUSDT' : 'ETHUSDT';
  const HOUR = 3600, time = Date.parse('2026-10-23T00:00:00Z') / 1000, now = (time + 3 * HOUR) * 1000;
  const iso = (seconds: number) => new Date(seconds * 1000).toISOString();
  const source = { market, sourceStatuses: [{ name: `Binance Spot ${symbol} 1H` }] };
  const candles = [100, 125, 75].map((close, i) => ({ time: time + i * HOUR, open: 100,
    high: Math.max(close, 100) + 1, low: Math.min(close, 100) - 1, close, closed: true }));
  const projection = () => ({ symbol, venue: 'Binance Spot', evaluatedThrough: iso(time + 3 * HOUR),
    levels: ['D', 'W', 'M'].flatMap(tf => [
      { id: tf + 'high', timeframe: tf, type: 'HIGH', price: 120, confirmedAt: iso(time - .001), broken: true, brokenTime: time + HOUR },
      { id: tf + 'low', timeframe: tf, type: 'LOW', price: 80, confirmedAt: iso(time - .001), broken: true, brokenTime: time + 2 * HOUR }
    ]) });
  const payload = () => ({ source, frames: [-1, 0, 1, 2].map(i => ({ startedAtMs: (time + i * HOUR) * 1000 })),
    gaps: Array.from({ length: 4 }, () => ({ left: 85, right: 115 })), intrusionBenchmarks: { history: [
      { timestamp: iso(time + HOUR).slice(0, 19).replace('T', ' '), direction: 'long' },
      { timestamp: iso(time + 2 * HOUR).slice(0, 19).replace('T', ' '), direction: 'short' }
    ] } });

  describe('Binance Spot chart and market isolation', () => {
    afterEach(() => { jest.restoreAllMocks(); jest.useRealTimers(); });
    test('fetches only api/v3 ETHUSDT klines with a separate bounded cache from BTC', async () => {
      const row = [time * 1000, '100', '101', '99', '100', '1', (time + HOUR) * 1000 - 1, '100'];
      (binanceGet as jest.Mock).mockReset().mockResolvedValue({ data: [row] });
      const eth = new BinanceSpotChartFeed(symbol), btc = new BinanceSpotChartFeed();
      const [a, b] = await Promise.all([eth.page(), eth.page()]);
      expect(a).toBe(b); expect(a.symbol).toBe(symbol);
      expect((await btc.page()).symbol).toBe('BTCUSDT');
      expect(binanceGet).toHaveBeenNthCalledWith(1, 'https://api.binance.com/api/v3/klines', expect.objectContaining({ params: { symbol, interval: '1h', limit: 1000 } }));
      expect(binanceGet).toHaveBeenCalledTimes(2);
      await eth.page(time * 1000);
      expect(binanceGet).toHaveBeenLastCalledWith(expect.any(String), expect.objectContaining({ params: expect.objectContaining({ symbol, endTime: time * 1000 - 1 }) }));
    });
    test('rejects BTC and Futures websocket messages on the ETH Spot parser', () => {
      const message = { e: 'kline', s: symbol, E: now, k: { s: symbol, i: '1h', x: true, t: time * 1000,
        T: (time + HOUR) * 1000 - 1, o: '100', h: '101', l: '99', c: '100', v: '1', q: '100' } };
      expect(data.streamCandle(message, symbol)).toMatchObject({ close: 100 });
      expect(data.streamCandle(message)).toBeNull();
      expect(data.streamCandle({ ...message, s: 'BTCUSDT' }, symbol)).toBeNull();
      expect(data.streamCandle({ ...message, e: 'continuous_kline' }, symbol)).toBeNull();
    });
    test('reads all D/W/M histories for ETH only and refuses stale, wrong-symbol or Futures results', async () => {
      jest.useFakeTimers(); jest.setSystemTime(now);
      const snapshot = (interval: any) => ({ symbol, venue: 'Binance Spot', interval,
        latestClosedCandleAt: new Date(higherTimeframes.latestExpectedFractalClose(interval, now)).toISOString(), records: [] });
      const d = jest.spyOn(higherTimeframes, 'binanceDailyFractalHistory').mockResolvedValue(snapshot('1d') as any);
      const w = jest.spyOn(higherTimeframes, 'binanceWeeklyFractalHistory').mockResolvedValue(snapshot('1w') as any);
      const m = jest.spyOn(higherTimeframes, 'binanceMonthlyFractalHistory').mockResolvedValue(snapshot('1M') as any);
      const feed = new BtcFractalProjectionFeed(undefined, undefined, undefined, market);
      const result = await feed.snapshot(); expect(await feed.snapshot()).toBe(result);
      expect(result.symbol).toBe(symbol);
      expect(d).toHaveBeenCalledWith(market, false, 'Binance Spot');
      expect(w).toHaveBeenCalledWith(market, false); expect(m).toHaveBeenCalledWith(market, false);
      for (const bad of [{ symbol: 'BTCUSDT' }, { venue: 'Binance Futures' }, { stale: true }]) {
        d.mockResolvedValue({ ...snapshot('1d'), ...bad } as any);
        await expect(new BtcFractalProjectionFeed(undefined, undefined, undefined, market).snapshot()).rejects.toThrow('not current');
      }
    });
    test('projects Daily, Weekly and Monthly ETH prices and rejects a mixed higher-timeframe snapshot', () => {
      const snap = (interval: string, tf: string) => ({ symbol, venue: 'Binance Spot', interval,
        latestClosedCandleAt: iso(time - .001), records: [{ id: tf, type: 'HIGH', priceExact: '120',
          pivotAt: '2026-06-01T00:00:00Z', confirmedAt: '2026-08-31T23:59:59.999Z', isLatestForType: true }] });
      const input = { symbol, venue: 'Binance Spot', evaluatedThrough: iso(time + 3 * HOUR),
        daily: snap('1d', 'D'), weekly: snap('1w', 'W'), monthly: snap('1M', 'M') };
      expect(fractals.projectFractals(input, candles, symbol).levels.map((l: any) => l.label)).toEqual(['DC', 'WC', 'MC']);
      expect(() => fractals.projectFractals(input, candles)).toThrow('source');
      input.weekly.symbol = 'BTCUSDT';
      expect(() => fractals.projectFractals(input, candles, symbol)).toThrow('invalid');
    });
    test('draws ETH gap boxes and same-candle Gap/D and Gap/W markers, refusing BTC substitutions', () => {
      const timeline = gaps.timelineFromPayload(payload(), market);
      expect(gaps.buildGapBoxes(timeline, now, symbol).boxes).toHaveLength(1);
      for (const build of [breaks.buildGapDailyBreakouts, breaks.buildGapWeeklyBreakouts]) {
        expect(build(candles, timeline, projection(), now, symbol).signals.map((s: any) => s.direction)).toEqual(['long', 'short']);
        expect(() => build(candles, timeline, { ...projection(), symbol: 'BTCUSDT' }, now, symbol)).toThrow('source');
      }
      expect(() => gaps.timelineFromPayload(payload())).toThrow('source');
      expect(() => gaps.withDecentraderHistory(timeline, { symbol: 'BTCUSDT' })).toThrow();
    });
    test('uses ETH Spot intrusion alert times, never Decentrader BTC, and applies the same Daily rules', () => {
      const history = intrusions.compactIntrusionHistory(payload(), now, market);
      expect(history.source).toBe('binance-spot-recorded-intrusions');
      expect(intrusions.buildIntrusionDailyBreakouts(candles, history, projection(), now, market).signals).toHaveLength(2);
      expect(() => intrusions.compactIntrusionHistory(payload(), now)).toThrow('source');
      expect(() => intrusions.compactIntrusionHistory({ ...payload(), source: { market: 'BTC-USD' } }, now, market)).toThrow('source');
    });
    test('projects only ETH manual entries/TPs, without changing plans or enabling live execution', () => {
      const plan = { id: 'eth-plan', market, status: 'ARMED', direction: 'long', closeTrigger: 100,
        takeProfits: [{ price: 110, allocationPct: 50 }, { price: 120, allocationPct: 50 }] };
      const snapshot = { market, enabled: false, overrides: [plan, { ...plan, id: 'btc-plan', market: 'BTC-USD' }] };
      const original = JSON.stringify(snapshot), result = manuals.fromSnapshot(snapshot, market);
      expect(result.planCount).toBe(1); expect(result.enabled).toBe(false); expect(manuals.buildBands(result.levels)).toHaveLength(2);
      expect(JSON.stringify(snapshot)).toBe(original); expect(() => manuals.fromSnapshot(snapshot)).toThrow('market');
    });
  });
});
