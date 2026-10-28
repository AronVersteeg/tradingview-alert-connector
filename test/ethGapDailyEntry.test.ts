import fs from 'fs';
import os from 'os';
import path from 'path';
import * as entry from '../src/services/ethGapDailyEntry';
import * as gap from '../src/services/decentraderGapMonitor';
import { ethIntrusionFuturesDeltaFeed, parseFuturesDeltaCandle } from '../src/services/intrusionFuturesDelta';
import { OpenLiquidityV2EthTradeMonitor, openLiquidityV2BtcIntrusionMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';
const fractals = require('../public/btc-spot-chart/fractals.js');
const purple = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const HOUR = 3_600_000, closeAt = Date.parse('2026-10-24T02:00:00Z'), now = closeAt + 5000;
const iso = (ms: number) => new Date(ms).toISOString();
const signal = (direction: 'long' | 'short' = 'long'): entry.EthGapDailySignal => ({
  time: (closeAt - HOUR) / 1000, closedAt: closeAt / 1000, direction, close: direction === 'long' ? 125 : 75,
  dailyLevels: [{ id: 'D|test', type: direction === 'long' ? 'HIGH' : 'LOW', price: direction === 'long' ? 120 : 80,
    confirmedAt: iso(closeAt - 2 * HOUR - 1) }]
});

function inputs(direction = 'long') {
  const candles = [100, direction === 'long' ? 125 : 75].map((close, i) => ({
    time: (closeAt - (2 - i) * HOUR) / 1000, closeTime: closeAt - (1 - i) * HOUR - 1,
    open: 100, high: Math.max(100, close) + 1, low: Math.min(100, close) - 1, close,
    closed: true, volume: 1, quoteVolume: 100
  }));
  const payload = {
    source: { market: 'ETH-USD', sourceStatuses: [{ name: 'Binance Spot ETHUSDT 1H', ok: true }] },
    modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1', quality: { priceStepUsd: 5, causalModel: true },
    frames: candles.map(c => ({ ...c, t: iso(c.time * 1000).slice(0, 19).replace('T', ' '), startedAtMs: c.time * 1000 })),
    gaps: [ { left: 85, right: 115, width: 30 }, { left: 85, right: 115, width: 30 } ],
    zoneSeed: [['L', 10, 85, 4], ['S', 10, 115, 3]],
    zoneDeltas: [[], [[direction === 'long' ? 'L' : 'S', 10, direction === 'long' ? 90 : 110, 1]]]
  };
  const intrusions = [{ timestamp: payload.frames[1].t, direction }];
  const page = { symbol: 'ETHUSDT', venue: 'Binance Spot', interval: '1h', fetchedAt: iso(now), candles, hasMore: false } as any;
  const daily = { symbol: 'ETHUSDT', venue: 'Binance Spot', interval: '1d', latestClosedCandleAt: '2026-10-23T23:59:59.999Z',
    records: [{ id: 'test', type: direction === 'long' ? 'HIGH' : 'LOW', priceExact: direction === 'long' ? '120' : '80',
      pivotAt: '2026-09-01T00:00:00Z', confirmedAt: '2026-10-23T23:59:59.999Z', isLatestForType: true }] };
  return { payload, intrusions, page, daily };
}

describe('ETH Spot gap intrusion + Daily entry signal', () => {
  test.each(['long', 'short'])('has exactly the same %s signal as the purple overlay, without Weekly/Monthly data', direction => {
    const { payload, intrusions, page, daily } = inputs(direction);
    const result = entry.evaluateEthGapDailySignal(payload, intrusions, page, daily, now);
    const projection = fractals.projectDailyFractals({ symbol: 'ETHUSDT', venue: 'Binance Spot', daily,
      evaluatedThrough: iso(closeAt) }, page.candles, 'ETHUSDT');
    const history = purple.compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } }, now, 'ETH-USD');
    expect(result).toEqual(purple.buildIntrusionDailyBreakouts(page.candles, history, projection, now, 'ETH-USD').signals[0]);
    expect(result).toMatchObject({ direction, time: (closeAt - HOUR) / 1000, closedAt: closeAt / 1000 });
  });
  test('rejects prior wick breaks, equality touches, mismatching intrusion candle/direction and unconfirmed Daily levels', () => {
    for (const kind of ['wick', 'touch', 'other-hour', 'other-direction', 'unconfirmed']) {
      const x = inputs();
      if (kind === 'wick') x.page.candles[0].high = 121;
      if (kind === 'touch') { x.page.candles[1].close = 120; x.payload.frames[1].close = 120; }
      if (kind === 'other-hour') x.intrusions[0].timestamp = x.payload.frames[0].t;
      if (kind === 'other-direction') x.intrusions[0].direction = 'short';
      if (kind === 'unconfirmed') x.daily.records[0].confirmedAt = iso(closeAt - HOUR);
      expect(entry.evaluateEthGapDailySignal(x.payload, x.intrusions, x.page, x.daily, now)).toBeUndefined();
    }
  });
  test('fails closed for Futures, stale/missing coverage, wrong market/model, or mismatched latest Spot candles', () => {
    for (const kind of ['futures', 'stale', 'old-daily', 'btc', 'model', 'coverage', 'price', 'open']) {
      const x = inputs();
      if (kind === 'futures') x.page.venue = 'Binance Futures';
      if (kind === 'stale') (x.daily as any).stale = true;
      if (kind === 'old-daily') x.daily.latestClosedCandleAt = '2026-10-22T23:59:59.999Z';
      if (kind === 'btc') x.payload.source.market = 'BTC-USD';
      if (kind === 'model') x.payload.quality.causalModel = false;
      if (kind === 'coverage') x.page.candles.shift();
      if (kind === 'price') x.payload.frames[1].close = 124;
      if (kind === 'open') x.page.candles[1].closed = false;
      expect(() => entry.evaluateEthGapDailySignal(x.payload, x.intrusions, x.page, x.daily, now)).toThrow();
    }
  });
  test('only accepts the current closed hour, within five minutes, after activation', () => {
    expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - 1), now)).toBe(true);
    expect(entry.freshEthGapDailySignal(signal(), iso(closeAt), now)).toBe(false);
    expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt + 300001)).toBe(false);
    expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt - 1)).toBe(false);
    expect(entry.freshEthGapDailySignal(signal(), iso(closeAt - HOUR), closeAt + HOUR)).toBe(false);
  });
});

describe('ETH final-5m Futures delta measurement utility (not an entry gate)', () => {
  const response = (buy: number) => {
    const hour = signal().time;
    const candle = parseFuturesDeltaCandle([(hour + 3300) * 1000, 0, 0, 0, 0, '100',
      closeAt - 1, 0, 0, String(buy)], hour, now);
    return { symbol: 'ETHUSDT', venue: 'Binance USD-M Futures', interval: '5m', timeBasis: 'signal-last-five-minutes',
      records: [{ hour, status: 'ok', candle }] } as any;
  };
  afterEach(() => jest.restoreAllMocks());

  test.each([
    ['long', 60, true, 20], ['long', 40, false, -20], ['short', 40, true, 20],
    ['short', 60, false, -20], ['long', 50, false, 0], ['short', 50, false, 0],
    ['long', 50.0001, true, .0002]
  ])('uses unrounded direction-normalized delta for %s / buy %s', async (direction, buy, passed, expected) => {
    const read = jest.spyOn(ethIntrusionFuturesDeltaFeed, 'snapshot').mockResolvedValue(response(buy as number));
    const result = await entry.readEthEntryFuturesDelta(signal(direction as 'long' | 'short'));
    expect(read).toHaveBeenCalledWith([signal().time]);
    expect(result).toMatchObject({ symbol: 'ETHUSDT', interval: '5m', passed,
      signalTime: signal().time, startedAt: signal().time + 3300, closedAt: closeAt / 1000 });
    expect(result.directionalDeltaPct).toBeCloseTo(expected as number, 8);
  });

  test('blocks absent/pending/unavailable candles, wrong market/venue/hour and corrupt values', async () => {
    const read = jest.spyOn(ethIntrusionFuturesDeltaFeed, 'snapshot');
    for (const kind of ['missing', 'pending', 'unavailable', 'btc', 'spot', 'hour', 'window', 'invalid', 'mismatch']) {
      const value = response(60);
      if (kind === 'missing') value.records = [];
      if (kind === 'pending' || kind === 'unavailable') value.records = [{ hour: signal().time, status: kind, retryAt: iso(now + 15000) }];
      if (kind === 'btc') value.symbol = 'BTCUSDT';
      if (kind === 'spot') value.venue = 'Binance Spot';
      if (kind === 'hour') value.records[0].hour -= 3600;
      if (kind === 'window') value.records[0].candle.startedAt -= 300;
      if (kind === 'invalid') value.records[0].candle.rawDeltaPct = 99;
      if (kind === 'mismatch') value.records[0].candle.closedAt += 3600;
      read.mockResolvedValue(value);
      await expect(entry.readEthEntryFuturesDelta(signal())).rejects.toThrow();
    }
    read.mockRejectedValue(new Error('cooldown'));
    await expect(entry.readEthEntryFuturesDelta(signal())).rejects.toThrow('cooldown');
  });
});

describe('ETH gap/Daily live execution and persistent trailing-only management', () => {
  const oldEnv = { ...process.env };
  let directory: string, file: string, monitor: any, orders: any, position: any;
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(now);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'eth-gap-daily-'));
    file = path.join(directory, 'eth.json');
    Object.assign(process.env, {
      OPEN_LIQUIDITY_V2_ETH_TRADE_STATE_FILE: file, OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED: 'true',
      OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED: 'true', OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED: 'true',
      DECENTRADER_DYNAMIC_SL_ENABLED: 'true', DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED: 'true',
      DYDX_V4_FRACTAL_TRAIL_ENABLED: 'true', DECENTRADER_DYNAMIC_TP_ENABLED: 'true',
      DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_BUFFER_PCT: '0.001', DECENTRADER_SL_LOOKBACK_BARS: '72',
      DECENTRADER_SL_MAX_DISTANCE_PCT: '0.3', DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY: '0',
      DECENTRADER_TRADE_RISK_USD: '15', DECENTRADER_TRADE_RISK_USD_CAP_BY_PCT: 'false'
    });
    position = undefined;
    jest.spyOn(entry, 'readEthEntryFuturesDelta').mockImplementation(async s => ({
      symbol: 'ETHUSDT', venue: 'Binance USD-M Futures', interval: '5m', signalTime: s.time,
      startedAt: s.time + 3300, closedAt: s.closedAt, fetchedAt: iso(now),
      rawDeltaPct: s.direction === 'long' ? 20 : -20, directionalDeltaPct: 20, passed: true
    }));
    orders = {
      getAccountSnapshot: jest.fn().mockImplementation(async () => ({ equity: 1000, freeCollateral: 1000,
        openPositionsCount: position ? 1 : 0, openPositions: position ? [position] : [],
        markets: { 'ETH-USD': { status: 'ACTIVE', oraclePrice: 120, initialMarginFraction: .1, stepSize: .1 } } })),
      getStatefulOrderCapacity: jest.fn().mockResolvedValue({ limit: 20, openOrders: 0, marketOpenOrders: 0, marketOrderLimit: 3 }),
      placeOrder: jest.fn().mockImplementation(async (alert: any) => {
        expect(JSON.parse(fs.readFileSync(file, 'utf8')).gapDailyEntry.intent).toMatchObject({ outcome: 'SUBMITTING', orderAlert: { profile: 'NO_TP', take_profits: [] } });
        position = { market: 'ETH-USD', size: alert.desired_position === 'LONG' ? .4 : -.4, entryPrice: 120 };
      }),
      syncTrailingStop: jest.fn().mockResolvedValue({ outcome: 'UPDATED' }),
      syncTakeProfits: jest.fn().mockResolvedValue({ outcome: 'UPDATED' })
    };
    monitor = new OpenLiquidityV2EthTradeMonitor({} as any);
    monitor.processStartedAt = closeAt - HOUR;
    monitor.configureTradeExecutor(orders);
    jest.spyOn(gap, 'fetchDydxHourlyCandlesForMarket').mockResolvedValue([115, 112, 110, 113, 116].map((low, i) => ({
      startedAt: iso(closeAt - (5 - i) * HOUR), resolution: '1HOUR', open: String(low + 2), high: String(low + 5),
      low: String(low), close: String(low + 3)
    })) as any);
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
  });
  afterEach(() => {
    monitor.stop(); jest.restoreAllMocks(); jest.useRealTimers();
    for (const key of Object.keys(process.env)) if (!(key in oldEnv)) delete process.env[key];
    Object.assign(process.env, oldEnv);
    fs.rmSync(directory, { recursive: true, force: true });
  });
  const state = () => ({ gapDailyEntry: { activatedAt: iso(closeAt - HOUR) } });

  test('places a risk-sized NO_TP entry with exact existing fractal SL and manages the actual smaller fill', async () => {
    const managed: any = state(), result: any = {};
    await monitor.executeGapDailyEntry(managed, signal(), result);
    expect(result.tradePlaced).toBe(true);
    expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    expect(orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ market: 'ETH-USD', profile: 'NO_TP',
      signal: 'LONG_ENTRY', static_sl: 109.89, take_profits: [] }));
    expect(managed.managedPosition).toMatchObject({ takeProfitMode: 'trailing-only', takeProfits: [], initialSize: .4, currentStop: 109.89 });
    expect(orders.syncTrailingStop).toHaveBeenCalled();
    expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
    expect(managed.gapDailyEntry.intent.futuresDelta).toBeUndefined();
    expect(orders.placeOrder.mock.calls[0][0].gap_daily_futures_delta).toBeUndefined();
    expect(entry.readEthEntryFuturesDelta).not.toHaveBeenCalled();
  });
  test.each([
    ['long', 'negative'], ['long', 'zero'], ['long', 'unavailable'],
    ['short', 'negative'], ['short', 'zero'], ['short', 'unavailable']
  ])('allows a valid purple %s signal independently from %s FD, with SL/trailing and no TPs', async (direction, condition) => {
    const delta = entry.readEthEntryFuturesDelta as jest.Mock;
    if (condition === 'unavailable') delta.mockRejectedValue(new Error('Futures unavailable'));
    else delta.mockResolvedValue({ directionalDeltaPct: condition === 'zero' ? 0 : -20, passed: false });
    const stop = direction === 'long' ? 110 : 130;
    monitor.getGapDailyStopPlan = jest.fn().mockResolvedValue({ market: 'ETH-USD', price: 120,
      timestamp: '2026-10-24 01:00:00', marketInfo: { oraclePrice: 120, status: 'ACTIVE' },
      activePlan: { direction, stop: { valid: true, price: stop },
        sizing: { size: 1, minimumOrderSize: .1, notional: 120, riskBudgetUsd: 15 }, takeProfits: [] } });
    const managed: any = state(), result: any = {};
    await monitor.executeGapDailyEntry(managed, signal(direction as 'long' | 'short'), result);
    expect(result.tradePlaced).toBe(true); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    expect(delta).not.toHaveBeenCalled(); expect(orders.syncTrailingStop).toHaveBeenCalled();
    expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    expect(managed.managedPosition).toMatchObject({ direction, takeProfitMode: 'trailing-only', currentStop: stop });
    expect(monitor.getStatus().gapDailyEntry.futuresDeltaFilter.enabled).toBe(false);
  });
  test('never resubmits the same candle even if the trade closed or intrusion counts changed', async () => {
    const managed: any = state();
    await monitor.executeGapDailyEntry(managed, signal(), {});
    delete managed.managedPosition; position = undefined;
    await monitor.executeGapDailyEntry(managed, { ...signal(), close: 126 }, {});
    expect(orders.placeOrder).toHaveBeenCalledTimes(1);
    expect(managed.gapDailyEntry.decision.reason).toMatch(/already handled/);
  });
  test('short entries use the protective stop above price and also have no TP orders', async () => {
    monitor.getGapDailyStopPlan = jest.fn().mockResolvedValue({ market: 'ETH-USD', price: 120,
      timestamp: '2026-10-24 01:00:00', marketInfo: { oraclePrice: 120, status: 'ACTIVE' },
      activePlan: { direction: 'short', stop: { valid: true, price: 130 },
        sizing: { size: 1, minimumOrderSize: .1, notional: 120, riskBudgetUsd: 15 }, takeProfits: [] } });
    const managed: any = state(), result: any = {};
    await monitor.executeGapDailyEntry(managed, signal('short'), result);
    expect(result.tradePlaced).toBe(true);
    expect(orders.placeOrder).toHaveBeenCalledWith(expect.objectContaining({ desired_position: 'SHORT',
      signal: 'SHORT_ENTRY', static_sl: 130, profile: 'NO_TP', take_profits: [] }));
    expect(managed.managedPosition).toMatchObject({ direction: 'short', currentStop: 130, takeProfitMode: 'trailing-only' });
  });
  test.each(['pair-off', 'sl-off', 'live-off', 'executor-off', 'expired', 'position'])('blocks unsafe or inapplicable entry: %s', async kind => {
    if (kind === 'pair-off') process.env.OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED = 'false';
    if (kind === 'sl-off') process.env.DECENTRADER_DYNAMIC_SL_ENABLED = 'false';
    if (kind === 'live-off') process.env.DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED = 'false';
    if (kind === 'executor-off') delete orders.syncTrailingStop;
    if (kind === 'expired') jest.setSystemTime(closeAt + 300001);
    if (kind === 'position') position = { market: 'ETH-USD', size: -1, entryPrice: 120 };
    const managed: any = state();
    await monitor.executeGapDailyEntry(managed, signal(), {});
    expect(managed.gapDailyEntry.decision.outcome).toBe('SKIPPED');
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('requires capacity for a protective stop even with zero TPs', async () => {
    orders.getStatefulOrderCapacity.mockResolvedValue({ limit: 20, openOrders: 20, marketOpenOrders: 0, marketOrderLimit: 0 });
    await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('protective stop');
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('waits for the latest dYdX closed hour instead of using a stale initial fractal stop', async () => {
    (gap.fetchDydxHourlyCandlesForMarket as jest.Mock).mockResolvedValue([115, 112, 110, 113, 116].map((low, i) => ({
      startedAt: iso(closeAt - (6 - i) * HOUR), open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
    })));
    await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('Awaiting the signal hour');
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('refuses to submit when the durable intent cannot be written', async () => {
    jest.spyOn(fs, 'writeFileSync').mockImplementation(() => { throw new Error('disk full'); });
    await expect(monitor.executeGapDailyEntry(state(), signal(), {})).rejects.toThrow('disk full');
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('arms at activation without opening a trade from old chart evidence', async () => {
    const read = jest.spyOn(entry, 'readEthGapDailySignal').mockResolvedValue(signal());
    const managed: any = {};
    await monitor.checkGapDailyEntry(managed, inputs().payload, {});
    expect(managed.gapDailyEntry.activatedAt).toBe(iso(now));
    await monitor.checkGapDailyEntry(managed, inputs().payload, {});
    expect(read).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('retries missing Daily data independently from Delay, then executes a fresh matching intrusion', async () => {
    process.env.DECENTRADER_DELAY_ENTRY_MODEL_ENABLED = 'false';
    const read = jest.spyOn(entry, 'readEthGapDailySignal').mockRejectedValueOnce(new Error('Daily unavailable')).mockResolvedValue(signal());
    const managed: any = state(), first: any = {}, second: any = {};
    await monitor.checkGapDailyEntry(managed, inputs().payload, first);
    expect(first.gapDailyEntryPending).toBe(true); expect(orders.placeOrder).not.toHaveBeenCalled();
    await monitor.checkGapDailyEntry(managed, inputs().payload, second);
    expect(read).toHaveBeenCalledTimes(2); expect(second.tradePlaced).toBe(true);
  });
  test('does not trade a weighted mixed-side intrusion which the purple overlay excludes', async () => {
    const read = jest.spyOn(entry, 'readEthGapDailySignal').mockResolvedValue(signal());
    const payload = inputs().payload;
    payload.zoneDeltas[1].push(['S', 5, 110, 1]);
    const managed: any = state();
    await monitor.checkGapDailyEntry(managed, payload, {});
    expect(managed.gapDailyEntry.decision.outcome).toBe('NO_INTRUSION');
    expect(read).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('does not register stale fills after an explicit fail-safe flatten', async () => {
    orders.placeOrder.mockResolvedValue({ outcome: 'TARGET_FAILED_FLATTENED', reason: 'flattened' });
    const managed: any = state();
    await monitor.executeGapDailyEntry(managed, signal(), {});
    expect(managed.managedPosition).toBeUndefined(); expect(managed.gapDailyEntry.intent.outcome).toBe('FLATTENED');
  });
  test('recovers a partial/uncertain submission with an SL and no TPs, without resubmitting', async () => {
    orders.placeOrder.mockImplementation(async () => {
      position = { market: 'ETH-USD', size: .4, entryPrice: 120 };
      throw new Error('socket reset after submit');
    });
    const managed: any = state();
    await monitor.executeGapDailyEntry(managed, signal(), {});
    expect(managed.managedPosition.takeProfitMode).toBe('trailing-only');
    expect(managed.gapDailyEntry.intent.outcome).toBe('CONFIRMED');
    expect(orders.syncTrailingStop).toHaveBeenCalled();
    expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    const restarted = new OpenLiquidityV2EthTradeMonitor({} as any);
    restarted.configureTradeExecutor(orders);
    process.env.OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED = 'false';
    await restarted.checkManagedPosition();
    expect(orders.syncTakeProfits).not.toHaveBeenCalled(); expect(orders.placeOrder).toHaveBeenCalledTimes(1);
  });
  test('continues trailing a persisted position after disabling entries and never prepares map TPs', async () => {
    const managed: any = state(); await monitor.executeGapDailyEntry(managed, signal(), {});
    const delta = entry.readEthEntryFuturesDelta as jest.Mock;
    delta.mockClear(); delta.mockRejectedValue(new Error('FD must not gate protection of an existing position'));
    process.env.OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED = 'false';
    const restarted: any = new OpenLiquidityV2EthTradeMonitor({} as any); restarted.configureTradeExecutor(orders);
    const plan = jest.spyOn(restarted, 'getTradePlan').mockRejectedValue(new Error('No map access allowed'));
    await restarted.checkManagedPosition();
    await restarted.syncManagedOrders(JSON.parse(fs.readFileSync(file, 'utf8')), {});
    expect(orders.syncTrailingStop.mock.calls.length).toBeGreaterThan(1);
    expect(plan).not.toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    expect(delta).not.toHaveBeenCalled();
  });
  test('recovers a delayed fill after restart even when entries are off, without a second entry or any TP', async () => {
    orders.placeOrder.mockRejectedValue(new Error('uncertain submission'));
    const managed: any = state();
    await expect(monitor.executeGapDailyEntry(managed, signal(), {})).rejects.toThrow('uncertain submission');
    expect(managed.gapDailyEntry.intent.outcome).toBe('UNKNOWN');
    // Legacy pending submissions from before the FD gate must still receive protection.
    delete managed.gapDailyEntry.intent.futuresDelta;
    delete managed.gapDailyEntry.intent.orderAlert.gap_daily_futures_delta;
    fs.writeFileSync(file, JSON.stringify(managed));
    const delta = entry.readEthEntryFuturesDelta as jest.Mock;
    delta.mockClear(); delta.mockRejectedValue(new Error('FD unavailable during protective recovery'));
    position = { market: 'ETH-USD', size: .4, entryPrice: 120 };
    process.env.OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED = 'false';
    const restarted = new OpenLiquidityV2EthTradeMonitor({} as any); restarted.configureTradeExecutor(orders);
    const result = await restarted.checkManagedPosition();
    expect(result.ok).toBe(true);
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
    expect(orders.syncTrailingStop).toHaveBeenCalled();
    expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
    expect(delta).not.toHaveBeenCalled();
  });
  test('a restart does not turn a pre-restart purple candle into a fresh entry', async () => {
    const restarted: any = new OpenLiquidityV2EthTradeMonitor({} as any); restarted.configureTradeExecutor(orders);
    const read = jest.spyOn(entry, 'readEthGapDailySignal').mockResolvedValue(signal());
    const managed: any = state();
    await restarted.checkGapDailyEntry(managed, inputs().payload, {});
    expect(managed.gapDailyEntry.activatedAt).toBe(iso(now));
    expect(read).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('reconciles an uncertain fill after five seconds without waiting for the regular management interval', async () => {
    orders.placeOrder.mockRejectedValue(new Error('uncertain submission'));
    const managed: any = state();
    await expect(monitor.executeGapDailyEntry(managed, signal(), {})).rejects.toThrow();
    position = { market: 'ETH-USD', size: .4, entryPrice: 120 };
    await jest.advanceTimersByTimeAsync(5000);
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.takeProfitMode).toBe('trailing-only');
    expect(orders.placeOrder).toHaveBeenCalledTimes(1); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
  });
  test('blocks only ETH old entry routes, preserving plans, and leaves the BTC observe-only setting unchanged', async () => {
    expect((await monitor.executeFractalEntry({ market: 'ETH-USD' })).tradeSkipped).toMatch(/experiment/);
    expect(await monitor.executeManualEntry({ market: 'ETH-USD' })).toMatchObject({ tradeDeferred: true });
    const result: any = {}; await monitor.executeAlertExclusive({}, {}, 'old-delay', result);
    expect(result.tradeSkipped).toMatch(/experiment/);
    expect(orders.placeOrder).not.toHaveBeenCalled();
    expect(openLiquidityV2BtcIntrusionMonitor.getStatus()).toMatchObject({ market: 'BTC-USD', autoTradeEnabled: false });
  });
});
