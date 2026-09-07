import fs from 'fs';
import os from 'os';
import path from 'path';
import * as gap from '../src/services/decentraderGapMonitor';
import { OpenLiquidityV2EthTradeMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';

const candles = [115, 112, 110, 113, 116].map((low, index) => ({
  startedAt: `2026-08-20T0${index}:00:00.000Z`, resolution: '1HOUR',
  open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3)
}));

function state(market = 'ETH-USD') {
  return { managedPosition: {
    market, direction: 'long', initialSize: 2, entryPrice: 110, currentStop: 100,
    currentStopFractalTimestamp: '2026-08-18T00:00:00.000Z',
    currentStopFractalCandleSource: 'dydx-1h', currentStopFractalSource: 'low',
    takeProfits: [], entrySignature: 'test', openedAt: '2026-08-18T00:00:00.000Z'
  } } as any;
}

function executor(market = 'ETH-USD') {
  return {
    getAccountSnapshot: jest.fn().mockResolvedValue({
      markets: { [market]: { oraclePrice: 120, stepSize: 0.1 } },
      openPositions: [{ market, size: 2, entryPrice: 110 }]
    }),
    placeOrder: jest.fn().mockResolvedValue(undefined),
    syncTrailingStop: jest.fn().mockResolvedValue({ outcome: 'UPDATED' }),
    syncTakeProfits: jest.fn().mockRejectedValue(new Error('TP unavailable'))
  };
}

describe('managed position safety independent from entry scanning', () => {
  const originalEnv = { ...process.env };
  let directory: string;
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'managed-safety-'));
    process.env.OPEN_LIQUIDITY_V2_ETH_TRADE_STATE_FILE = path.join(directory, 'eth.json');
    process.env.DECENTRADER_GAP_ALERT_STATE_FILE = path.join(directory, 'btc.json');
    process.env.OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED = 'false';
    process.env.DECENTRADER_AUTO_TRADE_ENABLED = 'false';
    process.env.DECENTRADER_DYNAMIC_SL_ENABLED = 'true';
    process.env.DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED = 'true';
    process.env.DECENTRADER_DYNAMIC_TP_ENABLED = 'true';
    process.env.DECENTRADER_SL_FRACTAL_WINDOW = '2';
    process.env.DECENTRADER_SL_BUFFER_PCT = '0.001';
    process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '0.3';
    process.env.DECENTRADER_SL_LOOKBACK_BARS = '72';
    process.env.DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY = '0';
    jest.spyOn(gap, 'fetchDydxHourlyCandlesForMarket').mockResolvedValue(candles as any);
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => {
    jest.restoreAllMocks();
    for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
    Object.assign(process.env, originalEnv);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('V2 trails while new entries are off and a map/TP plan fails', async () => {
    const collector = { getPayload: jest.fn().mockRejectedValue(new Error('map offline')) };
    const monitor = new OpenLiquidityV2EthTradeMonitor(collector as any) as any;
    const orders = executor();
    monitor.configureTradeExecutor(orders);
    const managed = state();
    const result: any = {};
    await monitor.syncManagedOrders(managed, result);
    expect(orders.syncTrailingStop).toHaveBeenCalledWith(expect.objectContaining({ trail_stop: 109.89 }));
    expect(managed.managedPosition.currentStop).toBeCloseTo(109.89);
    expect(result.dynamicTpSync.outcome).toBe('ERROR');
    expect(result.lastSuccessfulStopCheckAt).toBeDefined();
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });

  test('a TP submit failure cannot prevent V2 stop synchronization', async () => {
    const monitor = new OpenLiquidityV2EthTradeMonitor({} as any) as any;
    const orders = executor();
    monitor.configureTradeExecutor(orders);
    monitor.getTradePlan = jest.fn().mockImplementation(async () => {
      expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
      return { market: 'ETH-USD', price: 120, marketInfo: { oraclePrice: 120, stepSize: 0.1 },
        plans: { long: { takeProfits: [{ price: 130, score: 10 }], sizing: { minimumOrderSize: 0.1 } } } };
    });
    const result: any = {};
    await monitor.syncManagedOrders(state(), result);
    expect(orders.syncTakeProfits).toHaveBeenCalledTimes(1);
    expect(result.dynamicTpSync.outcome).toBe('ERROR');
  });

  test('entry toggle still rejects new entries without making account or order calls', async () => {
    const monitor = new OpenLiquidityV2EthTradeMonitor({} as any) as any;
    const orders = executor();
    monitor.configureTradeExecutor(orders);
    const result: any = {};
    await monitor.executeAlert({}, {}, 'new-entry', result);
    expect(result.tradeSkipped).toMatch(/disabled/);
    expect(orders.getAccountSnapshot).not.toHaveBeenCalled();
    expect(orders.placeOrder).not.toHaveBeenCalled();
  });

  test('management completes while the scanner is still waiting for its payload', async () => {
    let finish!: (payload: any) => void;
    const collector = { getPayload: jest.fn(() => new Promise((resolve) => { finish = resolve; })) };
    const monitor = new OpenLiquidityV2EthTradeMonitor(collector as any) as any;
    const orders = executor();
    monitor.configureTradeExecutor(orders);
    process.env.DECENTRADER_DYNAMIC_TP_ENABLED = 'false';
    const status = monitor.getStatus();
    fs.writeFileSync(status.stateFile, JSON.stringify(state()));
    const scan = monitor.check();
    const result = await monitor.checkManagedPosition();
    expect(result.ok).toBe(true);
    expect(orders.syncTrailingStop).toHaveBeenCalledTimes(1);
    finish({ frames: [] });
    await scan;
    expect(JSON.parse(fs.readFileSync(status.stateFile, 'utf8')).managedPosition.currentStop).toBeCloseTo(109.89);
  });

  test('BTC management is independent of a failed scanner and runs SL before TP', async () => {
    const monitor = new gap.DecentraderGapMonitor() as any;
    monitor.configureTradeExecutor(executor('BTC-USD'));
    monitor.getTradePlan = jest.fn().mockResolvedValue({});
    fs.writeFileSync(process.env.DECENTRADER_GAP_ALERT_STATE_FILE!, JSON.stringify(state('BTC-USD')));
    monitor.maybeSyncDynamicStopLoss = jest.fn(async (s, r) => { s.managedPosition.currentStop = 105; r.dynamicSlSync = { outcome: 'UPDATED' }; });
    monitor.maybeSyncDynamicTakeProfits = jest.fn(async () => { throw new Error('TP failed'); });
    const result = await monitor.checkManagedPosition();
    await monitor.takeProfitPromise;
    expect(monitor.maybeSyncDynamicStopLoss).toHaveBeenCalledTimes(1);
    expect(result.lastSuccessfulStopCheckAt).toBeDefined();
    expect(result.ok).toBe(true);
    expect(monitor.managementStatus.dynamicTpSync.outcome).toBe('ERROR');
    expect(JSON.parse(fs.readFileSync(process.env.DECENTRADER_GAP_ALERT_STATE_FILE!, 'utf8')).managedPosition.currentStop).toBe(105);
  });

  test.each(['BTC', 'ETH'])('%s stop cycles continue while TP preparation is pending', async (asset) => {
    const monitor: any = asset === 'BTC'
      ? new gap.DecentraderGapMonitor()
      : new OpenLiquidityV2EthTradeMonitor({} as any);
    monitor.configureTradeExecutor(executor(`${asset}-USD`));
    const file = asset === 'BTC' ? process.env.DECENTRADER_GAP_ALERT_STATE_FILE! : monitor.getStatus().stateFile;
    fs.writeFileSync(file, JSON.stringify(state(`${asset}-USD`)));
    let finish!: (value: any) => void;
    monitor.getTradePlan = jest.fn(() => new Promise(resolve => { finish = resolve; }));
    const stop = jest.fn(async (_state: any, result: any) => { result.dynamicSlSync = { outcome: 'UNCHANGED' }; });
    if (asset === 'BTC') {
      monitor.maybeSyncDynamicStopLoss = stop;
      monitor.maybeSyncDynamicTakeProfits = jest.fn().mockResolvedValue(undefined);
    } else {
      monitor.syncManagedStop = stop;
    }
    await monitor.checkManagedPosition();
    const pendingTp = monitor.takeProfitPromise;
    await monitor.checkManagedPosition();
    expect(stop).toHaveBeenCalledTimes(2);
    expect(monitor.getTradePlan).toHaveBeenCalledTimes(1);
    finish({ market: `${asset}-USD`, price: 120, plans: {} });
    await pendingTp;
  });

  test.each(['long', 'short'] as const)('initial %s SL stays at the buffered fractal even with a legacy minimum env', (direction) => {
    process.env.DECENTRADER_SL_MIN_DISTANCE_PCT = '0.1';
    const lows = direction === 'long' ? [101, 100.2, 100, 100.3, 101] : [99, 100, 100.2, 100, 99];
    const rows = lows.map((lowRef, i) => ({ timestamp: `2026-08-20T0${i}:00:00.000Z`, lowRef, highRef: lowRef + 1, ohlc4: lowRef + 0.5 }));
    const plan = gap.buildDirectionalPlan(direction, { equity: 1000, freeCollateral: 1000 } as any,
      { oraclePrice: direction === 'long' ? 100.1 : 101.1, initialMarginFraction: 0.1, stepSize: 0.1 },
      rows, 4, undefined, undefined, { longTp: [], shortTp: [] }, 100, 'balanced');
    expect(plan.stop.valid).toBe(true);
    expect(plan.stop.adjustedToMinDistance).toBe(false);
    expect(plan.stop.price).toBeCloseTo(direction === 'long' ? 99.9 : 101.3012);
  });

  test.each(['BTC', 'ETH'])('%s keeps the management timer when intrusion scanning is disabled', (asset) => {
    jest.useFakeTimers();
    process.env.DECENTRADER_GAP_MONITOR_ENABLED = 'false';
    process.env.OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED = 'false';
    process.env.DECENTRADER_GAP_POLL_MINUTES = '1';
    const monitor: any = asset === 'BTC'
      ? new gap.DecentraderGapMonitor()
      : new OpenLiquidityV2EthTradeMonitor({} as any);
    monitor.checkManagedPosition = jest.fn().mockResolvedValue({});
    const scan = jest.fn().mockResolvedValue({});
    if (asset === 'BTC') monitor.checkOnce = scan;
    else monitor.check = scan;
    try {
      monitor.start(0);
      jest.advanceTimersByTime(0);
      expect(monitor.checkManagedPosition).toHaveBeenCalledTimes(1);
      jest.advanceTimersByTime(60_000);
      expect(monitor.checkManagedPosition).toHaveBeenCalledTimes(2);
      expect(scan).not.toHaveBeenCalled();
      monitor.stop();
      jest.advanceTimersByTime(60_000);
      expect(monitor.checkManagedPosition).toHaveBeenCalledTimes(2);
    } finally {
      monitor.stop();
      jest.useRealTimers();
    }
  });
});
