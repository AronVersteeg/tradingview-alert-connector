import fs from 'fs';
import os from 'os';
import path from 'path';
import axios from 'axios';
import { btcCompanionEntryBlockReason, btcEntryMode, btcEntryModeStatus } from '../src/services/btcEntryMode';
import { BtcManualTradeOverrideMonitor, readBtcManualTradeOverrideStore } from '../src/services/btcManualTradeOverride';
import { DecentraderGapMonitor } from '../src/services/decentraderGapMonitor';
import { OpenLiquidityV2EthTradeMonitor, openLiquidityV2BtcIntrusionMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';
import { openLiquidityV2BtcCollector } from '../src/services/openLiquidityV2Replica';
import { PURPLE_LINE_SETUPS } from '../src/services/purpleLineSetup';
import { binanceHourlyCloseFeed } from '../src/services/binanceHourlyCloseFeed';

describe('BTC single entry model selector and ownership interlock', () => {
  const originalEnv = { ...process.env };
  let directory: string;
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-entry-mode-'));
    Object.assign(process.env, { BTC_ENTRY_MODE: 'manual', DECENTRADER_AUTO_TRADE_ENABLED: 'true',
      DECENTRADER_TRADE_MARKET: 'BTC-USD', OPEN_LIQUIDITY_V2_BTC_AUTO_TRADE_ENABLED: 'false',
      DECENTRADER_GAP_ALERT_STATE_FILE: path.join(directory, 'manual-owner.json'),
      OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE: path.join(directory, 'purple-owner.json'),
      BTC_MANUAL_TRADE_OVERRIDE_FILE: path.join(directory, 'manual-plans.json'),
      MANUAL_ENTRY_TP_OVERRIDE_ENABLED: 'true', DECENTRADER_DYNAMIC_SL_ENABLED: 'true',
      DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED: 'true', DECENTRADER_DYNAMIC_TP_ENABLED: 'true',
      DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_LOOKBACK_BARS: '72', DECENTRADER_SL_BUFFER_PCT: '.001',
      DECENTRADER_SL_MAX_DISTANCE_PCT: '.3', DECENTRADER_DYNAMIC_SL_FRACTAL_DELAY: '0' });
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(axios, 'get').mockRejectedValue(new Error('Unexpected network access in BTC mode test'));
  });
  afterEach(() => {
    jest.restoreAllMocks(); jest.useRealTimers();
    for (const key of Object.keys(process.env)) if (!(key in originalEnv)) delete process.env[key];
    Object.assign(process.env, originalEnv);
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test.each([[undefined, 'manual'], ['manual', 'manual'], [' PURPLE ', 'green'], ['green', 'green'], ['off', 'off'], ['true', 'off'], ['typo', 'off']])(
    'value %s selects %s, with invalid values failing closed', (value, expected) => {
      if (value === undefined) delete process.env.BTC_ENTRY_MODE; else process.env.BTC_ENTRY_MODE = value;
      expect(btcEntryMode()).toBe(expected);
      expect(btcEntryModeStatus().mode).toBe(expected);
    });
  test('BTC uses its actual Spot replica identity, not ETH/SOL or an invented model version', () => {
    expect(PURPLE_LINE_SETUPS['BTC-USD'].modelVersion).toBe((openLiquidityV2BtcCollector as any).config.modelVersion);
    expect(PURPLE_LINE_SETUPS['BTC-USD']).toMatchObject({ symbol: 'BTCUSDT', priceStepUsd: 100, enabledEnv: 'BTC_ENTRY_MODE' });
  });
  test('manual planning delegates exclusively to Public V2; a missing provider never falls back to Decentrader', async () => {
    const monitor = new DecentraderGapMonitor();
    await expect(monitor.getTradePlan({} as any, 'BTC-USD', 'long', 'manual-override')).rejects.toThrow('no Decentrader fallback');
    const nativePlan = { market: 'BTC-USD', planningSource: 'binance-spot-public-v2' };
    const provider = jest.fn().mockResolvedValue(nativePlan);
    monitor.configureManualBtcPlanProvider(provider);
    expect(await monitor.getTradePlan({} as any, 'BTC-USD', 'long', 'manual-override')).toBe(nativePlan);
    expect(provider).toHaveBeenCalledWith({}, 'long'); expect(axios.get).not.toHaveBeenCalled();
  });
  test('dynamic TP refresh for a native manual position keeps using Public V2 after a mode switch', async () => {
    process.env.BTC_ENTRY_MODE = 'off';
    const monitor: any = new DecentraderGapMonitor();
    const directionPlan = { takeProfits: [{ label: 'L TP1', price: 140, score: 10 }], sizing: { minimumOrderSize: .1 } };
    const provider = jest.fn().mockResolvedValue({ market: 'BTC-USD', planningSource: 'binance-spot-public-v2', price: 120,
      marketInfo: { oraclePrice: 120, stepSize: .1 }, plans: { long: directionPlan } });
    monitor.configureManualBtcPlanProvider(provider);
    const orders = { getAccountSnapshot: jest.fn(async () => ({ equity: 1000, freeCollateral: 1000, openPositionsCount: 1,
      markets: { 'BTC-USD': { oraclePrice: 120, stepSize: .1, initialMarginFraction: .1 } },
      openPositions: [{ market: 'BTC-USD', size: 1, entryPrice: 110 }] })),
      placeOrder: jest.fn(), syncTakeProfits: jest.fn(async () => ({ outcome: 'UPDATED' })) };
    monitor.configureTradeExecutor(orders);
    const result: any = {};
    await monitor.maybeSyncDynamicTakeProfits({ managedPosition: { market: 'BTC-USD', planSource: 'binance-spot-public-v2',
      direction: 'long', entryPrice: 110, initialSize: 1, currentStop: 100, takeProfitMode: 'dynamic',
      takeProfits: [{ label: 'L TP1', price: 140, size: 1 }] } }, result);
    expect(provider).toHaveBeenCalledTimes(1); expect(axios.get).not.toHaveBeenCalled();
    expect(result.dynamicTpSync.outcome).not.toBe('ERROR'); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test.each(['green', 'purple'])('%s arms Green while respecting the global execution kill switch', mode => {
    const monitor = new OpenLiquidityV2EthTradeMonitor({} as any, (openLiquidityV2BtcIntrusionMonitor as any).config);
    expect(monitor.getStatus()).toMatchObject({ autoTradeEnabled: false, observeOnly: true, entryMode: { mode: 'manual' } });
    process.env.BTC_ENTRY_MODE = mode;
    expect(monitor.getStatus()).toMatchObject({ autoTradeEnabled: true, observeOnly: false, entryMode: { mode: 'green' },
      gapDailyEntry: { setup: 'green-line', enabled: true, source: 'Binance Spot BTCUSDT 1H', takeProfitMode: 'trailing-only' } });
    process.env.DECENTRADER_AUTO_TRADE_ENABLED = 'false';
    expect(monitor.getStatus().autoTradeEnabled).toBe(false);
  });
  test.each(['purple', 'off', 'invalid'])('manual plans remain untouched and no candle/mail/order is requested in %s', async mode => {
    const monitor = new BtcManualTradeOverrideMonitor();
    const execute = jest.fn(), mail = jest.fn();
    monitor.configureEntryHandler({ executeManualBtcEntry: execute, sendManualBtcTriggerEmail: mail });
    monitor.arm({ direction: 'long', closeTrigger: 80000, takeProfits: [{ price: 85000, allocationPct: 100 }] });
    monitor.arm({ direction: 'short', closeTrigger: 70000, takeProfits: [] });
    const before = fs.readFileSync(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE!, 'utf8');
    const read = jest.spyOn(binanceHourlyCloseFeed, 'latest');
    process.env.BTC_ENTRY_MODE = mode;
    await monitor.checkOnce();
    expect(read).not.toHaveBeenCalled(); expect(execute).not.toHaveBeenCalled(); expect(mail).not.toHaveBeenCalled();
    expect(fs.readFileSync(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE!, 'utf8')).toBe(before);
    process.env.BTC_ENTRY_MODE = 'manual';
    expect(readBtcManualTradeOverrideStore().overrides.filter(row => row.status === 'ARMED')).toHaveLength(2);
  });
  test.each(['purple', 'off'])('direct manual entry cannot bypass %s', async mode => {
    process.env.BTC_ENTRY_MODE = mode;
    const monitor = new DecentraderGapMonitor();
    const orders = { getAccountSnapshot: jest.fn(), placeOrder: jest.fn() };
    monitor.configureTradeExecutor(orders);
    expect(await monitor.executeManualBtcEntry({ market: 'BTC-USD', direction: 'long', signature: 'blocked' } as any))
      .toMatchObject({ tradePlaced: false, tradeDeferred: true });
    expect(orders.getAccountSnapshot).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test('an armed manual plan does not veto purple; an unfinished manual execution does', () => {
    const file = process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE!;
    fs.writeFileSync(file, JSON.stringify({ version: 3, market: 'BTC-USD', overrides: [{ status: 'ARMED' }] }));
    expect(btcCompanionEntryBlockReason('purple')).toBeUndefined();
    fs.writeFileSync(file, JSON.stringify({ version: 3, market: 'BTC-USD', overrides: [{ status: 'EXECUTING' }] }));
    expect(btcCompanionEntryBlockReason('purple')).toMatch(/unfinished/);
  });
  test.each(['manual', 'purple'] as const)('a position retained by the other owner blocks new %s entries until its owner clears it', target => {
    const file = target === 'manual' ? process.env.OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE! : process.env.DECENTRADER_GAP_ALERT_STATE_FILE!;
    fs.writeFileSync(file, JSON.stringify({ managedPosition: { market: 'BTC-USD', currentStop: 70000 } }));
    expect(btcCompanionEntryBlockReason(target)).toMatch(/still manages/);
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).managedPosition.currentStop).toBe(70000);
    fs.writeFileSync(file, '{}'); expect(btcCompanionEntryBlockReason(target)).toBeUndefined();
  });
  test.each(['SUBMITTING', 'UNKNOWN'])('uncertain purple state %s blocks manual, even with an apparently flat indexer', outcome => {
    fs.writeFileSync(process.env.OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE!, JSON.stringify({ gapDailyEntry: { intent: { outcome } } }));
    expect(btcCompanionEntryBlockReason('manual')).toMatch(/uncertain/);
  });
  test('unreadable companion evidence fails closed without overwriting the file', () => {
    const file = process.env.OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE!;
    fs.writeFileSync(file, '{bad');
    expect(() => btcCompanionEntryBlockReason('manual')).toThrow('ownership cannot be verified');
    expect(fs.readFileSync(file, 'utf8')).toBe('{bad');
  });
  test('rejects aliased owner files rather than letting two managers own the same evidence', () => {
    process.env.OPEN_LIQUIDITY_V2_BTC_INTRUSION_STATE_FILE = process.env.DECENTRADER_GAP_ALERT_STATE_FILE;
    expect(() => btcCompanionEntryBlockReason('manual')).toThrow('must be distinct');
  });
  test('malformed manual plan metadata cannot be treated as proof that no manual execution exists', () => {
    fs.writeFileSync(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE!, JSON.stringify({ version: 3, market: 'BTC-USD', overrides: null }));
    expect(() => btcCompanionEntryBlockReason('purple')).toThrow('unexpected format');
  });
  test.each(['manual', 'purple', 'off'])('legacy BTC Shadow and Delay cannot create competing entries in %s', async mode => {
    process.env.BTC_ENTRY_MODE = mode;
    process.env.SHADOW_FRACTAL_ENTRY_MODEL_ENABLED = 'true';
    const monitor: any = new DecentraderGapMonitor(), orders = { getAccountSnapshot: jest.fn(), placeOrder: jest.fn() };
    monitor.configureTradeExecutor(orders);
    expect((await monitor.executeFractalEntry({ market: 'BTC-USD' } as any)).tradeSkipped).toMatch(/legacy Shadow/);
    const result: any = {};
    await monitor.executeTradeExclusive({}, 'old-delay', {}, result);
    expect(result.tradeSkipped).toMatch(/legacy Delay/); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test.each(['purple', 'off'])('an existing manual position retains locked TPs and fractal stop management after switching to %s', async mode => {
    jest.useFakeTimers(); jest.setSystemTime(new Date('2026-11-04T10:00:05Z'));
    process.env.BTC_ENTRY_MODE = mode;
    fs.writeFileSync(process.env.DECENTRADER_GAP_ALERT_STATE_FILE!, JSON.stringify({ managedPosition: {
      market: 'BTC-USD', direction: 'long', entryPrice: 110, initialSize: 1, currentStop: 100,
      currentStopFractalTimestamp: '2026-11-04T05:00:00Z', currentStopFractalCandleSource: 'dydx-1h',
      currentStopFractalSource: 'lowRef', takeProfitMode: 'manual-locked', takeProfits: [{ price: 140, size: 1 }] } }));
    const candles = [115, 112, 110, 113, 116].map((low, i) => ({ startedAt: `2026-11-04T0${5 + i}:00:00Z`,
      open: String(low + 2), high: String(low + 5), low: String(low), close: String(low + 3) }));
    (axios.get as jest.Mock).mockResolvedValue({ data: { candles } });
    const orders = { getAccountSnapshot: jest.fn(async () => ({ equity: 1000, freeCollateral: 1000, openPositionsCount: 1,
      markets: { 'BTC-USD': { oraclePrice: 120, stepSize: .1, initialMarginFraction: .1 } },
      openPositions: [{ market: 'BTC-USD', size: 1, entryPrice: 110 }] })),
      placeOrder: jest.fn(), syncTrailingStop: jest.fn(async () => ({ outcome: 'UPDATED' })), syncTakeProfits: jest.fn() };
    const monitor = new DecentraderGapMonitor(); monitor.configureTradeExecutor(orders);
    expect((await monitor.checkManagedPosition()).ok).toBe(true);
    const saved = JSON.parse(fs.readFileSync(process.env.DECENTRADER_GAP_ALERT_STATE_FILE!, 'utf8'));
    expect(saved.managedPosition.currentStop).toBeCloseTo(109.89);
    expect(saved.managedPosition.takeProfits).toEqual([{ price: 140, size: 1 }]);
    expect(orders.placeOrder).not.toHaveBeenCalled(); expect(orders.syncTakeProfits).not.toHaveBeenCalled();
  });
});
