import fs from 'fs';
import os from 'os';
import path from 'path';
import axios from 'axios';
import { spotEntryModeStatus, spotEntryModeExplicit, spotManualEntryEnabled } from '../src/services/spotEntryMode';
import { automaticSpotEntryEnabled, spotEntrySetup } from '../src/services/purpleLineSetup';
import { BtcManualTradeOverrideMonitor, manualTradeOverrideConfigForMarket } from '../src/services/btcManualTradeOverride';
import { OpenLiquidityV2EthTradeMonitor, openLiquidityV2EthTradeMonitor,
  openLiquidityV2SolTradeMonitor, openLiquidityV2ZecTradeMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';
import * as gap from '../src/services/decentraderGapMonitor';

describe.each(['ETH', 'SOL', 'ZEC'] as const)('%s entry selector', asset => {
  const originalEnv = { ...process.env }, market = `${asset}-USD`, env = `${asset}_ENTRY_MODE`;
  const configured = { ETH: openLiquidityV2EthTradeMonitor, SOL: openLiquidityV2SolTradeMonitor, ZEC: openLiquidityV2ZecTradeMonitor }[asset] as any;
  const config = configured.config;
  const hour = Date.parse('2026-11-19T12:00:00Z');
  let directory: string, file: string, plans: string, monitor: any, orders: any;
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(hour + 5000);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pair-entry-mode-'));
    file = path.join(directory, 'position.json'); plans = path.join(directory, 'plans.json');
    for (const pair of ['ETH', 'SOL', 'ZEC']) delete process.env[`${pair}_ENTRY_MODE`];
    Object.assign(process.env, { BTC_ENTRY_MODE: 'green', [config.stateFileEnv]: file,
      [`${asset}_MANUAL_TRADE_OVERRIDE_FILE`]: plans, [config.enabledEnv]: 'true', [config.autoTradeEnv]: 'true',
      [`OPEN_LIQUIDITY_V2_${asset}_GAP_DAILY_ENTRY_ENABLED`]: 'true', MANUAL_ENTRY_TP_OVERRIDE_ENABLED: 'true',
      DECENTRADER_AUTO_TRADE_ENABLED: 'true', DECENTRADER_DYNAMIC_SL_ENABLED: 'true',
      DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED: 'true', DYDX_V4_FRACTAL_TRAIL_ENABLED: 'true',
      DECENTRADER_DELAY_ENTRY_MODEL_ENABLED: 'true', SHADOW_FRACTAL_ENTRY_MODEL_ENABLED: 'true' });
    orders = { getAccountSnapshot: jest.fn(), placeOrder: jest.fn(), syncTrailingStop: jest.fn() };
    monitor = new OpenLiquidityV2EthTradeMonitor({} as any, config); monitor.configureTradeExecutor(orders);
    jest.spyOn(axios, 'get').mockRejectedValue(new Error('Unexpected network access'));
    jest.spyOn(gap, 'sendEmailBestEffort').mockResolvedValue({ sent: true });
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => {
    monitor.stop(); jest.restoreAllMocks(); jest.useRealTimers(); process.env = { ...originalEnv };
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test.each(['true', 'yes', '1', 'on'])('unset selector preserves the existing Green flag %s', value => {
    process.env[`OPEN_LIQUIDITY_V2_${asset}_GAP_DAILY_ENTRY_ENABLED`] = value;
    expect(spotEntryModeStatus(market)).toMatchObject({ env, mode: 'green', explicit: false, legacyFallback: true });
    expect(automaticSpotEntryEnabled(market)).toBe(true);
    process.env[`OPEN_LIQUIDITY_V2_${asset}_GAP_DAILY_ENTRY_ENABLED`] = 'false';
    expect(spotEntryModeStatus(market)).toMatchObject({ mode: 'manual', explicit: false });
    expect(automaticSpotEntryEnabled(market)).toBe(false);
  });
  test.each([
    ['purple', 'purple', 'purple-green-trail', true], [' PURPLE ', 'purple', 'purple-green-trail', true],
    ['purple-trail', 'purple', 'purple-green-trail', true], ['green', 'green', 'green-line', true], [' ATR ', 'atr', 'atr-line', true],
    [' ATR-PURPLE ', 'atr-purple', 'atr-purple-line', true],
    ['manual', 'manual', 'green-line', false], ['off', 'off', 'green-line', false], ['typo', 'off', 'green-line', false]
  ])('%s selects %s without altering BTC or another pair', (value, expected, setup, automatic) => {
    process.env[env] = String(value);
    expect(spotEntryModeStatus(market)).toMatchObject({ env, mode: expected, explicit: true, valid: value !== 'typo' });
    expect(automaticSpotEntryEnabled(market)).toBe(automatic);
    expect(spotEntrySetup(market)).toBe(setup);
    expect(spotManualEntryEnabled(market)).toBe(expected === 'manual');
    expect(spotEntryModeStatus('BTC-USD')).toMatchObject({ mode: 'green' });
    const other = asset === 'ETH' ? 'SOL' : 'ETH';
    process.env[`${other}_ENTRY_MODE`] = 'off';
    expect(spotEntryModeStatus(market)?.mode).toBe(expected);
    expect(spotEntryModeStatus('INJ-USD')).toBeUndefined();
    expect(spotEntryModeStatus('toString')).toBeUndefined();
    expect(spotEntryModeExplicit('INJ-USD')).toBe(false);
  });
  test.each(['purple', 'green', 'atr', 'atr-purple'])('%s overrides the old entry flag, but never the pair execution kill switch', mode => {
    process.env[env] = mode;
    process.env[`OPEN_LIQUIDITY_V2_${asset}_GAP_DAILY_ENTRY_ENABLED`] = 'false';
    expect(monitor.getStatus()).toMatchObject({ autoTradeEnabled: true, entryMode: { env, mode },
      gapDailyEntry: { enabled: true, env, initialStop: { mode: 'signal-body-open',
        trailingGate: 'later-same-direction-intrusion', trailingSignals: ['yellow'] } } });
    process.env[config.autoTradeEnv] = 'false';
    expect(monitor.getStatus().autoTradeEnabled).toBe(false);
  });
  test.each(['purple', 'green', 'atr', 'atr-purple', 'off', 'typo'])('manual plans stay untouched and unread candles/mail/orders stay absent in %s', async mode => {
    const manual = new BtcManualTradeOverrideMonitor(manualTradeOverrideConfigForMarket(market)!);
    const execute = jest.fn(), mail = jest.fn(); manual.configureEntryHandler({ executeManualEntry: execute, sendManualTriggerEmail: mail });
    manual.arm({ direction: 'long', closeTrigger: 120, takeProfits: [{ price: 140, allocationPct: 100 }] });
    manual.arm({ direction: 'short', closeTrigger: 100, takeProfits: [] });
    const before = fs.readFileSync(plans, 'utf8'); process.env[env] = mode;
    await manual.checkOnce();
    expect(manual.getStatus()).toMatchObject({ entriesPaused: true, entryMode: { mode: mode === 'typo' ? 'off' : mode } });
    expect(axios.get).not.toHaveBeenCalled(); expect(execute).not.toHaveBeenCalled(); expect(mail).not.toHaveBeenCalled();
    expect(fs.readFileSync(plans, 'utf8')).toBe(before);
    process.env[env] = 'manual'; expect(manual.getStatus().entriesPaused).toBe(false);
    expect(manual.getStatus().overrides.filter((row: any) => row.status === 'ARMED')).toHaveLength(2);
  });
  test.each(['green', 'purple', 'off'])('direct manual execution cannot bypass %s', async mode => {
    process.env[env] = mode;
    const result = await monitor.executeManualEntry({ market, direction: 'long', signature: 'blocked' });
    expect(result).toMatchObject({ tradePlaced: false, tradeDeferred: true });
    expect(orders.getAccountSnapshot).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test.each(['manual', 'off', 'purple', 'green', 'invalid'])('%s cannot fall through to legacy Shadow/Delay or queued automatic mail', async mode => {
    process.env[env] = mode;
    const request = { market, direction: 'long', signature: 'old-shadow' };
    expect((await monitor.executeFractalEntry(request)).tradeSkipped).toMatch(/legacy Shadow/);
    const result: any = {}; await monitor.executeAlertExclusive({}, {}, 'old-delay', result);
    expect(result.tradeSkipped).toMatch(/legacy Delay/);
    if (mode === 'manual' || mode === 'off' || mode === 'invalid') await monitor.flushPurpleTriggerEmails();
    expect(orders.placeOrder).not.toHaveBeenCalled(); expect(gap.sendEmailBestEffort).not.toHaveBeenCalled();
  });
  test.each(['SUBMITTING', 'UNKNOWN'])('manual cannot overtake an uncertain automatic %s submission', async outcome => {
    process.env[env] = 'manual';
    fs.writeFileSync(file, JSON.stringify({ gapDailyEntry: { intent: { outcome } } }));
    expect(await monitor.executeManualEntry({ market, direction: 'long', signature: 'manual' }))
      .toMatchObject({ tradePlaced: false, tradeDeferred: true, tradeSkipped: expect.stringContaining('uncertain') });
    expect(orders.getAccountSnapshot).not.toHaveBeenCalled(); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
  test.each(['EXECUTING', 'ARMED', 'corrupt'])('automatic entry respects %s manual ownership evidence', async kind => {
    process.env[env] = 'green';
    const raw = kind === 'corrupt' ? '{bad' : JSON.stringify({ version: 3, market, overrides: [{ status: kind }] });
    fs.writeFileSync(plans, raw);
    const state: any = { gapDailyEntry: { activatedAt: new Date(hour - 3_600_000).toISOString() } };
    const signal = { setup: 'green-line', time: hour / 1000 - 3600, closedAt: hour / 1000, direction: 'long', edge: 115, close: 125 };
    if (kind === 'corrupt') await expect(monitor.executeGapDailyEntry(state, signal, {})).rejects.toThrow('ownership');
    else if (kind === 'ARMED') {
      orders.getAccountSnapshot.mockRejectedValue(new Error('Reached account read'));
      await expect(monitor.executeGapDailyEntry(state, signal, {})).rejects.toThrow('Reached account read');
    } else {
      await monitor.executeGapDailyEntry(state, signal, {});
      expect(state.gapDailyEntry.decision).toMatchObject({ outcome: 'SKIPPED', reason: expect.stringContaining('unfinished') });
    }
    expect(fs.readFileSync(plans, 'utf8')).toBe(raw); expect(orders.placeOrder).not.toHaveBeenCalled();
  });
});
