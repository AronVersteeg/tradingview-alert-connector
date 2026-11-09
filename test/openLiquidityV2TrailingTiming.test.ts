import fs from 'fs';
import os from 'os';
import path from 'path';
import { OpenLiquidityV2EthTradeMonitor, openLiquidityV2BtcIntrusionMonitor,
  openLiquidityV2EthTradeMonitor, openLiquidityV2SolTradeMonitor } from '../src/services/openLiquidityV2EthTradeMonitor';

const hour = Date.parse('2026-11-09T14:00:00Z');
const initial = hour - 23 * 60_000;
const pairs = [openLiquidityV2BtcIntrusionMonitor, openLiquidityV2EthTradeMonitor, openLiquidityV2SolTradeMonitor];

describe('Close-aligned V2 protective checks independent of entry/map scans', () => {
  const originalEnv = { ...process.env };
  let monitor: any, management: jest.SpyInstance, map: jest.SpyInstance, directory: string;
  const ready = () => ({ ok: true, trailingCandle: { closedAt: new Date(hour).toISOString(), pending: false } });

  function setup(configured = openLiquidityV2EthTradeMonitor) {
    const config = (configured as any).config;
    process.env[config.enabledEnv] = 'false';
    process.env[config.autoTradeEnv] = 'false';
    process.env[config.pollMinutesEnv || 'DECENTRADER_GAP_POLL_MINUTES'] = '60';
    process.env[config.stateFileEnv] = path.join(directory, 'state.json');
    monitor = new OpenLiquidityV2EthTradeMonitor({} as any, config);
    map = jest.spyOn(monitor, 'check').mockResolvedValue({ ok: true });
    management = jest.spyOn(monitor, 'checkManagedPosition').mockResolvedValue(ready());
  }

  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(initial);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'v2-trailing-close-'));
    process.env.BTC_ENTRY_MODE = 'off';
    process.env.OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED = 'false';
    process.env.OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED = 'false';
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    setup();
  });
  afterEach(() => {
    monitor.stop(); jest.restoreAllMocks(); jest.useRealTimers(); process.env = { ...originalEnv };
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test.each(pairs)('checks $config.asset protection at close +2s even when entries are off', async configured => {
    setup(configured);
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    expect(management).toHaveBeenCalledTimes(1);
    expect(monitor.getStatus().trailingCloseCheck).toMatchObject({ enabled: true, delaySeconds: 2,
      maxRetrySeconds: 300, nextCheckAt: new Date(hour + 2000).toISOString() });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 1_999);
    expect(management).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(1);
    expect(management).toHaveBeenCalledTimes(2); expect(map).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(10 * 60_000);
    expect(management).toHaveBeenCalledTimes(2);
  });

  test('retries stale dYdX candles after five seconds, then stops once the new close is observed', async () => {
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    management.mockResolvedValueOnce({ ok: true, trailingCandle: { closedAt: new Date(hour - 3_600_000).toISOString(), pending: true } });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 2_000);
    expect(management).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(4_999); expect(management).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(1); expect(management).toHaveBeenCalledTimes(3);
    await jest.advanceTimersByTimeAsync(6 * 60_000); expect(management).toHaveBeenCalledTimes(3);
  });

  test('also retries when a shared in-flight poll reports the previous hour as fresh for that earlier check', async () => {
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    management.mockResolvedValueOnce({ ok: true, trailingCandle: { closedAt: new Date(hour - 3_600_000).toISOString(), pending: false } });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 7_000);
    expect(management).toHaveBeenCalledTimes(3);
  });

  test('retries errors within a five-minute bound and keeps the regular poll as fallback', async () => {
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    management.mockRejectedValue(new Error('indexer unavailable'));
    await jest.advanceTimersByTimeAsync(28 * 60_000);
    expect(management).toHaveBeenCalledTimes(16);
    await jest.advanceTimersByTimeAsync(10 * 60_000);
    expect(management).toHaveBeenCalledTimes(16);
    await jest.advanceTimersByTimeAsync(22 * 60_000);
    expect(management).toHaveBeenCalledTimes(17);
  });

  test('does not spin when there is no owned position or stop management is disabled', async () => {
    management.mockResolvedValue({ ok: true });
    monitor.start(0); await jest.advanceTimersByTimeAsync(30 * 60_000);
    expect(management).toHaveBeenCalledTimes(2); expect(map).not.toHaveBeenCalled();
  });

  test('cannot rearm a stopped timer after an in-flight close check resolves', async () => {
    let resolve: (value: any) => void = () => {};
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    management.mockImplementationOnce(() => new Promise(done => { resolve = done; }));
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 2_000);
    monitor.stop(); resolve({ ok: false });
    await jest.advanceTimersByTimeAsync(70 * 60_000);
    expect(management).toHaveBeenCalledTimes(2);
    expect(monitor.getStatus().trailingCloseCheck).toMatchObject({ enabled: false, nextCheckAt: undefined });
  });

  test('does not postpone the first check when startup occurs just after the hour', async () => {
    jest.setSystemTime(hour + 500);
    monitor.start(0); await jest.advanceTimersByTimeAsync(0);
    await jest.advanceTimersByTimeAsync(1500);
    expect(management).toHaveBeenCalledTimes(2);
  });
});
