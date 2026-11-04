import fs from 'fs';
import os from 'os';
import path from 'path';
import { BtcManualTradeOverrideMonitor, manualTradeOverrideConfigForMarket, readBtcManualTradeOverrideStore } from '../src/services/btcManualTradeOverride';
import { binanceGet, binanceRetryAt } from '../src/services/binanceHttp';
import { binanceHourlyCloseFeed } from '../src/services/binanceHourlyCloseFeed';

jest.mock('../src/services/binanceHttp', () => ({ binanceGet: jest.fn(), binanceRetryAt: jest.fn(() => 0) }));
jest.mock('../src/services/binanceHourlyCloseFeed', () => ({ binanceHourlyCloseFeed: { latest: jest.fn() } }));

describe('Manual entry recovery from unavailable candle data', () => {
  let directory: string;
  let monitor: BtcManualTradeOverrideMonitor;
  let execute: jest.Mock;
  const originalEnv = { ...process.env };

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(Date.parse('2026-10-02T11:30:00.000Z'));
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'manual-data-recovery-'));
    process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'state.json');
    process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED = 'true';
    monitor = new BtcManualTradeOverrideMonitor();
    execute = jest.fn(async () => ({ tradePlaced: true }));
    monitor.configureEntryHandler({ executeManualBtcEntry: execute, sendManualBtcTriggerEmail: async () => ({ sent: true }) });
    monitor.arm({ direction: 'long', closeTrigger: 80000, takeProfits: [] });
    jest.setSystemTime(Date.parse('2026-10-02T12:01:00.000Z'));
    (binanceGet as jest.Mock).mockReset();
    (binanceRetryAt as jest.Mock).mockReturnValue(0);
    (binanceHourlyCloseFeed.latest as jest.Mock).mockReturnValue(undefined);
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
    process.env = { ...originalEnv };
    fs.rmSync(directory, { recursive: true, force: true });
  });

  function rows(hour: number): any[] {
    const open = Date.UTC(2026, 9, 2, hour);
    return [[open, '79000', '81000', '78900', '80100', '1', open + 3_600_000 - 1]];
  }

  test('exposes the current manual stop policy without executing or fetching a candle', () => {
    expect(monitor.getStatus().initialStop).toMatchObject({ mode: 'signal-body-quarter', bodyFraction: .25,
      candleSource: 'dydx-1h', sameSignalHour: true, sizingSource: 'dydx-fractal', tightenOnly: true });
    expect(execute).not.toHaveBeenCalled(); expect(binanceGet).not.toHaveBeenCalled();
  });

  test('keeps the plan armed and retries shortly after the Binance cooldown, not an hour later', async () => {
    (binanceGet as jest.Mock).mockRejectedValue(new Error('cooldown active'));
    const retryAt = Date.now() + 300_000;
    (binanceRetryAt as jest.Mock).mockReturnValue(retryAt);
    await expect(monitor.checkOnce()).rejects.toThrow('cooldown active');
    expect(execute).not.toHaveBeenCalled();
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('ARMED');
    (monitor as any).scheduleNextRun();
    expect(monitor.getStatus().nextRunAt).toBe(new Date(retryAt + 1000).toISOString());
  });

  test('rejects an older closed candle and opens once after the latest candle becomes available', async () => {
    (binanceGet as jest.Mock).mockResolvedValue({ data: rows(10) });
    await expect(monitor.checkOnce()).rejects.toThrow('latest closed 1H candle');
    expect(execute).not.toHaveBeenCalled();
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('ARMED');
    (binanceGet as jest.Mock).mockResolvedValue({ data: rows(11) });
    await monitor.checkOnce();
    await monitor.checkOnce();
    expect(execute).toHaveBeenCalledTimes(1);
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('TRIGGERED');
  });

  test('BTC ignores the Futures stream and triggers only from Spot closes', async () => {
    (binanceGet as jest.Mock).mockResolvedValue({ data: rows(11) });
    const openTime = Date.UTC(2026, 9, 2, 11);
    (binanceHourlyCloseFeed.latest as jest.Mock).mockReturnValue({ symbol: 'BTCUSDT', openTime, closeTime: openTime + 3_600_000 - 1, close: '79900' });
    await monitor.checkOnce();
    expect(binanceHourlyCloseFeed.latest).not.toHaveBeenCalled();
    expect(binanceGet).toHaveBeenCalledWith('https://api.binance.com/api/v3/klines', expect.objectContaining({ params: { symbol: 'BTCUSDT', interval: '1h', limit: 12 } }));
    expect(execute).toHaveBeenCalledTimes(1);
    expect(execute.mock.calls[0][0].signalClose).toBe(80100);
    expect(monitor.getStatus().lastCandleObservation.source).toBe('binance-spot');
  });

  test('does not change the existing non-BTC manual Futures source', async () => {
    process.env.SOL_MANUAL_TRADE_OVERRIDE_FILE = path.join(directory, 'sol-plans.json');
    const sol = new BtcManualTradeOverrideMonitor(manualTradeOverrideConfigForMarket('SOL-USD')!);
    sol.configureEntryHandler({ executeManualEntry: execute, sendManualTriggerEmail: async () => ({ sent: true }) });
    jest.setSystemTime(Date.parse('2026-10-02T11:30:00Z'));
    sol.arm({ direction: 'long', closeTrigger: 100, takeProfits: [] });
    jest.setSystemTime(Date.parse('2026-10-02T12:01:00Z'));
    const openTime = Date.UTC(2026, 9, 2, 11);
    (binanceHourlyCloseFeed.latest as jest.Mock).mockReturnValue({ symbol: 'SOLUSDT', openTime, closeTime: openTime + 3_600_000 - 1, close: '110' });
    await sol.checkOnce();
    expect(binanceGet).not.toHaveBeenCalled(); expect(execute).toHaveBeenCalledTimes(1);
    expect(sol.getStatus().lastCandleObservation.source).toBe('binance-futures');
  });

  test('keeps a quarter-body entry armed while dYdX publishes the same hour, retries promptly and mails only the final result', async () => {
    const email = jest.fn(async () => ({ sent: true }));
    monitor.configureEntryHandler({ executeManualBtcEntry: execute, sendManualBtcTriggerEmail: email });
    (binanceGet as jest.Mock).mockResolvedValue({ data: rows(11) });
    execute.mockResolvedValueOnce({ tradePlaced: false, tradeDeferred: true, tradePendingData: true,
      tradeSkipped: 'Awaiting the same-hour dYdX body.' });
    await monitor.checkOnce();
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('ARMED');
    expect(email).not.toHaveBeenCalled();
    (monitor as any).scheduleNextRun();
    expect(monitor.getStatus().nextRunAt).toBe('2026-10-02T12:02:00.000Z');
    await monitor.checkOnce(); await monitor.checkOnce();
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('TRIGGERED');
    expect(execute).toHaveBeenCalledTimes(2); expect(email).toHaveBeenCalledTimes(1);
    expect(execute.mock.calls[0][0].signalCandleStartedAt).toBe(execute.mock.calls[1][0].signalCandleStartedAt);
    expect(execute.mock.calls[0][0].signature).toBe(execute.mock.calls[1][0].signature);
  });

  test('does not widen the existing entry freshness window while waiting for dYdX', async () => {
    (binanceGet as jest.Mock).mockResolvedValue({ data: rows(11) });
    execute.mockResolvedValue({ tradePlaced: false, tradeDeferred: true, tradePendingData: true });
    await monitor.checkOnce();
    jest.setSystemTime(Date.parse('2026-10-02T12:16:00Z'));
    await monitor.checkOnce();
    expect(execute).toHaveBeenCalledTimes(1);
    expect(readBtcManualTradeOverrideStore().overrides[0].status).toBe('ARMED');
    expect(monitor.getStatus().retryData).toBe(false);
  });
});
