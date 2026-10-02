import fs from 'fs';
import os from 'os';
import path from 'path';
import { BtcManualTradeOverrideMonitor, readBtcManualTradeOverrideStore } from '../src/services/btcManualTradeOverride';
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

  test('uses the same Futures candle from the stream during a REST ban, without REST requests', async () => {
    (binanceGet as jest.Mock).mockRejectedValue(new Error('cooldown active'));
    const openTime = Date.UTC(2026, 9, 2, 11);
    (binanceHourlyCloseFeed.latest as jest.Mock).mockReturnValue({ symbol: 'BTCUSDT', openTime, closeTime: openTime + 3_600_000 - 1, close: '80100' });
    await monitor.checkOnce();
    expect(binanceGet).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(1);
  });
});
