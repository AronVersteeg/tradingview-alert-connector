import fs from 'fs';
import os from 'os';
import path from 'path';
import { CoinGlassEthWhaleCollector } from '../src/services/coinGlassEthWhaleCollector';
import { fetchCoinGlassWhaleLevelsViaWebSocket } from '../src/services/decentraderGapMonitor';

jest.mock('../src/services/decentraderGapMonitor', () => ({
  fetchCoinGlassWhaleLevelsViaWebSocket: jest.fn()
}));

describe('CoinGlass collector recovery and observation integrity', () => {
  let directory: string;
  let previousEnv: NodeJS.ProcessEnv;
  const now = Date.parse('2026-10-03T12:00:00Z');
  const fetchLevels = fetchCoinGlassWhaleLevelsViaWebSocket as jest.Mock;
  const levels: any[] = [{
    source: 'coinglass', symbol: 'Binance_ETHUSDT', instrument: 'Binance_ETHUSDT',
    key: 'wall', side: 'sell', price: 3000, volumeUsd: 12_000_000, updatedAt: new Date(now).toISOString()
  }];
  const context = { frameTimestamp: '2026-10-03 11:00:00', currentPrice: 2900 };
  beforeEach(() => {
    previousEnv = { ...process.env };
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'whale-collector-'));
    process.env.COINGLASS_WHALE_ETH_HISTORY_FILE = path.join(directory, 'history.json');
    process.env.COINGLASS_WHALE_LEVELS_ENABLED = 'true';
    process.env.COINGLASS_WHALE_ETH_ENABLED = 'true';
    process.env.COINGLASS_WHALE_TIMEOUT_MS = '12000';
    delete process.env.COINGLASS_WHALE_ETH_TIMEOUT_MS;
    delete process.env.COINGLASS_WHALE_ETH_FAILURE_BACKOFF_SECONDS;
    delete process.env.COINGLASS_WHALE_FAILURE_BACKOFF_SECONDS;
    jest.useFakeTimers();
    jest.setSystemTime(now);
    fetchLevels.mockReset().mockResolvedValue(levels);
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  });
  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
    process.env = previousEnv;
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('uses the repaired timeout and records each successful snapshot only once', async () => {
    const collector = new CoinGlassEthWhaleCollector();
    collector.configureObservationProvider(async () => context);
    await collector.refresh('test');
    expect(fetchLevels).toHaveBeenCalledWith('Binance_ETHUSDT', 'm1', expect.any(Number), 75_000);
    expect(collector.snapshot()).toMatchObject({ freshness: 'FRESH', consecutiveFailures: 0, levels });
    expect(collector.snapshot().observations).toHaveLength(1);
    jest.setSystemTime(now + 60_000);
    collector.recordObservation('2026-10-03 12:00:00', 2950);
    expect(collector.snapshot().observations).toHaveLength(1);
    expect(collector.snapshot().observations[0].observedAt).toBe(new Date(now).toISOString());
  });

  test('failed snapshots retain history but cannot be recorded as fresh observations', async () => {
    const collector = new CoinGlassEthWhaleCollector();
    collector.configureObservationProvider(async () => context);
    await collector.refresh('success');
    jest.setSystemTime(now + 60_000);
    fetchLevels.mockRejectedValue(new Error('provider timeout'));
    await collector.refresh('failure');
    collector.recordObservation('2026-10-03 12:00:00', 2950);
    expect(collector.snapshot().observations).toHaveLength(1);
    jest.setSystemTime(now + 16 * 60_000);
    expect(collector.snapshot()).toMatchObject({ freshness: 'STALE', levels: [], error: 'provider timeout' });
    expect(collector.snapshot().history).toHaveLength(1);
    expect(new CoinGlassEthWhaleCollector().snapshot()).toMatchObject({ freshness: 'MISSING', levels: [] });
    expect(new CoinGlassEthWhaleCollector().snapshot().history).toHaveLength(1);
  });

  test('starts failure backoff at completion, then resets after successful recovery', async () => {
    const collector = new CoinGlassEthWhaleCollector();
    fetchLevels.mockImplementationOnce(async () => {
      jest.setSystemTime(now + 75_000);
      throw new Error('snapshot timeout');
    });
    await collector.refresh('failure');
    expect(collector.snapshot().nextAttemptAt).toBe(new Date(now + 135_000).toISOString());
    await collector.refresh('too-early');
    expect(fetchLevels).toHaveBeenCalledTimes(1);
    jest.setSystemTime(now + 135_000);
    await collector.refresh('recovery');
    expect(fetchLevels).toHaveBeenCalledTimes(2);
    expect(collector.snapshot()).toMatchObject({ freshness: 'FRESH', consecutiveFailures: 0, error: undefined, levels });
  });
});
