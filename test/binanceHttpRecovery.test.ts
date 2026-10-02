import fs from 'fs';
import os from 'os';
import path from 'path';

jest.mock('axios', () => ({ get: jest.fn() }));

describe('Binance transport recovery', () => {
  const url = 'https://fapi.binance.com/fapi/v1/klines';
  const config = { params: { symbol: 'BTCUSDT', interval: '1h', limit: 12 } };
  let directory: string;
  let get: jest.Mock;
  let http: typeof import('../src/services/binanceHttp');
  const originalEnv = { ...process.env };

  beforeEach(() => {
    jest.resetModules();
    jest.useFakeTimers();
    jest.setSystemTime(Date.parse('2026-10-02T12:00:00.000Z'));
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'binance-recovery-'));
    process.env.DECENTRALIZED_DOM_HISTORY_DIR = path.join(directory, 'dom');
    process.env.BINANCE_REQUEST_MIN_INTERVAL_MS = '100';
    process.env.BINANCE_REQUEST_WEIGHT_BUDGET_PER_MINUTE = '50';
    http = require('../src/services/binanceHttp');
    get = require('axios').get;
  });

  afterEach(() => {
    jest.useRealTimers();
    process.env = { ...originalEnv };
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('shares concurrent public candle reads and briefly reuses the successful response', async () => {
    get.mockResolvedValue({ data: [[1]], headers: {} });
    const [first, second] = await Promise.all([http.binanceGet(url, config), http.binanceGet(url, config)]);
    expect(first).toBe(second);
    await http.binanceGet(url, config);
    expect(get).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(2001);
    await http.binanceGet(url, config);
    expect(get).toHaveBeenCalledTimes(2);
  });

  test('does not share live depth reads', async () => {
    get.mockResolvedValue({ data: { bids: [] }, headers: {} });
    await http.binanceGet('https://fapi.binance.com/fapi/v1/depth', { params: { symbol: 'BTCUSDT', limit: 100 } });
    const second = http.binanceGet('https://fapi.binance.com/fapi/v1/depth', { params: { symbol: 'BTCUSDT', limit: 100 } });
    await jest.advanceTimersByTimeAsync(101);
    await second;
    expect(get).toHaveBeenCalledTimes(2);
  });

  test('honors shared-IP weight headers before sending the next request', async () => {
    get.mockResolvedValue({ data: [], headers: { 'x-mbx-used-weight-1m': '49' } });
    await http.binanceGet(url, config);
    const second = http.binanceGet(url, { params: { ...config.params, symbol: 'ETHUSDT' } });
    await jest.advanceTimersByTimeAsync(60_999);
    expect(get).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(1);
    await second;
    expect(get).toHaveBeenCalledTimes(2);
  });

  test('also limits estimated local weights when headers are absent', async () => {
    get.mockResolvedValue({ data: [], headers: {} });
    const depth = 'https://fapi.binance.com/fapi/v1/depth';
    await http.binanceGet(depth, { params: { limit: 1000 } });
    const second = http.binanceGet(depth, { params: { limit: 1000 } });
    await jest.advanceTimersByTimeAsync(100);
    await second;
    const third = http.binanceGet(depth, { params: { limit: 1000 } });
    await jest.advanceTimersByTimeAsync(60_899);
    expect(get).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(1);
    await third;
    expect(get).toHaveBeenCalledTimes(3);
  });

  test('blocks queued requests, persists the ban, and restores it after a process reload', async () => {
    const until = Date.now() + 3_600_000;
    get.mockRejectedValue({ response: { status: 418, headers: {}, data: { code: -1003, msg: `IP banned until ${until}` } } });
    const first = http.binanceGet(url, config);
    const second = http.binanceGet(url, { params: { ...config.params, symbol: 'ETHUSDT' } });
    await expect(first).rejects.toMatchObject({ retryAt: until });
    await expect(second).rejects.toMatchObject({ retryAt: until });
    expect(get).toHaveBeenCalledTimes(1);
    expect(http.binanceHttpStatus()[0]).toMatchObject({ blocked: true, localRequests5m: 1 });
    jest.resetModules();
    http = require('../src/services/binanceHttp');
    get = require('axios').get;
    await expect(http.binanceGet(url, config)).rejects.toMatchObject({ retryAt: until });
    expect(get).not.toHaveBeenCalled();
  });

  test('does not block Spot when Futures is banned and recovers Futures after expiry', async () => {
    const until = Date.now() + 10_000;
    get.mockRejectedValueOnce({ response: { status: 429, headers: {}, data: { code: -1003, msg: `banned until ${until}` } } });
    await expect(http.binanceGet(url, config)).rejects.toMatchObject({ retryAt: until });
    get.mockResolvedValue({ data: [], headers: {} });
    await http.binanceGet('https://api.binance.com/api/v3/klines', config);
    expect(get).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(10_001);
    await http.binanceGet(url, config);
    expect(get).toHaveBeenCalledTimes(3);
  });
});
