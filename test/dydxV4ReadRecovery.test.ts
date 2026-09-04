import axios from 'axios';
import http from 'http';
import { AddressInfo } from 'net';
import { IndexerClient, IndexerConfig } from '@dydxprotocol/v4-client-js';
import { createReadResilientIndexerClient } from '../src/services/dydx_v4/indexerReadRecovery';
import { DydxV4Client } from '../src/services/dydx_v4/dydxV4Client';

const config = new IndexerConfig('https://indexer.example', 'wss://indexer.example');
const networkError = () => Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });

describe('dYdX indexer read recovery', () => {
  beforeEach(() => {
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    jest.spyOn(console, 'info').mockImplementation(() => undefined);
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  function setup() {
    jest.useFakeTimers();
    const get = jest.fn();
    jest.spyOn(axios, 'create').mockReturnValue({ get } as any);
    return { get, indexer: createReadResilientIndexerClient(config) };
  }

  test('preserves SDK routes and query serialization, and returns fresh response data', async () => {
    const { get, indexer } = setup();
    const data = { markets: { 'BTC-USD': { ticker: 'BTC-USD' } } };
    get.mockResolvedValue({ data });

    await expect(indexer.markets.getPerpetualMarkets('BTC-USD')).resolves.toEqual(data);
    expect(get).toHaveBeenCalledWith('https://indexer.example/v4/perpetualMarkets?ticker=BTC-USD', {
      timeout: 8000, signal: expect.any(AbortSignal)
    });
    expect(get).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('recovers a reset without making the account appear flat', async () => {
    const { get, indexer } = setup();
    get.mockRejectedValueOnce(networkError()).mockResolvedValueOnce({
      data: { subaccount: { equity: '100', openPerpetualPositions: {
        'INJ-USD': { market: 'INJ-USD', size: '-74.4', entryPrice: '5.163' }
      } } }
    });
    const client = new DydxV4Client() as any;
    client.initialized = true;
    client.wallet = { address: 'dydx1private' };
    client.indexer = indexer;
    const result = client.getAccountSnapshot([]);
    await jest.runAllTimersAsync();
    const account = await result;
    expect(account.openPositionsCount).toBe(1);
    expect(account.openPositions[0].size).toBe(-74.4);
    expect(get).toHaveBeenCalledTimes(2);
    expect(console.info).toHaveBeenCalledWith('dYdX indexer read recovered.', expect.objectContaining({ attempts: 2 }));
    expect(JSON.stringify((console.warn as jest.Mock).mock.calls)).not.toContain('dydx1private');
    expect(jest.getTimerCount()).toBe(0);
  });

  test.each([408, 429, 500, 502, 503, 504])('retries transient HTTP %i', async status => {
    const { get, indexer } = setup();
    get.mockRejectedValueOnce({ response: { status } }).mockResolvedValueOnce({ data: { positions: [] } });
    const result = indexer.account.getSubaccountPerpetualPositions('dydx1test', 0);
    await jest.runAllTimersAsync();
    await expect(result).resolves.toEqual({ positions: [] });
    expect(get).toHaveBeenCalledTimes(2);
  });

  test.each([400, 401, 403, 404, 422])('does not retry HTTP %i', async status => {
    const { get, indexer } = setup();
    const error = { code: 'ECONNRESET', response: { status } };
    get.mockRejectedValue(error);
    await expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toBe(error);
    expect(get).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('does not retry an unrelated validation error', async () => {
    const { get, indexer } = setup();
    const error = new Error('invalid configuration');
    get.mockRejectedValue(error);
    await expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toBe(error);
    expect(get).toHaveBeenCalledTimes(1);
  });

  test('fails closed after three attempts, rather than returning an empty account', async () => {
    const { get, indexer } = setup();
    const error = networkError();
    get.mockRejectedValue(error);
    const result = expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toBe(error);
    await jest.runAllTimersAsync();
    await result;
    expect(get).toHaveBeenCalledTimes(3);
    expect(jest.getTimerCount()).toBe(0);
  });

  test.each(['seconds', 'date'])('respects Retry-After expressed as %s', async format => {
    const { get, indexer } = setup();
    const retryAfter = format === 'seconds' ? '2' : new Date(Date.now() + 2000).toUTCString();
    get.mockRejectedValueOnce({ response: { status: 429, headers: { 'retry-after': retryAfter } } })
      .mockResolvedValueOnce({ data: {} });
    const result = indexer.account.getSubaccount('dydx1test', 0);
    await jest.advanceTimersByTimeAsync(750);
    expect(get).toHaveBeenCalledTimes(1);
    await jest.runAllTimersAsync();
    await result;
    expect(get).toHaveBeenCalledTimes(2);
  });

  test('does not retry early when Retry-After exceeds the total time budget', async () => {
    const { get, indexer } = setup();
    const error = { response: { status: 429, headers: { 'retry-after': '120' } } };
    get.mockRejectedValue(error);
    await expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toBe(error);
    expect(get).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('aborts each stalled request before starting another one', async () => {
    const { get, indexer } = setup();
    const signals: AbortSignal[] = [];
    get.mockImplementation((_url, { signal }) => {
      signals.push(signal);
      return new Promise((_resolve, reject) => signal.addEventListener('abort', () => {
        reject(Object.assign(new Error('canceled'), { code: 'ERR_CANCELED' }));
      }, { once: true }));
    });
    const start = Date.now();
    const result = expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toThrow('canceled');
    await jest.runAllTimersAsync();
    await result;
    expect(get).toHaveBeenCalledTimes(3);
    expect(signals.every(signal => signal.aborted)).toBe(true);
    expect(Date.now() - start).toBeLessThanOrEqual(30000);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('keeps writes, other SDK instances and global Axios defaults unchanged', () => {
    const other = new IndexerClient(config);
    const timeout = axios.defaults.timeout;
    const { indexer } = setup();
    expect(indexer.account.post).toBe(other.account.post);
    expect(indexer.markets.post).toBe(other.markets.post);
    expect(indexer.utility.get).toBe(other.utility.get);
    expect(indexer.account.get).not.toBe(other.account.get);
    expect(indexer.markets.get).not.toBe(other.markets.get);
    expect(axios.defaults.timeout).toBe(timeout);
  });

  test('aborts the final attempt at the total budget, even with a long Retry-After', async () => {
    const { get, indexer } = setup();
    get.mockRejectedValueOnce({ response: { status: 429, headers: { 'retry-after': '25' } } })
      .mockImplementationOnce((_url, { signal }) => new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(new Error('canceled')), { once: true });
      }));
    const start = Date.now();
    const result = expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toThrow('canceled');
    await jest.runAllTimersAsync();
    await result;
    expect(Date.now() - start).toBe(30000);
    expect(get).toHaveBeenCalledTimes(2);
    expect(get.mock.calls[1][1].timeout).toBe(5000);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('recovers conditional-order reads with their original status filters', async () => {
    const { get, indexer } = setup();
    let untriggeredAttempts = 0;
    get.mockImplementation(async url => {
      if (url.includes('status=UNTRIGGERED')) {
        if (++untriggeredAttempts === 1) throw networkError();
        return { data: [{ id: 'stop', market: 'BTC-USD', status: 'UNTRIGGERED', type: 'STOP_MARKET' }] };
      }
      return { data: [] };
    });
    const client = new DydxV4Client() as any;
    client.wallet = { address: 'dydx1test' };
    client.indexer = indexer;
    const result = client.getOpenOrdersForMarket('BTC-USD');
    await jest.runAllTimersAsync();
    await expect(result).resolves.toEqual([expect.objectContaining({ id: 'stop' })]);
    expect(get).toHaveBeenCalledTimes(4);
    expect(untriggeredAttempts).toBe(2);
  });

  test('preserves an already reached position when the final pre-flatten read initially fails', async () => {
    const { get, indexer } = setup();
    get.mockRejectedValueOnce(networkError()).mockResolvedValueOnce({
      data: { positions: [{ market: 'INJ-USD', size: '-74.4', entryPrice: '5.163' }] }
    });
    const client = new DydxV4Client() as any;
    client.wallet = { address: 'dydx1test' };
    client.indexer = indexer;
    client.FAILSAFE_FLATTEN_ON_TARGET_FAILURE = true;
    client.reachTargetPositionSafely = jest.fn().mockRejectedValue(new Error('indexer lag'));
    client.cancelOpenOrders = jest.fn();
    client.flattenPositionSafely = jest.fn();
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    const result = client.reachTargetPositionOrFailsafeFlat('INJ-USD', -74.4);
    await jest.runAllTimersAsync();
    await expect(result).resolves.toBe(true);
    expect(client.cancelOpenOrders).not.toHaveBeenCalled();
    expect(client.flattenPositionSafely).not.toHaveBeenCalled();
  });

  test('legacy fallback propagates read failures instead of treating them as no positions', async () => {
    const client = new DydxV4Client() as any;
    const error = networkError();
    client.wallet = { address: 'dydx1test' };
    client.indexer = { account: {
      getSubaccountPerpetualPositions: jest.fn().mockRejectedValue(error),
      getSubaccountAssetPositions: jest.fn().mockResolvedValue({ positions: [] })
    } };
    await expect(client.getSubaccountSnapshotBestEffort()).rejects.toBe(error);
  });

  test('legacy fallback rejects missing methods rather than returning an empty account', async () => {
    const client = new DydxV4Client() as any;
    client.indexer = { account: {} };
    await expect(client.getSubaccountSnapshotBestEffort()).rejects.toThrow('read methods are unavailable');
  });

  test('really closes stalled HTTP connections, without requiring a response', async () => {
    let requests = 0;
    const sockets = new Set<import('net').Socket>();
    const server = http.createServer(() => { requests++; });
    server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    try {
      const endpoint = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      const indexer = createReadResilientIndexerClient(new IndexerConfig(endpoint, 'ws://localhost'), {
        timeoutMs: 100, maxAttempts: 3, maxElapsedMs: 2000, backoffMs: 5
      });
      await expect(indexer.account.getSubaccount('dydx1test', 0)).rejects.toThrow();
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(requests).toBe(3);
      expect(sockets.size).toBe(0);
    } finally {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });
});
