import { EventEmitter } from 'events';
import zlib from 'zlib';
import {
  coinGlassSnapshotTimeoutMs,
  decodeCoinGlassMessage,
  fetchCoinGlassWhaleMessage
} from '../src/services/coinGlassWhaleSocket';

jest.mock('ws', () => {
  const { EventEmitter } = require('events');
  const client: any = jest.fn(() => Object.assign(new EventEmitter(), {
    readyState: 1, send: jest.fn(), terminate: jest.fn()
  }));
  client.OPEN = 1;
  return client;
});

const symbol = 'Binance_BTCUSDT';
const payload = {
  channel: 'largeTakerOrder',
  params: { symbol, interval: 'm1' },
  data: [{ instrument: { symbol }, list: [{ price: 80_000, currentUsd: 12_000_000, side: 1 }] }]
};

describe('CoinGlass snapshot transport', () => {
  const client = require('ws');
  beforeEach(() => {
    jest.useFakeTimers();
    client.mockClear();
  });
  afterEach(() => jest.useRealTimers());

  test('allows an entire m1 push window even with an old 12-second timeout setting', () => {
    expect(coinGlassSnapshotTimeoutMs(12_000)).toBe(75_000);
    expect(coinGlassSnapshotTimeoutMs()).toBe(75_000);
    expect(coinGlassSnapshotTimeoutMs(90_000)).toBe(90_000);
    expect(coinGlassSnapshotTimeoutMs(Infinity)).toBe(75_000);
    expect(coinGlassSnapshotTimeoutMs(999_999)).toBe(120_000);
  });

  test.each(['json', 'gzip', 'zlib', 'raw'])(
    'decodes %s without accepting undecodable heartbeats', (format) => {
      const json = Buffer.from(JSON.stringify(payload));
      const encoded = format === 'gzip' ? zlib.gzipSync(json)
        : format === 'zlib' ? zlib.deflateSync(json)
          : format === 'raw' ? zlib.deflateRawSync(json) : json;
      expect(decodeCoinGlassMessage(encoded)).toEqual(payload);
      expect(decodeCoinGlassMessage('pong')).toBeUndefined();
      expect(decodeCoinGlassMessage(Buffer.from([255, 0, 1]))).toBeUndefined();
    }
  );

  test('accepts a late gzip snapshot, sends keepalive, and disposes timers/socket', async () => {
    const request = fetchCoinGlassWhaleMessage(symbol, 'm1', 12_000);
    const socket = client.mock.results[0].value;
    socket.emit('open');
    expect(JSON.parse(socket.send.mock.calls[0][0])).toMatchObject({
      method: 'subscribe', params: [{ symbol, channel: 'largeTakerOrder', interval: 'm1' }]
    });
    jest.advanceTimersByTime(50_000);
    expect(socket.terminate).not.toHaveBeenCalled();
    expect(socket.send).toHaveBeenCalledWith('ping');
    socket.emit('message', zlib.gzipSync(Buffer.from(JSON.stringify(payload))));
    await expect(request).resolves.toEqual(payload);
    expect(socket.terminate).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('ignores heartbeats, acknowledgements, wrong channels/symbols/intervals', async () => {
    const request = fetchCoinGlassWhaleMessage(symbol, 'm1');
    const socket = client.mock.results[0].value;
    socket.emit('open');
    for (const message of [
      'pong', JSON.stringify({ method: 'subscribe', data: [] }),
      JSON.stringify({ ...payload, channel: 'largeTakerTrade' }),
      JSON.stringify({ ...payload, params: { symbol: 'Binance_ETHUSDT' } }),
      JSON.stringify({ ...payload, params: { symbol, interval: 'm5' } })
    ]) socket.emit('message', Buffer.from(message));
    expect(socket.terminate).not.toHaveBeenCalled();
    socket.emit('message', Buffer.from(JSON.stringify({ ...payload, data: [] })));
    await expect(request).resolves.toMatchObject({ data: [] });
    expect(jest.getTimerCount()).toBe(0);
  });

  test('bounds connect and snapshot waits and preserves useful diagnostics', async () => {
    const connect = fetchCoinGlassWhaleMessage(symbol, 'm1');
    const connectError = expect(connect).rejects.toThrow('connect/upgrade');
    jest.advanceTimersByTime(10_000);
    await connectError;
    const snapshot = fetchCoinGlassWhaleMessage(symbol, 'm1');
    const snapshotError = expect(snapshot).rejects.toThrow('snapshot; symbol Binance_BTCUSDT; messages 1; decoded 0');
    const socket = client.mock.results[1].value;
    socket.emit('open');
    socket.emit('message', Buffer.from('pong'));
    jest.advanceTimersByTime(75_000);
    await snapshotError;
    expect(jest.getTimerCount()).toBe(0);
  });

  test.each(['error', 'close'])('rejects a premature %s without leaking timers', async (event) => {
    const request = fetchCoinGlassWhaleMessage(symbol, 'm1');
    const rejected = expect(request).rejects.toThrow(event === 'error' ? 'socket failure' : 'code 1006');
    const socket = client.mock.results[0].value;
    socket.emit('open');
    socket.emit(event, event === 'error' ? new Error('socket failure') : 1006);
    await rejected;
    expect(jest.getTimerCount()).toBe(0);
  });

  test('uses ws to assemble a fragmented binary snapshot across a protocol ping', async () => {
    jest.useRealTimers();
    const realWs = jest.requireActual('ws');
    const server = new realWs.Server({ port: 0, host: '127.0.0.1' });
    await new Promise<void>((resolve) => server.on('listening', resolve));
    client.mockImplementationOnce((_url: string, options: any) =>
      new realWs(`ws://127.0.0.1:${server.address().port}`, options));
    server.on('connection', (peer: any) => {
      peer.once('message', () => {
        const compressed = zlib.gzipSync(Buffer.from(JSON.stringify(payload)));
        const midpoint = Math.floor(compressed.length / 2);
        peer.send(compressed.subarray(0, midpoint), { binary: true, fin: false });
        peer.ping('alive');
        peer.send(compressed.subarray(midpoint), { binary: true, fin: true });
      });
    });
    try {
      await expect(fetchCoinGlassWhaleMessage(symbol, 'm1')).resolves.toEqual(payload);
    } finally {
      server.clients.forEach((peer: any) => peer.terminate());
      await new Promise<void>((resolve) => server.close(resolve));
    }
  });
});
