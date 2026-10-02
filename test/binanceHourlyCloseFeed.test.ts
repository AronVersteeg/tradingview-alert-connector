import fs from 'fs';
import os from 'os';
import path from 'path';
import { BinanceHourlyCloseFeed, parseBinanceHourlyClose } from '../src/services/binanceHourlyCloseFeed';

jest.mock('ws', () => {
  const { EventEmitter } = require('events');
  const client: any = jest.fn(() => Object.assign(new EventEmitter(), {
    readyState: 1, terminate: jest.fn(), ping: jest.fn()
  }));
  client.OPEN = 1;
  return client;
});

describe('Binance confirmed hourly close stream', () => {
  const now = Date.parse('2026-10-02T12:01:00.000Z');
  const open = Date.parse('2026-10-02T11:00:00.000Z');
  function payload(changes: any = {}): any {
    return { data: { e: 'kline', s: 'BTCUSDT', k: { s: 'BTCUSDT', i: '1h', x: true, t: open, T: open + 3_600_000 - 1, c: '80100.00', ...changes } } };
  }

  test('accepts only final Futures 1H candles and preserves exact prices', () => {
    expect(parseBinanceHourlyClose(payload(), now)).toMatchObject({ symbol: 'BTCUSDT', close: '80100.00', openTime: open });
    expect(parseBinanceHourlyClose(payload({ x: false }), now)).toBeUndefined();
    expect(parseBinanceHourlyClose(payload({ i: '1d' }), now)).toBeUndefined();
    expect(parseBinanceHourlyClose(payload({ s: 'ETHUSDT' }), now)).toBeUndefined();
    expect(parseBinanceHourlyClose(payload({ c: 'Infinity' }), now)).toBeUndefined();
    expect(parseBinanceHourlyClose(payload(), open)).toBeUndefined();
    expect(parseBinanceHourlyClose(payload({ T: open + 3_600_000 }), now)).toBeUndefined();
  });

  test('persists closes, rejects old candles for entry, and reconnects without duplicate sockets', () => {
    const previous = process.env.DECENTRALIZED_DOM_HISTORY_DIR;
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'hourly-feed-'));
    process.env.DECENTRALIZED_DOM_HISTORY_DIR = path.join(directory, 'dom');
    jest.useFakeTimers();
    jest.setSystemTime(now);
    const client = require('ws');
    client.mockClear();
    const feed = new BinanceHourlyCloseFeed();
    try {
      feed.start();
      feed.start();
      expect(client).toHaveBeenCalledTimes(1);
      const socket = client.mock.results[0].value;
      socket.emit('open');
      socket.emit('message', Buffer.from(JSON.stringify(payload())));
      expect(feed.latest('BTCUSDT')).toMatchObject({ close: '80100.00' });
      expect(feed.latest('ETHUSDT')).toBeUndefined();
      expect(new BinanceHourlyCloseFeed().latest('BTCUSDT')).toMatchObject({ openTime: open });
      socket.emit('message', Buffer.from(JSON.stringify(payload({ t: open - 3_600_000, T: open - 1, c: '79000' }))));
      expect(feed.latest('BTCUSDT')?.close).toBe('80100.00');
      expect(feed.latest('BTCUSDT', now + 3_600_000)).toBeUndefined();
      socket.emit('close');
      jest.advanceTimersByTime(5000);
      expect(client).toHaveBeenCalledTimes(2);
      feed.stop();
      client.mock.results[1].value.emit('close');
      jest.advanceTimersByTime(120_000);
      expect(client).toHaveBeenCalledTimes(2);
    } finally {
      feed.stop();
      jest.useRealTimers();
      if (previous === undefined) delete process.env.DECENTRALIZED_DOM_HISTORY_DIR;
      else process.env.DECENTRALIZED_DOM_HISTORY_DIR = previous;
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });
});
