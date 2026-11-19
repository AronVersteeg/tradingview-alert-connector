import { EventEmitter } from 'events';
import { BtcSpotCloseShadow, btcGreenObservation, parseBtcSpotClosedHour } from '../src/services/btcSpotCloseShadow';

const HOUR = 3_600_000;
const close = Date.parse('2026-11-19T17:00:00Z');
const event = (boundary = close) => ({ e: 'kline', s: 'BTCUSDT', k: {
  s: 'BTCUSDT', i: '1h', x: true, t: boundary - HOUR, T: boundary - 1, o: '100', h: '125', l: '95', c: '120'
} });
const observation = () => btcGreenObservation({
  frames: [{ startedAtMs: close - 2 * HOUR, open: 100, high: 110, low: 95, close: 103 },
    { startedAtMs: close - HOUR, open: 100, high: 125, low: 95, close: 120 }],
  gaps: [{ left: 90, right: 115 }, null]
}, [{ direction: 'long', signature: 'fixture' }], {
  setup: 'green-line', direction: 'long', edge: 115, time: (close - HOUR) / 1000, closedAt: close / 1000, close: 120
});

describe('BTC Binance Spot shadow close feed', () => {
  let shadow: BtcSpotCloseShadow;
  let sockets: Array<EventEmitter & { terminate: jest.Mock; ping: jest.Mock; readyState: number }>;
  let preview: jest.Mock;
  const send = (data = event()) => sockets.at(-1)!.emit('message', Buffer.from(JSON.stringify(data)));

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(close + 1000);
    sockets = [];
    preview = jest.fn(observation);
    shadow = new BtcSpotCloseShadow(preview, () => {
      const socket = Object.assign(new EventEmitter(), { terminate: jest.fn(), ping: jest.fn(), readyState: 1 });
      sockets.push(socket);
      return socket;
    });
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => { shadow.stop(); jest.restoreAllMocks(); jest.useRealTimers(); });

  test('accepts only the current finalized BTCUSDT Spot hour with valid, aligned OHLC', () => {
    expect(parseBtcSpotClosedHour(event(), Date.now())).toEqual({
      timestampMs: close - HOUR, closeTimeMs: close - 1, open: 100, high: 125, low: 95, close: 120
    });
    for (const fields of [{ x: false }, { s: 'ETHUSDT' }, { i: '1m' }, { T: close }, { t: close - HOUR - 1 },
      { h: '90' }, { l: '110' }, { o: '' }, { c: '0x66' }, { c: 'Infinity' }]) {
      expect(parseBtcSpotClosedHour({ ...event(), k: { ...event().k, ...fields } }, Date.now())).toBeUndefined();
    }
    expect(parseBtcSpotClosedHour(event(), close - 1)).toBeUndefined();
    expect(parseBtcSpotClosedHour(event(close - HOUR), Date.now())).toBeUndefined();
    expect(parseBtcSpotClosedHour(event(), close + 300001)).toBeUndefined();
  });

  test('compares the same hour once and records latency of both routes', () => {
    shadow.start();
    shadow.start();
    sockets[0].emit('open');
    send();
    send();
    expect(sockets).toHaveLength(1);
    expect(preview).toHaveBeenCalledTimes(1);
    expect(shadow.status().recent[0]).toMatchObject({ shadowDecision: 'GREEN', streamAfterCloseMs: 1000 });
    jest.setSystemTime(close + 61_000);
    shadow.observeRest(close, observation());
    shadow.observeRest(close, observation());
    expect(shadow.status().recent[0]).toMatchObject({ restDecisionAfterCloseMs: 61_000,
      comparison: { result: 'MATCH', leadMs: 60_000, fields: [] } });
  });

  test('flags different inputs even when both routes say Green', () => {
    shadow.start();
    const rest = observation();
    rest.gaps = [[90, 117], null];
    shadow.observeRest(close, rest);
    send();
    expect(shadow.status().recent[0]).toMatchObject({ comparison: { result: 'MISMATCH', fields: ['gaps'] } });
  });

  test('does not count a missing warm base or calculator failure as no signal', () => {
    preview.mockReturnValueOnce(undefined).mockImplementationOnce(() => { throw new Error('preview failed'); });
    shadow.start();
    send();
    shadow.observeRest(close, observation());
    expect(shadow.status().recent[0]).toMatchObject({ unavailable: expect.stringContaining('warmed REST') });
    expect(shadow.status().recent[0].comparison).toBeUndefined();
    jest.setSystemTime(close + HOUR + 1000);
    send(event(close + HOUR));
    expect(shadow.status().recent[0]).toMatchObject({ unavailable: 'preview failed' });
    expect(shadow.status().recent[0].shadowDecision).toBeUndefined();
  });

  test('reconnects without accepting late messages from the old socket and stops cleanly', async () => {
    shadow.start();
    sockets[0].emit('close');
    sockets[0].emit('message', Buffer.from(JSON.stringify(event())));
    expect(preview).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(5000);
    expect(sockets).toHaveLength(2);
    send();
    expect(preview).toHaveBeenCalledTimes(1);
    shadow.stop();
    expect(sockets[1].terminate).toHaveBeenCalledTimes(1);
    sockets[1].emit('close');
    await jest.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(2);
  });
});
