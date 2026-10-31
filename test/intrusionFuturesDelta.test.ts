import fs from 'fs';
import os from 'os';
import path from 'path';
import { IntrusionFuturesDeltaFeed, parseFuturesDeltaCandle, validateDeltaHours, btcIntrusionFuturesDeltaFeed, ethIntrusionFuturesDeltaFeed } from '../src/services/intrusionFuturesDelta';
import { binanceGet, binanceRetryAt } from '../src/services/binanceHttp';
import { btcSpotChartRouter } from '../src/controllers/btcSpotChart';
const browser = require('../public/btc-spot-chart/futures-delta.js');
const { IntrusionDailyBreakoutPrimitive } = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
jest.mock('../src/services/binanceHttp', () => ({ binanceGet: jest.fn(), binanceRetryAt: jest.fn(), binanceCooldownHttpResponse: jest.fn() }));
const hour = Date.parse('2026-10-01T12:00:00Z') / 1000;
const BtcIntrusionFuturesDeltaFeed = class extends IntrusionFuturesDeltaFeed {
  constructor(file: () => string) { super('BTCUSDT', file); }
};
const candle = (h = hour, buy = 35, volume = 100) => [(h + 3300) * 1000, '100', '102', '99', '101', String(volume), (h + 3600) * 1000 - 1, '10000', 50, String(buy), '3500', '0'];

describe('BTC annotation-only Futures delta feed', () => {
  let directory: string, file: string;
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime((hour + 7200) * 1000);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-f5m-')); file = path.join(directory, 'history.json');
    (binanceGet as jest.Mock).mockReset().mockResolvedValue({ data: [candle()] });
    (binanceRetryAt as jest.Mock).mockReset().mockReturnValue(0);
  });
  afterEach(() => {
    jest.restoreAllMocks(); jest.useRealTimers();
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('reads exactly xx:55-xx:59:59.999 of the signal hour from USD-M Futures, never Spot or the next candle', async () => {
    const result = await new BtcIntrusionFuturesDeltaFeed(() => file).snapshot([hour]);
    expect(binanceGet).toHaveBeenCalledWith('https://fapi.binance.com/fapi/v1/klines', expect.objectContaining({
      params: { symbol: 'BTCUSDT', interval: '5m', startTime: (hour + 3300) * 1000, endTime: (hour + 3600) * 1000 - 1, limit: 1 }
    }));
    expect(result).toMatchObject({ symbol: 'BTCUSDT', venue: 'Binance USD-M Futures', timeBasis: 'signal-last-five-minutes',
      records: [{ hour, status: 'ok', candle: { rawDeltaPct: -30.000000000000004 } }] });
  });

  test('rejects open hours, incorrect windows, zero/missing/impossible volume and oversized batches', () => {
    for (const hours of [[hour + 7200], [hour + 1], [], Array(9).fill(hour), [-3600]]) expect(() => validateDeltaHours(hours)).toThrow();
    const invalid = [candle(hour + 3600), candle(hour, 0, 0), candle(hour, 101), candle(hour, -1), candle(hour, NaN)];
    const wrongEnd = candle(); wrongEnd[6] = (hour + 3600) * 1000; invalid.push(wrongEnd);
    const empty = candle(); empty[9] = ''; invalid.push(empty);
    for (const raw of invalid) expect(() => parseFuturesDeltaCandle(raw, hour)).toThrow();
    expect(parseFuturesDeltaCandle(candle(hour, 50), hour).rawDeltaPct).toBe(0);
  });

  test('deduplicates concurrent reads, persists closed historical bars and survives a feed restart without refetching', async () => {
    const feed = new BtcIntrusionFuturesDeltaFeed(() => file);
    const [a, b] = await Promise.all([feed.snapshot([hour, hour]), feed.snapshot([hour])]);
    expect(a.records).toHaveLength(1); expect(a.records).toEqual(b.records); expect(binanceGet).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(file)).toBe(true);
    const copy: any = a.records[0]; copy.candle.rawDeltaPct = 99;
    const restarted = await new BtcIntrusionFuturesDeltaFeed(() => file).snapshot([hour]);
    expect((restarted.records[0] as any).candle.rawDeltaPct).toBeCloseTo(-30);
    expect(binanceGet).toHaveBeenCalledTimes(1);
  });

  test('admits only one new annotation read into the shared queue; unrelated hours return pending', async () => {
    let finish: any;
    (binanceGet as jest.Mock).mockReturnValue(new Promise(resolve => { finish = resolve; }));
    const feed = new BtcIntrusionFuturesDeltaFeed(() => file), first = feed.snapshot([hour]);
    const second = await feed.snapshot([hour - 3600]);
    expect(second.records[0]).toMatchObject({ status: 'pending' }); expect(binanceGet).toHaveBeenCalledTimes(1);
    finish({ data: [candle()] }); await first;
  });

  test('backs off on failures/cooldowns, never records failure as zero, and can recover later', async () => {
    const retry = Date.now() + 600_000;
    (binanceGet as jest.Mock).mockRejectedValueOnce(new Error('cooldown'));
    (binanceRetryAt as jest.Mock).mockReturnValue(retry);
    const feed = new BtcIntrusionFuturesDeltaFeed(() => file), failed = await feed.snapshot([hour]);
    expect(failed.records[0]).toMatchObject({ status: 'unavailable', retryAt: new Date(retry).toISOString() });
    expect(failed.records[0]).not.toHaveProperty('candle'); expect(fs.existsSync(file)).toBe(false);
    await feed.snapshot([hour]); expect(binanceGet).toHaveBeenCalledTimes(1);
    jest.setSystemTime(retry + 1);
    expect((await feed.snapshot([hour])).records[0].status).toBe('ok');
  });

  test('retries a just-closed but not-yet-published candle without claiming a value', async () => {
    jest.setSystemTime((hour + 3600) * 1000 + 1000);
    (binanceGet as jest.Mock).mockResolvedValue({ data: [] });
    const result = await new BtcIntrusionFuturesDeltaFeed(() => file).snapshot([hour]);
    expect(result.records[0]).toMatchObject({ status: 'unavailable', retryAt: new Date(Date.now() + 15000).toISOString() });
  });

  test('preserves a corrupted cache file and uses memory without overwriting the damaged history', async () => {
    fs.writeFileSync(file, '{broken');
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const feed = new BtcIntrusionFuturesDeltaFeed(() => file), result = await feed.snapshot([hour]);
    expect(result.records[0].status).toBe('ok'); expect(result.persistenceAvailable).toBe(false);
    expect(fs.readFileSync(file, 'utf8')).toBe('{broken');
    await feed.snapshot([hour]); expect(binanceGet).toHaveBeenCalledTimes(1);
  });

  test('does not load a file or start a background read when constructed', () => {
    const locate = jest.fn(() => file);
    new BtcIntrusionFuturesDeltaFeed(locate);
    expect(locate).not.toHaveBeenCalled(); expect(binanceGet).not.toHaveBeenCalled();
  });

  test.each(['ETHUSDT', 'SOLUSDT'] as const)('%s uses its own Futures source and persistent cache', async symbol => {
    const ethFile = path.join(directory, 'eth.json');
    const btc = new IntrusionFuturesDeltaFeed('BTCUSDT', () => file), eth = new IntrusionFuturesDeltaFeed(symbol, () => ethFile);
    await btc.snapshot([hour]);
    (binanceGet as jest.Mock).mockResolvedValue({ data: [candle(hour, 60)] });
    const result = await eth.snapshot([hour]);
    expect(result.symbol).toBe(symbol); expect((result.records[0] as any).candle.rawDeltaPct).toBeCloseTo(20);
    expect(binanceGet).toHaveBeenLastCalledWith(expect.any(String), expect.objectContaining({ params: expect.objectContaining({ symbol }) }));
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).symbol).toBe('BTCUSDT');
    expect(JSON.parse(fs.readFileSync(ethFile, 'utf8')).symbol).toBe(symbol);
    expect((await btc.snapshot([hour])).records[0]).toMatchObject({ candle: { rawDeltaPct: -30.000000000000004 } });
    expect(() => new IntrusionFuturesDeltaFeed('XRPUSDT' as any)).toThrow('symbol');
  });

  test('refuses a mixed-symbol disk cache and never overwrites that evidence with another market', async () => {
    await new IntrusionFuturesDeltaFeed('BTCUSDT', () => file).snapshot([hour]);
    const before = fs.readFileSync(file, 'utf8');
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    (binanceGet as jest.Mock).mockResolvedValue({ data: [candle(hour, 65)] });
    const result = await new IntrusionFuturesDeltaFeed('ETHUSDT', () => file).snapshot([hour]);
    expect(result.persistenceAvailable).toBe(false); expect((result.records[0] as any).candle.rawDeltaPct).toBeCloseTo(30);
    expect(fs.readFileSync(file, 'utf8')).toBe(before);
  });

  test('route rejects invalid requests before any feed read, and returns only annotation data', async () => {
    const layer: any = (btcSpotChartRouter as any).stack.find((l: any) => l.route?.path === '/open-liquidity/v2/btc-intrusion-futures-delta');
    const snapshot = jest.spyOn(btcIntrusionFuturesDeltaFeed, 'snapshot').mockResolvedValue({ records: [] } as any);
    const invoke = async (hours: any) => {
      const res: any = { setHeader: jest.fn(), status: jest.fn().mockReturnThis(), send: jest.fn().mockReturnThis() };
      await layer.route.stack[0].handle({ query: { hours } }, res); return res;
    };
    for (const input of [undefined, ['1', '2'], 'bad', String(hour + 7200), Array(9).fill(hour).join(',')]) expect((await invoke(input)).status).toHaveBeenCalledWith(400);
    expect(snapshot).not.toHaveBeenCalled();
    const valid = await invoke(String(hour)); expect(snapshot).toHaveBeenCalledWith([hour]); expect(valid.send).toHaveBeenCalledWith({ records: [] });
    const ethSnapshot = jest.spyOn(ethIntrusionFuturesDeltaFeed, 'snapshot').mockResolvedValue({ symbol: 'ETHUSDT', records: [] } as any);
    const ethLayer: any = (btcSpotChartRouter as any).stack.find((l: any) => l.route?.path === '/open-liquidity/v2/eth-intrusion-futures-delta');
    const res: any = { setHeader: jest.fn(), send: jest.fn() };
    await ethLayer.route.stack[0].handle({ query: { hours: String(hour) } }, res);
    expect(ethSnapshot).toHaveBeenCalledWith([hour]); expect(snapshot).toHaveBeenCalledTimes(1);
  });
});

describe('F5m direction labels and chart rendering', () => {
  const signal = (direction = 'short', time = hour) => ({ time, closedAt: time + 3600, direction });
  const record = (time = hour, buy = 35) => ({ hour: time, status: 'ok', candle: parseFuturesDeltaCandle(candle(time, buy), time, (time + 7200) * 1000) });

  test('normalizes short selling, preserves negative counterflow and a real measured zero', () => {
    expect(browser.deltaLabel(signal(), record())).toMatchObject({ text: 'F5m +30.00%', status: 'ok' });
    expect(browser.deltaLabel(signal('long'), record()).text).toBe('F5m -30.00%');
    expect(browser.deltaLabel(signal('long'), record(hour, 50)).text).toBe('F5m +0.00%');
    expect(browser.deltaLabel(signal(), undefined)).toMatchObject({ text: 'F5m ...', directionalDeltaPct: null });
    expect(browser.deltaLabel(signal(), { hour, status: 'unavailable' }).text).toBe('F5m n/a');
    expect(browser.deltaLabel(signal(), record(hour - 3600)).directionalDeltaPct).toBeNull();
  });

  test('rejects mixed venues/symbols or inconsistent/raw-corrupt values, rather than borrowing Spot data', () => {
    const payload = { symbol: 'BTCUSDT', venue: 'Binance USD-M Futures', interval: '5m', timeBasis: 'signal-last-five-minutes', records: [record()] };
    expect(browser.readDeltaResponse(payload)).toHaveLength(1);
    expect(browser.readDeltaResponse({ ...payload, symbol: 'ETHUSDT' }, 'ETHUSDT')).toHaveLength(1);
    expect(() => browser.readDeltaResponse(payload, 'ETHUSDT')).toThrow();
    for (const bad of [{ symbol: 'ETHUSDT' }, { venue: 'Binance Spot' }, { interval: '1h' }, { timeBasis: 'mail-time' }]) expect(() => browser.readDeltaResponse({ ...payload, ...bad })).toThrow();
    expect(() => browser.readDeltaResponse({ ...payload, records: [{ ...record(), candle: { ...record().candle, rawDeltaPct: 10 } }] })).toThrow();
  });

  test('renders labels over the unchanged signal lines, avoids collisions and hides them with the same checkbox', () => {
    const layer = new IntrusionDailyBreakoutPrimitive(), texts: any[] = [], ctx: any = {};
    for (const key of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke', 'fillRect', 'strokeRect']) ctx[key] = jest.fn();
    ctx.measureText = (text: string) => ({ width: text.length * 5 }); ctx.fillText = (text: string, x: number, y: number) => texts.push({ text, x, y });
    layer.attached({ chart: { timeScale: () => ({ timeToCoordinate: (time: number) => 100 + (time - hour) / 3600 * 10 }) }, requestUpdate: jest.fn() });
    const signals = [signal(), signal('long', hour + 3600)];
    layer.setData(signals); layer.setDeltaEnabled(true);
    layer.setDeltaRecords(new Map([[hour, record()], [hour + 3600, record(hour + 3600, 65)]]));
    const draw = () => layer.draw({ useMediaCoordinateSpace: (fn: any) => fn({ context: ctx, mediaSize: { width: 300, height: 200 } }) });
    draw(); const labels = layer.getDeltaLabels();
    expect(labels).toHaveLength(2); expect(labels[1].y).toBeGreaterThanOrEqual(labels[0].y + labels[0].height + 3);
    expect(texts.map(t => t.text)).toEqual(['F5m +30.00%', 'F5m +30.00%']);
    expect(layer.signals).toBe(signals); labels[0].text = 'changed'; expect(layer.getDeltaLabels()[0].text).not.toBe('changed');
    layer.setVisible(false); draw(); expect(layer.getDeltaLabels()).toEqual([]);
  });
});
