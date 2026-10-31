import fs from 'fs';
import os from 'os';
import path from 'path';
import { IntrusionSpotPocFeed, validatePocHour, btcIntrusionSpotPocFeed, ethIntrusionSpotPocFeed, solIntrusionSpotPocFeed } from '../src/services/intrusionSpotPoc';
import { binanceGet, binanceRetryAt } from '../src/services/binanceHttp';
import { btcSpotChartRouter } from '../src/controllers/btcSpotChart';
const browser = require('../public/btc-spot-chart/spot-poc.js');
const { IntrusionDailyBreakoutPrimitive } = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
jest.mock('../src/services/binanceHttp', () => ({ binanceGet: jest.fn(), binanceRetryAt: jest.fn(), binanceCooldownHttpResponse: jest.fn() }));
const hour = Date.parse('2026-10-01T12:00:00Z') / 1000;
const candle = (volume = '10', count = 4) => [hour * 1000, '100', '110', '90', '101', volume, (hour + 3600) * 1000 - 1, '1000', count];
const trade = (id: number, price: string, quantity: string, first = id, last = first, time = hour * 1000) =>
  ({ a: id, p: price, q: quantity, f: first, l: last, T: time });
const trades = () => [trade(10, '100', '2', 10, 11), trade(11, '101', '4', 12), trade(12, '100', '4', 13)];
let clock = (hour + 7200) * 1000;

describe('annotation-only exact-price Binance Spot POC', () => {
  let directory: string, file: string;
  let backgroundFeeds: IntrusionSpotPocFeed[];
  beforeEach(() => {
    jest.useFakeTimers(); clock += 1_000_000; jest.setSystemTime(clock);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'spot-poc-')); file = path.join(directory, 'history.json');
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle()] }).mockResolvedValue({ data: trades() });
    (binanceRetryAt as jest.Mock).mockReset().mockReturnValue(0);
    backgroundFeeds = [];
  });
  afterEach(() => { for (const feed of backgroundFeeds) feed.dispose(); jest.restoreAllMocks(); jest.useRealTimers(); fs.rmSync(directory, { recursive: true, force: true }); });
  const advance = () => jest.setSystemTime(Date.now() + 5001);
  const complete = async (feed: IntrusionSpotPocFeed) => {
    expect((await feed.snapshot(hour)).record.status).toBe('pending'); advance(); return feed.snapshot(hour);
  };

  test('sums base quantity by exact price over the complete signal hour, never Futures or a candle midpoint', async () => {
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    const result = await complete(feed);
    expect(result).toMatchObject({ symbol: 'BTCUSDT', venue: 'Binance Spot', method: 'exact-price-base-volume', record: {
      hour, status: 'ok', poc: { price: 100, priceVolumeBase: '6.00000000', volumeBase: '10.00000000', tradeCount: 4, aggregateTrades: 3, pages: 1 }
    } });
    expect(binanceGet).toHaveBeenNthCalledWith(1, 'https://api.binance.com/api/v3/klines', expect.objectContaining({
      params: { symbol: 'BTCUSDT', interval: '1h', startTime: hour * 1000, endTime: (hour + 3600) * 1000 - 1, limit: 1 } }));
    expect(binanceGet).toHaveBeenNthCalledWith(2, 'https://api.binance.com/api/v3/aggTrades', expect.objectContaining({
      params: { symbol: 'BTCUSDT', startTime: hour * 1000, endTime: (hour + 3600) * 1000 - 1, limit: 1000 } }));
    browser.readPocResponse(result, 'BTCUSDT', hour, Date.now());
  });

  test('paginates inclusively by next aggregate ID, never publishes partial profiles, and excludes next-hour trades', async () => {
    const rows = Array.from({ length: 1000 }, (_, i) => trade(i + 10, '100', '0.001'));
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle('3', 1002)] })
      .mockResolvedValueOnce({ data: rows }).mockResolvedValueOnce({ data: [trade(1010, '101', '1'), trade(1011, '101', '1'),
        trade(1012, '105', '999', 1012, 1012, (hour + 3600) * 1000)] });
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    const partial = await complete(feed);
    expect(partial.record.status).toBe('pending'); expect(partial.record).not.toHaveProperty('poc'); expect(fs.existsSync(file)).toBe(false);
    await feed.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(2);
    advance(); const full = await feed.snapshot(hour);
    expect(full.record).toMatchObject({ status: 'ok', poc: { price: 101, priceVolumeBase: '2.00000000', tradeCount: 1002, pages: 2 } });
    expect(binanceGet).toHaveBeenLastCalledWith(expect.stringContaining('/aggTrades'), expect.objectContaining({ params: { symbol: 'BTCUSDT', fromId: 1010, limit: 1000 } }));
  });

  test('uses exact fractional sums and resolves equal peak volumes deterministically to the lower price', async () => {
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle('0.6', 3)] })
      .mockResolvedValue({ data: [trade(1, '101', '0.3'), trade(2, '100', '0.1'), trade(3, '100', '0.2')] });
    expect((await complete(new IntrusionSpotPocFeed('BTCUSDT', () => file))).record).toMatchObject({ status: 'ok', poc: { price: 100, priceVolumeBase: '0.30000000' } });
  });

  test.each(['timeout of 8000ms exceeded', 'HTTP 503: Service unavailable'])('retains verified pages across %s and resumes the same next ID', async message => {
    const rows = Array.from({ length: 1000 }, (_, i) => trade(i + 10, '100', '0.001'));
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle('3', 1001)] })
      .mockResolvedValueOnce({ data: rows }).mockRejectedValueOnce(new Error(`Binance api.binance.com: ${message}`))
      .mockResolvedValueOnce({ data: [trade(1010, '101', '2')] });
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    expect((await complete(feed)).record).toMatchObject({ status: 'pending', pages: 1 }); advance();
    const interrupted: any = (await feed.snapshot(hour)).record;
    expect(interrupted).toMatchObject({ status: 'pending', pages: 1 }); expect(interrupted).not.toHaveProperty('poc');
    expect(Date.parse(interrupted.retryAt)).toBe(Date.now() + 15000);
    await feed.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(3); expect(fs.existsSync(file)).toBe(false);
    jest.setSystemTime(Date.parse(interrupted.retryAt) + 1);
    expect((await feed.snapshot(hour)).record).toMatchObject({ status: 'ok', poc: { price: 101, volumeBase: '3.00000000', tradeCount: 1001, pages: 2 } });
    expect(binanceGet).toHaveBeenCalledTimes(4);
    for (const call of (binanceGet as jest.Mock).mock.calls.slice(2)) expect(call[1].params).toEqual({ symbol: 'BTCUSDT', fromId: 1010, limit: 1000 });
  });

  test('a long shared cooldown does not expire the validated profile before its scheduled retry', async () => {
    const rows = Array.from({ length: 1000 }, (_, i) => trade(i + 10, '100', '0.001'));
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle('3', 1001)] })
      .mockResolvedValueOnce({ data: rows }).mockImplementationOnce(() => {
        (binanceRetryAt as jest.Mock).mockReturnValue(Date.now() + 600000); throw new Error('Binance cooldown');
      }).mockResolvedValueOnce({ data: [trade(1010, '101', '2')] });
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file); await complete(feed); advance();
    const retry: any = (await feed.snapshot(hour)).record;
    expect(retry.status).toBe('pending'); expect(retry.pages).toBe(1);
    jest.setSystemTime(Date.parse(retry.retryAt) + 1);
    expect((await feed.snapshot(hour)).record).toMatchObject({ status: 'ok', poc: { pages: 2, price: 101 } });
    expect(binanceGet).toHaveBeenCalledTimes(4);
  });

  test('non-retryable HTTP errors remain unavailable rather than looping without progress', async () => {
    (binanceGet as jest.Mock).mockReset().mockRejectedValue(new Error('Binance api.binance.com HTTP 400 code -1121: Invalid symbol'));
    const result = await new IntrusionSpotPocFeed('BTCUSDT', () => file).snapshot(hour);
    expect(result.record.status).toBe('unavailable'); expect(result.record).not.toHaveProperty('poc');
  });

  test('continues a started profile server-side without another browser request', async () => {
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file, { background: true }); backgroundFeeds.push(feed);
    expect((await feed.snapshot(hour)).record.status).toBe('pending');
    await jest.advanceTimersByTimeAsync(5001);
    expect(binanceGet).toHaveBeenCalledTimes(2);
    expect((await feed.snapshot(hour)).record).toMatchObject({ status: 'ok', poc: { price: 100 } });
    expect(binanceGet).toHaveBeenCalledTimes(2); expect(fs.existsSync(`${file}.partial`)).toBe(false);
  });

  test('background work shares the same five-second budget fairly across BTC and ETH', async () => {
    (binanceGet as jest.Mock).mockReset().mockImplementation((url: string) => Promise.resolve({ data: url.endsWith('klines') ? [candle()] : trades() }));
    const btc = new IntrusionSpotPocFeed('BTCUSDT', () => file, { background: true });
    const eth = new IntrusionSpotPocFeed('ETHUSDT', () => path.join(directory, 'eth.json'), { background: true });
    backgroundFeeds.push(btc, eth);
    await btc.snapshot(hour); await eth.snapshot(hour);
    await jest.advanceTimersByTimeAsync(15010);
    expect((await btc.snapshot(hour)).record.status).toBe('ok');
    expect((await eth.snapshot(hour)).record.status).toBe('ok');
    expect((binanceGet as jest.Mock).mock.calls.map(call => call[1].params.symbol)).toEqual(['BTCUSDT', 'ETHUSDT', 'BTCUSDT', 'ETHUSDT']);
    expect(binanceGet).toHaveBeenCalledTimes(4);
  });

  test('a new ETH job wakes the background scheduler before an older BTC retry becomes due', async () => {
    (binanceGet as jest.Mock).mockReset().mockRejectedValueOnce(new Error('temporary timeout'))
      .mockImplementation((url: string) => Promise.resolve({ data: url.endsWith('klines') ? [candle()] : trades() }));
    const btc = new IntrusionSpotPocFeed('BTCUSDT', () => file, { background: true });
    const eth = new IntrusionSpotPocFeed('ETHUSDT', () => path.join(directory, 'eth.json'), { background: true });
    backgroundFeeds.push(btc, eth);
    await btc.snapshot(hour); await jest.advanceTimersByTimeAsync(5001);
    await eth.snapshot(hour); await jest.advanceTimersByTimeAsync(5001);
    expect((await eth.snapshot(hour)).record.status).toBe('ok');
    expect(binanceGet).toHaveBeenCalledTimes(3);
  });

  test('restores a checkpoint after restart, revalidates its candle and resumes at the next ID', async () => {
    const rows = Array.from({ length: 1000 }, (_, i) => trade(i + 10, '100', '0.001'));
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle('3', 1001)] })
      .mockResolvedValueOnce({ data: rows }).mockResolvedValueOnce({ data: [candle('3', 1001)] })
      .mockResolvedValueOnce({ data: [trade(1010, '101', '2')] });
    const first = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    await complete(first); first.dispose();
    expect(JSON.parse(fs.readFileSync(`${file}.partial`, 'utf8')).pages).toBe(1);
    advance(); const restarted = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    expect((await restarted.snapshot(hour)).record).toMatchObject({ status: 'pending', pages: 1 });
    expect((binanceGet as jest.Mock).mock.calls[2][0]).toContain('/klines');
    advance(); expect((await restarted.snapshot(hour)).record).toMatchObject({ status: 'ok', poc: { price: 101, pages: 2 } });
    expect((binanceGet as jest.Mock).mock.calls[3][1].params.fromId).toBe(1010);
    expect(fs.existsSync(`${file}.partial`)).toBe(false);
  });

  test('corrupt checkpoints are preserved and never treated as a completed or trusted profile', async () => {
    const partial = `${file}.partial`; fs.writeFileSync(partial, '{broken');
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    const result = await complete(feed);
    expect(result.checkpointAvailable).toBe(false); expect(result.record.status).toBe('ok');
    expect(fs.readFileSync(partial, 'utf8')).toBe('{broken');
  });

  test('a restored checkpoint cannot publish a POC if its independent candle totals have changed', async () => {
    const first = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    await first.snapshot(hour); first.dispose(); advance();
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: [candle('11')] });
    const result = await new IntrusionSpotPocFeed('BTCUSDT', () => file).snapshot(hour);
    expect(result.record).toMatchObject({ status: 'unavailable', error: expect.stringContaining('differs') });
    expect(result.record).not.toHaveProperty('poc'); expect(fs.existsSync(file)).toBe(false);
  });

  test('the independently completed September BTC backfill is immediately usable without new Binance requests', async () => {
    const seedFile = () => path.join(__dirname, '..', 'public', 'btc-spot-chart', 'history', 'btc-spot-poc.json');
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file, { seedFile });
    const result = await feed.snapshot(1789977600);
    expect(result.record).toMatchObject({ status: 'ok', poc: { price: 83000, volumeBase: '4553.74746000', tradeCount: 373451, aggregateTrades: 151538, pages: 152 } });
    expect(binanceGet).not.toHaveBeenCalled(); browser.readPocResponse(result, 'BTCUSDT', 1789977600, Date.now());
  });

  test.each(['gap', 'duplicate', 'count', 'volume', 'time', 'price', 'precision', 'empty'])('rejects %s corruption without publishing any POC', async kind => {
    const rows = trades();
    if (kind === 'gap') rows[1].a++;
    if (kind === 'duplicate') rows[1].a--;
    if (kind === 'count') rows[0].l--;
    if (kind === 'volume') rows[0].q = '1';
    if (kind === 'time') rows[1].T--;
    if (kind === 'price') rows[1].p = '111';
    if (kind === 'precision') rows[1].q = '4.000000001';
    if (kind === 'empty') rows.splice(0);
    (binanceGet as jest.Mock).mockReset().mockResolvedValueOnce({ data: [candle()] }).mockResolvedValue({ data: rows });
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file), result = await complete(feed);
    expect(result.record.status).toBe('unavailable'); expect(result.record).not.toHaveProperty('poc'); expect(fs.existsSync(file)).toBe(false);
    await feed.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(2);
  });

  test.each(['ETHUSDT', 'SOLUSDT'] as const)('%s results persist separately from BTC and reload without requests', async symbol => {
    const btc = new IntrusionSpotPocFeed('BTCUSDT', () => file); const first: any = await complete(btc);
    first.record.poc.price = 999;
    expect((await new IntrusionSpotPocFeed('BTCUSDT', () => file).snapshot(hour)).record).toMatchObject({ status: 'ok', poc: { price: 100 } });
    expect(binanceGet).toHaveBeenCalledTimes(2);
    advance();
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: [candle()] });
    const ethFile = path.join(directory, 'eth.json');
    expect((await complete(new IntrusionSpotPocFeed(symbol, () => ethFile))).symbol).toBe(symbol);
    expect(binanceGet).toHaveBeenLastCalledWith(expect.any(String), expect.objectContaining({ params: expect.objectContaining({ symbol }) }));
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).symbol).toBe('BTCUSDT');
    expect(JSON.parse(fs.readFileSync(ethFile, 'utf8')).symbol).toBe(symbol);
    const before = (binanceGet as jest.Mock).mock.calls.length;
    const restarted = await new IntrusionSpotPocFeed(symbol, () => ethFile).snapshot(hour);
    expect(restarted.record).toMatchObject({ status: 'ok', poc: { price: 100 } });
    expect(binanceGet).toHaveBeenCalledTimes(before);
  });

  test('admits one page globally and alternates waiting BTC/ETH profiles without starvation', async () => {
    let finish: any;
    (binanceGet as jest.Mock).mockReset().mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
    const btc = new IntrusionSpotPocFeed('BTCUSDT', () => file), eth = new IntrusionSpotPocFeed('ETHUSDT', () => path.join(directory, 'eth.json'));
    const first = btc.snapshot(hour);
    expect((await btc.snapshot(hour)).record.status).toBe('pending');
    expect((await eth.snapshot(hour)).record.status).toBe('pending'); expect(binanceGet).toHaveBeenCalledTimes(1);
    finish({ data: [candle()] }); await first; advance();
    await btc.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(1);
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: [candle()] }).mockResolvedValue({ data: trades() });
    await eth.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(2);
    advance(); await eth.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(2);
    expect((await btc.snapshot(hour)).record.status).toBe('ok'); expect(binanceGet).toHaveBeenCalledTimes(3);
    advance(); expect((await eth.snapshot(hour)).record.status).toBe('ok'); expect(binanceGet).toHaveBeenCalledTimes(4);
  });

  test('expires abandoned profiles so another hour of the same chart can load', async () => {
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    await feed.snapshot(hour); advance(); await feed.snapshot(hour - 3600);
    expect(binanceGet).toHaveBeenCalledTimes(1);
    jest.setSystemTime(Date.now() + 121000);
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: [candle()] });
    await feed.snapshot(hour); expect(binanceGet).toHaveBeenCalledTimes(2);
  });

  test('honors shared cooldown before enqueuing any POC work', async () => {
    (binanceRetryAt as jest.Mock).mockReturnValue(Date.now() + 600_000);
    const result = await new IntrusionSpotPocFeed('BTCUSDT', () => file).snapshot(hour);
    expect(result.record).toMatchObject({ status: 'pending', retryAt: new Date(Date.now() + 600_000).toISOString() });
    expect(binanceGet).not.toHaveBeenCalled();
  });

  test('preserves corrupted cache files rather than overwriting evidence', async () => {
    fs.writeFileSync(file, '{broken'); jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    const result = await complete(new IntrusionSpotPocFeed('ETHUSDT', () => file));
    expect(result.persistenceAvailable).toBe(false); expect(result.record.status).toBe('ok');
    expect(fs.readFileSync(file, 'utf8')).toBe('{broken');
  });

  test('refuses a cache from the other symbol and preserves it byte-for-byte', async () => {
    await complete(new IntrusionSpotPocFeed('BTCUSDT', () => file));
    const before = fs.readFileSync(file, 'utf8'); advance();
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    (binanceGet as jest.Mock).mockResolvedValueOnce({ data: [candle()] });
    const result = await complete(new IntrusionSpotPocFeed('ETHUSDT', () => file));
    expect(result.persistenceAvailable).toBe(false); expect(result.record.status).toBe('ok');
    expect(fs.readFileSync(file, 'utf8')).toBe(before);
  });

  test('missing or misaligned candles cannot establish an expected volume', async () => {
    (binanceGet as jest.Mock).mockReset().mockResolvedValue({ data: [] });
    const feed = new IntrusionSpotPocFeed('BTCUSDT', () => file);
    const result = await feed.snapshot(hour);
    expect(result.record.status).toBe('unavailable'); expect(result.record).not.toHaveProperty('poc');
    expect(binanceGet).toHaveBeenCalledTimes(1); expect(fs.existsSync(file)).toBe(false);
  });

  test('constructors do no I/O and invalid/open/multiple hours cannot enter a feed or endpoint', async () => {
    const locate = jest.fn(() => file); new IntrusionSpotPocFeed('BTCUSDT', locate);
    expect(locate).not.toHaveBeenCalled(); expect(() => new IntrusionSpotPocFeed('XRPUSDT' as any)).toThrow();
    for (const bad of [NaN, -3600, hour + 1, Math.floor(Date.now() / 3_600_000) * 3600]) expect(() => validatePocHour(bad)).toThrow();
    for (const [asset, feed] of [['btc', btcIntrusionSpotPocFeed], ['eth', ethIntrusionSpotPocFeed], ['sol', solIntrusionSpotPocFeed]] as const) {
      const read = jest.spyOn(feed, 'snapshot').mockResolvedValue({ record: { hour, status: 'pending' } } as any);
      const layer = (btcSpotChartRouter as any).stack.find((l: any) => l.route?.path === `/open-liquidity/v2/${asset}-intrusion-spot-poc`);
      for (const query of [undefined, ['1', '2'], 'bad', `${hour},${hour}`, String(hour + 1)]) {
        const res: any = { setHeader: jest.fn(), status: jest.fn().mockReturnThis(), send: jest.fn() };
        await layer.route.stack[0].handle({ query: { hour: query } }, res); expect(res.status).toHaveBeenCalledWith(400);
      }
      expect(read).not.toHaveBeenCalled();
      await layer.route.stack[0].handle({ query: { hour: String(hour) } }, { setHeader: jest.fn(), send: jest.fn() });
      expect(read).toHaveBeenCalledWith(hour);
    }
  });

  test('browser rejects mismatched markets, Futures venues, midpoint methods and malformed data', async () => {
    const result = await complete(new IntrusionSpotPocFeed('BTCUSDT', () => file));
    for (const bad of [{ symbol: 'ETHUSDT' }, { venue: 'Binance USD-M Futures' }, { method: 'body-midpoint' }, { record: { hour, status: 'ok', poc: { price: 0 } } }]) {
      expect(() => browser.readPocResponse({ ...result, ...bad }, 'BTCUSDT', hour, Date.now())).toThrow();
    }
  });

  test('starts historical POC loading even when no purple candle is in the current viewport', () => {
    const signals = [{ time: hour }, { time: hour - 3600 }];
    const range = { from: hour + 86400, to: hour + 172800 };
    expect(browser.nextPocHour(signals, new Map(), undefined, range)).toBe(hour);
    expect(browser.nextPocHour(signals, new Map(), undefined, null)).toBe(hour);
  });

  test('prioritizes visible candles but finishes a partial profile across panning and retries', () => {
    const signals = [{ time: hour }, { time: hour - 3600 }], range = { from: hour - 3600, to: hour - 3600 };
    expect(browser.nextPocHour(signals, new Map(), undefined, range)).toBe(hour - 3600);
    const records = new Map([[hour, { status: 'pending', retryAt: new Date(Date.now() - 1).toISOString() }]]);
    expect(browser.nextPocHour(signals, records, hour, range)).toBe(hour);
    records.get(hour)!.retryAt = new Date(Date.now() + 5000).toISOString();
    expect(browser.nextPocHour(signals, records, hour, range)).toBeUndefined();
    expect(browser.nextPocHour(signals, records, hour, range, Date.now() + 5001)).toBe(hour);
  });

  test('skips completed candles and backed-off failures without blocking the rest of history', () => {
    const signals = [{ time: hour }, { time: hour - 3600 }];
    const records = new Map([[hour, { status: 'unavailable', retryAt: new Date(Date.now() + 300000).toISOString() }]]);
    expect(browser.nextPocHour(signals, records, undefined, null)).toBe(hour - 3600);
    records.set(hour - 3600, { status: 'ok', retryAt: '' });
    expect(browser.nextPocHour(signals, records, undefined, null)).toBeUndefined();
  });

  test('draws a 16px crossbar at POC, independent of FD, skips partial/mismatched records, and obeys the purple checkbox', () => {
    const layer = new IntrusionDailyBreakoutPrimitive(), ctx: any = {};
    for (const key of ['save', 'restore', 'beginPath', 'rect', 'clip', 'setLineDash', 'moveTo', 'lineTo', 'stroke']) ctx[key] = jest.fn();
    layer.attached({ chart: { timeScale: () => ({ timeToCoordinate: () => 100 }) }, series: { priceToCoordinate: (price: number) => 200 - price }, requestUpdate: jest.fn() });
    const signal = { time: hour, closedAt: hour + 3600, direction: 'short' }; layer.setData([signal]); layer.setPocEnabled(true);
    const draw = () => layer.draw({ useMediaCoordinateSpace: (fn: any) => fn({ context: ctx, mediaSize: { width: 300, height: 200 } }) });
    for (const record of [{ hour, status: 'pending' }, { hour: hour - 3600, status: 'ok', poc: { hour, price: 101 } }]) {
      layer.setPocRecords(new Map([[hour, record]])); draw(); expect(layer.getPocCrosses()).toEqual([]);
    }
    layer.setPocRecords(new Map([[hour, { hour, status: 'ok', poc: { hour, price: 101 } }]])); draw();
    expect(layer.getPocCrosses()).toEqual([{ time: hour, direction: 'short', price: 101, x: 100.5, y: 99.5, width: 16 }]);
    expect(ctx.moveTo).toHaveBeenLastCalledWith(92.5, 99.5); expect(ctx.lineTo).toHaveBeenLastCalledWith(108.5, 99.5);
    const copy = layer.getPocCrosses(); copy[0].price = 500; expect(layer.getPocCrosses()[0].price).toBe(101);
    expect(layer.signals).toEqual([signal]); layer.setVisible(false); draw(); expect(layer.getPocCrosses()).toEqual([]);
  });
});
