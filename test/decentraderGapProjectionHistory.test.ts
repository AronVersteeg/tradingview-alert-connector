import zlib from 'zlib';
import { buildDecentraderGapProjectionHistory } from '../src/services/decentraderGapProjectionHistory';
import { createBtcGapProjectionHistoryRouter } from '../src/controllers/btcGapProjectionHistory';

const start = Date.parse('2026-09-20T00:00:00Z') / 1000;
const frame = (index: number, price = 100) => ({ i: index, t: new Date((start + index * 3600) * 1000).toISOString().slice(0, 19).replace('T', ' '), price });
const event = (i: number, p: number, a = 1, s = 'L', l = 10) => ({ i, p, a, s, l });
const payload = () => ({ source: { method: 'getOHLCHourlyCalculations', params: ['btcusdt'] },
  frames: [frame(0), frame(1), frame(2)],
  events: [event(0, 80), event(0, 120, 1, 'S'), event(1, 90), event(1, 110, 0), event(2, 110, 1, 'S')] });

describe('Stored Decentrader map gap reconstruction, display only', () => {
  test('uses only compact gap fields and the same nearest active bars as the default map', () => {
    const input = payload(), before = JSON.stringify(input);
    const history = buildDecentraderGapProjectionHistory(input);
    expect(history).toMatchObject({ symbol: 'BTCUSDT', venue: 'Decentrader map', historicalOnly: true,
      historicalAccuracy: 'current-active-reconstruction' });
    expect(history.records).toEqual([
      { candleTime: start, availableAt: start + 3600, lower: 80, upper: 120 },
      { candleTime: start + 3600, availableAt: start + 7200, lower: 90, upper: 120 },
      { candleTime: start + 7200, availableAt: start + 10800, lower: 90, upper: 110 }
    ]);
    expect(JSON.stringify(input)).toBe(before);
    expect(JSON.stringify(history)).not.toMatch(/events|priceSource|takeProfits|smtp|orders/);
  });

  test('matches a brute-force map replay with duplicates, equal-price bars and unsorted events', () => {
    const input = payload();
    input.frames = Array.from({ length: 300 }, (_, i) => frame(i, 80 + i % 50));
    input.events = input.frames.flatMap((_, i) => [event(i, 60 + i % 65, i % 3 ? 1 : 0), event(i, 100 + i % 60, 1, 'S', 5)]).reverse();
    const result = buildDecentraderGapProjectionHistory(input);
    for (let i = 0; i < input.frames.length; i++) {
      const bars = input.events.filter((e) => e.i <= i && e.a);
      const price = input.frames[i].price;
      const lower = Math.max(...bars.filter((e) => e.p < price).map((e) => e.p));
      const upper = Math.min(...bars.filter((e) => e.p > price).map((e) => e.p));
      const valid = Number.isFinite(lower) && Number.isFinite(upper);
      expect(result.records[i]).toMatchObject({ lower: valid ? lower : null, upper: valid ? upper : null });
    }
  });

  test('does not invent a missing edge, does not backfill future events and rejects corrupt/other-market data', () => {
    const input = payload(); input.events = [event(0, 100), event(1, 80), event(2, 120, 1, 'S')];
    const records = buildDecentraderGapProjectionHistory(input).records;
    expect(records[0].lower).toBeNull(); expect(records[1].lower).toBeNull();
    expect(records[2]).toMatchObject({ lower: 80, upper: 120 });
    input.source.params = ['ethusdt'];
    expect(() => buildDecentraderGapProjectionHistory(input)).toThrow('BTC');
    const corrupt = payload(); corrupt.frames[1].t = corrupt.frames[0].t;
    expect(() => buildDecentraderGapProjectionHistory(corrupt)).toThrow('timestamp');
    const bad = payload(); bad.events[0].p = NaN;
    expect(() => buildDecentraderGapProjectionHistory(bad)).toThrow('event');
  });
});

describe('Read-only compact gap-history route', () => {
  const call = async (read: any, query: any = {}, encoding = '') => {
    const router = createBtcGapProjectionHistoryRouter(read);
    const route = (router as any).stack[0].route;
    expect(route.path).toBe('/decentrader/gap-projection-history');
    expect(route.methods).toEqual({ get: true });
    const res: any = { headers: {}, statusCode: 200,
      setHeader(key: string, value: string) { this.headers[key] = value; },
      status(code: number) { this.statusCode = code; return this; },
      type(value: string) { this.contentType = value; return this; },
      send(body: any) { this.body = body; return this; },
      json(body: any) { this.body = body; return this; } };
    await route.stack[0].handle({ query, headers: { 'accept-encoding': encoding } }, res);
    return res;
  };
  test('returns only the stored snapshot and supports compressed reads', async () => {
    const history = { ...buildDecentraderGapProjectionHistory(payload()), historicalAccuracy: 'observed-snapshots' as const };
    const read = jest.fn(() => history);
    const plain = await call(read);
    expect(JSON.parse(plain.body)).toEqual(history);
    const compressed = await call(read, { market: 'BTC-USD' }, 'gzip, deflate');
    expect(JSON.parse(zlib.gunzipSync(compressed.body).toString())).toEqual(history);
    expect(compressed.headers).toMatchObject({ 'Content-Encoding': 'gzip', 'Cache-Control': 'no-store' });
    expect(read).toHaveBeenCalledTimes(2);
  });
  test('quarantines current-active reconstructions rather than serving them as historical truth', async () => {
    const response = await call(() => buildDecentraderGapProjectionHistory(payload()));
    expect(response.statusCode).toBe(409);
    expect(response.body.code).toBe('HISTORICAL_STATE_UNVERIFIED');
    expect(response.body.records).toBeUndefined();
  });
  test('does not read unsupported markets and fails clearly before stored history is ready', async () => {
    const read = jest.fn(() => { throw new Error('Awaiting stored history'); });
    expect((await call(read, { market: 'ETH-USD' })).statusCode).toBe(400);
    expect(read).not.toHaveBeenCalled();
    const unavailable = await call(read);
    expect(unavailable.statusCode).toBe(503);
    expect(unavailable.body.error).toBe('Awaiting stored history');
  });
});
