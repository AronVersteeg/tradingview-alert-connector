export {};
const { buildIntrusionConfluence } = require('../public/btc-spot-chart/intrusion-confluence.js');
const { buildOrderBlocks } = require('../public/btc-spot-chart/order-blocks.js');
const HOUR = 3600, start = Date.parse('2026-11-01T00:00:00Z') / 1000;
const time = (hour: number) => start + hour * HOUR;
const iso = (hour: number) => new Date(time(hour) * 1000).toISOString();
const source = 'binance-spot-recorded-intrusions';
const intrusion = (hour: number, direction: string) => ({ time: time(hour), alertTime: iso(hour), direction });
const candle = (hour: number, overrides = {}) => ({ time: time(hour), open: 100, high: 115, low: 95,
  close: 100, volume: 1, closed: true, ...overrides });

describe.each(['BTC', 'ETH', 'SOL'])('%s intrusion confluence', asset => {
  const market = `${asset}-USD`, symbol = `${asset}USDT`;
  const candles = Array.from({ length: 9 }, (_, hour) => candle(hour));
  const history = (records: any[]) => ({ market, source, timeBasis: 'alert-candle-open-utc', records });
  const daily = (hour: number, direction: string) => ({ symbol, venue: 'Binance Spot', intrusionSource: source,
    signals: [{ time: time(hour), closedAt: time(hour + 1), direction }] });
  const model = (blocks: any[]) => ({ symbol, venue: 'Binance Spot', interval: '1h', blocks });
  const block = (direction: string, confirmed: number, broken?: number) => ({ id: `${direction}-${confirmed}`, direction,
    originTime: time(confirmed - 2), confirmedAt: time(confirmed), state: broken === undefined ? 'intact' : 'broken',
    brokenTime: broken === undefined ? null : time(broken), brokenAt: broken === undefined ? null : time(broken + 1) });
  const build = (records: any[], d?: any, ob?: any, through = 8) =>
    buildIntrusionConfluence(candles, history(records), d, ob, time(through) * 1000, market).signals;

  test('joins each branch independently and never borrows events from adjacent candles', () => {
    const records = [intrusion(1, 'long'), intrusion(3, 'short'), intrusion(5, 'long'), intrusion(6, 'short')];
    const ob = model([block('bullish', 3, 3), block('bullish', 6)]);
    expect(build(records, daily(1, 'long'), ob).map((signal: any) => signal.time)).toEqual([time(1), time(3), time(5)]);
    expect(build(records, undefined, ob).map((signal: any) => signal.time)).toEqual([time(3), time(5)]);
    expect(build(records, daily(1, 'long')).map((signal: any) => signal.time)).toEqual([time(1)]);
  });

  test('keeps one line for all matches and ignores duplicates', () => {
    const ob = model([block('bearish', 2, 5), block('bullish', 6), block('bullish', 6)]);
    expect(build([intrusion(5, 'long'), intrusion(5, 'long')], daily(5, 'long'), ob)).toEqual([{
      time: time(5), closedAt: time(6), matches: [{ direction: 'long', dailyBreak: true,
        brokenBlockIds: ['bearish-2'], newBlockIds: ['bullish-6'] }]
    }]);
  });

  test('checks direction, ignores open/future candles, and uses confirmation rather than origin', () => {
    expect(build([intrusion(4, 'short'), intrusion(5, 'short')], daily(5, 'long'), model([block('bullish', 6)]))).toEqual([]);
    expect(build([intrusion(5, 'long')], daily(5, 'long'), model([block('bullish', 6)]), 5)).toEqual([]);
    expect(buildIntrusionConfluence([candle(5, { closed: false })], history([intrusion(5, 'long')]), daily(5, 'long'),
      model([block('bullish', 6)]), time(8) * 1000, market).signals).toEqual([]);
  });

  test('consumes the real detector confirmation and mitigation without changing them', () => {
    const rows = [candle(0), candle(1, { high: 110, low: 90, volume: 10 }), candle(2), candle(3, { low: 89 }), candle(4)];
    const records = [intrusion(1, 'long'), intrusion(2, 'long'), intrusion(3, 'short')];
    const wick = buildOrderBlocks(rows, { symbol, length: 1, mitigation: 'Wick' }, time(8) * 1000);
    const close = buildOrderBlocks(rows, { symbol, length: 1, mitigation: 'Close' }, time(8) * 1000);
    expect(build(records, undefined, wick).map((signal: any) => signal.time)).toEqual([time(2), time(3)]);
    expect(build(records, undefined, close).map((signal: any) => signal.time)).toEqual([time(2)]);
  });

  test('rejects non-Spot and cross-market inputs', () => {
    expect(() => build([intrusion(1, 'long')], daily(1, 'long'), { ...model([]), venue: 'Binance Futures' })).toThrow('source');
    expect(() => build([], { ...daily(1, 'long'), symbol: 'INJUSDT' })).toThrow('source');
    expect(() => buildIntrusionConfluence(candles, { ...history([]), source: 'decentrader-recorded-intrusions' },
      undefined, model([]), time(8) * 1000, market)).toThrow('source');
  });
});
