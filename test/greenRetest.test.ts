import { greenRetestSignals, retestFractalRows, simulateGreenRetest } from '../src/research/greenRetest';
import { buildGreenRetestWithHistory, decodeGreenHistory, readGreenHistory, readRetestHistory, type GreenHistoryArchive } from '../src/research/greenRetestHistory';
import { purpleRetestContext } from '../src/research/purpleRetest';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../src/services/purpleLineSetup';

const HOUR = 3_600_000;
const base = Date.UTC(2026, 9, 7, 0);
const at = (i: number) => base + i * HOUR;
const iso = (i: number) => new Date(at(i)).toISOString();

function fixtures(direction: 'long' | 'short', yellows = [9, 10]) {
  const lows = [96, 94, 90, 93, 95, 101, 101, 100, 101, 102, 98];
  const rows = lows.map((low, i) => {
    const open = i === 4 ? 98 : i === 10 ? 99 : i >= 5 ? 103 : 100;
    const high = Math.max(open + 4, low + 1);
    const close = Math.max(open + 1, low);
    return { timestamp: iso(i), openRef: direction === 'long' ? open : 200 - open,
      highRef: direction === 'long' ? high : 200 - low, lowRef: direction === 'long' ? low : 200 - high,
      closeRef: direction === 'long' ? close : 200 - close, fractalCandleSource: 'dydx-1h' };
  });
  const signals = [5, 6].map(i => ({ time: at(i - 1) / 1000, closedAt: at(i) / 1000, direction,
    close: 100, edge: 99, setup: 'green-line' as const }));
  return { rows, context: { market: 'BTC-USD' as PurpleLineMarket, signals, yellows: yellows.map(i => ({ at: at(i), direction })),
    from: at(0), through: at(11) } };
}

describe('Green retest using current gates', () => {
  const original = process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
  beforeEach(() => { process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '.3'; });
  afterEach(() => {
    if (original === undefined) delete process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
    else process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = original;
  });
  test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s constructs Green and Yellow only from its own native Spot replica', market => {
    const setup = PURPLE_LINE_SETUPS[market];
    const frames = [100, 125].map((close, i) => ({ t: iso(i).slice(0, 19).replace('T', ' '),
      startedAtMs: at(i), close, price: close }));
    const payload = { frames, gaps: [{ left: 85, right: 115 }, { left: 85, right: 115 }],
      zoneSeed: [['L', 10, 85, 2], ['S', 10, 115, 2]], zoneDeltas: [[], [['L', 10, 90, 1]]],
      modelVersion: setup.modelVersion, quality: { causalModel: true, priceStepUsd: setup.priceStepUsd },
      source: { market, sourceStatuses: [{ name: `Binance Spot ${setup.symbol} 1H`, ok: true }] } };
    expect(greenRetestSignals(market, payload, at(2))).toMatchObject({ market,
      signals: [{ direction: 'long', closedAt: at(2) / 1000, edge: 115 }], yellows: [{ at: at(2), direction: 'long' }]
    });
    expect(() => greenRetestSignals(market, payload, at(2) - 1)).toThrow('Incomplete');
    const other = market === 'BTC-USD' ? 'ETH-USD' : 'BTC-USD';
    expect(() => greenRetestSignals(market, { ...payload, source: { ...payload.source, market: other } }, at(2))).toThrow(`${setup.asset} Spot`);
    expect(() => retestFractalRows(market, [{ ticker: other, resolution: '1HOUR', startedAt: iso(0), close: '125' }], at(2)))
      .toThrow('dYdX candle market');
  });

  test.each(['long', 'short'] as const)('%s sizes by fractal, starts at the signal body open and only tightens on a later Yellow', direction => {
    const { rows, context } = fixtures(direction);
    const result = simulateGreenRetest(rows, context);
    const [trade] = result.trades;
    expect(result.trades).toHaveLength(1);
    expect(result.skipped).toBe(1);
    expect(result.skippedWhileOpen).toBe(1);
    expect(trade.openedAt).toBe(at(5));
    expect(trade.initialStop).toBe(rows[4].openRef);
    expect(trade.entryPrice).toBe(rows[5].openRef);
    expect(trade.sizingStop).not.toBe(trade.initialStop);
    expect(trade.yellows.map(row => row.result)).toEqual(['UNCHANGED', 'TIGHTENED']);
    expect(trade.stops.map(row => row.at)).toEqual([at(5), at(10)]);
    expect(trade.yellows[1].fractal?.confirmedAt).toBe(at(10));
    expect(trade).toMatchObject({ status: 'CLOSED', closedAt: at(10), exitPrice: rows[10].openRef, exitReason: 'GAP_STOP' });
    expect(trade.grossPnl).toBeCloseTo(-4 * trade.size, 10);
    expect(trade.markToMarket).toBeUndefined();
  });

  test.each(['long', 'short'] as const)('marks an open %s to the final closed candle, without using later supplied prices', direction => {
    const { rows, context } = fixtures(direction);
    const [trade] = simulateGreenRetest(rows, { ...context, through: at(10) }).trades;
    expect(trade.status).toBe('OPEN');
    expect(trade.grossPnl).toBeUndefined();
    expect(trade.markToMarket).toEqual({ at: at(10), price: rows[9].closeRef, grossPnl: trade.size });
  });

  test('never trails for an opposite Yellow, and does not know whether the trade survived a missing candle', () => {
    const { rows, context } = fixtures('long');
    const opposite = context.yellows.map(row => ({ ...row, direction: 'short' as const }));
    const [trade] = simulateGreenRetest(rows, { ...context, yellows: opposite }).trades;
    expect(trade.stops).toHaveLength(1);
    expect(trade.yellows).toEqual([]);
    const gapped = simulateGreenRetest(rows.filter(row => row.timestamp !== iso(8)), context);
    expect(gapped).toMatchObject({ incompleteAt: at(8), trades: [{ status: 'INCOMPLETE' }] });
    expect(gapped.trades[0].exitPrice).toBeUndefined();
    expect(gapped.trades[0].markToMarket).toBeUndefined();
  });

  test('joins the archive to verified recent signals without reopening an active trade', () => {
    const { rows, context } = fixtures('long');
    const prefix = Array.from({ length: 30 }, (_, i) => ({ ...rows[0], timestamp: iso(i - 30) }));
    const past = { rows: [...prefix, ...rows.slice(0, 6)], context: { ...context, from: at(-30), through: at(6), yellows: [] } };
    const live = { rows: [...prefix.slice(3), ...rows], context: { ...context, from: at(-27) } };
    const joined = buildGreenRetestWithHistory(past, live);
    expect(joined).toMatchObject({ historyCoverage: 'archive-plus-recent', from: at(-30), through: at(11), candidateCount: 2,
      missingDydxHours: 0, skippedWhileOpen: 1, trades: [{ openedAt: at(5), status: 'CLOSED', closedAt: at(10) }] });
    expect(joined.trades).toHaveLength(1);

    const mismatch = { ...live, context: { ...live.context, signals: [] } };
    const missingHour = { ...live, rows: live.rows.filter(row => row.timestamp !== iso(6)) };
    const nonOverlapping = { ...live, context: { ...live.context, from: at(6) } };
    for (const newer of [mismatch, missingHour, nonOverlapping, undefined]) {
      expect(buildGreenRetestWithHistory(past, newer)).toMatchObject({
        historyCoverage: 'archive-only', through: at(6), trades: [{ openedAt: at(5), status: 'OPEN' }]
      });
    }
    expect(() => buildGreenRetestWithHistory(past, { ...live, context: { ...live.context, market: 'ETH-USD' } }))
      .toThrow('different markets');
  });

  test('keeps missing dYdX candles explicit and rejects stale model versions or invalid event chronology', () => {
    const { rows, context } = fixtures('long');
    const setup = PURPLE_LINE_SETUPS['BTC-USD'];
    const file: GreenHistoryArchive = {
      version: 1, model: setup.modelVersion, priceStepUsd: setup.priceStepUsd, cohortWindowHours: 8760,
      from: at(0), through: at(11), dydxFrom: at(0),
      dydx: rows.map((row, i) => i === 8 ? null : [row.openRef, row.highRef, row.lowRef, row.closeRef]),
      greens: context.signals.map(signal => [signal.closedAt * 1000, signal.direction, signal.edge, signal.close]),
      yellows: context.yellows.map(row => [row.at, row.direction])
    };
    const series = decodeGreenHistory('BTC-USD', file, at(11));
    expect(buildGreenRetestWithHistory(series, undefined)).toMatchObject({
      incompleteAt: at(8), trades: [{ status: 'INCOMPLETE' }]
    });
    expect(() => decodeGreenHistory('BTC-USD', { ...file, model: 'other' }, at(11))).toThrow('metadata');
    expect(() => decodeGreenHistory('SOL-USD', { ...file, model: PURPLE_LINE_SETUPS['SOL-USD'].modelVersion,
      priceStepUsd: PURPLE_LINE_SETUPS['SOL-USD'].priceStepUsd }, at(11))).toThrow('metadata');
    expect(() => decodeGreenHistory('BTC-USD', { ...file, greens: file.greens.slice().reverse() }, at(11))).toThrow('event history');
  });

  test.each(Object.keys(PURPLE_LINE_SETUPS) as PurpleLineMarket[])('%s ships its own usable multi-month archive', market => {
    const history = readGreenHistory(market);
    expect(history?.context.market).toBe(market);
    const replay = buildGreenRetestWithHistory(history, undefined);
    expect(replay.incompleteAt).toBeUndefined();
    expect(replay.through - replay.from).toBeGreaterThan(24 * 180 * HOUR);
    expect(replay.trades.length).toBeGreaterThan(1);
    const purple = buildGreenRetestWithHistory(readRetestHistory(market, 'purple'), undefined, 'purple');
    expect(purple).toMatchObject({ market, strategy: 'purple', from: replay.from, through: replay.through });
    expect(purple.incompleteAt).toBeUndefined();
    expect(purple.candidateCount).toBeLessThan(replay.candidateCount);
    expect(purple.trades.length).toBeGreaterThan(1);
    expect(purple.trades.every(trade => trade.dailyLevels?.length)).toBe(true);
  });

  test('Purple requires a directional intrusion and first confirmed Daily close break on the Green candle', () => {
    const cutoff = at(10);
    const signal = { time: at(9) / 1000, closedAt: cutoff / 1000, close: 115, direction: 'long' as const,
      edge: 105, setup: 'green-line' as const };
    const { context } = fixtures('long');
    const green = { ...context, signals: [signal], yellows: [{ at: cutoff, direction: 'long' as const }], through: cutoff };
    const hours = Array.from({ length: 999 }, (_, i) => ({ time: (cutoff - (999 - i) * HOUR) / 1000,
      closed: true, open: 100, high: i >= 997 ? 116 : 102, low: 98, close: i === 998 ? 115 : 100 }));
    const lastDaily = Date.UTC(2026, 9, 7) - 1;
    const record = { id: 'BTCUSDT|HIGH|1', market: 'BTC-USD' as const, symbol: 'BTCUSDT', type: 'HIGH' as const,
      price: 110, priceExact: '110', pivotAt: new Date(Date.UTC(2026, 9, 2)).toISOString(),
      confirmedAt: new Date(Date.UTC(2026, 9, 5) - 1).toISOString(), isLatestForType: true };
    const daily = { market: 'BTC-USD' as const, symbol: 'BTCUSDT', venue: 'Binance Spot' as const, interval: '1d' as const,
      latestClosedCandleAt: new Date(lastDaily).toISOString(), records: [record] };
    expect(purpleRetestContext(green, hours, daily).signals).toEqual([{ ...signal, dailyLevels: [110] }]);
    expect(purpleRetestContext({ ...green, yellows: [{ at: cutoff, direction: 'short' }] }, hours, daily).signals).toEqual([]);
    expect(purpleRetestContext(green, hours, { ...daily, records: [{ ...record, confirmedAt: iso(10) }] }).signals).toEqual([]);
    const previouslyBroken = hours.map((row, i) => i === 994 ? { ...row, high: 114, close: 111 } : row);
    expect(purpleRetestContext(green, previouslyBroken, daily).signals).toEqual([]);
    expect(() => purpleRetestContext(green, hours, { ...daily, symbol: 'ETHUSDT' })).toThrow('Unverified');
  });
});
