import { btcGreenRetestSignals, simulateBtcGreenRetest } from '../src/research/btcGreenRetest';
import { PURPLE_LINE_SETUPS } from '../src/services/purpleLineSetup';

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
  return { rows, context: { signals, yellows: yellows.map(i => ({ at: at(i), direction })), from: at(0), through: at(11) } };
}

describe('BTC Green retest using current gates', () => {
  const original = process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
  beforeEach(() => { process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = '.3'; });
  afterEach(() => {
    if (original === undefined) delete process.env.DECENTRADER_SL_MAX_DISTANCE_PCT;
    else process.env.DECENTRADER_SL_MAX_DISTANCE_PCT = original;
  });
  test('constructs Green and Yellow from native BTC Spot replica, not account history', () => {
    const setup = PURPLE_LINE_SETUPS['BTC-USD'];
    const frames = [100, 125].map((close, i) => ({ t: iso(i).slice(0, 19).replace('T', ' '),
      startedAtMs: at(i), close, price: close }));
    const payload = { frames, gaps: [{ left: 85, right: 115 }, { left: 85, right: 115 }],
      zoneSeed: [['L', 10, 85, 2], ['S', 10, 115, 2]], zoneDeltas: [[], [['L', 10, 90, 1]]],
      modelVersion: setup.modelVersion, quality: { causalModel: true, priceStepUsd: setup.priceStepUsd },
      source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H', ok: true }] } };
    expect(btcGreenRetestSignals(payload, at(2))).toMatchObject({
      signals: [{ direction: 'long', closedAt: at(2) / 1000, edge: 115 }], yellows: [{ at: at(2), direction: 'long' }]
    });
    expect(() => btcGreenRetestSignals(payload, at(2) - 1)).toThrow('Incomplete');
    expect(() => btcGreenRetestSignals({ ...payload, source: { ...payload.source, market: 'ETH-USD' } }, at(2))).toThrow('BTC Spot');
  });

  test.each(['long', 'short'] as const)('%s sizes by fractal, starts at the signal body open and only tightens on a later Yellow', direction => {
    const { rows, context } = fixtures(direction);
    const result = simulateBtcGreenRetest(rows, context);
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
  });

  test('never trails for an opposite Yellow, and does not know whether the trade survived a missing candle', () => {
    const { rows, context } = fixtures('long');
    const opposite = context.yellows.map(row => ({ ...row, direction: 'short' as const }));
    const [trade] = simulateBtcGreenRetest(rows, { ...context, yellows: opposite }).trades;
    expect(trade.stops).toHaveLength(1);
    expect(trade.yellows).toEqual([]);
    const gapped = simulateBtcGreenRetest(rows.filter(row => row.timestamp !== iso(8)), context);
    expect(gapped).toMatchObject({ incompleteAt: at(8), trades: [{ status: 'INCOMPLETE' }] });
    expect(gapped.trades[0].exitPrice).toBeUndefined();
  });
});
