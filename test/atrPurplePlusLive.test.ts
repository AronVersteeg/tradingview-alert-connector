import { atrPurpleParentLostBy, atrPurpleParentMatches, matchDydxClosedPosition, noteUnverifiedAtrPurpleCandidate,
  priorUnverifiedAtrPurpleCandidate, type AtrPurpleParent } from '../src/services/atrPurplePlusLive';
import { parseEntryModes } from '../src/services/entryModeSelection';

const market = 'BTC-USD', createdAt = '2026-11-27T12:00:01.204Z', createdAtHeight = '1000042';
const closedAt = '2026-11-27T13:50:00.000Z', close = Date.parse('2026-11-27T14:00:00.000Z');
const identity = { createdAt, createdAtHeight };
const closed = { market, ...identity, status: 'CLOSED', side: 'LONG', subaccountNumber: 0, size: '0.000', closedAt, realizedPnl: '-3.14' };

test('normalizes ordered automatic mode lists without repeating modes', () => {
  expect(parseEntryModes(' ATR-PURPLE , atr-purple-plus, atr-purple ')).toEqual({
    mode: 'atr-purple,atr-purple-plus', modes: ['atr-purple', 'atr-purple-plus'], valid: true
  });
  expect(parseEntryModes('purple-trail, green').modes).toEqual(['purple', 'green']);
});

test.each(['green,manual', 'off,purple', 'green,,atr', 'atr-purple-plus', 'atr-purple,typo'])('fails closed for %s', value => {
  expect(parseEntryModes(value)).toEqual({ mode: 'off', modes: [], valid: false });
});

test('looks up the exact closed exchange position, not a similar position or ambiguous match', () => {
  const match = (rows: unknown[]) => matchDydxClosedPosition(rows, market, 'long', identity);
  expect(match([closed])).toEqual({ closedAt: Date.parse(closedAt), realizedPnl: -3.14 });
  for (const mismatch of [{ market: 'ETH-USD' }, { status: 'OPEN' }, { side: 'SHORT' }, { subaccountNumber: 1 },
    { createdAtHeight: '1000041' }, { size: '0.01' }, { realizedPnl: '' }, { closedAt: createdAt.slice(0, 10) }]) {
    expect(match([{ ...closed, ...mismatch }])).toBeUndefined();
  }
  expect(match([closed, closed])).toBeUndefined();
});

test('a verified losing parent arms one later same-direction candle, never an earlier or unrelated one', () => {
  const parent = { direction: 'long' as const, signalClosedAt: Date.parse('2026-11-27T12:00:00Z'), position: identity,
    result: { closedAt: Date.parse(closedAt), realizedPnl: -3.14 }, consumedAt: close };
  expect(atrPurpleParentMatches(parent, 'long', close)).toBe(true);
  expect(atrPurpleParentMatches(parent, 'short', close)).toBe(false);
  expect(atrPurpleParentMatches(parent, 'long', close + 3_600_000)).toBe(false);
  expect(atrPurpleParentLostBy(parent, close)).toBe(true);
  expect(atrPurpleParentLostBy(parent, close - 3_600_000)).toBe(false);
  expect(atrPurpleParentLostBy({ ...parent, result: { ...parent.result, realizedPnl: 0 } }, close)).toBe(false);
});

test('a later Purple cannot replace the first candle when its loss evidence arrived late', () => {
  const parent: AtrPurpleParent = { direction: 'long', signalClosedAt: Date.parse(createdAt) - 3000, position: identity };
  expect(noteUnverifiedAtrPurpleCandidate(parent, close)).toBe(true);
  expect(noteUnverifiedAtrPurpleCandidate(parent, close)).toBe(false);
  expect(noteUnverifiedAtrPurpleCandidate(parent, close + 3_600_000)).toBe(true);
  parent.result = { closedAt: Date.parse(closedAt), realizedPnl: -1 };
  expect(priorUnverifiedAtrPurpleCandidate(parent, close + 3_600_000)).toBe(close);
  parent.result.closedAt = close + 10_000;
  expect(priorUnverifiedAtrPurpleCandidate(parent, close + 3_600_000)).toBeUndefined();
});
