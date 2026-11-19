import fs from 'fs';
import os from 'os';
import path from 'path';
import { BtcReplayStopJournal, BtcTradeReplay, btcReplayPositions, btcReplayRange } from '../src/services/dydx_v4/btcTradeReplay';

const HOUR = 3600000;
const opened = Date.parse('2026-10-05T10:02:00Z');
const closed = opened + 6 * HOUR;
const row = { id: 'internal', market: 'BTC-USD', side: 'LONG', status: 'CLOSED',
  createdAt: new Date(opened).toISOString(), closedAt: new Date(closed).toISOString(),
  entryPrice: '82000', exitPrice: '82500', size: '0', subaccountNumber: 0 };

describe('BTC execution replay', () => {
  let directory: string, file: string;
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-replay-test-'));
    file = path.join(directory, 'stops.json');
  });
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }));

  test('uses actual BTC positions, without exposing raw indexer or account identifiers', () => {
    const [trade] = btcReplayPositions([row, { ...row, market: 'ETH-USD' }, { ...row, entryPrice: '' }]);
    expect(trade).toEqual({
      id: expect.stringMatching(/^[0-9a-f]{24}$/), direction: 'long', openedAt: opened, entryPrice: 82000,
      status: 'CLOSED', closedAt: closed, exitPrice: 82500
    });
    expect(btcReplayPositions([{ ...row, status: 'OPEN' }])[0]).toMatchObject({ id: trade.id, closedAt: null, exitPrice: null });
    expect(btcReplayPositions([{ ...row, closedAt: '', exitPrice: null }])[0]).toMatchObject({ closedAt: null, exitPrice: null });
  });

  test('bounds candles for long-running trades and allows a separate exit window', () => {
    const trade = { openedAt: opened, closedAt: opened + 300 * HOUR };
    const now = opened + 301 * HOUR;
    const entry = btcReplayRange(trade, 'entry', now);
    const exit = btcReplayRange(trade, 'latest', now);
    expect(entry).toMatchObject({ hasEarlier: false, hasLater: true });
    expect(exit).toMatchObject({ hasEarlier: true, hasLater: false });
    expect(entry.through - entry.from).toBe(168 * HOUR);
    expect(exit.through - exit.from).toBe(168 * HOUR);
  });

  test('persists only first verified observation per stop, matches trade side and does not infer old stops', async () => {
    const journal = new BtcReplayStopJournal(file);
    journal.observe('first', 'SELL', 81500, opened + 2000);
    journal.observe('first', 'SELL', 81500, opened + 4000);
    journal.observe('second', 'SELL', 81700, opened + 2 * HOUR);
    journal.observe('wrong-side', 'BUY', 83000, opened + HOUR);
    journal.observe('outside-interval', 'SELL', 82000, closed + 1000);
    await (journal as any).write;
    const restarted = new BtcReplayStopJournal(file);
    await expect(restarted.forTrade(btcReplayPositions([row])[0], closed + HOUR)).resolves.toEqual([
      { at: opened + 2000, price: 81500 }, { at: opened + 2 * HOUR, price: 81700 }
    ]);
    await expect(new BtcReplayStopJournal(path.join(directory, 'absent.json')).forTrade(btcReplayPositions([row])[0], closed))
      .resolves.toEqual([]);
  });

  test('lazy loads exact selected closed candles; leaves missing data missing', async () => {
    const positions = jest.fn().mockResolvedValue([row]);
    const candles = jest.fn().mockImplementation(async (from: number, through: number) => [
      candle(through - HOUR), candle(through), candle(from - HOUR), candle(from)
    ]);
    const candle = (start: number) => ({ startedAt: new Date(start).toISOString(), open: '82000',
      high: '82200', low: '81700', close: '82001' });
    const replay = new BtcTradeReplay(positions, candles, new BtcReplayStopJournal(file));
    const list = await replay.snapshot() as { trades: ReturnType<typeof btcReplayPositions> };
    expect(candles).not.toHaveBeenCalled();
    await expect(replay.snapshot('unknown')).resolves.toBeUndefined();
    const detail = await replay.snapshot(list.trades[0].id, 'entry', closed + 2 * HOUR);
    expect(detail).toMatchObject({ trade: list.trades[0], candles: [{ closed: true }, { closed: true }], stops: [] });
    expect(positions).toHaveBeenCalledTimes(1);
  });
});
