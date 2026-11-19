import { createHash } from 'crypto';
import fs from 'fs';
import path from 'path';
import { dydxHourlyCandlesToFractalRows } from '../decentraderGapMonitor';

const HOUR = 3_600_000;
const MAX_BARS = 168;
const MAX_STOPS = 512;
const hour = (ms: number) => Math.floor(ms / HOUR) * HOUR;
type Stop = { key: string; at: number; price: number; side: 'BUY' | 'SELL' };

export function btcReplayPositions(rows: any[]) {
  return rows.flatMap(row => {
    const openedAt = Date.parse(row?.createdAt);
    const closedAt = Date.parse(row?.closedAt);
    const entryPrice = Number(row?.entryPrice);
    const exitPrice = Number(row?.exitPrice);
    const direction = row?.side === 'LONG' ? 'long' : row?.side === 'SHORT' ? 'short' : undefined;
    if (row?.market !== 'BTC-USD' || !direction || !Number.isFinite(openedAt) || !Number.isFinite(entryPrice) || entryPrice <= 0 ||
      !['OPEN', 'CLOSED', 'LIQUIDATED'].includes(row.status)) return [];
    const ended = row.status !== 'OPEN';
    return [{ id: createHash('sha256').update(JSON.stringify([row.id || '', row.createdAt, row.side])).digest('hex').slice(0, 24),
      direction, openedAt, entryPrice, status: row.status as 'OPEN' | 'CLOSED' | 'LIQUIDATED',
      closedAt: ended && Number.isFinite(closedAt) && closedAt >= openedAt ? closedAt : null,
      exitPrice: ended && Number.isFinite(exitPrice) && exitPrice > 0 ? exitPrice : null }];
  }).sort((a, b) => b.openedAt - a.openedAt).slice(0, 16);
}

export function btcReplayRange(trade: { openedAt: number; closedAt: number | null }, window: 'entry' | 'latest', now: number) {
  const first = hour(trade.openedAt) - 24 * HOUR;
  const last = Math.min(hour(trade.closedAt ?? now) + 9 * HOUR, hour(now));
  const from = window === 'latest' ? Math.max(first, last - MAX_BARS * HOUR) : first;
  const through = Math.min(last, from + MAX_BARS * HOUR);
  return { from, through, hasEarlier: from > first, hasLater: through < last };
}

export class BtcReplayStopJournal {
  private write?: Promise<void>;
  private loaded = false;
  private rows: Stop[] = [];
  private pending: Stop[] = [];

  constructor(private readonly file: string) {}

  observe(orderId: string, side: string, price: number, at = Date.now()): void {
    if (!orderId || !['BUY', 'SELL'].includes(side) || !Number.isFinite(price) || price <= 0) return;
    const key = createHash('sha256').update(orderId).digest('hex').slice(0, 24);
    if (this.rows.some(row => row.key === key) || this.pending.some(row => row.key === key)) return;
    this.pending.push({ key, side: side as Stop['side'], price, at });
    if (!this.write) {
      this.write = Promise.resolve().then(async () => {
        await this.load();
        while (this.pending.length) {
          const additions = this.pending.splice(0).filter(row => !this.rows.some(old => old.key === row.key));
          if (!additions.length) continue;
          this.rows = [...this.rows, ...additions].slice(-MAX_STOPS);
          const temporary = `${this.file}.${process.pid}.tmp`;
          await fs.promises.mkdir(path.dirname(this.file), { recursive: true });
          await fs.promises.writeFile(temporary, JSON.stringify(this.rows), { mode: 0o600 });
          await fs.promises.rename(temporary, this.file);
        }
      }).catch(() => { this.pending = []; console.warn('BTC replay stop evidence could not be saved.'); })
        .finally(() => { this.write = undefined; });
    }
  }

  async forTrade(trade: { direction: string; openedAt: number; closedAt: number | null }, now: number) {
    await this.load();
    const side = trade.direction === 'long' ? 'SELL' : 'BUY';
    return this.rows.filter(row => row.side === side && row.at >= trade.openedAt && row.at < (trade.closedAt ?? now + 1))
      .sort((a, b) => a.at - b.at).map(({ at, price }) => ({ at, price }));
  }

  private loadPromise?: Promise<void>;
  private load(): Promise<void> {
    if (this.loaded) return Promise.resolve();
    this.loadPromise ??= (async () => {
      try {
        const stat = await fs.promises.stat(this.file);
        if (stat.size > 128 * 1024) throw new Error('Replay journal exceeds size limit.');
        const rows = JSON.parse(await fs.promises.readFile(this.file, 'utf8'));
        if (!Array.isArray(rows)) throw new Error('Invalid replay journal.');
        this.rows = rows.filter((row: Stop) => row && typeof row.key === 'string' &&
          ['BUY', 'SELL'].includes(row.side) && Number.isFinite(row.at) && Number.isFinite(row.price) && row.price > 0).slice(-MAX_STOPS);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      this.loaded = true;
    })().finally(() => { this.loadPromise = undefined; });
    return this.loadPromise;
  }
}

type Positions = ReturnType<typeof btcReplayPositions>;

export class BtcTradeReplay {
  private positions?: { checkedAt: number; trades: Positions };
  private positionRead?: Promise<Positions>;
  private candles?: { key: string; checkedAt: number; rows: any[] };
  private candleRead?: { key: string; promise: Promise<any[]> };

  constructor(private readonly readPositions: () => Promise<unknown>, private readonly readCandles: (from: number, through: number) => Promise<unknown>,
    private readonly journal: BtcReplayStopJournal) {}

  private list(): Promise<Positions> {
    if (this.positions && Date.now() - this.positions.checkedAt < 30_000) return Promise.resolve(this.positions.trades);
    this.positionRead ??= this.readPositions().then(rows => {
      if (!Array.isArray(rows)) throw new Error('Missing dYdX position history.');
      const trades = btcReplayPositions(rows);
      this.positions = { trades, checkedAt: Date.now() };
      return trades;
    }).finally(() => { this.positionRead = undefined; });
    return this.positionRead;
  }

  private readWindow(from: number, through: number): Promise<any[]> {
    if (through <= from) return Promise.resolve([]);
    const key = `${from}|${through}`;
    if (this.candles?.key === key && Date.now() - this.candles.checkedAt < 30_000) return Promise.resolve(this.candles.rows);
    if (this.candleRead) {
      if (this.candleRead.key === key) return this.candleRead.promise;
      throw new Error('BTC replay candle read is busy.');
    }
    const promise = this.readCandles(from, through).then(rows => {
      if (!Array.isArray(rows)) throw new Error('Missing dYdX candle history.');
      const candles = new Map(dydxHourlyCandlesToFractalRows(rows, through).flatMap(row => {
        const start = Date.parse(row.timestamp);
        const { openRef: open, highRef: high, lowRef: low, closeRef: close } = row;
        return start >= from && start % HOUR === 0 && start + HOUR <= through &&
          [open, high, low, close].every(v => Number.isFinite(v) && v > 0) &&
          high >= Math.max(open, close) && low <= Math.min(open, close)
          ? [[start, { time: start / 1000, open, high, low, close, closed: true }] as const] : [];
      }));
      const result = [...candles.values()].sort((a, b) => a.time - b.time);
      this.candles = { key, checkedAt: Date.now(), rows: result };
      return result;
    }).finally(() => { this.candleRead = undefined; });
    this.candleRead = { key, promise };
    return promise;
  }

  async snapshot(id?: string, window: 'entry' | 'latest' = 'entry', now = Date.now()) {
    const trades = await this.list();
    if (!id) return { trades };
    const trade = trades.find(row => row.id === id);
    if (!trade) return undefined;
    const range = btcReplayRange(trade, window, now);
    const candles = await this.readWindow(range.from, range.through);
    const stops = trade.status !== 'OPEN' && trade.closedAt === null ? null
      : await this.journal.forTrade(trade, now).catch(() => null);
    return { trade, candles, stops, range, fractalWindow: 2 };
  }
}
