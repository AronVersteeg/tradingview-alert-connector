import fs from 'fs';
import path from 'path';
import Decimal from 'decimal.js';
import { binanceGet, binanceRetryAt } from './binanceHttp';

const BASE = 'https://api.binance.com/api/v3/';
const HOUR = 3600, LIMIT = 1000, MAX_RECORDS = 5000;
const METHOD = 'exact-price-base-volume';
const ZERO = BigInt(0);
type Symbol = 'BTCUSDT' | 'ETHUSDT';
export type SpotPoc = {
  hour: number; startedAt: number; closedAt: number; price: number;
  volumeBase: string; priceVolumeBase: string; tradeCount: number;
  aggregateTrades: number; priceLevels: number; pages: number; fetchedAt: string;
};
type Read = { hour: number; status: 'ok'; poc: SpotPoc } |
  { hour: number; status: 'pending' | 'unavailable'; retryAt: string; error?: string; pages?: number };
type Job = {
  owner: IntrusionSpotPocFeed; hour: number; touched: number; expected?: bigint; trades?: number;
  low?: number; high?: number; total: bigint; count: number; aggregates: number; pages: number;
  lastId?: number; lastRawId?: number; lastTime?: number; levels: Map<string, bigint>;
  waiting: boolean;
  retryAt?: number; consecutiveFailures?: number; lastError?: string;
};
// A chart annotation must not burst through the trading service's shared Binance queue.
const jobs = new Map<IntrusionSpotPocFeed, Job>();
let busy = false, nextReadAt = 0, lastOwner: IntrusionSpotPocFeed | undefined;

export function validatePocHour(hour: number, now = Date.now()): number {
  if (!Number.isSafeInteger(hour) || hour < 1502928000 || hour % HOUR || (hour + HOUR) * 1000 > now) {
    throw new Error('Supply one closed Spot candle-open hour as Unix seconds.');
  }
  return hour;
}

function units(value: unknown): bigint {
  if (typeof value !== 'string' || !/^\d+(\.\d{1,8})?$/.test(value)) throw new Error('Invalid Spot precision.');
  return BigInt(new Decimal(value).mul(1e8).toFixed(0));
}
function amount(value: bigint): string { return new Decimal(value.toString()).div(1e8).toFixed(8); }
function validStored(p: any): p is SpotPoc {
  try {
    validatePocHour(p.hour);
    return p.startedAt === p.hour && p.closedAt === p.hour + HOUR && Number.isFinite(p.price) && p.price > 0 &&
      units(p.volumeBase) > ZERO && units(p.priceVolumeBase) > ZERO && units(p.priceVolumeBase) <= units(p.volumeBase) &&
      [p.tradeCount, p.aggregateTrades, p.priceLevels, p.pages].every(n => Number.isSafeInteger(n) && n > 0) &&
      p.aggregateTrades <= p.tradeCount && p.priceLevels <= p.aggregateTrades && p.pages <= 2000 &&
      Number.isFinite(Date.parse(p.fetchedAt)) && Date.parse(p.fetchedAt) >= p.closedAt * 1000;
  } catch { return false; }
}

export class IntrusionSpotPocFeed {
  private records = new Map<number, SpotPoc>();
  private failures = new Map<number, Read>();
  private loaded = false;
  private persistenceDisabled = false;
  private readonly file: () => string;
  constructor(readonly symbol: Symbol, file?: () => string) {
    if (!['BTCUSDT', 'ETHUSDT'].includes(symbol)) throw new Error('Unsupported Spot POC symbol.');
    this.file = file || (() => {
      const dom = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
      return path.join(dom ? path.dirname(dom) : path.join(process.cwd(), 'data'), `${symbol.toLowerCase()}-intrusion-spot-poc.json`);
    });
  }

  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    try {
      if (fs.statSync(this.file()).size > 3_000_000) throw new Error('POC history exceeds its size bound.');
      const stored = JSON.parse(fs.readFileSync(this.file(), 'utf8'));
      if (stored.version !== 1 || stored.symbol !== this.symbol || stored.method !== METHOD ||
        !Array.isArray(stored.records) || stored.records.length > MAX_RECORDS || !stored.records.every(validStored)) {
        throw new Error('Invalid Spot POC history.');
      }
      for (const p of stored.records) this.records.set(p.hour, p);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      this.persistenceDisabled = true;
      console.warn(`${this.symbol} Spot POC history unavailable; preserved existing file:`, String(error));
    }
  }

  private persist(): void {
    if (this.persistenceDisabled) return;
    try {
      const file = this.file(), temporary = `${file}.${process.pid}.tmp`;
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(temporary, JSON.stringify({ version: 1, symbol: this.symbol, method: METHOD, records: [...this.records.values()] }));
      fs.renameSync(temporary, file);
    } catch (error) {
      this.persistenceDisabled = true;
      console.warn(`Could not persist ${this.symbol} Spot POC history:`, String(error));
    }
  }

  private pending(hour: number): Read {
    const job = jobs.get(this);
    return { hour, status: 'pending', retryAt: new Date(Math.max(Date.now() + 5000, nextReadAt,
      job?.hour === hour ? job.retryAt || 0 : 0, binanceRetryAt(BASE + 'aggTrades'))).toISOString(),
      ...(job?.owner === this && job.hour === hour ? { pages: job.pages, ...(job.lastError ? { error: job.lastError } : {}) } : {}) };
  }

  private async read(hour: number): Promise<Read> {
    const known = this.records.get(hour);
    if (known) return { hour, status: 'ok', poc: { ...known } };
    const failed = this.failures.get(hour);
    if (failed && failed.status !== 'ok' && Date.parse(failed.retryAt) > Date.now()) return { ...failed };
    if (!busy) for (const [owner, job] of jobs) if (Date.now() > Math.max(job.touched, job.retryAt || 0) + 120_000) jobs.delete(owner);
    let current = jobs.get(this);
    if (current && current.hour !== hour || !current && jobs.size >= 2) return this.pending(hour);
    if (!current) {
      current = { owner: this, hour, touched: Date.now(), total: ZERO, count: 0, aggregates: 0, pages: 0, levels: new Map(), waiting: true };
      jobs.set(this, current);
    }
    current.touched = Date.now(); current.waiting = true;
    const yieldToOtherChart = lastOwner === this && [...jobs.values()].some(other =>
      other.owner !== this && other.waiting && (other.retryAt || 0) <= Date.now() && Date.now() - other.touched < 20_000);
    if (busy || Date.now() < Math.max(nextReadAt, current.retryAt || 0, binanceRetryAt(BASE + 'aggTrades')) || yieldToOtherChart) return this.pending(hour);
    current.waiting = false;
    busy = true;
    let requestFailed = false;
    const request = async (endpoint: string, params: Record<string, string | number>) => {
      try {
        const response = await binanceGet<unknown[]>(BASE + endpoint, { timeout: 15000, params });
        current!.retryAt = 0; current!.consecutiveFailures = 0; current!.lastError = undefined;
        return response;
      } catch (error) { requestFailed = true; throw error; }
    };
    try {
      if (current.expected === undefined) {
        const response = await request('klines',
          { symbol: this.symbol, interval: '1h', startTime: hour * 1000, endTime: (hour + HOUR) * 1000 - 1, limit: 1 });
        const row = response.data?.[0] as any;
        if (!Array.isArray(response.data) || response.data.length !== 1 || !Array.isArray(row) ||
          Number(row[0]) !== hour * 1000 || Number(row[6]) !== (hour + HOUR) * 1000 - 1 ||
          !Number.isSafeInteger(row[8]) || row[8] <= 0 || units(row[5]) <= ZERO ||
          !Number.isFinite(Number(row[2])) || !Number.isFinite(Number(row[3])) || Number(row[3]) <= 0 || Number(row[2]) < Number(row[3])) {
          throw new Error('Exact closed Spot candle unavailable.');
        }
        current.expected = units(row[5]); current.trades = row[8];
        current.high = Number(row[2]); current.low = Number(row[3]);
        return this.pending(hour);
      }
      const params = current.lastId === undefined
        ? { symbol: this.symbol, startTime: hour * 1000, endTime: (hour + HOUR) * 1000 - 1, limit: LIMIT }
        : { symbol: this.symbol, fromId: current.lastId + 1, limit: LIMIT };
      const response = await request('aggTrades', params);
      if (!Array.isArray(response.data) || response.data.length > LIMIT) throw new Error('Invalid Spot trade page.');
      current.pages++;
      let ended = response.data.length < LIMIT;
      const otherLevels = [...jobs.values()].reduce((sum, item) => sum + (item === current ? 0 : item.levels.size), 0);
      for (const raw of response.data as any[]) {
        if (!raw || ![raw.a, raw.f, raw.l, raw.T].every(n => Number.isSafeInteger(n) && n >= 0) || raw.l < raw.f ||
          current.lastId !== undefined && raw.a !== current.lastId + 1 ||
          current.lastRawId !== undefined && raw.f !== current.lastRawId + 1 ||
          current.lastTime !== undefined && raw.T < current.lastTime || raw.T < hour * 1000) throw new Error('Non-contiguous Spot trade history.');
        if (raw.T >= (hour + HOUR) * 1000) { ended = true; break; }
        const quantity = units(raw.q), priceUnits = units(raw.p), price = Number(raw.p);
        if (quantity <= ZERO || priceUnits <= ZERO || price < current.low! || price > current.high!) throw new Error('Invalid Spot trade.');
        const key = amount(priceUnits);
        current.levels.set(key, (current.levels.get(key) || ZERO) + quantity);
        current.total += quantity; current.count += raw.l - raw.f + 1; current.aggregates++;
        current.lastId = raw.a; current.lastRawId = raw.l; current.lastTime = raw.T;
        if (current.total > current.expected || current.count > current.trades!) throw new Error('Spot trades exceed candle totals.');
        if (current.levels.size + otherLevels > 100_000 ||
            current.aggregates > 2_000_000) throw new Error('Spot POC memory bound reached.');
      }
      // Completion requires agreement with the independent 1H volume AND raw trade count.
      if (current.total === current.expected && current.count === current.trades) {
        let price = 0, peak = ZERO;
        for (const [level, volume] of current.levels) {
          if (volume > peak || volume === peak && Number(level) < price) { price = Number(level); peak = volume; }
        }
        const poc: SpotPoc = { hour, startedAt: hour, closedAt: hour + HOUR, price,
          volumeBase: amount(current.total), priceVolumeBase: amount(peak), tradeCount: current.count,
          aggregateTrades: current.aggregates, priceLevels: current.levels.size, pages: current.pages, fetchedAt: new Date().toISOString() };
        this.records.set(hour, poc); this.failures.delete(hour);
        while (this.records.size > MAX_RECORDS) this.records.delete(this.records.keys().next().value);
        this.persist(); jobs.delete(this);
        return { hour, status: 'ok', poc: { ...poc } };
      }
      if (ended || current.pages >= 2000) throw new Error('Incomplete Spot trade history; no POC published.');
      return this.pending(hour);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const status = /HTTP (\d{3})/.exec(message)?.[1];
      // A failed HTTP read has not modified any profile sums. Resume the same
      // next aggregate ID after backoff; data-validation failures still discard it.
      if (requestFailed && (!status || Number(status) >= 500 || ['418', '429'].includes(status))) {
        current.consecutiveFailures = (current.consecutiveFailures || 0) + 1;
        current.lastError = message;
        current.retryAt = Math.max(Date.now() + Math.min(300_000, 15_000 * 2 ** Math.min(5, current.consecutiveFailures - 1)),
          binanceRetryAt(BASE + 'aggTrades'));
        return this.pending(hour);
      }
      jobs.delete(this);
      const result: Read = { hour, status: 'unavailable',
        retryAt: new Date(Math.max(Date.now() + 300_000, binanceRetryAt(BASE + 'aggTrades'))).toISOString(),
        error: error instanceof Error ? error.message : String(error) };
      this.failures.set(hour, result);
      while (this.failures.size > MAX_RECORDS) this.failures.delete(this.failures.keys().next().value);
      return { ...result };
    } finally { busy = false; lastOwner = this; nextReadAt = Date.now() + 5000; }
  }

  async snapshot(hour: number) {
    validatePocHour(hour); this.load();
    const record = await this.read(hour);
    return { symbol: this.symbol, venue: 'Binance Spot', interval: '1h', method: METHOD,
      persistenceAvailable: !this.persistenceDisabled, record };
  }
}

export const btcIntrusionSpotPocFeed = new IntrusionSpotPocFeed('BTCUSDT');
export const ethIntrusionSpotPocFeed = new IntrusionSpotPocFeed('ETHUSDT');
