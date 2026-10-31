import fs from 'fs';
import path from 'path';
import { binanceGet, binanceRetryAt } from './binanceHttp';

const URL = 'https://fapi.binance.com/fapi/v1/klines';
const HOUR = 3600, MAX_RECORDS = 5000;
let annotationReadBusy = false;
export type FuturesDeltaCandle = {
  hour: number; startedAt: number; closedAt: number; volumeBase: number; takerBuyVolumeBase: number;
  rawDeltaPct: number; fetchedAt: string;
};
export type FuturesDeltaRead = { hour: number; status: 'ok'; candle: FuturesDeltaCandle } |
  { hour: number; status: 'pending' | 'unavailable'; retryAt: string; error?: string };

function historyFile(symbol: 'BTCUSDT' | 'ETHUSDT' | 'SOLUSDT'): string {
  const dom = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  return path.join(dom ? path.dirname(dom) : path.join(process.cwd(), 'data'), `${symbol.replace('USDT', '').toLowerCase()}-intrusion-futures-delta.json`);
}

export function validateDeltaHours(hours: number[], now = Date.now()): number[] {
  if (!Array.isArray(hours) || !hours.length || hours.length > 8 || hours.some(hour =>
    !Number.isSafeInteger(hour) || hour < 1567900800 || hour % HOUR || (hour + HOUR) * 1000 > now)) {
    throw new Error('Supply 1-8 closed candle-open hours as Unix seconds.');
  }
  return [...new Set(hours)];
}

export function parseFuturesDeltaCandle(raw: unknown, hour: number, now = Date.now()): FuturesDeltaCandle {
  validateDeltaHours([hour], now);
  if (!Array.isArray(raw)) throw new Error('Missing Futures 5m candle.');
  const start = Number(raw[0]), end = Number(raw[6]), volume = Number(raw[5]), buy = Number(raw[9]);
  if (raw[5] === '' || raw[9] === '' || raw[5] === null || raw[9] === null ||
    start !== (hour + 55 * 60) * 1000 || end !== (hour + HOUR) * 1000 - 1 || end >= now ||
    !Number.isFinite(volume) || !Number.isFinite(buy) || volume <= 0 || buy < 0 || buy > volume) {
    throw new Error('Invalid or zero-volume closed Futures 5m candle.');
  }
  return { hour, startedAt: hour + 55 * 60, closedAt: hour + HOUR, volumeBase: volume,
    takerBuyVolumeBase: buy, rawDeltaPct: (2 * buy / volume - 1) * 100, fetchedAt: new Date(now).toISOString() };
}

function validStored(value: any): value is FuturesDeltaCandle {
  return value && Number.isSafeInteger(value.hour) && value.hour % HOUR === 0 && value.hour >= 1567900800 &&
    value.startedAt === value.hour + 55 * 60 && value.closedAt === value.hour + HOUR &&
    value.closedAt * 1000 <= Date.now() && Number.isFinite(value.volumeBase) && value.volumeBase > 0 &&
    Number.isFinite(value.takerBuyVolumeBase) && value.takerBuyVolumeBase >= 0 && value.takerBuyVolumeBase <= value.volumeBase &&
    Number.isFinite(value.rawDeltaPct) && Math.abs(value.rawDeltaPct - (2 * value.takerBuyVolumeBase / value.volumeBase - 1) * 100) < 1e-9 &&
    Number.isFinite(Date.parse(value.fetchedAt)) && Date.parse(value.fetchedAt) >= value.closedAt * 1000;
}

// Lazy closed-candle annotation cache; no startup fetches or entry dependency.
export class IntrusionFuturesDeltaFeed {
  private records = new Map<number, FuturesDeltaCandle>();
  private failures = new Map<number, FuturesDeltaRead>();
  private pending = new Map<number, Promise<FuturesDeltaRead>>();
  private loaded = false;
  private persistenceDisabled = false;
  private readonly file: () => string;
  constructor(readonly symbol: 'BTCUSDT' | 'ETHUSDT' | 'SOLUSDT', file?: () => string) {
    if (!['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].includes(symbol)) throw new Error('Unsupported Futures delta symbol.');
    this.file = file || (() => historyFile(symbol));
  }

  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    try {
      if (fs.statSync(this.file()).size > 2_000_000) throw new Error('Delta history exceeds its size bound.');
      const stored = JSON.parse(fs.readFileSync(this.file(), 'utf8'));
      if (stored.version !== 1 || stored.symbol !== this.symbol || !Array.isArray(stored.records) ||
        stored.records.length > MAX_RECORDS || !stored.records.every(validStored)) throw new Error('Invalid delta history.');
      for (const row of stored.records) this.records.set(row.hour, row);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      // Preserve damaged history for investigation instead of silently replacing it.
      this.persistenceDisabled = true;
      console.warn(`${this.symbol} Futures delta history unavailable; preserved existing file:`, String(error));
    }
  }

  private persist(): void {
    if (this.persistenceDisabled) return;
    try {
      const file = this.file(), temporary = `${file}.${process.pid}.tmp`;
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(temporary, JSON.stringify({ version: 1, symbol: this.symbol, records: [...this.records.values()] }));
      fs.renameSync(temporary, file);
    } catch (error) {
      this.persistenceDisabled = true;
      console.warn(`Could not persist ${this.symbol} Futures delta history:`, String(error));
    }
  }

  private async read(hour: number): Promise<FuturesDeltaRead> {
    const known = this.records.get(hour);
    if (known) return { hour, status: 'ok', candle: { ...known } };
    const failed = this.failures.get(hour);
    if (failed && failed.status !== 'ok' && Date.parse(failed.retryAt) > Date.now()) return failed;
    const pending = this.pending.get(hour);
    if (pending) return pending;
    // At most one annotation request can enter the shared host queue at a time.
    if (annotationReadBusy) return { hour, status: 'pending', retryAt: new Date(Date.now() + 5000).toISOString() };
    annotationReadBusy = true;
    const run = (async (): Promise<FuturesDeltaRead> => {
      try {
        const response = await binanceGet<unknown[]>(URL, { timeout: 8000,
          params: { symbol: this.symbol, interval: '5m', startTime: (hour + 3300) * 1000, endTime: (hour + HOUR) * 1000 - 1, limit: 1 } });
        if (!Array.isArray(response.data) || response.data.length !== 1) throw new Error('Closed Futures candle not available yet.');
        const candle = parseFuturesDeltaCandle(response.data[0], hour);
        this.records.set(hour, candle); this.failures.delete(hour);
        while (this.records.size > MAX_RECORDS) this.records.delete(this.records.keys().next().value);
        this.persist();
        return { hour, status: 'ok', candle: { ...candle } };
      } catch (error) {
        const recent = Date.now() - (hour + HOUR) * 1000 < 120_000;
        const retryAt = Math.max(Date.now() + (recent ? 15_000 : 300_000), binanceRetryAt(URL));
        const result: FuturesDeltaRead = { hour, status: 'unavailable', retryAt: new Date(retryAt).toISOString(),
          error: error instanceof Error ? error.message : String(error) };
        this.failures.set(hour, result);
        while (this.failures.size > MAX_RECORDS) this.failures.delete(this.failures.keys().next().value);
        return result;
      }
    })().finally(() => { this.pending.delete(hour); annotationReadBusy = false; });
    this.pending.set(hour, run);
    return run;
  }

  async snapshot(hours: number[]) {
    const requested = validateDeltaHours(hours);
    this.load();
    const records: FuturesDeltaRead[] = [];
    for (const hour of requested) records.push(await this.read(hour));
    return { symbol: this.symbol, venue: 'Binance USD-M Futures', interval: '5m', timeBasis: 'signal-last-five-minutes',
      fetchedAt: new Date().toISOString(), persistenceAvailable: !this.persistenceDisabled, records };
  }
}

export const btcIntrusionFuturesDeltaFeed = new IntrusionFuturesDeltaFeed('BTCUSDT');
export const ethIntrusionFuturesDeltaFeed = new IntrusionFuturesDeltaFeed('ETHUSDT');
export const solIntrusionFuturesDeltaFeed = new IntrusionFuturesDeltaFeed('SOLUSDT');
