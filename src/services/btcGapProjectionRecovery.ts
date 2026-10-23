import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { GapProjectionHistory } from './decentraderGapProjectionHistory';

const MODEL = 'binance-spot-liquidation-cohorts-v2.4';
const HOUR = 3600;
type GapRow = GapProjectionHistory['records'][number];

function validRow(row: GapRow): boolean {
  return Number.isSafeInteger(row.candleTime) && row.candleTime > 0 && row.candleTime % HOUR === 0 &&
    row.availableAt === row.candleTime + HOUR && (row.lower === null && row.upper === null ||
      Number.isFinite(row.lower) && Number.isFinite(row.upper) && row.lower! > 0 && row.upper! > row.lower!);
}

// Display-only: an offline causal recovery plus the append-only local archive.
// No provider requests, trading imports, inferred missing hours or disk writes.
export class BtcGapProjectionRecovery {
  private pending?: Promise<GapProjectionHistory>;
  private cached?: GapProjectionHistory;
  private expiresAt = 0;
  constructor(private directory: () => string,
    private recoveryFile = path.join(process.cwd(), 'public', 'data', 'btc-gap-recovery-v1.json')) {}

  async read(): Promise<GapProjectionHistory> {
    if (this.cached && Date.now() < this.expiresAt) return this.cached;
    if (this.pending) return this.pending;
    this.pending = this.load();
    try {
      this.cached = await this.pending;
      this.expiresAt = Date.now() + 60_000;
      return this.cached;
    } finally { this.pending = undefined; }
  }

  private async load(): Promise<GapProjectionHistory> {
    const recovery = JSON.parse(await fs.promises.readFile(this.recoveryFile, 'utf8'));
    if (recovery.version !== 1 || recovery.symbol !== 'BTCUSDT' || recovery.modelVersion !== MODEL ||
        recovery.historicalAccuracy !== 'causal-model-replay' || !Array.isArray(recovery.records) || !recovery.records.length) {
      throw new Error('Invalid BTC causal gap recovery.');
    }
    const rows = new Map<number, GapRow>();
    let previous = 0;
    for (const tuple of recovery.records) {
      const row = { candleTime: tuple[0], availableAt: tuple[0] + HOUR, lower: tuple[1], upper: tuple[2] };
      if (tuple.length !== 3 || !validRow(row) || previous && row.candleTime !== previous + HOUR) throw new Error('Invalid recovery hour.');
      rows.set(row.candleTime, row); previous = row.candleTime;
    }
    const archived = new Map<number, GapRow>();
    const directory = path.join(this.directory(), 'gap-observations');
    let files: string[] = [];
    try { files = await fs.promises.readdir(directory); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    for (const file of files.filter(name => new RegExp(`^${MODEL.replace(/\./g, '\\.')}\\-\\d{4}-\\d{2}\\.jsonl$`).test(name)).sort()) {
      const text = await fs.promises.readFile(path.join(directory, file), 'utf8');
      for (const line of text.split('\n').filter(Boolean)) {
        const stored = JSON.parse(line);
        const { observedAt, fingerprint, previousFingerprint, kind, recordedAt, ...facts } = stored;
        if (stored.version !== 1 || stored.market !== 'BTC-USD' || stored.symbol !== 'BTCUSDT' ||
            stored.modelVersion !== MODEL || stored.accuracy !== 'causal-model-snapshot' ||
            !['first-seen', 'revision'].includes(kind) ||
            crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex') !== fingerprint) {
          throw new Error('Invalid BTC gap archive integrity.');
        }
        const candleTime = Date.parse(stored.candleStartedAt) / 1000;
        const row = { candleTime, availableAt: Date.parse(stored.candleClosedAt) / 1000,
          lower: stored.gap?.left ?? null, upper: stored.gap?.right ?? null };
        if (!validRow(row) || !Number.isFinite(Date.parse(observedAt)) || Date.parse(observedAt) < row.availableAt * 1000) {
          throw new Error('Invalid BTC archived gap hour.');
        }
        // A later revision never silently replaces the first recorded value.
        if (kind === 'first-seen' && !archived.has(candleTime)) archived.set(candleTime, row);
      }
    }
    for (const [time, row] of archived) rows.set(time, row);
    const cutoff = Math.floor(Date.now() / (HOUR * 1000)) * HOUR;
    return { symbol: 'BTCUSDT', venue: 'Binance Spot', historicalOnly: true,
      historicalAccuracy: 'causal-model-replay', modelVersion: MODEL,
      reconstructedAt: recovery.generatedAt, sourceCandleSha256: recovery.source?.sha256,
      records: [...rows.values()].filter(row => row.availableAt <= cutoff).sort((a, b) => a.candleTime - b.candleTime) };
  }
}
