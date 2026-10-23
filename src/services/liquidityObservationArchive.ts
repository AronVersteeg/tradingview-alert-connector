import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { readGapArchiveMonth } from './replicaGapArchive';

export type ObservationIdentity = { market: string; symbol: string; source: string; modelVersion: string;
  priceStepUsd?: number; cohortWindowHours?: number };
type Topic = 'source-candles' | 'intrusions';
type Evidence = { id: string; candleStartedAt: string; observedAt: string; evidenceType: string; data: any };
type Row = Evidence & ObservationIdentity & { version: 1; topic: Topic; fingerprint: string;
  kind: 'first-seen' | 'revision'; previousFingerprint?: string; recordedAt: string };

const digest = (value: any) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const MAX_PENDING_BYTES = 64 * 1024 * 1024;
let pendingBytes = 0;
export function observationTime(value: string): string {
  const timestamp = /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/.test(value) ? value.replace(' ', 'T') + 'Z' : value;
  const ms = Date.parse(timestamp);
  if (!Number.isFinite(ms) || ms % 3_600_000) throw new Error('Invalid liquidity observation candle.');
  return new Date(ms).toISOString();
}

function verify(row: Row, identity: ObservationIdentity, topic: Topic, previous?: string): void {
  const { fingerprint, previousFingerprint, kind, recordedAt, observedAt, ...facts } = row;
  if (row.version !== 1 || row.topic !== topic || Object.keys(identity).some(key => (row as any)[key] !== (identity as any)[key]) ||
      !row.id || observationTime(row.candleStartedAt) !== row.candleStartedAt || !Number.isFinite(Date.parse(observedAt)) ||
      !Number.isFinite(Date.parse(recordedAt)) || !['first-seen', 'revision'].includes(kind) ||
      kind === 'first-seen' && (previous !== undefined || previousFingerprint !== undefined) ||
      kind === 'revision' && (previous === undefined || previousFingerprint !== previous) ||
      digest(facts) !== fingerprint) throw new Error('Liquidity observation archive integrity failure.');
}

// Separate from bounded dashboard/trading state. Monthly journals retain original
// evidence and explicit revisions. Failures stay queued and retry without network.
export class LiquidityObservationArchive {
  private tail: Promise<void> = Promise.resolve();
  private pending = new Map<string, { topic: Topic; row: Evidence; bytes: number }>();
  private rejectedRecords = 0;
  private indices = new Map<string, { signatures: Map<string, string>; size: number; mtimeMs: number }>();
  private retry?: NodeJS.Timeout;
  private auditAfter = 0;
  private lastAudit: any;
  private lastAuditError?: string;
  private auditIndex = 0;
  private details = { rowsWritten: 0, revisionsWritten: 0, lastSuccessAt: undefined as string | undefined, lastError: undefined as string | undefined };
  constructor(readonly directory: string, readonly identity: ObservationIdentity) {
    if (!/^[a-zA-Z0-9._-]+$/.test(identity.modelVersion)) throw new Error('Invalid observation model identifier.');
  }
  status() { return { ...this.details, directory: path.join(this.directory, 'liquidity-observations'),
    pendingRecords: this.pending.size, retention: 'append-only; no automatic pruning', durableWrites: 'fsync',
    rejectedRecords: this.rejectedRecords, sharedPendingBytes: pendingBytes, sharedPendingLimitBytes: MAX_PENDING_BYTES,
    lastAudit: this.lastAudit, lastAuditError: this.lastAuditError }; }

  async flush(): Promise<void> {
    await this.tail;
    if (this.details.lastError) throw new Error(this.details.lastError);
  }

  close(): void { if (this.retry) clearTimeout(this.retry); this.retry = undefined; }

  async audit(month: string) {
    const gap = await readGapArchiveMonth(this.directory, { ...this.identity,
      accuracy: this.identity.source === 'decentrader' ? 'provider-observed-snapshot' : 'causal-model-snapshot' }, month);
    const candles = await this.read('source-candles', month), intrusions = await this.read('intrusions', month);
    const coverage = (rows: Array<{ candleStartedAt: string; kind: string }>, hourly: boolean) => {
      const hours = [...new Set(rows.filter(row => row.kind === 'first-seen').map(row => Date.parse(row.candleStartedAt)))].sort((a, b) => a - b);
      return { records: rows.length, recordedHours: hours.length,
        from: hours.length ? new Date(hours[0]).toISOString() : null,
        through: hours.length ? new Date(hours[hours.length - 1]).toISOString() : null,
        missingHoursInsideRecordedSpan: hourly && hours.length ? (hours[hours.length - 1] - hours[0]) / 3_600_000 + 1 - hours.length : null,
        fullMonthCoverageProven: false };
    };
    let freeBytes: number | null = null;
    try { const disk = await fs.promises.statfs(this.directory); freeBytes = disk.bavail * disk.bsize; } catch { /* Unknown is not zero. */ }
    return { identity: this.identity, month, integrity: 'verified',
      gaps: { missing: gap.missing, ...coverage(gap.rows, true) },
      candles: { missing: candles.missing, ...coverage(candles.rows, true) },
      intrusions: { missing: intrusions.missing, ...coverage(intrusions.rows, false) },
      freeBytes, lowSpace: freeBytes === null ? null : freeBytes < 128 * 1024 * 1024, writer: this.status() };
  }

  append(topic: Topic, evidence: Evidence[]): Promise<void> {
    // Capture now, before callers trim or update their in-memory records.
    const rows: Evidence[] = JSON.parse(JSON.stringify(evidence));
    const additions = new Map<string, { topic: Topic; row: Evidence; bytes: number }>();
    for (const row of rows) {
      const { observedAt, ...facts } = row;
      const key = `${topic}|${digest(facts)}`;
      if (!this.pending.has(key)) additions.set(key, { topic, row, bytes: Buffer.byteLength(JSON.stringify(row)) });
    }
    const bytes = [...additions.values()].reduce((sum, item) => sum + item.bytes, 0);
    if (pendingBytes + bytes > MAX_PENDING_BYTES) {
      this.rejectedRecords += additions.size;
      this.details.lastError = 'Archive backlog memory limit reached; new capture rejected, existing evidence retained.';
      console.error(this.details.lastError, this.identity);
      return Promise.reject(new Error(this.details.lastError));
    }
    for (const [key, item] of additions) { this.pending.set(key, item); pendingBytes += item.bytes; }
    if (!this.pending.size) return Promise.resolve();
    const run = this.tail.then(async () => {
      await new Promise<void>(resolve => setImmediate(resolve));
      for (const topic of ['source-candles', 'intrusions'] as Topic[]) {
        const batch = [...this.pending].filter(([, value]) => value.topic === topic);
        await this.write(topic, batch.map(([, value]) => value.row));
        for (const [key, item] of batch) { this.pending.delete(key); pendingBytes -= item.bytes; }
      }
      this.details.lastError = undefined;
      this.details.lastSuccessAt = new Date().toISOString();
      if (this.retry) clearTimeout(this.retry);
      this.retry = undefined;
      if (Date.now() >= this.auditAfter) {
        this.auditAfter = Date.now() + 3_600_000;
        try {
          const audit = await this.audit(new Date().toISOString().slice(0, 7));
          this.lastAudit = { at: new Date().toISOString(), month: audit.month, integrity: audit.integrity,
            gaps: audit.gaps, candles: audit.candles, intrusions: audit.intrusions,
            freeBytes: audit.freeBytes, lowSpace: audit.lowSpace };
          const olderMonths = (await this.months()).filter(month => month !== audit.month);
          if (olderMonths.length) {
            const historical = await this.audit(olderMonths[this.auditIndex++ % olderMonths.length]);
            this.lastAudit.historical = { month: historical.month, integrity: historical.integrity,
              gaps: historical.gaps, candles: historical.candles, intrusions: historical.intrusions };
          }
          this.lastAuditError = undefined;
          if (audit.lowSpace) console.warn('Liquidity archive persistent disk space low:', { ...this.identity, freeBytes: audit.freeBytes });
        } catch (error) {
          this.lastAuditError = error instanceof Error ? error.message : String(error);
          console.error('Liquidity archive integrity audit failed; files preserved:', { ...this.identity, error: this.lastAuditError });
        }
      }
    });
    this.tail = run.catch(error => {
      const message = error instanceof Error ? error.message : String(error);
      if (message !== this.details.lastError) console.error('Liquidity observation archive failed; trading unchanged:', { ...this.identity, error: message });
      this.details.lastError = message;
      if (!this.retry) {
        this.retry = setTimeout(() => { this.retry = undefined; void this.append(topic, []).catch(() => undefined); }, 60_000);
        this.retry.unref?.();
      }
    });
    return run;
  }

  private async write(topic: Topic, rows: Evidence[]): Promise<void> {
    const groups = new Map<string, Evidence[]>();
    for (const row of rows) {
      if (!row.id || observationTime(row.candleStartedAt) !== row.candleStartedAt || !Number.isFinite(Date.parse(row.observedAt)) || !row.data) {
        throw new Error('Invalid liquidity evidence.');
      }
      const file = path.join(this.directory, 'liquidity-observations', topic, `${this.identity.modelVersion}-${row.candleStartedAt.slice(0, 7)}.jsonl`);
      const group = groups.get(file) || []; group.push(row); groups.set(file, group);
    }
    for (const [file, batch] of groups) {
      await fs.promises.mkdir(path.dirname(file), { recursive: true });
      let stat: fs.Stats | undefined;
      try { stat = await fs.promises.stat(file); } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
      let cached = this.indices.get(file);
      let needsNewline = false;
      if (!cached || cached.size !== (stat?.size || 0) || cached.mtimeMs !== (stat?.mtimeMs || 0)) {
        const signatures = new Map<string, string>();
        const text = stat ? await fs.promises.readFile(file, 'utf8') : '';
        needsNewline = Boolean(text && !text.endsWith('\n'));
        for (const line of text.split('\n').filter(Boolean)) {
          const stored: Row = JSON.parse(line);
          verify(stored, this.identity, topic, signatures.get(stored.id));
          signatures.set(stored.id, stored.fingerprint);
        }
        cached = { signatures, size: stat?.size || 0, mtimeMs: stat?.mtimeMs || 0 };
      }
      const next = new Map(cached.signatures), additions: Row[] = [];
      for (const row of batch) {
        const { observedAt, ...evidence } = row;
        const facts = { ...this.identity, version: 1 as const, topic, ...evidence };
        const fingerprint = digest(facts), previousFingerprint = next.get(row.id);
        if (fingerprint === previousFingerprint) continue;
        additions.push({ ...facts, observedAt, fingerprint, previousFingerprint,
          kind: previousFingerprint ? 'revision' : 'first-seen', recordedAt: new Date().toISOString() });
        next.set(row.id, fingerprint);
      }
      {
        const handle = await fs.promises.open(file, 'a');
        try {
          if (additions.length) await handle.writeFile((needsNewline ? '\n' : '') + additions.map(row => JSON.stringify(row)).join('\n') + '\n');
          await handle.sync();
        } finally { await handle.close(); }
        stat = await fs.promises.stat(file);
      }
      this.indices.set(file, { signatures: next, size: stat?.size || 0, mtimeMs: stat?.mtimeMs || 0 });
      while (this.indices.size > 30) this.indices.delete(this.indices.keys().next().value);
      this.details.rowsWritten += additions.length;
      this.details.revisionsWritten += additions.filter(row => row.kind === 'revision').length;
      await new Promise<void>(resolve => setImmediate(resolve));
    }
  }

  private async months(): Promise<string[]> {
    const months = new Set<string>();
    for (const folder of ['gap-observations', 'liquidity-observations/source-candles', 'liquidity-observations/intrusions']) {
      let names: string[] = [];
      try { names = await fs.promises.readdir(path.join(this.directory, folder)); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
      for (const name of names) {
        const month = name.startsWith(this.identity.modelVersion + '-') && name.match(/-(\d{4}-\d{2})\.jsonl$/)?.[1];
        if (month) months.add(month);
      }
    }
    return [...months].sort();
  }

  async read(topic: Topic, month: string): Promise<{ rows: Row[]; integrity: 'verified'; missing: boolean }> {
    if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)) throw new Error('Use YYYY-MM for the archive month.');
    const file = path.join(this.directory, 'liquidity-observations', topic, `${this.identity.modelVersion}-${month}.jsonl`);
    let text: string;
    try { text = await fs.promises.readFile(file, 'utf8'); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { rows: [], integrity: 'verified', missing: true }; throw error; }
    const signatures = new Map<string, string>(), rows: Row[] = [];
    for (const line of text.split('\n').filter(Boolean)) {
      const row: Row = JSON.parse(line);
      verify(row, this.identity, topic, signatures.get(row.id));
      signatures.set(row.id, row.fingerprint); rows.push(row);
    }
    return { rows, integrity: 'verified', missing: false };
  }
}

const archives = new Map<string, LiquidityObservationArchive>();
export function liquidityObservationArchive(directory: string, identity: ObservationIdentity): LiquidityObservationArchive {
  const key = JSON.stringify([directory, identity]);
  let archive = archives.get(key);
  if (!archive) { archive = new LiquidityObservationArchive(directory, identity); archives.set(key, archive); }
  return archive;
}

export function decentraderObservationArchive(fallbackFile?: string): LiquidityObservationArchive {
  const file = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim() || fallbackFile ||
    path.join(process.cwd(), 'data', 'decentrader-gap-alert-state.json');
  return liquidityObservationArchive(path.dirname(file), { market: 'BTC-USD', symbol: 'BTCUSDT',
    source: 'decentrader', modelVersion: 'decentrader-btc-observed-v1' });
}

export async function archiveSourceCandles(archive: LiquidityObservationArchive, candles: Array<{
  timestampMs: number; closeTimeMs: number; open: number; high: number; low: number; close: number;
}>): Promise<void> {
  const observedAt = new Date().toISOString();
  return archive.append('source-candles', candles.map(candle => {
    if (!Number.isSafeInteger(candle.timestampMs) || candle.timestampMs % 3_600_000 || candle.closeTimeMs > Date.now() ||
        candle.closeTimeMs !== candle.timestampMs + 3_600_000 - 1 ||
        ![candle.open, candle.high, candle.low, candle.close].every(value => Number.isFinite(value) && value > 0) ||
        candle.high < Math.max(candle.open, candle.close) || candle.low > Math.min(candle.open, candle.close)) {
      throw new Error('Invalid closed source candle.');
    }
    const candleStartedAt = observationTime(new Date(candle.timestampMs).toISOString());
    return { id: candleStartedAt, candleStartedAt, observedAt, evidenceType: 'source-candle', data: { ...candle } };
  }));
}

export async function archiveIntrusionEvidence(archive: LiquidityObservationArchive, records: any[], imported = false): Promise<void> {
  return archive.append('intrusions', records.map(record => ({
    id: `${record.emailType || 'intrusion'}|${record.id || record.signature}`,
    candleStartedAt: observationTime(record.intrusionTimestamp || record.timestamp),
    observedAt: record.firstObservedAt || record.observedAt || new Date().toISOString(),
    evidenceType: imported ? 'stored-record-import' : 'recorded-intrusion',
    data: { signature: record.signature, timestamp: record.intrusionTimestamp || record.timestamp,
      sideCounts: record.sideCounts, direction: record.direction, alertPrice: record.alertPrice ?? record.price,
      gap: record.gap || record.previousGap, gapRevisions: record.gapRevisions,
      entrants: record.entrants || record.mapContext?.entrants,
      firstObservedAt: record.firstObservedAt, emailType: record.emailType,
      smtpSentAt: record.smtpSentAt, normalEmailSentAt: record.normalEmailSentAt, filteredEmailSentAt: record.filteredEmailSentAt,
      candleClosedAt: record.candleClosedAt || new Date(Date.parse(observationTime(record.intrusionTimestamp || record.timestamp)) + 3_600_000).toISOString(),
      afterCloseDelayMinutes: record.afterCloseDelayMinutes,
      detectionDelayMinutes: record.detectionDelayMinutes, processingDelayMinutes: record.processingDelayMinutes }
  })));
}
