import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { jsonlLines, jsonlNeedsNewline } from './jsonlFile';

type ArchiveAccuracy = 'causal-model-snapshot' | 'provider-observed-snapshot';
export type ArchiveIdentity = { market: string; symbol: string; modelVersion: string; accuracy?: ArchiveAccuracy };
type GapSnapshot = {
  effectiveAt: string; observedAt: string; referencePrice: number;
  open?: number; high?: number; low?: number; close?: number; sourceHours: number;
  gap: { left: number; right: number; leftEdge?: { relativeCount?: number }; rightEdge?: { relativeCount?: number } } | null;
};
export type ArchiveRow = ArchiveIdentity & {
  version: 1; accuracy: ArchiveAccuracy; kind: 'first-seen' | 'revision';
  candleStartedAt: string; candleClosedAt: string; observedAt: string; recordedAt: string;
  referencePrice: number; open?: number; high?: number; low?: number; close?: number; sourceHours: number;
  gap: { left: number; right: number; leftCount?: number; rightCount?: number } | null;
  fingerprint: string; previousFingerprint?: string;
};
type CapturedArchiveRow = Omit<ArchiveRow, 'fingerprint' | 'previousFingerprint' | 'kind' | 'recordedAt'>;

export function verifiedGapArchiveRows(text: string, identity: ArchiveIdentity): ArchiveRow[] {
  const rows: ArchiveRow[] = [], latest = new Map<string, string>();
  for (const line of text.split('\n').filter(Boolean)) {
    const stored = JSON.parse(line) as ArchiveRow;
    verifyGapArchiveRow(stored, identity, latest);
    rows.push(stored);
  }
  return rows;
}

function verifyGapArchiveRow(stored: ArchiveRow, identity: ArchiveIdentity, latest: Map<string, string>): void {
  const { observedAt, fingerprint, previousFingerprint, kind, recordedAt, ...facts } = stored;
  const previous = latest.get(stored.candleStartedAt);
  if (stored.version !== 1 || stored.market !== identity.market || stored.symbol !== identity.symbol ||
      stored.modelVersion !== identity.modelVersion || identity.accuracy && stored.accuracy !== identity.accuracy ||
      !['causal-model-snapshot', 'provider-observed-snapshot'].includes(stored.accuracy) ||
      !Number.isFinite(Date.parse(stored.candleStartedAt)) || Date.parse(stored.candleStartedAt) % 3_600_000 ||
      Date.parse(stored.candleClosedAt) !== Date.parse(stored.candleStartedAt) + 3_600_000 ||
      !Number.isFinite(Date.parse(observedAt)) || Date.parse(observedAt) < Date.parse(stored.candleClosedAt) ||
      !['first-seen', 'revision'].includes(kind) ||
      kind === 'first-seen' && (previous !== undefined || previousFingerprint !== undefined) ||
      kind === 'revision' && (previous === undefined || previousFingerprint !== previous) ||
      crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex') !== fingerprint) {
    throw new Error('Gap archive fingerprint or revision-chain mismatch; refusing to overwrite.');
  }
  latest.set(stored.candleStartedAt, fingerprint);
}

async function scanGapArchiveFile(file: string, identity: ArchiveIdentity, visit: (row: ArchiveRow) => void): Promise<void> {
  const latest = new Map<string, string>();
  let count = 0;
  for (const line of jsonlLines(file)) {
    const row = JSON.parse(line) as ArchiveRow;
    verifyGapArchiveRow(row, identity, latest);
    visit(row);
    if (++count % 128 === 0) await new Promise<void>(resolve => setImmediate(resolve));
  }
}

export async function readGapArchiveMonth(directory: string, identity: ArchiveIdentity, month: string, visit?: (row: ArchiveRow) => void) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month) || !/^[a-zA-Z0-9._-]+$/.test(identity.modelVersion)) throw new Error('Invalid gap archive month or model.');
  const rows: ArchiveRow[] = [];
  try { await scanGapArchiveFile(path.join(directory, 'gap-observations', `${identity.modelVersion}-${month}.jsonl`), identity, visit || (row => rows.push(row))); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { rows: [], integrity: 'verified' as const, missing: true }; throw error; }
  return { rows, integrity: 'verified' as const, missing: false };
}

// Independent of the rolling 500-frame replay. A later calculation can append
// a revision, but can never rewrite the original observation or invent live timing.
export class ReplicaGapArchive {
  private queue: Promise<void> = Promise.resolve();
  private indices = new Map<string, Map<string, string>>();
  private fileVersions = new Map<string, { size: number; mtimeMs: number }>();
  private pending = new Map<string, CapturedArchiveRow>();
  private retry?: NodeJS.Timeout;
  private rejectedRecords = 0;
  private archiveStatus: { directory?: string; rowsWritten: number; revisionsWritten: number; lastSuccessAt?: string; lastError?: string } = {
    rowsWritten: 0, revisionsWritten: 0
  };

  status() { return { ...this.archiveStatus, pendingRecords: this.pending.size,
    pendingLimitRecords: 10_000, rejectedRecords: this.rejectedRecords,
    retention: 'append-only; no automatic pruning', durableWrites: 'fsync' }; }

  close(): void { if (this.retry) clearTimeout(this.retry); this.retry = undefined; }

  append(directory: string, identity: ArchiveIdentity, snapshots: GapSnapshot[]): Promise<void> {
    // Copy primitives now: the rolling replay may advance before queued disk I/O.
    const captured: CapturedArchiveRow[] = snapshots.map((snapshot) => ({
      ...identity, version: 1 as const, accuracy: identity.accuracy || 'causal-model-snapshot' as ArchiveAccuracy,
      candleStartedAt: snapshot.effectiveAt,
      candleClosedAt: Number.isFinite(Date.parse(snapshot.effectiveAt))
        ? new Date(Date.parse(snapshot.effectiveAt) + 3_600_000).toISOString() : '',
      observedAt: snapshot.observedAt, referencePrice: snapshot.referencePrice,
      open: snapshot.open, high: snapshot.high, low: snapshot.low, close: snapshot.close,
      sourceHours: snapshot.sourceHours,
      gap: snapshot.gap ? { left: snapshot.gap.left, right: snapshot.gap.right,
        leftCount: snapshot.gap.leftEdge?.relativeCount, rightCount: snapshot.gap.rightEdge?.relativeCount } : null
    }));
    const task = this.queue.then(async () => {
      const archiveDirectory = path.join(directory, 'gap-observations');
      this.archiveStatus.directory = archiveDirectory;
      if (!/^[a-zA-Z0-9._-]+$/.test(identity.modelVersion)) throw new Error('Invalid gap archive model version.');
      const batches = new Map<string, CapturedArchiveRow[]>();
      for (const row of captured) {
        const started = Date.parse(row.candleStartedAt);
        if (!Number.isFinite(started) || started % 3_600_000 ||
            !Number.isFinite(Date.parse(row.observedAt)) || Date.parse(row.observedAt) < started + 3_600_000 ||
            !(Number.isFinite(row.referencePrice) && row.referencePrice > 0) ||
            ![row.open, row.high, row.low, row.close].every((value) => value === undefined || Number.isFinite(value) && value > 0) ||
            row.accuracy === 'causal-model-snapshot' && [row.open, row.high, row.low, row.close].some((value) => value === undefined) ||
            !Number.isInteger(row.sourceHours) || row.sourceHours < 1 ||
            row.gap && !(Number.isFinite(row.gap.left) && Number.isFinite(row.gap.right) && row.gap.left > 0 && row.gap.right > row.gap.left)) {
          throw new Error('Invalid or unclosed gap archive snapshot.');
        }
        const { observedAt, ...facts } = row;
        const key = crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex');
        if (!this.pending.has(key)) {
          if (this.pending.size >= 10_000) {
            this.rejectedRecords += 1;
            throw new Error('Gap archive backlog limit reached; new capture rejected, existing evidence retained.');
          }
          this.pending.set(key, row);
        }
      }
      for (const row of this.pending.values()) {
        const file = path.join(archiveDirectory, `${row.modelVersion}-${row.candleStartedAt.slice(0, 7)}.jsonl`);
        const batch = batches.get(file) || [];
        batch.push(row); batches.set(file, batch);
      }
      await fs.promises.mkdir(archiveDirectory, { recursive: true });
      for (const [file, rows] of batches) {
        let index = this.indices.get(file);
        let stat: fs.Stats | undefined;
        try { stat = await fs.promises.stat(file); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        const fileVersion = this.fileVersions.get(file);
        if (fileVersion?.size !== (stat?.size || 0) || fileVersion?.mtimeMs !== (stat?.mtimeMs || 0)) index = undefined;
        let needsNewline = false;
        if (!index) {
          index = new Map();
          try {
            needsNewline = jsonlNeedsNewline(file);
            await scanGapArchiveFile(file, identity, stored => index!.set(stored.candleStartedAt, stored.fingerprint));
          }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        }
        const next = new Map(index), additions: ArchiveRow[] = [];
        for (const row of rows) {
          const { observedAt, ...facts } = row;
          const fingerprint = crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex');
          const previousFingerprint = next.get(row.candleStartedAt);
          if (previousFingerprint === fingerprint) continue;
          additions.push({ ...row, fingerprint, previousFingerprint,
            kind: previousFingerprint ? 'revision' : 'first-seen', recordedAt: new Date().toISOString() });
          next.set(row.candleStartedAt, fingerprint);
        }
        try {
          {
            const handle = await fs.promises.open(file, 'a');
            try {
              if (additions.length) await handle.writeFile((needsNewline ? '\n' : '') + additions.map((row) => JSON.stringify(row)).join('\n') + '\n');
              await handle.sync();
            } finally { await handle.close(); }
          }
        } catch (error) { this.indices.delete(file); throw error; }
        this.indices.set(file, next);
        stat = await fs.promises.stat(file);
        this.fileVersions.set(file, { size: stat.size, mtimeMs: stat.mtimeMs });
        for (const row of rows) {
          const { observedAt, ...facts } = row;
          this.pending.delete(crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex'));
        }
        // A write batch spans at most a few months; older indexes can be reloaded.
        while (this.indices.size > 3) this.indices.delete(this.indices.keys().next().value);
        this.archiveStatus.rowsWritten += additions.length;
        this.archiveStatus.revisionsWritten += additions.filter((row) => row.kind === 'revision').length;
      }
      this.archiveStatus.lastSuccessAt = new Date().toISOString();
      this.archiveStatus.lastError = undefined;
      if (this.retry) clearTimeout(this.retry);
      this.retry = undefined;
    });
    this.queue = task.catch((error) => {
      this.archiveStatus.lastError = error instanceof Error ? error.message : String(error);
      if (this.pending.size && !this.retry) {
        this.retry = setTimeout(() => { this.retry = undefined; void this.append(directory, identity, []).catch(() => undefined); }, 60_000);
        this.retry.unref?.();
      }
    });
    return task;
  }
}
