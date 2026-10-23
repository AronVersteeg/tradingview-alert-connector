import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

type ArchiveAccuracy = 'causal-model-snapshot' | 'provider-observed-snapshot';
type ArchiveIdentity = { market: string; symbol: string; modelVersion: string; accuracy?: ArchiveAccuracy };
type GapSnapshot = {
  effectiveAt: string; observedAt: string; referencePrice: number;
  open?: number; high?: number; low?: number; close?: number; sourceHours: number;
  gap: { left: number; right: number; leftEdge?: { relativeCount?: number }; rightEdge?: { relativeCount?: number } } | null;
};
type ArchiveRow = ArchiveIdentity & {
  version: 1; accuracy: ArchiveAccuracy; kind: 'first-seen' | 'revision';
  candleStartedAt: string; candleClosedAt: string; observedAt: string; recordedAt: string;
  referencePrice: number; open?: number; high?: number; low?: number; close?: number; sourceHours: number;
  gap: { left: number; right: number; leftCount?: number; rightCount?: number } | null;
  fingerprint: string; previousFingerprint?: string;
};

// Independent of the rolling 500-frame replay. A later calculation can append
// a revision, but can never rewrite the original observation or invent live timing.
export class ReplicaGapArchive {
  private queue: Promise<void> = Promise.resolve();
  private indices = new Map<string, Map<string, string>>();
  private archiveStatus: { directory?: string; rowsWritten: number; revisionsWritten: number; lastSuccessAt?: string; lastError?: string } = {
    rowsWritten: 0, revisionsWritten: 0
  };

  status() { return { ...this.archiveStatus }; }

  append(directory: string, identity: ArchiveIdentity, snapshots: GapSnapshot[]): Promise<void> {
    // Copy primitives now: the rolling replay may advance before queued disk I/O.
    const captured = snapshots.map((snapshot) => ({
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
      const batches = new Map<string, typeof captured>();
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
        const file = path.join(archiveDirectory, `${identity.modelVersion}-${row.candleStartedAt.slice(0, 7)}.jsonl`);
        const batch = batches.get(file) || [];
        batch.push(row); batches.set(file, batch);
      }
      await fs.promises.mkdir(archiveDirectory, { recursive: true });
      for (const [file, rows] of batches) {
        let index = this.indices.get(file);
        let needsNewline = false;
        if (!index) {
          index = new Map();
          let text = '';
          try { text = await fs.promises.readFile(file, 'utf8'); }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
          needsNewline = Boolean(text && !text.endsWith('\n'));
          for (const line of text.split('\n').filter(Boolean)) {
            const stored = JSON.parse(line) as ArchiveRow;
            if (stored.version !== 1 || stored.market !== identity.market || stored.symbol !== identity.symbol || stored.modelVersion !== identity.modelVersion ||
                !stored.candleStartedAt || typeof stored.fingerprint !== 'string') throw new Error('Invalid existing gap archive; refusing to overwrite.');
            const { observedAt, fingerprint, previousFingerprint, kind, recordedAt, ...facts } = stored;
            if (crypto.createHash('sha256').update(JSON.stringify(facts)).digest('hex') !== fingerprint) {
              throw new Error('Gap archive fingerprint mismatch; refusing to overwrite.');
            }
            index.set(stored.candleStartedAt, stored.fingerprint);
          }
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
          if (additions.length) await fs.promises.appendFile(file,
            (needsNewline ? '\n' : '') + additions.map((row) => JSON.stringify(row)).join('\n') + '\n');
        } catch (error) { this.indices.delete(file); throw error; }
        this.indices.set(file, next);
        // A write batch spans at most a few months; older indexes can be reloaded.
        while (this.indices.size > 3) this.indices.delete(this.indices.keys().next().value);
        this.archiveStatus.rowsWritten += additions.length;
        this.archiveStatus.revisionsWritten += additions.filter((row) => row.kind === 'revision').length;
      }
      this.archiveStatus.lastSuccessAt = new Date().toISOString();
      this.archiveStatus.lastError = undefined;
    });
    this.queue = task.catch((error) => { this.archiveStatus.lastError = error instanceof Error ? error.message : String(error); });
    return task;
  }
}
