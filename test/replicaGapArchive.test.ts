import fs from 'fs';
import os from 'os';
import path from 'path';
import { ReplicaGapArchive } from '../src/services/replicaGapArchive';
import { OpenLiquidityV2ReplicaCollector } from '../src/services/openLiquidityV2Replica';

const identity = { market: 'BTC-USD', symbol: 'BTCUSDT', modelVersion: 'archive-test' };
const hour = 3_600_000, base = Date.parse('2026-08-19T00:00:00Z');
const snapshot = (index = 0) => ({ effectiveAt: new Date(base + index * hour).toISOString(),
  observedAt: '2026-10-23T12:00:00.000Z', referencePrice: 64000, open: 64000, high: 64500, low: 63000, close: 64100,
  sourceHours: 8760, gap: { left: 61000, right: 67000 } });

describe('Append-only causal-model gap archive', () => {
  let directory: string;
  const file = () => path.join(directory, 'gap-observations', 'archive-test-2026-08.jsonl');
  const rows = () => fs.readFileSync(file(), 'utf8').trim().split('\n').map((line) => JSON.parse(line));
  beforeEach(() => { directory = fs.mkdtempSync(path.join(os.tmpdir(), 'gap-archive-')); });
  afterEach(() => { jest.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true }); });

  test('retains first-seen model values after a restart without pretending they were live observations in August', async () => {
    const archive = new ReplicaGapArchive();
    await archive.append(directory, identity, [snapshot(), snapshot(1)]);
    const original = fs.readFileSync(file(), 'utf8');
    await new ReplicaGapArchive().append(directory, identity, [snapshot(), snapshot(1)]);
    expect(fs.readFileSync(file(), 'utf8')).toBe(original);
    expect(rows()[0]).toMatchObject({ kind: 'first-seen', accuracy: 'causal-model-snapshot',
      candleStartedAt: '2026-08-19T00:00:00.000Z', candleClosedAt: '2026-08-19T01:00:00.000Z',
      observedAt: '2026-10-23T12:00:00.000Z', gap: { left: 61000, right: 67000 } });
  });

  test('appends a corrected right edge as an explicit revision and never rewrites the earlier 67000', async () => {
    const archive = new ReplicaGapArchive();
    await archive.append(directory, identity, [snapshot()]);
    const original = fs.readFileSync(file(), 'utf8');
    await archive.append(directory, identity, [{ ...snapshot(), gap: { left: 61000, right: 87400 } }]);
    expect(fs.readFileSync(file(), 'utf8').startsWith(original)).toBe(true);
    expect(rows()[0].gap.right).toBe(67000);
    expect(rows()[1]).toMatchObject({ kind: 'revision', gap: { right: 87400 }, previousFingerprint: rows()[0].fingerprint });
    expect(archive.status()).toMatchObject({ rowsWritten: 2, revisionsWritten: 1 });
    await new ReplicaGapArchive().append(directory, identity, [{ ...snapshot(), gap: { left: 61000, right: 87400 } }]);
    expect(rows()).toHaveLength(2);
  });

  test('rolling replay eviction and repeated observedAt timestamps do not delete or duplicate history', async () => {
    const archive = new ReplicaGapArchive();
    await archive.append(directory, identity, [snapshot(), snapshot(1)]);
    await archive.append(directory, identity, [{ ...snapshot(1), observedAt: '2026-10-23T13:00:00.000Z' }, snapshot(2)]);
    expect(rows()).toHaveLength(3);
    expect(rows()[0].candleStartedAt).toBe(snapshot().effectiveAt);
  });

  test('records actual provider observations without fabricating historical OHLC or an on-time arrival', async () => {
    const archive = new ReplicaGapArchive();
    const { open, high, low, close, ...provider } = snapshot();
    await archive.append(directory, { ...identity, accuracy: 'provider-observed-snapshot' }, [provider]);
    expect(rows()[0]).toMatchObject({ accuracy: 'provider-observed-snapshot',
      observedAt: '2026-10-23T12:00:00.000Z', candleStartedAt: '2026-08-19T00:00:00.000Z' });
    expect(rows()[0].high).toBeUndefined();
    await new ReplicaGapArchive().append(directory, { ...identity, accuracy: 'provider-observed-snapshot' }, [provider]);
    expect(rows()).toHaveLength(1);
    await expect(archive.append(directory, identity, [provider])).rejects.toThrow('Invalid');
  });

  test('copies queued values and serializes concurrent writes', async () => {
    const archive = new ReplicaGapArchive(), first = snapshot();
    const a = archive.append(directory, identity, [first]);
    first.gap.right = 87400;
    const b = archive.append(directory, identity, [snapshot(1)]);
    await Promise.all([a, b]);
    expect(rows()).toHaveLength(2);
    expect(rows()[0].gap.right).toBe(67000);
  });

  test('refuses corrupt existing files and invalid/open snapshots without overwriting evidence', async () => {
    const archive = new ReplicaGapArchive();
    await archive.append(directory, identity, [snapshot()]);
    const damaged = fs.readFileSync(file(), 'utf8').replace('67000', '87400');
    fs.writeFileSync(file(), damaged);
    const fresh = new ReplicaGapArchive();
    await expect(fresh.append(directory, identity, [snapshot(1)])).rejects.toThrow('fingerprint');
    expect(fs.readFileSync(file(), 'utf8')).toBe(damaged);
    expect(fresh.status().lastError).toContain('fingerprint');
    await expect(archive.append(directory, identity, [{ ...snapshot(), effectiveAt: 'invalid' }])).rejects.toThrow('Invalid');
    await expect(archive.append(directory, identity, [{ ...snapshot(), observedAt: '2026-08-19T00:30:00Z' }])).rejects.toThrow('unclosed');
  });

  test('failed writes are visible and retries do not lose records from a prematurely committed index', async () => {
    const archive = new ReplicaGapArchive();
    const write = jest.spyOn(fs.promises, 'appendFile').mockRejectedValueOnce(new Error('disk full'));
    await expect(archive.append(directory, identity, [snapshot()])).rejects.toThrow('disk full');
    expect(archive.status().lastError).toBe('disk full');
    write.mockRestore();
    await archive.append(directory, identity, [snapshot()]);
    expect(rows()).toHaveLength(1);
    expect(archive.status().lastError).toBeUndefined();
  });

  test('collector archive failure leaves the existing replay persistence and execution path independent', async () => {
    const collector = new OpenLiquidityV2ReplicaCollector() as any;
    collector.historyDirectory = () => directory;
    collector.snapshots = [];
    collector.gapArchive.append = jest.fn().mockRejectedValue(new Error('archive unavailable'));
    const log = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    collector.persistHistoryLater();
    await collector.persistencePromise;
    expect(log).toHaveBeenCalledWith(expect.stringContaining('live execution unchanged'), expect.any(Object));
    expect(JSON.parse(fs.readFileSync(collector.historyFile(), 'utf8'))).toEqual([]);
    collector.stop();
  });
});
