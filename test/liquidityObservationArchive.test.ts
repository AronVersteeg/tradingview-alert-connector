import fs from 'fs';
import os from 'os';
import path from 'path';
import { LiquidityObservationArchive, archiveSourceCandles, archiveIntrusionEvidence } from '../src/services/liquidityObservationArchive';
import { ReplicaGapArchive, readGapArchiveMonth } from '../src/services/replicaGapArchive';
import { openLiquidityV2BtcCollector, openLiquidityV2EthCollector, openLiquidityV2InjCollector, openLiquidityV2SolCollector,
  openLiquidityV2ZecCollector, openLiquidityV2GoldCollector, openLiquidityV2SilverCollector, buildReplicaSnapshots } from '../src/services/openLiquidityV2Replica';

const base = Date.parse('2026-08-19T00:00:00Z'), hour = 3_600_000;
const identity = { market: 'BTC-USD', symbol: 'BTCUSDT', source: 'binance-spot', modelVersion: 'archive-test' };
const candle = (index: number) => ({ timestampMs: base + index * hour, closeTimeMs: base + (index + 1) * hour - 1,
  open: 64000, high: 65000, low: 63000, close: 64500 });
const intrusion = (right = 67000) => ({ signature: 'example|L|10|61000:1', timestamp: '2026-08-19 00:00:00',
  observedAt: '2026-08-19T02:10:00Z', sideCounts: '1 left edge', gap: { left: 61000, right, width: right - 61000 },
  entrants: [{ side: 'L', leverage: 10, price: 61000, count: 1 }], normalEmailSentAt: '2026-08-19T02:10:02Z' });

describe('Durable, independent liquidity evidence archive', () => {
  let directory: string, archive: LiquidityObservationArchive;
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'liquidity-observations-'));
    archive = new LiquidityObservationArchive(directory, identity);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(async () => {
    await archive.flush().catch(() => undefined); archive.close();
    jest.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true });
  });

  test('keeps source candles across restarts, duplicates and a rolling display eviction', async () => {
    await archiveSourceCandles(archive, [candle(0), candle(1)]);
    const fresh = new LiquidityObservationArchive(directory, identity);
    await archiveSourceCandles(fresh, [candle(1), candle(2)]);
    const result = await archive.read('source-candles', '2026-08');
    expect(result.rows).toHaveLength(3);
    expect(result.rows[0]).toMatchObject({ kind: 'first-seen', evidenceType: 'source-candle', data: candle(0) });
    expect(Date.parse(result.rows[0].observedAt)).toBeGreaterThan(base + hour);
    fresh.close();
  });

  test('retains original candle and intrusion evidence, appending only explicit corrections', async () => {
    await archiveSourceCandles(archive, [candle(0)]);
    await archiveSourceCandles(archive, [{ ...candle(0), high: 66000 }]);
    await archiveIntrusionEvidence(archive, [intrusion()]);
    await archiveIntrusionEvidence(archive, [intrusion(87400)]);
    const rows = (await archive.read('intrusions', '2026-08')).rows;
    expect(rows.map(row => [row.kind, row.data.gap.right])).toEqual([['first-seen', 67000], ['revision', 87400]]);
    expect(rows[1].previousFingerprint).toBe(rows[0].fingerprint);
    expect(rows[0].data.normalEmailSentAt).toBe('2026-08-19T02:10:02Z');
    expect((await archive.read('source-candles', '2026-08')).rows.map(row => row.data.high)).toEqual([65000, 66000]);
  });

  test('copies queued evidence before callers change or prune their state', async () => {
    const record = intrusion();
    const pending = archiveIntrusionEvidence(archive, [record]);
    record.gap.right = 87400; record.entrants[0].price = 62000;
    await pending;
    const row = (await archive.read('intrusions', '2026-08')).rows[0];
    expect(row.data.gap.right).toBe(67000); expect(row.data.entrants[0].price).toBe(61000);
  });

  test('disk failure retains deduplicated original evidence and retries even without another intrusion', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] });
    try {
      const open = jest.spyOn(fs.promises, 'open').mockRejectedValue(new Error('disk full'));
      await expect(archiveIntrusionEvidence(archive, [intrusion()])).rejects.toThrow('disk full');
      await expect(archiveIntrusionEvidence(archive, [intrusion()])).rejects.toThrow('disk full');
      expect(archive.status()).toMatchObject({ pendingRecords: 1, lastError: 'disk full' });
      open.mockRestore();
      await jest.advanceTimersByTimeAsync(60_000);
      await archive.flush();
      expect(archive.status()).toMatchObject({ pendingRecords: 0, lastError: undefined });
      expect((await archive.read('intrusions', '2026-08')).rows).toHaveLength(1);
    } finally { jest.useRealTimers(); }
  });

  test('audits actual coverage without filling missing hours or claiming a full provider month', async () => {
    await archiveSourceCandles(archive, [candle(0), candle(2)]);
    const gaps = new ReplicaGapArchive();
    await gaps.append(directory, identity, [0, 2].map(index => ({ effectiveAt: new Date(base + index * hour).toISOString(),
      observedAt: new Date().toISOString(), referencePrice: 64000, open: 64000, high: 65000, low: 63000, close: 64500,
      sourceHours: 8760, gap: { left: 61000, right: 67000 } })));
    const audit = await archive.audit('2026-08');
    expect(audit.candles).toMatchObject({ recordedHours: 2, missingHoursInsideRecordedSpan: 1, fullMonthCoverageProven: false });
    expect(audit.gaps.missingHoursInsideRecordedSpan).toBe(1);
    expect(audit.intrusions.missing).toBe(true);
    expect(audit.freeBytes).toBeGreaterThan(0); gaps.close();
  });

  test('detects corruption and a broken revision chain without replacing evidence', async () => {
    await archiveIntrusionEvidence(archive, [intrusion()]);
    const file = path.join(directory, 'liquidity-observations/intrusions/archive-test-2026-08.jsonl');
    const original = fs.readFileSync(file, 'utf8'), corrupt = original.replace('67000', '87400');
    fs.writeFileSync(file, corrupt);
    await expect(archive.audit('2026-08')).rejects.toThrow('integrity');
    expect(fs.readFileSync(file, 'utf8')).toBe(corrupt);
    fs.writeFileSync(file, original);
  });

  test('rejects open/invalid candles, other identities and unsafe month paths', async () => {
    await expect(archiveSourceCandles(archive, [{ ...candle(0), closeTimeMs: Date.now() + hour }])).rejects.toThrow('closed');
    await expect(archiveSourceCandles(archive, [{ ...candle(0), high: 1 }])).rejects.toThrow('closed');
    await archiveIntrusionEvidence(archive, [intrusion()], true);
    expect((await archive.read('intrusions', '2026-08')).rows[0].evidenceType).toBe('stored-record-import');
    const wrong = new LiquidityObservationArchive(directory, { ...identity, market: 'ETH-USD' });
    await expect(wrong.read('intrusions', '2026-08')).rejects.toThrow('integrity'); wrong.close();
    await expect(archive.read('intrusions', '../2026-08')).rejects.toThrow('YYYY-MM');
  });

  test('reproduces archived gap edges from original archived candles after a restart', async () => {
    const source = Array.from({ length: 20 }, (_, index) => candle(index));
    const options = { frameLimit: 10, cohortWindowHours: 12, modelVersion: identity.modelVersion };
    const snapshots = buildReplicaSnapshots(source, options);
    const gaps = new ReplicaGapArchive();
    await gaps.append(directory, identity, snapshots);
    await archiveSourceCandles(archive, source);
    const restarted = new LiquidityObservationArchive(directory, identity);
    const original = (await restarted.read('source-candles', '2026-08')).rows.filter(row => row.kind === 'first-seen').map(row => row.data);
    const replayed = buildReplicaSnapshots(original, options);
    const saved = (await readGapArchiveMonth(directory, identity, '2026-08')).rows;
    expect(replayed.map(row => [row.effectiveAt, row.gap?.left, row.gap?.right]))
      .toEqual(saved.map(row => [row.candleStartedAt, row.gap?.left, row.gap?.right]));
    await archiveSourceCandles(restarted, [{ ...candle(20), high: 88000, low: 58000 }]);
    expect((await readGapArchiveMonth(directory, identity, '2026-08')).rows).toEqual(saved);
    restarted.close(); gaps.close();
  });

  test('bounds outage memory explicitly without silently deleting queued originals', async () => {
    await archiveIntrusionEvidence(archive, [intrusion()]);
    const byteLength = jest.spyOn(Buffer, 'byteLength').mockReturnValueOnce(65 * 1024 * 1024);
    await expect(archiveIntrusionEvidence(archive, [intrusion(87400)])).rejects.toThrow('memory limit');
    byteLength.mockRestore();
    expect((await archive.read('intrusions', '2026-08')).rows).toHaveLength(1);
    expect(archive.status().rejectedRecords).toBe(1);
    await archiveIntrusionEvidence(archive, [intrusion(87400)]);
    expect((await archive.read('intrusions', '2026-08')).rows[0].data.gap.right).toBe(67000);
    expect(archive.status().lastError).toBeUndefined();
  });

  test('every pair has its own source identity, model parameters and archive regardless of auto trading', () => {
    const collectors = [openLiquidityV2BtcCollector, openLiquidityV2EthCollector, openLiquidityV2InjCollector,
      openLiquidityV2SolCollector, openLiquidityV2ZecCollector, openLiquidityV2GoldCollector, openLiquidityV2SilverCollector];
    const locations = collectors.map(collector => collector.observationArchive());
    expect(new Set(locations.map(location => location.directory)).size).toBe(7);
    expect(locations.map(location => location.identity.market)).toEqual(['BTC-USD', 'ETH-USD', 'INJ-USD', 'SOL-USD', 'ZEC-USD', 'PAXG-USD', 'XAG-USD']);
    expect(locations.every(location => location.identity.cohortWindowHours === 8760 && location.identity.priceStepUsd! > 0)).toBe(true);
    expect(locations.slice(-2).every(location => location.identity.source === 'binance-futures')).toBe(true);
  });
});
