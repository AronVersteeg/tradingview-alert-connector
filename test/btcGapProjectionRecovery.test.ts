import fs from 'fs';
import os from 'os';
import path from 'path';
import { BtcGapProjectionRecovery } from '../src/services/btcGapProjectionRecovery';
import { ReplicaGapArchive } from '../src/services/replicaGapArchive';
import { createBtcGapProjectionHistoryRouter } from '../src/controllers/btcGapProjectionHistory';

const model = 'binance-spot-liquidation-cohorts-v2.4';
const start = Date.parse('2026-08-19T14:00:00Z') / 1000;
describe('BTC display-only causal recovery', () => {
  let directory: string, recoveryFile: string;
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-gap-recovery-'));
    recoveryFile = path.join(directory, 'recovery.json');
    fs.writeFileSync(recoveryFile, JSON.stringify({ version: 1, symbol: 'BTCUSDT', modelVersion: model,
      historicalAccuracy: 'causal-model-replay', records: [[start, 61000, 67000], [start + 3600, 61600, 69500]] }));
  });
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }));

  test('restores both edges, reuses concurrent reads and never imports other model/provider archives', async () => {
    const read = new BtcGapProjectionRecovery(() => directory, recoveryFile);
    const [a, b] = await Promise.all([read.read(), read.read()]);
    expect(a).toBe(b);
    expect(a).toMatchObject({ venue: 'Binance Spot', historicalAccuracy: 'causal-model-replay', modelVersion: model });
    expect(a.records[0]).toEqual({ candleTime: start, availableAt: start + 3600, lower: 61000, upper: 67000 });
    expect(await read.read()).toBe(a);
    fs.mkdirSync(path.join(directory, 'gap-observations'));
    fs.writeFileSync(path.join(directory, 'gap-observations', 'decentrader-btc-observed-v1-2026-08.jsonl'), 'unrelated');
    expect((await new BtcGapProjectionRecovery(() => directory, recoveryFile).read()).records).toEqual(a.records);
  });

  test('extends from persisted first observations, preserves missing hours and does not adopt later revisions', async () => {
    const archive = new ReplicaGapArchive();
    const snapshot = (index: number, right: number) => ({ effectiveAt: new Date((start + index * 3600) * 1000).toISOString(),
      observedAt: new Date().toISOString(), referencePrice: 64000, open: 64000, high: 65000, low: 63000, close: 64000,
      sourceHours: 8760, gap: { left: 61000, right } });
    const identity = { market: 'BTC-USD', symbol: 'BTCUSDT', modelVersion: model };
    await archive.append(directory, identity, [snapshot(1, 70000), snapshot(4, 71000)]);
    await archive.append(directory, identity, [snapshot(1, 87400)]);
    const rows = (await new BtcGapProjectionRecovery(() => directory, recoveryFile).read()).records;
    expect(rows.map(row => [row.candleTime, row.upper])).toEqual([[start, 67000], [start + 3600, 70000], [start + 14400, 71000]]);
    const file = path.join(directory, 'gap-observations', `${model}-2026-08.jsonl`);
    fs.appendFileSync(file, '{truncated');
    await expect(new BtcGapProjectionRecovery(() => directory, recoveryFile).read()).rejects.toThrow();
  });

  test('rejects incomplete recovery hours and does not call exchange APIs', async () => {
    const data = JSON.parse(fs.readFileSync(recoveryFile, 'utf8'));
    data.records[1][0] += 3600;
    fs.writeFileSync(recoveryFile, JSON.stringify(data));
    await expect(new BtcGapProjectionRecovery(() => directory, recoveryFile).read()).rejects.toThrow('hour');
  });

  test('shipped recovery has a full warmup, contiguous year, and the August right edge is not the October survivor', async () => {
    const shipped = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'public/data/btc-gap-recovery-v1.json'), 'utf8'));
    expect(shipped.source.candles).toBe(shipped.records.length + 8760);
    expect(shipped.source.sha256).toMatch(/^[0-9a-f]{64}$/);
    const rows = (await new BtcGapProjectionRecovery(() => directory).read()).records;
    expect(rows.length).toBeGreaterThanOrEqual(8760);
    expect(rows.find(row => row.candleTime === start)).toMatchObject({ lower: 61000, upper: 67000 });
    expect(rows.at(-1)).toMatchObject({ lower: 79400, upper: 87400 });
  });

  test('separate GET recovery route has no execution callbacks and rejects other markets', async () => {
    const read = jest.fn(() => new BtcGapProjectionRecovery(() => directory, recoveryFile).read());
    const router = createBtcGapProjectionHistoryRouter(() => { throw new Error('Do not use current active flags'); }, read);
    const route = (router as any).stack[1].route;
    expect(route.path).toBe('/open-liquidity/v2/gap-projection-history');
    expect(route.methods).toEqual({ get: true });
    const res: any = { code: 200, setHeader: jest.fn(), status(code: number) { this.code = code; return this; }, json: jest.fn() };
    await route.stack[0].handle({ query: { market: 'ETH-USD' } }, res);
    expect(res.code).toBe(400); expect(read).not.toHaveBeenCalled();
    await route.stack[0].handle({ query: { market: 'BTC-USD' } }, res);
    expect(read).toHaveBeenCalledTimes(1);
    expect(res.json).toHaveBeenLastCalledWith(expect.objectContaining({ historicalAccuracy: 'causal-model-replay' }));
  });
});
