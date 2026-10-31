import fs from 'fs';
import os from 'os';
import path from 'path';
import { PurpleLineContinuity } from '../src/services/purpleLineContinuity';
import { freshEthGapDailySignal } from '../src/services/ethGapDailyEntry';

describe('durable, short-interruption purple continuity', () => {
  const close = Date.parse('2026-10-31T14:00:00Z');
  let directory: string, file: string, active: boolean, monitors: PurpleLineContinuity[];
  const make = (market: 'ETH-USD' | 'SOL-USD' = 'SOL-USD') => {
    const monitor = new PurpleLineContinuity(market, file, () => active); monitors.push(monitor); return monitor;
  };
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(close - 30_000);
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'purple-continuity-')); file = path.join(directory, 'lease.json');
    active = true; monitors = []; jest.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => {
    monitors.forEach(m => m.stop()); jest.restoreAllMocks(); jest.useRealTimers(); fs.rmSync(directory, { recursive: true, force: true });
  });
  test('a crash over candle close preserves prior activation, without relaxing the five-minute signal window', () => {
    const previous = make(); previous.start(); const activation = previous.activation()!; previous.stop();
    jest.setSystemTime(close + 5000); const restarted = make(); restarted.start();
    expect(restarted.activation()).toBe(activation);
    expect(restarted.snapshot()).toMatchObject({ ready: true, resumedAfterRestart: true });
    const signal: any = { time: close / 1000 - 3600, closedAt: close / 1000 };
    expect(freshEthGapDailySignal(signal, restarted.activation()!)).toBe(true);
    jest.setSystemTime(close + 300001);
    expect(freshEthGapDailySignal(signal, activation)).toBe(false);
  });
  test.each(['missing', 'corrupt', 'wrong-pair', 'wrong-model', 'disabled', 'future', 'old', 'oversized'])('does not resume unproven continuity: %s', kind => {
    const previous = make(); previous.start(); previous.stop();
    const stored = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (kind === 'missing') fs.unlinkSync(file);
    else if (kind === 'corrupt') fs.writeFileSync(file, '{broken');
    else if (kind === 'oversized') fs.writeFileSync(file, ' '.repeat(5000));
    else {
      if (kind === 'wrong-pair') stored.market = 'ETH-USD';
      if (kind === 'wrong-model') stored.modelVersion = 'wrong';
      if (kind === 'disabled') stored.active = false;
      if (kind === 'future') stored.observedAt = new Date(close + 60_000).toISOString();
      if (kind === 'old') stored.observedAt = stored.activatedAt = new Date(close - 600_000).toISOString();
      fs.writeFileSync(file, JSON.stringify(stored));
    }
    jest.setSystemTime(close + 5000); const restarted = make(); restarted.start();
    expect(restarted.activation()).toBe(new Date(close + 5000).toISOString());
    expect(restarted.snapshot().resumedAfterRestart).toBe(false);
    expect(freshEthGapDailySignal({ time: close / 1000 - 3600, closedAt: close / 1000 } as any, restarted.activation()!)).toBe(false);
  });
  test('a deliberate disable invalidates continuity even if re-enabled within one minute', () => {
    const monitor = make(); monitor.start(); const before = monitor.activation();
    active = false; monitor.heartbeat(); expect(monitor.activation()).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).active).toBe(false);
    jest.setSystemTime(close + 5000); active = true; monitor.heartbeat();
    expect(monitor.activation()).not.toBe(before); expect(monitor.snapshot().resumedAfterRestart).toBe(false);
  });
  test('only a tiny heartbeat is persisted every minute and stopping leaves no timer', () => {
    const monitor = make(); monitor.start(); const write = jest.spyOn(fs, 'writeFileSync');
    jest.advanceTimersByTime(120_000); expect(write).toHaveBeenCalledTimes(2);
    expect(fs.statSync(file).size).toBeLessThan(4096);
    monitor.stop(); jest.advanceTimersByTime(120_000); expect(write).toHaveBeenCalledTimes(2);
  });
  test('failed durable persistence never grants resumed eligibility or damages the last valid lease', () => {
    const previous = make(); previous.start(); previous.stop(); const before = fs.readFileSync(file, 'utf8');
    jest.setSystemTime(close + 5000);
    jest.spyOn(fs, 'fsyncSync').mockImplementation(() => { throw new Error('disk unavailable'); });
    const restarted = make(); restarted.start(); expect(restarted.activation()).toBeUndefined();
    expect(restarted.snapshot()).toMatchObject({ ready: false, reason: 'CONTINUITY_WRITE_FAILED' });
    expect(fs.readFileSync(file, 'utf8')).toBe(before);
  });
});
