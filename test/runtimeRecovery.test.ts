import { EventEmitter } from 'events';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { execFileSync } from 'child_process';
import { RuntimeHealth } from '../src/services/runtimeHealth';
const { createSupervisor } = require('../scripts/start-server.cjs');

describe('single-child trading service supervision', () => {
  let children: any[], spawn: jest.Mock, exit: jest.Mock, log: jest.Mock;
  let supervisor: any;
  beforeEach(() => {
    jest.useFakeTimers(); jest.setSystemTime(0);
    children = []; exit = jest.fn(); log = jest.fn();
    spawn = jest.fn(() => { const child: any = new EventEmitter(); child.pid = children.length + 10;
      child.kill = jest.fn(); children.push(child); return child; });
    supervisor = createSupervisor({ spawn, exit, log, file: '/fixture-only.cjs', env: { SAFE_SETTING: 'kept' } });
  });
  afterEach(() => { jest.clearAllTimers(); jest.useRealTimers(); });

  test('starts exactly one child with inherited settings and never changes trading switches', () => {
    supervisor.start(); supervisor.start();
    expect(spawn).toHaveBeenCalledTimes(1);
    expect(spawn).toHaveBeenCalledWith(process.execPath, ['/fixture-only.cjs'], expect.objectContaining({
      env: { SAFE_SETTING: 'kept', NODE_ENV: 'production', SERVICE_SUPERVISED: '1', SERVICE_RESTART_COUNT: '0' }
    }));
    jest.advanceTimersByTime(86_400_000);
    expect(spawn).toHaveBeenCalledTimes(1); expect(children[0].kill).not.toHaveBeenCalled();
  });
  test.each([[134, null], [1, null], [0, null], [null, 'SIGABRT']])('recovers code=%s signal=%s only after the old process closes', (code, signal) => {
    supervisor.start(); children[0].emit('exit', code, signal);
    jest.advanceTimersByTime(3000); expect(spawn).toHaveBeenCalledTimes(1);
    children[0].emit('close', code, signal);
    jest.advanceTimersByTime(1999); expect(spawn).toHaveBeenCalledTimes(1);
    jest.advanceTimersByTime(1); expect(spawn).toHaveBeenCalledTimes(2);
    expect(spawn.mock.calls[1][2].env.SERVICE_RESTART_COUNT).toBe('1');
  });
  test('repeated crashes back off to sixty seconds, resetting only after ten minutes healthy', () => {
    supervisor.start();
    for (const delay of [2000, 4000, 8000, 16000, 32000, 60000, 60000]) {
      children.at(-1).emit('close', 134, null);
      expect(log.mock.calls.at(-1)[1].retryMs).toBe(delay);
      jest.advanceTimersByTime(delay);
    }
    jest.advanceTimersByTime(600_000); children.at(-1).emit('close', 134, null);
    expect(log.mock.calls.at(-1)[1].retryMs).toBe(2000);
  });
  test('an intentional deploy shutdown is forwarded once and never respawns', () => {
    supervisor.start(); supervisor.stop('SIGTERM'); supervisor.stop('SIGTERM');
    expect(children[0].kill).toHaveBeenCalledTimes(1);
    expect(children[0].kill).toHaveBeenCalledWith('SIGTERM');
    children[0].emit('close', null, 'SIGTERM'); jest.advanceTimersByTime(60_000);
    expect(exit).toHaveBeenCalledWith(0); expect(spawn).toHaveBeenCalledTimes(1);
  });
  test('shutdown cancels a pending recovery instead of starting a new process', () => {
    supervisor.start(); children[0].emit('close', 134, null); supervisor.stop();
    jest.advanceTimersByTime(60_000); expect(spawn).toHaveBeenCalledTimes(1); expect(exit).toHaveBeenCalledWith(0);
  });
  test('forced termination is only a shutdown deadline, never a healthy-process recycle', () => {
    supervisor.start(); supervisor.stop(); jest.advanceTimersByTime(25_000);
    expect(children[0].kill.mock.calls).toEqual([['SIGTERM'], ['SIGKILL']]);
    children[0].emit('close', null, 'SIGKILL'); expect(exit).toHaveBeenCalledWith(0);
  });
  test('a synchronous launch failure backs off without creating overlapping children', () => {
    spawn.mockImplementationOnce(() => { throw Object.assign(new Error('missing executable'), { code: 'ENOENT' }); });
    supervisor.start(); expect(log.mock.calls.at(-1)[1]).toMatchObject({ error: 'ENOENT', retryMs: 2000 });
    jest.advanceTimersByTime(2000); expect(children).toHaveLength(1);
  });
});

describe('runtime health without exchange dependencies or secret exposure', () => {
  afterEach(() => { jest.restoreAllMocks(); jest.useRealTimers(); });
  test('reports bounded memory metadata, not account data, secrets or false upstream readiness', () => {
    process.env.RUNTIME_TEST_SECRET = 'must-not-appear';
    const result = new RuntimeHealth().snapshot();
    delete process.env.RUNTIME_TEST_SECRET;
    expect(result.ok).toBe(true); expect(result.memoryMb.rss).toBeGreaterThan(0);
    expect(result.memoryMb.heapLimit).toBeGreaterThan(0);
    expect(JSON.stringify(result)).not.toMatch(/must-not-appear|account|api.key|wallet|mnemonic/i);
  });
  test('diagnostics are single-instance, bounded and do not catch/swallow fatal exceptions', () => {
    jest.useFakeTimers(); const log = jest.spyOn(console, 'log').mockImplementation(() => {});
    const before = process.listenerCount('uncaughtException');
    const health = new RuntimeHealth(); health.start(); health.start();
    jest.advanceTimersByTime(120_000); expect(log).toHaveBeenCalledTimes(3);
    expect(process.listenerCount('uncaughtException')).toBe(before);
    health.stop(); jest.advanceTimersByTime(120_000); expect(log).toHaveBeenCalledTimes(3);
  });
});

test('real isolated child exiting 134 is restarted and then stopped without a third launch', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'runtime-recovery-'));
  try {
    const output = execFileSync(process.execPath, [path.join(__dirname, 'runtimeRecoveryFixture.cjs')], {
      encoding: 'utf8', timeout: 15_000, env: { ...process.env, RECOVERY_FIXTURE_DIRECTORY: directory }
    });
    expect(JSON.parse(output)).toMatchObject({ launches: 2, simulatedCrashCode: 134, recovered: true, cleanShutdown: true });
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
}, 20_000);
