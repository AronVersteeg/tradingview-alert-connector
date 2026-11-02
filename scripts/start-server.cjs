'use strict';
const { spawn } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');

function runtimeMemoryBudget(env, read = file => fs.readFileSync(file, 'utf8'), total = os.totalmem()) {
  if (!env.RENDER || /--max[-_]old[-_]space[-_]size\b|--max[-_]heap[-_]size\b/.test(env.NODE_OPTIONS || '')) return undefined;
  const limits = [];
  for (const file of ['/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes', '/sys/fs/cgroup/memory.limit_in_bytes']) {
    try {
      const raw = String(read(file)).trim(), bytes = /^\d+$/.test(raw) ? Number(raw) : NaN;
      if (Number.isSafeInteger(bytes) && bytes > 0 && bytes <= total) limits.push(bytes);
    } catch { /* A missing cgroup file is not proof of a memory limit. */ }
  }
  if (!limits.length) return undefined;
  const memoryLimitMb = Math.floor(Math.min(...limits) / 1024 / 1024);
  if (memoryLimitMb < 256 || memoryLimitMb > 2048) return undefined;
  // Leave 40% for native allocations, buffers, the supervisor and the OS.
  const oldSpaceMb = Math.min(1024, Math.floor(memoryLimitMb * 0.6));
  return { memoryLimitMb, oldSpaceMb, reservedMb: memoryLimitMb - oldSpaceMb };
}

function createSupervisor(options = {}) {
  const launch = options.spawn || spawn, now = options.now || Date.now;
  const schedule = options.setTimeout || setTimeout, cancel = options.clearTimeout || clearTimeout;
  const log = options.log || ((event, fields) => console.log(event, fields));
  const exit = options.exit || (code => process.exit(code));
  const file = options.file || path.join(__dirname, '..', 'dist', 'index.js');
  const env = options.env || process.env;
  const budget = runtimeMemoryBudget(env, options.readMemoryLimit, options.totalMemory);
  let child, retry, deadline, stopping = false, failures = 0, restarts = 0;

  function start() {
    if (stopping || child || retry) return;
    const started = now();
    let current;
    try {
      current = launch(process.execPath, [...(budget ? [`--max-old-space-size=${budget.oldSpaceMb}`] : []), file], { stdio: ['ignore', 'inherit', 'inherit'],
        env: { ...env, NODE_ENV: env.NODE_ENV || 'production', SERVICE_SUPERVISED: '1', SERVICE_RESTART_COUNT: String(restarts),
          ...(budget ? { SERVICE_HEAP_BUDGET_MB: String(budget.oldSpaceMb), SERVICE_MEMORY_LIMIT_MB: String(budget.memoryLimitMb) } : {}) } });
    } catch (error) { failed(1, null, started, error.code || error.name); return; }
    child = current;
    if (budget) log('Service memory budget:', budget);
    log('Service child started:', { pid: current.pid, restarts });
    current.once('error', error => log('Service child spawn error:', { code: error.code || error.name }));
    // Wait for close, not just exit: never overlap an old trading process with a new one.
    current.once('close', (code, signal) => {
      if (child !== current) return;
      child = undefined;
      if (deadline) { cancel(deadline); deadline = undefined; }
      if (stopping) { exit(0); return; }
      failed(code, signal, started);
    });
  }

  function failed(code, signal, started, error) {
    if (stopping) return;
    if (now() - started >= 10 * 60_000) failures = 0;
    const retryMs = Math.min(60_000, 2000 * 2 ** Math.min(failures++, 5));
    restarts++;
    log('Service child exited; automatic recovery scheduled:', { code, signal, error, retryMs, restarts });
    retry = schedule(() => { retry = undefined; start(); }, retryMs);
  }

  function stop(signal = 'SIGTERM') {
    if (stopping) return;
    stopping = true;
    if (retry) { cancel(retry); retry = undefined; }
    if (!child) { exit(0); return; }
    log('Service supervisor stopping:', { signal, pid: child.pid });
    child.kill(signal);
    deadline = schedule(() => { if (child) child.kill('SIGKILL'); }, 25_000);
    deadline.unref?.();
  }
  return { start, stop };
}

if (require.main === module) {
  const supervisor = createSupervisor();
  process.on('SIGTERM', () => supervisor.stop('SIGTERM'));
  process.on('SIGINT', () => supervisor.stop('SIGINT'));
  supervisor.start();
}
module.exports = { createSupervisor, runtimeMemoryBudget };
