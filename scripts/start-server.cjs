'use strict';
const { spawn } = require('node:child_process');
const path = require('node:path');

function createSupervisor(options = {}) {
  const launch = options.spawn || spawn, now = options.now || Date.now;
  const schedule = options.setTimeout || setTimeout, cancel = options.clearTimeout || clearTimeout;
  const log = options.log || ((event, fields) => console.log(event, fields));
  const exit = options.exit || (code => process.exit(code));
  const file = options.file || path.join(__dirname, '..', 'dist', 'index.js');
  const env = options.env || process.env;
  let child, retry, deadline, stopping = false, failures = 0, restarts = 0;

  function start() {
    if (stopping || child || retry) return;
    const started = now();
    let current;
    try {
      current = launch(process.execPath, [file], { stdio: ['ignore', 'inherit', 'inherit'],
        env: { ...env, NODE_ENV: env.NODE_ENV || 'production', SERVICE_SUPERVISED: '1', SERVICE_RESTART_COUNT: String(restarts) } });
    } catch (error) { failed(1, null, started, error.code || error.name); return; }
    child = current;
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
module.exports = { createSupervisor };
