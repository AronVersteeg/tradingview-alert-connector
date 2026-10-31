'use strict';
// This fixture never imports the application, exchanges, collectors or SMTP.
const fs = require('node:fs');
const path = require('node:path');
const directory = process.env.RECOVERY_FIXTURE_DIRECTORY;
if (!directory) throw new Error('A temporary fixture directory is required.');
const crashed = path.join(directory, 'crashed'), ready = path.join(directory, 'ready');
if (process.env.RECOVERY_FIXTURE_CHILD === '1') {
  if (!fs.existsSync(crashed)) { fs.writeFileSync(crashed, '134'); process.exit(134); }
  fs.writeFileSync(ready, String(process.pid));
  setInterval(() => {}, 1000);
} else {
  const { createSupervisor } = require('../scripts/start-server.cjs');
  let launches = 0, crashCode;
  const supervisor = createSupervisor({ file: __filename,
    env: { ...process.env, RECOVERY_FIXTURE_CHILD: '1' },
    log: (event, data) => {
      if (event === 'Service child started:') launches++;
      if (event === 'Service child exited; automatic recovery scheduled:') crashCode = data.code;
    },
    exit: code => {
      clearInterval(check);
      console.log(JSON.stringify({ launches, simulatedCrashCode: crashCode,
        recovered: fs.existsSync(ready), cleanShutdown: code === 0 }));
      process.exit(code);
    } });
  const check = setInterval(() => { if (fs.existsSync(ready)) supervisor.stop(); }, 20);
  process.on('SIGTERM', () => supervisor.stop());
  supervisor.start();
}
