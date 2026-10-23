// Read local/server disk only. Never initialize collectors, SMTP or exchanges.
const fs = require('node:fs');
const path = require('node:path');
const [directory, market, symbol, source, modelVersion, month] = process.argv.slice(2);
if (!directory || !market || !symbol || !source || !modelVersion || !month) {
  throw new Error('Usage: node scripts/audit-liquidity-storage.cjs <directory> <market> <symbol> <source> <modelVersion> <YYYY-MM>');
}
const built = path.join(__dirname, '../dist/services/liquidityObservationArchive.js');
let library;
if (fs.existsSync(built)) library = require(built);
else { require('ts-node/register/transpile-only'); library = require('../src/services/liquidityObservationArchive'); }
const archive = new library.LiquidityObservationArchive(directory, { market, symbol, source, modelVersion });
archive.audit(month).then(result => {
  console.log(JSON.stringify(result, null, 2));
}).catch(error => { console.error(error.message); process.exitCode = 1; }).finally(() => archive.close());
