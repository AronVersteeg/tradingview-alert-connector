import fs from 'node:fs/promises';
import axios from 'axios';
import { parseSpotChartCandles } from '../src/services/binanceSpotChart';
import { buildDailyFractalHistory, latestExpectedFractalClose, parseBinanceDailyCandles } from '../src/services/binanceDailyFractalHistory';
import { purpleLineSetupForMarket, type PurpleLineMarket } from '../src/services/purpleLineSetup';
import { decodePurpleHistory, purpleHistoryPath, readGreenHistory, type PurpleHistory } from '../src/research/greenRetestHistory';
import { purpleRetestContext } from '../src/research/purpleRetest';

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const market = (process.argv[2] || '').toUpperCase() as PurpleLineMarket;

async function page(symbol: string, interval: '1h' | '1d', from: number, to: number) {
  const unit = interval === '1h' ? HOUR : DAY;
  const result: unknown[] = [];
  for (let start = from; start < to; start += 1000 * unit) {
    const end = Math.min(to, start + 1000 * unit);
    const { data } = await axios.get<unknown[]>('https://data-api.binance.vision/api/v3/klines', { timeout: 25_000,
      params: { symbol, interval, startTime: start, endTime: end - 1, limit: (end - start) / unit } });
    if (!Array.isArray(data) || data.length !== (end - start) / unit) throw new Error(`Incomplete Binance ${symbol} ${interval} page.`);
    result.push(...data);
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  return result;
}

async function main() {
  const setup = purpleLineSetupForMarket(market);
  if (!setup) throw new Error('Expected BTC-USD, ETH-USD, SOL-USD or ZEC-USD.');
  const green = readGreenHistory(market);
  if (!green) throw new Error(`No ${market} Green archive.`);
  const { from, through } = green.context;
  const hoursFrom = from - 999 * HOUR;
  const hours = parseSpotChartCandles(await page(setup.symbol, '1h', hoursFrom, through), through);
  if (hours.length !== (through - hoursFrom) / HOUR || hours.some((row, i) => !row.closed || row.time * 1000 !== hoursFrom + i * HOUR)) {
    throw new Error(`Incomplete ${market} closed Spot hours.`);
  }
  const daysTo = latestExpectedFractalClose('1d', through) + 1;
  const daysFrom = Math.floor(from / DAY) * DAY - 999 * DAY;
  const days = parseBinanceDailyCandles(await page(setup.symbol, '1d', daysFrom, daysTo), through);
  if (days.length !== (daysTo - daysFrom) / DAY || days.some((row, i) => row.openTime !== daysFrom + i * DAY ||
      row.closeTime !== row.openTime + DAY - 1 || !(Number(row.low) > 0) || Number(row.high) < Number(row.low))) {
    throw new Error(`Incomplete ${market} closed Spot days.`);
  }
  const purple = purpleRetestContext(green.context, hours, { market, symbol: setup.symbol, venue: 'Binance Spot',
    interval: '1d', latestClosedCandleAt: new Date(daysTo - 1).toISOString(), records: buildDailyFractalHistory(days, market) });
  const archive: PurpleHistory = { version: 1, market, model: setup.modelVersion, from, through,
    entries: purple.signals.map(signal => [signal.closedAt * 1000, signal.dailyLevels!]) };
  decodePurpleHistory(green, archive);
  await fs.writeFile(purpleHistoryPath(market), JSON.stringify(archive, null, 2) + '\n');
  console.log(`${market}: wrote ${archive.entries.length} Purple entries`);
}

main().catch(error => { console.error(`${market} Purple history build failed:`, error.message); process.exitCode = 1; });
