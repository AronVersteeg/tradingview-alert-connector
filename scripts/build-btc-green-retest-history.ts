import fs from 'node:fs/promises';
import path from 'node:path';
import axios from 'axios';
import { IncrementalReplicaReplay, type SpotCandle } from '../src/services/openLiquidityV2Replica';
import type { DydxRsiCandle } from '../src/services/decentraderGapMonitor';
import { PURPLE_LINE_SETUPS } from '../src/services/purpleLineSetup';
import { btcGreenRetestSignals, btcRetestFractalRows } from '../src/research/btcGreenRetest';
import { BTC_GREEN_HISTORY_VERSION, BTC_GREEN_HISTORY_WARMUP_HOURS, decodeBtcGreenHistory,
  type BtcGreenHistoryArchive } from '../src/research/btcGreenRetestHistory';

const HOUR = 3_600_000;
const from = Date.parse(process.argv[2] || '2025-01-01T00:00:00Z');
const observedAt = Date.now();
const through = process.argv[3] ? Date.parse(process.argv[3]) : Math.floor(observedAt / HOUR) * HOUR - HOUR;
const setup = PURPLE_LINE_SETUPS['BTC-USD'];
const iso = (ms: number) => new Date(ms).toISOString();
const pause = () => new Promise(resolve => setTimeout(resolve, 250));

async function fetchPages<T>(start: number, page: (start: number, end: number, count: number) => Promise<T[]>): Promise<T[]> {
  const rows: T[] = [];
  for (let at = start; at < through;) {
    const end = Math.min(at + 750 * HOUR, through);
    const result = await page(at, end, (end - at) / HOUR);
    rows.push(...result);
    console.log(`${iso(at)} to ${iso(end)}: ${result.length} candles`);
    at = end;
    await pause();
  }
  return rows;
}

async function main() {
  if (![from, through].every(ms => Number.isSafeInteger(ms) && ms > 0 && ms % HOUR === 0) ||
      through <= from + HOUR || through > observedAt || through - from > 25_000 * HOUR) throw new Error('Invalid archive range.');
  const warmupFrom = from - BTC_GREEN_HISTORY_WARMUP_HOURS * HOUR;
  const spot = await fetchPages<SpotCandle>(warmupFrom, async (start, end, limit) => {
    const { data } = await axios.get<unknown[][]>('https://data-api.binance.vision/api/v3/klines', { timeout: 25_000,
      params: { symbol: 'BTCUSDT', interval: '1h', startTime: start, endTime: end - 1, limit } });
    return data.map(([t, o, h, l, c, , endTime]) => ({ timestampMs: Number(t), closeTimeMs: Number(endTime),
      open: Number(o), high: Number(h), low: Number(l), close: Number(c) }));
  });
  if (spot.length !== (through - warmupFrom) / HOUR || spot.some((candle, i) =>
    candle.timestampMs !== warmupFrom + i * HOUR || candle.closeTimeMs !== candle.timestampMs + HOUR - 1 ||
    ![candle.open, candle.high, candle.low, candle.close].every(price => Number.isFinite(price) && price > 0) ||
    candle.high < Math.max(candle.open, candle.close) || candle.low > Math.min(candle.open, candle.close))) {
    throw new Error('Binance Spot source has missing or invalid hours.');
  }
  const dydxFrom = from - 1000 * HOUR;
  const sourceDydx = await fetchPages<DydxRsiCandle>(dydxFrom, async (start, end, limit) => {
    const { data } = await axios.get<{ candles: (DydxRsiCandle & { ticker: string })[] }>(
      'https://indexer.dydx.trade/v4/candles/perpetualMarkets/BTC-USD', { timeout: 25_000,
        params: { resolution: '1HOUR', fromISO: iso(start), toISO: iso(end), limit } });
    return data.candles.filter(row => row.ticker === 'BTC-USD' && row.resolution === '1HOUR' &&
      Date.parse(row.startedAt) >= start && Date.parse(row.startedAt) < end);
  });
  const rows = btcRetestFractalRows(sourceDydx, through);
  const dydxHours = new Map(rows.map(row => [Date.parse(row.timestamp), row]));
  if (rows.length !== dydxHours.size) throw new Error('Duplicate dYdX hours.');
  const dydx: BtcGreenHistoryArchive['dydx'] = [];
  for (let at = dydxFrom; at < through; at += HOUR) {
    const row = dydxHours.get(at);
    if (!row) console.warn(`Missing dYdX hour (left blank): ${iso(at)}`);
    dydx.push(row ? [row.openRef, row.highRef, row.lowRef, row.closeRef] : null);
  }

  const archive: BtcGreenHistoryArchive = { version: BTC_GREEN_HISTORY_VERSION, model: setup.modelVersion,
    priceStepUsd: setup.priceStepUsd, cohortWindowHours: BTC_GREEN_HISTORY_WARMUP_HOURS, from, through, dydxFrom,
    dydx, greens: [], yellows: [] };
  const baseline = BTC_GREEN_HISTORY_WARMUP_HOURS + 1;
  const replica = await IncrementalReplicaReplay.create(spot.slice(0, baseline), { frameLimit: 2,
    priceStepUsd: setup.priceStepUsd, modelVersion: setup.modelVersion, cohortWindowHours: BTC_GREEN_HISTORY_WARMUP_HOURS });
  for (let i = baseline; i < spot.length; i++) {
    const candle = spot[i];
    replica.append(candle, iso(candle.timestampMs + HOUR));
    const [previous, next] = replica.snapshots;
    const payload = { modelVersion: setup.modelVersion, quality: { causalModel: true, priceStepUsd: setup.priceStepUsd },
      source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H', ok: true }] },
      frames: [previous, next].map(row => ({ t: row.effectiveAt.slice(0, 19).replace('T', ' '),
        startedAtMs: Date.parse(row.effectiveAt), close: row.close, price: row.referencePrice })),
      gaps: [previous.gap, next.gap], zoneSeed: previous.zoneSeed, zoneDeltas: [[], next.zoneDeltas] };
    const result = btcGreenRetestSignals(payload, candle.timestampMs + HOUR);
    archive.greens.push(...result.signals.map((signal): BtcGreenHistoryArchive['greens'][number] =>
      [signal.closedAt * 1000, signal.direction, signal.edge, signal.close]));
    archive.yellows.push(...result.yellows.map((row): BtcGreenHistoryArchive['yellows'][number] => [row.at, row.direction]));
    if ((i - baseline + 1) % 2000 === 0) console.log(`Replica reconstructed through ${iso(result.through)}`);
  }
  decodeBtcGreenHistory(archive);
  const output = path.resolve('research-data/btc-green-retest-history.json');
  await fs.mkdir(path.dirname(output), { recursive: true });
  // One record per line makes the reproducible snapshot reviewable and diffable.
  const { dydx: candles, greens, yellows, ...metadata } = archive;
  const recordLines = (key: string, rows: unknown[]) => `  ${JSON.stringify(key)}: [\n${rows.map(row => `    ${JSON.stringify(row)}`).join(',\n')}\n  ]`;
  const header = JSON.stringify(metadata, null, 2).slice(0, -2);
  await fs.writeFile(output, `${header},\n${recordLines('dydx', candles)},\n${recordLines('greens', greens)},\n${recordLines('yellows', yellows)}\n}\n`);
  console.log(`Wrote ${rows.length} dYdX candles, ${greens.length} Green candidates and ${yellows.length} Yellows: ${output}`);
}

main().catch(error => { console.error('BTC Green history build failed:', error.message); process.exitCode = 1; });
