import fs from 'fs';
import path from 'path';

import { binanceGet } from './binanceHttp';

const BINANCE_FUTURES_DATA_URL = 'https://fapi.binance.com/futures/data/openInterestHist';
const PERIOD_MS = 5 * 60 * 1000;
const PAGE_LIMIT = 500;
const MAX_RECORDS_PER_SYMBOL = 200_000;

export type BinanceOpenInterestPoint = {
  timestamp: number;
  contractOpenInterest: number;
  openInterestUsd: number;
};

export type BinanceOpenInterestWindow = {
  source: 'binance-futures-open-interest';
  symbol: string;
  from: string;
  to: string;
  fetchedAt: string;
  samples: number;
  firstTimestamp?: string;
  lastTimestamp?: string;
  startContractOpenInterest?: number;
  endContractOpenInterest?: number;
  contractChangePct?: number;
  startOpenInterestUsd?: number;
  endOpenInterestUsd?: number;
  usdChangePct?: number;
};

type StoredHistory = {
  version: 1;
  symbol: string;
  records: BinanceOpenInterestPoint[];
};

const exactWindowCache = new Map<string, BinanceOpenInterestWindow>();

function finite(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function timestampMs(value: string): number | undefined {
  const parsed = Date.parse(value.includes('T') ? value : `${value.replace(' ', 'T')}Z`);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function baseHistoryDirectory(): string {
  const configured = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  return configured ? path.dirname(configured) : path.join(process.cwd(), 'data');
}

function historyFile(symbol: string): string {
  const configuredDirectory = String(process.env.BINANCE_OPEN_INTEREST_HISTORY_DIR || '').trim();
  const directory = configuredDirectory || path.join(baseHistoryDirectory(), 'binance-open-interest');
  return path.join(directory, `${symbol.toLowerCase()}.json`);
}

function readHistory(symbol: string): BinanceOpenInterestPoint[] {
  try {
    const parsed = JSON.parse(fs.readFileSync(historyFile(symbol), 'utf8')) as StoredHistory;
    return Array.isArray(parsed.records) ? parsed.records : [];
  } catch {
    return [];
  }
}

function writeHistory(symbol: string, incoming: BinanceOpenInterestPoint[]): void {
  if (!incoming.length) return;
  const file = historyFile(symbol);
  const byTimestamp = new Map<number, BinanceOpenInterestPoint>();
  for (const point of [...readHistory(symbol), ...incoming]) {
    if (Number.isFinite(point.timestamp)) byTimestamp.set(point.timestamp, point);
  }
  const records = [...byTimestamp.values()]
    .sort((left, right) => left.timestamp - right.timestamp)
    .slice(-MAX_RECORDS_PER_SYMBOL);
  const payload: StoredHistory = { version: 1, symbol, records };
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(payload));
  fs.renameSync(temporary, file);
}

export function summarizeOpenInterestWindow(input: {
  symbol: string;
  fromMs: number;
  toMs: number;
  points: BinanceOpenInterestPoint[];
  fetchedAt?: string;
}): BinanceOpenInterestWindow {
  const causalPoints = input.points
    .filter((point) => point.timestamp >= input.fromMs && point.timestamp <= input.toMs)
    .sort((left, right) => left.timestamp - right.timestamp);
  const first = causalPoints[0];
  const last = causalPoints[causalPoints.length - 1];
  const contractChangePct = first && last && first.contractOpenInterest > 0
    ? ((last.contractOpenInterest / first.contractOpenInterest) - 1) * 100
    : undefined;
  const usdChangePct = first && last && first.openInterestUsd > 0
    ? ((last.openInterestUsd / first.openInterestUsd) - 1) * 100
    : undefined;

  return {
    source: 'binance-futures-open-interest',
    symbol: input.symbol,
    from: new Date(input.fromMs).toISOString(),
    to: new Date(input.toMs).toISOString(),
    fetchedAt: input.fetchedAt || new Date().toISOString(),
    samples: causalPoints.length,
    firstTimestamp: first ? new Date(first.timestamp).toISOString() : undefined,
    lastTimestamp: last ? new Date(last.timestamp).toISOString() : undefined,
    startContractOpenInterest: first?.contractOpenInterest,
    endContractOpenInterest: last?.contractOpenInterest,
    contractChangePct,
    startOpenInterestUsd: first?.openInterestUsd,
    endOpenInterestUsd: last?.openInterestUsd,
    usdChangePct
  };
}

async function fetchPoints(symbol: string, fromMs: number, toMs: number): Promise<BinanceOpenInterestPoint[]> {
  const records: BinanceOpenInterestPoint[] = [];
  let cursor = fromMs;
  for (let page = 0; page < 12 && cursor <= toMs; page += 1) {
    const response = await binanceGet<any[]>(BINANCE_FUTURES_DATA_URL, {
      params: {
        symbol,
        period: '5m',
        startTime: cursor,
        endTime: toMs,
        limit: PAGE_LIMIT
      },
      timeout: 15_000
    });
    const rows = Array.isArray(response.data) ? response.data : [];
    const pagePoints = rows.map((row) => ({
      timestamp: Number(row?.timestamp),
      contractOpenInterest: Number(row?.sumOpenInterest),
      openInterestUsd: Number(row?.sumOpenInterestValue)
    })).filter((point) =>
      Number.isFinite(point.timestamp) &&
      Number.isFinite(point.contractOpenInterest) &&
      Number.isFinite(point.openInterestUsd)
    );
    records.push(...pagePoints);
    if (rows.length < PAGE_LIMIT || !pagePoints.length) break;
    const nextCursor = pagePoints[pagePoints.length - 1].timestamp + PERIOD_MS;
    if (nextCursor <= cursor) break;
    cursor = nextCursor;
  }
  return records;
}

export async function observeBinanceOpenInterestWindow(input: {
  symbol: string;
  from: string;
  to: string;
}): Promise<BinanceOpenInterestWindow> {
  const symbol = String(input.symbol || '').trim().toUpperCase();
  const fromMs = timestampMs(input.from);
  const toMs = timestampMs(input.to);
  if (!symbol || fromMs === undefined || toMs === undefined || toMs <= fromMs) {
    return summarizeOpenInterestWindow({
      symbol,
      fromMs: fromMs || 0,
      toMs: toMs || 0,
      points: []
    });
  }

  const cacheKey = `${symbol}|${fromMs}|${toMs}`;
  const cached = exactWindowCache.get(cacheKey);
  if (cached) return cached;

  let points: BinanceOpenInterestPoint[] = [];
  try {
    points = await fetchPoints(symbol, fromMs, toMs);
    writeHistory(symbol, points);
  } catch (error) {
    points = readHistory(symbol);
    console.warn('Binance Open Interest window refresh failed; using stored causal samples if available.', {
      symbol,
      from: new Date(fromMs).toISOString(),
      to: new Date(toMs).toISOString(),
      storedSamples: points.length,
      error: error instanceof Error ? error.message : String(error)
    });
  }

  const summary = summarizeOpenInterestWindow({ symbol, fromMs, toMs, points });
  if (summary.samples >= 2) exactWindowCache.set(cacheKey, summary);
  return summary;
}
