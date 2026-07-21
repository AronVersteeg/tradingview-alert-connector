import fs from 'fs';
import path from 'path';

import axios from 'axios';

const WebSocketClient = require('ws');

type DomSide = 'buy' | 'sell';
type VenueName = 'dydx' | 'hyperliquid';

export type DomLevel = {
  price: number;
  size: number;
  orders?: number;
};

export type DomTrade = {
  id: string;
  timestamp: number;
  side: DomSide;
  price: number;
  size: number;
};

export type DomBookMetrics = {
  mid: number;
  spreadBps: number;
  depthCoverageBps: { bid: number; ask: number };
  bidDepthUsd: Record<string, number>;
  askDepthUsd: Record<string, number>;
  imbalance: Record<string, number>;
  micropriceOffsetBps: number;
};

export type DomVenueMinute = {
  samples: number;
  failedPolls: number;
  mid: number;
  spreadBps: number;
  depthCoverageBps: { bid: number; ask: number };
  bidDepthUsd: Record<string, number>;
  askDepthUsd: Record<string, number>;
  imbalance: Record<string, number>;
  imbalanceMin: Record<string, number>;
  imbalanceMax: Record<string, number>;
  micropriceOffsetBps: number;
  bidAddedUsd: number;
  bidRemovedUsd: number;
  askAddedUsd: number;
  askRemovedUsd: number;
  buyTakerUsd: number;
  sellTakerUsd: number;
  buyTrades: number;
  sellTrades: number;
  largestTradeUsd: number;
};

export type DomMinuteRecord = {
  version: 1;
  bucketStart: string;
  bucketEnd: string;
  market: 'BTC-USD';
  venues: Partial<Record<VenueName, DomVenueMinute>>;
  crossVenue: {
    availableVenues: number;
    midSpreadBps: number;
    consensusImbalance25Bps: number;
    consensusTakerDeltaUsd: number;
  };
};

type VenueSnapshot = {
  venue: VenueName;
  observedAt: number;
  bids: DomLevel[];
  asks: DomLevel[];
  topOfBook?: { bid: number; ask: number };
  trades: DomTrade[];
};

type VenueAccumulator = {
  samples: number;
  failedPolls: number;
  latest?: DomBookMetrics;
  depthSums: Record<string, { bid: number; ask: number; imbalance: number }>;
  imbalanceMin: Record<string, number>;
  imbalanceMax: Record<string, number>;
  micropriceSum: number;
  spreadSum: number;
  bidAddedUsd: number;
  bidRemovedUsd: number;
  askAddedUsd: number;
  askRemovedUsd: number;
  buyTakerUsd: number;
  sellTakerUsd: number;
  buyTrades: number;
  sellTrades: number;
  largestTradeUsd: number;
};

type CollectorStatus = {
  enabled: boolean;
  running: boolean;
  readOnly: true;
  market: 'BTC-USD';
  pollSeconds: number;
  bucketSeconds: number;
  sources: Array<{
    venue: VenueName;
    public: true;
    requiresApiKey: false;
    tradeStreamConnected: boolean;
    lastSuccessAt?: string;
    lastErrorAt?: string;
    lastError?: string;
  }>;
  historyDirectory: string;
  currentBucketStart?: string;
  lastStoredAt?: string;
  storedRecordsThisRun: number;
  coverage?: {
    from?: string;
    to?: string;
    sourceFiles: number;
  };
  latest?: DomMinuteRecord;
  current?: DomMinuteRecord;
};

const DEPTH_BANDS_BPS = [5, 10, 25, 50, 100];
const MARKET = 'BTC-USD' as const;
const DEFAULT_DYDX_HTTP_URL = 'https://indexer.dydx.trade';
const DEFAULT_HYPERLIQUID_HTTP_URL = 'https://api.hyperliquid.xyz/info';

function finiteNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function positiveNumber(value: unknown): number {
  return Math.max(0, finiteNumber(value));
}

function rounded(value: number, decimals = 4): number {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function boundedInteger(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed)
    ? Math.max(min, Math.min(max, Math.floor(parsed)))
    : fallback;
}

function envEnabled(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return String(raw).trim().toLowerCase() === 'true';
}

function emptyBandRecord(fill = 0): Record<string, number> {
  return Object.fromEntries(DEPTH_BANDS_BPS.map((band) => [String(band), fill]));
}

function normalizeLevels(levels: any[]): DomLevel[] {
  if (!Array.isArray(levels)) return [];
  return levels
    .map((level) => ({
      price: positiveNumber(level?.price ?? level?.px),
      size: positiveNumber(level?.size ?? level?.sz),
      orders: level?.n === undefined ? undefined : positiveNumber(level.n)
    }))
    .filter((level) => level.price > 0 && level.size > 0);
}

function levelMap(levels: DomLevel[]): Map<number, number> {
  return new Map(levels.map((level) => [level.price, level.size]));
}

export function calculateBookMetrics(
  bids: DomLevel[],
  asks: DomLevel[],
  topOfBook?: { bid: number; ask: number }
): DomBookMetrics | undefined {
  const bestBid = topOfBook?.bid || Math.max(...bids.map((level) => level.price));
  const bestAsk = topOfBook?.ask || Math.min(...asks.map((level) => level.price));
  if (!Number.isFinite(bestBid) || !Number.isFinite(bestAsk) || bestBid <= 0 || bestAsk <= bestBid) {
    return undefined;
  }

  const mid = (bestBid + bestAsk) / 2;
  const bidDepthUsd = emptyBandRecord();
  const askDepthUsd = emptyBandRecord();
  const imbalance = emptyBandRecord();

  for (const band of DEPTH_BANDS_BPS) {
    const distance = band / 10_000;
    const bidFloor = mid * (1 - distance);
    const askCeiling = mid * (1 + distance);
    const bidDepth = bids
      .filter((level) => level.price >= bidFloor && level.price <= mid)
      .reduce((sum, level) => sum + level.price * level.size, 0);
    const askDepth = asks
      .filter((level) => level.price <= askCeiling && level.price >= mid)
      .reduce((sum, level) => sum + level.price * level.size, 0);
    const total = bidDepth + askDepth;
    bidDepthUsd[String(band)] = rounded(bidDepth, 2);
    askDepthUsd[String(band)] = rounded(askDepth, 2);
    imbalance[String(band)] = total > 0 ? rounded((bidDepth - askDepth) / total, 6) : 0;
  }

  const bestBidSize = bids.find((level) => level.price === bestBid)?.size || 0;
  const bestAskSize = asks.find((level) => level.price === bestAsk)?.size || 0;
  const topSize = bestBidSize + bestAskSize;
  const microprice = topSize > 0
    ? (bestAsk * bestBidSize + bestBid * bestAskSize) / topSize
    : mid;

  return {
    mid: rounded(mid, 4),
    spreadBps: rounded(((bestAsk - bestBid) / mid) * 10_000, 6),
    depthCoverageBps: {
      bid: rounded(((mid - Math.min(...bids.map((level) => level.price))) / mid) * 10_000, 3),
      ask: rounded(((Math.max(...asks.map((level) => level.price)) - mid) / mid) * 10_000, 3)
    },
    bidDepthUsd,
    askDepthUsd,
    imbalance,
    micropriceOffsetBps: rounded(((microprice - mid) / mid) * 10_000, 6)
  };
}

export function calculateBookChanges(
  previous: { bids: DomLevel[]; asks: DomLevel[] } | undefined,
  current: { bids: DomLevel[]; asks: DomLevel[] },
  referenceMid?: number,
  bandBps?: number
): { bidAddedUsd: number; bidRemovedUsd: number; askAddedUsd: number; askRemovedUsd: number } {
  if (!previous) {
    return { bidAddedUsd: 0, bidRemovedUsd: 0, askAddedUsd: 0, askRemovedUsd: 0 };
  }

  const insideBand = (level: DomLevel) => {
    if (!referenceMid || !bandBps) return true;
    return Math.abs(((level.price - referenceMid) / referenceMid) * 10_000) <= bandBps;
  };
  const compare = (oldLevels: DomLevel[], newLevels: DomLevel[]) => {
    const oldMap = levelMap(oldLevels.filter(insideBand));
    const newMap = levelMap(newLevels.filter(insideBand));
    const prices = new Set([...oldMap.keys(), ...newMap.keys()]);
    let addedUsd = 0;
    let removedUsd = 0;
    for (const price of prices) {
      const deltaSize = (newMap.get(price) || 0) - (oldMap.get(price) || 0);
      if (deltaSize > 0) addedUsd += deltaSize * price;
      if (deltaSize < 0) removedUsd += Math.abs(deltaSize) * price;
    }
    return { addedUsd, removedUsd };
  };

  const bids = compare(previous.bids, current.bids);
  const asks = compare(previous.asks, current.asks);
  return {
    bidAddedUsd: rounded(bids.addedUsd, 2),
    bidRemovedUsd: rounded(bids.removedUsd, 2),
    askAddedUsd: rounded(asks.addedUsd, 2),
    askRemovedUsd: rounded(asks.removedUsd, 2)
  };
}

function createVenueAccumulator(): VenueAccumulator {
  return {
    samples: 0,
    failedPolls: 0,
    depthSums: Object.fromEntries(
      DEPTH_BANDS_BPS.map((band) => [String(band), { bid: 0, ask: 0, imbalance: 0 }])
    ),
    imbalanceMin: emptyBandRecord(Number.POSITIVE_INFINITY),
    imbalanceMax: emptyBandRecord(Number.NEGATIVE_INFINITY),
    micropriceSum: 0,
    spreadSum: 0,
    bidAddedUsd: 0,
    bidRemovedUsd: 0,
    askAddedUsd: 0,
    askRemovedUsd: 0,
    buyTakerUsd: 0,
    sellTakerUsd: 0,
    buyTrades: 0,
    sellTrades: 0,
    largestTradeUsd: 0
  };
}

function aggregateVenue(accumulator: VenueAccumulator, snapshot: VenueSnapshot, metrics: DomBookMetrics, changes: ReturnType<typeof calculateBookChanges>, newTrades: DomTrade[]): void {
  accumulator.samples += 1;
  accumulator.latest = metrics;
  accumulator.spreadSum += metrics.spreadBps;
  accumulator.micropriceSum += metrics.micropriceOffsetBps;
  accumulator.bidAddedUsd += changes.bidAddedUsd;
  accumulator.bidRemovedUsd += changes.bidRemovedUsd;
  accumulator.askAddedUsd += changes.askAddedUsd;
  accumulator.askRemovedUsd += changes.askRemovedUsd;

  for (const band of DEPTH_BANDS_BPS.map(String)) {
    accumulator.depthSums[band].bid += metrics.bidDepthUsd[band];
    accumulator.depthSums[band].ask += metrics.askDepthUsd[band];
    accumulator.depthSums[band].imbalance += metrics.imbalance[band];
    accumulator.imbalanceMin[band] = Math.min(accumulator.imbalanceMin[band], metrics.imbalance[band]);
    accumulator.imbalanceMax[band] = Math.max(accumulator.imbalanceMax[band], metrics.imbalance[band]);
  }

  for (const trade of newTrades) {
    const notional = trade.price * trade.size;
    accumulator.largestTradeUsd = Math.max(accumulator.largestTradeUsd, notional);
    if (trade.side === 'buy') {
      accumulator.buyTakerUsd += notional;
      accumulator.buyTrades += 1;
    } else {
      accumulator.sellTakerUsd += notional;
      accumulator.sellTrades += 1;
    }
  }
}

function finalizeVenue(accumulator: VenueAccumulator): DomVenueMinute | undefined {
  const latest = accumulator.latest;
  if (!latest || accumulator.samples <= 0) return undefined;
  const divisor = accumulator.samples;
  const bidDepthUsd = emptyBandRecord();
  const askDepthUsd = emptyBandRecord();
  const imbalance = emptyBandRecord();
  const imbalanceMin = emptyBandRecord();
  const imbalanceMax = emptyBandRecord();
  for (const band of DEPTH_BANDS_BPS.map(String)) {
    bidDepthUsd[band] = rounded(accumulator.depthSums[band].bid / divisor, 2);
    askDepthUsd[band] = rounded(accumulator.depthSums[band].ask / divisor, 2);
    imbalance[band] = rounded(accumulator.depthSums[band].imbalance / divisor, 6);
    imbalanceMin[band] = rounded(accumulator.imbalanceMin[band], 6);
    imbalanceMax[band] = rounded(accumulator.imbalanceMax[band], 6);
  }
  return {
    samples: accumulator.samples,
    failedPolls: accumulator.failedPolls,
    mid: latest.mid,
    spreadBps: rounded(accumulator.spreadSum / divisor, 6),
    depthCoverageBps: latest.depthCoverageBps,
    bidDepthUsd,
    askDepthUsd,
    imbalance,
    imbalanceMin,
    imbalanceMax,
    micropriceOffsetBps: rounded(accumulator.micropriceSum / divisor, 6),
    bidAddedUsd: rounded(accumulator.bidAddedUsd, 2),
    bidRemovedUsd: rounded(accumulator.bidRemovedUsd, 2),
    askAddedUsd: rounded(accumulator.askAddedUsd, 2),
    askRemovedUsd: rounded(accumulator.askRemovedUsd, 2),
    buyTakerUsd: rounded(accumulator.buyTakerUsd, 2),
    sellTakerUsd: rounded(accumulator.sellTakerUsd, 2),
    buyTrades: accumulator.buyTrades,
    sellTrades: accumulator.sellTrades,
    largestTradeUsd: rounded(accumulator.largestTradeUsd, 2)
  };
}

export function buildDomMinuteRecord(
  bucketStartMs: number,
  bucketSeconds: number,
  venueAccumulators: Partial<Record<VenueName, VenueAccumulator>>
): DomMinuteRecord {
  const venues: Partial<Record<VenueName, DomVenueMinute>> = {};
  for (const venue of ['dydx', 'hyperliquid'] as VenueName[]) {
    const accumulator = venueAccumulators[venue];
    if (accumulator) {
      const finalized = finalizeVenue(accumulator);
      if (finalized) venues[venue] = finalized;
    }
  }
  const available = Object.values(venues);
  const mids = available.map((venue) => venue.mid).filter((value) => value > 0);
  const averageMid = mids.length ? mids.reduce((sum, value) => sum + value, 0) / mids.length : 0;
  const takerDelta = available.reduce(
    (sum, venue) => sum + venue.buyTakerUsd - venue.sellTakerUsd,
    0
  );
  return {
    version: 1,
    bucketStart: new Date(bucketStartMs).toISOString(),
    bucketEnd: new Date(bucketStartMs + bucketSeconds * 1000).toISOString(),
    market: MARKET,
    venues,
    crossVenue: {
      availableVenues: available.length,
      midSpreadBps: mids.length > 1 && averageMid > 0
        ? rounded(((Math.max(...mids) - Math.min(...mids)) / averageMid) * 10_000, 6)
        : 0,
      consensusImbalance25Bps: available.length
        ? rounded(available.reduce((sum, venue) => sum + venue.imbalance['25'], 0) / available.length, 6)
        : 0,
      consensusTakerDeltaUsd: rounded(takerDelta, 2)
    }
  };
}

function normalizeDydxSnapshot(bookData: any, tradesData: any, observedAt: number): VenueSnapshot {
  return {
    venue: 'dydx',
    observedAt,
    bids: normalizeLevels(bookData?.bids),
    asks: normalizeLevels(bookData?.asks),
    trades: Array.isArray(tradesData?.trades)
      ? tradesData.trades.map((trade: any) => ({
          id: String(trade?.id || `${trade?.createdAt}|${trade?.price}|${trade?.size}|${trade?.side}`),
          timestamp: Date.parse(String(trade?.createdAt || '')) || observedAt,
          side: (String(trade?.side || '').toUpperCase() === 'BUY' ? 'buy' : 'sell') as DomSide,
          price: positiveNumber(trade?.price),
          size: positiveNumber(trade?.size)
        })).filter((trade: DomTrade) => trade.price > 0 && trade.size > 0)
      : []
  };
}

function normalizeHyperliquidSnapshot(bookData: any, tradesData: any, observedAt: number): VenueSnapshot {
  return {
    venue: 'hyperliquid',
    observedAt,
    bids: normalizeLevels(bookData?.depthLevels?.[0] ?? bookData?.levels?.[0]),
    asks: normalizeLevels(bookData?.depthLevels?.[1] ?? bookData?.levels?.[1]),
    topOfBook: bookData?.fineLevels
      ? {
          bid: positiveNumber(bookData.fineLevels?.[0]?.[0]?.px),
          ask: positiveNumber(bookData.fineLevels?.[1]?.[0]?.px)
        }
      : undefined,
    trades: Array.isArray(tradesData)
      ? tradesData.map((trade: any) => ({
          id: `${trade?.time}|${trade?.tid}`,
          timestamp: positiveNumber(trade?.time) || observedAt,
          side: (String(trade?.side || '').toUpperCase() === 'B' ? 'buy' : 'sell') as DomSide,
          price: positiveNumber(trade?.px),
          size: positiveNumber(trade?.sz)
        })).filter((trade: DomTrade) => trade.price > 0 && trade.size > 0)
      : []
  };
}

export class DecentralizedDomCollector {
  private interval: NodeJS.Timeout | undefined;
  private pollPromise: Promise<void> | undefined;
  private bucketStartMs: number | undefined;
  private accumulators: Partial<Record<VenueName, VenueAccumulator>> = {};
  private previousBooks: Partial<Record<VenueName, { bids: DomLevel[]; asks: DomLevel[] }>> = {};
  private seenTradeIds: Partial<Record<VenueName, Set<string>>> = {};
  private initializedTrades: Partial<Record<VenueName, boolean>> = {};
  private sourceStatus: CollectorStatus['sources'] = [
    { venue: 'dydx', public: true, requiresApiKey: false, tradeStreamConnected: false },
    { venue: 'hyperliquid', public: true, requiresApiKey: false, tradeStreamConnected: false }
  ];
  private tradeSockets: Partial<Record<VenueName, any>> = {};
  private tradeSocketHeartbeats: Partial<Record<VenueName, NodeJS.Timeout>> = {};
  private tradeSocketReconnects: Partial<Record<VenueName, NodeJS.Timeout>> = {};
  private streamedTrades: Partial<Record<VenueName, DomTrade[]>> = {};
  private latestRecord: DomMinuteRecord | undefined;
  private storedRecords = 0;

  private enabled(): boolean {
    return envEnabled('DECENTRALIZED_DOM_COLLECTOR_ENABLED', true);
  }

  private pollSeconds(): number {
    return boundedInteger(process.env.DECENTRALIZED_DOM_POLL_SECONDS, 15, 5, 300);
  }

  private bucketSeconds(): number {
    return boundedInteger(process.env.DECENTRALIZED_DOM_BUCKET_SECONDS, 60, 60, 3600);
  }

  private retentionDays(): number {
    return boundedInteger(process.env.DECENTRALIZED_DOM_RETENTION_DAYS, 120, 7, 730);
  }

  private dydxHttpUrl(): string {
    return String(process.env.DECENTRALIZED_DOM_DYDX_HTTP_URL || DEFAULT_DYDX_HTTP_URL)
      .trim()
      .replace(/\/+$/, '');
  }

  private hyperliquidHttpUrl(): string {
    return String(process.env.DECENTRALIZED_DOM_HYPERLIQUID_HTTP_URL || DEFAULT_HYPERLIQUID_HTTP_URL)
      .trim();
  }

  historyDirectory(): string {
    const explicit = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
    if (explicit) return explicit;
    const monitorStateFile = String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim();
    const renderDisk = path.join(path.parse(process.cwd()).root, 'app', 'data');
    const base = monitorStateFile
      ? path.dirname(monitorStateFile)
      : fs.existsSync(renderDisk)
        ? renderDisk
        : path.join(process.cwd(), 'data');
    return path.join(base, 'decentralized-dom');
  }

  start(): void {
    if (!this.enabled() || this.interval) return;
    fs.mkdirSync(this.historyDirectory(), { recursive: true });
    this.pruneHistory();
    this.connectTradeStream('dydx');
    this.connectTradeStream('hyperliquid');
    this.poll().catch((error) => console.error('Initial decentralized DOM collection failed:', error));
    this.interval = setInterval(() => {
      this.poll().catch((error) => console.error('Decentralized DOM collection failed:', error));
    }, this.pollSeconds() * 1000);
    console.log('Decentralized DOM collector started:', {
      market: MARKET,
      sources: this.sourceStatus.map((source) => source.venue),
      pollSeconds: this.pollSeconds(),
      bucketSeconds: this.bucketSeconds(),
      historyDirectory: this.historyDirectory(),
      readOnly: true
    });
  }

  stop(): void {
    if (this.interval) clearInterval(this.interval);
    this.interval = undefined;
    for (const venue of ['dydx', 'hyperliquid'] as VenueName[]) {
      if (this.tradeSocketHeartbeats[venue]) clearInterval(this.tradeSocketHeartbeats[venue]);
      if (this.tradeSocketReconnects[venue]) clearTimeout(this.tradeSocketReconnects[venue]);
      this.tradeSockets[venue]?.close();
      this.tradeSocketHeartbeats[venue] = undefined;
      this.tradeSocketReconnects[venue] = undefined;
      this.tradeSockets[venue] = undefined;
      const source = this.sourceStatus.find((candidate) => candidate.venue === venue);
      if (source) source.tradeStreamConnected = false;
    }
  }

  private async fetchDydx(): Promise<VenueSnapshot> {
    const observedAt = Date.now();
    const baseUrl = this.dydxHttpUrl();
    const [book, trades] = await Promise.all([
      axios.get(`${baseUrl}/v4/orderbooks/perpetualMarket/BTC-USD`, { timeout: 12_000 }),
      axios.get(`${baseUrl}/v4/trades/perpetualMarket/BTC-USD?limit=100`, { timeout: 12_000 })
    ]);
    const snapshot = normalizeDydxSnapshot(book.data, trades.data, observedAt);
    snapshot.trades.push(...this.drainStreamedTrades('dydx'));
    return snapshot;
  }

  private async fetchHyperliquid(): Promise<VenueSnapshot> {
    const observedAt = Date.now();
    const [fineBook, depthBook, trades] = await Promise.all([
      axios.post(
        this.hyperliquidHttpUrl(),
        { type: 'l2Book', coin: 'BTC', nSigFigs: 5 },
        { timeout: 12_000, headers: { 'Content-Type': 'application/json' } }
      ),
      axios.post(
        this.hyperliquidHttpUrl(),
        { type: 'l2Book', coin: 'BTC', nSigFigs: 4 },
        { timeout: 12_000, headers: { 'Content-Type': 'application/json' } }
      ),
      axios.post(
        this.hyperliquidHttpUrl(),
        { type: 'recentTrades', coin: 'BTC' },
        { timeout: 12_000, headers: { 'Content-Type': 'application/json' } }
      )
    ]);
    const snapshot = normalizeHyperliquidSnapshot(
      { ...depthBook.data, depthLevels: depthBook.data?.levels, fineLevels: fineBook.data?.levels },
      trades.data,
      observedAt
    );
    snapshot.trades.push(...this.drainStreamedTrades('hyperliquid'));
    return snapshot;
  }

  private connectTradeStream(venue: VenueName): void {
    if (!this.enabled() || this.tradeSockets[venue]) return;
    const source = this.sourceStatus.find((candidate) => candidate.venue === venue)!;
    const url = venue === 'dydx'
      ? String(process.env.DECENTRALIZED_DOM_DYDX_WS_URL || 'wss://indexer.dydx.trade/v4/ws')
      : String(process.env.DECENTRALIZED_DOM_HYPERLIQUID_WS_URL || 'wss://api.hyperliquid.xyz/ws');
    const socket = new WebSocketClient(url);
    this.tradeSockets[venue] = socket;

    socket.on('open', () => {
      source.tradeStreamConnected = true;
      source.lastError = undefined;
      const subscription = venue === 'dydx'
        ? { type: 'subscribe', channel: 'v4_trades', id: MARKET, batched: false }
        : { method: 'subscribe', subscription: { type: 'trades', coin: 'BTC' } };
      socket.send(JSON.stringify(subscription));
      if (this.tradeSocketHeartbeats[venue]) clearInterval(this.tradeSocketHeartbeats[venue]);
      this.tradeSocketHeartbeats[venue] = setInterval(() => {
        if (socket.readyState !== WebSocketClient.OPEN) return;
        if (venue === 'hyperliquid') socket.send(JSON.stringify({ method: 'ping' }));
        else socket.ping();
      }, 30_000);
    });

    socket.on('message', (raw: any) => {
      try {
        const message = JSON.parse(String(raw));
        if (venue === 'dydx') {
          if (message?.type !== 'channel_data' || message?.channel !== 'v4_trades') return;
          const trades = normalizeDydxSnapshot({}, message?.contents, Date.now()).trades;
          this.queueStreamedTrades(venue, trades);
          return;
        }
        if (message?.channel !== 'trades') return;
        const trades = normalizeHyperliquidSnapshot({}, message?.data, Date.now()).trades;
        this.queueStreamedTrades(venue, trades);
      } catch (error) {
        source.lastErrorAt = new Date().toISOString();
        source.lastError = `Trade stream parse failed: ${error instanceof Error ? error.message : String(error)}`;
      }
    });

    socket.on('error', (error: Error) => {
      source.lastErrorAt = new Date().toISOString();
      source.lastError = `Trade stream: ${error.message}`;
    });

    socket.on('close', () => {
      source.tradeStreamConnected = false;
      if (this.tradeSocketHeartbeats[venue]) clearInterval(this.tradeSocketHeartbeats[venue]);
      this.tradeSocketHeartbeats[venue] = undefined;
      this.tradeSockets[venue] = undefined;
      if (!this.interval || this.tradeSocketReconnects[venue]) return;
      this.tradeSocketReconnects[venue] = setTimeout(() => {
        this.tradeSocketReconnects[venue] = undefined;
        this.connectTradeStream(venue);
      }, 5_000);
    });
  }

  private queueStreamedTrades(venue: VenueName, trades: DomTrade[]): void {
    const queue = this.streamedTrades[venue] || [];
    queue.push(...trades);
    this.streamedTrades[venue] = queue.slice(-10_000);
  }

  private drainStreamedTrades(venue: VenueName): DomTrade[] {
    const trades = this.streamedTrades[venue] || [];
    this.streamedTrades[venue] = [];
    return trades;
  }

  async poll(): Promise<void> {
    if (this.pollPromise) return this.pollPromise;
    this.pollPromise = this.pollInternal().finally(() => {
      this.pollPromise = undefined;
    });
    return this.pollPromise;
  }

  private async pollInternal(): Promise<void> {
    const now = Date.now();
    const bucketMs = this.bucketSeconds() * 1000;
    const nextBucketStart = Math.floor(now / bucketMs) * bucketMs;
    if (this.bucketStartMs !== undefined && nextBucketStart > this.bucketStartMs) {
      this.flushBucket();
    }
    if (this.bucketStartMs === undefined) {
      this.bucketStartMs = nextBucketStart;
      this.accumulators = {
        dydx: createVenueAccumulator(),
        hyperliquid: createVenueAccumulator()
      };
    }

    const results = await Promise.allSettled([this.fetchDydx(), this.fetchHyperliquid()]);
    const venues: VenueName[] = ['dydx', 'hyperliquid'];
    results.forEach((result, index) => {
      const venue = venues[index];
      const sourceStatus = this.sourceStatus.find((source) => source.venue === venue)!;
      const accumulator = this.accumulators[venue] || createVenueAccumulator();
      this.accumulators[venue] = accumulator;
      if (result.status === 'rejected') {
        accumulator.failedPolls += 1;
        sourceStatus.lastErrorAt = new Date().toISOString();
        sourceStatus.lastError = result.reason instanceof Error
          ? result.reason.message
          : String(result.reason);
        return;
      }

      const snapshot = result.value;
      const metrics = calculateBookMetrics(snapshot.bids, snapshot.asks, snapshot.topOfBook);
      if (!metrics) {
        accumulator.failedPolls += 1;
        sourceStatus.lastErrorAt = new Date().toISOString();
        sourceStatus.lastError = 'Order book did not contain a valid two-sided market.';
        return;
      }
      sourceStatus.lastSuccessAt = new Date(snapshot.observedAt).toISOString();
      sourceStatus.lastError = undefined;
      const changes = calculateBookChanges(this.previousBooks[venue], snapshot, metrics.mid, 25);
      this.previousBooks[venue] = { bids: snapshot.bids, asks: snapshot.asks };
      const newTrades = this.takeNewTrades(venue, snapshot.trades);
      aggregateVenue(accumulator, snapshot, metrics, changes, newTrades);
    });
  }

  private takeNewTrades(venue: VenueName, trades: DomTrade[]): DomTrade[] {
    const seen = this.seenTradeIds[venue] || new Set<string>();
    this.seenTradeIds[venue] = seen;
    if (!this.initializedTrades[venue]) {
      trades.forEach((trade) => seen.add(trade.id));
      this.initializedTrades[venue] = true;
      return [];
    }
    const fresh = trades.filter((trade) => !seen.has(trade.id));
    trades.forEach((trade) => seen.add(trade.id));
    if (seen.size > 10_000) {
      const recent = [...seen].slice(-5_000);
      this.seenTradeIds[venue] = new Set(recent);
    }
    return fresh;
  }

  private flushBucket(): void {
    if (this.bucketStartMs === undefined) return;
    const record = buildDomMinuteRecord(this.bucketStartMs, this.bucketSeconds(), this.accumulators);
    if (record.crossVenue.availableVenues > 0) {
      const file = path.join(
        this.historyDirectory(),
        `dom-${record.bucketStart.slice(0, 10)}.ndjson`
      );
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.appendFileSync(file, `${JSON.stringify(record)}\n`);
      this.latestRecord = record;
      this.storedRecords += 1;
    }
    const bucketMs = this.bucketSeconds() * 1000;
    this.bucketStartMs = Math.floor(Date.now() / bucketMs) * bucketMs;
    this.accumulators = {
      dydx: createVenueAccumulator(),
      hyperliquid: createVenueAccumulator()
    };
  }

  private currentRecord(): DomMinuteRecord | undefined {
    if (this.bucketStartMs === undefined) return undefined;
    return buildDomMinuteRecord(this.bucketStartMs, this.bucketSeconds(), this.accumulators);
  }

  getStatus(): CollectorStatus {
    return {
      enabled: this.enabled(),
      running: Boolean(this.interval),
      readOnly: true,
      market: MARKET,
      pollSeconds: this.pollSeconds(),
      bucketSeconds: this.bucketSeconds(),
      sources: this.sourceStatus,
      historyDirectory: this.historyDirectory(),
      currentBucketStart: this.bucketStartMs === undefined
        ? undefined
        : new Date(this.bucketStartMs).toISOString(),
      lastStoredAt: this.latestRecord?.bucketEnd,
      storedRecordsThisRun: this.storedRecords,
      coverage: this.getCoverage(),
      latest: this.latestRecord,
      current: this.currentRecord()
    };
  }

  getCoverage(): { from?: string; to?: string; sourceFiles: number } {
    const directory = this.historyDirectory();
    if (!fs.existsSync(directory)) return { sourceFiles: 0 };
    const files = fs.readdirSync(directory)
      .filter((name) => /^dom-\d{4}-\d{2}-\d{2}\.ndjson$/.test(name))
      .sort();
    if (!files.length) return { sourceFiles: 0 };

    const firstRecord = this.readBoundaryRecord(path.join(directory, files[0]), false);
    const lastRecord = this.readBoundaryRecord(path.join(directory, files[files.length - 1]), true);
    return {
      from: firstRecord?.bucketStart,
      to: lastRecord?.bucketEnd,
      sourceFiles: files.length
    };
  }

  getHistory(input: { from?: string; to?: string; maxPoints?: number }): {
    ok: true;
    readOnly: true;
    market: 'BTC-USD';
    from: string;
    to: string;
    records: DomMinuteRecord[];
    sourceFiles: number;
  } {
    const now = Date.now();
    const fromMs = Number.isFinite(Date.parse(String(input.from || '')))
      ? Date.parse(String(input.from))
      : now - 24 * 60 * 60 * 1000;
    const toMs = Number.isFinite(Date.parse(String(input.to || '')))
      ? Date.parse(String(input.to))
      : now;
    const safeFrom = Math.max(now - this.retentionDays() * 86_400_000, Math.min(fromMs, toMs));
    const safeTo = Math.max(safeFrom, Math.min(Math.max(fromMs, toMs), now + 60_000));
    const files = this.historyFilesBetween(safeFrom, safeTo);
    const records: DomMinuteRecord[] = [];
    for (const file of files) {
      try {
        const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean);
        for (const line of lines) {
          const record = JSON.parse(line) as DomMinuteRecord;
          const timestamp = Date.parse(record.bucketStart);
          if (timestamp >= safeFrom && timestamp <= safeTo) records.push(record);
        }
      } catch (error) {
        console.warn('Decentralized DOM history file could not be read:', {
          file,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }

    const maxPoints = boundedInteger(input.maxPoints, 720, 10, 5_000);
    const stride = Math.max(1, Math.ceil(records.length / maxPoints));
    const sampled = stride === 1
      ? records
      : records.filter((_record, index) => index % stride === 0 || index === records.length - 1);
    return {
      ok: true,
      readOnly: true,
      market: MARKET,
      from: new Date(safeFrom).toISOString(),
      to: new Date(safeTo).toISOString(),
      records: sampled,
      sourceFiles: files.length
    };
  }

  private historyFilesBetween(fromMs: number, toMs: number): string[] {
    const directory = this.historyDirectory();
    if (!fs.existsSync(directory)) return [];
    const dates = new Set<string>();
    for (let cursor = new Date(fromMs); cursor.getTime() <= toMs + 86_400_000; cursor = new Date(cursor.getTime() + 86_400_000)) {
      dates.add(cursor.toISOString().slice(0, 10));
    }
    return [...dates]
      .map((date) => path.join(directory, `dom-${date}.ndjson`))
      .filter((file) => fs.existsSync(file));
  }

  private readBoundaryRecord(file: string, fromEnd: boolean): DomMinuteRecord | undefined {
    try {
      const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean);
      const line = fromEnd ? lines[lines.length - 1] : lines[0];
      return line ? JSON.parse(line) as DomMinuteRecord : undefined;
    } catch {
      return undefined;
    }
  }

  private pruneHistory(): void {
    const directory = this.historyDirectory();
    if (!fs.existsSync(directory)) return;
    const cutoff = Date.now() - this.retentionDays() * 86_400_000;
    for (const name of fs.readdirSync(directory)) {
      const match = name.match(/^dom-(\d{4}-\d{2}-\d{2})\.ndjson$/);
      if (!match) continue;
      const timestamp = Date.parse(`${match[1]}T00:00:00Z`);
      if (Number.isFinite(timestamp) && timestamp < cutoff) {
        fs.unlinkSync(path.join(directory, name));
      }
    }
  }
}

export const decentralizedDomCollector = new DecentralizedDomCollector();
