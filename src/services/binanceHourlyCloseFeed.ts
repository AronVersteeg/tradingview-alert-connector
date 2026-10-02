import fs from 'fs';
import path from 'path';
import { BINANCE_DAILY_FRACTAL_MARKETS } from './binanceDailyFractalHistory';

const WebSocketClient = require('ws');
const HOUR_MS = 3_600_000;
const SYMBOLS: string[] = Object.values(BINANCE_DAILY_FRACTAL_MARKETS).map((market) => market.symbol);
export const BINANCE_HOURLY_CLOSE_STREAM_URL = `wss://fstream.binance.com/market/stream?streams=${SYMBOLS.map((symbol) => `${symbol.toLowerCase()}@kline_1h`).join('/')}`;

export type BinanceHourlyClose = { symbol: string; openTime: number; closeTime: number; close: string };

export function parseBinanceHourlyClose(payload: any, nowMs = Date.now()): BinanceHourlyClose | undefined {
  const data = payload?.data || payload;
  const k = data?.k;
  const symbol = String(data?.s || '').toUpperCase();
  const openTime = Number(k?.t);
  const closeTime = Number(k?.T);
  const close = String(k?.c || '');
  if (data?.e !== 'kline' || k?.x !== true || k?.i !== '1h' || k?.s !== symbol || !SYMBOLS.includes(symbol)) return undefined;
  if (!Number.isFinite(openTime) || openTime % HOUR_MS !== 0 || closeTime !== openTime + HOUR_MS - 1 || closeTime > nowMs) return undefined;
  if (!Number.isFinite(Number(close)) || !(Number(close) > 0)) return undefined;
  return { symbol, openTime, closeTime, close };
}

function stateFile(): string {
  const history = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  return path.join(history ? path.dirname(history) : path.join(process.cwd(), 'data'), 'binance-hourly-closes.json');
}

export class BinanceHourlyCloseFeed {
  private socket: any;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private heartbeat: NodeJS.Timeout | undefined;
  private started = false;
  private loaded = false;
  private failures = 0;
  private closes = new Map<string, BinanceHourlyClose>();
  private lastMessageAt: number | undefined;
  private lastError: string | undefined;

  start(): void {
    if (this.started) return;
    this.started = true;
    this.load();
    this.connect();
  }

  stop(): void {
    this.started = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.reconnectTimer = undefined;
    this.heartbeat = undefined;
    const socket = this.socket;
    this.socket = undefined;
    if (socket) socket.terminate();
  }

  latest(symbol: string, nowMs = Date.now()): BinanceHourlyClose | undefined {
    this.load();
    const candle = this.closes.get(symbol);
    const expectedOpen = (Math.floor(nowMs / HOUR_MS) - 1) * HOUR_MS;
    return candle?.openTime === expectedOpen && candle.closeTime <= nowMs ? { ...candle } : undefined;
  }

  status(): any {
    this.load();
    return {
      connected: this.socket?.readyState === WebSocketClient.OPEN,
      lastMessageAt: this.lastMessageAt ? new Date(this.lastMessageAt).toISOString() : undefined,
      lastError: this.lastError,
      symbols: SYMBOLS.map((symbol) => ({
        symbol,
        latestClosedAt: this.closes.get(symbol) ? new Date(this.closes.get(symbol)!.closeTime).toISOString() : undefined,
        current: Boolean(this.latest(symbol))
      }))
    };
  }

  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    try {
      const rows = JSON.parse(fs.readFileSync(stateFile(), 'utf8'));
      if (!Array.isArray(rows)) return;
      for (const row of rows) {
        const candle = parseBinanceHourlyClose({ e: 'kline', s: row.symbol, k: {
          s: row.symbol, i: '1h', x: true, t: row.openTime, T: row.closeTime, c: row.close
        } });
        if (candle) this.closes.set(candle.symbol, candle);
      }
    } catch { /* A fresh stream has no saved closes. */ }
  }

  private record(payload: any): void {
    const candle = parseBinanceHourlyClose(payload);
    if (!candle || candle.openTime <= (this.closes.get(candle.symbol)?.openTime ?? -1)) return;
    this.closes.set(candle.symbol, candle);
    try {
      const file = stateFile();
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const temporary = `${file}.${process.pid}.tmp`;
      fs.writeFileSync(temporary, JSON.stringify([...this.closes.values()]));
      fs.renameSync(temporary, file);
    } catch (error) {
      console.warn('Could not persist Binance hourly closes:', error instanceof Error ? error.message : String(error));
    }
  }

  private connect(): void {
    if (!this.started) return;
    const socket = new WebSocketClient(BINANCE_HOURLY_CLOSE_STREAM_URL, { handshakeTimeout: 15_000, maxPayload: 1_048_576 });
    this.socket = socket;
    let alive = true;
    socket.on('open', () => {
      if (this.socket !== socket || !this.started) { socket.terminate(); return; }
      this.lastError = undefined;
      console.log('Binance Futures hourly close stream connected.', { symbols: SYMBOLS });
      this.heartbeat = setInterval(() => {
        if (!alive) { socket.terminate(); return; }
        alive = false;
        if (socket.readyState === WebSocketClient.OPEN) socket.ping();
      }, 30_000);
    });
    socket.on('pong', () => { alive = true; });
    socket.on('message', (data: any) => {
      if (this.socket !== socket) return;
      alive = true;
      this.lastMessageAt = Date.now();
      this.failures = 0;
      try { this.record(JSON.parse(data.toString())); } catch { /* Ignore malformed frames. */ }
    });
    socket.on('error', (error: Error) => { if (this.socket === socket) this.lastError = error.message; });
    socket.on('close', () => {
      if (this.socket !== socket) return;
      if (this.heartbeat) clearInterval(this.heartbeat);
      this.heartbeat = undefined;
      if (this.socket === socket) this.socket = undefined;
      if (!this.started) return;
      this.failures += 1;
      const delay = Math.min(60_000, 5000 * 2 ** Math.min(4, this.failures - 1));
      console.warn('Binance Futures hourly close stream disconnected; reconnect scheduled.', { delayMs: delay, error: this.lastError });
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = undefined;
        this.connect();
      }, delay);
    });
  }
}

export const binanceHourlyCloseFeed = new BinanceHourlyCloseFeed();
