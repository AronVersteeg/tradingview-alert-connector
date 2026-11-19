import type { GreenLineSignal } from './ethGapDailyEntry';
import type { SpotCandle } from './openLiquidityV2Replica';

const WebSocketClient = require('ws');
const HOUR_MS = 3_600_000;
const MAX_CLOSE_AGE_MS = 300_000;
const HISTORY_LIMIT = 48;
const URL = 'wss://stream.binance.com:443/ws/btcusdt@kline_1h';

export function parseBtcSpotClosedHour(event: any, now: number): SpotCandle | undefined {
  const row = event?.k;
  const boundary = Math.floor(now / HOUR_MS) * HOUR_MS;
  if (event?.e !== 'kline' || event.s !== 'BTCUSDT' || row?.s !== 'BTCUSDT' || row.i !== '1h' || row.x !== true ||
      !Number.isSafeInteger(row.t) || row.t <= 0 || row.t !== boundary - HOUR_MS || row.T !== boundary - 1 ||
      now - boundary > MAX_CLOSE_AGE_MS) return undefined;
  const prices = [row.o, row.h, row.l, row.c];
  if (!prices.every(price => typeof price === 'string' && /^\d+(?:\.\d+)?$/.test(price))) return undefined;
  const [open, high, low, close] = prices.map(Number);
  if (![open, high, low, close].every(price => Number.isFinite(price) && price > 0) ||
      high < Math.max(open, close) || low > Math.min(open, close)) return undefined;
  return { timestampMs: row.t, closeTimeMs: row.T, open, high, low, close };
}

type GreenObservation = ReturnType<typeof btcGreenObservation>;

export function btcGreenObservation(
  payload: any, intrusions: { direction: string; signature: string }[], signal?: GreenLineSignal
) {
  return {
    candles: payload.frames.slice(-2).map((row: any) => [row.startedAtMs, row.open, row.high, row.low, row.close]),
    gaps: payload.gaps?.slice(-2).map((gap: any) => gap ? [gap.left, gap.right] : null),
    intrusions: intrusions.map(row => `${row.direction}|${row.signature}`).sort(),
    decision: signal ? 'GREEN' : intrusions.length ? 'NO_GAP_BREAK' : 'NO_INTRUSION',
    signal: signal ? { direction: signal.direction, edge: signal.edge, time: signal.time, close: signal.close } : null
  };
}

type HourRecord = {
  closedAt: number;
  stream?: { receivedAt: number; evaluatedAt?: number; decision?: GreenObservation; unavailable?: string };
  rest?: { evaluatedAt: number; decision: GreenObservation };
  comparison?: { result: 'MATCH' | 'MISMATCH'; fields: string[]; leadMs: number };
};

export class BtcSpotCloseShadow {
  private socket?: any;
  private heartbeat?: NodeJS.Timeout;
  private reconnect?: NodeJS.Timeout;
  private running = false;
  private failures = 0;
  private lastError?: string;
  private lastMessageAt?: number;
  private readonly records = new Map<number, HourRecord>();

  constructor(private readonly preview: (candle: SpotCandle, now: number) => GreenObservation | undefined,
    private readonly openSocket = () => new WebSocketClient(URL, { handshakeTimeout: 15_000, maxPayload: 64 * 1024 })) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.connect();
  }

  stop(): void {
    this.running = false;
    if (this.reconnect) clearTimeout(this.reconnect);
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.reconnect = this.heartbeat = undefined;
    const socket = this.socket;
    this.socket = undefined;
    socket?.terminate();
  }

  observeRest(closedAt: number, decision: GreenObservation, now = Date.now()): void {
    if (!this.running || now < closedAt || now - closedAt > MAX_CLOSE_AGE_MS) return;
    const record = this.record(closedAt);
    if (record.rest) return;
    record.rest = { evaluatedAt: now, decision };
    this.compare(record);
  }

  status() {
    return {
      readOnly: true,
      running: this.running,
      connected: this.socket?.readyState === WebSocketClient.OPEN,
      lastMessageAt: this.lastMessageAt === undefined ? undefined : new Date(this.lastMessageAt).toISOString(),
      lastError: this.lastError,
      recent: [...this.records.values()].reverse().map(row => ({
        candleClosedAt: new Date(row.closedAt).toISOString(),
        streamAfterCloseMs: row.stream && row.stream.receivedAt - row.closedAt,
        shadowDecisionAfterCloseMs: row.stream?.evaluatedAt === undefined ? undefined : row.stream.evaluatedAt - row.closedAt,
        restDecisionAfterCloseMs: row.rest && row.rest.evaluatedAt - row.closedAt,
        shadowDecision: row.stream?.decision?.decision,
        restDecision: row.rest?.decision.decision,
        unavailable: row.stream?.unavailable,
        comparison: row.comparison
      }))
    };
  }

  private record(closedAt: number): HourRecord {
    const existing = this.records.get(closedAt);
    if (existing) return existing;
    const added = { closedAt };
    this.records.set(closedAt, added);
    if (this.records.size > HISTORY_LIMIT) this.records.delete(this.records.keys().next().value!);
    return added;
  }

  private receive(event: any, now: number): void {
    const candle = parseBtcSpotClosedHour(event, now);
    if (!candle) return;
    const record = this.record(candle.closeTimeMs + 1);
    if (record.stream) return;
    record.stream = { receivedAt: now };
    try {
      record.stream.decision = this.preview(candle, now);
      if (record.stream.decision) record.stream.evaluatedAt = Date.now();
      else record.stream.unavailable = 'No consecutive warmed REST replay base for this close.';
    } catch (error) {
      record.stream.unavailable = error instanceof Error ? error.message : String(error);
    }
    if (record.stream.unavailable) console.log('BTC Spot shadow close not evaluated:', {
      candleClosedAt: new Date(record.closedAt).toISOString(), reason: record.stream.unavailable
    });
    this.compare(record);
  }

  private compare(record: HourRecord): void {
    const shadow = record.stream?.decision, rest = record.rest?.decision;
    if (!shadow || !rest || record.comparison) return;
    const fields = (Object.keys(shadow) as (keyof GreenObservation)[])
      .filter(key => JSON.stringify(shadow[key]) !== JSON.stringify(rest[key]));
    record.comparison = { result: fields.length ? 'MISMATCH' : 'MATCH', fields,
      leadMs: record.rest!.evaluatedAt - record.stream!.evaluatedAt! };
    console.log('BTC Spot shadow close compared:', {
      candleClosedAt: new Date(record.closedAt).toISOString(), ...record.comparison,
      shadowDecisionAfterCloseMs: record.stream!.evaluatedAt! - record.closedAt,
      restDecisionAfterCloseMs: record.rest!.evaluatedAt - record.closedAt,
      shadowDecision: shadow.decision, restDecision: rest.decision
    });
  }

  private scheduleReconnect(): void {
    if (!this.running || this.reconnect) return;
    const delay = Math.min(60_000, 5_000 * 2 ** Math.min(4, this.failures++));
    this.reconnect = setTimeout(() => {
      this.reconnect = undefined;
      this.connect();
    }, delay);
    this.reconnect.unref();
  }

  private connect(): void {
    if (!this.running || this.socket) return;
    let socket: any;
    try {
      socket = this.openSocket();
      this.socket = socket;
    } catch (error) {
      this.lastError = error instanceof Error ? error.message : String(error);
      this.scheduleReconnect();
      return;
    }
    let alive = true;
    socket.on('open', () => {
      if (this.socket !== socket || !this.running) return;
      this.lastError = undefined;
      this.heartbeat = setInterval(() => {
        if (!alive) { socket.terminate(); return; }
        alive = false;
        if (socket.readyState === WebSocketClient.OPEN) socket.ping();
      }, 30_000);
      this.heartbeat.unref();
    });
    socket.on('pong', () => { alive = true; });
    socket.on('message', (data: Buffer) => {
      if (this.socket !== socket || !this.running) return;
      alive = true;
      this.failures = 0;
      this.lastMessageAt = Date.now();
      let parsed: any;
      try { parsed = JSON.parse(data.toString()); } catch { return; }
      this.receive(parsed, this.lastMessageAt);
    });
    socket.on('error', (error: Error) => { if (this.socket === socket) this.lastError = error.message; });
    socket.on('close', () => {
      if (this.socket !== socket) return;
      this.socket = undefined;
      if (this.heartbeat) clearInterval(this.heartbeat);
      this.heartbeat = undefined;
      this.scheduleReconnect();
    });
  }
}
