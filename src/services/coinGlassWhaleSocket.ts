import zlib from 'zlib';

const WebSocketClient = require('ws');
const CHANNEL = 'largeTakerOrder';
const MAX_MESSAGE_BYTES = 8 * 1024 * 1024;

export function coinGlassSnapshotTimeoutMs(requested?: number): number {
  // Snapshots can arrive on the next m1 push, rather than immediately on subscribe.
  return Math.max(75_000, Math.min(120_000, Number.isFinite(requested) ? requested as number : 75_000));
}

export function decodeCoinGlassMessage(payload: Buffer | string): any | undefined {
  const buffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const attempts = [
    () => buffer.toString('utf8'),
    () => zlib.gunzipSync(buffer, { maxOutputLength: MAX_MESSAGE_BYTES }).toString('utf8'),
    () => zlib.inflateSync(buffer, { maxOutputLength: MAX_MESSAGE_BYTES }).toString('utf8'),
    () => zlib.inflateRawSync(buffer, { maxOutputLength: MAX_MESSAGE_BYTES }).toString('utf8')
  ];
  for (const attempt of attempts) {
    try {
      return JSON.parse(attempt());
    } catch {
      // Heartbeats are not JSON; only accept a fully decoded JSON message.
    }
  }
  return undefined;
}

export function fetchCoinGlassWhaleMessage(
  symbol: string,
  interval: string,
  timeoutMs?: number
): Promise<any> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocketClient('wss://wss.coinglass.com/v2/ws', {
      origin: 'https://www.coinglass.com',
      handshakeTimeout: 10_000,
      perMessageDeflate: false,
      maxPayload: MAX_MESSAGE_BYTES,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    let settled = false;
    let opened = false;
    let receivedMessages = 0;
    let decodedMessages = 0;
    let timer: NodeJS.Timeout;
    let heartbeat: NodeJS.Timeout | undefined;
    function finish(error: Error | null, message?: any): void {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (heartbeat) clearInterval(heartbeat);
      socket.terminate();
      if (error) reject(error);
      else resolve(message);
    }
    function timeout(): void {
      finish(new Error(`CoinGlass whale WebSocket timed out (${opened ? 'snapshot' : 'connect/upgrade'}; symbol ${symbol}; messages ${receivedMessages}; decoded ${decodedMessages}).`));
    }
    timer = setTimeout(timeout, 10_000);
    socket.on('open', () => {
      if (settled) return;
      opened = true;
      clearTimeout(timer);
      timer = setTimeout(timeout, coinGlassSnapshotTimeoutMs(timeoutMs));
      socket.send(JSON.stringify({
        method: 'subscribe',
        params: [{ listenerGuid: `${symbol}#_${CHANNEL}_${interval}`, symbol, interval, channel: CHANNEL }]
      }));
      heartbeat = setInterval(() => {
        if (socket.readyState === WebSocketClient.OPEN && !settled) socket.send('ping');
      }, 5_000);
    });
    socket.on('message', (payload: Buffer | string) => {
      if (settled) return;
      receivedMessages += 1;
      const message = decodeCoinGlassMessage(payload);
      if (!message) return;
      decodedMessages += 1;
      if (message.channel !== CHANNEL || !Array.isArray(message.data)) return;
      if (message.params?.symbol && message.params.symbol !== symbol) return;
      if (message.params?.interval && message.params.interval !== interval) return;
      finish(null, message);
    });
    socket.on('error', (error: Error) => finish(error));
    socket.on('close', (code: number) => {
      if (!settled) finish(new Error(`CoinGlass whale WebSocket closed before a snapshot arrived (symbol ${symbol}; code ${code}).`));
    });
  });
}
