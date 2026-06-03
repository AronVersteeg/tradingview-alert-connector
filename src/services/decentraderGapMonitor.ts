import axios from 'axios';
import fs from 'fs';
import net from 'net';
import path from 'path';
import tls from 'tls';

const API_URL = 'https://www.decentrader.com/api';
const LEVERAGES = [3, 5, 10];

type DecentraderRow = Record<string, any>;

type LiquidityBar = {
  key: string;
  side: 'L' | 'S';
  leverage: number;
  price: number;
  count: number;
  newCount?: number;
  gapSide?: 'left' | 'right';
  sideOfPrice?: 'left' | 'right' | 'price';
};

type Gap = {
  left: number;
  right: number;
  width: number;
  price: number;
  leftEdge: LiquidityBar;
  rightEdge: LiquidityBar;
  leftToPrice: number;
  rightToPrice: number;
};

type GapAlert = {
  timestamp: string;
  timestampNl: string;
  price: number;
  previousGap: Gap;
  entrants: LiquidityBar[];
  left: LiquidityBar[];
  right: LiquidityBar[];
};

type MonitorStatus = {
  enabled: boolean;
  running: boolean;
  symbol: string;
  pollMinutes: number;
  hasSmtp: boolean;
  lastStartedAt?: string;
  lastFinishedAt?: string;
  lastError?: string;
  lastResult?: any;
};

type SmtpSettings = {
  host: string;
  port: number;
  username: string;
  password: string;
  sender: string;
  recipients: string[];
  useTls: boolean;
  useSsl: boolean;
  timeoutMs: number;
  jobName: string;
};

type AlertState = {
  lastAlertObservedSignature?: string | null;
  lastAlertSentSignature?: string;
  lastAlertSentAt?: string;
  lastCheckedAt?: string;
  lastDataTimestamp?: string;
};

function parseBool(value: string | undefined, fallback = false): boolean {
  if (value === undefined || value.trim() === '') return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(value.trim().toLowerCase());
}

function parseRecipients(value: string | undefined): string[] {
  return String(value || '')
    .replace(/;/g, ',')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseNumber(value: any): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function money(value: number | undefined): string {
  if (!Number.isFinite(value)) return '-';
  return '$' + Math.round(value as number).toLocaleString('en-US');
}

function priceKey(price: number): string {
  return price.toFixed(8).replace(/0+$/, '').replace(/\.$/, '');
}

function nlTime(timestamp: string | undefined): string {
  if (!timestamp) return '-';
  const date = new Date(timestamp.replace(' ', 'T') + 'Z');
  return new Intl.DateTimeFormat('nl-NL', {
    timeZone: 'Europe/Amsterdam',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  })
    .format(date)
    .replace(',', '') + ' NL';
}

function nowNlIso(): string {
  return new Date().toISOString();
}

function smtpSettingsFromEnv(): SmtpSettings | undefined {
  const host = String(process.env.SMTP_HOST || '').trim();
  const recipients = parseRecipients(process.env.SMTP_TO || process.env.NOTIFY_EMAIL);
  if (!host || !recipients.length) return undefined;

  const username = String(process.env.SMTP_USERNAME || process.env.SMTP_USER || '').trim();
  const password = String(process.env.SMTP_PASSWORD || process.env.SMTP_APP_PASSWORD || '');
  const sender = String(process.env.SMTP_FROM || process.env.NOTIFY_FROM || '').trim() || username;
  if (!sender) return undefined;

  const useSsl = parseBool(process.env.SMTP_USE_SSL, false);
  const defaultPort = useSsl ? 465 : 587;
  return {
    host,
    port: Number(process.env.SMTP_PORT || defaultPort),
    username,
    password,
    sender,
    recipients,
    useTls: parseBool(process.env.SMTP_USE_TLS, !useSsl),
    useSsl,
    timeoutMs: smtpTimeoutMs(),
    jobName:
      String(process.env.DECENTRADER_GAP_ALERT_JOB_NAME || 'Decentrader BTC gap monitor').trim() ||
      'Decentrader BTC gap monitor'
  };
}

function smtpTimeoutMs(): number {
  const timeoutMs = Number(process.env.SMTP_TIMEOUT_MS || '');
  if (Number.isFinite(timeoutMs) && timeoutMs > 0) return timeoutMs;

  const timeoutSeconds = Number(process.env.SMTP_TIMEOUT_SECONDS || '');
  if (Number.isFinite(timeoutSeconds) && timeoutSeconds > 0) {
    return timeoutSeconds * 1000;
  }

  return 20000;
}

function escapeEmailHeader(value: string): string {
  return value.replace(/[\r\n]/g, ' ').trim();
}

function dotEscape(body: string): string {
  return body.replace(/\r?\n/g, '\r\n').replace(/^\./gm, '..');
}

class SmtpSession {
  private socket: net.Socket | tls.TLSSocket | undefined;
  private buffer = '';

  constructor(private settings: SmtpSettings) {}

  async send(subject: string, body: string): Promise<void> {
    await this.connect();
    await this.readResponse();
    await this.command(`EHLO ${this.localName()}`);

    if (this.settings.useTls && !this.settings.useSsl) {
      await this.command('STARTTLS');
      this.socket = await new Promise<tls.TLSSocket>((resolve, reject) => {
        const secureSocket = tls.connect({
          socket: this.socket as net.Socket,
          servername: this.settings.host
        });
        secureSocket.once('secureConnect', () => resolve(secureSocket));
        secureSocket.once('error', reject);
        secureSocket.on('data', (chunk) => {
          this.buffer += chunk.toString('utf8');
        });
      });
      this.buffer = '';
      await this.command(`EHLO ${this.localName()}`);
    }

    if (this.settings.username || this.settings.password) {
      await this.command('AUTH LOGIN');
      await this.command(Buffer.from(this.settings.username).toString('base64'));
      await this.command(Buffer.from(this.settings.password).toString('base64'));
    }

    await this.command(`MAIL FROM:<${this.settings.sender}>`);
    for (const recipient of this.settings.recipients) {
      await this.command(`RCPT TO:<${recipient}>`);
    }

    await this.command('DATA');
    await this.writeRaw(this.buildMessage(subject, body) + '\r\n.');
    await this.readResponse();
    await this.command('QUIT', false);
    this.socket?.end();
  }

  private connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      const socket = this.settings.useSsl
        ? tls.connect(
            {
              port: this.settings.port,
              host: this.settings.host,
              servername: this.settings.host
            },
            resolve
          )
        : net.connect(
            {
              port: this.settings.port,
              host: this.settings.host
            },
            resolve
          );
      socket.setTimeout(this.settings.timeoutMs, () => {
        socket.destroy(new Error('SMTP timeout'));
      });
      socket.once('error', onError);
      socket.on('data', (chunk) => {
        this.buffer += chunk.toString('utf8');
      });
      this.socket = socket;
    });
  }

  private localName(): string {
    return 'render-decentrader-gap-monitor.local';
  }

  private buildMessage(subject: string, body: string): string {
    return [
      `From: ${escapeEmailHeader(this.settings.sender)}`,
      `To: ${escapeEmailHeader(this.settings.recipients.join(', '))}`,
      `Subject: ${escapeEmailHeader(subject)}`,
      `Date: ${new Date().toUTCString()}`,
      'Content-Type: text/plain; charset=utf-8',
      '',
      dotEscape(body)
    ].join('\r\n');
  }

  private async command(command: string, expectOk = true): Promise<string> {
    await this.writeRaw(command);
    const response = await this.readResponse();
    if (expectOk && !/^[23]/.test(response)) {
      throw new Error(`SMTP command failed (${command}): ${response.trim()}`);
    }
    return response;
  }

  private writeRaw(value: string): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socket) return reject(new Error('SMTP socket is not connected'));
      this.socket.write(value + '\r\n', (error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  private readResponse(): Promise<string> {
    return new Promise((resolve, reject) => {
      const startedAt = Date.now();
      const tick = () => {
        const lines = this.buffer.split(/\r?\n/).filter(Boolean);
        const lastLine = lines[lines.length - 1] || '';
        if (/^\d{3} /.test(lastLine)) {
          const response = this.buffer;
          this.buffer = '';
          resolve(response);
          return;
        }
        if (Date.now() - startedAt > this.settings.timeoutMs) {
          reject(new Error('SMTP response timeout'));
          return;
        }
        setTimeout(tick, 25);
      };
      tick();
    });
  }
}

async function sendEmail(settings: SmtpSettings, subject: string, body: string): Promise<void> {
  await new SmtpSession(settings).send(subject, body);
}

function eventForRow(row: DecentraderRow, rowIndex: number, side: 'long' | 'short', leverage: number) {
  const prefix = `${side}${leverage}`;
  return {
    rowIndex,
    side,
    leverage,
    roundedPrice: parseNumber(row[`${prefix}Rounded`]),
    active: Number(row[`${prefix}Active`] || 0)
  };
}

function activeBarsForFrame(rows: DecentraderRow[], frameIndex: number): LiquidityBar[] {
  const bars = new Map<string, LiquidityBar>();
  for (let rowIndex = 0; rowIndex <= frameIndex; rowIndex++) {
    const row = rows[rowIndex];
    for (const side of ['long', 'short'] as const) {
      for (const leverage of LEVERAGES) {
        const event = eventForRow(row, rowIndex, side, leverage);
        if (!event.active || event.roundedPrice === undefined) continue;

        const compactSide = side === 'long' ? 'L' : 'S';
        const key = `${compactSide}|${leverage}|${priceKey(event.roundedPrice)}`;
        const existing =
          bars.get(key) ||
          ({
            key,
            side: compactSide,
            leverage,
            price: event.roundedPrice,
            count: 0
          } as LiquidityBar);
        existing.count += 1;
        bars.set(key, existing);
      }
    }
  }
  return Array.from(bars.values());
}

function cleanGapForBars(frame: DecentraderRow, bars: LiquidityBar[]): Gap | undefined {
  const price = parseNumber(frame.ohlc4);
  if (price === undefined) return undefined;

  const leftBars = bars.filter((bar) => bar.price < price);
  const rightBars = bars.filter((bar) => bar.price > price);
  if (!leftBars.length || !rightBars.length) return undefined;

  const leftEdge = leftBars.reduce((best, bar) => (bar.price > best.price ? bar : best));
  const rightEdge = rightBars.reduce((best, bar) => (bar.price < best.price ? bar : best));
  if (leftEdge.price >= rightEdge.price) return undefined;

  return {
    left: leftEdge.price,
    right: rightEdge.price,
    width: rightEdge.price - leftEdge.price,
    price,
    leftEdge,
    rightEdge,
    leftToPrice: price - leftEdge.price,
    rightToPrice: rightEdge.price - price
  };
}

function detectGapIntrusion(rows: DecentraderRow[], frameIndex: number): GapAlert | undefined {
  if (frameIndex <= 0) return undefined;

  const previousFrame = rows[frameIndex - 1];
  const currentFrame = rows[frameIndex];
  const currentPrice = parseNumber(currentFrame.ohlc4);
  if (currentPrice === undefined) return undefined;

  const previousBars = activeBarsForFrame(rows, frameIndex - 1);
  const currentBars = activeBarsForFrame(rows, frameIndex);
  const previousGap = cleanGapForBars(previousFrame, previousBars);
  if (!previousGap) return undefined;

  const previousCounts = new Map(previousBars.map((bar) => [bar.key, bar.count]));
  const entrants = currentBars
    .filter((bar) => bar.count > (previousCounts.get(bar.key) || 0))
    .filter((bar) => bar.price > previousGap.left && bar.price < previousGap.right)
    .map((bar) => ({
      ...bar,
      newCount: bar.count - (previousCounts.get(bar.key) || 0),
      gapSide:
        bar.price - previousGap.left <= previousGap.right - bar.price
          ? 'left'
          : 'right',
      sideOfPrice: bar.price < currentPrice ? 'left' : bar.price > currentPrice ? 'right' : 'price'
    })) as LiquidityBar[];

  return {
    timestamp: String(currentFrame.timestamp || ''),
    timestampNl: nlTime(currentFrame.timestamp),
    price: currentPrice,
    previousGap,
    entrants,
    left: entrants.filter((bar) => bar.gapSide === 'left'),
    right: entrants.filter((bar) => bar.gapSide === 'right')
  };
}

function gapIntrusionsSince(rows: DecentraderRow[], lastDataTimestamp?: string): GapAlert[] {
  if (rows.length < 2) return [];

  let startIndex = rows.length - 1;
  if (lastDataTimestamp) {
    const lastIndex = rows.findIndex((row) => String(row.timestamp || '') === lastDataTimestamp);
    startIndex = lastIndex >= 0 ? lastIndex : rows.length - 1;
  }

  const alerts: GapAlert[] = [];
  for (let frameIndex = Math.max(1, startIndex); frameIndex < rows.length; frameIndex += 1) {
    const alert = detectGapIntrusion(rows, frameIndex);
    if (alert?.entrants.length) alerts.push(alert);
  }

  return alerts;
}

function buildTimelapsePayload(rows: DecentraderRow[], symbol: string): any {
  const events: any[] = [];
  const prices: number[] = [];
  const firstSeen = new Set<string>();
  const currentZoneCounts = new Map<string, { s: 'L' | 'S'; l: number; p: number; c: number }>();

  rows.forEach((row, rowIndex) => {
    const framePrice = parseNumber(row.ohlc4);
    if (framePrice !== undefined) prices.push(framePrice);

    for (const side of ['long', 'short'] as const) {
      for (const leverage of LEVERAGES) {
        const event = eventForRow(row, rowIndex, side, leverage);
        if (event.roundedPrice === undefined) continue;

        const compactSide = side === 'long' ? 'L' : 'S';
        const zoneKey = `${compactSide}|${leverage}|${priceKey(event.roundedPrice)}`;
        const isFirstSeen = !firstSeen.has(zoneKey);
        firstSeen.add(zoneKey);
        prices.push(event.roundedPrice);

        events.push({
          i: rowIndex,
          s: compactSide,
          l: leverage,
          p: event.roundedPrice,
          a: event.active ? 1 : 0,
          n: isFirstSeen ? 1 : 0
        });

        if (event.active) {
          const existing =
            currentZoneCounts.get(zoneKey) ||
            ({ s: compactSide, l: leverage, p: event.roundedPrice, c: 0 } as {
              s: 'L' | 'S';
              l: number;
              p: number;
              c: number;
            });
          existing.c += 1;
          currentZoneCounts.set(zoneKey, existing);
        }
      }
    }
  });

  return {
    source: {
      url: 'https://www.decentrader.com/liquidity-maps/?coin=btc',
      api: API_URL,
      method: 'getOHLCHourlyCalculations',
      params: [symbol],
      note:
        'Live cloud payload served by the Render Decentrader gap monitor. Times are Decentrader UTC and shown in the chart as Europe/Amsterdam.'
    },
    range: {
      minPrice: Math.min(...prices),
      maxPrice: Math.max(...prices)
    },
    frames: rows.map((row, index) => ({
      i: index,
      t: row.timestamp,
      price: parseNumber(row.ohlc4)
    })),
    events,
    topCurrentZones: Array.from(currentZoneCounts.values())
      .sort((a, b) => b.c - a.c || a.p - b.p)
      .slice(0, 40)
  };
}

function gapAlertSignature(alert: GapAlert): string {
  return `${alert.timestamp}|${alert.entrants
    .map((bar) => `${bar.key}:${bar.count}`)
    .sort()
    .join('|')}`;
}

function sideCounts(alert: GapAlert): string {
  const parts: string[] = [];
  if (alert.left.length) parts.push(`${alert.left.length} left edge`);
  if (alert.right.length) parts.push(`${alert.right.length} right edge`);
  if (!parts.length && alert.entrants.length) parts.push(`${alert.entrants.length} around price`);
  return parts.join(' + ') || 'no intrusions';
}

function alertBody(alert: GapAlert, symbol: string): string {
  const gap = alert.previousGap;
  const lines = [
    `Decentrader ${symbol.toUpperCase()} liquidity gap alert`,
    '',
    `Time: ${alert.timestampNl} (${alert.timestamp} UTC)`,
    `Price: ${money(alert.price)}`,
    `Previous clean gap: ${money(gap.left)} -> ${money(gap.right)}`,
    `Gap width: ${money(gap.width)}`,
    `Distance to price: L ${money(gap.leftToPrice)} / R ${money(gap.rightToPrice)}`,
    '',
    `New or expanded histos inside previous gap: ${sideCounts(alert)}`
  ];

  for (const bar of alert.entrants) {
    const gapSide = bar.gapSide ? `${bar.gapSide} edge` : 'inside gap';
    const priceSide =
      bar.sideOfPrice === 'price' ? 'at price' : `${bar.sideOfPrice || 'unknown'} of price`;
    const added = bar.newCount && bar.newCount > 1 ? ` +${bar.newCount}` : '';
    lines.push(
      `- ${bar.side === 'L' ? 'Long' : 'Short'} ${bar.leverage}x ${money(bar.price)}${added} (${gapSide}, ${priceSide})`
    );
  }

  lines.push('', 'Bron: https://www.decentrader.com/liquidity-maps/?coin=btc');
  lines.push('Let op: dit is een liquidity-gap event, geen automatisch trade-advies.');
  return lines.join('\n');
}

function readState(stateFile: string): AlertState {
  try {
    return JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  } catch {
    return {};
  }
}

function writeState(stateFile: string, state: AlertState): void {
  fs.mkdirSync(path.dirname(stateFile), { recursive: true });
  fs.writeFileSync(stateFile, JSON.stringify(state, null, 2));
}

async function fetchSnapshot(symbol: string): Promise<DecentraderRow[]> {
  const response = await axios.post(
    API_URL,
    {
      jsonrpc: '2.0',
      id: '0',
      method: 'getOHLCHourlyCalculations',
      params: [symbol]
    },
    {
      timeout: 45000,
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Origin: 'https://www.decentrader.com',
        Referer: 'https://www.decentrader.com/liquidity-maps/?coin=btc',
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36'
      }
    }
  );

  const result = response.data?.result;
  if (!Array.isArray(result)) {
    throw new Error('Decentrader response did not contain a result array');
  }
  return result;
}

export class DecentraderGapMonitor {
  private interval: NodeJS.Timeout | undefined;
  private latestRows: DecentraderRow[] | undefined;
  private latestTimelapsePayload: any | undefined;
  private status: MonitorStatus = {
    enabled: false,
    running: false,
    symbol: 'btcusdt',
    pollMinutes: 10,
    hasSmtp: false
  };

  start(): void {
    const config = this.config();
    this.status = {
      ...this.status,
      enabled: config.enabled,
      symbol: config.symbol,
      pollMinutes: config.pollMinutes,
      hasSmtp: smtpSettingsFromEnv() !== undefined
    };

    if (!config.enabled || this.interval) return;

    this.checkOnce().catch((error) => {
      console.error('Initial Decentrader gap check failed:', error);
    });
    this.interval = setInterval(() => {
      this.checkOnce().catch((error) => {
        console.error('Decentrader gap monitor failed:', error);
      });
    }, config.pollMinutes * 60 * 1000);
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = undefined;
    }
  }

  getStatus(): MonitorStatus {
    const config = this.config();
    return {
      ...this.status,
      enabled: config.enabled,
      symbol: config.symbol,
      pollMinutes: config.pollMinutes,
      hasSmtp: smtpSettingsFromEnv() !== undefined
    };
  }

  async checkOnce(): Promise<any> {
    const config = this.config();
    this.status = {
      ...this.status,
      enabled: config.enabled,
      symbol: config.symbol,
      pollMinutes: config.pollMinutes,
      hasSmtp: smtpSettingsFromEnv() !== undefined,
      running: true,
      lastStartedAt: nowNlIso(),
      lastError: undefined
    };

    try {
      const rows = await fetchSnapshot(config.symbol);
      this.latestRows = rows;
      this.latestTimelapsePayload = buildTimelapsePayload(rows, config.symbol);
      const state = readState(config.stateFile);
      const previousDataTimestamp = state.lastDataTimestamp;
      const alerts = gapIntrusionsSince(rows, previousDataTimestamp);
      state.lastCheckedAt = nowNlIso();
      state.lastDataTimestamp = rows[rows.length - 1]?.timestamp;

      const result: any = {
        ok: true,
        symbol: config.symbol,
        rows: rows.length,
        latestTimestamp: state.lastDataTimestamp,
        latestTimestampNl: nlTime(state.lastDataTimestamp),
        alert: null,
        alerts: [],
        alertCount: alerts.length,
        emailConfigured: smtpSettingsFromEnv() !== undefined,
        emailSent: false,
        emailSentCount: 0,
        duplicate: false
      };

      if (!alerts.length) {
        state.lastAlertObservedSignature = null;
        writeState(config.stateFile, state);
        this.status = {
          ...this.status,
          running: false,
          lastFinishedAt: nowNlIso(),
          lastResult: result
        };
        return result;
      }

      const smtpSettings = smtpSettingsFromEnv();
      for (const alert of alerts) {
        const signature = gapAlertSignature(alert);
        const alertSummary = {
          signature,
          timestamp: alert.timestamp,
          timestampNl: alert.timestampNl,
          price: alert.price,
          sideCounts: sideCounts(alert),
          gap: alert.previousGap,
          entrants: alert.entrants
        };
        state.lastAlertObservedSignature = signature;
        result.alert = alertSummary;
        result.alerts.push(alertSummary);

        if (signature === state.lastAlertSentSignature) {
          result.duplicate = true;
          continue;
        }

        if (smtpSettings) {
          await sendEmail(
            smtpSettings,
            `[${smtpSettings.jobName}] ${alert.timestampNl} | ${sideCounts(alert)}`,
            alertBody(alert, config.symbol)
          );
          state.lastAlertSentSignature = signature;
          state.lastAlertSentAt = nowNlIso();
          result.emailSent = true;
          result.emailSentCount += 1;
        }
      }

      writeState(config.stateFile, state);
      this.status = {
        ...this.status,
        running: false,
        lastFinishedAt: nowNlIso(),
        lastResult: result
      };
      return result;
    } catch (error) {
      this.status = {
        ...this.status,
        running: false,
        lastFinishedAt: nowNlIso(),
        lastError: error instanceof Error ? error.message : String(error)
      };
      throw error;
    }
  }

  async getTimelapsePayload(): Promise<any> {
    const config = this.config();
    if (this.latestTimelapsePayload) {
      return this.latestTimelapsePayload;
    }

    const rows = await fetchSnapshot(config.symbol);
    this.latestRows = rows;
    this.latestTimelapsePayload = buildTimelapsePayload(rows, config.symbol);
    return this.latestTimelapsePayload;
  }

  private config() {
    const pollMinutes = Math.max(1, Number(process.env.DECENTRADER_GAP_POLL_MINUTES || 10));
    return {
      enabled: parseBool(process.env.DECENTRADER_GAP_MONITOR_ENABLED, false),
      symbol: String(process.env.DECENTRADER_GAP_SYMBOL || 'btcusdt').trim().toLowerCase() || 'btcusdt',
      pollMinutes,
      stateFile:
        String(process.env.DECENTRADER_GAP_ALERT_STATE_FILE || '').trim() ||
        path.join(process.cwd(), 'data', 'decentrader-gap-alert-state.json')
    };
  }
}

export const decentraderGapMonitor = new DecentraderGapMonitor();
