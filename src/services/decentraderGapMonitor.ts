import axios from 'axios';
import fs from 'fs';
import net from 'net';
import path from 'path';
import tls from 'tls';
import { AlertObject } from '../types';

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

type TradeZone = {
  direction: 'long' | 'short';
  rank: number;
  price: number;
  count: number;
  score: number;
  selectionScore?: number;
  peak?: boolean;
  distance: number;
  leverages: number[];
  fresh: number;
};

type TpHit = {
  timestamp: string;
  timestampNl: string;
  price: number;
  previousPrice: number;
  direction: 'long' | 'short';
  label: string;
  zone: TradeZone;
};

type TpBacktestOptions = {
  lookaheadBars?: number;
  maxTrades?: number;
};

type DydxOpenPosition = {
  market: string;
  side?: string;
  size: number;
  entryPrice?: number;
};

type DydxSizingAccountSnapshot = {
  equity: number;
  freeCollateral: number;
  openPositionsCount: number;
  openPositions?: DydxOpenPosition[];
  markets: Record<
    string,
    {
      oraclePrice?: number;
      initialMarginFraction: number;
      maintenanceMarginFraction?: number;
      stepSize: number;
      status?: string;
    }
  >;
  updatedAt?: string;
};

type TradePlanDirection = 'long' | 'short';
type TradeDecisionOutcome = 'PLACED' | 'SKIPPED' | 'ERROR' | 'READY';

type TradeEvaluationOptions = {
  dryRun?: boolean;
  simulatedDirection?: TradePlanDirection;
  liveTestHoldSeconds?: number;
};

type FractalLevel = {
  kind: 'top' | 'bottom';
  price: number;
  timestamp: string;
  index: number;
  window: number;
  source: 'highRef' | 'lowRef' | 'ohlc4';
};

type FractalStop = {
  price?: number;
  rawFractalPrice?: number;
  wickGuardPrice?: number;
  wickGuardSource?: 'highRef' | 'lowRef';
  buffer?: number;
  source: 'confirmed-top-fractal' | 'confirmed-bottom-fractal' | 'missing-fractal' | 'invalid-distance';
  fractal?: FractalLevel;
  distance?: number;
  riskPct?: number;
  minDistancePct: number;
  maxDistancePct: number;
  valid: boolean;
  adjustedToMinDistance?: boolean;
  reason?: string;
};

type DecentraderTradeExecutor = {
  getAccountSnapshot: (markets: string[]) => Promise<DydxSizingAccountSnapshot>;
  placeOrder: (alert: AlertObject) => Promise<void>;
  syncTakeProfits?: (alert: AlertObject) => Promise<any>;
  syncTrailingStop?: (alert: AlertObject) => Promise<any>;
};

type MonitorStatus = {
  enabled: boolean;
  running: boolean;
  symbol: string;
  pollMinutes: number;
  hasSmtp: boolean;
  autoTradeEnabled?: boolean;
  hasTradeExecutor?: boolean;
  tradeRiskPct?: number;
  tradeRiskUsd?: number;
  slMaxDistancePct?: number;
  tpMaxLevels?: number;
  tpAllocation?: 'fixed-fractions' | 'map-weighted';
  dynamicTpEnabled?: boolean;
  hasDynamicTpExecutor?: boolean;
  dynamicSlEnabled?: boolean;
  dynamicSlLiveUpdatesEnabled?: boolean;
  hasDynamicSlExecutor?: boolean;
  lastStartedAt?: string;
  lastFinishedAt?: string;
  lastError?: string;
  lastResult?: any;
  lastTradeDecision?: any;
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
  lastTpHitObservedSignature?: string | null;
  lastTpHitSentSignature?: string;
  lastTpHitSentSignatures?: string[];
  lastTpHitSentAt?: string;
  lastTradeExecutedSignature?: string;
  lastTradeExecutedAt?: string;
  lastTradeExecutionError?: string;
  lastTradeDecision?: any;
  managedPosition?: {
    market: string;
    direction: TradePlanDirection;
    openedAt: string;
    entrySignature: string;
    initialSize: number;
    entryPrice?: number;
    currentStop?: number;
    currentStopUpdatedAt?: string;
  };
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

function numberOrZero(value: any): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function existingMarketPosition(
  account: DydxSizingAccountSnapshot,
  market: string
): DydxOpenPosition | undefined {
  const normalizedMarket = market.replace(/_/g, '-').toUpperCase();

  return account.openPositions?.find(
    (position) =>
      String(position.market || '').replace(/_/g, '-').toUpperCase() === normalizedMarket &&
      Math.abs(numberOrZero(position.size)) > 0
  );
}

function positionDirection(position: DydxOpenPosition | undefined): TradePlanDirection | undefined {
  const size = numberOrZero(position?.size);
  if (size > 0) return 'long';
  if (size < 0) return 'short';
  return undefined;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function floorToStep(value: number, step: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0;
  if (!Number.isFinite(step) || step <= 0) return value;

  const decimals = Math.max(0, (String(step).split('.')[1] || '').length);
  return Number((Math.floor(value / step) * step).toFixed(decimals));
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

async function sendEmailBestEffort(
  settings: SmtpSettings,
  subject: string,
  body: string
): Promise<{ sent: boolean; error?: string }> {
  try {
    await sendEmail(settings, subject, body);
    return { sent: true };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    console.error('Decentrader email send failed; monitor/trading will continue:', {
      subject,
      error: message
    });

    return {
      sent: false,
      error: message
    };
  }
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

function firstSeenKeysForFrame(rows: DecentraderRow[], frameIndex: number): Set<string> {
  const seen = new Set<string>();
  const firstSeen = new Set<string>();

  for (let rowIndex = 0; rowIndex <= frameIndex; rowIndex++) {
    const row = rows[rowIndex];
    for (const side of ['long', 'short'] as const) {
      for (const leverage of LEVERAGES) {
        const event = eventForRow(row, rowIndex, side, leverage);
        if (!event.active || event.roundedPrice === undefined) continue;

        const compactSide = side === 'long' ? 'L' : 'S';
        const key = `${compactSide}|${leverage}|${priceKey(event.roundedPrice)}`;
        if (!seen.has(key) && rowIndex === frameIndex) {
          firstSeen.add(key);
        }
        seen.add(key);
      }
    }
  }

  return firstSeen;
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

function buildSimulatedGapAlert(
  rows: DecentraderRow[],
  frameIndex: number,
  direction: TradePlanDirection,
  gap: Gap | undefined
): GapAlert {
  const frame = rows[frameIndex];
  const price = parseNumber(frame?.ohlc4);

  if (!frame || price === undefined) {
    throw new Error('Cannot simulate edge: latest Decentrader frame has no price.');
  }

  if (!gap) {
    throw new Error('Cannot simulate edge: latest Decentrader frame has no clean gap.');
  }

  const gapSide = direction === 'long' ? 'left' : 'right';
  const sourceBar = direction === 'long' ? gap.leftEdge : gap.rightEdge;
  const entrant: LiquidityBar = {
    ...sourceBar,
    key: `SIMULATED_${gapSide.toUpperCase()}_${sourceBar.key}`,
    count: sourceBar.count + 1,
    newCount: 1,
    gapSide,
    sideOfPrice: sourceBar.price < price ? 'left' : sourceBar.price > price ? 'right' : 'price'
  };

  return {
    timestamp: String(frame.timestamp || ''),
    timestampNl: nlTime(frame.timestamp),
    price,
    previousGap: gap,
    entrants: [entrant],
    left: direction === 'long' ? [entrant] : [],
    right: direction === 'short' ? [entrant] : []
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

function tradeClusterStep(span: number): number {
  if (span > 60000) return 1000;
  if (span > 30000) return 500;
  if (span > 12000) return 250;
  return 100;
}

function tradeZonesForFrame(rows: DecentraderRow[], frameIndex: number): { longTp: TradeZone[]; shortTp: TradeZone[] } {
  const frame = rows[frameIndex];
  const price = parseNumber(frame?.ohlc4);
  if (price === undefined) return { longTp: [], shortTp: [] };

  const bars = activeBarsForFrame(rows, frameIndex);
  const firstSeenKeys = firstSeenKeysForFrame(rows, frameIndex);
  const gap = cleanGapForBars(frame, bars);
  const prices = [price, ...bars.map((bar) => bar.price)];
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const step = tradeClusterStep(maxPrice - minPrice);
  const minDistance = Math.max(100, step * 0.5);
  const leverageWeight = new Map<number, number>([
    [3, 1],
    [5, 1.35],
    [10, 1.7]
  ]);
  const clusters = new Map<
    string,
    {
      direction: 'long' | 'short';
      priceSum: number;
      weightSum: number;
      weighted: number;
      count: number;
      fresh: number;
      leverages: Set<number>;
    }
  >();

  function addCandidate(direction: 'long' | 'short', bar: LiquidityBar): void {
    const isLongTarget = direction === 'long';
    const isCorrectSide = isLongTarget ? bar.side === 'S' && bar.price > price : bar.side === 'L' && bar.price < price;
    if (!isCorrectSide || Math.abs(bar.price - price) < minDistance) return;

    const bucket = Math.round(bar.price / step) * step;
    const key = `${direction}|${bucket}`;
    const weight = leverageWeight.get(bar.leverage) || 1;
    const cluster =
      clusters.get(key) ||
      ({
        direction,
        priceSum: 0,
        weightSum: 0,
        weighted: 0,
        count: 0,
        fresh: 0,
        leverages: new Set<number>()
      } as {
        direction: 'long' | 'short';
        priceSum: number;
        weightSum: number;
        weighted: number;
        count: number;
        fresh: number;
        leverages: Set<number>;
      });

    cluster.priceSum += bar.price * weight;
    cluster.weightSum += weight;
    cluster.weighted += bar.count * weight;
    cluster.count += bar.count;
    cluster.fresh += firstSeenKeys.has(bar.key) ? 1 : 0;
    cluster.leverages.add(bar.leverage);
    clusters.set(key, cluster);
  }

  for (const bar of bars) {
    addCandidate('long', bar);
    addCandidate('short', bar);
  }

  const zones = Array.from(clusters.values()).map((cluster) => {
    const zonePrice = Math.round(cluster.priceSum / cluster.weightSum / 50) * 50;
    const distance = Math.abs(zonePrice - price);
    const distancePct = distance / Math.max(1, price);
    const distanceFactor = Math.max(0.35, 1 - Math.min(distancePct / 0.22, 1) * 0.65);
    const overlapFactor = 1 + Math.max(0, cluster.leverages.size - 1) * 0.22;
    const freshFactor = 1 + Math.min(cluster.fresh, 3) * 0.12;
    let gapFactor = 1;

    if (gap) {
      const edge = cluster.direction === 'long' ? gap.right : gap.left;
      const edgeDistance = Math.abs(zonePrice - edge);
      gapFactor += Math.max(0, 1 - edgeDistance / (step * 6)) * 0.25;
    }

    return {
      direction: cluster.direction,
      rank: 0,
      price: zonePrice,
      count: cluster.count,
      score: Math.max(1, Math.round(cluster.weighted * overlapFactor * distanceFactor * freshFactor * gapFactor)),
      distance,
      leverages: Array.from(cluster.leverages).sort((a, b) => a - b),
      fresh: cluster.fresh
    } as TradeZone;
  });

  function ranked(direction: 'long' | 'short'): TradeZone[] {
    const priceOrdered = zones
      .filter((zone) => zone.direction === direction)
      .sort((a, b) => a.price - b.price || b.score - a.score);
    const strongestScore = Math.max(0, ...priceOrdered.map((zone) => zone.score));
    const minimumRelativeScore = Math.max(2, strongestScore * 0.2);

    const eligible = priceOrdered
      .filter((zone) =>
        zone.score >= minimumRelativeScore ||
        zone.leverages.length >= 2 ||
        (zone.fresh > 0 && zone.count >= 2)
      )
      .map((zone) => {
        const index = priceOrdered.findIndex((candidate) => candidate.price === zone.price);
        const previous = index > 0 ? priceOrdered[index - 1] : undefined;
        const next = index >= 0 && index < priceOrdered.length - 1 ? priceOrdered[index + 1] : undefined;
        const peak =
          (!previous || zone.score >= previous.score) &&
          (!next || zone.score >= next.score) &&
          Boolean(previous || next);
        const overlapBoost = 1 + Math.max(0, zone.leverages.length - 1) * 0.08;
        const freshBoost = 1 + Math.min(zone.fresh, 3) * 0.05;
        const peakBoost = peak ? 1.18 : 1;

        return {
          ...zone,
          peak,
          selectionScore: Math.round(zone.score * overlapBoost * freshBoost * peakBoost)
        };
      });

    const maxLevels = decentraderMaxTpLevels();
    const nearTpMaxDistancePct = envFraction('DECENTRADER_TP1_MAX_DISTANCE_PCT', 0.045);
    const stagedTpDistanceCaps = [
      nearTpMaxDistancePct,
      envFraction('DECENTRADER_TP2_MAX_DISTANCE_PCT', 0.09),
      envFraction('DECENTRADER_TP3_MAX_DISTANCE_PCT', 0.14)
    ];
    const nearTpMinimumScore = Math.max(2, strongestScore * 0.12);

    const selected = new Map<number, TradeZone>();

    for (const cap of stagedTpDistanceCaps) {
      if (selected.size >= maxLevels) break;

      const stagedCandidate = eligible
        .filter((zone) =>
          !selected.has(zone.price) &&
          zone.distance / Math.max(1, price) <= cap &&
          (
            zone.score >= nearTpMinimumScore ||
            zone.leverages.length >= 2 ||
            zone.peak ||
            zone.fresh > 0
          )
        )
        .sort((a, b) =>
          a.distance - b.distance ||
          (b.selectionScore || b.score) - (a.selectionScore || a.score) ||
          b.score - a.score
        )[0];

      if (stagedCandidate) selected.set(stagedCandidate.price, stagedCandidate);
    }

    for (const zone of eligible
      .sort((a, b) =>
        (b.selectionScore || b.score) - (a.selectionScore || a.score) ||
        b.score - a.score ||
        a.distance - b.distance
      )) {
      selected.set(zone.price, zone);
      if (selected.size >= maxLevels) break;
    }

    return Array.from(selected.values())
      .sort((a, b) => {
        if (direction === 'long') return a.price - b.price || b.score - a.score;
        return b.price - a.price || b.score - a.score;
      })
      .map((zone, index) => ({ ...zone, rank: index + 1 }));
  }

  return {
    longTp: ranked('long'),
    shortTp: ranked('short')
  };
}

function detectTpHits(rows: DecentraderRow[], frameIndex: number): TpHit[] {
  if (frameIndex <= 0) return [];

  const previousFrame = rows[frameIndex - 1];
  const currentFrame = rows[frameIndex];
  const previousPrice = parseNumber(previousFrame?.ohlc4);
  const currentPrice = parseNumber(currentFrame?.ohlc4);
  if (previousPrice === undefined || currentPrice === undefined) return [];

  const previousZones = tradeZonesForFrame(rows, frameIndex - 1);
  const hits: TpHit[] = [];
  const timestamp = String(currentFrame.timestamp || '');

  for (const zone of previousZones.longTp) {
    if (previousPrice < zone.price && currentPrice >= zone.price) {
      hits.push({
        timestamp,
        timestampNl: nlTime(currentFrame.timestamp),
        price: currentPrice,
        previousPrice,
        direction: 'long',
        label: `L TP${zone.rank}`,
        zone
      });
    }
  }

  for (const zone of previousZones.shortTp) {
    if (previousPrice > zone.price && currentPrice <= zone.price) {
      hits.push({
        timestamp,
        timestampNl: nlTime(currentFrame.timestamp),
        price: currentPrice,
        previousPrice,
        direction: 'short',
        label: `S TP${zone.rank}`,
        zone
      });
    }
  }

  return hits;
}

function tpHitsSince(rows: DecentraderRow[], lastDataTimestamp?: string): TpHit[] {
  if (rows.length < 2) return [];

  let startIndex = rows.length - 1;
  if (lastDataTimestamp) {
    const lastIndex = rows.findIndex((row) => String(row.timestamp || '') === lastDataTimestamp);
    startIndex = lastIndex >= 0 ? lastIndex : rows.length - 1;
  }

  const hits: TpHit[] = [];
  for (let frameIndex = Math.max(1, startIndex); frameIndex < rows.length; frameIndex += 1) {
    hits.push(...detectTpHits(rows, frameIndex));
  }

  return hits;
}

function rowHighLow(row: DecentraderRow): { high: number; low: number; price: number } | undefined {
  const price = parseNumber(row?.ohlc4);
  if (price === undefined) return undefined;

  const high = Math.max(
    price,
    parseNumber(row?.highRef) ?? price,
    parseNumber(row?.high) ?? price,
    parseNumber(row?.h) ?? price
  );
  const low = Math.min(
    price,
    parseNumber(row?.lowRef) ?? price,
    parseNumber(row?.low) ?? price,
    parseNumber(row?.l) ?? price
  );

  return { high, low, price };
}

function backtestTpZones(rows: DecentraderRow[], options: TpBacktestOptions = {}): any {
  const lookaheadBars = Math.max(1, Math.min(240, Math.floor(options.lookaheadBars || 48)));
  const maxTrades = Math.max(1, Math.min(1000, Math.floor(options.maxTrades || 300)));
  const trades: any[] = [];
  const byRank = new Map<number, any>();
  const byFeature = {
    peak: { candidates: 0, hitsBeforeStop: 0 },
    overlap2Plus: { candidates: 0, hitsBeforeStop: 0 },
    fresh: { candidates: 0, hitsBeforeStop: 0 }
  };

  function rankStats(rank: number): any {
    const existing =
      byRank.get(rank) ||
      {
        rank,
        candidates: 0,
        hitsBeforeStop: 0,
        ambiguousSameBar: 0,
        avgBarsToHit: 0,
        avgSelectionScore: 0,
        avgDistancePct: 0
      };
    byRank.set(rank, existing);
    return existing;
  }

  for (
    let frameIndex = rows.length - 1 - lookaheadBars;
    frameIndex >= 1 && trades.length < maxTrades;
    frameIndex -= 1
  ) {
    const alert = detectGapIntrusion(rows, frameIndex);
    const direction = mapDirectionFromAlert(alert);
    if (!alert || !direction) continue;

    const entry = rowHighLow(rows[frameIndex]);
    if (!entry) continue;

    const stop = buildFractalStop(rows, frameIndex, direction, entry.price);
    if (!stop.valid || !stop.price) continue;

    const zones = tradeZonesForFrame(rows, frameIndex);
    const takeProfits = direction === 'long' ? zones.longTp : zones.shortTp;
    if (!takeProfits.length) continue;

    const tpResults = takeProfits.map((zone) => ({
      label: `${direction === 'long' ? 'L' : 'S'} TP${zone.rank}`,
      rank: zone.rank,
      price: zone.price,
      score: zone.score,
      selectionScore: zone.selectionScore || zone.score,
      peak: Boolean(zone.peak),
      fresh: zone.fresh,
      leverages: zone.leverages,
      distancePct: Math.abs(zone.price - entry.price) / Math.max(1, entry.price),
      hit: false,
      hitBeforeStop: false,
      ambiguousSameBar: false,
      barsToHit: null as number | null
    }));

    let stopHitAt: number | null = null;
    let stopped = false;

    for (
      let offset = 1;
      offset <= lookaheadBars && frameIndex + offset < rows.length;
      offset += 1
    ) {
      const future = rowHighLow(rows[frameIndex + offset]);
      if (!future) continue;

      const stopTouched = direction === 'long'
        ? future.low <= stop.price
        : future.high >= stop.price;

      for (const tp of tpResults) {
        if (tp.hit) continue;

        const tpTouched = direction === 'long'
          ? future.high >= tp.price
          : future.low <= tp.price;

        if (!tpTouched) continue;

        tp.hit = true;
        tp.barsToHit = offset;
        tp.ambiguousSameBar = stopTouched;
        tp.hitBeforeStop = !stopped && !stopTouched;
      }

      if (stopTouched) {
        stopHitAt = offset;
        stopped = true;
        break;
      }
    }

    for (const tp of tpResults) {
      const stats = rankStats(tp.rank);
      stats.candidates += 1;
      stats.hitsBeforeStop += tp.hitBeforeStop ? 1 : 0;
      stats.ambiguousSameBar += tp.ambiguousSameBar ? 1 : 0;
      stats.avgSelectionScore += tp.selectionScore;
      stats.avgDistancePct += tp.distancePct;
      stats.avgBarsToHit += tp.hitBeforeStop && tp.barsToHit ? tp.barsToHit : 0;

      if (tp.peak) {
        byFeature.peak.candidates += 1;
        byFeature.peak.hitsBeforeStop += tp.hitBeforeStop ? 1 : 0;
      }
      if (tp.leverages.length >= 2) {
        byFeature.overlap2Plus.candidates += 1;
        byFeature.overlap2Plus.hitsBeforeStop += tp.hitBeforeStop ? 1 : 0;
      }
      if (tp.fresh > 0) {
        byFeature.fresh.candidates += 1;
        byFeature.fresh.hitsBeforeStop += tp.hitBeforeStop ? 1 : 0;
      }
    }

    trades.push({
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      direction,
      entryPrice: entry.price,
      stopPrice: stop.price,
      stopHitAt,
      lookaheadBars,
      gap: {
        left: alert.previousGap.left,
        right: alert.previousGap.right,
        width: alert.previousGap.width
      },
      takeProfits: tpResults
    });
  }

  trades.reverse();

  const rankSummary = Array.from(byRank.values()).map((stats) => ({
    ...stats,
    hitRateBeforeStop: stats.candidates ? stats.hitsBeforeStop / stats.candidates : 0,
    ambiguousRate: stats.candidates ? stats.ambiguousSameBar / stats.candidates : 0,
    avgSelectionScore: stats.candidates ? stats.avgSelectionScore / stats.candidates : 0,
    avgDistancePct: stats.candidates ? stats.avgDistancePct / stats.candidates : 0,
    avgBarsToHit: stats.hitsBeforeStop ? stats.avgBarsToHit / stats.hitsBeforeStop : null
  }));

  const featureSummary = Object.fromEntries(
    Object.entries(byFeature).map(([key, stats]) => [
      key,
      {
        ...stats,
        hitRateBeforeStop: stats.candidates ? stats.hitsBeforeStop / stats.candidates : 0
      }
    ])
  );

  return {
    ok: true,
    methodology:
      'Historical Decentrader edge replay. For each edge alert, the current TP selector is frozen, then future candles are scanned for TP touches before the fractal SL. Same-candle TP+SL is treated as ambiguous, not a clean hit.',
    lookaheadBars,
    maxTrades,
    tradeCount: trades.length,
    rankSummary,
    featureSummary,
    trades: trades.slice(-50)
  };
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

function sizingModeConfig(mode: string) {
  return (
    {
      conservative: { targetMultiplier: 0.45, collateralUse: 0.45, equityLeverage: 3.0, minWeight: 0.04 },
      balanced: { targetMultiplier: 0.75, collateralUse: 0.6, equityLeverage: 5.0, minWeight: 0.05 },
      growth: { targetMultiplier: 1.0, collateralUse: 0.85, equityLeverage: 8.0, minWeight: 0.06 }
    } as Record<
      string,
      { targetMultiplier: number; collateralUse: number; equityLeverage: number; minWeight: number }
    >
  )[mode] || { targetMultiplier: 1.0, collateralUse: 0.85, equityLeverage: 8.0, minWeight: 0.06 };
}

function effectiveTargetNotional(equity: number, mode: string): number {
  const baseTarget = Math.max(50, Number(process.env.DECENTRADER_TRADE_BASE_NOTIONAL || 1000));
  if (mode !== 'growth') return baseTarget;

  const referenceEquity = Math.max(
    1,
    Number(process.env.DECENTRADER_TRADE_REFERENCE_EQUITY || equity || 1)
  );
  const compoundFactor = Math.pow(clamp(Math.max(0, equity) / referenceEquity, 0.25, 12), 0.35);
  return baseTarget * compoundFactor;
}

function confidenceMultiplier(score: number): number {
  if (score >= 85) return 1.25;
  if (score >= 75) return 1;
  if (score >= 65) return 0.75;
  return 0.5;
}

function decentraderAutoTradeEnabled(): boolean {
  return parseBool(process.env.DECENTRADER_AUTO_TRADE_ENABLED, false);
}

function decentraderTradeMarket(): string {
  return String(process.env.DECENTRADER_TRADE_MARKET || 'BTC-USD')
    .replace(/_/g, '-')
    .trim()
    .toUpperCase();
}

function decentraderMaxTpLevels(): number {
  const parsed = Number(process.env.DECENTRADER_TP_MAX_LEVELS || 6);
  return Number.isFinite(parsed) && parsed > 0
    ? clamp(Math.floor(parsed), 1, 6)
    : 6;
}

function decentraderTpAllocationMode(): 'fixed-fractions' | 'map-weighted' {
  return String(process.env.DECENTRADER_TP_SIZE_FRACTIONS || '').trim()
    ? 'fixed-fractions'
    : 'map-weighted';
}

function decentraderDynamicTpEnabled(): boolean {
  return parseBool(process.env.DECENTRADER_DYNAMIC_TP_ENABLED, true);
}

function decentraderDynamicSlEnabled(): boolean {
  return parseBool(process.env.DECENTRADER_DYNAMIC_SL_ENABLED, true);
}

function decentraderDynamicSlCoverageSyncEnabled(): boolean {
  return parseBool(process.env.DECENTRADER_DYNAMIC_SL_COVERAGE_SYNC_ENABLED, false);
}

function decentraderDynamicSlLiveUpdatesEnabled(): boolean {
  return parseBool(process.env.DECENTRADER_DYNAMIC_SL_LIVE_UPDATES_ENABLED, false);
}

function decentraderDynamicSlMinImprovementPct(): number {
  return envFraction('DECENTRADER_DYNAMIC_SL_MIN_IMPROVEMENT_PCT', 0.0025);
}

function decentraderTpFractions(takeProfits: any[]): number[] {
  if (!takeProfits.length) return [];

  const configured = String(process.env.DECENTRADER_TP_SIZE_FRACTIONS || '')
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => (value > 1 ? value / 100 : value));
  const rawFractions = takeProfits.map((tp, index) => {
    if (configured[index]) return configured[index];

    const score = Math.max(
      1,
      numberOrZero(tp?.selectionScore) ||
      numberOrZero(tp?.score)
    );
    const rankDecay = 1 / Math.sqrt(index + 1);
    return Math.sqrt(score) * rankDecay;
  });
  const sum = rawFractions.reduce((total, value) => total + value, 0);

  return rawFractions.map((value) => value / Math.max(sum, Number.EPSILON));
}

function allocateTakeProfitSizes(size: number, stepSize: number, fractions: number[]): number[] {
  if (!fractions.length || size <= 0 || stepSize <= 0) return [];

  const decimals = Math.max(0, (String(stepSize).split('.')[1] || '').length);
  const totalSteps = Math.floor((size + stepSize * 0.000001) / stepSize);
  const rawSteps = fractions.map((fraction) => totalSteps * fraction);
  const allocatedSteps = rawSteps.map((steps) => Math.floor(steps));
  let remainingSteps = totalSteps - allocatedSteps.reduce((total, steps) => total + steps, 0);
  const remainderOrder = rawSteps
    .map((steps, index) => ({ index, remainder: steps - Math.floor(steps) }))
    .sort((a, b) => b.remainder - a.remainder || a.index - b.index);

  for (const item of remainderOrder) {
    if (remainingSteps <= 0) break;
    allocatedSteps[item.index] += 1;
    remainingSteps -= 1;
  }

  return allocatedSteps.map((steps) => Number((steps * stepSize).toFixed(decimals)));
}

function envPositiveNumber(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function envFraction(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  const value = Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  return value > 1 ? value / 100 : value;
}

function decentraderTradeRiskPct(): number {
  return envFraction('DECENTRADER_TRADE_RISK_PCT', 0.0075);
}

function decentraderTradeRiskUsd(): number | undefined {
  const parsed = Number(process.env.DECENTRADER_TRADE_RISK_USD);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function decentraderTradeRiskBudget(equity: number): {
  riskBudgetUsd: number;
  riskPct: number;
  configuredRiskUsd?: number;
  cappedByPct: boolean;
  source: 'fixed-usd' | 'equity-pct';
} {
  const pct = decentraderTradeRiskPct();
  const pctBudget = Math.max(0, numberOrZero(equity) * pct);
  const configuredRiskUsd = decentraderTradeRiskUsd();

  if (configuredRiskUsd !== undefined) {
    const riskBudgetUsd = Math.min(configuredRiskUsd, pctBudget);
    return {
      riskBudgetUsd,
      riskPct: pct,
      configuredRiskUsd,
      cappedByPct: riskBudgetUsd < configuredRiskUsd,
      source: 'fixed-usd'
    };
  }

  return {
    riskBudgetUsd: pctBudget,
    riskPct: pct,
    cappedByPct: false,
    source: 'equity-pct'
  };
}

function decentraderSlFractalWindow(): number {
  return Math.max(1, Math.floor(envPositiveNumber('DECENTRADER_SL_FRACTAL_WINDOW', 2)));
}

function decentraderSlLookbackBars(): number {
  return Math.max(8, Math.floor(envPositiveNumber('DECENTRADER_SL_LOOKBACK_BARS', 72)));
}

function decentraderSlBufferPct(): number {
  return envFraction('DECENTRADER_SL_BUFFER_PCT', 0.001);
}

function decentraderSlRangeBufferMultiplier(): number {
  return envPositiveNumber('DECENTRADER_SL_BUFFER_RANGE_MULTIPLIER', 0.25);
}

function decentraderSlMinDistancePct(): number {
  return envFraction('DECENTRADER_SL_MIN_DISTANCE_PCT', 0.0025);
}

function decentraderSlMaxDistancePct(): number {
  return envFraction('DECENTRADER_SL_MAX_DISTANCE_PCT', 0.05);
}

function decentraderSkipTradeWithoutSl(): boolean {
  return parseBool(process.env.DECENTRADER_SKIP_TRADE_WITHOUT_SL, true);
}

function median(values: number[]): number | undefined {
  const sorted = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!sorted.length) return undefined;

  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

function medianHourlyRange(rows: DecentraderRow[], frameIndex: number, lookback: number): number | undefined {
  const start = Math.max(1, frameIndex - lookback + 1);
  const ranges: number[] = [];

  for (let index = start; index <= frameIndex; index += 1) {
    const current = parseNumber(rows[index]?.ohlc4);
    const previous = parseNumber(rows[index - 1]?.ohlc4);

    if (current !== undefined && previous !== undefined) {
      ranges.push(Math.abs(current - previous));
    }
  }

  return median(ranges);
}

function confirmedFractals(
  rows: DecentraderRow[],
  frameIndex: number,
  kind: 'top' | 'bottom',
  window: number,
  lookback: number,
  source?: 'highRef' | 'lowRef' | 'ohlc4'
): FractalLevel[] {
  const key = source || (kind === 'top' ? 'highRef' : 'lowRef');
  const firstIndex = Math.max(window, frameIndex - lookback);
  const lastIndex = frameIndex - window;
  const fractals: FractalLevel[] = [];

  for (let index = firstIndex; index <= lastIndex; index += 1) {
    const price = parseNumber(rows[index]?.[key]);
    if (price === undefined) continue;

    let confirmed = true;
    for (let offset = -window; offset <= window; offset += 1) {
      if (offset === 0) continue;

      const neighborPrice = parseNumber(rows[index + offset]?.[key]);
      if (neighborPrice === undefined) {
        confirmed = false;
        break;
      }

      if (kind === 'top' ? price <= neighborPrice : price >= neighborPrice) {
        confirmed = false;
        break;
      }
    }

    if (confirmed) {
      fractals.push({
        kind,
        price,
        timestamp: String(rows[index]?.timestamp || ''),
        index,
        window,
        source: key as 'highRef' | 'lowRef' | 'ohlc4'
      });
    }
  }

  return fractals;
}

function latestValidFractal(
  rows: DecentraderRow[],
  frameIndex: number,
  direction: TradePlanDirection,
  entryPrice: number
): FractalLevel | undefined {
  const kind = direction === 'long' ? 'bottom' : 'top';
  const fractals = confirmedFractals(
    rows,
    frameIndex,
    kind,
    decentraderSlFractalWindow(),
    decentraderSlLookbackBars()
  );
  const validRefFractal = fractals
    .reverse()
    .find((fractal) => direction === 'long' ? fractal.price < entryPrice : fractal.price > entryPrice);

  if (validRefFractal) {
    return validRefFractal;
  }

  return confirmedFractals(
    rows,
    frameIndex,
    kind,
    decentraderSlFractalWindow(),
    decentraderSlLookbackBars(),
    'ohlc4'
  )
    .reverse()
    .find((fractal) => direction === 'long' ? fractal.price < entryPrice : fractal.price > entryPrice);
}

function wickGuardForFractal(
  rows: DecentraderRow[],
  fractal: FractalLevel,
  direction: TradePlanDirection
): { price: number; source: 'highRef' | 'lowRef' } {
  const key = direction === 'long' ? 'lowRef' : 'highRef';
  const start = Math.max(0, fractal.index - fractal.window);
  const end = Math.min(rows.length - 1, fractal.index + fractal.window);
  const candidates: number[] = [];

  for (let index = start; index <= end; index += 1) {
    const value = parseNumber(rows[index]?.[key]);

    if (value !== undefined) {
      candidates.push(value);
    }
  }

  if (!candidates.length) {
    return {
      price: fractal.price,
      source: direction === 'long' ? 'lowRef' : 'highRef'
    };
  }

  return {
    price: direction === 'long'
      ? Math.min(fractal.price, ...candidates)
      : Math.max(fractal.price, ...candidates),
    source: key
  };
}

function buildFractalStop(
  rows: DecentraderRow[],
  frameIndex: number,
  direction: TradePlanDirection,
  entryPrice: number
): FractalStop {
  const minDistancePct = decentraderSlMinDistancePct();
  const maxDistancePct = decentraderSlMaxDistancePct();
  const fractal = latestValidFractal(rows, frameIndex, direction, entryPrice);
  const rangeBuffer = (medianHourlyRange(rows, frameIndex, decentraderSlLookbackBars()) || 0) *
    decentraderSlRangeBufferMultiplier();
  const pctBuffer = entryPrice * decentraderSlBufferPct();
  const buffer = Math.max(pctBuffer, rangeBuffer);

  if (!fractal) {
    return {
      source: 'missing-fractal',
      minDistancePct,
      maxDistancePct,
      valid: false,
      reason: decentraderSkipTradeWithoutSl()
        ? 'No confirmed fractal stop found; trade will be skipped.'
        : 'No confirmed fractal stop found.'
    };
  }

  const wickGuard = wickGuardForFractal(rows, fractal, direction);
  let price = direction === 'long'
    ? wickGuard.price - buffer
    : wickGuard.price + buffer;
  let distance = Math.abs(entryPrice - price);
  let riskPct = distance / Math.max(1, entryPrice);
  let adjustedToMinDistance = false;

  if (riskPct < minDistancePct) {
    price = direction === 'long'
      ? entryPrice * (1 - minDistancePct)
      : entryPrice * (1 + minDistancePct);
    distance = Math.abs(entryPrice - price);
    riskPct = distance / Math.max(1, entryPrice);
    adjustedToMinDistance = true;
  }

  if (riskPct > maxDistancePct) {
    return {
      price,
      rawFractalPrice: fractal.price,
      wickGuardPrice: wickGuard.price,
      wickGuardSource: wickGuard.source,
      buffer,
      source: 'invalid-distance',
      fractal,
      distance,
      riskPct,
      minDistancePct,
      maxDistancePct,
      valid: false,
      reason: `Fractal stop distance ${riskPct} is above maximum ${maxDistancePct}.`
    };
  }

  return {
    price,
    rawFractalPrice: fractal.price,
    wickGuardPrice: wickGuard.price,
    wickGuardSource: wickGuard.source,
    buffer,
    source: direction === 'long' ? 'confirmed-bottom-fractal' : 'confirmed-top-fractal',
    fractal,
    distance,
    riskPct,
    minDistancePct,
    maxDistancePct,
    valid: true,
    adjustedToMinDistance,
    reason: adjustedToMinDistance
      ? `Fractal stop was widened to minimum distance ${minDistancePct}.`
      : undefined
  };
}

function mapDirectionFromAlert(alert: GapAlert | undefined): TradePlanDirection | undefined {
  if (!alert?.entrants.length) return undefined;
  const leftWeight = alert.left.reduce((sum, bar) => sum + (bar.newCount || 1) * bar.leverage, 0);
  const rightWeight = alert.right.reduce((sum, bar) => sum + (bar.newCount || 1) * bar.leverage, 0);

  if (leftWeight > rightWeight) return 'long';
  if (rightWeight > leftWeight) return 'short';
  return undefined;
}

function mapScoreForDirection(
  direction: TradePlanDirection,
  gap: Gap | undefined,
  alert: GapAlert | undefined,
  zone: TradeZone | undefined,
  entryPrice: number,
  stopPrice: number
): number {
  const tpScore = zone ? clamp(zone.score / 2, 0, 38) : 0;
  const overlapScore = zone ? clamp(zone.leverages.length * 7, 0, 21) : 0;
  const freshScore = zone ? clamp(zone.fresh * 4, 0, 12) : 0;
  const alertDirection = mapDirectionFromAlert(alert);
  const alertScore = alertDirection === direction ? 24 : alert?.entrants.length ? 8 : 0;
  const stopRiskPct = Math.abs(entryPrice - stopPrice) / Math.max(1, entryPrice);
  const stopScore = clamp(18 - stopRiskPct * 100, 0, 18);
  const gapScore = gap ? clamp(12 - (gap.width / Math.max(1, entryPrice)) * 50, 0, 12) : 0;

  return Math.round(clamp(tpScore + overlapScore + freshScore + alertScore + stopScore + gapScore, 0, 100));
}

function buildDirectionalPlan(
  direction: TradePlanDirection,
  account: DydxSizingAccountSnapshot,
  marketInfo: DydxSizingAccountSnapshot['markets'][string],
  rows: DecentraderRow[],
  frameIndex: number,
  gap: Gap | undefined,
  alert: GapAlert | undefined,
  zones: { longTp: TradeZone[]; shortTp: TradeZone[] },
  fallbackPrice: number,
  mode: string
) {
  const isLong = direction === 'long';
  const tpZones = isLong ? zones.longTp : zones.shortTp;
  const triggerPrice = gap ? (isLong ? gap.left : gap.right) : fallbackPrice;
  const marketPrice = marketInfo.oraclePrice || fallbackPrice;
  const stop = buildFractalStop(rows, frameIndex, direction, marketPrice);
  const stopPrice = stop.valid ? stop.price : undefined;
  const equity = numberOrZero(account.equity);
  const freeCollateral = numberOrZero(account.freeCollateral);
  const marginFraction = Math.max(0.01, numberOrZero(marketInfo.initialMarginFraction) || 0.1);
  const stepSize = numberOrZero(marketInfo.stepSize) || 0.0001;
  const targetNotional = effectiveTargetNotional(equity, mode);
  const modeConfig = sizingModeConfig(mode);
  const mapScore = stopPrice
    ? mapScoreForDirection(direction, gap, alert, tpZones[0], marketPrice, stopPrice)
    : 0;
  const weight = clamp((mapScore / 100) * 0.12, 0.04, 0.16);
  const confidence = confidenceMultiplier(mapScore);
  const stopDistance = stopPrice ? Math.abs(marketPrice - stopPrice) : 0;
  const stopRiskPct = stopDistance / Math.max(1, marketPrice);
  const stopBrake = clamp(1 - Math.max(0, stopRiskPct - 0.035) / 0.16, 0.35, 1);
  const desiredNotional = targetNotional * modeConfig.targetMultiplier * confidence * stopBrake;
  const flatCollateral = Math.max(freeCollateral, equity * 0.8);
  const collateralBudget = flatCollateral * modeConfig.collateralUse * Math.max(weight, modeConfig.minWeight);
  const collateralCappedNotional = collateralBudget / marginFraction;
  const equityCappedNotional = equity * modeConfig.equityLeverage * Math.max(weight, modeConfig.minWeight);
  const riskBudget = decentraderTradeRiskBudget(equity);
  const riskBudgetUsd = riskBudget.riskBudgetUsd;
  const riskCappedNotional =
    stop.valid && stopDistance > 0
      ? (riskBudgetUsd / stopDistance) * marketPrice
      : 0;
  const notional = Math.max(
    0,
    Math.min(desiredNotional, collateralCappedNotional, equityCappedNotional, riskCappedNotional)
  );
  const rawSize = notional / Math.max(1, marketPrice);
  const size = floorToStep(rawSize, stepSize);
  const minimumOrderRiskUsd = stop.valid ? stopDistance * stepSize : 0;
  const minimumOrderRiskPctOfEquity =
    equity > 0
      ? minimumOrderRiskUsd / equity
      : 0;
  const invalidSize = stop.valid && size < stepSize;

  return {
    direction,
    status: !stop.valid
      ? 'invalid-stop'
      : invalidSize
        ? 'invalid-size'
        : mapDirectionFromAlert(alert) === direction
          ? 'alert-active'
          : 'watch',
    statusReason: !stop.valid
      ? stop.reason
      : invalidSize
        ? `Risk-capped size ${rawSize} is below dYdX minimum ${stepSize}.`
        : undefined,
    trigger: {
      type: gap ? (isLong ? 'gap-left-edge' : 'gap-right-edge') : 'no-clean-gap',
      price: triggerPrice
    },
    entryReference: {
      price: marketPrice,
      source: marketInfo.oraclePrice ? 'dydx-oracle' : 'decentrader-frame'
    },
    stop: stopPrice
      ? {
          valid: true,
          price: stopPrice,
          distance: stopDistance,
          riskPct: stopRiskPct,
          source: stop.source,
          rawFractalPrice: stop.rawFractalPrice,
          wickGuardPrice: stop.wickGuardPrice,
          wickGuardSource: stop.wickGuardSource,
          buffer: stop.buffer,
          fractal: stop.fractal,
          minDistancePct: stop.minDistancePct,
          maxDistancePct: stop.maxDistancePct,
          adjustedToMinDistance: stop.adjustedToMinDistance,
          reason: stop.reason
        }
      : {
          source: stop.source,
          valid: false,
          reason: stop.reason,
          minDistancePct: stop.minDistancePct,
          maxDistancePct: stop.maxDistancePct,
          fractal: stop.fractal || null
        },
    takeProfits: tpZones.map((zone) => ({
      label: `${isLong ? 'L' : 'S'} TP${zone.rank}`,
      price: zone.price,
      score: zone.score,
      selectionScore: zone.selectionScore,
      peak: zone.peak,
      count: zone.count,
      fresh: zone.fresh,
      leverages: zone.leverages,
      distance: Math.abs(zone.price - marketPrice)
    })),
    sizing: {
      mode,
      baseTargetNotional: Math.max(50, Number(process.env.DECENTRADER_TRADE_BASE_NOTIONAL || 1000)),
      effectiveTargetNotional: targetNotional,
      desiredNotional,
      notional: size * marketPrice,
      size,
      rawSize,
      minimumOrderSize: stepSize,
      minimumOrderRiskUsd,
      minimumOrderRiskPctOfEquity,
      equityFraction: weight,
      riskPct: riskBudget.riskPct,
      riskBudgetUsd,
      configuredRiskUsd: riskBudget.configuredRiskUsd,
      riskBudgetSource: riskBudget.source,
      riskBudgetCappedByPct: riskBudget.cappedByPct,
      confidenceScore: mapScore,
      confidenceMultiplier: confidence,
      stopBrake,
      caps: {
        collateralBudget,
        collateralCappedNotional,
        equityCappedNotional,
        riskCappedNotional,
        marginFraction,
        stepSize
      }
    }
  };
}

function buildDecentraderOrderAlert(plan: any, signature: string): AlertObject {
  const activePlan = plan.activePlan;
  const direction = activePlan?.direction as TradePlanDirection | undefined;
  const maxTpLevels = decentraderMaxTpLevels();
  const takeProfits = Array.isArray(activePlan?.takeProfits)
    ? activePlan.takeProfits.slice(0, maxTpLevels)
    : [];
  const fractions = decentraderTpFractions(takeProfits);
  const referencePrice =
    numberOrZero(activePlan?.entryReference?.price) ||
    numberOrZero(plan.price) ||
    numberOrZero(activePlan?.trigger?.price);
  const size = numberOrZero(activePlan?.sizing?.size);
  const minimumOrderSize = numberOrZero(activePlan?.sizing?.minimumOrderSize);
  const takeProfitSizes = allocateTakeProfitSizes(size, minimumOrderSize, fractions);
  const stopPrice = numberOrZero(activePlan?.stop?.price);

  if (direction !== 'long' && direction !== 'short') {
    throw new Error('Decentrader trade execution skipped: no active long/short plan.');
  }

  if (activePlan?.status === 'invalid-stop' || (!activePlan?.stop?.valid && !stopPrice)) {
    throw new Error(activePlan?.stop?.reason || 'Decentrader trade execution skipped: no valid fractal stop.');
  }

  if (!Number.isFinite(size) || size <= 0) {
    throw new Error('Decentrader trade execution skipped: active plan has no positive dYdX size.');
  }

  if (!Number.isFinite(referencePrice) || referencePrice <= 0) {
    throw new Error('Decentrader trade execution skipped: active plan has no valid reference price.');
  }

  if (!Number.isFinite(stopPrice) || stopPrice <= 0) {
    throw new Error('Decentrader trade execution skipped: active plan has no positive static SL.');
  }

  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_liquidity_map',
    market: plan.market || decentraderTradeMarket(),
    price: referencePrice,
    entry_price: referencePrice,
    size,
    sizeUsd: numberOrZero(activePlan?.sizing?.notional),
    desired_position: direction === 'long' ? 'LONG' : 'SHORT',
    time: Date.parse(String(plan.timestamp || '').replace(' ', 'T') + 'Z') || Date.now(),
    signal: direction === 'long' ? 'LONG_ENTRY' : 'SHORT_ENTRY',
    profile: 'MANAGED',
    static_sl: stopPrice,
    take_profits: takeProfits
      .map((tp: any, index: number) => ({
        label: tp.label || `${direction === 'long' ? 'L' : 'S'} TP${index + 1}`,
        price: tp.price,
        size: takeProfitSizes[index] || 0,
        zone_score: numberOrZero(tp.score),
        zone_selection_score: numberOrZero(tp.selectionScore),
        zone_peak: Boolean(tp.peak),
        zone_count: numberOrZero(tp.count),
        zone_fresh: numberOrZero(tp.fresh),
        zone_leverages: Array.isArray(tp.leverages) ? tp.leverages : [],
        distance: numberOrZero(tp.distance)
      }))
      .filter((tp: any) => tp.size > 0),
    decentrader: {
      signature,
      timestamp: plan.timestamp,
      timestampNl: plan.timestampNl,
      direction,
      confidenceScore: activePlan?.sizing?.confidenceScore,
      equityFraction: activePlan?.sizing?.equityFraction,
      notional: activePlan?.sizing?.notional,
      stop: activePlan?.stop,
      note: 'Generated by Decentrader edge trigger with confirmed fractal SL and map-derived TP levels.'
    }
  } as AlertObject;
}

function buildDecentraderDynamicTpAlert(plan: any, position: DydxOpenPosition): AlertObject {
  const direction: TradePlanDirection = position.size > 0 ? 'long' : 'short';
  const directionalPlan = plan?.plans?.[direction];
  const maxTpLevels = decentraderMaxTpLevels();
  const takeProfits = Array.isArray(directionalPlan?.takeProfits)
    ? directionalPlan.takeProfits.slice(0, maxTpLevels)
    : [];
  const fractions = decentraderTpFractions(takeProfits);
  const size = Math.abs(numberOrZero(position.size));
  const minimumOrderSize =
    numberOrZero(directionalPlan?.sizing?.minimumOrderSize) ||
    numberOrZero(plan?.marketInfo?.stepSize);
  const takeProfitSizes = allocateTakeProfitSizes(size, minimumOrderSize, fractions);
  const referencePrice =
    numberOrZero(directionalPlan?.entryReference?.price) ||
    numberOrZero(plan?.marketInfo?.oraclePrice) ||
    numberOrZero(plan?.price);

  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_dynamic_liquidity_tps',
    market: plan.market || decentraderTradeMarket(),
    price: referencePrice,
    entry_price: referencePrice,
    size,
    desired_position: direction === 'long' ? 'LONG' : 'SHORT',
    time: Date.now(),
    signal: 'SYNC_TAKE_PROFITS',
    profile: 'MANAGED',
    take_profits: takeProfits
      .map((tp: any, index: number) => ({
        label: tp.label || `${direction === 'long' ? 'L' : 'S'} TP${index + 1}`,
        price: tp.price,
        size: takeProfitSizes[index] || 0,
        zone_score: numberOrZero(tp.score),
        zone_selection_score: numberOrZero(tp.selectionScore),
        zone_peak: Boolean(tp.peak),
        zone_count: numberOrZero(tp.count),
        zone_fresh: numberOrZero(tp.fresh),
        zone_leverages: Array.isArray(tp.leverages) ? tp.leverages : [],
        distance: numberOrZero(tp.distance)
      }))
      .filter((tp: any) => tp.size > 0),
    decentrader: {
      timestamp: plan.timestamp,
      timestampNl: plan.timestampNl,
      direction,
      dynamicTpSync: true,
      note: 'TP-only sync from latest Decentrader liquidity zones; position and SL must remain unchanged.'
    }
  } as AlertObject;
}

function buildDecentraderDynamicSlAlert(
  plan: any,
  position: DydxOpenPosition,
  trailStop: number
): AlertObject {
  const direction: TradePlanDirection = position.size > 0 ? 'long' : 'short';
  const directionalPlan = plan?.plans?.[direction];
  const referencePrice =
    numberOrZero(directionalPlan?.entryReference?.price) ||
    numberOrZero(plan?.marketInfo?.oraclePrice) ||
    numberOrZero(plan?.price);

  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_dynamic_fractal_sl',
    market: plan.market || decentraderTradeMarket(),
    price: referencePrice,
    entry_price: referencePrice,
    size: Math.abs(numberOrZero(position.size)),
    desired_position: direction === 'long' ? 'LONG' : 'SHORT',
    time: Date.now(),
    signal: 'DECENTRADER_TRAILING_STOP_SYNC',
    profile: 'MANAGED',
    trail_stop: trailStop,
    static_sl: trailStop,
    decentrader: {
      timestamp: plan.timestamp,
      timestampNl: plan.timestampNl,
      direction,
      dynamicSlSync: true,
      stop: directionalPlan?.stop,
      note: 'Add-only fractal trailing SL from latest confirmed Decentrader fractal; older stops are preserved as fallback.'
    }
  } as AlertObject;
}

function buildDecentraderLiveTestOrderAlert(
  plan: any,
  signature: string,
  direction: TradePlanDirection
): AlertObject {
  const market = String(plan.market || decentraderTradeMarket()).replace(/_/g, '-').toUpperCase();
  const referencePrice =
    numberOrZero(plan.marketInfo?.oraclePrice) ||
    numberOrZero(plan.activePlan?.entryReference?.price) ||
    numberOrZero(plan.price);
  const size = Math.max(0.001, numberOrZero(plan.marketInfo?.stepSize));
  const isLong = direction === 'long';

  if (!Number.isFinite(referencePrice) || referencePrice <= 0) {
    throw new Error('Decentrader live test cannot start without a valid dYdX reference price.');
  }

  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_liquidity_map_live_test',
    market,
    price: referencePrice,
    entry_price: referencePrice,
    size,
    sizeUsd: size * referencePrice,
    desired_position: isLong ? 'LONG' : 'SHORT',
    time: Date.now(),
    signal: isLong ? 'LONG_ENTRY' : 'SHORT_ENTRY',
    profile: 'MANAGED',
    static_sl: isLong ? referencePrice * 0.99 : referencePrice * 1.01,
    take_profits: [
      {
        label: 'LIVE TEST TP',
        price: isLong ? referencePrice * 1.01 : referencePrice * 0.99,
        size_fraction: 1
      }
    ],
    decentrader: {
      signature,
      timestamp: plan.timestamp,
      timestampNl: plan.timestampNl,
      direction,
      liveTest: true,
      note: 'Temporary live end-to-end test order. Automatically flattened after the configured hold.'
    }
  } as AlertObject;
}

function buildDecentraderLiveTestFlatAlert(orderAlert: AlertObject): AlertObject {
  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_liquidity_map_live_test',
    market: orderAlert.market,
    price: orderAlert.price,
    desired_position: 'FLAT',
    time: Date.now(),
    signal: 'FLAT',
    profile: 'MANAGED',
    decentrader: {
      liveTest: true,
      note: 'Automatic flatten after temporary live end-to-end test.'
    }
  } as AlertObject;
}

function buildDecentraderStopBreachFlatAlert(
  market: string,
  price: number,
  position: DydxOpenPosition,
  stopPrice: number
): AlertObject {
  const direction: TradePlanDirection = position.size > 0 ? 'long' : 'short';

  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_stop_breach_residual_flat',
    market,
    price,
    entry_price: price,
    size: Math.abs(numberOrZero(position.size)),
    desired_position: 'FLAT',
    time: Date.now(),
    signal: 'FLAT',
    profile: 'MANAGED',
    decentrader: {
      direction,
      stopPrice,
      residualSize: position.size,
      stopBreachResidualFlat: true,
      note: 'Automatic residual flatten after managed Decentrader SL was already breached.'
    }
  } as AlertObject;
}

function buildDecentraderFlatCleanupAlert(
  market: string,
  managedPosition: NonNullable<AlertState['managedPosition']>
): AlertObject {
  return {
    exchange: 'dydxv4',
    strategy: 'decentrader_flat_order_cleanup',
    market,
    price: managedPosition.entryPrice || 1,
    entry_price: managedPosition.entryPrice || 1,
    desired_position: 'FLAT',
    time: Date.now(),
    signal: 'FLAT',
    profile: 'MANAGED',
    decentrader: {
      direction: managedPosition.direction,
      entrySignature: managedPosition.entrySignature,
      flatOrderCleanup: true,
      note: 'Position is already flat; run dYdX FLAT target flow to cancel remaining managed TP/SL orders.'
    }
  } as AlertObject;
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

function tpHitSignature(hit: TpHit): string {
  return `${hit.timestamp}|${hit.label}|${hit.zone.price}|${hit.zone.score}`;
}

function tpHitBody(hit: TpHit, symbol: string): string {
  const lines = [
    `Decentrader ${symbol.toUpperCase()} liquidity TP hit`,
    '',
    `Time: ${hit.timestampNl} (${hit.timestamp} UTC)`,
    `Hit: ${hit.label} ${money(hit.zone.price)}`,
    `Price move: ${money(hit.previousPrice)} -> ${money(hit.price)}`,
    `Zone score: ${hit.zone.score}`,
    `Zone side: ${hit.direction === 'long' ? 'long target above price' : 'short target below price'}`,
    `Leverages: ${hit.zone.leverages.map((leverage) => `${leverage}x`).join(' + ') || '-'}`,
    `Zone count: ${hit.zone.count}`,
    '',
    'Bron: https://www.decentrader.com/liquidity-maps/?coin=btc',
    'Let op: dit is een liquidity TP-hit event, geen automatisch trade-advies.'
  ];

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
  private tradeExecutor: DecentraderTradeExecutor | undefined;
  private status: MonitorStatus = {
    enabled: false,
    running: false,
    symbol: 'btcusdt',
    pollMinutes: 10,
    hasSmtp: false,
    autoTradeEnabled: false,
    hasTradeExecutor: false
  };

  configureTradeExecutor(executor: DecentraderTradeExecutor): void {
    this.tradeExecutor = executor;
    this.status = {
      ...this.status,
      hasTradeExecutor: true,
      autoTradeEnabled: decentraderAutoTradeEnabled()
    };
  }

  start(): void {
    const config = this.config();
    this.status = {
      ...this.status,
      enabled: config.enabled,
      symbol: config.symbol,
      pollMinutes: config.pollMinutes,
      hasSmtp: smtpSettingsFromEnv() !== undefined,
      autoTradeEnabled: decentraderAutoTradeEnabled(),
      hasTradeExecutor: this.tradeExecutor !== undefined
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
    const state = readState(config.stateFile);

    return {
      ...this.status,
      enabled: config.enabled,
      symbol: config.symbol,
      pollMinutes: config.pollMinutes,
      hasSmtp: smtpSettingsFromEnv() !== undefined,
      autoTradeEnabled: decentraderAutoTradeEnabled(),
      hasTradeExecutor: this.tradeExecutor !== undefined,
      tradeRiskPct: decentraderTradeRiskPct(),
      tradeRiskUsd: decentraderTradeRiskUsd(),
      slMaxDistancePct: decentraderSlMaxDistancePct(),
      tpMaxLevels: decentraderMaxTpLevels(),
      tpAllocation: decentraderTpAllocationMode(),
      dynamicTpEnabled: decentraderDynamicTpEnabled(),
      hasDynamicTpExecutor: typeof this.tradeExecutor?.syncTakeProfits === 'function',
      dynamicSlEnabled: decentraderDynamicSlEnabled(),
      dynamicSlLiveUpdatesEnabled: decentraderDynamicSlLiveUpdatesEnabled(),
      hasDynamicSlExecutor: typeof this.tradeExecutor?.syncTrailingStop === 'function',
      lastTradeDecision: state.lastTradeDecision
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
      autoTradeEnabled: decentraderAutoTradeEnabled(),
      hasTradeExecutor: this.tradeExecutor !== undefined,
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
      const tpHits = tpHitsSince(rows, previousDataTimestamp);
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
        alertCount: alerts.length + tpHits.length,
        gapAlertCount: alerts.length,
        tpHit: null,
        tpHits: [],
        tpHitCount: tpHits.length,
        emailConfigured: smtpSettingsFromEnv() !== undefined,
        emailSent: false,
        emailSentCount: 0,
        emailErrors: [],
        autoTradeEnabled: decentraderAutoTradeEnabled(),
        tradeExecutorConfigured: this.tradeExecutor !== undefined,
        tradeAttempted: false,
        tradePlaced: false,
        tradeSkipped: null,
        tradeError: null,
        tradeAlert: null,
        tradePlan: null,
        tradeDecision: null,
        dynamicSlSync: null,
        dynamicTpSync: null,
        lastTradeDecision: state.lastTradeDecision || null,
        duplicate: false
      };

      await this.maybeSyncDynamicStopLoss(state, result);
      await this.maybeSyncDynamicTakeProfits(state, result);

      if (!alerts.length) {
        state.lastAlertObservedSignature = null;
      }
      if (!tpHits.length) {
        state.lastTpHitObservedSignature = null;
      }

      if (!alerts.length && !tpHits.length) {
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
        console.log('Decentrader gap alert detected:', alertSummary);

        const emailDuplicate = signature === state.lastAlertSentSignature;
        if (emailDuplicate) {
          result.duplicate = true;
        } else if (smtpSettings) {
          const emailResult = await sendEmailBestEffort(
            smtpSettings,
            `[${smtpSettings.jobName}] ${alert.timestampNl} | ${sideCounts(alert)}`,
            alertBody(alert, config.symbol)
          );

          if (emailResult.sent) {
            state.lastAlertSentSignature = signature;
            state.lastAlertSentAt = nowNlIso();
            result.emailSent = true;
            result.emailSentCount += 1;
          } else {
            result.emailErrors.push({
              type: 'gap-alert',
              signature,
              timestamp: alert.timestamp,
              timestampNl: alert.timestampNl,
              error: emailResult.error
            });
          }
        }

        await this.maybeExecuteTradeForAlert(alert, signature, state, result);
      }

      const sentTpHitSignatures = new Set<string>([
        ...(state.lastTpHitSentSignatures || []),
        ...(state.lastTpHitSentSignature ? [state.lastTpHitSentSignature] : [])
      ]);

      for (const hit of tpHits) {
        const signature = tpHitSignature(hit);
        const hitSummary = {
          signature,
          timestamp: hit.timestamp,
          timestampNl: hit.timestampNl,
          price: hit.price,
          previousPrice: hit.previousPrice,
          label: hit.label,
          direction: hit.direction,
          zone: hit.zone
        };
        state.lastTpHitObservedSignature = signature;
        result.tpHit = hitSummary;
        result.tpHits.push(hitSummary);

        if (sentTpHitSignatures.has(signature)) {
          result.duplicate = true;
          continue;
        }

        if (smtpSettings) {
          const emailResult = await sendEmailBestEffort(
            smtpSettings,
            `[${smtpSettings.jobName}] ${hit.timestampNl} | ${hit.label} hit ${money(hit.zone.price)}`,
            tpHitBody(hit, config.symbol)
          );

          if (emailResult.sent) {
            state.lastTpHitSentSignature = signature;
            sentTpHitSignatures.add(signature);
            state.lastTpHitSentAt = nowNlIso();
            result.emailSent = true;
            result.emailSentCount += 1;
          } else {
            result.emailErrors.push({
              type: 'tp-hit',
              signature,
              timestamp: hit.timestamp,
              timestampNl: hit.timestampNl,
              label: hit.label,
              error: emailResult.error
            });
          }
        }
      }
      state.lastTpHitSentSignatures = Array.from(sentTpHitSignatures).slice(-50);

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

  private async maybeSyncDynamicTakeProfits(state: AlertState, result: any): Promise<void> {
    if (!decentraderDynamicTpEnabled()) {
      result.dynamicTpSync = {
        outcome: 'DISABLED',
        reason: 'DECENTRADER_DYNAMIC_TP_ENABLED is false.'
      };
      return;
    }

    if (!decentraderAutoTradeEnabled()) {
      result.dynamicTpSync = {
        outcome: 'SKIPPED',
        reason: 'Auto-trading is disabled; dynamic TP orders were preserved.'
      };
      return;
    }

    const executor = this.tradeExecutor;
    const syncTakeProfits = executor?.syncTakeProfits;

    if (!executor || !syncTakeProfits) {
      result.dynamicTpSync = {
        outcome: 'SKIPPED',
        reason: 'The dYdX executor does not expose TP-only synchronization.'
      };
      return;
    }

    const market = decentraderTradeMarket();

    try {
      const account = await executor.getAccountSnapshot([market]);
      const position = existingMarketPosition(account, market);

      if (!position) {
        delete state.managedPosition;
        result.dynamicTpSync = {
          outcome: 'SKIPPED',
          reason: `${market} is flat; no dynamic TP sync needed.`
        };
        return;
      }

      const managedPosition = state.managedPosition;
      const positionDirection: TradePlanDirection = position.size > 0 ? 'long' : 'short';
      const entryPriceMismatch = Boolean(
        managedPosition?.entryPrice &&
        position.entryPrice &&
        Math.abs(position.entryPrice - managedPosition.entryPrice) / managedPosition.entryPrice > 0.005
      );
      const positionGrewBeyondManagedSize =
        managedPosition &&
        Math.abs(position.size) > managedPosition.initialSize + 0.00000001;

      if (
        !managedPosition ||
        managedPosition.market.replace(/_/g, '-').toUpperCase() !== market ||
        managedPosition.direction !== positionDirection ||
        entryPriceMismatch ||
        positionGrewBeyondManagedSize
      ) {
        result.dynamicTpSync = {
          outcome: 'SKIPPED',
          reason: 'Existing BTC position was not opened by this Decentrader monitor; TP orders were preserved.',
          market,
          position,
          managedPosition: managedPosition || null
        };
        return;
      }

      const plan = await this.getTradePlan(account, market);
      const alert = buildDecentraderDynamicTpAlert(plan, position);
      const takeProfits = (alert as any).take_profits || [];

      if (!takeProfits.length) {
        result.dynamicTpSync = {
          outcome: 'SKIPPED',
          reason: 'No placeable map-derived TP levels; existing TP orders were preserved.',
          market,
          position
        };
        return;
      }

      const syncResult = await syncTakeProfits(alert);
      result.dynamicTpSync = {
        ...syncResult,
        market,
        position,
        timestamp: plan.timestamp,
        timestampNl: plan.timestampNl,
        takeProfits
      };
      console.log('Decentrader dynamic TP sync:', result.dynamicTpSync);
    } catch (error) {
      result.dynamicTpSync = {
        outcome: 'ERROR',
        reason: error instanceof Error ? error.message : String(error),
        market
      };
      console.error('Decentrader dynamic TP sync failed; position and SL were left unchanged:', error);
    }
  }

  private async maybeSyncDynamicStopLoss(state: AlertState, result: any): Promise<void> {
    if (!decentraderDynamicSlEnabled()) {
      result.dynamicSlSync = {
        outcome: 'DISABLED',
        reason: 'DECENTRADER_DYNAMIC_SL_ENABLED is false.'
      };
      return;
    }

    if (!decentraderAutoTradeEnabled()) {
      result.dynamicSlSync = {
        outcome: 'SKIPPED',
        reason: 'Auto-trading is disabled; dynamic SL orders were preserved.'
      };
      return;
    }

    const executor = this.tradeExecutor;
    const syncTrailingStop = executor?.syncTrailingStop;

    if (!executor || !syncTrailingStop) {
      result.dynamicSlSync = {
        outcome: 'SKIPPED',
        reason: 'The dYdX executor does not expose add-only SL synchronization.'
      };
      return;
    }

    const market = decentraderTradeMarket();

    try {
      const account = await executor.getAccountSnapshot([market]);
      const position = existingMarketPosition(account, market);

      if (!position) {
        const managedPosition = state.managedPosition;

        if (managedPosition) {
          const flatCleanupAlert = buildDecentraderFlatCleanupAlert(market, managedPosition);

          console.warn('Decentrader managed position is flat; running dYdX FLAT cleanup for remaining TP/SL orders:', {
            market,
            managedPosition
          });

          await executor.placeOrder(flatCleanupAlert);
          delete state.managedPosition;
          result.dynamicSlSync = {
            outcome: 'FLAT_CLEANED_UP',
            reason: `${market} is flat; dYdX FLAT target flow was run to cancel remaining managed TP/SL orders.`,
            market,
            managedPosition,
            flatAlert: {
              strategy: flatCleanupAlert.strategy,
              market: flatCleanupAlert.market,
              desired_position: (flatCleanupAlert as any).desired_position,
              signal: (flatCleanupAlert as any).signal,
              profile: (flatCleanupAlert as any).profile
            }
          };
          console.log('Decentrader flat managed-order cleanup:', result.dynamicSlSync);
          return;
        }

        delete state.managedPosition;
        result.dynamicSlSync = {
          outcome: 'SKIPPED',
          reason: `${market} is flat; no dynamic SL sync needed.`
        };
        return;
      }

      const managedPosition = state.managedPosition;
      const positionDirection: TradePlanDirection = position.size > 0 ? 'long' : 'short';
      const entryPriceMismatch = Boolean(
        managedPosition?.entryPrice &&
        position.entryPrice &&
        Math.abs(position.entryPrice - managedPosition.entryPrice) / managedPosition.entryPrice > 0.005
      );
      const positionGrewBeyondManagedSize =
        managedPosition &&
        Math.abs(position.size) > managedPosition.initialSize + 0.00000001;

      if (
        !managedPosition ||
        managedPosition.market.replace(/_/g, '-').toUpperCase() !== market ||
        managedPosition.direction !== positionDirection ||
        entryPriceMismatch ||
        positionGrewBeyondManagedSize
      ) {
        result.dynamicSlSync = {
          outcome: 'SKIPPED',
          reason: 'Existing BTC position was not opened by this Decentrader monitor; SL orders were preserved.',
          market,
          position,
          managedPosition: managedPosition || null
        };
        return;
      }

      if (!managedPosition.currentStop || managedPosition.currentStop <= 0) {
        result.dynamicSlSync = {
          outcome: 'SKIPPED',
          reason: 'Managed position has no known current stop yet; add-only trailing SL skipped.',
          market,
          position,
          managedPosition
        };
        return;
      }

      const plan = await this.getTradePlan(account, market);
      const directionalPlan = plan?.plans?.[positionDirection];
      const candidateStop = numberOrZero(directionalPlan?.stop?.price);
      const currentPrice =
        numberOrZero(directionalPlan?.entryReference?.price) ||
        numberOrZero(plan?.marketInfo?.oraclePrice) ||
        numberOrZero(plan?.price);
      const stopBreached =
        currentPrice > 0 &&
        (positionDirection === 'long'
          ? currentPrice <= managedPosition.currentStop
          : currentPrice >= managedPosition.currentStop);

      if (stopBreached) {
        const flatAlert = buildDecentraderStopBreachFlatAlert(
          market,
          currentPrice,
          position,
          managedPosition.currentStop
        );

        console.error('Managed Decentrader SL was breached while residual position remained open; flattening residual:', {
          market,
          position,
          currentPrice,
          currentStop: managedPosition.currentStop,
          managedPosition
        });

        await executor.placeOrder(flatAlert);

        const accountAfterFlat = await executor.getAccountSnapshot([market]);
        const remainingPosition = existingMarketPosition(accountAfterFlat, market);

        if (!remainingPosition) {
          delete state.managedPosition;
        }

        result.dynamicSlSync = {
          outcome: remainingPosition ? 'FLATTEN_ATTEMPTED' : 'FLATTENED',
          reason: remainingPosition
            ? 'Managed SL was breached; residual flatten was attempted but a position is still visible.'
            : 'Managed SL was breached; residual position was flattened through the dYdX FLAT target flow.',
          market,
          position,
          remainingPosition: remainingPosition || null,
          currentPrice,
          breachedStop: managedPosition.currentStop,
          flatAlert: {
            strategy: flatAlert.strategy,
            market: flatAlert.market,
            desired_position: (flatAlert as any).desired_position,
            size: flatAlert.size,
            signal: (flatAlert as any).signal,
            profile: (flatAlert as any).profile
          }
        };
        console.log('Decentrader stop-breach residual flat:', result.dynamicSlSync);
        return;
      }

      const minImprovementPct = decentraderDynamicSlMinImprovementPct();
      const improves =
        positionDirection === 'long'
          ? candidateStop > managedPosition.currentStop * (1 + minImprovementPct)
          : candidateStop < managedPosition.currentStop * (1 - minImprovementPct);

      if (
        !directionalPlan?.stop?.valid ||
        !candidateStop ||
        !improves
      ) {
        if (!decentraderDynamicSlCoverageSyncEnabled()) {
          result.dynamicSlSync = {
            outcome: 'UNCHANGED',
            reason: 'Latest confirmed fractal stop does not improve the managed SL enough; coverage refresh is disabled to avoid duplicate dYdX conditional stops when indexer visibility lags.',
            market,
            position,
            currentStop: managedPosition.currentStop,
            candidateStop: candidateStop || null,
            minImprovementPct,
            coverageSyncEnabled: false,
            stop: directionalPlan?.stop || null
          };

          console.log('Decentrader dynamic SL coverage sync skipped:', result.dynamicSlSync);
          return;
        }

        const coverageAlert = buildDecentraderDynamicSlAlert(
          plan,
          position,
          managedPosition.currentStop
        );
        const coverageSyncResult = await syncTrailingStop(coverageAlert);

        result.dynamicSlSync = {
          ...coverageSyncResult,
          outcome: coverageSyncResult?.outcome === 'UPDATED' ? 'UPDATED' : 'UNCHANGED',
          reason:
            coverageSyncResult?.outcome === 'UPDATED'
              ? 'Latest confirmed fractal stop does not improve the managed SL enough, but protective stop coverage was resized/refreshed.'
              : 'Latest confirmed fractal stop does not improve the managed SL enough; protective stop coverage was checked.',
          market,
          position,
          currentStop: managedPosition.currentStop,
          candidateStop: candidateStop || null,
          minImprovementPct,
          stop: directionalPlan?.stop || null
        };

        if (coverageSyncResult?.outcome === 'UPDATED') {
          managedPosition.currentStopUpdatedAt = nowNlIso();
        }

        console.log('Decentrader dynamic SL coverage sync:', result.dynamicSlSync);
        return;
      }

      if (!decentraderDynamicSlLiveUpdatesEnabled()) {
        result.dynamicSlSync = {
          outcome: 'READY',
          reason: 'Latest confirmed fractal stop improves the managed SL, but live dynamic SL order updates are disabled to avoid duplicate dYdX conditional stops while indexer visibility is unreliable.',
          market,
          position,
          previousStop: managedPosition.currentStop,
          candidateStop,
          liveUpdatesEnabled: false,
          timestamp: plan.timestamp,
          timestampNl: plan.timestampNl,
          stop: directionalPlan.stop
        };

        console.log('Decentrader dynamic SL live update skipped:', result.dynamicSlSync);
        return;
      }

      const alert = buildDecentraderDynamicSlAlert(plan, position, candidateStop);
      const syncResult = await syncTrailingStop(alert);
      result.dynamicSlSync = {
        ...syncResult,
        market,
        position,
        previousStop: managedPosition.currentStop,
        candidateStop,
        timestamp: plan.timestamp,
        timestampNl: plan.timestampNl,
        stop: directionalPlan.stop
      };

      if (syncResult?.outcome === 'UPDATED' || syncResult?.outcome === 'UNCHANGED') {
        managedPosition.currentStop = candidateStop;
        managedPosition.currentStopUpdatedAt = nowNlIso();
      }

      console.log('Decentrader dynamic SL sync:', result.dynamicSlSync);
    } catch (error) {
      result.dynamicSlSync = {
        outcome: 'ERROR',
        reason: error instanceof Error ? error.message : String(error),
        market
      };
      console.error('Decentrader dynamic SL sync failed; existing stops were left unchanged:', error);
    }
  }

  private recordTradeDecision(
    state: AlertState,
    result: any,
    alert: GapAlert,
    signature: string,
    outcome: TradeDecisionOutcome,
    reason: string,
    details: Record<string, unknown> = {}
  ): void {
    const decision = {
      at: nowNlIso(),
      outcome,
      reason,
      signature,
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      price: alert.price,
      sideCounts: sideCounts(alert),
      direction: mapDirectionFromAlert(alert) || 'none',
      ...details
    };

    state.lastTradeDecision = decision;
    result.tradeDecision = decision;
    result.lastTradeDecision = decision;
    console.log('Decentrader auto-trade decision:', decision);
  }

  private async maybeExecuteTradeForAlert(
    alert: GapAlert,
    signature: string,
    state: AlertState,
    result: any,
    options: TradeEvaluationOptions = {}
  ): Promise<void> {
    if (!decentraderAutoTradeEnabled()) {
      result.tradeSkipped = 'DECENTRADER_AUTO_TRADE_ENABLED is not true.';
      this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped);
      return;
    }

    if (!this.tradeExecutor) {
      result.tradeSkipped = 'No dYdX trade executor is configured.';
      this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped);
      return;
    }

    if (signature === state.lastTradeExecutedSignature) {
      result.tradeSkipped = 'Duplicate Decentrader trade signature.';
      result.duplicate = true;
      this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped);
      return;
    }

    const latestTimestamp = this.latestRows?.[this.latestRows.length - 1]?.timestamp;
    if (alert.timestamp !== latestTimestamp) {
      result.tradeSkipped = `Stale Decentrader alert ${alert.timestamp}; latest frame is ${latestTimestamp || '-'}.`;
      this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
        latestTimestamp: latestTimestamp || null
      });
      return;
    }

    const direction = mapDirectionFromAlert(alert);
    if (!direction) {
      result.tradeSkipped = 'No one-sided edge direction for Decentrader trade.';
      this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped);
      return;
    }

    const market = decentraderTradeMarket();
    result.tradeAttempted = true;
    console.log('Decentrader auto-trade evaluation started:', {
      dryRun: Boolean(options.dryRun),
      signature,
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      direction,
      market,
      price: alert.price
    });

    try {
      const account = await this.tradeExecutor.getAccountSnapshot([market]);
      const openMarketPosition = existingMarketPosition(account, market);
      const openMarketDirection = positionDirection(openMarketPosition);

      if (openMarketPosition && (options.liveTestHoldSeconds !== undefined || openMarketDirection === direction)) {
        result.tradeSkipped = `Existing ${market} position detected; new edge trade skipped.`;
        result.tradeAttempted = false;
        this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
          market,
          existingPosition: openMarketPosition,
          existingDirection: openMarketDirection || null,
          requestedDirection: direction,
          openPositionsCount: account.openPositionsCount
        });
        return;
      }

      if (openMarketPosition && openMarketDirection && openMarketDirection !== direction) {
        result.reversal = {
          from: openMarketDirection,
          to: direction,
          existingPosition: openMarketPosition
        };
        console.warn('Decentrader opposite edge detected; switching through dYdX target flow.', {
          market,
          signature,
          from: openMarketDirection,
          to: direction,
          existingPosition: openMarketPosition
        });
      }

      if (!openMarketPosition && !account.openPositions && account.openPositionsCount > 0) {
        result.tradeSkipped = `Existing position detected but ${market} direction could not be verified; new edge trade skipped.`;
        result.tradeAttempted = false;
        this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
          market,
          existingPosition: null,
          openPositionsCount: account.openPositionsCount
        });
        return;
      }

      const plan = await this.getTradePlan(account, market, options.simulatedDirection);

      if (plan.timestamp !== alert.timestamp) {
        result.tradeSkipped = `Trade plan timestamp ${plan.timestamp} does not match alert ${alert.timestamp}.`;
        result.tradeAttempted = false;
        this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
          planTimestamp: plan.timestamp
        });
        return;
      }

      if (plan.signal?.direction !== direction || !plan.activePlan) {
        result.tradeSkipped = `Trade plan direction ${plan.signal?.direction || 'none'} did not match alert ${direction}.`;
        result.tradeAttempted = false;
        this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
          planDirection: plan.signal?.direction || 'none'
        });
        return;
      }

      if (
        plan.activePlan.status === 'invalid-stop' ||
        plan.activePlan.status === 'invalid-size' ||
        plan.activePlan.stop?.valid === false
      ) {
        if (options.liveTestHoldSeconds !== undefined) {
          result.normalPlanWouldSkip = {
            status: plan.activePlan.status,
            reason:
              plan.activePlan.statusReason ||
              plan.activePlan.stop?.reason ||
              'The normal production plan is invalid.',
            stop: plan.activePlan.stop,
            sizing: plan.activePlan.sizing
          };
          console.warn('Decentrader live test overriding normal production safety skip:', {
            signature,
            direction,
            market,
            normalPlanWouldSkip: result.normalPlanWouldSkip
          });
        } else {
          result.tradeSkipped =
            plan.activePlan.statusReason ||
            plan.activePlan.stop?.reason ||
            'Trade skipped because the active trade plan is invalid.';
          result.tradeAttempted = false;
          result.tradePlan = {
            timestamp: plan.timestamp,
            timestampNl: plan.timestampNl,
            market: plan.market,
            direction: plan.activePlan.direction,
            status: plan.activePlan.status,
            statusReason: plan.activePlan.statusReason,
            stop: plan.activePlan.stop,
            size: plan.activePlan.sizing?.size,
            rawSize: plan.activePlan.sizing?.rawSize,
            minimumOrderSize: plan.activePlan.sizing?.minimumOrderSize,
            minimumOrderRiskUsd: plan.activePlan.sizing?.minimumOrderRiskUsd,
            minimumOrderRiskPctOfEquity: plan.activePlan.sizing?.minimumOrderRiskPctOfEquity,
            notional: plan.activePlan.sizing?.notional
          };
          this.recordTradeDecision(state, result, alert, signature, 'SKIPPED', result.tradeSkipped, {
            market: plan.market,
            status: plan.activePlan.status,
            stop: plan.activePlan.stop,
            size: plan.activePlan.sizing?.size,
            rawSize: plan.activePlan.sizing?.rawSize,
            minimumOrderSize: plan.activePlan.sizing?.minimumOrderSize,
            minimumOrderRiskUsd: plan.activePlan.sizing?.minimumOrderRiskUsd,
            minimumOrderRiskPctOfEquity: plan.activePlan.sizing?.minimumOrderRiskPctOfEquity,
            notional: plan.activePlan.sizing?.notional
          });
          return;
        }
      }

      const orderAlert = options.liveTestHoldSeconds !== undefined
        ? buildDecentraderLiveTestOrderAlert(plan, signature, direction)
        : buildDecentraderOrderAlert(plan, signature);
      result.tradePlan = {
        timestamp: plan.timestamp,
        timestampNl: plan.timestampNl,
        market: plan.market,
        direction: plan.activePlan.direction,
        status: plan.activePlan.status,
        stop: plan.activePlan.stop,
        size: plan.activePlan.sizing?.size,
        notional: plan.activePlan.sizing?.notional,
        confidenceScore: plan.activePlan.sizing?.confidenceScore,
        takeProfits: plan.activePlan.takeProfits
      };
      result.tradeAlert = {
        strategy: orderAlert.strategy,
        market: orderAlert.market,
        desired_position: orderAlert.desired_position,
        size: orderAlert.size,
        sizeUsd: orderAlert.sizeUsd,
        price: orderAlert.price,
        entry_price: (orderAlert as any).entry_price,
        static_sl: (orderAlert as any).static_sl,
        signal: (orderAlert as any).signal,
        profile: (orderAlert as any).profile,
        take_profits: (orderAlert as any).take_profits
      };

      if (options.dryRun) {
        result.orderPlacementAttempted = false;
        result.tradePlaced = false;
        result.tradeSkipped = null;
        this.recordTradeDecision(
          state,
          result,
          alert,
          signature,
          'READY',
          'End-to-end dry run reached the dYdX placement boundary. No order was placed.',
          {
            dryRun: true,
            market: orderAlert.market,
            desiredPosition: orderAlert.desired_position,
            size: orderAlert.size,
            notional: orderAlert.sizeUsd,
            staticSl: (orderAlert as any).static_sl,
            takeProfits: (orderAlert as any).take_profits,
            reversal: result.reversal || null
          }
        );
        return;
      }

      if (options.liveTestHoldSeconds !== undefined) {
        const holdSeconds = clamp(Math.floor(options.liveTestHoldSeconds), 5, 60);
        const flatAlert = buildDecentraderLiveTestFlatAlert(orderAlert);
        let entryCompleted = false;
        let flattenCompleted = false;

        result.liveTest = {
          enabled: true,
          holdSeconds,
          entryCompleted,
          flattenCompleted
        };
        result.orderPlacementAttempted = true;

        try {
          console.warn('Decentrader LIVE TEST placing temporary order:', {
            signature,
            market: orderAlert.market,
            desiredPosition: orderAlert.desired_position,
            size: orderAlert.size,
            staticSl: (orderAlert as any).static_sl,
            takeProfits: (orderAlert as any).take_profits,
            holdSeconds
          });
          await this.tradeExecutor.placeOrder(orderAlert);
          const entrySnapshot = await this.tradeExecutor.getAccountSnapshot([orderAlert.market]);
          const observedEntryPosition = existingMarketPosition(entrySnapshot, orderAlert.market);
          const expectedLong = direction === 'long';

          if (
            !observedEntryPosition ||
            (expectedLong && observedEntryPosition.size <= 0) ||
            (!expectedLong && observedEntryPosition.size >= 0)
          ) {
            throw new Error(
              `Live test entry flow completed but no matching ${direction.toUpperCase()} ${orderAlert.market} position was found.`
            );
          }

          entryCompleted = true;
          result.liveTest.entryCompleted = true;
          result.liveTest.entryPosition = observedEntryPosition;
          console.warn('Decentrader LIVE TEST entry flow completed; waiting before automatic flat:', {
            signature,
            market: orderAlert.market,
            entryPosition: observedEntryPosition,
            holdSeconds
          });
          await new Promise((resolve) => setTimeout(resolve, holdSeconds * 1000));
        } finally {
          console.warn('Decentrader LIVE TEST automatic flat starting:', {
            signature,
            market: orderAlert.market,
            entryCompleted
          });
          let remainingPosition: DydxOpenPosition | undefined;

          for (let flattenAttempt = 1; flattenAttempt <= 2; flattenAttempt += 1) {
            try {
              await this.tradeExecutor.placeOrder(flatAlert);
              const flatSnapshot = await this.tradeExecutor.getAccountSnapshot([orderAlert.market]);
              remainingPosition = existingMarketPosition(flatSnapshot, orderAlert.market);

              if (!remainingPosition) {
                flattenCompleted = true;
                result.liveTest.flattenCompleted = true;
                break;
              }
            } catch (flattenError) {
              if (flattenAttempt === 2) throw flattenError;
              console.error('Decentrader LIVE TEST automatic flat attempt failed; retrying:', {
                signature,
                market: orderAlert.market,
                flattenAttempt,
                error: flattenError instanceof Error ? flattenError.message : String(flattenError)
              });
              continue;
            }

            console.error('Decentrader LIVE TEST position remains after flat attempt; retrying:', {
              signature,
              market: orderAlert.market,
              flattenAttempt,
              remainingPosition
            });
          }

          if (!flattenCompleted) {
            result.liveTest.remainingPosition = remainingPosition || null;
            throw new Error(
              `Live test automatic flat completed but ${orderAlert.market} position ${remainingPosition?.size ?? 'unknown'} is still open.`
            );
          }

          console.warn('Decentrader LIVE TEST automatic flat completed:', {
            signature,
            market: orderAlert.market
          });
        }

        result.tradePlaced = entryCompleted;
        result.tradeSkipped = null;
        this.recordTradeDecision(
          state,
          result,
          alert,
          signature,
          'PLACED',
          'Live end-to-end test order flow completed and position was automatically flattened.',
          {
            liveTest: true,
            market: orderAlert.market,
            desiredPosition: orderAlert.desired_position,
            size: orderAlert.size,
            holdSeconds,
            entryCompleted,
            flattenCompleted
          }
        );
        return;
      }

      await this.tradeExecutor.placeOrder(orderAlert);
      const entrySnapshot = await this.tradeExecutor.getAccountSnapshot([orderAlert.market]);
      const observedEntryPosition = existingMarketPosition(entrySnapshot, orderAlert.market);
      const expectedLong = direction === 'long';

      if (
        !observedEntryPosition ||
        (expectedLong && observedEntryPosition.size <= 0) ||
        (!expectedLong && observedEntryPosition.size >= 0)
      ) {
        throw new Error(
          `dYdX order flow completed but no matching ${direction.toUpperCase()} ${orderAlert.market} position was found.`
        );
      }

      state.lastTradeExecutedSignature = signature;
      state.lastTradeExecutedAt = nowNlIso();
      state.lastTradeExecutionError = undefined;
      state.managedPosition = {
        market: orderAlert.market,
        direction,
        openedAt: state.lastTradeExecutedAt,
        entrySignature: signature,
        initialSize: Math.abs(observedEntryPosition.size),
        entryPrice: observedEntryPosition.entryPrice,
        currentStop: numberOrZero((orderAlert as any).static_sl),
        currentStopUpdatedAt: state.lastTradeExecutedAt
      };
      result.tradePlaced = true;
      result.tradeSkipped = null;
      this.recordTradeDecision(state, result, alert, signature, 'PLACED', 'dYdX order flow completed.', {
        market: orderAlert.market,
        desiredPosition: orderAlert.desired_position,
        size: orderAlert.size,
        notional: orderAlert.sizeUsd,
        observedPosition: observedEntryPosition,
        staticSl: (orderAlert as any).static_sl,
        takeProfits: (orderAlert as any).take_profits,
        reversal: result.reversal || null
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      state.lastTradeExecutionError = message;
      result.tradeError = message;
      console.error('Decentrader auto trade failed:', error);
      this.recordTradeDecision(state, result, alert, signature, 'ERROR', message, {
        market
      });
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

  async getTpBacktest(options: TpBacktestOptions = {}): Promise<any> {
    const config = this.config();
    const cachedRows = this.latestRows;
    const rows = cachedRows && cachedRows.length ? cachedRows : await fetchSnapshot(config.symbol);
    this.latestRows = rows;

    return backtestTpZones(rows, options);
  }

  async getTradePlan(
    account: DydxSizingAccountSnapshot,
    market = 'BTC-USD',
    simulatedDirection?: TradePlanDirection
  ): Promise<any> {
    const config = this.config();
    const rows = this.latestRows || (await fetchSnapshot(config.symbol));
    this.latestRows = rows;
    this.latestTimelapsePayload = this.latestTimelapsePayload || buildTimelapsePayload(rows, config.symbol);

    const frameIndex = rows.length - 1;
    const frame = rows[frameIndex];

    if (!frame) {
      throw new Error('No Decentrader rows available for trade planning.');
    }

    const price = parseNumber(frame?.ohlc4);
    const bars = activeBarsForFrame(rows, frameIndex);
    const gap = cleanGapForBars(frame, bars);
    const detectedAlert = detectGapIntrusion(rows, frameIndex);
    const alert = simulatedDirection
      ? buildSimulatedGapAlert(rows, frameIndex, simulatedDirection, gap)
      : detectedAlert;
    const zones = tradeZonesForFrame(rows, frameIndex);
    const normalizedMarket = market.replace(/_/g, '-').toUpperCase();
    const marketInfo =
      account.markets[normalizedMarket] ||
      account.markets[market] ||
      Object.values(account.markets)[0];

    if (price === undefined) {
      throw new Error('Latest Decentrader frame has no price.');
    }

    if (!marketInfo) {
      throw new Error(`No dYdX market info available for ${normalizedMarket}.`);
    }

    const mode = String(process.env.DECENTRADER_TRADE_SIZING_MODE || 'growth')
      .trim()
      .toLowerCase();
    const longPlan = buildDirectionalPlan('long', account, marketInfo, rows, frameIndex, gap, alert, zones, price, mode);
    const shortPlan = buildDirectionalPlan('short', account, marketInfo, rows, frameIndex, gap, alert, zones, price, mode);
    const activeDirection = simulatedDirection || mapDirectionFromAlert(alert);
    const activePlan = activeDirection === 'long' ? longPlan : activeDirection === 'short' ? shortPlan : null;

    return {
      ok: true,
      symbol: config.symbol,
      market: normalizedMarket,
      timestamp: String(frame.timestamp || ''),
      timestampNl: nlTime(frame.timestamp),
      price,
      signal: {
        direction: activeDirection || 'none',
        simulated: Boolean(simulatedDirection),
        simulatedEdge: simulatedDirection === 'long'
          ? 'left'
          : simulatedDirection === 'short'
            ? 'right'
            : undefined,
        reason: activeDirection
          ? simulatedDirection
            ? `${activeDirection} bias from simulated ${simulatedDirection === 'long' ? 'left' : 'right'} edge`
            : `${activeDirection} bias from latest gap intrusion`
          : 'No fresh one-sided gap intrusion on latest frame',
        alert: alert
          ? {
              sideCounts: sideCounts(alert),
              entrants: alert.entrants,
              leftCount: alert.left.length,
              rightCount: alert.right.length
            }
          : null
      },
      account: {
        equity: account.equity,
        freeCollateral: account.freeCollateral,
        openPositionsCount: account.openPositionsCount,
        updatedAt: account.updatedAt
      },
      gap: gap || null,
      marketInfo,
      activePlan,
      plans: {
        long: longPlan,
        short: shortPlan
      },
      note:
        'Planning layer. It estimates trigger, confirmed-fractal SL, map TP and dYdX size from live equity; this endpoint does not place orders.'
    };
  }

  private async evaluateSimulatedEdge(
    edge: 'left' | 'right',
    options: TradeEvaluationOptions
  ): Promise<any> {
    const config = this.config();
    const direction: TradePlanDirection = edge === 'left' ? 'long' : 'short';
    const rows = await fetchSnapshot(config.symbol);
    const frameIndex = rows.length - 1;
    const frame = rows[frameIndex];
    const bars = activeBarsForFrame(rows, frameIndex);
    const gap = cleanGapForBars(frame, bars);
    const alert = buildSimulatedGapAlert(rows, frameIndex, direction, gap);
    const signature = `simulation-e2e|${edge}|${alert.timestamp}|${Date.now()}`;
    const state = { ...readState(config.stateFile) };
    const result: any = {
      ok: true,
      dryRun: Boolean(options.dryRun),
      liveTest: options.liveTestHoldSeconds !== undefined,
      orderPlacementAttempted: false,
      pipeline: 'synthetic map alert -> production auto-trade evaluation -> dYdX placement boundary',
      edge,
      direction,
      market: decentraderTradeMarket(),
      timestamp: alert.timestamp,
      timestampNl: alert.timestampNl,
      alert: {
        signature,
        price: alert.price,
        sideCounts: sideCounts(alert),
        gap: alert.previousGap,
        entrants: alert.entrants
      },
      tradeAttempted: false,
      tradePlaced: false,
      tradeSkipped: null,
      tradeError: null,
      tradeAlert: null,
      tradePlan: null,
      tradeDecision: null
    };

    this.latestRows = rows;
    this.latestTimelapsePayload = buildTimelapsePayload(rows, config.symbol);
    console.log('Decentrader end-to-end edge simulation detected:', {
      dryRun: Boolean(options.dryRun),
      liveTest: options.liveTestHoldSeconds !== undefined,
      signature,
      edge,
      direction,
      market: decentraderTradeMarket(),
      timestamp: alert.timestamp,
      price: alert.price,
      sideCounts: sideCounts(alert)
    });

    await this.maybeExecuteTradeForAlert(alert, signature, state, result, {
      ...options,
      simulatedDirection: direction
    });

    result.outcome = result.tradeDecision?.outcome || 'UNKNOWN';
    result.reason =
      result.tradeDecision?.reason ||
      result.tradeSkipped ||
      result.tradeError ||
      'Dry-run evaluation completed without a recorded decision.';
    return result;
  }

  async simulateEdge(edge: 'left' | 'right'): Promise<any> {
    return this.evaluateSimulatedEdge(edge, { dryRun: true });
  }

  async runLiveEdgeTest(edge: 'left' | 'right', holdSeconds = 20): Promise<any> {
    const normalizedHoldSeconds = Number.isFinite(holdSeconds)
      ? clamp(Math.floor(holdSeconds), 5, 60)
      : 20;

    return this.evaluateSimulatedEdge(edge, {
      liveTestHoldSeconds: normalizedHoldSeconds
    });
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
