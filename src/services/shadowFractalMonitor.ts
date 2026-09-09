import fs from 'fs';
import path from 'path';

import { binanceGet } from './binanceHttp';
import {
  BINANCE_DAILY_FRACTAL_MARKETS,
  BinanceDailyCandle,
  ConfirmedWilliamsFractal,
  DailyFractalHistoryItem,
  DailyFractalMarket,
  binanceDailyFractalHistory,
  confirmedWilliamsFractals
} from './binanceDailyFractalHistory';
import { sendEmailBestEffort, smtpSettingsFromEnv } from './decentraderGapMonitor';
import {
  FractalEntryHandler,
  FractalEntryRequest,
  fractalEntryModelEnabled
} from './entryModelSwitches';

const BINANCE_FUTURES_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';
const HOUR_MS = 60 * 60_000;
const CLOSE_BUFFER_MS = 15_000;
export const SHADOW_FRACTAL_MAX_ENTRY_DELAY_MS = 15 * 60_000;

const SHADOW_MARKETS: Array<{ market: DailyFractalMarket; asset: string; symbol: string }> = [
  { market: 'BTC-USD', asset: 'BTC', symbol: 'BTCUSDT' },
  { market: 'ETH-USD', asset: 'ETH', symbol: 'ETHUSDT' },
  { market: 'INJ-USD', asset: 'INJ', symbol: 'INJUSDT' },
  { market: 'SOL-USD', asset: 'SOL', symbol: 'SOLUSDT' },
  { market: 'ZEC-USD', asset: 'ZEC', symbol: 'ZECUSDT' },
  { market: 'PAXG-USD', asset: 'GOLD', symbol: 'XAUUSDT' },
  { market: 'XAG-USD', asset: 'SILVER', symbol: 'XAGUSDT' }
];

export type ShadowHourlyCandle = BinanceDailyCandle & {
  open: string;
  close: string;
};

export type ShadowFractalSignal = {
  direction: 'LONG' | 'SHORT';
  candle: ShadowHourlyCandle;
  previousCandle: ShadowHourlyCandle;
  hourlyFractal: ConfirmedWilliamsFractal;
  dailyFractal: DailyFractalHistoryItem;
};

export type ShadowFractalAlertRecord = {
  signature: string;
  market: DailyFractalMarket;
  asset: string;
  symbol: string;
  direction: 'LONG' | 'SHORT';
  candleStartedAt: string;
  candleClosedAt: string;
  candleClose: string;
  previousClose: string;
  hourlyFractal: ConfirmedWilliamsFractal;
  dailyFractal: {
    type: 'HIGH' | 'LOW';
    price: number;
    priceExact: string;
    pivotAt: string;
    confirmedAt: string;
  };
  observedAt: string;
  emailAttempts?: number;
  lastEmailAttemptAt?: string;
  emailSentAt?: string;
  emailError?: string;
  tradeAttempts?: number;
  lastTradeAttemptAt?: string;
  tradePlacedAt?: string;
  tradeSkipped?: string;
  tradeError?: string;
};

type ShadowFractalState = {
  version: 1;
  updatedAt?: string;
  records: ShadowFractalAlertRecord[];
};

function boolValue(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function stateFile(): string {
  return String(process.env.SHADOW_FRACTAL_HISTORY_FILE || '').trim()
    || path.join(process.cwd(), 'data', 'shadow-fractal-alert-history.json');
}

function readState(): ShadowFractalState {
  try {
    const parsed = JSON.parse(fs.readFileSync(stateFile(), 'utf8'));
    return {
      version: 1,
      updatedAt: typeof parsed?.updatedAt === 'string' ? parsed.updatedAt : undefined,
      records: Array.isArray(parsed?.records) ? parsed.records : []
    };
  } catch {
    return { version: 1, records: [] };
  }
}

function writeState(state: ShadowFractalState): void {
  const target = stateFile();
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, JSON.stringify(state, null, 2));
}

function exactDecimal(value: unknown): string | undefined {
  const text = String(value ?? '').trim();
  return text && Number.isFinite(Number(text)) ? text : undefined;
}

export function parseBinanceHourlyCandles(rows: unknown[], nowMs = Date.now()): ShadowHourlyCandle[] {
  return rows
    .map((row) => {
      if (!Array.isArray(row)) return undefined;
      const openTime = Number(row[0]);
      const open = exactDecimal(row[1]);
      const high = exactDecimal(row[2]);
      const low = exactDecimal(row[3]);
      const close = exactDecimal(row[4]);
      const closeTime = Number(row[6]);
      if (
        !Number.isFinite(openTime) ||
        !Number.isFinite(closeTime) ||
        closeTime > nowMs ||
        !open || !high || !low || !close
      ) {
        return undefined;
      }
      return { openTime, closeTime, open, high, low, close };
    })
    .filter((candle): candle is ShadowHourlyCandle => Boolean(candle))
    .sort((left, right) => left.openTime - right.openTime);
}

function latestHourlyFractal(
  fractals: ConfirmedWilliamsFractal[],
  type: 'HIGH' | 'LOW',
  knownAtMs: number
): ConfirmedWilliamsFractal | undefined {
  return fractals
    .filter((fractal) => fractal.type === type && Date.parse(fractal.confirmedAt) <= knownAtMs)
    .sort((left, right) => Date.parse(right.confirmedAt) - Date.parse(left.confirmedAt))[0];
}

function latestDailyFractal(
  records: DailyFractalHistoryItem[],
  type: 'HIGH' | 'LOW',
  knownAtMs: number
): DailyFractalHistoryItem | undefined {
  return records
    .filter((record) => record.type === type && Date.parse(record.confirmedAt) <= knownAtMs)
    .sort((left, right) => Date.parse(right.confirmedAt) - Date.parse(left.confirmedAt))[0];
}

export function evaluateShadowFractalBreakout(
  hourlyCandles: ShadowHourlyCandle[],
  dailyRecords: DailyFractalHistoryItem[]
): ShadowFractalSignal | undefined {
  if (hourlyCandles.length < 6) return undefined;
  const candle = hourlyCandles[hourlyCandles.length - 1];
  const previousCandle = hourlyCandles[hourlyCandles.length - 2];
  const knownAtMs = candle.closeTime;
  const hourlyFractals = confirmedWilliamsFractals(hourlyCandles);
  const hourlyHigh = latestHourlyFractal(hourlyFractals, 'HIGH', knownAtMs);
  const hourlyLow = latestHourlyFractal(hourlyFractals, 'LOW', knownAtMs);
  const dailyHigh = latestDailyFractal(dailyRecords, 'HIGH', knownAtMs);
  const dailyLow = latestDailyFractal(dailyRecords, 'LOW', knownAtMs);
  const close = Number(candle.close);
  const previousClose = Number(previousCandle.close);

  if (hourlyHigh && dailyHigh) {
    const breakoutLevel = Math.max(hourlyHigh.price, dailyHigh.price);
    if (close > breakoutLevel && previousClose <= breakoutLevel) {
      return {
        direction: 'LONG',
        candle,
        previousCandle,
        hourlyFractal: hourlyHigh,
        dailyFractal: dailyHigh
      };
    }
  }

  if (hourlyLow && dailyLow) {
    const breakoutLevel = Math.min(hourlyLow.price, dailyLow.price);
    if (close < breakoutLevel && previousClose >= breakoutLevel) {
      return {
        direction: 'SHORT',
        candle,
        previousCandle,
        hourlyFractal: hourlyLow,
        dailyFractal: dailyLow
      };
    }
  }

  return undefined;
}

export function shadowFractalEntryIsFresh(candleClosedAtMs: number, nowMs = Date.now()): boolean {
  const ageMs = nowMs - candleClosedAtMs;
  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= SHADOW_FRACTAL_MAX_ENTRY_DELAY_MS;
}

function nlTimestamp(timestampMs: number): string {
  return new Intl.DateTimeFormat('nl-NL', {
    timeZone: 'Europe/Amsterdam',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(new Date(timestampMs)).replace(',', '') + ' NL';
}

function signalBody(config: typeof SHADOW_MARKETS[number], signal: ShadowFractalSignal): string {
  return [
    'Live Shadow strategy entry signal.',
    'A dYdX entry is attempted only when the master model and this pair\'s auto-trade switch are enabled.',
    '',
    `Market: ${config.market}`,
    `Source: Binance Futures ${config.symbol}`,
    `Signal: ${signal.direction}`,
    `1H candle: ${nlTimestamp(signal.candle.openTime)}`,
    `1H candle close: ${signal.candle.close}`,
    `Previous 1H close: ${signal.previousCandle.close}`,
    '',
    `Confirmed 1H Williams ${signal.hourlyFractal.type.toLowerCase()}: ${signal.hourlyFractal.priceExact}`,
    `1H fractal pivot: ${nlTimestamp(Date.parse(signal.hourlyFractal.pivotAt))}`,
    `1H fractal confirmed: ${nlTimestamp(Date.parse(signal.hourlyFractal.confirmedAt))}`,
    `Confirmed Daily Williams ${signal.dailyFractal.type.toLowerCase()}: ${signal.dailyFractal.priceExact}`,
    `Daily fractal pivot: ${nlTimestamp(Date.parse(signal.dailyFractal.pivotAt))}`,
    `Daily fractal confirmed: ${nlTimestamp(Date.parse(signal.dailyFractal.confirmedAt))}`,
    '',
    'Rule: the newly closed 1H candle crossed and closed beyond both latest causally confirmed Williams levels.',
    'Delay: none. Decentrader: not consulted.'
  ].join('\n');
}

async function fetchHourlyCandles(symbol: string): Promise<ShadowHourlyCandle[]> {
  const response = await binanceGet<unknown[]>(BINANCE_FUTURES_KLINES_URL, {
    params: { symbol, interval: '1h', limit: 500 },
    timeout: 20_000
  });
  const candles = parseBinanceHourlyCandles(Array.isArray(response.data) ? response.data : []);
  if (candles.length < 6) throw new Error(`Binance ${symbol} returned only ${candles.length} closed 1H candles.`);
  return candles;
}

export class ShadowFractalMonitor {
  private initialTimer: NodeJS.Timeout | undefined;
  private nextTimer: NodeJS.Timeout | undefined;
  private checkPromise: Promise<void> | undefined;
  private status: any = { enabled: false, running: false, readOnly: false };
  private readonly entryHandlers = new Map<string, FractalEntryHandler>();

  private enabled(): boolean {
    return fractalEntryModelEnabled();
  }

  private marketEnabled(asset: string): boolean {
    return boolValue(process.env[`SHADOW_FRACTAL_${asset}_ENABLED`], true);
  }

  configureEntryHandler(market: string, handler: FractalEntryHandler): void {
    this.entryHandlers.set(String(market).replace(/_/g, '-').toUpperCase(), handler);
  }

  start(initialDelayMs = 390_000): void {
    if (this.initialTimer || this.nextTimer) return;
    this.status = {
      ...this.status,
      enabled: this.enabled(),
      emailConfigured: smtpSettingsFromEnv() !== undefined,
      markets: SHADOW_MARKETS.filter((config) => this.marketEnabled(config.asset)).map((config) => config.market)
    };
    if (!this.enabled()) {
      console.log('Shadow fractal monitor disabled.');
      return;
    }

    this.initialTimer = setTimeout(() => {
      this.initialTimer = undefined;
      this.runAndReschedule();
    }, initialDelayMs);
    console.log('Shadow fractal entry model scheduled:', {
      initialDelayMs,
      markets: this.status.markets,
      source: 'Binance Futures 1H + 1D',
      rule: 'closed 1H cross beyond latest confirmed 1H and Daily Williams fractals'
    });
  }

  stop(): void {
    if (this.initialTimer) clearTimeout(this.initialTimer);
    if (this.nextTimer) clearTimeout(this.nextTimer);
    this.initialTimer = undefined;
    this.nextTimer = undefined;
  }

  getStatus(): any {
    const state = readState();
    return {
      ...this.status,
      enabled: this.enabled(),
      readOnly: false,
      liveEntryEnabled: this.enabled(),
      emailConfigured: smtpSettingsFromEnv() !== undefined,
      historyFile: stateFile(),
      records: state.records.length,
      recent: state.records.slice(-25).reverse()
    };
  }

  async checkOnce(): Promise<void> {
    if (this.checkPromise) return this.checkPromise;
    this.checkPromise = this.performCheck().finally(() => {
      this.checkPromise = undefined;
    });
    return this.checkPromise;
  }

  private runAndReschedule(): void {
    this.checkOnce()
      .catch((error) => console.error('Shadow fractal monitor check failed:', error))
      .finally(() => this.scheduleNextRun());
  }

  private scheduleNextRun(): void {
    const now = Date.now();
    const retryNeeded = Array.isArray(this.status.lastResult) && this.status.lastResult.some(
      (result: any) => result.retryEmail === true
    );
    const nextRunAt = retryNeeded
      ? now + 5 * 60_000
      : (Math.floor(now / HOUR_MS) + 1) * HOUR_MS + CLOSE_BUFFER_MS;
    this.nextTimer = setTimeout(() => {
      this.nextTimer = undefined;
      this.runAndReschedule();
    }, Math.max(1_000, nextRunAt - now));
    this.status.nextRunAt = new Date(nextRunAt).toISOString();
  }

  private async performCheck(): Promise<void> {
    const startedAt = new Date().toISOString();
    this.status = { ...this.status, running: true, lastStartedAt: startedAt, lastError: undefined };
    const results: any[] = [];

    try {
      for (const config of SHADOW_MARKETS) {
        if (!this.marketEnabled(config.asset)) continue;
        try {
          results.push(await this.checkMarket(config));
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          results.push({ market: config.market, error: message });
          console.error(`${config.asset} Shadow fractal check failed:`, error);
        }
      }
      this.status = {
        ...this.status,
        running: false,
        lastFinishedAt: new Date().toISOString(),
        lastResult: results,
        lastError: results.some((result) => result.error) ? 'One or more market checks failed.' : undefined
      };
    } catch (error) {
      this.status = {
        ...this.status,
        running: false,
        lastFinishedAt: new Date().toISOString(),
        lastError: error instanceof Error ? error.message : String(error)
      };
      throw error;
    }
  }

  private async checkMarket(config: typeof SHADOW_MARKETS[number]): Promise<any> {
    if (BINANCE_DAILY_FRACTAL_MARKETS[config.market].symbol !== config.symbol) {
      throw new Error(`Shadow symbol mismatch for ${config.market}.`);
    }
    const [hourlyCandles, dailySnapshot] = await Promise.all([
      fetchHourlyCandles(config.symbol),
      binanceDailyFractalHistory(config.market)
    ]);
    const latestCandle = hourlyCandles[hourlyCandles.length - 1];
    const signal = evaluateShadowFractalBreakout(hourlyCandles, dailySnapshot.records);
    if (!signal) {
      return {
        market: config.market,
        candleStartedAt: new Date(latestCandle.openTime).toISOString(),
        signal: 'NONE'
      };
    }

    const signature = `${config.market}|${signal.direction}|${signal.candle.openTime}|${signal.hourlyFractal.priceExact}|${signal.dailyFractal.priceExact}`;
    const state = readState();
    let record = state.records.find((candidate) => candidate.signature === signature);
    if (!record) {
      record = {
        signature,
        market: config.market,
        asset: config.asset,
        symbol: config.symbol,
        direction: signal.direction,
        candleStartedAt: new Date(signal.candle.openTime).toISOString(),
        candleClosedAt: new Date(signal.candle.closeTime).toISOString(),
        candleClose: signal.candle.close,
        previousClose: signal.previousCandle.close,
        hourlyFractal: signal.hourlyFractal,
        dailyFractal: {
          type: signal.dailyFractal.type,
          price: signal.dailyFractal.price,
          priceExact: signal.dailyFractal.priceExact,
          pivotAt: signal.dailyFractal.pivotAt,
          confirmedAt: signal.dailyFractal.confirmedAt
        },
        observedAt: new Date().toISOString()
      };
      state.records.push(record);
      state.updatedAt = new Date().toISOString();
      writeState(state);
    }

    const timestamp = nlTimestamp(signal.candle.openTime);
    const subject = `${config.asset} Shadow ${signal.direction === 'LONG' ? 'Long' : 'Short'} | ${timestamp}`;
    let emailSent = Boolean(record.emailSentAt);
    if (!emailSent && Number(record.emailAttempts || 0) < 3) {
      const smtp = smtpSettingsFromEnv();
      if (!smtp) {
        record.emailError = 'SMTP is not configured.';
      } else {
        record.emailAttempts = Number(record.emailAttempts || 0) + 1;
        record.lastEmailAttemptAt = new Date().toISOString();
        const email = await sendEmailBestEffort(smtp, subject, signalBody(config, signal));
        emailSent = email.sent;
        if (email.sent) {
          record.emailSentAt = new Date().toISOString();
          record.emailError = undefined;
        } else {
          record.emailError = email.error;
        }
      }
    }

    let tradeResult: any = record.lastTradeAttemptAt
      ? { duplicate: true, tradePlaced: Boolean(record.tradePlacedAt), tradeSkipped: record.tradeSkipped, tradeError: record.tradeError }
      : undefined;
    if (!record.lastTradeAttemptAt) {
      const handler = this.entryHandlers.get(config.market);
      record.tradeAttempts = Number(record.tradeAttempts || 0) + 1;
      record.lastTradeAttemptAt = new Date().toISOString();
      state.updatedAt = record.lastTradeAttemptAt;
      writeState(state);

      if (!shadowFractalEntryIsFresh(signal.candle.closeTime)) {
        const ageMinutes = Math.max(0, (Date.now() - signal.candle.closeTime) / 60_000);
        tradeResult = {
          tradePlaced: false,
          tradeSkipped: `Shadow signal is ${ageMinutes.toFixed(1)} minutes old; live entries are limited to 15 minutes after the 1H close.`
        };
      } else if (!handler) {
        tradeResult = { tradePlaced: false, tradeError: `No fractal entry handler is configured for ${config.market}.` };
      } else {
        const request: FractalEntryRequest = {
          market: config.market,
          direction: signal.direction === 'LONG' ? 'long' : 'short',
          signature: `shadow-fractal|${signature}`,
          signalCandleStartedAt: new Date(signal.candle.openTime).toISOString(),
          signalCandleClosedAt: new Date(signal.candle.closeTime).toISOString(),
          signalClose: Number(signal.candle.close),
          hourlyFractal: signal.hourlyFractal.price,
          dailyFractal: signal.dailyFractal.price
        };
        tradeResult = await handler.executeFractalEntry(request);
      }

      if (tradeResult?.tradePlaced) {
        record.tradePlacedAt = new Date().toISOString();
        record.tradeSkipped = undefined;
        record.tradeError = undefined;
      } else {
        record.tradeSkipped = tradeResult?.tradeSkipped;
        record.tradeError = tradeResult?.tradeError;
      }
    }
    state.updatedAt = new Date().toISOString();
    writeState(state);

    console.log(`${config.asset} Shadow ${signal.direction} observed:`, {
      subject,
      market: config.market,
      symbol: config.symbol,
      close: signal.candle.close,
      hourlyFractal: signal.hourlyFractal.priceExact,
      dailyFractal: signal.dailyFractal.priceExact,
      emailSent,
      tradePlaced: Boolean(tradeResult?.tradePlaced),
      tradeSkipped: tradeResult?.tradeSkipped,
      tradeError: tradeResult?.tradeError,
      readOnly: false
    });
    return {
      market: config.market,
      signal: signal.direction,
      emailSent,
      emailSentAt: record.emailSentAt,
      retryEmail: !emailSent && Boolean(smtpSettingsFromEnv()) && Number(record.emailAttempts || 0) < 3,
      emailError: record.emailError,
      tradeResult
    };
  }
}

export const shadowFractalMonitor = new ShadowFractalMonitor();
