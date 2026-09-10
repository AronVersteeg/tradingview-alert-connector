import fs from 'fs';
import path from 'path';

import { binanceGet } from './binanceHttp';
import { allocateStepSizes } from './decentraderExecutionPolicy';

const HOUR_MS = 60 * 60_000;
const CLOSE_BUFFER_MS = 20_000;
export const MANUAL_OVERRIDE_MAX_ENTRY_DELAY_MS = 15 * 60_000;
const MAX_TAKE_PROFITS = 6;
const BINANCE_FUTURES_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';

export type BtcManualHourlyCandle = {
  openTime: number;
  closeTime: number;
  close: string;
};

export type BtcManualTakeProfit = {
  price: number;
  allocationPct: number;
};

export type BtcManualTradeOverrideRequest = {
  direction: 'long' | 'short';
  closeTrigger: number;
  takeProfits: BtcManualTakeProfit[];
  expiresInHours?: number;
};

export type BtcManualEntryRequest = {
  market: 'BTC-USD';
  direction: 'long' | 'short';
  signature: string;
  signalCandleStartedAt: string;
  signalCandleClosedAt: string;
  signalClose: number;
  closeTrigger: number;
  takeProfits: BtcManualTakeProfit[];
};

export type BtcManualEntryHandler = {
  executeManualBtcEntry: (request: BtcManualEntryRequest) => Promise<any>;
};

export type BtcManualTradeOverrideState = {
  version: 1;
  market: 'BTC-USD';
  status: 'IDLE' | 'ARMED' | 'EXECUTING' | 'TRIGGERED' | 'SKIPPED' | 'ERROR' | 'CANCELLED' | 'EXPIRED';
  direction?: 'long' | 'short';
  closeTrigger?: number;
  takeProfits?: BtcManualTakeProfit[];
  armedAt?: string;
  expiresAt?: string;
  cancelledAt?: string;
  lastEvaluatedCandleStartedAt?: string;
  triggeredAt?: string;
  signalCandleStartedAt?: string;
  signalCandleClosedAt?: string;
  signalClose?: number;
  result?: any;
  updatedAt: string;
};

function boolValue(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || String(value).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export function btcManualTradeOverrideEnabled(): boolean {
  return boolValue(process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED, false);
}

function stateFile(): string {
  return String(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE || '').trim()
    || path.join(process.cwd(), 'data', 'btc-manual-trade-override.json');
}

function idleState(): BtcManualTradeOverrideState {
  return {
    version: 1,
    market: 'BTC-USD',
    status: 'IDLE',
    updatedAt: new Date().toISOString()
  };
}

export function readBtcManualTradeOverrideState(): BtcManualTradeOverrideState {
  try {
    const parsed = JSON.parse(fs.readFileSync(stateFile(), 'utf8'));
    if (parsed?.version !== 1 || parsed?.market !== 'BTC-USD') return idleState();
    return parsed as BtcManualTradeOverrideState;
  } catch {
    return idleState();
  }
}

function writeState(state: BtcManualTradeOverrideState): void {
  const target = stateFile();
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const temporary = `${target}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(state, null, 2));
  fs.renameSync(temporary, target);
}

export function btcManualTradeOverrideIsArmed(nowMs = Date.now()): boolean {
  const state = readBtcManualTradeOverrideState();
  return (
    (state.status === 'ARMED' || state.status === 'EXECUTING') &&
    Date.parse(String(state.expiresAt || '')) > nowMs
  );
}

function positiveNumber(value: unknown, label: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`${label} must be a positive number.`);
  return parsed;
}

export function normalizeBtcManualTradeOverrideRequest(
  input: any,
  nowMs = Date.now()
): BtcManualTradeOverrideState {
  const direction = String(input?.direction || '').trim().toLowerCase();
  if (direction !== 'long' && direction !== 'short') {
    throw new Error('Direction must be long or short.');
  }
  const closeTrigger = positiveNumber(input?.closeTrigger, 'Close trigger');
  const rawTakeProfits = Array.isArray(input?.takeProfits) ? input.takeProfits : [];
  if (rawTakeProfits.length > MAX_TAKE_PROFITS) {
    throw new Error(`At most ${MAX_TAKE_PROFITS} manual take profits are supported.`);
  }
  const takeProfits = rawTakeProfits
    .filter((item: any) => String(item?.price ?? '').trim() !== '')
    .map((item: any, index: number) => ({
      price: positiveNumber(item.price, `TP${index + 1} price`),
      allocationPct: positiveNumber(item.allocationPct, `TP${index + 1} allocation`)
    }));
  if (takeProfits.length) {
    const prices = takeProfits.map((level) => level.price);
    if (new Set(prices).size !== prices.length) throw new Error('Manual TP prices must be unique.');
    if (direction === 'long' && prices.some((price) => price <= closeTrigger)) {
      throw new Error('Every long TP must be above the close trigger.');
    }
    if (direction === 'short' && prices.some((price) => price >= closeTrigger)) {
      throw new Error('Every short TP must be below the close trigger.');
    }
    const totalPct = takeProfits.reduce((total, level) => total + level.allocationPct, 0);
    if (Math.abs(totalPct - 100) > 0.01) {
      throw new Error(`Manual TP allocations must total 100%; received ${totalPct.toFixed(2)}%.`);
    }
    takeProfits.sort((left, right) => direction === 'long' ? left.price - right.price : right.price - left.price);
  }
  const requestedHours = Number(input?.expiresInHours ?? 24);
  const expiresInHours = Number.isFinite(requestedHours) ? Math.max(1, Math.min(168, requestedHours)) : 24;
  const armedAt = new Date(nowMs).toISOString();
  return {
    version: 1,
    market: 'BTC-USD',
    status: 'ARMED',
    direction,
    closeTrigger,
    takeProfits,
    armedAt,
    expiresAt: new Date(nowMs + expiresInHours * HOUR_MS).toISOString(),
    updatedAt: armedAt
  };
}

export function matchingManualOverrideCandle(
  state: BtcManualTradeOverrideState,
  candles: BtcManualHourlyCandle[],
  nowMs = Date.now()
): BtcManualHourlyCandle | undefined {
  if (state.status !== 'ARMED' || !state.direction || !(Number(state.closeTrigger) > 0)) return undefined;
  const armedAtMs = Date.parse(String(state.armedAt || ''));
  const expiresAtMs = Date.parse(String(state.expiresAt || ''));
  const latest = candles
    .filter((candle) => candle.closeTime > armedAtMs && candle.closeTime <= nowMs && candle.closeTime <= expiresAtMs)
    .sort((left, right) => right.openTime - left.openTime)[0];
  if (!latest || nowMs - latest.closeTime > MANUAL_OVERRIDE_MAX_ENTRY_DELAY_MS) return undefined;
  return state.direction === 'long'
    ? Number(latest.close) > Number(state.closeTrigger) ? latest : undefined
    : Number(latest.close) < Number(state.closeTrigger) ? latest : undefined;
}

async function fetchBtcHourlyCandles(nowMs = Date.now()): Promise<BtcManualHourlyCandle[]> {
  const response = await binanceGet<unknown[]>(BINANCE_FUTURES_KLINES_URL, {
    params: { symbol: 'BTCUSDT', interval: '1h', limit: 12 },
    timeout: 20_000
  });
  const candles = (Array.isArray(response.data) ? response.data : [])
    .map((row): BtcManualHourlyCandle | undefined => {
      if (!Array.isArray(row)) return undefined;
      const openTime = Number(row[0]);
      const close = String(row[4] ?? '').trim();
      const closeTime = Number(row[6]);
      if (!Number.isFinite(openTime) || !Number.isFinite(closeTime) || closeTime > nowMs || !(Number(close) > 0)) {
        return undefined;
      }
      return { openTime, closeTime, close };
    })
    .filter((candle): candle is BtcManualHourlyCandle => Boolean(candle))
    .sort((left, right) => left.openTime - right.openTime);
  if (!candles.length) throw new Error('Binance BTCUSDT returned no closed 1H candles.');
  return candles;
}

export function buildManualTakeProfitOrderLevels(
  direction: 'long' | 'short',
  takeProfits: BtcManualTakeProfit[],
  positionSize: number,
  stepSize: number,
  currentPrice: number
): any[] {
  if (!takeProfits.length) return [];
  const invalidPrice = takeProfits.find((level) => direction === 'long'
    ? level.price <= currentPrice
    : level.price >= currentPrice);
  if (invalidPrice) {
    throw new Error(`Manual TP ${invalidPrice.price} is not beyond the current ${direction} entry price ${currentPrice}.`);
  }
  const fractions = takeProfits.map((level) => level.allocationPct / 100);
  const sizes = allocateStepSizes(positionSize, stepSize, fractions, [0]);
  if (sizes.some((size) => !(size > 0))) {
    throw new Error('BTC position is too small to allocate every requested manual TP at the dYdX step size.');
  }
  return takeProfits.map((level, index) => ({
    label: `${direction === 'long' ? 'L' : 'S'} TP${index + 1}`,
    name: `${direction === 'long' ? 'L' : 'S'} TP${index + 1}`,
    price: level.price,
    size: sizes[index],
    allocation_pct: level.allocationPct,
    manual_locked: true
  }));
}

export class BtcManualTradeOverrideMonitor {
  private initialTimer: NodeJS.Timeout | undefined;
  private nextTimer: NodeJS.Timeout | undefined;
  private checkPromise: Promise<void> | undefined;
  private entryHandler: BtcManualEntryHandler | undefined;
  private status: any = { running: false };

  configureEntryHandler(handler: BtcManualEntryHandler): void {
    this.entryHandler = handler;
  }

  start(initialDelayMs = 30_000): void {
    if (this.initialTimer || this.nextTimer) return;
    this.initialTimer = setTimeout(() => {
      this.initialTimer = undefined;
      this.runAndReschedule();
    }, initialDelayMs);
    console.log('BTC manual entry/TP override monitor scheduled:', {
      enabled: btcManualTradeOverrideEnabled(),
      initialDelayMs,
      source: 'Binance Futures BTCUSDT 1H closed candles'
    });
  }

  getStatus(): any {
    return {
      enabled: btcManualTradeOverrideEnabled(),
      configured: Boolean(this.entryHandler),
      ...this.status,
      override: readBtcManualTradeOverrideState()
    };
  }

  arm(input: any): BtcManualTradeOverrideState {
    if (!btcManualTradeOverrideEnabled()) {
      throw new Error('MANUAL_ENTRY_TP_OVERRIDE_ENABLED is false.');
    }
    if (readBtcManualTradeOverrideState().status === 'EXECUTING') {
      throw new Error('The current BTC manual override is already executing and cannot be replaced.');
    }
    const state = normalizeBtcManualTradeOverrideRequest(input);
    writeState(state);
    console.log('BTC manual entry/TP override armed:', state);
    return state;
  }

  cancel(): BtcManualTradeOverrideState {
    const previous = readBtcManualTradeOverrideState();
    if (previous.status === 'EXECUTING') {
      throw new Error('The BTC manual override is already executing and can no longer be cancelled.');
    }
    const now = new Date().toISOString();
    const state: BtcManualTradeOverrideState = {
      ...previous,
      status: 'CANCELLED',
      cancelledAt: now,
      updatedAt: now
    };
    writeState(state);
    console.log('BTC manual entry/TP override cancelled:', state);
    return state;
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
      .catch((error) => console.error('BTC manual entry/TP override check failed:', error))
      .finally(() => this.scheduleNextRun());
  }

  private scheduleNextRun(): void {
    const now = Date.now();
    const nextRunAt = (Math.floor(now / HOUR_MS) + 1) * HOUR_MS + CLOSE_BUFFER_MS;
    this.nextTimer = setTimeout(() => {
      this.nextTimer = undefined;
      this.runAndReschedule();
    }, Math.max(1_000, nextRunAt - now));
    this.status.nextRunAt = new Date(nextRunAt).toISOString();
  }

  private async performCheck(): Promise<void> {
    const startedAt = new Date().toISOString();
    this.status = { ...this.status, running: true, lastStartedAt: startedAt, lastError: undefined };
    try {
      if (!btcManualTradeOverrideEnabled()) return;
      const state = readBtcManualTradeOverrideState();
      if (state.status !== 'ARMED') return;
      const nowMs = Date.now();
      if (Date.parse(String(state.expiresAt || '')) <= nowMs) {
        const expired = { ...state, status: 'EXPIRED' as const, updatedAt: new Date(nowMs).toISOString() };
        writeState(expired);
        return;
      }
      const candles = await fetchBtcHourlyCandles(nowMs);
      const latest = candles[candles.length - 1];
      state.lastEvaluatedCandleStartedAt = latest ? new Date(latest.openTime).toISOString() : undefined;
      state.updatedAt = new Date().toISOString();
      const candle = matchingManualOverrideCandle(state, candles, nowMs);
      if (!candle) {
        writeState(state);
        return;
      }
      state.status = 'EXECUTING';
      state.triggeredAt = new Date().toISOString();
      state.signalCandleStartedAt = new Date(candle.openTime).toISOString();
      state.signalCandleClosedAt = new Date(candle.closeTime).toISOString();
      state.signalClose = Number(candle.close);
      state.updatedAt = state.triggeredAt;
      writeState(state);

      const signature = `btc-manual-override|${state.direction}|${candle.openTime}|${state.closeTrigger}|${state.armedAt}`;
      let result: any;
      if (!this.entryHandler) {
        result = { tradePlaced: false, tradeError: 'No BTC manual entry handler is configured.' };
      } else {
        result = await this.entryHandler.executeManualBtcEntry({
          market: 'BTC-USD',
          direction: state.direction!,
          signature,
          signalCandleStartedAt: state.signalCandleStartedAt,
          signalCandleClosedAt: state.signalCandleClosedAt,
          signalClose: state.signalClose,
          closeTrigger: state.closeTrigger!,
          takeProfits: state.takeProfits || []
        });
      }
      state.result = result;
      state.status = result?.tradePlaced ? 'TRIGGERED' : result?.tradeError ? 'ERROR' : 'SKIPPED';
      state.updatedAt = new Date().toISOString();
      writeState(state);
      console.log('BTC manual entry/TP override completed:', state);
    } catch (error) {
      const state = readBtcManualTradeOverrideState();
      const message = error instanceof Error ? error.message : String(error);
      if (state.status === 'ARMED' || state.status === 'EXECUTING') {
        state.status = 'ERROR';
        state.result = { tradePlaced: false, tradeError: message };
        state.updatedAt = new Date().toISOString();
        writeState(state);
      }
      this.status.lastError = message;
      throw error;
    } finally {
      this.status = { ...this.status, running: false, lastFinishedAt: new Date().toISOString() };
    }
  }
}

export const btcManualTradeOverrideMonitor = new BtcManualTradeOverrideMonitor();
