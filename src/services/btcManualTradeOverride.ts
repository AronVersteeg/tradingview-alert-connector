import fs from 'fs';
import path from 'path';

import { binanceGet } from './binanceHttp';
import { allocateStepSizes } from './decentraderExecutionPolicy';

const HOUR_MS = 60 * 60_000;
const CLOSE_BUFFER_MS = 20_000;
export const MANUAL_OVERRIDE_MAX_ENTRY_DELAY_MS = 15 * 60_000;
export const MANUAL_OVERRIDE_RECOVERY_MAX_AGE_MS = 4 * HOUR_MS;
export const MANUAL_OVERRIDE_MAX_RECOVERY_ATTEMPTS = 1;
const MAX_TAKE_PROFITS = 6;
const MAX_ACTIVE_OVERRIDES = 20;
const MAX_TERMINAL_OVERRIDES = 50;
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
  recovery?: boolean;
};

export type BtcManualEntryHandler = {
  executeManualBtcEntry: (request: BtcManualEntryRequest) => Promise<any>;
  sendManualBtcTriggerEmail?: (request: BtcManualEntryRequest, result: any) => Promise<{
    sent: boolean;
    error?: string;
  }>;
};

export type BtcManualTradeOverrideState = {
  version: 1;
  id: string;
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
  triggerEmailSentAt?: string;
  triggerEmailError?: string;
  recoveryAttempts?: number;
  recoveredAt?: string;
  result?: any;
  updatedAt: string;
};

export type BtcManualTradeOverrideStore = {
  version: 3;
  market: 'BTC-USD';
  overrides: BtcManualTradeOverrideState[];
  updatedAt: string;
};

export function btcManualTriggerTimestampNl(signalCandleStartedAt: string): string {
  return new Intl.DateTimeFormat('nl-NL', {
    timeZone: 'Europe/Amsterdam',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(new Date(signalCandleStartedAt)).replace(',', '') + ' NL';
}

export function btcManualTriggerEmailSubject(request: BtcManualEntryRequest): string {
  return `BTC MANUAL ${request.direction.toUpperCase()} TRIGGERED | ${btcManualTriggerTimestampNl(request.signalCandleStartedAt)}`;
}

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

function emptyStore(): BtcManualTradeOverrideStore {
  return {
    version: 3,
    market: 'BTC-USD',
    overrides: [],
    updatedAt: new Date().toISOString()
  };
}

function legacyOverrideId(override: any, index: number): string {
  const direction = override?.direction === 'short' ? 'short' : 'long';
  const armedAt = Date.parse(String(override?.armedAt || ''));
  const timestamp = Number.isFinite(armedAt) ? armedAt : index;
  return `btc-manual-${direction}-${timestamp}-${Number(override?.closeTrigger) || 0}`;
}

function normalizeStoredOverrides(overrides: any[]): BtcManualTradeOverrideState[] {
  const ids = new Set<string>();
  return overrides
    .filter((override: any) => override?.direction === 'long' || override?.direction === 'short')
    .map((override: any, index) => {
      let id = String(override?.id || '').trim();
      if (!id || id === 'long' || id === 'short' || ids.has(id)) id = legacyOverrideId(override, index);
      while (ids.has(id)) id = `${id}-${index}`;
      ids.add(id);
      const normalized = { ...override, id } as BtcManualTradeOverrideState;
      if (normalized.status === 'ARMED' || normalized.status === 'EXECUTING') {
        delete normalized.expiresAt;
      }
      return normalized;
    });
}

export function readBtcManualTradeOverrideStore(): BtcManualTradeOverrideStore {
  try {
    const parsed = JSON.parse(fs.readFileSync(stateFile(), 'utf8'));
    if (parsed?.version === 3 && parsed?.market === 'BTC-USD' && Array.isArray(parsed.overrides)) {
      return {
        ...parsed,
        overrides: normalizeStoredOverrides(parsed.overrides)
      } as BtcManualTradeOverrideStore;
    }
    if (parsed?.version === 2 && parsed?.market === 'BTC-USD' && Array.isArray(parsed.overrides)) {
      return {
        ...parsed,
        version: 3,
        overrides: normalizeStoredOverrides(parsed.overrides)
      } as BtcManualTradeOverrideStore;
    }
    if (
      parsed?.version === 1 &&
      parsed?.market === 'BTC-USD' &&
      (parsed?.direction === 'long' || parsed?.direction === 'short')
    ) {
      return {
        version: 3,
        market: 'BTC-USD',
        overrides: normalizeStoredOverrides([parsed]),
        updatedAt: parsed.updatedAt || new Date().toISOString()
      };
    }
    return emptyStore();
  } catch {
    return emptyStore();
  }
}

function writeStore(store: BtcManualTradeOverrideStore): void {
  const target = stateFile();
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const temporary = `${target}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(store, null, 2));
  fs.renameSync(temporary, target);
}

export function btcManualTradeOverrideIsArmed(): boolean {
  return readBtcManualTradeOverrideStore().overrides.some((state) => (
    state.status === 'ARMED' || state.status === 'EXECUTING'
  ));
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
  const armedAt = new Date(nowMs).toISOString();
  return {
    version: 1,
    id: `btc-manual-${direction}-${nowMs}-${closeTrigger}`,
    market: 'BTC-USD',
    status: 'ARMED',
    direction,
    closeTrigger,
    takeProfits,
    armedAt,
    updatedAt: armedAt
  };
}

export function upsertBtcManualTradeOverride(
  store: BtcManualTradeOverrideStore,
  override: BtcManualTradeOverrideState
): BtcManualTradeOverrideStore {
  const active = store.overrides.filter((candidate) => (
    candidate.status === 'ARMED' || candidate.status === 'EXECUTING'
  ));
  const executing = active.find((candidate) => candidate.status === 'EXECUTING');
  if (executing) {
    throw new Error(`BTC manual plan ${executing.id} is executing; add the new plan after it completes.`);
  }
  if (active.length >= MAX_ACTIVE_OVERRIDES) {
    throw new Error(`At most ${MAX_ACTIVE_OVERRIDES} active BTC manual plans are supported.`);
  }
  const duplicate = active.find((candidate) => (
    candidate.direction === override.direction && Number(candidate.closeTrigger) === Number(override.closeTrigger)
  ));
  if (duplicate) {
    throw new Error(`An armed ${override.direction} BTC plan already uses close trigger ${override.closeTrigger}.`);
  }
  const opposite = active.filter((candidate) => candidate.direction !== override.direction);
  const conflictingOpposite = opposite.find((candidate) => override.direction === 'long'
    ? Number(override.closeTrigger) <= Number(candidate.closeTrigger)
    : Number(override.closeTrigger) >= Number(candidate.closeTrigger));
  if (conflictingOpposite && override.direction === 'long') {
    throw new Error(`Long close trigger must be above the armed short trigger ${conflictingOpposite.closeTrigger}.`);
  }
  if (conflictingOpposite && override.direction === 'short') {
    throw new Error(`Short close trigger must be below the armed long trigger ${conflictingOpposite.closeTrigger}.`);
  }
  const overrides = [...store.overrides, override];
  const activeOverrides = overrides
    .filter((candidate) => candidate.status === 'ARMED' || candidate.status === 'EXECUTING')
    .sort((left, right) => {
      if (left.direction !== right.direction) return left.direction === 'long' ? -1 : 1;
      const priceDifference = left.direction === 'long'
        ? Number(left.closeTrigger) - Number(right.closeTrigger)
        : Number(right.closeTrigger) - Number(left.closeTrigger);
      return priceDifference || Date.parse(String(left.armedAt || '')) - Date.parse(String(right.armedAt || ''));
    });
  const terminalOverrides = overrides
    .filter((candidate) => candidate.status !== 'ARMED' && candidate.status !== 'EXECUTING')
    .sort((left, right) => Date.parse(String(right.updatedAt || '')) - Date.parse(String(left.updatedAt || '')))
    .slice(0, MAX_TERMINAL_OVERRIDES);
  return {
    version: 3,
    market: 'BTC-USD',
    overrides: [...activeOverrides, ...terminalOverrides],
    updatedAt: override.updatedAt
  };
}

export function matchingManualOverrideCandle(
  state: BtcManualTradeOverrideState,
  candles: BtcManualHourlyCandle[],
  nowMs = Date.now()
): BtcManualHourlyCandle | undefined {
  if (state.status !== 'ARMED' || !state.direction || !(Number(state.closeTrigger) > 0)) return undefined;
  const armedAtMs = Date.parse(String(state.armedAt || ''));
  const latest = candles
    .filter((candle) => candle.closeTime > armedAtMs && candle.closeTime <= nowMs)
    .sort((left, right) => right.openTime - left.openTime)[0];
  if (!latest || nowMs - latest.closeTime > MANUAL_OVERRIDE_MAX_ENTRY_DELAY_MS) return undefined;
  return state.direction === 'long'
    ? Number(latest.close) > Number(state.closeTrigger) ? latest : undefined
    : Number(latest.close) < Number(state.closeTrigger) ? latest : undefined;
}

export function recoverableManualOverrideRequest(
  state: BtcManualTradeOverrideState,
  nowMs = Date.now()
): BtcManualEntryRequest | undefined {
  const failedFlat = state.status === 'SKIPPED' &&
    state.result?.tradePlacement?.outcome === 'TARGET_FAILED_FLATTENED';
  const triggeredAtMs = Date.parse(String(state.triggeredAt || ''));
  const signalStartedAtMs = Date.parse(String(state.signalCandleStartedAt || ''));
  const signalClosedAtMs = Date.parse(String(state.signalCandleClosedAt || ''));
  const attempts = Number(state.recoveryAttempts || 0);
  if (
    !failedFlat ||
    attempts >= MANUAL_OVERRIDE_MAX_RECOVERY_ATTEMPTS ||
    !state.direction ||
    !(Number(state.closeTrigger) > 0) ||
    !(Number(state.signalClose) > 0) ||
    !Number.isFinite(triggeredAtMs) ||
    !Number.isFinite(signalStartedAtMs) ||
    !Number.isFinite(signalClosedAtMs) ||
    nowMs - triggeredAtMs > MANUAL_OVERRIDE_RECOVERY_MAX_AGE_MS
  ) {
    return undefined;
  }
  return {
    market: 'BTC-USD',
    direction: state.direction,
    signature: String(state.result?.signature || (
      `btc-manual-override|${state.direction}|${signalStartedAtMs}|${state.closeTrigger}|${state.armedAt}`
    )),
    signalCandleStartedAt: new Date(signalStartedAtMs).toISOString(),
    signalCandleClosedAt: new Date(signalClosedAtMs).toISOString(),
    signalClose: Number(state.signalClose),
    closeTrigger: Number(state.closeTrigger),
    takeProfits: state.takeProfits || [],
    recovery: true
  };
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
    const store = readBtcManualTradeOverrideStore();
    const activeOverride = store.overrides.find((override) => (
      override.status === 'ARMED' || override.status === 'EXECUTING'
    ));
    return {
      enabled: btcManualTradeOverrideEnabled(),
      configured: Boolean(this.entryHandler),
      ...this.status,
      overrides: store.overrides,
      override: activeOverride || store.overrides[0]
    };
  }

  arm(input: any): BtcManualTradeOverrideState {
    if (!btcManualTradeOverrideEnabled()) {
      throw new Error('MANUAL_ENTRY_TP_OVERRIDE_ENABLED is false.');
    }
    const state = normalizeBtcManualTradeOverrideRequest(input);
    const store = upsertBtcManualTradeOverride(readBtcManualTradeOverrideStore(), state);
    writeStore(store);
    console.log('BTC manual entry/TP override armed:', state);
    return state;
  }

  update(idInput: unknown, input: any): BtcManualTradeOverrideState {
    if (!btcManualTradeOverrideEnabled()) {
      throw new Error('MANUAL_ENTRY_TP_OVERRIDE_ENABLED is false.');
    }
    const id = String(idInput || '').trim();
    if (!id) throw new Error('Plan id is required when editing a BTC manual override.');
    const store = readBtcManualTradeOverrideStore();
    const previous = store.overrides.find((override) => override.id === id);
    if (!previous) throw new Error(`No BTC manual plan ${id} exists.`);
    if (previous.status === 'EXECUTING') {
      throw new Error(`BTC manual plan ${previous.id} is already executing and can no longer be edited.`);
    }
    if (previous.status !== 'ARMED') {
      throw new Error(`BTC manual plan ${previous.id} is ${previous.status.toLowerCase()} and is not armed.`);
    }
    const replacement = {
      ...normalizeBtcManualTradeOverrideRequest(input),
      id: previous.id
    };
    const withoutPrevious: BtcManualTradeOverrideStore = {
      ...store,
      overrides: store.overrides.filter((override) => override.id !== previous.id)
    };
    const updatedStore = upsertBtcManualTradeOverride(withoutPrevious, replacement);
    writeStore(updatedStore);
    console.log('BTC manual entry/TP override updated:', {
      previous,
      replacement
    });
    return replacement;
  }

  cancel(idOrDirectionInput?: unknown): BtcManualTradeOverrideState {
    const idOrDirection = String(idOrDirectionInput || '').trim();
    if (!idOrDirection) throw new Error('Plan id is required when cancelling a BTC manual override.');
    const store = readBtcManualTradeOverrideStore();
    const armedDirectionMatches = store.overrides.filter((override) => (
      override.direction === idOrDirection && override.status === 'ARMED'
    ));
    if (armedDirectionMatches.length > 1) {
      throw new Error(`Multiple ${idOrDirection} BTC plans exist; cancel by plan id.`);
    }
    const previous = store.overrides.find((override) => override.id === idOrDirection)
      || armedDirectionMatches[0];
    if (!previous) throw new Error(`No BTC manual plan ${idOrDirection} exists.`);
    if (previous.status === 'EXECUTING') {
      throw new Error(`BTC manual plan ${previous.id} is already executing and can no longer be cancelled.`);
    }
    if (previous.status !== 'ARMED') {
      throw new Error(`BTC manual plan ${previous.id} is ${previous.status.toLowerCase()} and is not armed.`);
    }
    const now = new Date().toISOString();
    const state: BtcManualTradeOverrideState = {
      ...previous,
      status: 'CANCELLED',
      cancelledAt: now,
      updatedAt: now
    };
    writeStore({
      ...store,
      overrides: store.overrides.map((override) => override.id === previous.id ? state : override),
      updatedAt: now
    });
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
      let store = readBtcManualTradeOverrideStore();
      const nowMs = Date.now();
      const nowIso = new Date(nowMs).toISOString();
      const recovery = store.overrides
        .map((override) => ({ override, request: recoverableManualOverrideRequest(override, nowMs) }))
        .find((candidate) => candidate.request);
      if (recovery?.request) {
        const state = recovery.override;
        state.status = 'EXECUTING';
        state.recoveryAttempts = Number(state.recoveryAttempts || 0) + 1;
        state.recoveredAt = nowIso;
        state.updatedAt = nowIso;
        store.updatedAt = nowIso;
        writeStore(store);
        console.warn('Retrying a flat fail-safe BTC manual entry once with fresh market depth:', {
          direction: state.direction,
          signalCandleStartedAt: state.signalCandleStartedAt,
          signalClose: state.signalClose,
          closeTrigger: state.closeTrigger,
          recoveryAttempt: state.recoveryAttempts
        });
        await this.completeExecution(store, state, recovery.request);
        return;
      }
      const armed = store.overrides.filter((override) => override.status === 'ARMED');
      if (!armed.length) {
        writeStore({ ...store, updatedAt: nowIso });
        return;
      }
      const candles = await fetchBtcHourlyCandles(nowMs);
      const latest = candles[candles.length - 1];
      for (const override of armed) {
        override.lastEvaluatedCandleStartedAt = latest ? new Date(latest.openTime).toISOString() : undefined;
        override.updatedAt = nowIso;
      }
      const matched = armed
        .map((override) => ({ override, candle: matchingManualOverrideCandle(override, candles, nowMs) }))
        .find((candidate) => candidate.candle);
      if (!matched?.candle) {
        writeStore({ ...store, updatedAt: nowIso });
        return;
      }
      const state = matched.override;
      const candle = matched.candle;
      state.status = 'EXECUTING';
      state.triggeredAt = nowIso;
      state.signalCandleStartedAt = new Date(candle.openTime).toISOString();
      state.signalCandleClosedAt = new Date(candle.closeTime).toISOString();
      state.signalClose = Number(candle.close);
      state.updatedAt = state.triggeredAt;
      writeStore(store);

      const signature = `btc-manual-override|${state.direction}|${candle.openTime}|${state.closeTrigger}|${state.armedAt}`;
      const entryRequest: BtcManualEntryRequest = {
        market: 'BTC-USD',
        direction: state.direction!,
        signature,
        signalCandleStartedAt: state.signalCandleStartedAt,
        signalCandleClosedAt: state.signalCandleClosedAt,
        signalClose: state.signalClose,
        closeTrigger: state.closeTrigger!,
        takeProfits: state.takeProfits || []
      };
      await this.completeExecution(store, state, entryRequest);
    } catch (error) {
      const store = readBtcManualTradeOverrideStore();
      const message = error instanceof Error ? error.message : String(error);
      const executing = store.overrides.find((state) => state.status === 'EXECUTING');
      if (executing) {
        executing.status = 'ERROR';
        executing.result = { tradePlaced: false, tradeError: message };
        executing.updatedAt = new Date().toISOString();
        store.updatedAt = executing.updatedAt;
        writeStore(store);
      }
      this.status.lastError = message;
      throw error;
    } finally {
      this.status = { ...this.status, running: false, lastFinishedAt: new Date().toISOString() };
    }
  }

  private async completeExecution(
    store: BtcManualTradeOverrideStore,
    state: BtcManualTradeOverrideState,
    entryRequest: BtcManualEntryRequest
  ): Promise<void> {
    let result: any;
    if (!this.entryHandler) {
      result = { tradePlaced: false, tradeError: 'No BTC manual entry handler is configured.' };
    } else {
      result = await this.entryHandler.executeManualBtcEntry(entryRequest);
    }
    if (result?.tradeDeferred) {
      state.status = 'ARMED';
      state.result = result;
      state.triggeredAt = undefined;
      state.signalCandleStartedAt = undefined;
      state.signalCandleClosedAt = undefined;
      state.signalClose = undefined;
      state.updatedAt = new Date().toISOString();
      store.updatedAt = state.updatedAt;
      writeStore(store);
      console.log('BTC manual entry/TP override deferred; plan remains armed:', {
        id: state.id,
        direction: state.direction,
        closeTrigger: state.closeTrigger,
        reason: result.tradeSkipped
      });
      return;
    }
    if (this.entryHandler?.sendManualBtcTriggerEmail) {
      const email = await this.entryHandler.sendManualBtcTriggerEmail(entryRequest, result);
      state.triggerEmailSentAt = email.sent ? new Date().toISOString() : undefined;
      state.triggerEmailError = email.error;
    } else {
      state.triggerEmailError = 'No manual BTC trigger email handler is configured.';
    }
    state.result = result;
    state.status = result?.tradePlaced ? 'TRIGGERED' : result?.tradeError ? 'ERROR' : 'SKIPPED';
    state.updatedAt = new Date().toISOString();
    store.updatedAt = state.updatedAt;
    writeStore(store);
    console.log('BTC manual entry/TP override completed:', state);
  }
}

export const btcManualTradeOverrideMonitor = new BtcManualTradeOverrideMonitor();
