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
  market: string;
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
  executeManualEntry?: (request: BtcManualEntryRequest) => Promise<any>;
  executeManualBtcEntry?: (request: BtcManualEntryRequest) => Promise<any>;
  sendManualTriggerEmail?: (request: BtcManualEntryRequest, result: any) => Promise<{
    sent: boolean;
    error?: string;
  }>;
  sendManualBtcTriggerEmail?: (request: BtcManualEntryRequest, result: any) => Promise<{
    sent: boolean;
    error?: string;
  }>;
};

export type BtcManualTradeOverrideState = {
  version: 1;
  id: string;
  market: string;
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
  market: string;
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

export type ManualTradeOverrideConfig = {
  market: string;
  asset: 'BTC' | 'ETH' | 'INJ' | 'SOL' | 'ZEC' | 'GOLD' | 'SILVER';
  symbol: string;
  stateFileEnv: string;
  stateFileName: string;
};

const MANUAL_OVERRIDE_CONFIGS: ManualTradeOverrideConfig[] = [
  { market: 'BTC-USD', asset: 'BTC', symbol: 'BTCUSDT', stateFileEnv: 'BTC_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'btc-manual-trade-override.json' },
  { market: 'ETH-USD', asset: 'ETH', symbol: 'ETHUSDT', stateFileEnv: 'ETH_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'eth-manual-trade-override.json' },
  { market: 'INJ-USD', asset: 'INJ', symbol: 'INJUSDT', stateFileEnv: 'INJ_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'inj-manual-trade-override.json' },
  { market: 'SOL-USD', asset: 'SOL', symbol: 'SOLUSDT', stateFileEnv: 'SOL_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'sol-manual-trade-override.json' },
  { market: 'ZEC-USD', asset: 'ZEC', symbol: 'ZECUSDT', stateFileEnv: 'ZEC_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'zec-manual-trade-override.json' },
  { market: 'PAXG-USD', asset: 'GOLD', symbol: 'XAUUSDT', stateFileEnv: 'GOLD_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'gold-manual-trade-override.json' },
  { market: 'XAG-USD', asset: 'SILVER', symbol: 'XAGUSDT', stateFileEnv: 'SILVER_MANUAL_TRADE_OVERRIDE_FILE', stateFileName: 'silver-manual-trade-override.json' }
];

const BTC_MANUAL_OVERRIDE_CONFIG = MANUAL_OVERRIDE_CONFIGS[0];

function normalizedMarket(value: unknown): string {
  return String(value || '').replace(/_/g, '-').toUpperCase();
}

export function manualTradeOverrideConfigForMarket(market: unknown): ManualTradeOverrideConfig | undefined {
  const normalized = normalizedMarket(market || 'BTC-USD');
  return MANUAL_OVERRIDE_CONFIGS.find((config) => config.market === normalized);
}

export function btcManualTriggerEmailSubject(request: BtcManualEntryRequest): string {
  const asset = manualTradeOverrideConfigForMarket(request.market)?.asset || request.market.replace('-USD', '');
  return `${asset} MANUAL ${request.direction.toUpperCase()} TRIGGERED | ${btcManualTriggerTimestampNl(request.signalCandleStartedAt)}`;
}

function boolValue(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || String(value).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export function btcManualTradeOverrideEnabled(): boolean {
  return boolValue(process.env.MANUAL_ENTRY_TP_OVERRIDE_ENABLED, false);
}

function stateFile(config = BTC_MANUAL_OVERRIDE_CONFIG): string {
  const configured = String(process.env[config.stateFileEnv] || '').trim();
  if (configured) return configured;
  const btcConfigured = String(process.env.BTC_MANUAL_TRADE_OVERRIDE_FILE || '').trim();
  if (config.market !== 'BTC-USD' && btcConfigured) {
    return path.join(path.dirname(btcConfigured), config.stateFileName);
  }
  return path.join(process.cwd(), 'data', config.stateFileName);
}

function emptyStore(config = BTC_MANUAL_OVERRIDE_CONFIG): BtcManualTradeOverrideStore {
  return {
    version: 3,
    market: config.market,
    overrides: [],
    updatedAt: new Date().toISOString()
  };
}

function legacyOverrideId(override: any, index: number, config = BTC_MANUAL_OVERRIDE_CONFIG): string {
  const direction = override?.direction === 'short' ? 'short' : 'long';
  const armedAt = Date.parse(String(override?.armedAt || ''));
  const timestamp = Number.isFinite(armedAt) ? armedAt : index;
  return `${config.asset.toLowerCase()}-manual-${direction}-${timestamp}-${Number(override?.closeTrigger) || 0}`;
}

function normalizeStoredOverrides(overrides: any[], config = BTC_MANUAL_OVERRIDE_CONFIG): BtcManualTradeOverrideState[] {
  const ids = new Set<string>();
  return overrides
    .filter((override: any) => override?.direction === 'long' || override?.direction === 'short')
    .map((override: any, index) => {
      let id = String(override?.id || '').trim();
      if (!id || id === 'long' || id === 'short' || ids.has(id)) id = legacyOverrideId(override, index, config);
      while (ids.has(id)) id = `${id}-${index}`;
      ids.add(id);
      const normalized = { ...override, id } as BtcManualTradeOverrideState;
      if (normalized.status === 'ARMED' || normalized.status === 'EXECUTING') {
        delete normalized.expiresAt;
      }
      return normalized;
    });
}

export function readManualTradeOverrideStore(
  config = BTC_MANUAL_OVERRIDE_CONFIG
): BtcManualTradeOverrideStore {
  try {
    const parsed = JSON.parse(fs.readFileSync(stateFile(config), 'utf8'));
    if (parsed?.version === 3 && normalizedMarket(parsed?.market) === config.market && Array.isArray(parsed.overrides)) {
      return {
        ...parsed,
        market: config.market,
        overrides: normalizeStoredOverrides(parsed.overrides, config)
      } as BtcManualTradeOverrideStore;
    }
    if (parsed?.version === 2 && normalizedMarket(parsed?.market) === config.market && Array.isArray(parsed.overrides)) {
      return {
        ...parsed,
        version: 3,
        market: config.market,
        overrides: normalizeStoredOverrides(parsed.overrides, config)
      } as BtcManualTradeOverrideStore;
    }
    if (
      parsed?.version === 1 &&
      normalizedMarket(parsed?.market) === config.market &&
      (parsed?.direction === 'long' || parsed?.direction === 'short')
    ) {
      return {
        version: 3,
        market: config.market,
        overrides: normalizeStoredOverrides([parsed], config),
        updatedAt: parsed.updatedAt || new Date().toISOString()
      };
    }
    return emptyStore(config);
  } catch {
    return emptyStore(config);
  }
}

export function readBtcManualTradeOverrideStore(): BtcManualTradeOverrideStore {
  return readManualTradeOverrideStore(BTC_MANUAL_OVERRIDE_CONFIG);
}

function writeStore(store: BtcManualTradeOverrideStore, config = BTC_MANUAL_OVERRIDE_CONFIG): void {
  const target = stateFile(config);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const temporary = `${target}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(store, null, 2));
  fs.renameSync(temporary, target);
}

export function btcManualTradeOverrideIsArmed(): boolean {
  return manualTradeOverrideIsArmed('BTC-USD');
}

export function manualTradeOverrideIsArmed(market: string): boolean {
  const config = manualTradeOverrideConfigForMarket(market);
  if (!config) return false;
  return readManualTradeOverrideStore(config).overrides.some((state) => (
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
  nowMs = Date.now(),
  config = BTC_MANUAL_OVERRIDE_CONFIG
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
    id: `${config.asset.toLowerCase()}-manual-${direction}-${nowMs}-${closeTrigger}`,
    market: config.market,
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
  override: BtcManualTradeOverrideState,
  config = BTC_MANUAL_OVERRIDE_CONFIG
): BtcManualTradeOverrideStore {
  const active = store.overrides.filter((candidate) => (
    candidate.status === 'ARMED' || candidate.status === 'EXECUTING'
  ));
  const executing = active.find((candidate) => candidate.status === 'EXECUTING');
  if (executing) {
    throw new Error(`${config.asset} manual plan ${executing.id} is executing; add the new plan after it completes.`);
  }
  if (active.length >= MAX_ACTIVE_OVERRIDES) {
    throw new Error(`At most ${MAX_ACTIVE_OVERRIDES} active ${config.asset} manual plans are supported.`);
  }
  const duplicate = active.find((candidate) => (
    candidate.direction === override.direction && Number(candidate.closeTrigger) === Number(override.closeTrigger)
  ));
  if (duplicate) {
    throw new Error(`An armed ${override.direction} ${config.asset} plan already uses close trigger ${override.closeTrigger}.`);
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
    market: config.market,
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
    market: state.market,
    direction: state.direction,
    signature: String(state.result?.signature || (
      `${state.market.toLowerCase()}-manual-override|${state.direction}|${signalStartedAtMs}|${state.closeTrigger}|${state.armedAt}`
    )),
    signalCandleStartedAt: new Date(signalStartedAtMs).toISOString(),
    signalCandleClosedAt: new Date(signalClosedAtMs).toISOString(),
    signalClose: Number(state.signalClose),
    closeTrigger: Number(state.closeTrigger),
    takeProfits: state.takeProfits || [],
    recovery: true
  };
}

async function fetchHourlyCandles(
  config = BTC_MANUAL_OVERRIDE_CONFIG,
  nowMs = Date.now()
): Promise<BtcManualHourlyCandle[]> {
  const response = await binanceGet<unknown[]>(BINANCE_FUTURES_KLINES_URL, {
    params: { symbol: config.symbol, interval: '1h', limit: 12 },
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
  if (!candles.length) throw new Error(`Binance ${config.symbol} returned no closed 1H candles.`);
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
    throw new Error('Position is too small to allocate every requested manual TP at the dYdX step size.');
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

  constructor(private readonly config: ManualTradeOverrideConfig = BTC_MANUAL_OVERRIDE_CONFIG) {}

  configureEntryHandler(handler: BtcManualEntryHandler): void {
    this.entryHandler = handler;
  }

  start(initialDelayMs = 30_000): void {
    if (this.initialTimer || this.nextTimer) return;
    this.initialTimer = setTimeout(() => {
      this.initialTimer = undefined;
      this.runAndReschedule();
    }, initialDelayMs);
    console.log(`${this.config.asset} manual entry/TP override monitor scheduled:`, {
      enabled: btcManualTradeOverrideEnabled(),
      initialDelayMs,
      source: `Binance Futures ${this.config.symbol} 1H closed candles`,
      stateFile: stateFile(this.config)
    });
  }

  getStatus(): any {
    const store = readManualTradeOverrideStore(this.config);
    const activeOverride = store.overrides.find((override) => (
      override.status === 'ARMED' || override.status === 'EXECUTING'
    ));
    return {
      enabled: btcManualTradeOverrideEnabled(),
      configured: Boolean(this.entryHandler),
      market: this.config.market,
      asset: this.config.asset,
      symbol: this.config.symbol,
      ...this.status,
      overrides: store.overrides,
      override: activeOverride || store.overrides[0]
    };
  }

  arm(input: any): BtcManualTradeOverrideState {
    if (!btcManualTradeOverrideEnabled()) {
      throw new Error('MANUAL_ENTRY_TP_OVERRIDE_ENABLED is false.');
    }
    const state = normalizeBtcManualTradeOverrideRequest(input, Date.now(), this.config);
    const store = upsertBtcManualTradeOverride(readManualTradeOverrideStore(this.config), state, this.config);
    writeStore(store, this.config);
    console.log(`${this.config.asset} manual entry/TP override armed:`, state);
    return state;
  }

  update(idInput: unknown, input: any): BtcManualTradeOverrideState {
    if (!btcManualTradeOverrideEnabled()) {
      throw new Error('MANUAL_ENTRY_TP_OVERRIDE_ENABLED is false.');
    }
    const id = String(idInput || '').trim();
    if (!id) throw new Error(`Plan id is required when editing a ${this.config.asset} manual override.`);
    const store = readManualTradeOverrideStore(this.config);
    const previous = store.overrides.find((override) => override.id === id);
    if (!previous) throw new Error(`No ${this.config.asset} manual plan ${id} exists.`);
    if (previous.status === 'EXECUTING') {
      throw new Error(`${this.config.asset} manual plan ${previous.id} is already executing and can no longer be edited.`);
    }
    if (previous.status !== 'ARMED') {
      throw new Error(`${this.config.asset} manual plan ${previous.id} is ${previous.status.toLowerCase()} and is not armed.`);
    }
    const replacement = {
      ...normalizeBtcManualTradeOverrideRequest(input, Date.now(), this.config),
      id: previous.id
    };
    const withoutPrevious: BtcManualTradeOverrideStore = {
      ...store,
      overrides: store.overrides.filter((override) => override.id !== previous.id)
    };
    const updatedStore = upsertBtcManualTradeOverride(withoutPrevious, replacement, this.config);
    writeStore(updatedStore, this.config);
    console.log(`${this.config.asset} manual entry/TP override updated:`, {
      previous,
      replacement
    });
    return replacement;
  }

  cancel(idOrDirectionInput?: unknown): BtcManualTradeOverrideState {
    const idOrDirection = String(idOrDirectionInput || '').trim();
    if (!idOrDirection) throw new Error(`Plan id is required when cancelling a ${this.config.asset} manual override.`);
    const store = readManualTradeOverrideStore(this.config);
    const armedDirectionMatches = store.overrides.filter((override) => (
      override.direction === idOrDirection && override.status === 'ARMED'
    ));
    if (armedDirectionMatches.length > 1) {
      throw new Error(`Multiple ${idOrDirection} ${this.config.asset} plans exist; cancel by plan id.`);
    }
    const previous = store.overrides.find((override) => override.id === idOrDirection)
      || armedDirectionMatches[0];
    if (!previous) throw new Error(`No ${this.config.asset} manual plan ${idOrDirection} exists.`);
    if (previous.status === 'EXECUTING') {
      throw new Error(`${this.config.asset} manual plan ${previous.id} is already executing and can no longer be cancelled.`);
    }
    if (previous.status !== 'ARMED') {
      throw new Error(`${this.config.asset} manual plan ${previous.id} is ${previous.status.toLowerCase()} and is not armed.`);
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
    }, this.config);
    console.log(`${this.config.asset} manual entry/TP override cancelled:`, state);
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
      .catch((error) => console.error(`${this.config.asset} manual entry/TP override check failed:`, error))
      .finally(() => this.scheduleNextRun());
  }

  private scheduleNextRun(): void {
    const now = Date.now();
    const marketOffsetMs = Math.max(0, MANUAL_OVERRIDE_CONFIGS.findIndex((config) => (
      config.market === this.config.market
    ))) * 5_000;
    const nextRunAt = (Math.floor(now / HOUR_MS) + 1) * HOUR_MS + CLOSE_BUFFER_MS + marketOffsetMs;
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
      let store = readManualTradeOverrideStore(this.config);
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
        writeStore(store, this.config);
        console.warn(`Retrying a flat fail-safe ${this.config.asset} manual entry once with fresh market depth:`, {
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
        writeStore({ ...store, updatedAt: nowIso }, this.config);
        return;
      }
      const candles = await fetchHourlyCandles(this.config, nowMs);
      const latest = candles[candles.length - 1];
      for (const override of armed) {
        override.lastEvaluatedCandleStartedAt = latest ? new Date(latest.openTime).toISOString() : undefined;
        override.updatedAt = nowIso;
      }
      const matched = armed
        .map((override) => ({ override, candle: matchingManualOverrideCandle(override, candles, nowMs) }))
        .find((candidate) => candidate.candle);
      if (!matched?.candle) {
        writeStore({ ...store, updatedAt: nowIso }, this.config);
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
      writeStore(store, this.config);

      const signature = `${this.config.asset.toLowerCase()}-manual-override|${state.direction}|${candle.openTime}|${state.closeTrigger}|${state.armedAt}`;
      const entryRequest: BtcManualEntryRequest = {
        market: this.config.market,
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
      const store = readManualTradeOverrideStore(this.config);
      const message = error instanceof Error ? error.message : String(error);
      const executing = store.overrides.find((state) => state.status === 'EXECUTING');
      if (executing) {
        executing.status = 'ERROR';
        executing.result = { tradePlaced: false, tradeError: message };
        executing.updatedAt = new Date().toISOString();
        store.updatedAt = executing.updatedAt;
        writeStore(store, this.config);
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
      result = { tradePlaced: false, tradeError: `No ${this.config.asset} manual entry handler is configured.` };
    } else {
      const execute = this.entryHandler.executeManualEntry || this.entryHandler.executeManualBtcEntry;
      result = execute
        ? await execute.call(this.entryHandler, entryRequest)
        : { tradePlaced: false, tradeError: `No ${this.config.asset} manual entry method is configured.` };
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
      writeStore(store, this.config);
      console.log(`${this.config.asset} manual entry/TP override deferred; plan remains armed:`, {
        id: state.id,
        direction: state.direction,
        closeTrigger: state.closeTrigger,
        reason: result.tradeSkipped
      });
      return;
    }
    const sendEmail = this.entryHandler?.sendManualTriggerEmail || this.entryHandler?.sendManualBtcTriggerEmail;
    if (sendEmail) {
      const email = await sendEmail.call(this.entryHandler, entryRequest, result);
      state.triggerEmailSentAt = email.sent ? new Date().toISOString() : undefined;
      state.triggerEmailError = email.error;
    } else {
      state.triggerEmailError = `No manual ${this.config.asset} trigger email handler is configured.`;
    }
    state.result = result;
    state.status = result?.tradePlaced ? 'TRIGGERED' : result?.tradeError ? 'ERROR' : 'SKIPPED';
    state.updatedAt = new Date().toISOString();
    store.updatedAt = state.updatedAt;
    writeStore(store, this.config);
    console.log(`${this.config.asset} manual entry/TP override completed:`, state);
  }
}

export const manualTradeOverrideMonitors = new Map(
  MANUAL_OVERRIDE_CONFIGS.map((config) => [
    config.market,
    new BtcManualTradeOverrideMonitor(config)
  ])
);

export function manualTradeOverrideMonitorForMarket(market: unknown): BtcManualTradeOverrideMonitor | undefined {
  return manualTradeOverrideMonitors.get(normalizedMarket(market || 'BTC-USD'));
}

export const btcManualTradeOverrideMonitor = manualTradeOverrideMonitors.get('BTC-USD')!;
