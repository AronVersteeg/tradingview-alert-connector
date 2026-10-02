import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';
import fs from 'fs';
import path from 'path';

type BinanceHostState = {
  tail: Promise<void>;
  nextRequestAt: number;
  cooldownUntil: number;
  requests: Array<{ at: number; weight: number }>;
  usedWeight: number;
  weightObservedAt: number;
  lastRateLimit?: {
    at: string; status?: number; code?: number; endpoint: string; symbol?: string;
    retryAt: string; localRequests5m: number; usedWeight1m?: number;
  };
};

const hostStates = new Map<string, BinanceHostState>();
const pendingReads = new Map<string, Promise<AxiosResponse<any>>>();
const recentReads = new Map<string, { expiresAt: number; response: AxiosResponse<any> }>();
const MINUTE_MS = 60_000;

function boundedInteger(value: string | undefined, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(maximum, Math.max(minimum, parsed));
}

function requestIntervalMs(): number {
  return boundedInteger(process.env.BINANCE_REQUEST_MIN_INTERVAL_MS, 750, 100, 5_000);
}

function cooldownFile(): string {
  const history = String(process.env.DECENTRALIZED_DOM_HISTORY_DIR || '').trim();
  return path.join(history ? path.dirname(history) : path.join(process.cwd(), 'data'), 'binance-rest-cooldowns.json');
}

function storedCooldown(host: string): number {
  try {
    const value = Number(JSON.parse(fs.readFileSync(cooldownFile(), 'utf8'))[host]);
    return Number.isFinite(value) && value > Date.now() ? value : 0;
  } catch { return 0; }
}

function persistCooldowns(): void {
  try {
    const file = cooldownFile();
    let stored: Record<string, number> = {};
    try { stored = JSON.parse(fs.readFileSync(file, 'utf8')); } catch { /* First cooldown. */ }
    for (const [host, state] of hostStates) stored[host] = state.cooldownUntil;
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const temporary = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify(stored));
    fs.renameSync(temporary, file);
  } catch (error) {
    console.warn('Could not persist Binance REST cooldowns:', error instanceof Error ? error.message : String(error));
  }
}

function stateFor(url: string): { host: string; state: BinanceHostState } {
  const host = new URL(url).host.toLowerCase();
  let state = hostStates.get(host);
  if (!state) {
    state = {
      tail: Promise.resolve(), nextRequestAt: 0, cooldownUntil: storedCooldown(host),
      requests: [], usedWeight: 0, weightObservedAt: 0
    };
    hostStates.set(host, state);
  }
  return { host, state };
}

export function binanceRetryAt(url: string): number {
  return stateFor(url).state.cooldownUntil;
}

export function binanceHttpStatus(): any {
  const now = Date.now();
  return ['fapi.binance.com', 'api.binance.com'].map((host) => {
    const state = stateFor(`https://${host}`).state;
    return {
      host,
      blocked: state.cooldownUntil > now,
      retryAt: state.cooldownUntil > now ? new Date(state.cooldownUntil).toISOString() : undefined,
      usedWeight1m: state.weightObservedAt ? state.usedWeight : undefined,
      weightObservedAt: state.weightObservedAt ? new Date(state.weightObservedAt).toISOString() : undefined,
      localRequests5m: state.requests.filter((request) => request.at > now - 5 * MINUTE_MS).length,
      localWeight1m: state.requests.filter((request) => request.at > now - MINUTE_MS).reduce((sum, request) => sum + request.weight, 0),
      recovering: state.cooldownUntil > 0 && now >= state.cooldownUntil && now < state.cooldownUntil + MINUTE_MS,
      lastRateLimit: state.lastRateLimit
    };
  });
}

export class BinanceCooldownError extends Error {
  constructor(public readonly host: string, public readonly retryAt: number) {
    super(`Binance ${host} request cooldown active until ${new Date(retryAt).toISOString()}.`);
    this.name = 'BinanceCooldownError';
  }
}

export function binanceCooldownHttpResponse(error: unknown): {
  status: number; retryAfter: string; body: { ok: false; unavailable: true; error: string; retryAt: string };
} | undefined {
  if (!(error instanceof BinanceCooldownError)) return undefined;
  return {
    status: 503,
    retryAfter: String(Math.max(1, Math.ceil((error.retryAt - Date.now()) / 1000))),
    body: { ok: false, unavailable: true, error: error.message, retryAt: new Date(error.retryAt).toISOString() }
  };
}

export function binanceRequestWeight(url: string, config?: AxiosRequestConfig): number {
  const endpoint = new URL(url).pathname;
  const limit = Number(config?.params?.limit || 500);
  if (endpoint === '/fapi/v1/klines') return limit < 100 ? 1 : limit < 500 ? 2 : limit <= 1000 ? 5 : 10;
  if (endpoint === '/fapi/v1/depth') return limit <= 50 ? 2 : limit <= 100 ? 5 : limit <= 500 ? 10 : 20;
  if (endpoint === '/api/v3/klines') return 2;
  return 1;
}

function observeWeight(state: BinanceHostState, headers: any): void {
  const weight = Number(headers?.['x-mbx-used-weight-1m']);
  if (Number.isFinite(weight) && weight >= 0) {
    state.usedWeight = weight;
    state.weightObservedAt = Date.now();
  }
}

function readKey(url: string, config?: AxiosRequestConfig): string | undefined {
  // Only public candle/OI reads may be reused. Never cache live orderbooks.
  if (config?.headers || config?.auth || config?.paramsSerializer || config?.transformResponse) return undefined;
  const parsed = new URL(url);
  if (!['/fapi/v1/klines', '/api/v3/klines', '/futures/data/openInterestHist'].includes(parsed.pathname)) return undefined;
  const params = Object.entries(config?.params || {}).sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify([url, params, config?.responseType]);
}

function responseData(error: any): any {
  return error?.response?.data;
}

export function isBinanceRateLimitError(error: any): boolean {
  const status = Number(error?.response?.status);
  const code = Number(responseData(error)?.code);
  const message = String(responseData(error)?.msg || error?.message || '').toLowerCase();
  return status === 418 || status === 429 || code === -1003 || message.includes('too many requests');
}

export function binanceRateLimitUntil(error: any, nowMs = Date.now()): number | undefined {
  if (!isBinanceRateLimitError(error)) return undefined;
  const candidates: number[] = [];
  const retryAfter = error?.response?.headers?.['retry-after'];
  const retrySeconds = Number(retryAfter);
  if (Number.isFinite(retrySeconds) && retrySeconds > 0) {
    candidates.push(nowMs + retrySeconds * 1_000);
  } else if (retryAfter) {
    const retryDate = Date.parse(String(retryAfter));
    if (Number.isFinite(retryDate)) candidates.push(retryDate);
  }

  const message = String(responseData(error)?.msg || error?.message || '');
  const bannedUntil = message.match(/banned until\s+(\d{10,})/i);
  if (bannedUntil) {
    const parsed = Number(bannedUntil[1]);
    if (Number.isFinite(parsed)) candidates.push(parsed);
  }

  return candidates.length ? Math.max(...candidates) : nowMs + 60_000;
}

function cooldownError(host: string, cooldownUntil: number): Error {
  return new BinanceCooldownError(host, cooldownUntil);
}

function compactRequestError(error: any, host: string): Error {
  const status = Number(error?.response?.status);
  const code = responseData(error)?.code;
  const detail = responseData(error)?.msg || error?.message || String(error);
  const statusText = Number.isFinite(status) ? ` HTTP ${status}` : '';
  const codeText = code !== undefined ? ` code ${code}` : '';
  return new Error(`Binance ${host}${statusText}${codeText}: ${detail}`);
}

export async function binanceGet<T = any>(
  url: string,
  config?: AxiosRequestConfig
): Promise<AxiosResponse<T>> {
  const { host, state } = stateFor(url);
  // Do not return cached data as fresh while the feed is known to be blocked.
  if (state.cooldownUntil > Date.now()) throw cooldownError(host, state.cooldownUntil);
  const key = readKey(url, config);
  if (key) {
    const cached = recentReads.get(key);
    if (cached && cached.expiresAt > Date.now()) return cached.response;
    const pending = pendingReads.get(key);
    if (pending) return pending;
  }
  const run = async (): Promise<AxiosResponse<T>> => {
    const weight = binanceRequestWeight(url, config);
    const budget = boundedInteger(process.env.BINANCE_REQUEST_WEIGHT_BUDGET_PER_MINUTE, 1200, 50, 2000);
    // Binance headers include other traffic on the same outbound IP.
    while (true) {
      const now = Date.now();
      if (state.cooldownUntil > now) throw cooldownError(host, state.cooldownUntil);
      state.requests = state.requests.filter((request) => request.at > now - 5 * MINUTE_MS);
      const minute = state.requests.filter((request) => request.at > now - MINUTE_MS);
      const localWeight = minute.reduce((sum, request) => sum + request.weight, 0);
      let availableAt = state.nextRequestAt;
      // Resume just after expiry, then drain queued collectors slowly for one minute.
      if (state.cooldownUntil > 0) availableAt = Math.max(availableAt, state.cooldownUntil + 2_000);
      if (localWeight + weight > budget && minute.length) availableAt = Math.max(availableAt, minute[0].at + MINUTE_MS + 1000);
      if (state.requests.length >= 500) availableAt = Math.max(availableAt, state.requests[0].at + 5 * MINUTE_MS + 1000);
      if (state.usedWeight + weight >= budget && Math.floor(state.weightObservedAt / MINUTE_MS) === Math.floor(now / MINUTE_MS)) {
        availableAt = Math.max(availableAt, (Math.floor(now / MINUTE_MS) + 1) * MINUTE_MS + 1000);
      }
      const waitMs = availableAt - now;
      if (waitMs <= 0) break;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
    const recovering = state.cooldownUntil > 0 && Date.now() < state.cooldownUntil + MINUTE_MS;
    state.nextRequestAt = Date.now() + Math.max(requestIntervalMs(), recovering ? 3_000 : 0);
    state.requests.push({ at: Date.now(), weight });

    try {
      const response = await axios.get<T>(url, config);
      observeWeight(state, response.headers);
      return response;
    } catch (error) {
      observeWeight(state, (error as any)?.response?.headers);
      const cooldownUntil = binanceRateLimitUntil(error);
      if (cooldownUntil && cooldownUntil > state.cooldownUntil) {
        state.cooldownUntil = cooldownUntil;
        const status = Number((error as any)?.response?.status);
        const code = Number(responseData(error)?.code);
        state.lastRateLimit = {
          at: new Date().toISOString(),
          status: Number.isFinite(status) ? status : undefined,
          code: Number.isFinite(code) ? code : undefined,
          endpoint: new URL(url).pathname,
          symbol: config?.params?.symbol,
          retryAt: new Date(cooldownUntil).toISOString(),
          localRequests5m: state.requests.length,
          usedWeight1m: state.weightObservedAt ? state.usedWeight : undefined
        };
        persistCooldowns();
        console.warn('Binance REST cooldown activated:', {
          host,
          endpoint: new URL(url).pathname,
          symbol: config?.params?.symbol,
          usedWeight1m: state.weightObservedAt ? state.usedWeight : undefined,
          weightObservedAt: state.weightObservedAt ? new Date(state.weightObservedAt).toISOString() : undefined,
          localRequests5m: state.requests.length,
          cooldownUntil: new Date(cooldownUntil).toISOString(),
          reason: responseData(error)?.msg || (error instanceof Error ? error.message : String(error))
        });
      }
      if (cooldownUntil) throw cooldownError(host, state.cooldownUntil);
      throw compactRequestError(error, host);
    }
  };

  const request = state.tail.then(run, run);
  state.tail = request.then(() => undefined, () => undefined);
  if (key) pendingReads.set(key, request);
  try {
    const response = await request;
    if (key) {
      for (const [cachedKey, cached] of recentReads) {
        if (cached.expiresAt <= Date.now()) recentReads.delete(cachedKey);
      }
      if (recentReads.size >= 64) recentReads.delete(recentReads.keys().next().value!);
      recentReads.set(key, { expiresAt: Date.now() + 2000, response });
    }
    return response;
  } finally {
    if (key) pendingReads.delete(key);
  }
}
