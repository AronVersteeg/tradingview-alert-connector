import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';

type BinanceHostState = {
  tail: Promise<void>;
  nextRequestAt: number;
  cooldownUntil: number;
};

const hostStates = new Map<string, BinanceHostState>();

function boundedInteger(value: string | undefined, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(maximum, Math.max(minimum, parsed));
}

function requestIntervalMs(): number {
  return boundedInteger(process.env.BINANCE_REQUEST_MIN_INTERVAL_MS, 350, 100, 5_000);
}

function stateFor(url: string): { host: string; state: BinanceHostState } {
  const host = new URL(url).host.toLowerCase();
  let state = hostStates.get(host);
  if (!state) {
    state = { tail: Promise.resolve(), nextRequestAt: 0, cooldownUntil: 0 };
    hostStates.set(host, state);
  }
  return { host, state };
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
  return new Error(`Binance ${host} request cooldown active until ${new Date(cooldownUntil).toISOString()}.`);
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
  const run = async (): Promise<AxiosResponse<T>> => {
    const now = Date.now();
    if (state.cooldownUntil > now) throw cooldownError(host, state.cooldownUntil);

    const waitMs = Math.max(0, state.nextRequestAt - now);
    if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
    state.nextRequestAt = Date.now() + requestIntervalMs();

    try {
      return await axios.get<T>(url, config);
    } catch (error) {
      const cooldownUntil = binanceRateLimitUntil(error);
      if (cooldownUntil && cooldownUntil > state.cooldownUntil) {
        state.cooldownUntil = cooldownUntil;
        console.warn('Binance REST cooldown activated:', {
          host,
          cooldownUntil: new Date(cooldownUntil).toISOString(),
          reason: responseData(error)?.msg || (error instanceof Error ? error.message : String(error))
        });
      }
      throw compactRequestError(error, host);
    }
  };

  const request = state.tail.then(run, run);
  state.tail = request.then(() => undefined, () => undefined);
  return request;
}
