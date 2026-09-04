import axios from 'axios';
import { IndexerClient, IndexerConfig } from '@dydxprotocol/v4-client-js';
import { generateQueryPath } from '@dydxprotocol/v4-client-js/build/src/clients/helpers/request-helpers';

type ReadPolicy = {
  timeoutMs: number;
  maxAttempts: number;
  maxElapsedMs: number;
  backoffMs: number;
};

const DEFAULT_POLICY: Readonly<ReadPolicy> = {
  timeoutMs: 8_000,
  maxAttempts: 3,
  maxElapsedMs: 30_000,
  backoffMs: 500
};

const RETRY_CODES = new Set(['ECONNRESET', 'ECONNABORTED', 'ETIMEDOUT', 'EAI_AGAIN', 'EPIPE']);
const RETRY_STATUSES = new Set([408, 429, 500, 502, 503, 504]);

function retryAfterMs(value: unknown): number {
  if (typeof value !== 'string' && typeof value !== 'number') return 0;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1_000);
  const date = Date.parse(String(value));
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
}

export function createReadResilientIndexerClient(
  config: IndexerConfig,
  policy: Readonly<ReadPolicy> = DEFAULT_POLICY
): IndexerClient {
  const indexer = new IndexerClient(config);
  const http = axios.create({ timeout: policy.timeoutMs });

  // This SDK's RestClient stores apiTimeout but does not pass it to Axios.
  // Replace GET only on these instances; keep SDK routes/queries and all writes intact.
  for (const [moduleName, module] of [['account', indexer.account], ['markets', indexer.markets]] as const) {
    module.get = async (requestPath: string, params = {}) => {
      const url = `${module.host}${generateQueryPath(requestPath, params)}`;
      const startedAt = Date.now();
      const endpoint = requestPath.replace(/\/addresses\/[^/]+/, '/addresses/[redacted]');

      for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
        const controller = new AbortController();
        const timeoutMs = Math.min(policy.timeoutMs, policy.maxElapsedMs - (Date.now() - startedAt));
        const timer = setTimeout(() => controller.abort(), Math.max(1, timeoutMs));
        let failure: any;
        let timedOut = false;
        try {
          const response = await http.get(url, { timeout: Math.max(1, timeoutMs), signal: controller.signal });
          if (attempt > 1) {
            console.info('dYdX indexer read recovered.', {
              module: moduleName, endpoint, attempts: attempt, elapsedMs: Date.now() - startedAt
            });
          }
          return response.data;
        } catch (error) {
          failure = error;
          timedOut = controller.signal.aborted;
        } finally {
          clearTimeout(timer);
        }

        const status = failure?.response?.status;
        const retryable = status !== undefined
          ? RETRY_STATUSES.has(status)
          : timedOut || RETRY_CODES.has(failure?.code);
        const delayMs = Math.max(
          policy.backoffMs * 2 ** (attempt - 1) + Math.floor(Math.random() * policy.backoffMs / 2),
          retryAfterMs(failure?.response?.headers?.['retry-after'])
        );
        const remainingMs = policy.maxElapsedMs - (Date.now() - startedAt);
        if (!retryable || attempt === policy.maxAttempts || delayMs >= remainingMs) {
          console.warn('dYdX indexer read failed.', {
            module: moduleName, endpoint, attempts: attempt, status,
            code: timedOut ? 'ETIMEDOUT' : failure?.code,
            retryable, elapsedMs: Date.now() - startedAt
          });
          throw failure;
        }

        console.warn('dYdX indexer read retry scheduled.', {
          module: moduleName, endpoint, attempt, maxAttempts: policy.maxAttempts,
          status, code: timedOut ? 'ETIMEDOUT' : failure?.code, retryInMs: delayMs
        });
        await new Promise(resolve => setTimeout(resolve, delayMs));
        if (Date.now() - startedAt >= policy.maxElapsedMs) throw failure;
      }
      throw new Error('dYdX indexer read attempt budget exhausted.');
    };
  }

  return indexer;
}
