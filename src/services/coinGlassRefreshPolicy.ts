export type CoinGlassRefreshState = {
  now: number;
  fetchedAt?: number;
  lastAttemptAt?: number;
  consecutiveFailures: number;
  pollMs: number;
  failureBackoffBaseMs: number;
  failureBackoffMaxMs: number;
};

export function coinGlassFailureBackoffMs(
  consecutiveFailures: number,
  baseMs: number,
  maxMs: number
): number {
  if (!(consecutiveFailures > 0)) return 0;
  const exponent = Math.min(10, Math.max(0, Math.floor(consecutiveFailures) - 1));
  return Math.min(maxMs, baseMs * 2 ** exponent);
}

export function coinGlassRefreshWaitMs(state: CoinGlassRefreshState): number {
  if (state.consecutiveFailures > 0 && state.lastAttemptAt !== undefined) {
    const backoffMs = coinGlassFailureBackoffMs(
      state.consecutiveFailures,
      state.failureBackoffBaseMs,
      state.failureBackoffMaxMs
    );
    return Math.max(0, backoffMs - Math.max(0, state.now - state.lastAttemptAt));
  }

  if (state.fetchedAt !== undefined) {
    return Math.max(0, state.pollMs - Math.max(0, state.now - state.fetchedAt));
  }

  return 0;
}
