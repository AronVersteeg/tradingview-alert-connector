import { btcEntryModeStatus } from './btcEntryMode';
import { parseEntryModes } from './entryModeSelection';

const PAIR_MODES = {
  'ETH-USD': { env: 'ETH_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED' },
  'INJ-USD': { env: 'INJ_ENTRY_MODE', legacyEnv: undefined },
  'WLD-USD': { env: 'WLD_ENTRY_MODE', legacyEnv: undefined },
  'SOL-USD': { env: 'SOL_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED' },
  'ZEC-USD': { env: 'ZEC_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_ZEC_GAP_DAILY_ENTRY_ENABLED' }
} as const;

export function spotEntryModeStatus(market: string) {
  if (market === 'BTC-USD') return { ...btcEntryModeStatus(), explicit: true, legacyFallback: false };
  if (!Object.prototype.hasOwnProperty.call(PAIR_MODES, market)) return undefined;
  const config = PAIR_MODES[market as keyof typeof PAIR_MODES];
  const value = String(process.env[config.env] || '').trim().toLowerCase();
  const explicit = value !== '';
  const legacyGreen = config.legacyEnv !== undefined && ['1', 'true', 'yes', 'on'].includes(String(process.env[config.legacyEnv] || '').trim().toLowerCase());
  const fallback = config.legacyEnv === undefined ? 'off' : 'manual';
  return { env: config.env, ...parseEntryModes(explicit ? value : legacyGreen ? 'green' : fallback),
    explicit, legacyFallback: !explicit && config.legacyEnv !== undefined, legacyEnv: config.legacyEnv,
    default: fallback, positionManagement: 'existing owner continues; switching never changes a live position' };
}

export function spotManualEntryEnabled(market: string): boolean {
  const status = spotEntryModeStatus(market);
  return !status || status.mode === 'manual';
}

export function spotEntryModeExplicit(market: string): boolean {
  // Without a legacy fallback, an unset selector still owns entries in Off mode.
  return spotEntryModeStatus(market)?.legacyFallback === false;
}
