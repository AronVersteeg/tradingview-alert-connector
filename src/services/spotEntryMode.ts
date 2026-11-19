import { BtcEntryMode, btcEntryModeStatus } from './btcEntryMode';

const PAIR_MODES = {
  'ETH-USD': { env: 'ETH_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED' },
  'SOL-USD': { env: 'SOL_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED' },
  'ZEC-USD': { env: 'ZEC_ENTRY_MODE', legacyEnv: 'OPEN_LIQUIDITY_V2_ZEC_GAP_DAILY_ENTRY_ENABLED' }
} as const;

export function spotEntryModeStatus(market: string) {
  if (market === 'BTC-USD') return { ...btcEntryModeStatus(), explicit: true, legacyFallback: false };
  if (!Object.prototype.hasOwnProperty.call(PAIR_MODES, market)) return undefined;
  const config = PAIR_MODES[market as keyof typeof PAIR_MODES];
  const value = String(process.env[config.env] || '').trim().toLowerCase();
  const explicit = value !== '';
  const legacyGreen = ['1', 'true', 'yes', 'on'].includes(String(process.env[config.legacyEnv] || '').trim().toLowerCase());
  const valid = !explicit || ['manual', 'green', 'purple', 'purple-trail', 'off'].includes(value);
  const mode: BtcEntryMode = !explicit ? legacyGreen ? 'green' : 'manual'
    : value === 'purple' || value === 'purple-trail' ? 'purple'
    : value === 'manual' || value === 'green' ? value : 'off';
  return { env: config.env, mode, valid, explicit, legacyFallback: !explicit, legacyEnv: config.legacyEnv,
    default: 'manual', positionManagement: 'existing owner continues; switching never changes a live position' };
}

export function spotManualEntryEnabled(market: string): boolean {
  const status = spotEntryModeStatus(market);
  return !status || status.mode === 'manual';
}

export function spotEntryModeExplicit(market: string): boolean {
  return spotEntryModeStatus(market)?.explicit === true;
}
