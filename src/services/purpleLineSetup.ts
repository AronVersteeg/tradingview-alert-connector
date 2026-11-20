import { spotEntryModeStatus } from './spotEntryMode';

export const PURPLE_LINE_SETUPS = {
  'BTC-USD': { asset: 'BTC', symbol: 'BTCUSDT', priceStepUsd: 100,
    modelVersion: 'binance-spot-liquidation-cohorts-v2.4', enabledEnv: 'BTC_ENTRY_MODE' },
  'ETH-USD': { asset: 'ETH', symbol: 'ETHUSDT', priceStepUsd: 5,
    modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED' },
  'SOL-USD': { asset: 'SOL', symbol: 'SOLUSDT', priceStepUsd: 0.1,
    modelVersion: 'binance-spot-sol-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED' },
  'ZEC-USD': { asset: 'ZEC', symbol: 'ZECUSDT', priceStepUsd: 1,
    modelVersion: 'binance-spot-zec-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_ZEC_GAP_DAILY_ENTRY_ENABLED' }
} as const;

export type PurpleLineMarket = keyof typeof PURPLE_LINE_SETUPS;
export type SpotEntrySetup = 'green-line' | 'atr-line' | 'purple-green-trail';

export function spotEntrySetup(market: string): SpotEntrySetup {
  const mode = spotEntryModeStatus(market)?.mode;
  return mode === 'purple' ? 'purple-green-trail' : mode === 'atr' ? 'atr-line' : 'green-line';
}

export function purpleLineSetupForMarket(market: string) {
  return Object.prototype.hasOwnProperty.call(PURPLE_LINE_SETUPS, market)
    ? PURPLE_LINE_SETUPS[market as PurpleLineMarket] : undefined;
}

export function automaticSpotEntryEnabled(market: string): boolean {
  const mode = spotEntryModeStatus(market)?.mode;
  return mode === 'green' || mode === 'purple' || mode === 'atr';
}

// Retain the existing configuration helper for compatibility.
export const purpleLineEntryEnabled = automaticSpotEntryEnabled;
