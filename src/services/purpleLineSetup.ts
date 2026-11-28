import { spotEntryModeStatus } from './spotEntryMode';

export const PURPLE_LINE_SETUPS = {
  'BTC-USD': { asset: 'BTC', symbol: 'BTCUSDT', priceStepUsd: 100,
    modelVersion: 'binance-spot-liquidation-cohorts-v2.4', enabledEnv: 'BTC_ENTRY_MODE' },
  'ETH-USD': { asset: 'ETH', symbol: 'ETHUSDT', priceStepUsd: 5,
    modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED' },
  'INJ-USD': { asset: 'INJ', symbol: 'INJUSDT', priceStepUsd: 0.01,
    modelVersion: 'binance-spot-inj-liquidation-cohorts-v2.1', enabledEnv: 'INJ_ENTRY_MODE' },
  'WLD-USD': { asset: 'WLD', symbol: 'WLDUSDT', priceStepUsd: 0.005,
    modelVersion: 'binance-spot-wld-liquidation-cohorts-v2.1', enabledEnv: 'WLD_ENTRY_MODE' },
  'SOL-USD': { asset: 'SOL', symbol: 'SOLUSDT', priceStepUsd: 0.1,
    modelVersion: 'binance-spot-sol-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED' },
  'ZEC-USD': { asset: 'ZEC', symbol: 'ZECUSDT', priceStepUsd: 1,
    modelVersion: 'binance-spot-zec-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_ZEC_GAP_DAILY_ENTRY_ENABLED' }
} as const;

export type PurpleLineMarket = keyof typeof PURPLE_LINE_SETUPS;
const AUTOMATIC_SETUPS = { green: 'green-line', atr: 'atr-line', purple: 'purple-green-trail',
  'atr-purple': 'atr-purple-line', 'atr-purple-plus': 'atr-purple-plus-line' } as const;
export type SpotEntrySetup = typeof AUTOMATIC_SETUPS[keyof typeof AUTOMATIC_SETUPS];

export function spotEntrySetups(market: string): SpotEntrySetup[] {
  return (spotEntryModeStatus(market)?.modes || []).map(mode => AUTOMATIC_SETUPS[mode]);
}

export function spotEntrySetup(market: string): SpotEntrySetup {
  return spotEntrySetups(market)[0] ?? 'green-line';
}

export const spotSetupUsesPurple = (setup: SpotEntrySetup) => setup === 'purple-green-trail' ||
  setup === 'atr-purple-line' || setup === 'atr-purple-plus-line';
export const spotSetupUsesAtr = (setup: SpotEntrySetup) => setup === 'atr-line' || setup === 'atr-purple-line';
export const spotSetupLabel = (setup: SpotEntrySetup) => ({ 'green-line': 'Green', 'atr-line': 'ATR',
  'purple-green-trail': 'Purple', 'atr-purple-line': 'ATR Purple', 'atr-purple-plus-line': 'ATR Purple+' }[setup]);

export function purpleLineSetupForMarket(market: string) {
  return Object.prototype.hasOwnProperty.call(PURPLE_LINE_SETUPS, market)
    ? PURPLE_LINE_SETUPS[market as PurpleLineMarket] : undefined;
}

export function automaticSpotEntryEnabled(market: string): boolean {
  return spotEntrySetups(market).length > 0;
}

// Retain the existing configuration helper for compatibility.
export const purpleLineEntryEnabled = automaticSpotEntryEnabled;
