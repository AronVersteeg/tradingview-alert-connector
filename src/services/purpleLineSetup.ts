export const PURPLE_LINE_SETUPS = {
  'ETH-USD': { asset: 'ETH', symbol: 'ETHUSDT', priceStepUsd: 5,
    modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_ETH_GAP_DAILY_ENTRY_ENABLED' },
  'SOL-USD': { asset: 'SOL', symbol: 'SOLUSDT', priceStepUsd: 0.1,
    modelVersion: 'binance-spot-sol-liquidation-cohorts-v2.1',
    enabledEnv: 'OPEN_LIQUIDITY_V2_SOL_GAP_DAILY_ENTRY_ENABLED' }
} as const;

export type PurpleLineMarket = keyof typeof PURPLE_LINE_SETUPS;

export function purpleLineSetupForMarket(market: string) {
  return Object.prototype.hasOwnProperty.call(PURPLE_LINE_SETUPS, market)
    ? PURPLE_LINE_SETUPS[market as PurpleLineMarket] : undefined;
}

export function purpleLineEntryEnabled(market: string): boolean {
  const config = purpleLineSetupForMarket(market);
  return Boolean(config && ['true', '1', 'yes', 'on'].includes(
    String(process.env[config.enabledEnv] || '').trim().toLowerCase()));
}
