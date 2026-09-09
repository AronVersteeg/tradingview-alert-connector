function boolValue(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || String(value).trim() === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export const DELAY_ENTRY_MODEL_ENV = 'DECENTRADER_DELAY_ENTRY_MODEL_ENABLED';
export const FRACTAL_ENTRY_MODEL_ENV = 'SHADOW_FRACTAL_ENTRY_MODEL_ENABLED';

export type FractalEntryRequest = {
  market: string;
  direction: 'long' | 'short';
  signature: string;
  signalCandleStartedAt: string;
  signalCandleClosedAt: string;
  signalClose: number;
  hourlyFractal: number;
  dailyFractal: number;
};

export type FractalEntryHandler = {
  executeFractalEntry: (request: FractalEntryRequest) => Promise<any>;
};

export function delayEntryModelEnabled(): boolean {
  return boolValue(process.env[DELAY_ENTRY_MODEL_ENV], true);
}

export function fractalEntryModelEnabled(): boolean {
  return boolValue(process.env[FRACTAL_ENTRY_MODEL_ENV], false);
}
