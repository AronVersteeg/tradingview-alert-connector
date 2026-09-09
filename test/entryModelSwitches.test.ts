import {
  DELAY_ENTRY_MODEL_ENV,
  FRACTAL_ENTRY_MODEL_ENV,
  delayEntryModelEnabled,
  fractalEntryModelEnabled
} from '../src/services/entryModelSwitches';

describe('entry model switches', () => {
  const originalDelay = process.env[DELAY_ENTRY_MODEL_ENV];
  const originalFractal = process.env[FRACTAL_ENTRY_MODEL_ENV];

  afterEach(() => {
    if (originalDelay === undefined) delete process.env[DELAY_ENTRY_MODEL_ENV];
    else process.env[DELAY_ENTRY_MODEL_ENV] = originalDelay;
    if (originalFractal === undefined) delete process.env[FRACTAL_ENTRY_MODEL_ENV];
    else process.env[FRACTAL_ENTRY_MODEL_ENV] = originalFractal;
  });

  test('preserves Delay and keeps live Shadow off when variables are absent', () => {
    delete process.env[DELAY_ENTRY_MODEL_ENV];
    delete process.env[FRACTAL_ENTRY_MODEL_ENV];
    expect(delayEntryModelEnabled()).toBe(true);
    expect(fractalEntryModelEnabled()).toBe(false);
  });

  test('can disable Delay and enable live Shadow independently', () => {
    process.env[DELAY_ENTRY_MODEL_ENV] = 'false';
    process.env[FRACTAL_ENTRY_MODEL_ENV] = 'true';
    expect(delayEntryModelEnabled()).toBe(false);
    expect(fractalEntryModelEnabled()).toBe(true);
  });
});
