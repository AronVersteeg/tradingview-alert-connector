import {
  allocateStepSizes,
  selectDelayedNewest,
  selectEntryNotional
} from '../src/services/decentraderExecutionPolicy';

describe('Decentrader execution policy', () => {
  test('fractal delay selects the configured older confirmed fractal', () => {
    expect(selectDelayedNewest(['f1', 'f2', 'f3'], 0)).toBe('f3');
    expect(selectDelayedNewest(['f1', 'f2', 'f3'], 1)).toBe('f2');
    expect(selectDelayedNewest(['f1', 'f2', 'f3'], 2)).toBe('f1');
    expect(selectDelayedNewest(['f1', 'f2', 'f3'], 3)).toBeUndefined();
  });

  test('gap-edge TP receives one market step before weighted allocation', () => {
    const sizes = allocateStepSizes(0.0006, 0.0001, [0.01, 0.45, 0.3, 0.24], [0]);

    expect(sizes[0]).toBeGreaterThanOrEqual(0.0001);
    expect(sizes.reduce((total, size) => total + size, 0)).toBeCloseTo(0.0006, 8);
  });

  test('allocation never exceeds the available position steps', () => {
    const sizes = allocateStepSizes(0.0002, 0.0001, [0.1, 0.2, 0.3, 0.4], [0]);

    expect(sizes[0]).toBe(0.0001);
    expect(sizes.reduce((total, size) => total + size, 0)).toBeCloseTo(0.0002, 8);
  });

  test('fixed USD risk sizing is led by stop risk instead of soft map and equity caps', () => {
    const notional = selectEntryNotional({
      fixedUsdRisk: true,
      desiredNotional: 500,
      collateralCappedNotional: 350,
      equityCappedNotional: 66,
      riskCappedNotional: 110,
      hardCollateralCappedNotional: 5800
    });

    expect(notional).toBe(110);
  });

  test('fixed USD risk sizing still respects hard available collateral', () => {
    const notional = selectEntryNotional({
      fixedUsdRisk: true,
      desiredNotional: 500,
      collateralCappedNotional: 350,
      equityCappedNotional: 66,
      riskCappedNotional: 110,
      hardCollateralCappedNotional: 80
    });

    expect(notional).toBe(80);
  });

  test('percentage risk sizing preserves the existing soft caps', () => {
    const notional = selectEntryNotional({
      fixedUsdRisk: false,
      desiredNotional: 500,
      collateralCappedNotional: 350,
      equityCappedNotional: 66,
      riskCappedNotional: 110,
      hardCollateralCappedNotional: 5800
    });

    expect(notional).toBe(66);
  });
});
