export function selectDelayedNewest<T>(items: T[], delay: number): T | undefined {
  const normalizedDelay = Math.max(0, Math.floor(Number(delay) || 0));
  const index = items.length - 1 - normalizedDelay;
  return index >= 0 ? items[index] : undefined;
}

export function selectEntryNotional(options: {
  fixedUsdRisk: boolean;
  desiredNotional: number;
  collateralCappedNotional: number;
  equityCappedNotional: number;
  riskCappedNotional: number;
  hardCollateralCappedNotional: number;
}): number {
  const candidates = options.fixedUsdRisk
    ? [options.riskCappedNotional, options.hardCollateralCappedNotional]
    : [
        options.desiredNotional,
        options.collateralCappedNotional,
        options.equityCappedNotional,
        options.riskCappedNotional
      ];

  return Math.max(
    0,
    Math.min(...candidates.map((value) =>
      Number.isFinite(value) && value > 0 ? value : 0
    ))
  );
}

export function allocateStepSizes(
  size: number,
  stepSize: number,
  fractions: number[],
  reservedIndices: number[] = []
): number[] {
  if (!fractions.length || size <= 0 || stepSize <= 0) return [];

  const decimals = Math.max(0, (String(stepSize).split('.')[1] || '').length);
  const totalSteps = Math.floor((size + stepSize * 0.000001) / stepSize);
  const allocatedSteps = fractions.map(() => 0);
  const validReservedIndices = Array.from(new Set(reservedIndices))
    .filter((index) => Number.isInteger(index) && index >= 0 && index < fractions.length)
    .slice(0, totalSteps);

  for (const index of validReservedIndices) {
    allocatedSteps[index] += 1;
  }

  const remainingTotalSteps = totalSteps - validReservedIndices.length;
  const positiveFractions = fractions.map((fraction) =>
    Number.isFinite(fraction) && fraction > 0 ? fraction : 0
  );
  const fractionSum = positiveFractions.reduce((total, fraction) => total + fraction, 0);
  const normalizedFractions = fractionSum > 0
    ? positiveFractions.map((fraction) => fraction / fractionSum)
    : fractions.map(() => 1 / fractions.length);
  const rawSteps = normalizedFractions.map((fraction) => remainingTotalSteps * fraction);

  rawSteps.forEach((steps, index) => {
    allocatedSteps[index] += Math.floor(steps);
  });

  let remainder = totalSteps - allocatedSteps.reduce((total, steps) => total + steps, 0);
  const remainderOrder = rawSteps
    .map((steps, index) => ({ index, remainder: steps - Math.floor(steps) }))
    .sort((a, b) => b.remainder - a.remainder || a.index - b.index);

  for (const item of remainderOrder) {
    if (remainder <= 0) break;
    allocatedSteps[item.index] += 1;
    remainder -= 1;
  }

  return allocatedSteps.map((steps) => Number((steps * stepSize).toFixed(decimals)));
}
