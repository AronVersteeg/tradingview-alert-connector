type DirectionSignal = { closedAt: number; direction: 'long' | 'short' };

export function filterGreenContinuation<T extends DirectionSignal>(candidates: readonly T[], purples: readonly DirectionSignal[]): T[] {
  const anchors = [...purples].sort((a, b) => a.closedAt - b.closedAt);
  let next = 0;
  return [...candidates].sort((a, b) => a.closedAt - b.closedAt).filter(candidate => {
    while (next < anchors.length && anchors[next].closedAt < candidate.closedAt) next++;
    return next > 0 && anchors[next - 1].direction === candidate.direction;
  });
}
