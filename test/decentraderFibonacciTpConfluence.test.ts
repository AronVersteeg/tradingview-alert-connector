import { gapFibonacciConfluenceForZone } from '../src/services/decentraderGapMonitor';

const gap = {
  left: 60_000,
  right: 66_000,
  width: 6_000
} as any;

describe('Decentrader gap Fibonacci TP confluence', () => {
  test('matches an existing long TP zone near the 1.618 gap extension', () => {
    const confluence = gapFibonacciConfluenceForZone('long', 69_750, gap, 100);

    expect(confluence?.source).toBe('gap-extension');
    expect(confluence?.ratio).toBe(1.618);
    expect(confluence?.price).toBeCloseTo(69_708);
    expect(confluence?.distance).toBeCloseTo(42);
    expect(confluence?.tolerance).toBe(100);
    expect(confluence?.proximity).toBeCloseTo(0.58);
  });

  test('mirrors the same extension for an existing short TP zone', () => {
    const confluence = gapFibonacciConfluenceForZone('short', 56_300, gap, 100);

    expect(confluence?.ratio).toBe(1.618);
    expect(confluence?.price).toBeCloseTo(56_292);
    expect(confluence?.distance).toBeCloseTo(8);
  });

  test('does not mark a zone inside the gap', () => {
    expect(gapFibonacciConfluenceForZone('long', 65_000, gap, 100)).toBeUndefined();
    expect(gapFibonacciConfluenceForZone('short', 61_000, gap, 100)).toBeUndefined();
  });

  test('does not mark an unrelated liquidity zone outside tolerance', () => {
    expect(gapFibonacciConfluenceForZone('long', 70_100, gap, 100)).toBeUndefined();
  });
});
