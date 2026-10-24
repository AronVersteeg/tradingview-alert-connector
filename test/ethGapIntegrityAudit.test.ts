export {};
const { auditPayload, compareSavedLabel } = require('../scripts/audit-eth-gap-source.cjs');
const start = Date.parse('2026-10-20T00:00:00Z'), hour = 3_600_000;
function fixture() {
  const candles = [3000, 3050, 3100].map((price, i) => ({ timestampMs: start + i * hour, open: price, high: price + 5, low: price - 5, close: price }));
  const payload = {
    source: { market: 'ETH-USD', sourceStatuses: [{ name: 'Binance Spot ETHUSDT 1H', ok: true }] },
    modelVersion: 'binance-spot-eth-liquidation-cohorts-v2.1', quality: { priceStepUsd: 5, causalModel: true },
    frames: candles.map(c => ({ ...c, startedAtMs: c.timestampMs, t: new Date(c.timestampMs).toISOString().slice(0, 19).replace('T', ' '), price: c.close })),
    gaps: [{ left: 2800, right: 3200, width: 400 }, { left: 2900, right: 3150, width: 250 }, { left: 2900, right: 3300, width: 400 }],
    zoneSeed: [['L', 10, 2800, 1], ['S', 10, 3200, 1]],
    zoneDeltas: [[], [['L', 10, 2900, 1], ['S', 10, 3150, 1]], [['S', 10, 3200, 0], ['S', 10, 3150, 0], ['S', 10, 3300, 1]]],
    intrusionBenchmarks: { history: [{ timestamp: '2026-10-20 01:00:00', direction: 'mixed', gap: { left: 2800, right: 3200 },
      smtpSentAt: '2026-10-20T12:00:00Z' }] }
  };
  return { candles, payload };
}
describe('Independent ETH Spot payload integrity audit', () => {
  test('recomputes nearest edges on every frame from its own cohort seed/deltas, in both directions', () => {
    const { candles, payload } = fixture();
    const original = JSON.stringify(payload), report = auditPayload(payload, candles);
    expect(report).toMatchObject({ frames: 3, candleMatches: 3, uniqueLeftEdges: 2, uniqueRightEdges: 3,
      leftChanges: 1, rightChanges: 2, benchmarkOverlap: 1, reconstructedIntrusions: 1, issues: [] });
    expect(JSON.stringify(payload)).toBe(original);
  });
  test('detects a frozen right edge even if the left edge continues to look correct', () => {
    const { candles, payload } = fixture();
    payload.gaps[1].right = payload.gaps[2].right = 3200;
    payload.gaps[1].width = payload.gaps[2].width = 300;
    const report = auditPayload(payload, candles);
    expect(report.issues.filter((r: any) => r.type === 'nearest-edge-mismatch')).toHaveLength(2);
    expect(report.issues[0].expected).toEqual([2900, 3150]);
  });
  test('detects wrong candles and a saved intrusion using the wrong previous gap', () => {
    const { candles, payload } = fixture(); candles[1].open = 123;
    payload.intrusionBenchmarks.history[0].gap.right = 9999;
    const issues = auditPayload(payload, candles).issues;
    expect(issues.map((r: any) => r.type).sort()).toEqual(['saved-intrusion-mismatch', 'spot-ohlc-mismatch']);
  });
  test('rejects mixed BTC/Futures sources and reports older labels as unverifiable, not correct by assumption', () => {
    const { candles, payload } = fixture();
    payload.intrusionBenchmarks.history[0].timestamp = '2026-07-01 00:00:00';
    expect(auditPayload(payload, candles)).toMatchObject({ benchmarkOverlap: 0, benchmarkOutsideCoverage: 1 });
    payload.source.sourceStatuses.push({ name: 'Binance Futures ETHUSDT 1H', ok: true });
    expect(() => auditPayload(payload, candles)).toThrow();
    payload.source.sourceStatuses = [{ name: 'Binance Spot BTCUSDT 1H', ok: true }];
    expect(() => auditPayload(payload, candles)).toThrow();
  });
  test('distinguishes missing original gap evidence from an actual mismatch or a documented revision', () => {
    const record = { timestamp: '2026-08-27 09:00:00', direction: 'long' }, gap = { left: 2800, right: 3200 };
    const entrants = [['L', 10, 2900, 1]];
    expect(compareSavedLabel(record, gap, entrants).kind).toBe('original-gap-missing');
    expect(compareSavedLabel({ ...record, gap }, gap, entrants).kind).toBe('match');
    const changed = { ...record, gap: { left: 2800, right: 9999 } };
    expect(compareSavedLabel(changed, gap, entrants).kind).toBe('gap-mismatch');
    expect(compareSavedLabel({ ...changed, gapRevisions: [{ gap }] }, gap, entrants).kind).toBe('saved-revision');
    expect(compareSavedLabel(record, gap, []).kind).toBe('intrusion-mismatch');
  });
});
