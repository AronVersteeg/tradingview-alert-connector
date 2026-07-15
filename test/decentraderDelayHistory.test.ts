import {
  buildDecentraderDelayRecord,
  summarizeDecentraderDelayRecords
} from '../src/services/decentraderDelayHistory';

describe('Decentrader SMTP delay history', () => {
  test('counts only fully available one-hour candles', () => {
    const record = buildDecentraderDelayRecord({
      signature: '2026-07-14 22:00:00|L|10|65100:1',
      emailType: 'normal',
      sideCounts: '1 left edge',
      intrusionTimestamp: '2026-07-14 22:00:00',
      intrusionTimestampNl: '15-07-2026 00:00 NL',
      smtpSentAt: '2026-07-15T00:13:00.000Z'
    });

    expect(record).toBeDefined();
    expect(record?.delayMinutes).toBe(133);
    expect(record?.delayCandles1h).toBeCloseTo(2.2167, 3);
    expect(record?.completedCandles1h).toBe(2);
  });

  test('summarizes minimum, average and maximum completed candle delays', () => {
    const records = [60, 150, 245].map((delayMinutes, index) => ({
      id: String(index),
      signature: String(index),
      emailType: 'normal' as const,
      sideCounts: '1 left edge',
      intrusionTimestamp: '2026-07-14 22:00:00',
      intrusionTimestampNl: '15-07-2026 00:00 NL',
      smtpSentAt: '2026-07-15T00:00:00.000Z',
      delayMinutes,
      delayCandles1h: delayMinutes / 60,
      completedCandles1h: Math.floor(delayMinutes / 60)
    }));

    expect(summarizeDecentraderDelayRecords(records)).toEqual(expect.objectContaining({
      count: 3,
      minCompletedCandles1h: 1,
      averageCompletedCandles1h: 7 / 3,
      maxCompletedCandles1h: 4,
      minMinutes: 60,
      averageMinutes: 455 / 3,
      maxMinutes: 245
    }));
  });
});
