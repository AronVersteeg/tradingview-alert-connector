import fs from 'fs';
import os from 'os';
import path from 'path';

import { evaluateIntrusionImpulseQuality } from '../src/services/intrusionImpulseQuality';
import { intrusionTheListSnapshot, recordIntrusionTheList } from '../src/services/intrusionTheList';

describe('The List', () => {
  const previousFile = process.env.INTRUSION_THE_LIST_FILE;
  let directory = '';

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'intrusion-list-'));
    process.env.INTRUSION_THE_LIST_FILE = path.join(directory, 'the-list.json');
  });

  afterEach(() => {
    if (previousFile === undefined) delete process.env.INTRUSION_THE_LIST_FILE;
    else process.env.INTRUSION_THE_LIST_FILE = previousFile;
    fs.rmSync(directory, { recursive: true, force: true });
  });

  test('starts with the five user-labeled reference cases', () => {
    const snapshot = intrusionTheListSnapshot();
    expect(snapshot.methodology.selectedMetric).toBe('OI_FLUSH_PCT');
    expect(snapshot.methodology.strongWhenContractChangePctLte).toBe(-1.8);
    expect(snapshot.methodology.labeledSampleSize).toBe(5);
    expect(snapshot.records.filter((record) => record.userLabel === 'STRONG')).toHaveLength(2);
    expect(snapshot.records.filter((record) => record.userLabel === 'WEAK')).toHaveLength(3);
  });

  test('updates live diagnostics without overwriting a user label', () => {
    const impulseQuality = evaluateIntrusionImpulseQuality({
      direction: 'long', alertTimestamp: '2026-08-19 20:00:00',
      review: { delayCutoffAt: '2026-08-19T22:32:57.517Z' }, domRecords: [],
      openInterest: {
        source: 'binance-futures-open-interest', symbol: 'BTCUSDT',
        from: '2026-08-19T20:00:00.000Z', to: '2026-08-19T22:32:57.517Z',
        fetchedAt: '2026-08-19T22:32:57.517Z', samples: 31,
        contractChangePct: -2.3, usdChangePct: -1.2
      }
    });
    recordIntrusionTheList({
      market: 'BTC-USD', symbol: 'BTCUSDT', asset: 'BTC',
      alertTimestamp: '2026-08-19 20:00:00', timestampNl: '19-08-2026 22:00 NL',
      direction: 'long', delayCutoffAt: '2026-08-19T22:32:57.517Z',
      filteredStatus: 'PASS', impulseQuality, candleReview: { status: 'PASS' }
    });

    const record = intrusionTheListSnapshot().records
      .find((candidate) => candidate.key === 'BTC-USD|2026-08-19 20:00:00');
    expect(record?.userLabel).toBe('STRONG');
    expect(record?.automaticLabel).toBe('IQ STRONG');
    expect(record?.candleReview).toEqual({ status: 'PASS' });
  });
});
