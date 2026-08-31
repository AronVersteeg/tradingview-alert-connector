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

  test('starts with the eleven user-labeled reference cases', () => {
    const snapshot = intrusionTheListSnapshot();
    expect(snapshot.methodology.selectedMetric).toBe('OI_FLUSH_PCT');
    expect(snapshot.methodology.strongWhenContractChangePctLte).toBe(-1.8);
    expect(snapshot.methodology.labeledSampleSize).toBe(11);
    expect(snapshot.records.filter((record) => record.userLabel === 'STRONG')).toHaveLength(3);
    expect(snapshot.records.filter((record) => record.userLabel === 'WEAK')).toHaveLength(8);

    const moderateStrong = snapshot.records
      .find((record) => record.key === 'INJ-USD|2026-08-28 16:00:00');
    expect(moderateStrong).toMatchObject({
      automaticLabel: 'IQ STRONG',
      userLabel: 'STRONG',
      direction: 'short',
      delayCutoffAt: '2026-08-28T17:47:52.761Z'
    });
    expect(moderateStrong?.impulseQuality.openInterest?.contractChangePct).toBeCloseTo(-1.846121, 6);
    expect(moderateStrong?.userLabelNote).toContain('small profit');
  });

  test('records the August 30 ZEC false outcome separately from its original IQ assessment', () => {
    const records = intrusionTheListSnapshot().records
      .filter((record) => record.key === 'ZEC-USD|2026-08-30 12:00:00');
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      userLabel: 'WEAK', automaticLabel: 'IQ WEAK', direction: 'long', filteredStatus: 'PASS',
      timestampNl: '30-08-2026 14:00 NL', delayCutoffAt: '2026-08-30T13:14:17.895Z'
    });
    expect(records[0].userLabelNote).toContain('User-confirmed false impulse');
    expect(records[0].impulseQuality.openInterest).toMatchObject({
      contractChangePct: 0.4456443352116146, usdChangePct: 1.3486931446338835, samples: 15
    });
  });

  test('attaches the ZEC false outcome without replacing stored diagnostics or duplicating the case', () => {
    const key = 'ZEC-USD|2026-08-29 14:00:00';
    const seed = intrusionTheListSnapshot().records.find((record) => record.key === key)!;
    expect(seed).toMatchObject({
      automaticLabel: 'IQ WEAK', userLabel: 'WEAK', direction: 'long', filteredStatus: 'PASS',
      timestampNl: '29-08-2026 16:00 NL', delayCutoffAt: '2026-08-29T15:04:21.006Z'
    });
    expect(seed.impulseQuality.openInterest).toMatchObject({
      contractChangePct: 2.6769214175664358, usdChangePct: 6.241772732132067, samples: 13
    });
    const storedQuality = {
      ...seed.impulseQuality,
      openInterest: {
        ...seed.impulseQuality.openInterest!,
        fetchedAt: '2026-08-29T15:04:21.607Z',
        startContractOpenInterest: 542249.089,
        endContractOpenInterest: 556764.671
      }
    };
    const candleReview = { status: 'PASS', candleColors: ['green'], volumeDeltaColors: ['green'] };
    fs.writeFileSync(process.env.INTRUSION_THE_LIST_FILE!, JSON.stringify({ records: [{
      ...seed, userLabel: undefined, userLabelNote: undefined,
      impulseQuality: storedQuality, candleReview
    }] }));

    for (let read = 0; read < 2; read++) {
      const snapshot = intrusionTheListSnapshot();
      const matches = snapshot.records.filter((record) => record.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0].userLabel).toBe('WEAK');
      expect(matches[0].userLabelNote).toContain('User-confirmed false impulse');
      expect(matches[0].automaticLabel).toBe('IQ WEAK');
      expect(matches[0].impulseQuality).toEqual(storedQuality);
      expect(matches[0].candleReview).toEqual(candleReview);
      expect(snapshot.methodology.labeledSampleSize).toBe(11);
    }
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
      filteredStatus: 'PASS', impulseQuality, candleReview: {
        status: 'PASS', closedCandlesChecked: 2,
        candleTimestamps: ['2026-08-19T20:00:00.000Z', '2026-08-19T21:00:00.000Z'],
        candleOpens: [100, 102], candleCloses: [102, 105],
        quoteVolume: [1_000, 2_000], volumeDeltaQuote: [200, 600]
      }
    });

    const record = intrusionTheListSnapshot().records
      .find((candidate) => candidate.key === 'BTC-USD|2026-08-19 20:00:00');
    expect(record?.userLabel).toBe('STRONG');
    expect(record?.automaticLabel).toBe('IQ STRONG');
    expect(record?.candleReview?.status).toBe('PASS');
    expect(record?.binance).toMatchObject({
      source: 'binance-futures', causal: true, closedCandles: 2,
      totalQuoteVolume: 3_000, cumulativeTakerDeltaQuote: 800,
      alignedTakerDeltaCandles: 2, takerDeltaPersistencePct: 100,
      oiContractChangePct: -2.3, oiPriceRegime: 'POSITION_FLUSH_WITH_MOVE'
    });
    expect(record?.binance?.priceChangePct).toBeCloseTo(5);
    expect(record?.binance?.directionalTakerDeltaRatio).toBeCloseTo(800 / 3_000);
  });
});
