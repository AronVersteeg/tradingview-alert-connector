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

  test('starts with the twenty-three user-labeled reference cases', () => {
    const snapshot = intrusionTheListSnapshot();
    expect(snapshot.methodology.selectedMetric).toBe('OI_FLUSH_PCT');
    expect(snapshot.methodology.strongWhenContractChangePctLte).toBe(-1.8);
    expect(snapshot.methodology.labeledSampleSize).toBe(23);
    expect(snapshot.records.filter((record) => record.userLabel === 'STRONG')).toHaveLength(4);
    expect(snapshot.records.filter((record) => record.userLabel === 'WEAK')).toHaveLength(19);

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

  test('records the five late-August and September false outcomes against their original cases', () => {
    const records = intrusionTheListSnapshot().records;
    const expected = [
      ['INJ-USD|2026-08-31 01:00:00', 1.0504007381189506, false],
      ['INJ-USD|2026-09-01 08:00:00', 1.0059355189300012, false],
      ['SOL-USD|2026-09-01 18:00:00', 0.0720722355503911, false],
      ['INJ-USD|2026-09-01 20:00:00', 0.7572342804394117, true],
      ['INJ-USD|2026-09-02 00:00:00', 0.9776061498412592, true]
    ] as const;

    for (const [key, oiChange, completeFalse] of expected) {
      const matches = records.filter((record) => record.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0]).toMatchObject({
        userLabel: 'WEAK', automaticLabel: 'IQ WEAK', direction: 'short', filteredStatus: 'PASS'
      });
      expect(matches[0].impulseQuality.openInterest?.contractChangePct).toBeCloseTo(oiChange, 10);
      expect(matches[0].userLabelNote).toContain('User-confirmed');
      expect(matches[0].userLabelNote?.includes('complete false impulse')).toBe(completeFalse);
    }
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
      expect(snapshot.methodology.labeledSampleSize).toBe(23);
    }
  });

  test('adds the seven September outcomes with Dutch times and unchanged automatic assessments', () => {
    const records = intrusionTheListSnapshot().records;
    const expected = [
      ['BTC-USD|2026-09-03 18:00:00', '03-09-2026 20:00 NL', 'WEAK', -0.487456221422522],
      ['SOL-USD|2026-09-02 09:00:00', '02-09-2026 11:00 NL', 'WEAK', 0.49889688740754057],
      ['SOL-USD|2026-09-02 10:00:00', '02-09-2026 12:00 NL', 'WEAK', 1.564014295545335],
      ['ZEC-USD|2026-09-02 10:00:00', '02-09-2026 12:00 NL', 'WEAK', -1.297872996637106],
      ['ZEC-USD|2026-09-02 11:00:00', '02-09-2026 13:00 NL', 'WEAK', 1.4691702303321952],
      ['INJ-USD|2026-09-02 22:00:00', '03-09-2026 00:00 NL', 'WEAK', 0.5564487181533462],
      ['ZEC-USD|2026-09-03 14:00:00', '03-09-2026 16:00 NL', 'STRONG', 7.790200965905969]
    ] as const;
    for (const [key, timestampNl, userLabel, oiChange] of expected) {
      const matches = records.filter((record) => record.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0]).toMatchObject({ timestampNl, userLabel, automaticLabel: 'IQ WEAK' });
      expect(matches[0].impulseQuality.openInterest?.contractChangePct).toBeCloseTo(oiChange, 10);
    }
    expect(records.find((record) => record.key === expected[5][0])?.userLabelNote)
      .toContain('strong false impulse');
    expect(records.find((record) => record.key === expected[6][0])?.userLabelNote)
      .toContain('realized profit is not confirmed');
  });

  test('merges the September positive outcome without changing live diagnostics on repeated reads', () => {
    const key = 'ZEC-USD|2026-09-03 14:00:00';
    const seed = intrusionTheListSnapshot().records.find((record) => record.key === key)!;
    const storedQuality = {
      ...seed.impulseQuality,
      openInterest: {
        ...seed.impulseQuality.openInterest!,
        fetchedAt: '2026-09-03T15:59:31.861Z',
        startContractOpenInterest: 536243.098,
        endContractOpenInterest: 578017.513
      }
    };
    const candleReview = { status: 'PASS', candleColors: ['green'], volumeDeltaColors: ['green'] };
    fs.writeFileSync(process.env.INTRUSION_THE_LIST_FILE!, JSON.stringify({ records: [{
      ...seed, userLabel: undefined, userLabelNote: undefined,
      impulseQuality: storedQuality, candleReview
    }] }));
    for (let read = 0; read < 2; read++) {
      const matches = intrusionTheListSnapshot().records.filter((record) => record.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0]).toMatchObject({ userLabel: 'STRONG', automaticLabel: 'IQ WEAK' });
      expect(matches[0].impulseQuality).toEqual(storedQuality);
      expect(matches[0].candleReview).toEqual(candleReview);
      expect(matches[0].userLabelNote).toContain('potential profit');
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
