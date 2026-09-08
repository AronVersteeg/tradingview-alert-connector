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

  test('starts with twenty-nine cases, separating impulse labels from a loss-only report', () => {
    const snapshot = intrusionTheListSnapshot();
    expect(snapshot.methodology.selectedMetric).toBe('OI_FLUSH_PCT');
    expect(snapshot.methodology.strongWhenContractChangePctLte).toBe(-1.8);
    expect(snapshot.records).toHaveLength(29);
    expect(snapshot.methodology.labeledSampleSize).toBe(28);
    expect(snapshot.records.filter((record) => record.userLabel === 'STRONG')).toHaveLength(6);
    expect(snapshot.records.filter((record) => record.userLabel === 'WEAK')).toHaveLength(22);

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
      expect(snapshot.methodology.labeledSampleSize).toBe(28);
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
      .toContain('Realized profit is not confirmed');
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

  test('attaches the September 4 pronounced ZEC false outcome and preserves stored evidence', () => {
    const key = 'ZEC-USD|2026-09-04 08:00:00';
    const seed = intrusionTheListSnapshot().records.find((record) => record.key === key)!;
    expect(seed).toMatchObject({
      timestampNl: '04-09-2026 10:00 NL', direction: 'long', filteredStatus: 'PASS',
      automaticLabel: 'IQ WEAK', userLabel: 'WEAK', delayCutoffAt: '2026-09-04T09:57:05.955Z'
    });
    expect(seed.impulseQuality.openInterest).toMatchObject({
      contractChangePct: 0.2026593466398774, usdChangePct: 4.124369330869282, samples: 24
    });
    const storedQuality = {
      ...seed.impulseQuality,
      openInterest: {
        ...seed.impulseQuality.openInterest!,
        fetchedAt: '2026-09-04T09:57:06.556Z',
        startContractOpenInterest: 609966.439,
        endContractOpenInterest: 611202.593
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
      expect(matches[0]).toMatchObject({ userLabel: 'WEAK', automaticLabel: 'IQ WEAK' });
      expect(matches[0].userLabelNote).toContain('hele dikke false impulse');
      expect(matches[0].impulseQuality).toEqual(storedQuality);
      expect(matches[0].candleReview).toEqual(candleReview);
      expect(snapshot.methodology.labeledSampleSize).toBe(28);
    }
  });

  test('adds September observations without turning reported outcomes into automatic IQ labels', () => {
    const records = intrusionTheListSnapshot().records;
    const cases = [
      ['INJ-USD|2026-09-04 14:00:00', 'FALSE', 0.08404117625182561],
      ['INJ-USD|2026-09-06 05:00:00', 'POTENTIAL', -0.9797534630839766],
      ['INJ-USD|2026-09-06 09:00:00', 'UNCLASSIFIED', 0.07732865324154847],
      ['INJ-USD|2026-09-06 19:00:00', 'TRUE', -0.28532620766250627],
      ['SOL-USD|2026-09-06 12:00:00', 'FALSE', -0.3840384759166149]
    ] as const;
    for (const [key, impulse, oi] of cases) {
      const matches = records.filter(r => r.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0]).toMatchObject({ automaticLabel: 'IQ WEAK', userOutcome: { source: 'user', reportedOn: '2026-09-07', impulse } });
      expect(matches[0].impulseQuality.openInterest?.contractChangePct).toBeCloseTo(oi, 10);
    }
    const loss = records.find(r => r.key === cases[2][0])!;
    expect(loss.userLabel).toBeUndefined();
    expect(loss.userOutcome).toMatchObject({ tradeResult: 'LOSS', exitReason: 'STOP_LOSS' });
    const potential = records.find(r => r.key === cases[1][0])!;
    expect(potential.userOutcome).toMatchObject({ tradeResult: 'UNCONFIRMED', exitReason: 'TRAILING_STOP' });
    expect(records.find(r => r.key === cases[3][0])?.userOutcome?.dailyFractalBreak)
      .toMatchObject({ reported: true, verifiedBeforeDelayCutoff: false });
  });

  test('the six reference measurements agree with the archived original assessments', () => {
    const evidence = JSON.parse(fs.readFileSync(path.join(__dirname, '../docs/research/the-list-2026-09-07-evidence.json'), 'utf8'));
    const records = intrusionTheListSnapshot().records;
    expect(evidence.records).toHaveLength(6);
    for (const original of evidence.records) {
      const reference = records.find(r => r.key === original.key)!;
      expect(reference).toMatchObject({
        timestampNl: original.timestampNl, direction: original.direction,
        delayCutoffAt: original.delayCutoffAt, automaticLabel: original.automaticLabel
      });
      expect(reference.impulseQuality.openInterest).toMatchObject({
        contractChangePct: original.impulseQuality.openInterest.contractChangePct,
        usdChangePct: original.impulseQuality.openInterest.usdChangePct,
        samples: original.impulseQuality.openInterest.samples
      });
    }
  });

  test('revises a persisted ZEC annotation without overwriting its original Delay evidence', () => {
    const key = 'ZEC-USD|2026-09-03 14:00:00';
    const seed = intrusionTheListSnapshot().records.find(r => r.key === key)!;
    const stored = { ...seed, userOutcome: undefined, userLabelNote: 'Old potential-profit report',
      impulseQuality: { ...seed.impulseQuality, reasons: ['Original live evidence retained'] } };
    fs.writeFileSync(process.env.INTRUSION_THE_LIST_FILE!, JSON.stringify({ records: [stored] }));
    for (let i = 0; i < 2; i++) {
      const matches = intrusionTheListSnapshot().records.filter(r => r.key === key);
      expect(matches).toHaveLength(1);
      expect(matches[0].userLabelNote).toContain('start of an impulse');
      expect(matches[0].userOutcome?.dailyFractalBreak?.verifiedBeforeDelayCutoff).toBe(false);
      expect(matches[0].impulseQuality).toEqual(stored.impulseQuality);
    }
  });

  test('keeps a newer user revision ahead of seeded annotations', () => {
    const seed = intrusionTheListSnapshot().records.find(r => r.key === 'ZEC-USD|2026-09-03 14:00:00')!;
    const updated = { ...seed, userLabel: 'WEAK', userLabelNote: 'Later correction',
      userOutcome: { source: 'user', reportedOn: '2026-09-08', impulse: 'FALSE', tradeResult: 'LOSS' } };
    fs.writeFileSync(process.env.INTRUSION_THE_LIST_FILE!, JSON.stringify({ records: [updated] }));
    const record = intrusionTheListSnapshot().records.find(r => r.key === seed.key)!;
    expect(record.userLabel).toBe('WEAK');
    expect(record.userLabelNote).toBe('Later correction');
  });

  test('live diagnostic refresh preserves the new structured user outcome', () => {
    const seed = intrusionTheListSnapshot().records.find(r => r.key === 'INJ-USD|2026-09-06 05:00:00')!;
    const impulseQuality = { ...seed.impulseQuality, reasons: ['Refreshed diagnostic evidence'] };
    recordIntrusionTheList({ ...seed, impulseQuality, userOutcome: undefined });
    const record = intrusionTheListSnapshot().records.find(r => r.key === seed.key)!;
    expect(record.userOutcome).toEqual(seed.userOutcome);
    expect(record.userLabel).toBe('STRONG');
    expect(record.automaticLabel).toBe('IQ WEAK');
    expect(record.impulseQuality.reasons).toEqual(['Refreshed diagnostic evidence']);
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
