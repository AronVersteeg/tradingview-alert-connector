import {
  aggregateIntrusionDomWindow,
  buildIntrusionDomEvidence,
  buildIntrusionFlowFlipEvidence,
  IntrusionDomWindow
} from '../src/services/decentraderIntrusionDomStudy';
import { DomMinuteRecord } from '../src/services/decentralizedDomCollector';

function minute(input: {
  at: string;
  mid: number;
  takerDelta: number;
  bidAdded?: number;
  bidRemoved?: number;
  askAdded?: number;
  askRemoved?: number;
}): DomMinuteRecord {
  return {
    version: 1,
    bucketStart: input.at,
    bucketEnd: new Date(Date.parse(input.at) + 60_000).toISOString(),
    market: 'BTC-USD',
    venues: {
      dydx: {
        samples: 4,
        failedPolls: 0,
        mid: input.mid,
        spreadBps: 0.5,
        depthCoverageBps: { bid: 100, ask: 100 },
        bidDepthUsd: {},
        askDepthUsd: {},
        imbalance: { '25': 0.1 },
        imbalanceMin: { '25': 0 },
        imbalanceMax: { '25': 0.2 },
        micropriceOffsetBps: 0,
        bidAddedUsd: input.bidAdded || 0,
        bidRemovedUsd: input.bidRemoved || 0,
        askAddedUsd: input.askAdded || 0,
        askRemovedUsd: input.askRemoved || 0,
        buyTakerUsd: Math.max(0, input.takerDelta),
        sellTakerUsd: Math.max(0, -input.takerDelta),
        buyTrades: 1,
        sellTrades: 1,
        largestTradeUsd: 100_000
      }
    },
    crossVenue: {
      availableVenues: 1,
      midSpreadBps: 0,
      consensusImbalance25Bps: 0.1,
      consensusTakerDeltaUsd: input.takerDelta
    }
  };
}

function window(overrides: Partial<IntrusionDomWindow> = {}): IntrusionDomWindow {
  return {
    from: '2026-07-21T05:00:00.000Z',
    to: '2026-07-21T06:00:00.000Z',
    coverageMinutes: 60,
    startPrice: 65_000,
    endPrice: 65_500,
    rawPriceReturnPct: 0.5,
    directionalPriceReturnPct: 0.5,
    rawTakerDeltaUsd: 20_000_000,
    directionalTakerDeltaUsd: 20_000_000,
    rawBookPressureUsd: 50_000_000,
    directionalBookPressureUsd: 50_000_000,
    averageImbalance25Bps: 0,
    directionalImbalance25Bps: 0,
    largestTradeUsd: 2_000_000,
    bidNetChangeUsd: 0,
    askNetChangeUsd: -50_000_000,
    ...overrides
  };
}

describe('Decentrader intrusion DOM study', () => {
  test('normalizes price, taker flow and visible book pressure to the signal direction', () => {
    const from = Date.parse('2026-07-21T05:00:00.000Z');
    const result = aggregateIntrusionDomWindow([
      minute({ at: '2026-07-21T05:00:00.000Z', mid: 65_000, takerDelta: 2_000_000, bidRemoved: 5_000_000, askRemoved: 20_000_000 }),
      minute({ at: '2026-07-21T05:01:00.000Z', mid: 65_100, takerDelta: 3_000_000, bidRemoved: 5_000_000, askRemoved: 30_000_000 })
    ], from, from + 60 * 60_000, 'long');

    expect(result).toEqual(expect.objectContaining({
      coverageMinutes: 2,
      rawTakerDeltaUsd: 5_000_000,
      directionalTakerDeltaUsd: 5_000_000,
      rawBookPressureUsd: 40_000_000,
      directionalBookPressureUsd: 40_000_000
    }));
    expect(result?.directionalPriceReturnPct).toBeGreaterThan(0);
  });

  test('marks the benchmark impulse only when directional DOM and CoinGlass evidence align', () => {
    const evidence = buildIntrusionDomEvidence({
      intrusion1h: window(),
      confirmation1h: window({ directionalPriceReturnPct: 0.3 })
    }, {
      event: {
        observedAt: '2026-07-21T05:00:00.000Z',
        ageMinutes: 0,
        price: 65_000,
        levelCount: 10,
        supportCount: 5,
        frictionCount: 5,
        supportUsd: 200_000_000,
        frictionUsd: 80_000_000
      },
      review: {
        observedAt: '2026-07-21T07:30:00.000Z',
        ageMinutes: 0,
        price: 66_000,
        levelCount: 5,
        supportCount: 5,
        frictionCount: 0,
        supportUsd: 190_000_000,
        frictionUsd: 0
      },
      frictionRemovedUsd: 80_000_000,
      frictionRemovalPct: 1,
      supportRetentionPct: 0.95
    });

    expect(evidence.score).toBe(6);
    expect(evidence.available).toBe(6);
    expect(evidence.validDomPattern).toBe(true);
    expect(evidence.classification).toBe('IMPULSE_CANDIDATE');
  });

  test('treats falling price as progress when CoinGlass friction clears for a short', () => {
    const evidence = buildIntrusionDomEvidence({
      intrusion1h: window({
        startPrice: 65_000,
        endPrice: 64_500,
        rawPriceReturnPct: -0.5,
        directionalPriceReturnPct: 0.5
      }),
      confirmation1h: window({ directionalPriceReturnPct: 0.3 })
    }, {
      event: {
        observedAt: '2026-07-21T05:00:00.000Z',
        ageMinutes: 0,
        price: 65_000,
        levelCount: 10,
        supportCount: 5,
        frictionCount: 5,
        supportUsd: 200_000_000,
        frictionUsd: 80_000_000
      },
      review: {
        observedAt: '2026-07-21T07:30:00.000Z',
        ageMinutes: 0,
        price: 64_000,
        levelCount: 5,
        supportCount: 5,
        frictionCount: 0,
        supportUsd: 190_000_000,
        frictionUsd: 0
      },
      frictionRemovedUsd: 80_000_000,
      frictionRemovalPct: 1,
      supportRetentionPct: 0.95
    }, 'short');

    expect(evidence.components.find((component) => component.key === 'cg-friction-removal')).toEqual(
      expect.objectContaining({ available: true, passed: true })
    );
    expect(evidence.validDomPattern).toBe(true);
  });

  test('marks a strong flow flip without claiming that price has officially reversed', () => {
    const flowFlip = buildIntrusionFlowFlipEvidence({
      pre1h: window({
        directionalPriceReturnPct: 0.38,
        directionalTakerDeltaUsd: 60_000_000,
        directionalBookPressureUsd: 117_000_000
      }),
      intrusion1h: window({
        directionalPriceReturnPct: 0.12,
        directionalTakerDeltaUsd: -39_000_000,
        directionalBookPressureUsd: -20_000_000
      }),
      confirmation1h: window({ directionalPriceReturnPct: -0.14 })
    });

    expect(flowFlip).toEqual(expect.objectContaining({
      score: 6,
      available: 6,
      candidate: true,
      strong: true,
      classification: 'FLOW_FLIP_STRONG'
    }));
  });

  test('does not flag partial buildup when aggressive flow and confirmation stay positive', () => {
    const flowFlip = buildIntrusionFlowFlipEvidence({
      pre1h: window(),
      intrusion1h: window({
        directionalPriceReturnPct: -0.02,
        directionalTakerDeltaUsd: 19_000_000,
        directionalBookPressureUsd: -21_000_000
      }),
      confirmation1h: window({ directionalPriceReturnPct: 0.37 })
    });

    expect(flowFlip.candidate).toBe(false);
    expect(flowFlip.classification).toBe('NONE');
  });
});
