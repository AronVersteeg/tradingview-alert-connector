import { DomMinuteRecord, DomVenueMinute } from '../src/services/decentralizedDomCollector';
import { evaluateIntrusionImpulseQuality } from '../src/services/intrusionImpulseQuality';

function venueMinute(mid: number): DomVenueMinute {
  return {
    samples: 1,
    failedPolls: 0,
    mid,
    spreadBps: 1,
    depthCoverageBps: { bid: 100, ask: 100 },
    bidDepthUsd: { '25': 200_000 },
    askDepthUsd: { '25': 100_000 },
    imbalance: { '25': 0.33 },
    imbalanceMin: { '25': 0.25 },
    imbalanceMax: { '25': 0.4 },
    micropriceOffsetBps: 1,
    bidAddedUsd: 10_000,
    bidRemovedUsd: 1_000,
    askAddedUsd: 1_000,
    askRemovedUsd: 2_000,
    buyTakerUsd: 15_000,
    sellTakerUsd: 5_000,
    buyTrades: 10,
    sellTrades: 4,
    largestTradeUsd: 5_000
  };
}

function minuteRecord(startMinute: number): DomMinuteRecord {
  const start = Date.UTC(2026, 6, 21, 5, startMinute);
  const mid = 100 + startMinute * 0.05;
  return {
    version: 1,
    bucketStart: new Date(start).toISOString(),
    bucketEnd: new Date(start + 60_000).toISOString(),
    market: 'BTC-USD',
    venues: { dydx: venueMinute(mid) },
    crossVenue: {
      availableVenues: 1,
      midSpreadBps: 0,
      consensusImbalance25Bps: 0.33,
      consensusTakerDeltaUsd: 10_000
    }
  };
}

describe('intrusion impulse quality', () => {
  const review = {
    status: 'PASS',
    delayCutoffAt: '2026-07-21T05:20:00.000Z',
    candleOpens: [100, 101],
    candleCloses: [101, 102.5],
    quoteVolume: [1_000, 1_000],
    volumeDeltaQuote: [200, 200]
  };

  test('labels a fully causal confirming window as strong', () => {
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long',
      alertTimestamp: '2026-07-21 05:00:00',
      gapWidth: 5,
      review,
      domRecords: Array.from({ length: 10 }, (_, index) => minuteRecord(index)),
      coinGlass: {
        source: 'first-observed',
        buyUsd: 20_000_000,
        sellUsd: 10_000_000,
        fetchedAt: '2026-07-21T05:15:00.000Z'
      },
      evaluatedAt: '2026-07-21T05:20:01.000Z'
    });

    expect(result.label).toBe('IQ STRONG');
    expect(result.score).toBe(8);
    expect(result.availableSignals).toBe(8);
    expect(result.dom.window?.coverageMinutes).toBe(10);
    expect(result.coinGlass.directionalRatio).toBe(2);
    expect(result.dataCutoffAt).toBe(review.delayCutoffAt);
  });

  test('ignores DOM and CoinGlass observations after the SMTP cutoff', () => {
    const futureDom = Array.from({ length: 10 }, (_, index) => minuteRecord(20 + index));
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long',
      alertTimestamp: '2026-07-21 05:00:00',
      gapWidth: 5,
      review,
      domRecords: futureDom,
      coinGlass: {
        source: 'future-observation',
        buyUsd: 100_000_000,
        sellUsd: 1,
        fetchedAt: '2026-07-21T05:21:00.000Z'
      }
    });

    expect(result.label).toBe('IQ DATA GAP');
    expect(result.dom.window).toBeUndefined();
    expect(result.coinGlass.directionalRatio).toBeUndefined();
    expect(result.coinGlass.fetchedAt).toBeNull();
    expect(result.reasons[0]).toContain('Causal DOM coverage');
  });
});
