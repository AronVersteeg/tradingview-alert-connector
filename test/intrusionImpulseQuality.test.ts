import { DomMinuteRecord, DomVenueMinute } from '../src/services/decentralizedDomCollector';
import { evaluateIntrusionImpulseQuality } from '../src/services/intrusionImpulseQuality';

function venueMinute(mid: number): DomVenueMinute {
  return {
    samples: 1, failedPolls: 0, mid, spreadBps: 1,
    depthCoverageBps: { bid: 100, ask: 100 },
    bidDepthUsd: { '25': 200_000 }, askDepthUsd: { '25': 100_000 },
    imbalance: { '25': 0.33 }, imbalanceMin: { '25': 0.25 }, imbalanceMax: { '25': 0.4 },
    micropriceOffsetBps: 1, bidAddedUsd: 10_000, bidRemovedUsd: 1_000,
    askAddedUsd: 1_000, askRemovedUsd: 2_000, buyTakerUsd: 15_000,
    sellTakerUsd: 5_000, buyTrades: 10, sellTrades: 4, largestTradeUsd: 5_000
  };
}

function minuteRecord(startMinute: number): DomMinuteRecord {
  const start = Date.UTC(2026, 7, 19, 20, startMinute);
  return {
    version: 1,
    bucketStart: new Date(start).toISOString(),
    bucketEnd: new Date(start + 60_000).toISOString(),
    market: 'BTC-USD',
    venues: { dydx: venueMinute(100 + startMinute * 0.05) },
    crossVenue: {
      availableVenues: 1, midSpreadBps: 0,
      consensusImbalance25Bps: 0.33, consensusTakerDeltaUsd: 10_000
    }
  };
}

describe('intrusion impulse quality v2', () => {
  const review = {
    status: 'PASS',
    delayCutoffAt: '2026-08-19T20:20:00.000Z',
    candleOpens: [100, 101], candleCloses: [101, 102.5],
    quoteVolume: [1_000, 1_000], volumeDeltaQuote: [200, 200]
  };

  test('uses one causal OI flush variable to classify a strong event', () => {
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long', alertTimestamp: '2026-08-19 20:00:00', gapWidth: 5,
      review, domRecords: [],
      openInterest: {
        source: 'binance-futures-open-interest', symbol: 'BTCUSDT',
        from: '2026-08-19T20:00:00.000Z', to: review.delayCutoffAt,
        fetchedAt: review.delayCutoffAt, samples: 5,
        contractChangePct: -2.26, usdChangePct: -1.21
      }
    });

    expect(result.label).toBe('IQ STRONG');
    expect(result.headline).toBe('IQ STRONG OI -2.26%');
    expect(result.score).toBe(1);
    expect(result.availableSignals).toBe(1);
    expect(result.selectedMetric).toBe('OI_FLUSH_PCT');
  });

  test('does not let bullish DOM and CoinGlass override a weak OI flush', () => {
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long', alertTimestamp: '2026-08-19 20:00:00', gapWidth: 5,
      review,
      domRecords: Array.from({ length: 10 }, (_, index) => minuteRecord(index)),
      coinGlass: {
        source: 'first-observed', buyUsd: 100_000_000, sellUsd: 1,
        fetchedAt: '2026-08-19T20:15:00.000Z'
      },
      openInterest: {
        source: 'binance-futures-open-interest', symbol: 'BTCUSDT',
        from: '2026-08-19T20:00:00.000Z', to: review.delayCutoffAt,
        fetchedAt: review.delayCutoffAt, samples: 5,
        contractChangePct: -1.39, usdChangePct: -4.36
      }
    });

    expect(result.label).toBe('IQ WEAK');
    expect(result.score).toBe(0);
    expect(result.dom.window?.coverageMinutes).toBe(10);
    expect(result.coinGlass.directionalRatio).toBe(100_000_000);
  });

  test('reports a data gap when causal OI is unavailable', () => {
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long', alertTimestamp: '2026-08-19 20:00:00', gapWidth: 5,
      review, domRecords: []
    });
    expect(result.label).toBe('IQ DATA GAP');
    expect(result.availableSignals).toBe(0);
  });

  test('rejects an OI window extending past the SMTP cutoff', () => {
    const result = evaluateIntrusionImpulseQuality({
      direction: 'long', alertTimestamp: '2026-08-19 20:00:00', gapWidth: 5,
      review, domRecords: [],
      openInterest: {
        source: 'binance-futures-open-interest', symbol: 'BTCUSDT',
        from: '2026-08-19T20:00:00.000Z', to: '2026-08-19T20:21:00.000Z',
        fetchedAt: '2026-08-19T20:21:00.000Z', samples: 5,
        contractChangePct: -5, usdChangePct: -5
      }
    });
    expect(result.label).toBe('IQ DATA GAP');
  });
});
