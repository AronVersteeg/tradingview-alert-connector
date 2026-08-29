import { classifyForwardHurdle } from '../src/services/intrusionForwardHurdle';

describe('intrusion forward hurdle scanner', () => {
  test('finds a close Binance ask wall ahead of a long entry', () => {
    const result = classifyForwardHurdle({
      symbol: 'BTCUSDT',
      direction: 'long',
      referencePrice: 100,
      review: { volumeDeltaQuote: [1_000, 500] },
      observedAt: '2026-08-28T12:00:00.000Z',
      depthPayload: {
        lastUpdateId: 123,
        asks: [
          ['100.10', '1'],
          ['100.20', '1'],
          ['100.30', '1'],
          ['100.40', '1'],
          ['100.50', '20']
        ]
      }
    });

    expect(result.status).toBe('CLOSE');
    expect(result.source).toBe('binance-futures-depth');
    expect(result.firstHurdle).toMatchObject({ side: 'ask', source: 'binance' });
    expect(result.firstHurdle?.price).toBeCloseTo(100.5);
    expect(result.directionalDelayFlowUsd).toBe(1_500);
    expect(result.flowToHurdleRatio).toBeGreaterThan(0);
  });

  test('uses a persistent CoinGlass bid wall ahead of a short entry', () => {
    const result = classifyForwardHurdle({
      symbol: 'ETHUSDT',
      direction: 'short',
      referencePrice: 100,
      review: { volumeDeltaQuote: [-2_000] },
      observedAt: '2026-08-28T12:00:00.000Z',
      coinGlassSnapshot: {
        fetchedAt: '2026-08-28T11:59:00.000Z',
        minUsd: 1_000,
        levels: [{ side: 'buy', price: 99, volumeUsd: 5_000, startedAt: Date.parse('2026-08-27T12:00:00.000Z') }]
      }
    });

    expect(result.status).toBe('AHEAD');
    expect(result.source).toBe('coinglass');
    expect(result.firstHurdle).toMatchObject({ side: 'bid', source: 'coinglass', effectiveUsd: 5_000 });
    expect(result.directionalDelayFlowUsd).toBe(2_000);
    expect(result.flowToHurdleRatio).toBeCloseTo(0.4);
  });

  test('rejects stale CoinGlass walls and keeps current Binance depth authoritative', () => {
    const result = classifyForwardHurdle({
      symbol: 'BTCUSDT',
      direction: 'long',
      referencePrice: 100,
      review: { volumeDeltaQuote: [1_000] },
      observedAt: '2026-08-28T12:00:00.000Z',
      depthPayload: {
        lastUpdateId: 456,
        asks: []
      },
      coinGlassSnapshot: {
        enabled: true,
        fetchedAt: '2026-08-28T10:00:00.000Z',
        minUsd: 1_000,
        levels: [{ side: 'sell', price: 100.5, volumeUsd: 50_000 }]
      }
    });

    expect(result.source).toBe('binance-futures-depth');
    expect(result.status).toBe('CLEAR');
    expect(result.headline).toBe('NO CLOSE HURDLE | CG STALE');
    expect(result.coinGlass).toMatchObject({ stale: true, levelsConsidered: 0 });
    expect(result.firstHurdle).toBeUndefined();
  });

  test('reports a data gap when stale CoinGlass is the only hurdle source', () => {
    const result = classifyForwardHurdle({
      symbol: 'ETHUSDT',
      direction: 'short',
      referencePrice: 100,
      observedAt: '2026-08-28T12:00:00.000Z',
      coinGlassSnapshot: {
        enabled: true,
        fetchedAt: '2026-08-28T11:00:00.000Z',
        minUsd: 1_000,
        levels: [{ side: 'buy', price: 99.5, volumeUsd: 50_000 }]
      }
    });

    expect(result.status).toBe('DATA_GAP');
    expect(result.source).toBe('none');
    expect(result.headline).toBe('HURDLE DATA GAP | CG STALE');
  });
});
