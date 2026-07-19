import {
  calculateBookChanges,
  calculateBookMetrics,
  DomLevel
} from '../src/services/decentralizedDomCollector';

describe('decentralized DOM collector metrics', () => {
  const bids: DomLevel[] = [
    { price: 99.9, size: 10 },
    { price: 99.5, size: 20 },
    { price: 99, size: 30 }
  ];
  const asks: DomLevel[] = [
    { price: 100.1, size: 5 },
    { price: 100.5, size: 10 },
    { price: 101, size: 15 }
  ];

  test('calculates depth bands, spread, imbalance and microprice', () => {
    const metrics = calculateBookMetrics(bids, asks);

    expect(metrics?.mid).toBe(100);
    expect(metrics?.spreadBps).toBeCloseTo(20, 4);
    expect(metrics?.bidDepthUsd['10']).toBeCloseTo(999, 2);
    expect(metrics?.askDepthUsd['10']).toBeCloseTo(500.5, 2);
    expect(metrics?.imbalance['10']).toBeGreaterThan(0);
    expect(metrics?.micropriceOffsetBps).toBeGreaterThan(0);
  });

  test('separates visible additions from visible removals by side', () => {
    const changes = calculateBookChanges(
      {
        bids: [
          { price: 100, size: 2 },
          { price: 99, size: 3 }
        ],
        asks: [
          { price: 101, size: 4 },
          { price: 102, size: 2 }
        ]
      },
      {
        bids: [
          { price: 100, size: 3 },
          { price: 99, size: 1 }
        ],
        asks: [
          { price: 101, size: 2 },
          { price: 103, size: 1 }
        ]
      }
    );

    expect(changes.bidAddedUsd).toBe(100);
    expect(changes.bidRemovedUsd).toBe(198);
    expect(changes.askAddedUsd).toBe(103);
    expect(changes.askRemovedUsd).toBe(406);
  });

  test('limits comparable visible book changes to the configured mid-price band', () => {
    const changes = calculateBookChanges(
      {
        bids: [
          { price: 99.9, size: 1 },
          { price: 90, size: 100 }
        ],
        asks: [
          { price: 100.1, size: 1 },
          { price: 110, size: 100 }
        ]
      },
      {
        bids: [
          { price: 99.9, size: 2 },
          { price: 90, size: 200 }
        ],
        asks: [
          { price: 100.1, size: 0.5 },
          { price: 110, size: 200 }
        ]
      },
      100,
      25
    );

    expect(changes.bidAddedUsd).toBe(99.9);
    expect(changes.bidRemovedUsd).toBe(0);
    expect(changes.askAddedUsd).toBe(0);
    expect(changes.askRemovedUsd).toBe(50.05);
  });

  test('rejects a one-sided or crossed book', () => {
    expect(calculateBookMetrics(bids, [])).toBeUndefined();
    expect(calculateBookMetrics([{ price: 101, size: 1 }], [{ price: 100, size: 1 }]))
      .toBeUndefined();
  });
});
