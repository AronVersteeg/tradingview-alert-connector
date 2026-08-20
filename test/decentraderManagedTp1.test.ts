import {
  AlertState,
  buildDecentraderDynamicTpAlert,
  stabilizeManagedTakeProfits
} from '../src/services/decentraderGapMonitor';

function managedPosition(takeProfits?: any[]): NonNullable<AlertState['managedPosition']> {
  return {
    market: 'BTC-USD',
    direction: 'long',
    openedAt: '2026-08-20T00:00:00.000Z',
    entrySignature: 'entry-1',
    initialSize: 1,
    takeProfits
  };
}

describe('managed TP1 lifecycle', () => {
  test('keeps entry TP1 fixed while TP2+ remain dynamic', () => {
    const managed = managedPosition([
      { label: 'L TP1', price: 110, size: 0.2 },
      { label: 'L TP2', price: 120, size: 0.8 }
    ]);

    const result = stabilizeManagedTakeProfits(managed, [
      { label: 'L TP1', price: 105, size: 0.3 },
      { label: 'L TP2', price: 125, size: 0.3 },
      { label: 'L TP3', price: 140, size: 0.4 }
    ], 1, '2026-08-20T01:00:00.000Z');

    expect(result.consumedNow).toBe(false);
    expect(result.takeProfits[0]).toMatchObject({ label: 'L TP1', price: 110, size: 0.2 });
    expect(result.takeProfits.slice(1).map((level) => level.price)).toEqual([125, 140]);
    expect(result.takeProfits.reduce((total, level) => total + level.size, 0)).toBeCloseTo(1, 10);
  });

  test('marks TP1 consumed after the first position reduction and never recreates it', () => {
    const managed = managedPosition([
      { label: 'L TP1', price: 110, size: 0.2 },
      { label: 'L TP2', price: 120, size: 0.8 }
    ]);
    stabilizeManagedTakeProfits(managed, managed.takeProfits || [], 1, '2026-08-20T01:00:00.000Z');

    const consumed = stabilizeManagedTakeProfits(managed, [
      { label: 'L TP1', price: 115, size: 0.2 },
      { label: 'L TP2', price: 130, size: 0.3 },
      { label: 'L TP3', price: 145, size: 0.5 }
    ], 0.8, '2026-08-20T02:00:00.000Z');
    const later = stabilizeManagedTakeProfits(managed, [
      { label: 'L TP1', price: 118, size: 0.2 },
      { label: 'L TP2', price: 135, size: 0.3 },
      { label: 'L TP3', price: 150, size: 0.5 }
    ], 0.8, '2026-08-20T03:00:00.000Z');

    expect(consumed.consumedNow).toBe(true);
    expect(consumed.lifecycle?.consumedAt).toBe('2026-08-20T02:00:00.000Z');
    expect(consumed.takeProfits.map((level) => level.price)).toEqual([130, 145]);
    expect(later.consumedNow).toBe(false);
    expect(later.takeProfits.map((level) => level.price)).toEqual([135, 150]);
    expect(later.takeProfits.some((level) => level.label === 'L TP1')).toBe(false);
    expect(later.takeProfits.reduce((total, level) => total + level.size, 0)).toBeCloseTo(0.8, 10);
  });

  test('migrates an already reduced live position from its persisted entry decision', () => {
    const managed = managedPosition();
    const result = stabilizeManagedTakeProfits(
      managed,
      [
        { label: 'L TP1', price: 115, size: 0.2 },
        { label: 'L TP2', price: 130, size: 0.8 }
      ],
      0.8,
      '2026-08-20T02:00:00.000Z',
      [
        { label: 'L TP1', price: 110, size: 0.2 },
        { label: 'L TP2', price: 120, size: 0.8 }
      ]
    );

    expect(result.lifecycle?.lockedLevel.price).toBe(110);
    expect(result.lifecycle?.consumedAt).toBe('2026-08-20T02:00:00.000Z');
    expect(result.takeProfits).toHaveLength(1);
    expect(result.takeProfits[0]).toMatchObject({ label: 'L TP2', price: 130 });
    expect(result.takeProfits[0].size).toBeCloseTo(0.8, 10);
  });

  test('does not mistake TP2 for TP1 when minimum-size allocation omitted TP1', () => {
    const managed = managedPosition([
      { label: 'L TP2', price: 120, size: 0.5 },
      { label: 'L TP3', price: 140, size: 0.5 }
    ]);

    const result = stabilizeManagedTakeProfits(managed, [
      { label: 'L TP2', price: 125, size: 0.5 },
      { label: 'L TP3', price: 145, size: 0.5 }
    ], 1, '2026-08-20T01:00:00.000Z');

    expect(result.lifecycle).toBeUndefined();
    expect(result.takeProfits.map((level) => level.price)).toEqual([125, 145]);
  });

  test('restores zero-sized TP candidates after TP1 was consumed and covers the full position', () => {
    const managed = managedPosition([
      { label: 'L TP1', price: 69950, size: 0.0005 },
      { label: 'L TP2', price: 80000, size: 0.0003 }
    ]);
    managed.initialSize = 0.002;
    stabilizeManagedTakeProfits(
      managed,
      managed.takeProfits || [],
      0.002,
      '2026-08-20T01:00:00.000Z',
      [],
      0.0001
    );

    const result = stabilizeManagedTakeProfits(
      managed,
      [
        { label: 'L TP1', price: 69950, size: 0.0005 },
        { label: 'L TP2', price: 80000, size: 0 },
        { label: 'L TP3', price: 85950, size: 0 },
        { label: 'L TP4', price: 95950, size: 0.0005 },
        { label: 'L TP5', price: 99950, size: 0.0003 },
        { label: 'L TP6', price: 119950, size: 0.0001 }
      ],
      0.0013,
      '2026-08-20T02:00:00.000Z',
      [],
      0.0001
    );

    expect(result.consumedNow).toBe(true);
    expect(result.takeProfits.map((level) => level.price)).toEqual([
      80000, 85950, 95950, 99950, 119950
    ]);
    expect(result.takeProfits.every((level) => level.size >= 0.0001)).toBe(true);
    expect(result.takeProfits.reduce((total, level) => total + level.size, 0)).toBeCloseTo(0.0013, 10);
  });

  test('keeps all map candidates available until lifecycle reallocation', () => {
    const alert = buildDecentraderDynamicTpAlert({
      market: 'BTC-USD',
      price: 72000,
      marketInfo: { oraclePrice: 72000, stepSize: 0.0001 },
      plans: {
        long: {
          entryReference: { price: 72000 },
          sizing: { minimumOrderSize: 0.0001 },
          takeProfits: [
            { label: 'L TP1', price: 73000, selectionScore: 600 },
            { label: 'L TP2', price: 80000, selectionScore: 500 },
            { label: 'L TP3', price: 85950, selectionScore: 400 },
            { label: 'L TP4', price: 95950, selectionScore: 300 },
            { label: 'L TP5', price: 99950, selectionScore: 200 },
            { label: 'L TP6', price: 119950, selectionScore: 100 }
          ]
        }
      }
    }, {
      market: 'BTC-USD',
      side: 'LONG',
      size: 0.0003,
      entryPrice: 70000
    });

    const levels = (alert as any).take_profits;
    expect(levels).toHaveLength(6);
    expect(levels.some((level: any) => level.size === 0)).toBe(true);
  });
});
