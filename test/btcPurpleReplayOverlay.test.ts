export {};
const { readReplay, loadedReplaySignals } = require('../public/btc-spot-chart/historical-replay.js');
const payload = require('../public/btc-spot-chart/btc-purple-replay.json');
const greenPayload = require('../public/btc-spot-chart/btc-green-replay.json');

describe('BTC Purple visual-only backfill', () => {
  test('loads the 144 reconstructed signals strictly before September 21', () => {
    const { signals, first } = readReplay(payload);
    expect(signals).toHaveLength(144);
    expect(new Date(first * 1000).toISOString()).toBe('2023-11-24T15:00:00.000Z');
    expect(signals.at(-1).closedAt).toBeLessThanOrEqual(Date.UTC(2026, 8, 21) / 1000);
    expect(signals[0].source).toBe('binance-spot-purple-reconstruction');
  });

  test('requires a matching closed Spot candle and gives saved markers priority', () => {
    const replay = readReplay(payload);
    const [first, second, third, fourth] = replay.signals;
    const candles = [{ ...first, closed: true }, { ...second, closed: false },
      { ...third, close: third.close + 1, closed: true }, { ...fourth, closed: true }];
    expect(loadedReplaySignals(replay, candles, [fourth], 'BTC-USD')).toEqual([first]);
    expect(loadedReplaySignals(replay, candles, [], 'ETH-USD')).toEqual([]);
  });

  test('rejects foreign data, duplicate hours, and data reaching beyond the historical cutoff', () => {
    expect(() => readReplay({ ...payload, symbol: 'ETHUSDT' })).toThrow();
    expect(() => readReplay({ ...payload, signals: [payload.signals[0], payload.signals[0]] })).toThrow();
    expect(() => readReplay({ ...payload, signals: [[payload.before, 'short', 80000]] })).toThrow();
  });
});

describe('BTC Green visual-only backfill', () => {
  test('loads the non-overlapping Spot positions and requires a flat handoff to recorded history', () => {
    const replay = readReplay(greenPayload, 'green');
    expect(greenPayload.candidateCount).toBe(406);
    expect(replay.signals).toHaveLength(181);
    expect(replay.before).toBe(Date.UTC(2026, 8, 21) / 1000);
    expect(replay.signals[0].source).toBe('binance-spot-green-reconstruction');
    expect(() => readReplay({...greenPayload, positionAtCutoff:'open'}, 'green')).toThrow();
    expect(() => readReplay(greenPayload, 'purple')).toThrow();
    expect(() => readReplay(payload, 'green')).toThrow();
    expect(loadedReplaySignals(replay, [{...replay.signals[0],closed:true}], [], 'BTC-USD')).toEqual([replay.signals[0]]);
  });
});
