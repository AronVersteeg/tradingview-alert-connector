(function (root) {
  'use strict';
  const HOUR = 3600;
  const CUTOFF = Date.UTC(2026, 8, 21) / 1000;
  const MODEL_VERSIONS = Object.freeze({
    BTCUSDT: 'binance-spot-liquidation-cohorts-v2.4',
    ETHUSDT: 'binance-spot-eth-liquidation-cohorts-v2.1',
    SOLUSDT: 'binance-spot-sol-liquidation-cohorts-v2.1',
    ZECUSDT: 'binance-spot-zec-liquidation-cohorts-v2.1'
  });

  function readReplay(payload, kind = 'purple', symbol = 'BTCUSDT') {
    if (!['purple', 'green'].includes(kind) || !Object.hasOwn(MODEL_VERSIONS, symbol) || (kind === 'purple' && symbol !== 'BTCUSDT')) {
      throw new Error('Unknown Spot replay kind or symbol');
    }
    const source = `binance-spot-${kind}-reconstruction`;
    const before = payload?.before;
    if (payload?.source !== source || payload.symbol !== symbol || payload.modelVersion !== MODEL_VERSIONS[symbol] ||
        !Number.isSafeInteger(before) || before <= 0 || before % HOUR || (symbol === 'BTCUSDT' && before !== CUTOFF) ||
        (kind === 'green' && payload.positionAtCutoff !== 'flat') ||
        !Array.isArray(payload.signals) || !payload.signals.length || payload.signals.length > 1000) {
      throw new Error('Unexpected Spot historical reconstruction');
    }
    let previous = 0;
    const signals = payload.signals.map(([time, direction, close]) => {
      if (!Number.isSafeInteger(time) || time % HOUR || time <= previous || time + HOUR > before ||
          !['long', 'short'].includes(direction) || !Number.isFinite(close) || close <= 0) {
        throw new Error('Invalid Spot historical reconstruction signal');
      }
      previous = time;
      return { time, closedAt: time + HOUR, direction, close, source };
    });
    return { signals, first: signals[0].time, before, market: symbol.replace('USDT', '-USD') };
  }

  function loadedReplaySignals(replay, candles, recorded, market) {
    if (!replay || market !== replay.market) return [];
    const closed = new Map(candles.filter(c => c.closed === true).map(c => [c.time, c.close]));
    const observed = new Set(recorded.map(signal => signal.time));
    return replay.signals.filter(signal => !observed.has(signal.time) && closed.get(signal.time) === signal.close);
  }

  const api = Object.freeze({ readReplay, loadedReplaySignals });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotHistoricalReplay = api;
})(typeof globalThis === 'object' ? globalThis : this);
