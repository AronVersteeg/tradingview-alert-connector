(function (root) {
  'use strict';
  const HOUR = 3600;
  const CUTOFF = Date.UTC(2026, 8, 21) / 1000;

  function readReplay(payload, kind = 'purple') {
    if (!['purple', 'green'].includes(kind)) throw new Error('Unknown BTC replay kind');
    const source = `binance-spot-${kind}-reconstruction`;
    if (payload?.source !== source || payload.symbol !== 'BTCUSDT' ||
        payload.modelVersion !== 'binance-spot-liquidation-cohorts-v2.4' || payload.before !== CUTOFF ||
        (kind === 'green' && payload.positionAtCutoff !== 'flat') ||
        !Array.isArray(payload.signals) || !payload.signals.length || payload.signals.length > 1000) {
      throw new Error('Unexpected BTC historical reconstruction');
    }
    let previous = 0;
    const signals = payload.signals.map(([time, direction, close]) => {
      if (!Number.isSafeInteger(time) || time % HOUR || time <= previous || time + HOUR > CUTOFF ||
          !['long', 'short'].includes(direction) || !Number.isFinite(close) || close <= 0) {
        throw new Error('Invalid BTC historical reconstruction signal');
      }
      previous = time;
      return { time, closedAt: time + HOUR, direction, close, source };
    });
    return { signals, first: signals[0].time, before: CUTOFF };
  }

  function loadedReplaySignals(replay, candles, recorded, market) {
    if (!replay || market !== 'BTC-USD') return [];
    const closed = new Map(candles.filter(c => c.closed === true).map(c => [c.time, c.close]));
    const observed = new Set(recorded.map(signal => signal.time));
    return replay.signals.filter(signal => !observed.has(signal.time) && closed.get(signal.time) === signal.close);
  }

  const api = Object.freeze({ readReplay, loadedReplaySignals });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotHistoricalReplay = api;
})(typeof globalThis === 'object' ? globalThis : this);
