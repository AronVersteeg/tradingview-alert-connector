(function (root) {
  'use strict';
  const HOUR = 3600;

  function normalizeCandle(value) {
    if (!value || !Number.isSafeInteger(value.time) || value.time <= 0 || value.time % HOUR !== 0) return null;
    const numbers = ['open', 'high', 'low', 'close', 'volume', 'quoteVolume'];
    if (numbers.some((key) => typeof value[key] !== 'number' || !Number.isFinite(value[key]))) return null;
    if (Math.min(value.open, value.high, value.low, value.close) <= 0 ||
      value.high < Math.max(value.open, value.close) || value.low > Math.min(value.open, value.close) ||
      value.high < value.low || value.volume < 0 || value.quoteVolume < 0) return null;
    return { ...value, version: Number(value.version) || 0 };
  }

  function streamCandle(message, symbol = 'BTCUSDT') {
    const k = message && message.k;
    if (!['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].includes(symbol) || !k || message.e !== 'kline' || message.s !== symbol || k.s !== symbol ||
      k.i !== '1h' || typeof k.x !== 'boolean' || !Number.isSafeInteger(k.t) ||
      k.T !== k.t + HOUR * 1000 - 1 || !Number.isFinite(message.E)) return null;
    return normalizeCandle({
      time: k.t / 1000, closeTime: k.T,
      open: Number(k.o), high: Number(k.h), low: Number(k.l), close: Number(k.c),
      volume: Number(k.v), quoteVolume: Number(k.q), closed: k.x, version: message.E
    });
  }

  function mergeCandles(existing, incoming, limit = 12000) {
    const values = new Map();
    for (const candidate of [...existing, ...incoming]) {
      const candle = normalizeCandle(candidate);
      if (!candle) continue;
      const previous = values.get(candle.time);
      if (!previous || candle.version >= previous.version) values.set(candle.time, candle);
    }
    return [...values.values()].sort((a, b) => a.time - b.time).slice(-limit);
  }

  const api = Object.freeze({ normalizeCandle, streamCandle, mergeCandles });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotChartData = api;
})(typeof globalThis === 'object' ? globalThis : this);
