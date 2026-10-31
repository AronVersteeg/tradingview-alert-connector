(function (root) {
  'use strict';
  function checkedDeltaRecord(record) {
    if (!record || !Number.isSafeInteger(record.hour) || record.hour % 3600 || !['ok', 'pending', 'unavailable'].includes(record.status)) {
      throw new Error('Invalid Futures delta record');
    }
    if (record.status !== 'ok') {
      if (!Number.isFinite(Date.parse(record.retryAt))) throw new Error('Invalid Futures delta retry time');
      return { hour: record.hour, status: record.status, retryAt: record.retryAt };
    }
    const c = record.candle;
    if (!c || c.hour !== record.hour || c.startedAt !== record.hour + 3300 || c.closedAt !== record.hour + 3600 ||
      !Number.isFinite(c.volumeBase) || c.volumeBase <= 0 || !Number.isFinite(c.takerBuyVolumeBase) ||
      c.takerBuyVolumeBase < 0 || c.takerBuyVolumeBase > c.volumeBase || !Number.isFinite(c.rawDeltaPct) ||
      Math.abs(c.rawDeltaPct - (2 * c.takerBuyVolumeBase / c.volumeBase - 1) * 100) > 1e-8) throw new Error('Invalid Futures delta candle');
    return { hour: record.hour, status: 'ok', candle: { ...c } };
  }
  function readDeltaResponse(payload, symbol = 'BTCUSDT') {
    if (!['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].includes(symbol) || payload?.symbol !== symbol || payload.venue !== 'Binance USD-M Futures' || payload.interval !== '5m' ||
      payload.timeBasis !== 'signal-last-five-minutes' || !Array.isArray(payload.records) || payload.records.length > 8) {
      throw new Error('Unexpected Futures delta source');
    }
    return payload.records.map(checkedDeltaRecord);
  }
  function deltaLabel(signal, record) {
    if (!['long', 'short'].includes(signal.direction) || signal.closedAt !== signal.time + 3600) throw new Error('Invalid signal candle');
    if (!record || record.hour !== signal.time || record.status !== 'ok') return {
      text: record?.status === 'unavailable' ? 'F5m n/a' : 'F5m ...', status: record?.status || 'pending', directionalDeltaPct: null };
    const checked = checkedDeltaRecord(record);
    const value = (signal.direction === 'long' ? 1 : -1) * checked.candle.rawDeltaPct;
    const rounded = Math.abs(value) < .005 ? 0 : value;
    return { text: `F5m ${rounded >= 0 ? '+' : ''}${rounded.toFixed(2)}%`, status: 'ok', directionalDeltaPct: value };
  }
  const api = Object.freeze({ checkedDeltaRecord, readDeltaResponse, deltaLabel });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcIntrusionFuturesDelta = api;
})(typeof globalThis === 'object' ? globalThis : this);
