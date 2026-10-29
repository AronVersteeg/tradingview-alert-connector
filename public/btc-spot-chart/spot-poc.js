(function (root) {
  'use strict';
  function nextPocHour(signals, records, workingHour, range, now = Date.now()) {
    const hours = [...new Set(signals.map(signal => signal.time))].sort((a, b) => b - a);
    const due = hour => {
      const record = records.get(hour);
      return !record || record.status !== 'ok' && Date.parse(record.retryAt) <= now;
    };
    // Finish a partial profile even when the user pans away; otherwise each pan
    // can abandon its pages and block the shared server job for two minutes.
    if (hours.includes(workingHour) && records.get(workingHour)?.status === 'pending') {
      return due(workingHour) ? workingHour : undefined;
    }
    const candidates = hours.filter(due);
    return candidates.find(hour => range && hour >= range.from && hour <= range.to) ?? candidates[0];
  }
  function readPocResponse(payload, symbol, hour, now = Date.now()) {
    if (!['BTCUSDT', 'ETHUSDT'].includes(symbol) || payload?.symbol !== symbol || payload.venue !== 'Binance Spot' ||
        payload.interval !== '1h' || payload.method !== 'exact-price-base-volume') throw new Error('Unexpected Spot POC source');
    const record = payload.record;
    if (record?.hour !== hour || !Number.isSafeInteger(hour) || hour % 3600 || (hour + 3600) * 1000 > now) throw new Error('Unexpected POC candle');
    if (record.status === 'ok') {
      const p = record.poc;
      if (!p || p.hour !== hour || p.startedAt !== hour || p.closedAt !== hour + 3600 ||
          !Number.isFinite(p.price) || p.price <= 0 || !Number.isFinite(Number(p.volumeBase)) || Number(p.volumeBase) <= 0 ||
          !Number.isFinite(Number(p.priceVolumeBase)) || Number(p.priceVolumeBase) <= 0 || Number(p.priceVolumeBase) > Number(p.volumeBase) ||
          ![p.tradeCount, p.aggregateTrades, p.priceLevels, p.pages].every(n => Number.isSafeInteger(n) && n > 0) ||
          p.aggregateTrades > p.tradeCount || p.priceLevels > p.aggregateTrades ||
          !Number.isFinite(Date.parse(p.fetchedAt)) || Date.parse(p.fetchedAt) < p.closedAt * 1000) throw new Error('Invalid Spot POC');
      return { hour, status: 'ok', poc: { ...p } };
    }
    if (!['pending', 'unavailable'].includes(record.status) || !Number.isFinite(Date.parse(record.retryAt))) throw new Error('Invalid POC availability');
    return { ...record };
  }
  const api = Object.freeze({ readPocResponse, nextPocHour });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcIntrusionSpotPoc = api;
})(typeof globalThis === 'object' ? globalThis : this);
