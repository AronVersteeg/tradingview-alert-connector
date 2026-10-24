(function (root) {
  'use strict';
  const engine = typeof module === 'object' && module.exports ? require('./gap-weekly-breakouts.js') : root.BtcSpotGapWeeklyBreakouts;
  const SOURCE = 'decentrader-recorded-intrusions';

  function compactIntrusionHistory(payload, nowMs = Date.now()) {
    if (payload?.source?.method !== 'getOHLCHourlyCalculations' ||
        payload.source.url !== 'https://www.decentrader.com/liquidity-maps/?coin=btc') throw new Error('Unexpected Decentrader intrusion source');
    const histories = [payload.intrusionDomStudy?.history, payload.delayHistory?.records].filter(Array.isArray);
    if (!histories.length) throw new Error('Decentrader intrusion history missing');
    const records = new Map();
    let ignoredRecords = 0;
    for (const row of histories.flat()) {
      const raw = row?.timestamp || row?.intrusionTimestamp;
      const timestamp = typeof raw === 'string' && /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/.test(raw) ? raw.replace(' ', 'T') + 'Z' : raw;
      const ms = typeof timestamp === 'string' && /Z$/.test(timestamp) ? Date.parse(timestamp) : NaN;
      const text = `${row?.sideCounts || ''} ${row?.signature || ''}`;
      const long = row?.direction === 'long' || /left edge|\|L\|/i.test(text);
      const short = row?.direction === 'short' || /right edge|\|S\|/i.test(text);
      if (!Number.isFinite(ms) || ms % 3_600_000 || ms < 0 || ms >= nowMs || long === short) { ignoredRecords++; continue; }
      const direction = long ? 'long' : 'short';
      records.set(`${ms}|${direction}`, { time: ms / 1000, alertTime: new Date(ms).toISOString(), direction });
    }
    return { market: 'BTC-USD', source: SOURCE, timeBasis: 'alert-candle-open-utc', fetchedAt: new Date(nowMs).toISOString(),
      ignoredRecords, records: [...records.values()].sort((a, b) => a.time - b.time || a.direction.localeCompare(b.direction)) };
  }

  function buildIntrusionDailyBreakouts(candles, history, fractals, nowMs = Date.now()) {
    if (history?.market !== 'BTC-USD' || history.source !== SOURCE || history.timeBasis !== 'alert-candle-open-utc' ||
        !Array.isArray(history.records)) throw new Error('Unexpected Decentrader intrusion history');
    const records = new Map();
    for (const record of history.records) {
      if (!Number.isSafeInteger(record.time) || record.time % 3600 || !['long', 'short'].includes(record.direction) ||
          Date.parse(record.alertTime) !== record.time * 1000) throw new Error('Invalid intrusion alert time');
      records.set(`${record.time}|${record.direction}`, record);
    }
    const model = engine.buildDailyFractalBreakouts(candles, fractals, nowMs);
    return { ...model, intrusionSource: SOURCE, timeBasis: history.timeBasis, signals: model.signals.flatMap(signal => {
      const record = records.get(`${signal.time}|${signal.direction}`);
      return record ? [{ ...signal, intrusion: { ...record } }] : [];
    }) };
  }

  class IntrusionDailyBreakoutPrimitive extends engine.GapWeeklyBreakoutPrimitive {
    constructor() { super('#ba68c8'); }
  }

  const api = Object.freeze({ compactIntrusionHistory, buildIntrusionDailyBreakouts, IntrusionDailyBreakoutPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotIntrusionDailyBreakouts = api;
})(typeof globalThis === 'object' ? globalThis : this);
