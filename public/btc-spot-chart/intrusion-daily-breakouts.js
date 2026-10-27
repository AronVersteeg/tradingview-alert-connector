(function (root) {
  'use strict';
  const engine = typeof module === 'object' && module.exports ? require('./gap-weekly-breakouts.js') : root.BtcSpotGapWeeklyBreakouts;
  const delta = typeof module === 'object' && module.exports ? require('./futures-delta.js') : root.BtcIntrusionFuturesDelta;
  const SOURCE = 'decentrader-recorded-intrusions';

  function compactIntrusionHistory(payload, nowMs = Date.now(), market = 'BTC-USD') {
    const eth = market === 'ETH-USD';
    const validSource = eth ? payload?.source?.market === market &&
      payload.source.sourceStatuses?.some(source => source.name === 'Binance Spot ETHUSDT 1H')
      : market === 'BTC-USD' && payload?.source?.method === 'getOHLCHourlyCalculations' &&
        payload.source.url === 'https://www.decentrader.com/liquidity-maps/?coin=btc';
    if (!validSource) throw new Error('Unexpected recorded intrusion source');
    const histories = [eth ? payload.intrusionBenchmarks?.history : payload.intrusionDomStudy?.history, payload.delayHistory?.records].filter(Array.isArray);
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
    return { market, source: eth ? 'binance-spot-recorded-intrusions' : SOURCE, timeBasis: 'alert-candle-open-utc', fetchedAt: new Date(nowMs).toISOString(),
      ignoredRecords, records: [...records.values()].sort((a, b) => a.time - b.time || a.direction.localeCompare(b.direction)) };
  }

  function buildIntrusionBreakouts(candles, history, fractals, nowMs, market, timeframe) {
    const expectedSource = market === 'ETH-USD' ? 'binance-spot-recorded-intrusions' : SOURCE;
    if (!['BTC-USD', 'ETH-USD'].includes(market) || history?.market !== market || history.source !== expectedSource || history.timeBasis !== 'alert-candle-open-utc' ||
        !Array.isArray(history.records)) throw new Error('Unexpected Decentrader intrusion history');
    const records = new Map();
    for (const record of history.records) {
      if (!Number.isSafeInteger(record.time) || record.time % 3600 || !['long', 'short'].includes(record.direction) ||
          Date.parse(record.alertTime) !== record.time * 1000) throw new Error('Invalid intrusion alert time');
      records.set(`${record.time}|${record.direction}`, record);
    }
    const evaluate = timeframe === 'W' ? engine.buildWeeklyFractalBreakouts : engine.buildDailyFractalBreakouts;
    const model = evaluate(candles, fractals, nowMs, market === 'ETH-USD' ? 'ETHUSDT' : 'BTCUSDT');
    return { ...model, intrusionSource: expectedSource, timeBasis: history.timeBasis, signals: model.signals.flatMap(signal => {
      const record = records.get(`${signal.time}|${signal.direction}`);
      return record ? [{ ...signal, intrusion: { ...record } }] : [];
    }) };
  }

  function buildIntrusionDailyBreakouts(candles, history, fractals, nowMs = Date.now(), market = 'BTC-USD') {
    return buildIntrusionBreakouts(candles, history, fractals, nowMs, market, 'D');
  }

  function buildIntrusionWeeklyBreakouts(candles, history, fractals, nowMs = Date.now(), market = 'BTC-USD') {
    return buildIntrusionBreakouts(candles, history, fractals, nowMs, market, 'W');
  }

  class IntrusionDailyBreakoutPrimitive extends engine.GapWeeklyBreakoutPrimitive {
    constructor() { super('#ba68c8'); this.deltaEnabled = false; this.deltaRecords = new Map(); this.deltaLabels = []; this.fontSize = 11; }
    setDeltaEnabled(enabled) { this.deltaEnabled = Boolean(enabled); this.deltaLabels = []; this.requestUpdate?.(); }
    setDeltaRecords(records) { this.deltaRecords = new Map(records); this.requestUpdate?.(); }
    setLabelFontSize(size) { if (size >= 8 && size <= 16) this.fontSize = size; this.requestUpdate?.(); }
    setVisible(visible) { super.setVisible(visible); this.deltaLabels = []; }
    getDeltaLabels() { return this.deltaLabels.map(label => ({ ...label })); }
    draw(target) {
      super.draw(target); this.deltaLabels = [];
      if (!this.deltaEnabled || !this.visible || !this.chart) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        ctx.font = `${this.fontSize}px Arial`; ctx.textBaseline = 'top';
        const h = this.fontSize + 6, lanes = Array(Math.max(0, Math.min(7, Math.floor((height - 14) / (h + 3))))).fill(-1000);
        for (const signal of this.signals.slice().sort((a, b) => a.time - b.time)) {
          const anchor = this.chart.timeScale().timeToCoordinate(signal.time);
          if (!Number.isFinite(anchor) || anchor < 0 || anchor > width) continue;
          const value = delta.deltaLabel(signal, this.deltaRecords.get(signal.time));
          const w = ctx.measureText(value.text).width + 8;
          if (w + 6 > width) continue;
          const x = Math.max(3, Math.min(width - w - 3, anchor - w / 2));
          const lane = lanes.findIndex(right => x >= right + 4);
          if (lane < 0) continue;
          lanes[lane] = x + w;
          const y = 7 + lane * (h + 3);
          ctx.globalAlpha = .95; ctx.fillStyle = '#161a25'; ctx.fillRect(x, y, w, h);
          ctx.strokeStyle = '#865094'; ctx.lineWidth = 1; ctx.setLineDash([]); ctx.globalAlpha = .7; ctx.strokeRect(x, y, w, h);
          ctx.globalAlpha = 1; ctx.fillStyle = '#d8b4e6'; ctx.fillText(value.text, x + 4, y + 3);
          this.deltaLabels.push({ time: signal.time, direction: signal.direction, ...value, x, y, width: w, height: h });
        }
        ctx.restore();
      });
    }
  }

  const api = Object.freeze({ compactIntrusionHistory, buildIntrusionDailyBreakouts, buildIntrusionWeeklyBreakouts, IntrusionDailyBreakoutPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.BtcSpotIntrusionDailyBreakouts = api;
    root.dispatchEvent(new CustomEvent('btc-spot-chart:intrusion-source-ready'));
  }
})(typeof globalThis === 'object' ? globalThis : this);
