(function (root) {
  'use strict';
  const HOUR = 3600;
  const KNOWN_STARTS = Object.freeze([Object.freeze({
    lower: 79400, upper: 87400,
    fromTime: Date.parse('2026-09-21T08:00:00Z') / 1000,
    // User-supplied 10:00 Europe/Amsterdam; retained frames verify continuity
    // from Sep 30 through this observed cutoff, not the earlier birth itself.
    verifiedThrough: Date.parse('2026-10-21T08:00:00Z') / 1000
  })]);

  function timelineFromPayload(payload) {
    if (payload?.source?.market !== 'BTC-USD' ||
      !payload.source.sourceStatuses?.some((source) => source.name === 'Binance Spot BTCUSDT 1H') ||
      !Array.isArray(payload.frames) || !payload.frames.length || !Array.isArray(payload.gaps) || payload.frames.length !== payload.gaps.length) {
      throw new Error('Unexpected BTC Public V2 gap source');
    }
    let previous = 0;
    const records = payload.frames.map((frame, index) => {
      const candleTime = frame.startedAtMs / 1000;
      if (!Number.isSafeInteger(candleTime) || candleTime % HOUR || candleTime <= previous) throw new Error('Invalid gap frame timestamp');
      previous = candleTime;
      const gap = payload.gaps[index];
      if (gap !== null && (!gap || typeof gap.left !== 'number' || typeof gap.right !== 'number' ||
        !Number.isFinite(gap.left) || !Number.isFinite(gap.right) || gap.left <= 0 || gap.right <= gap.left)) throw new Error('Invalid gap edges');
      return { candleTime, availableAt: candleTime + HOUR, lower: gap?.left ?? null, upper: gap?.right ?? null };
    });
    return { symbol: 'BTCUSDT', venue: 'Binance Spot', modelVersion: payload.modelVersion, records };
  }

  function buildGapBoxes(timeline, nowMs = Date.now()) {
    if (timeline?.symbol !== 'BTCUSDT' || timeline.venue !== 'Binance Spot' || !Array.isArray(timeline.records)) throw new Error('Unexpected gap timeline');
    const cutoff = Math.floor(nowMs / (HOUR * 1000)) * HOUR;
    const rows = timeline.records.filter((row) => row.availableAt <= cutoff);
    if (!rows.length) throw new Error('Awaiting closed gap frames');
    const boxes = [];
    let current, previous;
    for (const row of rows) {
      if (previous && row.availableAt !== previous.availableAt + HOUR) {
        if (current) { current.toTime = previous.availableAt + HOUR; current.openEnded = false; }
        current = undefined;
      }
      const contiguous = previous && row.availableAt === previous.availableAt + HOUR;
      if (row.lower === null || row.upper === null) {
        if (current) { current.toTime = row.availableAt; current.openEnded = false; }
        current = undefined;
      } else if (!current || current.lower !== row.lower || current.upper !== row.upper) {
        if (current) { current.toTime = row.availableAt; current.openEnded = false; }
        current = { fromTime: row.availableAt, toTime: row.availableAt, candleTime: row.candleTime,
          lower: row.lower, upper: row.upper, startKnown: Boolean(contiguous), startSource: contiguous ? 'histomap' : 'unknown', openEnded: true };
        if (!boxes.length && !previous) {
          const anchor = KNOWN_STARTS.find((item) => item.lower === current.lower && item.upper === current.upper &&
            item.fromTime <= current.fromTime && current.fromTime <= item.verifiedThrough);
          if (anchor) { current.fromTime = anchor.fromTime; current.startKnown = true; current.startSource = 'user-confirmed'; }
        }
        boxes.push(current);
      }
      previous = row;
    }
    if (current) current.toTime = previous.availableAt;
    return { boxes, evaluatedThrough: previous.availableAt, coverageFrom: rows[0].availableAt };
  }

  class GapBoxPrimitive {
    constructor() {
      this.model = null; this.indices = new Map(); this.visible = true; this.lead = 3; this.lastIndex = 0;
      this.view = { zOrder: () => 'normal', renderer: () => ({ draw: (target) => this.draw(target) }) };
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.series = this.requestUpdate = undefined; }
    updateAllViews() {}
    paneViews() { return [this.view]; }
    setData(model, candles) {
      this.model = model; this.indices = new Map();
      candles.forEach((candle, index) => { this.indices.set(candle.time, index); this.indices.set(candle.time + HOUR, index + 1); });
      this.firstTime = candles[0]?.time; this.lastIndex = candles.length - 1;
      this.requestUpdate?.();
    }
    setOptions(options) {
      if (typeof options.visible === 'boolean') this.visible = options.visible;
      if (Number.isInteger(options.lead) && options.lead >= 1 && options.lead <= 72) this.lead = options.lead;
      this.requestUpdate?.();
    }
    draw(target) {
      if (!this.visible || !this.model || !this.chart || !this.series || this.firstTime === undefined) return;
      const stale = this.model.evaluatedThrough < Math.floor(Date.now() / (HOUR * 1000)) * HOUR;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        ctx.strokeStyle = '#9edfa5'; ctx.lineWidth = 1.5;
        for (const box of this.model.boxes) {
          const clippedStart = box.fromTime < this.firstTime;
          const from = this.indices.get(box.fromTime), to = this.indices.get(box.toTime);
          const left = clippedStart ? 0 : from === undefined ? null : this.chart.timeScale().logicalToCoordinate(from);
          const right = box.openEnded && !stale ? this.chart.timeScale().logicalToCoordinate(this.lastIndex + this.lead)
            : to === undefined ? null : this.chart.timeScale().logicalToCoordinate(to);
          const a = this.series.priceToCoordinate(box.upper), b = this.series.priceToCoordinate(box.lower);
          if (left === null || right === null || a === null || b === null || right < 0 || left > width) continue;
          ctx.setLineDash(box.openEnded && stale ? [5, 4] : []);
          ctx.beginPath(); ctx.moveTo(left, a); ctx.lineTo(right, a); ctx.lineTo(right, b); ctx.lineTo(left, b);
          // The first retained frame does not prove the original gap's birth.
          if (box.startKnown && !clippedStart) ctx.lineTo(left, a);
          ctx.stroke();
        }
        ctx.restore();
      });
    }
  }

  const api = Object.freeze({ timelineFromPayload, buildGapBoxes, GapBoxPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.BtcSpotGapBoxes = api;
    root.dispatchEvent(new CustomEvent('btc-spot-chart:gap-source-ready'));
  }
})(typeof globalThis === 'object' ? globalThis : this);
