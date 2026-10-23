(function (root) {
  'use strict';
  const HOUR = 3600;

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
      if (previous && (row.breakBefore || row.availableAt !== previous.availableAt + HOUR)) {
        if (current) { current.toTime = previous.availableAt + HOUR; current.openEnded = false; }
        current = undefined;
      }
      const contiguous = previous && !row.breakBefore && row.availableAt === previous.availableAt + HOUR;
      if (row.lower === null || row.upper === null) {
        if (current) { current.toTime = row.availableAt; current.openEnded = false; }
        current = undefined;
      } else if (!current || current.lower !== row.lower || current.upper !== row.upper) {
        if (current) { current.toTime = row.availableAt; current.openEnded = false; }
        current = { fromTime: row.availableAt, toTime: row.availableAt, candleTime: row.candleTime,
          lower: row.lower, upper: row.upper, startKnown: Boolean(contiguous),
          startSource: contiguous ? (row.source === 'decentrader-map' ? 'decentrader-map' : 'histomap') : 'unknown', openEnded: true };
        boxes.push(current);
      }
      previous = row;
    }
    if (current) current.toTime = previous.availableAt;
    return { boxes, evaluatedThrough: previous.availableAt, coverageFrom: rows[0].availableAt,
      history: timeline.history ? { ...timeline.history } : undefined };
  }

  function withDecentraderHistory(live, history) {
    if (live?.symbol !== 'BTCUSDT' || live.venue !== 'Binance Spot' || !live.records?.length ||
        history?.symbol !== 'BTCUSDT' || history.venue !== 'Decentrader map' || history.historicalOnly !== true ||
        history.historicalAccuracy !== 'observed-snapshots' ||
        !Array.isArray(history.records) || !history.records.length) throw new Error('Unexpected Decentrader gap history');
    let previous = 0;
    for (const row of history.records) {
      if (!Number.isSafeInteger(row.candleTime) || row.candleTime % HOUR || row.candleTime <= previous ||
          row.availableAt !== row.candleTime + HOUR ||
          !(row.lower === null && row.upper === null || Number.isFinite(row.lower) && Number.isFinite(row.upper) &&
            row.lower > 0 && row.upper > row.lower)) throw new Error('Invalid Decentrader gap history');
      previous = row.candleTime;
    }
    const first = live.records[0], lookup = new Map(history.records.map((row) => [row.candleTime, row]));
    const sameEdges = (a, b) => a && b && a.lower === b.lower && a.upper === b.upper;
    const overlapping = live.records.filter((row) => lookup.has(row.candleTime));
    const mismatches = overlapping.filter((row) => !sameEdges(row, lookup.get(row.candleTime))).length;
    // A source handover may continue a box only if both sources agree at the
    // boundary. Public V2 always wins overlap; Decentrader never supplies live data.
    const boundaryMatches = sameEdges(first, lookup.get(first.candleTime));
    const older = history.records.filter((row) => row.candleTime < first.candleTime)
      .map((row) => ({ ...row, source: 'decentrader-map' }));
    const records = live.records.map((row, index) => ({ ...row, breakBefore: index === 0 && older.length > 0 && !boundaryMatches }));
    return { ...live, records: [...older, ...records], history: {
      source: 'Decentrader map', historicalOnly: true, records: older.length,
      coverageFrom: history.records[0].availableAt, coverageThrough: history.records.at(-1).availableAt,
      overlapCompared: overlapping.length, mismatches, boundaryMatches
    } };
  }

  class GapBoxPrimitive {
    constructor() {
      this.model = null; this.indices = new Map(); this.visible = true; this.lead = 10; this.lastIndex = 0;
      this.view = { zOrder: () => 'normal', renderer: () => ({ draw: (target) => this.draw(target) }) };
      this.axisViews = ['upper', 'lower'].map((edge) => ({
        coordinate: () => {
          const gap = this.model?.boxes.find((box) => box.openEnded);
          return gap ? this.series?.priceToCoordinate(gap[edge]) ?? -10000 : -10000;
        },
        text: () => {
          const gap = this.model?.boxes.find((box) => box.openEnded);
          return gap && this.series ? this.series.priceFormatter().format(gap[edge]) : '';
        },
        textColor: () => '#131722', backColor: () => '#00ff00', tickVisible: () => true,
        visible: () => Boolean(this.visible && this.series && this.model?.boxes.some((box) => box.openEnded) &&
          this.model.evaluatedThrough >= Math.floor(Date.now() / (HOUR * 1000)) * HOUR)
      }));
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.series = this.requestUpdate = undefined; }
    updateAllViews() {}
    paneViews() { return [this.view]; }
    priceAxisViews() { return this.axisViews; }
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
        ctx.strokeStyle = '#00ff00'; ctx.lineWidth = 1.5;
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

  const api = Object.freeze({ timelineFromPayload, withDecentraderHistory, buildGapBoxes, GapBoxPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.BtcSpotGapBoxes = api;
    root.dispatchEvent(new CustomEvent('btc-spot-chart:gap-source-ready'));
  }
})(typeof globalThis === 'object' ? globalThis : this);
