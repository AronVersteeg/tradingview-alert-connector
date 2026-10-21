(function (root) {
  'use strict';
  const HOUR = 3600;

  function buildHourlyFractalBoxes(input, nowMs = Date.now()) {
    const candles = input.filter((row) => row.closed === true && (row.time + HOUR) * 1000 <= nowMs)
      .slice().sort((a, b) => a.time - b.time);
    const fractals = [], boxes = [];
    let high, low, current;
    for (let index = 0; index < candles.length; index++) {
      const candle = candles[index];
      if (index > 0 && candle.time !== candles[index - 1].time + HOUR) {
        if (current) { current.toTime = candles[index - 1].time + HOUR; current.openEnded = false; }
        high = low = current = undefined;
      }
      if (index < 4 || candle.time !== candles[index - 4].time + 4 * HOUR) continue;
      const pivot = candles[index - 2];
      // Same tie rules as confirmedWilliamsFractals: newest equal pivot wins.
      const isHigh = pivot.high >= candles[index - 4].high && pivot.high >= candles[index - 3].high &&
        pivot.high > candles[index - 1].high && pivot.high > candle.high;
      const isLow = pivot.low <= candles[index - 4].low && pivot.low <= candles[index - 3].low &&
        pivot.low < candles[index - 1].low && pivot.low < candle.low;
      const confirmedAt = candle.time + HOUR;
      if (isHigh) { high = pivot.high; fractals.push({ type: 'HIGH', price: high, pivotTime: pivot.time, confirmedAt }); }
      if (isLow) { low = pivot.low; fractals.push({ type: 'LOW', price: low, pivotTime: pivot.time, confirmedAt }); }
      if ((!isHigh && !isLow) || high === undefined || low === undefined) continue;
      if (current && current.high === high && current.low === low) continue;
      if (current) { current.toTime = pivot.time; current.openEnded = false; }
      current = { fromTime: pivot.time, toTime: confirmedAt, confirmedAt, high, low, openEnded: true };
      boxes.push(current);
    }
    // The last confirmed range covers the live candle, without using its high/low.
    if (current) current.toTime = Math.max(current.fromTime, ...input.map((row) => row.time + HOUR));
    return { boxes, fractals, evaluatedThrough: candles.length ? candles[candles.length - 1].time + HOUR : null };
  }

  class HourlyFractalBoxPrimitive {
    constructor() {
      this.boxes = []; this.indices = new Map(); this.visible = true;
      this.view = { zOrder: () => 'bottom', renderer: () => ({ draw: (target) => this.draw(target) }) };
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.series = this.requestUpdate = undefined; }
    updateAllViews() {}
    paneViews() { return [this.view]; }
    setData(boxes, candles) {
      this.boxes = boxes;
      this.indices = new Map();
      candles.forEach((candle, index) => {
        this.indices.set(candle.time, index);
        this.indices.set(candle.time + HOUR, index + 1);
      });
      this.requestUpdate?.();
    }
    setVisible(visible) { this.visible = visible; this.requestUpdate?.(); }
    draw(target) {
      if (!this.visible || !this.chart || !this.series) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        ctx.fillStyle = 'rgba(210, 214, 224, 0.26)';
        for (const box of this.boxes) {
          const from = this.indices.get(box.fromTime), to = this.indices.get(box.toTime);
          if (from === undefined || to === undefined) continue;
          const left = this.chart.timeScale().logicalToCoordinate(from);
          const right = this.chart.timeScale().logicalToCoordinate(to);
          const a = this.series.priceToCoordinate(box.high), b = this.series.priceToCoordinate(box.low);
          if (left === null || right === null || a === null || b === null || right < 0 || left > width) continue;
          const x = Math.max(0, left), end = Math.min(width, right);
          ctx.fillRect(x, Math.min(a, b), Math.max(0, end - x), Math.abs(b - a));
        }
        ctx.restore();
      });
    }
  }

  const api = Object.freeze({ buildHourlyFractalBoxes, HourlyFractalBoxPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcHourlyFractalBoxes = api;
})(typeof globalThis === 'object' ? globalThis : this);
