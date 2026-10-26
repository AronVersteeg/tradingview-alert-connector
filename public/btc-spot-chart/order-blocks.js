// Adapted from Order Block Detector, copyright LuxAlgo.
// https://www.luxalgo.com/library/indicator/order-block-detector/
// CC BY-NC-SA 4.0: https://creativecommons.org/licenses/by-nc-sa/4.0/
// Changes: closed-candle JS replay, retained mitigations, coverage metadata and chart primitive.
(function (root) {
  'use strict';
  const HOUR = 3600;
  const COLORS = { bullish: '#169400', bearish: '#ff1100' };

  function buildOrderBlocks(input, options = {}, nowMs = Date.now()) {
    const length = options.length ?? 5, mitigation = options.mitigation ?? 'Wick';
    if (!Number.isInteger(length) || length < 1 || length > 100 || !['Wick', 'Close'].includes(mitigation)) {
      throw new Error('Invalid order block settings');
    }
    const candles = input.filter(c => c.closed === true && (c.time + HOUR) * 1000 <= nowMs)
      .slice().sort((a, b) => a.time - b.time);
    const blocks = [], active = new Set();
    let segmentStart = 0, bias, gaps = 0;
    for (let i = 0; i < candles.length; i++) {
      const c = candles[i], previous = candles[i - 1];
      if (!Number.isSafeInteger(c.time) || c.time % HOUR || previous && c.time <= previous.time ||
        ![c.open, c.high, c.low, c.close, c.volume].every(Number.isFinite) || c.volume < 0 ||
        Math.min(c.open, c.high, c.low, c.close) <= 0 || c.high < Math.max(c.open, c.close) || c.low > Math.min(c.open, c.close)) {
        throw new Error('Invalid Spot order block candle');
      }
      if (previous && c.time !== previous.time + HOUR) {
        for (const block of active) {
          block.state = 'coverage-gap'; block.coverageEndedAt = previous.time + HOUR;
        }
        active.clear(); segmentStart = i; bias = undefined; gaps++;
      }
      if (i - segmentStart < length) continue;
      const right = candles.slice(i - length + 1, i + 1), pivot = candles[i - length];
      const upper = Math.max(...right.map(row => row.high)), lower = Math.min(...right.map(row => row.low));
      // Pine os: the high branch has priority, otherwise retain the preceding state.
      bias = pivot.high > upper ? 'bearish' : pivot.low < lower ? 'bullish' : bias;
      if (i - segmentStart >= 2 * length && pivot.volume > 0 && bias) {
        const left = candles.slice(i - 2 * length, i - length);
        // ta.pivothigh accepts older ties, but a newer equal volume wins the pivot.
        const volumePeak = left.every(row => row.volume <= pivot.volume) && right.every(row => row.volume < pivot.volume);
        if (volumePeak) {
          const median = (pivot.high + pivot.low) / 2;
          const top = bias === 'bullish' ? median : pivot.high, bottom = bias === 'bullish' ? pivot.low : median;
          const block = { id: `${pivot.time}|${bias}`, direction: bias, top, bottom, average: (top + bottom) / 2,
            volume: pivot.volume, originTime: pivot.time, confirmedAt: c.time + HOUR,
            state: 'intact', brokenTime: null, brokenAt: null };
          blocks.push(block); active.add(block);
        }
      }
      const targetBull = mitigation === 'Close' ? Math.min(...right.map(row => row.close)) : lower;
      const targetBear = mitigation === 'Close' ? Math.max(...right.map(row => row.close)) : upper;
      for (const block of active) {
        if (block.direction === 'bullish' ? targetBull < block.bottom : targetBear > block.top) {
          block.state = 'broken'; block.brokenTime = c.time; block.brokenAt = c.time + HOUR;
          active.delete(block);
        }
      }
    }
    return { symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h', length, mitigation, blocks, gaps,
      candleCount: candles.length, historyFrom: candles[0]?.time ?? null,
      evaluatedThrough: candles.at(-1)?.time + HOUR || null,
      confirmationDelayCandles: length, brokenOpacityMultiplier: .5 };
  }

  class OrderBlockPrimitive {
    constructor() {
      this.blocks = []; this.indices = new Map(); this.visible = true;
      this.view = { zOrder: () => 'bottom', renderer: () => ({ draw: target => this.draw(target) }) };
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.series = this.requestUpdate = undefined; }
    paneViews() { return [this.view]; }
    updateAllViews() {}
    setVisible(visible) { this.visible = Boolean(visible); this.requestUpdate?.(); }
    setData(model, candles) {
      this.blocks = model.blocks; this.indices = new Map();
      candles.forEach((c, i) => { this.indices.set(c.time, i); this.indices.set(c.time + HOUR, i + 1); });
      this.requestUpdate?.();
    }
    draw(target) {
      if (!this.visible || !this.chart || !this.series) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        for (const block of this.blocks) {
          const from = this.indices.get(block.originTime);
          const endTime = block.state === 'broken' ? block.brokenAt : block.coverageEndedAt;
          const to = endTime ? this.indices.get(endTime) : undefined;
          const start = from === undefined ? null : this.chart.timeScale().logicalToCoordinate(from);
          const end = endTime ? to === undefined ? null : this.chart.timeScale().logicalToCoordinate(to) : width;
          const top = this.series.priceToCoordinate(block.top), bottom = this.series.priceToCoordinate(block.bottom);
          if (start === null || end === null || top === null || bottom === null || end <= 0 || start >= width ||
            Math.max(top, bottom) < 0 || Math.min(top, bottom) > height) continue;
          const x = Math.max(0, start), right = Math.min(width, end), y = Math.min(top, bottom), h = Math.abs(bottom - top);
          if (right <= x || !h) continue;
          const strength = block.state === 'intact' ? 1 : .5;
          ctx.fillStyle = COLORS[block.direction]; ctx.globalAlpha = .2 * strength;
          ctx.fillRect(x, y, right - x, h);
          ctx.strokeStyle = COLORS[block.direction]; ctx.lineWidth = 1; ctx.globalAlpha = .3 * strength;
          ctx.setLineDash(block.state === 'intact' ? [] : [3, 3]); ctx.strokeRect(x, y, right - x, h);
          const avg = this.series.priceToCoordinate(block.average);
          if (avg !== null) {
            ctx.strokeStyle = '#9598a1'; ctx.globalAlpha = .63 * strength; ctx.setLineDash([]);
            ctx.beginPath(); ctx.moveTo(x, avg); ctx.lineTo(right, avg); ctx.stroke();
          }
        }
        ctx.restore();
      });
    }
  }
  const api = Object.freeze({ buildOrderBlocks, OrderBlockPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotOrderBlocks = api;
})(typeof globalThis === 'object' ? globalThis : this);
