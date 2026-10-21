(function (root) {
  'use strict';
  const HOUR = 3600;

  function projectFractals(bundle, input) {
    if (bundle?.symbol !== 'BTCUSDT' || bundle.venue !== 'Binance Spot') throw new Error('Unexpected fractal source');
    const through = Date.parse(bundle.evaluatedThrough) / 1000;
    if (!Number.isSafeInteger(through) || through % HOUR !== 0) throw new Error('Invalid projection cutoff');
    const candles = input.filter((candle) => candle.closed === true && candle.time + HOUR <= through)
      .slice().sort((a, b) => a.time - b.time);
    if (!candles.length || candles[candles.length - 1].time + HOUR !== through) throw new Error('Awaiting latest closed 1H candle');
    const times = new Set(candles.map((candle) => candle.time));
    const first = candles[0].time;
    const levels = [];
    for (const [key, timeframe, period] of [['daily', 'D', 24 * HOUR], ['weekly', 'W', 168 * HOUR]]) {
      const snapshot = bundle[key];
      if (!snapshot || snapshot.venue !== 'Binance Spot' || snapshot.symbol !== 'BTCUSDT' || snapshot.stale ||
        !Array.isArray(snapshot.records)) throw new Error('Fractal source is stale or invalid');
      const coverageStart = (Date.parse(snapshot.latestClosedCandleAt) + 1) / 1000;
      if (!Number.isSafeInteger(coverageStart) || coverageStart > through) throw new Error('Invalid fractal coverage');
      // Only the unfinished Daily/Weekly period needs new 1H break validation.
      // Earlier breaks are already present in the confirmed higher-timeframe history.
      for (let time = coverageStart; time < through; time += HOUR) {
        if (!times.has(time)) throw new Error('Missing closed 1H candles for fractal verification');
      }
      for (const record of snapshot.records) {
        const confirmed = Date.parse(record.confirmedAt) / 1000;
        const pivot = Date.parse(record.pivotAt) / 1000;
        const price = Number(record.priceExact);
        if (!Number.isFinite(confirmed) || confirmed >= through || !Number.isSafeInteger(pivot) ||
          !['HIGH', 'LOW'].includes(record.type) || !(price > 0)) continue;
        const column = record.type === 'HIGH' ? 'high' : 'low';
        const crosses = (candle) => candle.time > confirmed && (record.type === 'HIGH' ? candle.high > price : candle.low < price);
        const originBeforeHistory = pivot + period <= first;
        const originCovered = pivot >= first && pivot + period <= through &&
          Array.from({ length: period / HOUR }, (_, i) => pivot + i * HOUR).every((time) => times.has(time));
        const origin = originCovered ? candles.find((candle) => candle.time >= pivot && candle.time < pivot + period && candle[column] === price) : undefined;
        const coarseBreak = record.firstBrokenAt ? Date.parse(record.firstBrokenAt) / 1000 : undefined;
        let brokenTime;
        let broken = coarseBreak !== undefined;
        let breakBeforeHistory = false;
        if (coarseBreak !== undefined) {
          breakBeforeHistory = coarseBreak + period <= first;
          const covered = coarseBreak >= first && Array.from({ length: period / HOUR }, (_, i) => coarseBreak + i * HOUR)
            .filter((time) => time < through).every((time) => times.has(time));
          if (covered) brokenTime = candles.find((candle) => candle.time >= coarseBreak && candle.time < coarseBreak + period && crosses(candle))?.time;
        } else {
          const breakCandle = candles.find(crosses);
          if (breakCandle) { broken = true; brokenTime = breakCandle.time; }
        }
        levels.push({
          id: `${timeframe}|${record.id}`, timeframe, type: record.type,
          price, priceExact: record.priceExact, label: `${timeframe}${record.isLatestForType ? 'C' : ''}`,
          isCurrent: Boolean(record.isLatestForType), pivotAt: record.pivotAt, confirmedAt: record.confirmedAt,
          originTime: origin?.time ?? null, originBeforeHistory,
          broken, brokenTime: brokenTime ?? null, breakBeforeHistory
        });
      }
    }
    return { levels, evaluatedThrough: bundle.evaluatedThrough };
  }

  class FractalPrimitive {
    constructor() {
      this.levels = [];
      this.options = { daily: true, weekly: true, broken: true };
      this.view = { zOrder: () => 'normal', renderer: () => ({ draw: (target) => this.draw(target) }) };
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = undefined; this.series = undefined; this.requestUpdate = undefined; }
    paneViews() { return [this.view]; }
    updateAllViews() {}
    setData(levels) { this.levels = levels; this.requestUpdate?.(); }
    setOptions(options) { Object.assign(this.options, options); this.requestUpdate?.(); }
    visibleLevels() {
      return this.levels.filter((level) => this.options[level.timeframe === 'D' ? 'daily' : 'weekly'] &&
        (!level.broken || this.options.broken) && !level.breakBeforeHistory &&
        (level.originTime !== null || level.originBeforeHistory) && (!level.broken || level.brokenTime !== null));
    }
    draw(target) {
      if (!this.chart || !this.series) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save();
        ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        const labels = [];
        const levels = this.visibleLevels().slice().sort((a, b) => b.timeframe.localeCompare(a.timeframe));
        for (const level of levels) {
          const y = this.series.priceToCoordinate(level.price);
          const origin = level.originBeforeHistory ? 0 : this.chart.timeScale().timeToCoordinate(level.originTime);
          const end = level.broken ? this.chart.timeScale().timeToCoordinate(level.brokenTime) : width - 8;
          if (y === null || y < 8 || y > height - 8 || origin === null || end === null || end < 0 || origin > width) continue;
          const color = level.timeframe === 'D' ? '#ffeb3b' : '#ff9800';
          ctx.strokeStyle = color;
          ctx.globalAlpha = level.broken ? .65 : 1;
          ctx.lineWidth = level.timeframe === 'W' ? (level.broken ? 2 : 4) : (level.isCurrent ? 2 : 1.2);
          ctx.setLineDash(level.broken ? [5, 5] : []);
          ctx.beginPath(); ctx.moveTo(Math.max(0, origin), y); ctx.lineTo(Math.min(width, end), y); ctx.stroke();
          if (level.broken && end <= width && end - Math.max(0, origin) > 15) {
            ctx.fillStyle = color; ctx.font = '600 10px Arial'; ctx.textBaseline = 'bottom';
            ctx.fillText(level.label, Math.max(0, end - ctx.measureText(level.label).width - 3), y - 3);
          }
          if (!level.broken && origin < width - 125) {
            let label = labels.find((item) => item.price === level.price);
            if (!label) { label = { price: level.price, y, parts: [] }; labels.push(label); }
            label.parts.push({ text: level.label, color });
          }
        }
        ctx.globalAlpha = 1; ctx.setLineDash([]); ctx.font = '600 11px Arial'; ctx.textBaseline = 'middle';
        labels.sort((a, b) => a.y - b.y);
        let last = -20;
        for (const label of labels) { label.labelY = Math.max(label.y, last + 17); last = label.labelY; }
        if (labels.length && last > height - 10) {
          let bottom = height - 10;
          for (let i = labels.length - 1; i >= 0; i--) { labels[i].labelY = Math.min(labels[i].labelY, bottom); bottom = labels[i].labelY - 17; }
        }
        for (const label of labels) {
          const price = label.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
          const parts = label.parts;
          const textWidth = ctx.measureText(parts.map((part) => part.text).join(' / ') + '  ' + price).width;
          let x = width - textWidth - 10;
          ctx.fillStyle = '#131722'; ctx.fillRect(x - 4, label.labelY - 8, textWidth + 8, 16);
          if (Math.abs(label.y - label.labelY) > 2) {
            ctx.strokeStyle = parts[0].color; ctx.lineWidth = 1;
            ctx.beginPath(); ctx.moveTo(x - 7, label.y); ctx.lineTo(x - 4, label.labelY); ctx.stroke();
          }
          parts.forEach((part, index) => {
            const text = part.text + (index < parts.length - 1 ? ' / ' : '  ');
            ctx.fillStyle = part.color; ctx.fillText(text, x, label.labelY); x += ctx.measureText(text).width;
          });
          ctx.fillStyle = parts[parts.length - 1].color; ctx.fillText(price, x, label.labelY);
        }
        ctx.restore();
      });
    }
  }

  const api = Object.freeze({ projectFractals, FractalPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotFractals = api;
})(typeof globalThis === 'object' ? globalThis : this);
