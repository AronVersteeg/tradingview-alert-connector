(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else {
    root.BtcSpotManualLevels = api;
    root.dispatchEvent(new CustomEvent('btc-spot-chart:manual-source-ready'));
  }
})(typeof window === 'object' ? window : globalThis, function () {
  'use strict';
  const COLOR = '#4599ff';
  const BOX_HISTORY_CANDLES = 12;
  const COLUMN_GAP_CANDLES = 2;
  const positive = (value) => typeof value === 'number' && Number.isFinite(value) && value > 0;

  function projectionBounds(candleCount, lead = 10, column = 0) {
    if (candleCount < 1) return null;
    const from = Math.max(0, candleCount - 1 - BOX_HISTORY_CANDLES), to = candleCount - 1 + lead;
    const shift = column * (to - from + COLUMN_GAP_CANDLES);
    return { from: from + shift, to: to + shift };
  }

  function fromSnapshot(snapshot) {
    if (!snapshot || snapshot.market !== 'BTC-USD') throw new Error('Unexpected manual plan market');
    const plans = Array.isArray(snapshot.overrides) ? snapshot.overrides : snapshot.override ? [snapshot.override] : null;
    if (!plans) throw new Error('Manual plans unavailable');
    const levels = [];
    let planCount = 0;
    for (const plan of plans) {
      if (!plan || !['ARMED', 'EXECUTING'].includes(plan.status)) continue;
      if ((plan.market && plan.market !== 'BTC-USD') || typeof plan.id !== 'string' || !plan.id ||
          !['long', 'short'].includes(plan.direction) || !positive(plan.closeTrigger)) continue;
      planCount++;
      const direction = plan.direction === 'long' ? 'L' : 'S';
      const entry = String(plan.closeTrigger);
      const parsedStart = Date.parse(plan.armedAt || plan.updatedAt || '');
      const base = { planId: plan.id, direction: plan.direction, entryPrice: plan.closeTrigger,
        fromTime: Number.isFinite(parsedStart) ? parsedStart / 1000 : null };
      levels.push({ ...base, id: `${plan.id}:entry`, kind: 'entry', price: plan.closeTrigger, title: `${direction} ${entry} entry` });
      (Array.isArray(plan.takeProfits) ? plan.takeProfits : []).forEach((tp, index) => {
        if (!tp || !positive(tp.price)) return;
        levels.push({ ...base, id: `${plan.id}:tp:${index}`, kind: 'tp', price: tp.price,
          title: `${direction} ${entry} TP${index + 1}`, allocationPct: positive(tp.allocationPct) ? tp.allocationPct : null });
      });
    }
    return { market: 'BTC-USD', enabled: snapshot.enabled === true, planCount, levels, observedAt: Date.now() };
  }

  function buildBands(levels) {
    const plans = new Map();
    for (const level of levels) {
      if (level.kind === 'entry') plans.set(level.planId, { entry: level, targets: [] });
    }
    for (const level of levels) {
      const plan = plans.get(level.planId);
      if (plan && level.kind === 'tp' && (plan.entry.direction === 'long' ? level.price > plan.entry.price : level.price < plan.entry.price)) {
        plan.targets.push(level.price);
      }
    }
    const bands = [];
    const placed = [];
    const shades = ['14, 55, 155', '35, 130, 235', '125, 205, 255', '220, 245, 255'];
    for (const { entry, targets } of plans.values()) {
      const sorted = [...new Set(targets)].sort((a, b) => entry.direction === 'long' ? a - b : b - a);
      if (!sorted.length) continue;
      const low = Math.min(entry.price, ...sorted), high = Math.max(entry.price, ...sorted);
      // Later conflicting plans always sit to the right of earlier ones.
      const overlaps = placed.filter((box) => low < box.high && high > box.low);
      const column = overlaps.length ? Math.max(...overlaps.map((box) => box.column)) + 1 : 0;
      placed.push({ low, high, column });
      let fromPrice = entry.price;
      sorted.forEach((price, index) => {
        const color = index < shades.length ? shades[index] :
          `${Math.min(248, 220 + (index - 3) * 10)}, ${Math.min(252, 245 + (index - 3) * 3)}, 255`;
        bands.push({ planId: entry.planId, fromTime: entry.fromTime, fromPrice, toPrice: price, column,
          color, opacity: .50, targetNumber: index + 1 });
        fromPrice = price;
      });
    }
    return bands;
  }

  class ManualLevelLines {
    constructor(series) {
      this.series = series; this.lines = new Map(); this.levels = []; this.visible = true; this.candles = []; this.lead = 10;
      this.view = { zOrder: () => 'bottom', renderer: () => ({ draw: (target) => this.draw(target) }) };
    }
    attached({ chart, requestUpdate }) { this.chart = chart; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.requestUpdate = undefined; }
    paneViews() { return [this.view]; }
    updateAllViews() {}
    setData(model) { this.levels = model.levels.map((level) => ({ ...level })); this.bands = buildBands(this.levels); this.render(); }
    setCandles(candles) { this.candles = candles; this.requestUpdate?.(); }
    setLead(lead) { this.lead = lead; this.requestUpdate?.(); }
    setVisible(visible) { this.visible = visible; this.render(); }
    visiblePrices() { return this.visible ? this.levels.map((level) => level.price) : []; }
    rightLead(candleCount = this.candles.length) {
      const column = this.visible ? Math.max(0, ...(this.bands || []).map((band) => band.column)) : 0;
      const bounds = projectionBounds(candleCount, this.lead, column);
      return bounds ? bounds.to - (candleCount - 1) : this.lead;
    }
    projectionBoxes() {
      const boxes = new Map();
      for (const band of this.bands || []) {
        const bounds = projectionBounds(this.candles.length, this.lead, band.column);
        if (!bounds) continue;
        const box = boxes.get(band.planId) || { planId: band.planId, column: band.column, ...bounds,
          low: Infinity, high: -Infinity };
        box.low = Math.min(box.low, band.fromPrice, band.toPrice);
        box.high = Math.max(box.high, band.fromPrice, band.toPrice);
        boxes.set(band.planId, box);
      }
      return [...boxes.values()];
    }
    render() {
      // Co-located targets share one label instead of covering each other on the price axis.
      const groups = new Map();
      for (const level of this.levels) {
        const group = groups.get(level.price) || [];
        group.push(level); groups.set(level.price, group);
      }
      for (const [price, line] of this.lines) {
        if (!groups.has(price)) { this.series.removePriceLine(line); this.lines.delete(price); }
      }
      for (const [price, levels] of groups) {
        const titles = [...new Set(levels.map((level) => level.title))];
        const options = { price, color: COLOR, lineWidth: 1,
          lineStyle: levels.some((level) => level.kind === 'entry') ? 0 : 2,
          lineVisible: this.visible, axisLabelVisible: this.visible,
          axisLabelColor: COLOR, axisLabelTextColor: '#101722',
          title: titles.slice(0, 2).join(' / ') + (titles.length > 2 ? ` +${titles.length - 2}` : '') };
        if (this.lines.has(price)) this.lines.get(price).applyOptions(options);
        else this.lines.set(price, this.series.createPriceLine(options));
      }
      this.requestUpdate?.();
    }
    draw(target) {
      if (!this.visible || !this.chart || !this.candles.length) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        const scale = this.chart.timeScale();
        const separators = [];
        for (const band of this.bands || []) {
          const bounds = projectionBounds(this.candles.length, this.lead, band.column);
          const left = scale.logicalToCoordinate(bounds.from), right = scale.logicalToCoordinate(bounds.to);
          const a = this.series.priceToCoordinate(band.fromPrice), b = this.series.priceToCoordinate(band.toPrice);
          if (left === null || right === null || a === null || b === null || right <= left || right < 0 || left > width) continue;
          const x = Math.max(0, left), end = Math.min(width, right);
          ctx.fillStyle = `rgba(${band.color}, ${band.opacity})`;
          ctx.fillRect(x, Math.min(a, b), Math.max(0, end - x), Math.abs(b - a));
          ctx.strokeStyle = 'rgba(69, 153, 255, 0.5)'; ctx.lineWidth = 1;
          ctx.beginPath(); ctx.moveTo(left, a); ctx.lineTo(left, b); ctx.moveTo(right, a); ctx.lineTo(right, b); ctx.stroke();
          separators.push({ from: x, to: end, y: b });
        }
        // Draw boundaries after all fills so neighbouring bands cannot cover them.
        ctx.strokeStyle = 'rgba(149, 211, 255, 0.9)'; ctx.lineWidth = 2; ctx.setLineDash([]);
        ctx.beginPath();
        for (const line of separators) { ctx.moveTo(line.from, line.y); ctx.lineTo(line.to, line.y); }
        ctx.stroke();
        ctx.restore();
      });
    }
    destroy() { for (const line of this.lines.values()) this.series.removePriceLine(line); this.lines.clear(); }
  }
  return { fromSnapshot, buildBands, projectionBounds, ManualLevelLines, COLOR };
});
