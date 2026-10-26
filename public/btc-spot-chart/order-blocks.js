// Adapted from Order Block Detector, copyright LuxAlgo.
// https://www.luxalgo.com/library/indicator/order-block-detector/
// CC BY-NC-SA 4.0: https://creativecommons.org/licenses/by-nc-sa/4.0/
// Changes: closed-candle JS replay, retained mitigations, coverage metadata and chart primitive.
(function (root) {
  'use strict';
  const HOUR = 3600;
  const COLORS = { bullish: '#169400', bearish: '#ff1100' };
  const LABEL_COLORS = { bullish: '#91a98e', bearish: '#b99a95', mixed: '#a2a9b6' };
  const priceText = new Intl.NumberFormat('en-US', { maximumFractionDigits: 8 });
  const volumeText = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });

  function clusterActiveBlocks(blocks) {
    const active = blocks.filter(block => block.state === 'intact' && Number.isFinite(block.bottom) &&
      Number.isFinite(block.top) && block.bottom > 0 && block.top >= block.bottom)
      .slice().sort((a, b) => a.bottom - b.bottom || a.top - b.top || a.id.localeCompare(b.id));
    const clusters = [];
    for (const block of active) {
      let cluster = clusters.at(-1);
      if (!cluster || block.bottom > cluster.top) {
        cluster = { bottom: block.bottom, top: block.top, direction: block.direction, blockIds: [], pivotVolumes: new Map(), volumeComplete: true };
        clusters.push(cluster);
      }
      cluster.top = Math.max(cluster.top, block.top);
      if (cluster.direction !== block.direction) cluster.direction = 'mixed';
      cluster.blockIds.push(block.id);
      // These are whole origin-candle volumes, not volume measured inside the zone.
      if (!Number.isSafeInteger(block.originTime) || !Number.isFinite(block.volume) || block.volume < 0) cluster.volumeComplete = false;
      else if (cluster.pivotVolumes.has(block.originTime) && cluster.pivotVolumes.get(block.originTime) !== block.volume) cluster.volumeComplete = false;
      else cluster.pivotVolumes.set(block.originTime, block.volume);
    }
    const result = clusters.map(({ pivotVolumes, ...cluster }) => ({ ...cluster, count: cluster.blockIds.length,
      id: cluster.blockIds.join(','), volumeCandleCount: pivotVolumes.size,
      pivotVolumeBtc: cluster.volumeComplete ? [...pivotVolumes.values()].reduce((sum, volume) => sum + volume, 0) : null,
      volumeRank: null, volumeSharePct: null, volumeSource: 'origin-candle-base-volume' }));
    const ranked = result.filter(cluster => cluster.volumeComplete).slice().sort((a, b) => b.pivotVolumeBtc - a.pivotVolumeBtc || a.bottom - b.bottom);
    const total = ranked.reduce((sum, cluster) => sum + cluster.pivotVolumeBtc, 0);
    ranked.forEach((cluster, i) => {
      cluster.volumeRank = i && cluster.pivotVolumeBtc === ranked[i - 1].pivotVolumeBtc ? ranked[i - 1].volumeRank : i + 1;
      cluster.volumeSharePct = total > 0 ? cluster.pivotVolumeBtc / total * 100 : 0;
    });
    return result;
  }

  function placeClusterLabels(items, height) {
    let used = 0;
    const selected = items.slice().sort((a, b) => (b.pivotVolumeBtc ?? -1) - (a.pivotVolumeBtc ?? -1) || b.count - a.count ||
      Math.abs(a.anchorY - height / 2) - Math.abs(b.anchorY - height / 2)).filter(item => {
      if (used + item.height + 3 > height - 8) return false;
      used += item.height + 3; return true;
    }).map(item => ({ ...item })).sort((a, b) => a.anchorY - b.anchorY);
    let bottom = 4;
    for (const item of selected) { item.y = Math.max(bottom, item.anchorY - item.height / 2); bottom = item.y + item.height + 3; }
    bottom = height - 4;
    for (let i = selected.length - 1; i >= 0; i--) {
      const item = selected[i]; item.y = Math.min(item.y, bottom - item.height); bottom = item.y - 3;
    }
    return selected;
  }

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
      this.blocks = []; this.clusters = []; this.labels = []; this.indices = new Map(); this.visible = true;
      this.view = { zOrder: () => 'bottom', renderer: () => ({ draw: target => this.draw(target) }) };
      this.labelView = { zOrder: () => 'top', renderer: () => ({ draw: target => this.drawLabels(target) }) };
    }
    attached({ chart, series, requestUpdate }) { this.chart = chart; this.series = series; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.series = this.requestUpdate = undefined; }
    paneViews() { return [this.view, this.labelView]; }
    updateAllViews() {}
    setVisible(visible) { this.visible = Boolean(visible); this.labels = []; this.requestUpdate?.(); }
    setLabelAnchor(read) { this.labelAnchor = read; this.requestUpdate?.(); }
    getClusters() { return this.clusters.map(cluster => ({ ...cluster, blockIds: [...cluster.blockIds] })); }
    getLabels() { return this.labels.map(label => ({ ...label, lines: [...label.lines] })); }
    setData(model, candles) {
      this.blocks = model.blocks; this.clusters = clusterActiveBlocks(this.blocks); this.labels = []; this.indices = new Map();
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
    drawLabels(target) {
      this.labels = [];
      if (!this.visible || !this.chart || !this.series) return;
      const anchor = this.labelAnchor?.();
      if (!Number.isFinite(anchor) || anchor < 0) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        const x = anchor + 10, available = width - x - 4;
        if (available < 30) return;
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        const items = [];
        for (const cluster of this.clusters) {
          const a = this.series.priceToCoordinate(cluster.top), b = this.series.priceToCoordinate(cluster.bottom);
          if (!Number.isFinite(a) || !Number.isFinite(b) || Math.max(a, b) < 4 || Math.min(a, b) > height - 4) continue;
          const low = priceText.format(cluster.bottom), high = priceText.format(cluster.top);
          const volume = cluster.volumeComplete ? `PV ~${volumeText.format(cluster.pivotVolumeBtc)} BTC #${cluster.volumeRank}` : 'PV n/a';
          let fontSize = 10, lines = [`${low} - ${high} | ${volume}`];
          ctx.font = `${fontSize}px Arial`;
          if (ctx.measureText(lines[0]).width > available) {
            lines = ctx.measureText(`${low} - ${high}`).width <= available ? [`${low} - ${high}`] : [high, low];
            let line = '';
            for (const word of volume.split(' ')) {
              if (line && ctx.measureText(`${line} ${word}`).width > available) { lines.push(line); line = word; }
              else line = line ? `${line} ${word}` : word;
            }
            if (line) lines.push(line);
          }
          const longest = Math.max(...lines.map(line => ctx.measureText(line).width));
          fontSize = Math.min(fontSize, Math.floor(10 * available / Math.max(1, longest)));
          if (fontSize < 8) continue;
          ctx.font = `${fontSize}px Arial`;
          items.push({ id: cluster.id, count: cluster.count, direction: cluster.direction, bottom: cluster.bottom, top: cluster.top,
            pivotVolumeBtc: cluster.pivotVolumeBtc, volumeComplete: cluster.volumeComplete, volumeRank: cluster.volumeRank,
            x, lines, fontSize, width: Math.max(...lines.map(line => ctx.measureText(line).width)),
            height: lines.length * (fontSize + 2), anchorY: Math.max(4, Math.min(height - 4, (a + b) / 2)) });
        }
        this.labels = placeClusterLabels(items, height);
        for (const label of this.labels) {
          const color = LABEL_COLORS[label.direction] || LABEL_COLORS.mixed;
          ctx.globalAlpha = .9; ctx.fillStyle = '#131722'; ctx.fillRect(label.x - 2, label.y - 1, label.width + 4, label.height + 2);
          ctx.globalAlpha = .55; ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.setLineDash([]);
          ctx.beginPath(); ctx.moveTo(anchor + 3, label.anchorY); ctx.lineTo(label.x - 3, label.y + label.height / 2); ctx.stroke();
          ctx.globalAlpha = .9; ctx.fillStyle = color; ctx.font = `${label.fontSize}px Arial`; ctx.textBaseline = 'top';
          label.lines.forEach((line, i) => ctx.fillText(line, label.x, label.y + i * (label.fontSize + 2)));
        }
        ctx.restore();
      });
    }
  }
  const api = Object.freeze({ buildOrderBlocks, clusterActiveBlocks, placeClusterLabels, OrderBlockPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotOrderBlocks = api;
})(typeof globalThis === 'object' ? globalThis : this);
