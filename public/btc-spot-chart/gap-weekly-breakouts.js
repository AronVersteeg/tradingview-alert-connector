(function (root) {
  'use strict';
  const HOUR = 3600;

  function buildFractalCloseBreakouts(input, fractals, nowMs, timeframe, levelsKey, symbol) {
    if (!['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].includes(symbol) || fractals?.symbol !== symbol || fractals.venue !== 'Binance Spot' || !Array.isArray(fractals.levels)) {
      throw new Error('Unexpected BTC Spot breakout source');
    }
    const fractalThrough = Date.parse(fractals.evaluatedThrough) / 1000;
    if (!Number.isSafeInteger(fractalThrough) || fractalThrough % HOUR) throw new Error('Invalid fractal coverage');
    const candles = input.filter(candle => candle.closed === true).slice().sort((a, b) => a.time - b.time);
    let previousTime = 0;
    for (const candle of candles) {
      if (!Number.isSafeInteger(candle.time) || candle.time % HOUR || candle.time <= previousTime ||
          ![candle.open, candle.high, candle.low, candle.close].every(price => Number.isFinite(price) && price > 0) ||
          candle.high < Math.max(candle.open, candle.close) || candle.low > Math.min(candle.open, candle.close)) {
        throw new Error('Invalid breakout candle');
      }
      previousTime = candle.time;
    }
    const through = Math.min(Math.floor(nowMs / (HOUR * 1000)) * HOUR, fractalThrough,
      candles.at(-1)?.time + HOUR || 0);
    const levels = fractals.levels.filter(level => level.timeframe === timeframe && ['HIGH', 'LOW'].includes(level.type));
    const signals = [];
    for (let i = 1; i < candles.length; i++) {
      const candle = candles[i], previous = candles[i - 1];
      if (candle.time + HOUR > through || candle.time !== previous.time + HOUR) continue;
      const long = candle.close > previous.close;
      const short = candle.close < previous.close;
      if (!long && !short) continue;
      const matches = levels.filter(level => {
        const confirmedMs = Date.parse(level.confirmedAt);
        if (!Number.isFinite(level.price) || level.price <= 0 || !Number.isFinite(confirmedMs) ||
            confirmedMs >= candle.time * 1000 || level.breakBeforeHistory ||
            level.broken && (!Number.isSafeInteger(level.brokenTime) || level.brokenTime < candle.time)) return false;
        return long ? level.type === 'HIGH' && previous.close <= level.price && candle.close > level.price
          : level.type === 'LOW' && previous.close >= level.price && candle.close < level.price;
      });
      if (!matches.length) continue;
      signals.push({ time: candle.time, closedAt: candle.time + HOUR, direction: long ? 'long' : 'short', close: candle.close,
        [levelsKey]: matches.map(level => ({ id: level.id, type: level.type, price: level.price, confirmedAt: level.confirmedAt })) });
    }
    return { symbol, venue: 'Binance Spot', signals,
      evaluatedThrough: through ? new Date(through * 1000).toISOString() : null };
  }

  function buildGapFractalBreakouts(input, timeline, fractals, nowMs, timeframe, levelsKey, symbol) {
    if (timeline?.symbol !== symbol || timeline.venue !== 'Binance Spot' || !Array.isArray(timeline.records)) {
      throw new Error('Unexpected BTC Spot breakout source');
    }
    const gaps = new Map();
    let lastGap = 0;
    for (const row of timeline.records) {
      if (!Number.isSafeInteger(row.candleTime) || row.candleTime % HOUR || row.availableAt !== row.candleTime + HOUR ||
          row.availableAt <= lastGap || !(row.lower === null && row.upper === null ||
          Number.isFinite(row.lower) && Number.isFinite(row.upper) && row.lower > 0 && row.upper > row.lower)) {
        throw new Error('Invalid gap breakout history');
      }
      gaps.set(row.availableAt, row); lastGap = row.availableAt;
    }
    const model = buildFractalCloseBreakouts(input, fractals, nowMs, timeframe, levelsKey, symbol);
    const through = Math.min(Date.parse(model.evaluatedThrough || '') / 1000 || 0, lastGap ? lastGap + HOUR : 0);
    const previous = new Map(input.filter(candle => candle.closed === true).map(candle => [candle.time + HOUR, candle.close]));
    const signals = model.signals.flatMap(signal => {
      // Use the completed box known at candle OPEN, not its expanded replacement.
      const gap = gaps.get(signal.time), priorClose = previous.get(signal.time);
      if (signal.closedAt > through || !gap || gap.lower === null || gap.upper === null ||
          !(signal.direction === 'long' ? priorClose <= gap.upper && signal.close > gap.upper
            : priorClose >= gap.lower && signal.close < gap.lower)) return [];
      return [{ ...signal, gap: { lower: gap.lower, upper: gap.upper, candleTime: gap.candleTime } }];
    });
    return { ...model, signals, evaluatedThrough: through ? new Date(through * 1000).toISOString() : null };
  }

  function buildDailyFractalBreakouts(input, fractals, nowMs = Date.now(), symbol = 'BTCUSDT') {
    return buildFractalCloseBreakouts(input, fractals, nowMs, 'D', 'dailyLevels', symbol);
  }

  function buildWeeklyFractalBreakouts(input, fractals, nowMs = Date.now(), symbol = 'BTCUSDT') {
    return buildFractalCloseBreakouts(input, fractals, nowMs, 'W', 'weeklyLevels', symbol);
  }

  function buildGapWeeklyBreakouts(input, timeline, fractals, nowMs = Date.now(), symbol = 'BTCUSDT') {
    return buildGapFractalBreakouts(input, timeline, fractals, nowMs, 'W', 'weeklyLevels', symbol);
  }

  function buildGapDailyBreakouts(input, timeline, fractals, nowMs = Date.now(), symbol = 'BTCUSDT') {
    return buildGapFractalBreakouts(input, timeline, fractals, nowMs, 'D', 'dailyLevels', symbol);
  }

  class GapWeeklyBreakoutPrimitive {
    constructor(color = '#ff9800') {
      this.color = color;
      this.signals = []; this.visible = true;
      this.view = { zOrder: () => 'top', renderer: () => ({ draw: target => this.draw(target) }) };
    }
    attached({ chart, requestUpdate }) { this.chart = chart; this.requestUpdate = requestUpdate; }
    detached() { this.chart = this.requestUpdate = undefined; }
    updateAllViews() {}
    paneViews() { return [this.view]; }
    setData(signals) { this.signals = signals; this.requestUpdate?.(); }
    setVisible(visible) { this.visible = Boolean(visible); this.requestUpdate?.(); }
    draw(target) {
      if (!this.visible || !this.chart || !this.signals.length) return;
      target.useMediaCoordinateSpace(({ context: ctx, mediaSize: { width, height } }) => {
        ctx.save(); ctx.beginPath(); ctx.rect(0, 0, width, height); ctx.clip();
        ctx.strokeStyle = this.color; ctx.lineWidth = 1; ctx.globalAlpha = .9; ctx.setLineDash([]);
        for (const signal of this.signals) {
          const x = this.chart.timeScale().timeToCoordinate(signal.time);
          if (x === null || x < 0 || x > width) continue;
          const crisp = Math.floor(x) + .5;
          ctx.beginPath(); ctx.moveTo(crisp, 0); ctx.lineTo(crisp, height); ctx.stroke();
        }
        ctx.restore();
      });
    }
  }

  class GapDailyBreakoutPrimitive extends GapWeeklyBreakoutPrimitive {
    constructor() { super('#fff59d'); }
  }

  const api = Object.freeze({ buildGapWeeklyBreakouts, buildGapDailyBreakouts, buildDailyFractalBreakouts, buildWeeklyFractalBreakouts,
    GapWeeklyBreakoutPrimitive, GapDailyBreakoutPrimitive });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotGapWeeklyBreakouts = api;
})(typeof globalThis === 'object' ? globalThis : this);
