// Offline research only. Execution is deliberately not connected to an exchange.
export type ReplayCandle = { time: number; open: number; high: number; low: number; close: number; closed: boolean };
export type ReplaySignal = { time: number; closedAt: number; direction: 'long' | 'short'; dailyLevels?: any[] };
export type ReplayStop = { valid: boolean; price?: number; reason?: string; fractal?: { timestamp: string; index: number; price: number } };
export type StopSelector = (rows: any[], index: number, direction: 'long' | 'short', price: number, options: any) => ReplayStop;
export type ReplayConfig = { riskUsd: number; feeBps: number; slippageBps: number; fractalDelay: number; warmupBars: number };
export type ReplayTrade = {
  id: number; direction: 'long' | 'short'; signalTime: number; entryTime: number; entryPrice: number; initialStop: number;
  quantityBtc: number; riskUsd: number; status: 'open' | 'closed'; stop: number; anchor: string;
  stops: { effectiveAt: number; price: number; pivotTime: string; pivotPrice: number }[];
  exitCandleTime?: number; exitWindowEnd?: number; exitPrice?: number; exitReason?: 'stop' | 'gap-stop';
  grossPnl?: number; fees?: number; netPnl?: number; rMultiple?: number;
  unrealizedGross?: number; unrealizedNetIfClosed?: number;
};
const HOUR = 3600;

export function validateReplayCandles(input: ReplayCandle[]): ReplayCandle[] {
  const candles = input.filter(c => c.closed).slice().sort((a, b) => a.time - b.time);
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    if (!Number.isSafeInteger(c.time) || c.time % HOUR || i && c.time !== candles[i - 1].time + HOUR ||
      ![c.open, c.high, c.low, c.close].every(v => Number.isFinite(v) && v > 0) ||
      c.low > Math.min(c.open, c.close) || c.high < Math.max(c.open, c.close)) {
      throw new Error('Replay requires valid contiguous closed 1H candles; gaps are not assumed flat');
    }
  }
  if (!candles.length) throw new Error('Replay has no closed candles');
  return candles;
}

export function summarizeReplay(trades: ReplayTrade[]) {
  const closed = trades.filter(t => t.status === 'closed');
  const net = closed.map(t => t.netPnl!);
  const wins = net.filter(v => v > 0), losses = net.filter(v => v < 0);
  let equity = 0, peak = 0, maxDrawdownUsd = 0;
  const equityCurve = closed.slice().sort((a, b) => a.exitWindowEnd! - b.exitWindowEnd! || a.id - b.id).map(t => {
    equity += t.netPnl!; peak = Math.max(peak, equity); maxDrawdownUsd = Math.max(maxDrawdownUsd, peak - equity);
    return { time: t.exitWindowEnd!, pnl: equity, tradeId: t.id };
  });
  const gain = wins.reduce((a, b) => a + b, 0), loss = -losses.reduce((a, b) => a + b, 0);
  const sorted = net.slice().sort((a, b) => a - b);
  const best = sorted.length ? sorted.at(-1)! : null;
  return { trades: trades.length, closed: closed.length, open: trades.length - closed.length,
    wins: wins.length, losses: losses.length, breakeven: net.filter(v => v === 0).length,
    winRatePct: closed.length ? wins.length / closed.length * 100 : null,
    grossPnlUsd: closed.reduce((sum, t) => sum + t.grossPnl!, 0),
    feesUsd: closed.reduce((sum, t) => sum + t.fees!, 0), netPnlUsd: equity,
    profitFactor: loss ? gain / loss : null, noLosses: closed.length > 0 && !loss,
    expectancyUsd: closed.length ? equity / closed.length : null,
    bestTradeUsd: best, worstTradeUsd: sorted[0] ?? null,
    pnlWithoutBestTradeUsd: best === null ? null : equity - best,
    bestTradeShareOfNetPct: best !== null && equity > 0 ? best / equity * 100 : null,
    medianTradeUsd: sorted.length ? (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2 : null,
    maxRealizedDrawdownUsd: maxDrawdownUsd,
    unrealizedGrossUsd: trades.filter(t => t.status === 'open').reduce((sum, t) => sum + t.unrealizedGross!, 0),
    equityCurve };
}

export function replayFractalStrategy(input: ReplayCandle[], signals: ReplaySignal[], config: ReplayConfig,
  selectStop: StopSelector, independent = false) {
  if (!(config.riskUsd > 0) || !Number.isFinite(config.riskUsd) ||
    ![config.feeBps, config.slippageBps].every(v => Number.isFinite(v) && v >= 0 && v < 1000) ||
    !Number.isInteger(config.fractalDelay) || config.fractalDelay < 0 ||
    !Number.isInteger(config.warmupBars) || config.warmupBars < 1) throw new Error('Invalid replay settings');
  const candles = validateReplayCandles(input), times = new Set(candles.map(c => c.time));
  const byClose = new Map<number, ReplaySignal[]>(), unique = new Set<string>();
  const skipped: { signalTime: number; direction: string; reason: string }[] = [];
  for (const signal of signals) {
    const key = `${signal.time}|${signal.direction}`;
    if (!Number.isSafeInteger(signal.time) || signal.time % HOUR || signal.closedAt !== signal.time + HOUR ||
      !['long', 'short'].includes(signal.direction)) throw new Error('Invalid replay signal');
    if (unique.has(key)) continue;
    unique.add(key);
    if (!times.has(signal.time) || !times.has(signal.closedAt)) {
      skipped.push({ signalTime: signal.time, direction: signal.direction, reason: 'Signal or next entry candle outside closed history' });
      continue;
    }
    byClose.set(signal.closedAt, [...(byClose.get(signal.closedAt) || []), signal]);
  }
  const rows = candles.map(c => ({ timestamp: new Date(c.time * 1000).toISOString(),
    highRef: c.high, lowRef: c.low, openRef: c.open, closeRef: c.close, ohlc4: (c.open + c.high + c.low + c.close) / 4 }));
  const trades: ReplayTrade[] = [];
  let active: ReplayTrade[] = [];
  const slip = config.slippageBps / 10000, fee = config.feeBps / 10000;
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i], candidates = byClose.get(c.time) || [];
    for (const signal of candidates) {
      let reason: string | undefined;
      if (candidates.length > 1) reason = 'Conflicting directions on one candle';
      else if (!independent && active.length) reason = 'Position already open at entry time';
      else if (i < config.warmupBars) reason = 'Insufficient stop warmup';
      if (reason) { skipped.push({ signalTime: signal.time, direction: signal.direction, reason }); continue; }
      const sign = signal.direction === 'long' ? 1 : -1, entry = c.open * (1 + sign * slip);
      // Pass only already closed rows. A future low/high cannot confirm an entry stop.
      const stop = selectStop(rows.slice(0, i), i - 1, signal.direction, entry, { enforceMinDistance: false });
      if (!stop.valid || !stop.fractal || !Number.isFinite(stop.price) || !(stop.price! > 0) || !(sign * (entry - stop.price!) > 0)) {
        skipped.push({ signalTime: signal.time, direction: signal.direction, reason: stop.reason || 'No valid confirmed protective fractal' }); continue;
      }
      const trade: ReplayTrade = { id: trades.length + 1, direction: signal.direction, signalTime: signal.time,
        entryTime: c.time, entryPrice: entry, initialStop: stop.price!, quantityBtc: config.riskUsd / Math.abs(entry - stop.price!),
        riskUsd: config.riskUsd, status: 'open', stop: stop.price!, anchor: stop.fractal.timestamp,
        stops: [{ effectiveAt: c.time, price: stop.price!, pivotTime: stop.fractal.timestamp, pivotPrice: stop.fractal.price }] };
      trades.push(trade); active.push(trade);
    }
    for (const trade of active) {
      const sign = trade.direction === 'long' ? 1 : -1;
      const gap = sign * (c.open - trade.stop) <= 0;
      const hit = gap || (sign === 1 ? c.low <= trade.stop : c.high >= trade.stop);
      if (hit) {
        trade.status = 'closed'; trade.exitCandleTime = c.time;
        trade.exitWindowEnd = gap ? c.time : c.time + HOUR;
        trade.exitReason = gap ? 'gap-stop' : 'stop';
        trade.exitPrice = (gap ? c.open : trade.stop) * (1 - sign * slip);
        trade.grossPnl = sign * (trade.exitPrice - trade.entryPrice) * trade.quantityBtc;
        trade.fees = (trade.entryPrice + trade.exitPrice) * trade.quantityBtc * fee;
        trade.netPnl = trade.grossPnl - trade.fees; trade.rMultiple = trade.netPnl / config.riskUsd;
        continue;
      }
      const candidate = selectStop(rows.slice(0, i + 1), i, trade.direction, c.close,
        { afterFractalTimestamp: trade.anchor, fractalDelay: config.fractalDelay, enforceMinDistance: false });
      if (candidate.valid && candidate.fractal && Number.isFinite(candidate.price) &&
        sign * (candidate.price! - trade.stop) > 0 && sign * (c.close - candidate.price!) > 0 &&
        Date.parse(candidate.fractal.timestamp) > Date.parse(trade.anchor)) {
        trade.stop = candidate.price!; trade.anchor = candidate.fractal.timestamp;
        trade.stops.push({ effectiveAt: c.time + HOUR, price: candidate.price!,
          pivotTime: candidate.fractal.timestamp, pivotPrice: candidate.fractal.price });
      }
    }
    active = active.filter(t => t.status === 'open');
  }
  for (const trade of active) {
    const sign = trade.direction === 'long' ? 1 : -1, close = candles.at(-1)!.close;
    trade.unrealizedGross = sign * (close - trade.entryPrice) * trade.quantityBtc;
    trade.unrealizedNetIfClosed = sign * (close * (1 - sign * slip) - trade.entryPrice) * trade.quantityBtc -
      (trade.entryPrice + close * (1 - sign * slip)) * trade.quantityBtc * fee;
  }
  return { config, mode: independent ? 'independent-signals' : 'one-position-at-a-time', trades, skipped,
    summary: summarizeReplay(trades), coverage: { candles: candles.length, from: candles[0].time, through: candles.at(-1)!.time + HOUR } };
}
