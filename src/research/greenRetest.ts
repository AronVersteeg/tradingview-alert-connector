import { buildFractalStop, type DecentraderRow, type DydxRsiCandle, dydxHourlyCandlesToFractalRows, mapDirectionFromAlert } from '../services/decentraderGapMonitor';
import { withDydxBodyOpenStop } from '../services/dydxSignalBodyStop';
import { reconstructReplicaIntrusions } from '../services/openLiquidityV2EthTradeMonitor';
import { PURPLE_LINE_SETUPS, type PurpleLineMarket } from '../services/purpleLineSetup';
import type { GreenLineSignal } from '../services/ethGapDailyEntry';

const { timelineFromPayload } = require('../../public/btc-spot-chart/gap-boxes.js');
const { compactIntrusionHistory, buildGapIntrusionMarkers } = require('../../public/btc-spot-chart/intrusion-daily-breakouts.js');
const HOUR = 3_600_000;
const RISK_USD = 15;
type Direction = 'long' | 'short';
type Fractal = { at: number; confirmedAt: number; price: number };
type Yellow = { at: number; result: 'TIGHTENED' | 'UNCHANGED'; stop: number; fractal?: Fractal };
type Trade = {
  id: string; direction: Direction; signalAt: number; openedAt: number; entryPrice: number; gapEdge: number;
  sizingStop: number; sizingFractal?: Fractal; initialStop: number; size: number; riskUsd: number;
  stops: { at: number; price: number }[]; yellows: Yellow[];
  closedAt?: number; exitPrice?: number; exitReason?: 'STOP' | 'GAP_STOP'; grossPnl?: number;
  markToMarket?: { at: number; price: number; grossPnl: number };
  status: 'OPEN' | 'CLOSED' | 'INCOMPLETE';
};

export function greenRetestSignals(market: PurpleLineMarket, payload: any, now: number) {
  const setup = PURPLE_LINE_SETUPS[market];
  if (payload?.source?.market !== market || payload.modelVersion !== setup.modelVersion ||
      payload.quality?.causalModel !== true || payload.quality?.priceStepUsd !== setup.priceStepUsd ||
      !payload.source.sourceStatuses?.some((source: any) => source.name === `Binance Spot ${setup.symbol} 1H` && source.ok)) {
    throw new Error(`Current native ${setup.asset} Spot replica is unavailable.`);
  }
  const frames = payload.frames;
  if (!Array.isArray(frames) || frames.length < 2 || frames.some((row: any, index: number) =>
    !Number.isSafeInteger(row.startedAtMs) || row.startedAtMs % HOUR || row.startedAtMs + HOUR > now ||
    (index > 0 && row.startedAtMs !== frames[index - 1].startedAtMs + HOUR))) throw new Error(`Incomplete ${setup.asset} Spot replay hours.`);
  const intrusions = reconstructReplicaIntrusions(payload, frames[0].t).alerts.map(alert => ({
    timestamp: alert.timestamp, direction: mapDirectionFromAlert(alert) || 'mixed'
  }));
  const history = compactIntrusionHistory({ source: payload.source, intrusionBenchmarks: { history: intrusions } },
    now, market, { includeGapHours: true });
  const candles = frames.map((frame: any) => ({ time: frame.startedAtMs / 1000, closed: true, close: frame.close }));
  const timeline = timelineFromPayload(payload, market);
  const signals: GreenLineSignal[] = buildGapIntrusionMarkers(candles, history, timeline, now, market)
    .map((row: GreenLineSignal) => ({ ...row, setup: 'green-line',
      close: frames[(row.time * 1000 - frames[0].startedAtMs) / HOUR].close }));
  const yellows = history.records.map((row: { time: number; direction: Direction }) =>
    ({ at: row.time * 1000 + HOUR, direction: row.direction }));
  return { market, signals, yellows, from: frames[0].startedAtMs, through: frames.at(-1).startedAtMs + HOUR };
}

function fractal(stop: ReturnType<typeof buildFractalStop>): Fractal | undefined {
  if (!stop.fractal) return undefined;
  const at = Date.parse(stop.fractal.timestamp);
  return Number.isFinite(at) ? { at, confirmedAt: at + (stop.fractal.window + 1) * HOUR, price: stop.fractal.price } : undefined;
}

export function simulateGreenRetest(rows: DecentraderRow[], context: ReturnType<typeof greenRetestSignals>) {
  const asset = PURPLE_LINE_SETUPS[context.market].asset;
  const byHour = new Map<number, number>();
  const segmentStarts: number[] = [];
  let previousAt: number | undefined;
  rows.forEach((row, index) => {
    const at = Date.parse(row.timestamp);
    segmentStarts.push(previousAt === at - HOUR ? segmentStarts[index - 1] : index);
    byHour.set(at, index);
    previousAt = at;
  });
  const entries = new Map(context.signals.map(signal => [signal.closedAt * 1000, signal]));
  const yellows = new Set(context.yellows.map(row => `${row.at}|${row.direction}`));
  const trades: Trade[] = [];
  let position: Trade | undefined;
  let incompleteAt: number | undefined;
  let skipped = 0;
  let skippedWhileOpen = 0;
  const tail = (index: number) => rows.slice(Math.max(segmentStarts[index], index + 1 - 1000), index + 1);
  for (let at = context.from + HOUR; at < context.through; at += HOUR) {
    const i = byHour.get(at);
    if (i === undefined) {
      if (position) { position.status = 'INCOMPLETE'; incompleteAt = at; break; }
      if (entries.has(at)) skipped++;
      continue;
    }
    const candle = rows[i];
    const signal = entries.get(at);
    if (!position && signal) {
      const priorIndex = byHour.get(at - HOUR);
      const known = priorIndex === undefined ? [] : tail(priorIndex);
      if (known.length >= 5) {
        const entryPrice = candle.openRef;
        const sizing = buildFractalStop(known, known.length - 1, signal.direction, entryPrice, { enforceMinDistance: false });
        if (sizing.valid && sizing.price) {
          try {
            const plannedSize = RISK_USD / Math.abs(entryPrice - sizing.price);
            const plan = withDydxBodyOpenStop({ direction: signal.direction, stop: sizing, sizing: { size: plannedSize } },
              rows[priorIndex!], signal, entryPrice, asset);
            const initialStop = plan.stop.price;
            const size = Math.min(plannedSize, RISK_USD / Math.abs(entryPrice - initialStop));
            if (!Number.isFinite(size) || size <= 0) throw new Error('Invalid replay sizing.');
            position = { id: String(at), direction: signal.direction, signalAt: at - HOUR, openedAt: at, entryPrice,
              gapEdge: signal.edge, sizingStop: sizing.price, sizingFractal: fractal(sizing), initialStop, size,
              riskUsd: Math.abs(entryPrice - initialStop) * size, stops: [{ at, price: initialStop }], yellows: [], status: 'OPEN' };
            trades.push(position);
          } catch { /* The live body-open guard also rejects an unprotective stop. */ }
        }
      }
      if (!position) skipped++;
    } else if (position && signal) {
      skipped++;
      skippedWhileOpen++;
    }

    if (!position) continue;
    const current = position.stops.at(-1)!.price, sign = position.direction === 'long' ? 1 : -1;
    const adverseOpen = sign * (candle.openRef - current) <= 0;
    const touched = sign * ((sign > 0 ? candle.lowRef : candle.highRef) - current) <= 0;
    if (touched) {
      const exitPrice = adverseOpen ? candle.openRef : current;
      Object.assign(position, { closedAt: at, exitPrice, exitReason: adverseOpen ? 'GAP_STOP' : 'STOP',
        grossPnl: sign * (exitPrice - position.entryPrice) * position.size, status: 'CLOSED' });
      position = undefined;
      continue;
    }
    const nextClose = at + HOUR;
    if (nextClose <= position.openedAt || !yellows.has(`${nextClose}|${position.direction}`)) continue;
    const known = tail(i);
    const candidate = buildFractalStop(known, known.length - 1, position.direction, candle.closeRef, { enforceMinDistance: false });
    const next = candidate.price;
    const improved = candidate.valid && candidate.fractal?.source === (sign > 0 ? 'lowRef' : 'highRef') && next !== undefined &&
      sign * (next - current) > 0 && sign * (candle.closeRef - next) > 0;
    if (improved) position.stops.push({ at: nextClose, price: next! });
    position.yellows.push({ at: nextClose, result: improved ? 'TIGHTENED' : 'UNCHANGED',
      stop: improved ? next! : current, fractal: fractal(candidate) });
  }
  const lastClosed = byHour.get(context.through - HOUR);
  if (position?.status === 'OPEN' && lastClosed !== undefined) {
    const price = rows[lastClosed].closeRef;
    const sign = position.direction === 'long' ? 1 : -1;
    position.markToMarket = { at: context.through, price, grossPnl: sign * (price - position.entryPrice) * position.size };
  }
  return { trades, skipped, skippedWhileOpen, incompleteAt };
}

export function retestFractalRows(market: PurpleLineMarket, candles: (DydxRsiCandle & { ticker?: string })[] | undefined, now: number) {
  if (candles?.some(candle => candle.ticker !== market || candle.resolution !== '1HOUR')) {
    throw new Error(`Unexpected dYdX candle market or interval for ${market} Green retest.`);
  }
  return dydxHourlyCandlesToFractalRows(candles, now).filter(row => {
    const at = Date.parse(row.timestamp);
    return Number.isFinite(at) && at % HOUR === 0 && [row.openRef, row.highRef, row.lowRef, row.closeRef].every(value => Number.isFinite(value) && value > 0) &&
      row.highRef >= Math.max(row.openRef, row.closeRef) && row.lowRef <= Math.min(row.openRef, row.closeRef);
  });
}

export function buildGreenRetestFromRows(context: ReturnType<typeof greenRetestSignals>, rows: DecentraderRow[]) {
  if (!rows.some(row => Date.parse(row.timestamp) >= context.from && Date.parse(row.timestamp) < context.through)) {
    throw new Error(`dYdX ${context.market} candles unavailable for this replay period.`);
  }
  const result = simulateGreenRetest(rows, context);
  const candles = rows.filter(row => Date.parse(row.timestamp) < context.through).map(row => ({
    time: Date.parse(row.timestamp) / 1000, open: row.openRef, high: row.highRef, low: row.lowRef, close: row.closeRef, closed: true
  }));
  return { ...result, candles, market: context.market, from: context.from, through: context.through,
    candidateCount: context.signals.length, riskBudgetUsd: RISK_USD };
}
