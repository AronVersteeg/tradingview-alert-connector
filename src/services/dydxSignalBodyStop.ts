export const DYDX_BODY_STOP_FRACTION = 0.25;

type BodyStopSignal = { time: number; closedAt: number; direction: 'long' | 'short' };

export class ManualStopCandlePendingError extends Error {}

export function withDydxBodyQuarterStop(activePlan: any, candle: any, signal: BodyStopSignal, referencePrice: number, asset = 'ETH'): any {
  const sizingStop = activePlan?.stop;
  if (!sizingStop?.valid || !Number.isFinite(Number(sizingStop.price)) || !(Number(sizingStop.price) > 0) ||
    !sizingStop.fractal || activePlan.status === 'invalid-stop') {
    throw new Error(sizingStop?.reason || `${asset} requires a valid original dYdX fractal for order sizing.`);
  }
  const open = Number(candle?.openRef), close = Number(candle?.closeRef);
  const high = Number(candle?.highRef), low = Number(candle?.lowRef);
  if (candle?.fractalCandleSource !== 'dydx-1h' || Date.parse(candle.timestamp) !== signal.time * 1000 ||
    signal.closedAt !== signal.time + 3600 || signal.closedAt * 1000 > Date.now() ||
    ![open, close, high, low, referencePrice].every(value => Number.isFinite(value) && value > 0) ||
    low > Math.min(open, close) || high < Math.max(open, close)) {
    throw new Error(`${asset} quarter-body SL requires valid closed dYdX OHLC from the exact Binance signal hour.`);
  }
  if (activePlan.direction !== signal.direction) throw new Error(`${asset} quarter-body SL direction does not match the entry signal.`);
  const bodyStopPrice = open + DYDX_BODY_STOP_FRACTION * (close - open);
  if (signal.direction === 'long' ? bodyStopPrice >= referencePrice : bodyStopPrice <= referencePrice) {
    throw new Error(`${asset} dYdX quarter-body SL is not on the protective side of the current oracle price.`);
  }
  const distance = Math.abs(referencePrice - bodyStopPrice), riskPct = distance / Math.max(1, referencePrice);
  if (Number.isFinite(sizingStop.maxDistancePct) && riskPct > sizingStop.maxDistancePct) {
    throw new Error(`${asset} dYdX quarter-body SL exceeds the existing maximum stop distance.`);
  }
  // Keep sizing and the confirmed dYdX fractal anchor; replace only the execution stop price.
  return { ...activePlan, sizingStop: { ...sizingStop, fractal: { ...sizingStop.fractal } },
    stop: { ...sizingStop, price: bodyStopPrice, source: 'dydx-signal-body-quarter', distance, riskPct,
      buffer: 0, adjustedToMinDistance: false, wickGuardPrice: undefined, wickGuardSource: undefined,
      reason: 'dYdX signal-body SL at 25% from open; original confirmed fractal retained as the trailing anchor.' },
    initialStopPolicy: { mode: 'signal-body-quarter', bodyFraction: DYDX_BODY_STOP_FRACTION, candleSource: 'dydx-1h',
      candleStartedAt: new Date(signal.time * 1000).toISOString(), candleClosedAt: new Date(signal.closedAt * 1000).toISOString(),
      open, close, bodyStopPrice, sizingStopPrice: sizingStop.price, sizingSource: 'dydx-fractal',
      fractalPlannedStopRiskUsd: activePlan.sizing?.plannedStopRiskUsd,
      bodyStopPlannedStopRiskUsd: Number(activePlan.sizing?.size) * distance } };
}

export function withManualDydxBodyQuarterPlan(plan: any, request: {
  market: string; direction: 'long' | 'short'; signalCandleStartedAt: string; signalCandleClosedAt: string;
}, asset: string): any {
  const time = Date.parse(request.signalCandleStartedAt), close = time + 3_600_000;
  const requestedClose = Date.parse(request.signalCandleClosedAt);
  if (plan.market !== String(request.market).replace(/_/g, '-').toUpperCase() || !Number.isFinite(time) || time % 3_600_000 ||
    ![close - 1, close].includes(requestedClose)) {
    throw new Error(`${asset} manual quarter-body SL requires an exact same-market closed 1H signal candle.`);
  }
  if (!plan.stopReferenceCandle || Date.parse(plan.stopReferenceCandle.timestamp) < time) {
    throw new ManualStopCandlePendingError(`${asset} is awaiting the exact closed dYdX signal hour for its manual quarter-body SL; no order sent.`);
  }
  const reference = Number(plan.marketInfo?.oraclePrice) || Number(plan.price);
  const activePlan = withDydxBodyQuarterStop(plan.activePlan, plan.stopReferenceCandle,
    { time: time / 1000, closedAt: close / 1000, direction: request.direction }, reference, asset);
  return { ...plan, activePlan,
    ...(plan.plans ? { plans: { ...plan.plans, [request.direction]: activePlan } } : {}) };
}
