import { btcManualTriggerTimestampNl } from './btcManualTradeOverride';
import { isBtcPurpleTrailSignal, type SpotEntrySignal } from './ethGapDailyEntry';

export type PurpleTriggerEmail = {
  signature: string; subject: string; body: string; outcome: string; createdAt: string;
  attempts: number; sentAt?: string; lastAttemptAt?: string; retryAt?: string; error?: string;
};

export function purpleTriggerEmail(market: string, signal: SpotEntrySignal, entry: any, result: any): PurpleTriggerEmail {
  const asset = market.replace('-USD', '');
  const green = 'edge' in signal;
  const purpleTrail = isBtcPurpleTrailSignal(signal);
  const atr = 'atr' in signal ? signal.atr : undefined;
  const headline = `${asset} ${atr ? 'ATR' : green ? 'GREEN' : 'PURPLE'} ${signal.direction.toUpperCase()} TRIGGERED`;
  const signature = `${market}|GAP_DAILY|${signal.time}|${signal.direction}`;
  const intent = entry.intent?.signature === signature ? entry.intent : undefined;
  const decision = entry.decision?.signal?.time === signal.time || entry.decision?.outcome === 'ERROR' ? entry.decision : {};
  const outcome = intent?.outcome === 'CONFIRMED' ? 'PLACED'
    : intent?.outcome === 'FLATTENED' ? 'FLATTENED'
    : intent?.outcome === 'NO_POSITION' ? 'NO_POSITION_OBSERVED'
    : intent?.outcome === 'NOT_SUBMITTED' ? 'NOT_SUBMITTED'
    : ['SUBMITTING', 'UNKNOWN'].includes(intent?.outcome) ? 'UNCONFIRMED'
    : decision.outcome || (result.tradePlaced ? 'PLACED' : 'ERROR');
  const plan = intent?.plan?.activePlan;
  const initialStopLabel = plan?.initialStopPolicy?.mode === 'signal-body-open'
    ? 'dYdX signal candle body open'
    : plan?.initialStopPolicy?.mode === 'signal-body-quarter'
    ? 'dYdX signal-body 25% from open'
    : plan?.initialStopPolicy?.mode === 'signal-body-midpoint' ? 'dYdX signal-body midpoint'
    : plan?.initialStopPolicy?.mode === 'confirmed-fractal' ? 'confirmed dYdX fractal' : 'dYdX initial stop';
  const started = new Date(signal.time * 1000).toISOString(), closed = new Date(signal.closedAt * 1000).toISOString();
  const subject = `${headline} | ${btcManualTriggerTimestampNl(started)}`;
  const body = [
    headline,
    `Source: Binance Spot ${asset}USDT 1H intrusion + ${purpleTrail ? 'gap edge and Daily fractal' : green ? 'gap edge' : 'Daily fractal'} close break`,
    `Signal candle open: ${btcManualTriggerTimestampNl(started)}`,
    `Signal candle close: ${btcManualTriggerTimestampNl(closed)}`,
    `Binance Spot signal close: ${signal.close}`,
    atr ? `dYdX directional break body / prior Binance Spot ATR: ${atr.ratio.toFixed(2)}x` : '',
    ...(green ? [`Previous gap ${signal.direction === 'long' ? 'upper' : 'lower'} edge: ${signal.edge}`]
      : signal.dailyLevels.map(level => `Daily ${level.type}: ${level.price}`)),
    purpleTrail ? `Broken gap edge: ${signal.direction === 'long' ? signal.gap.upper : signal.gap.lower}` : '',
    `Execution market: dYdX ${market}`,
    `Order result: ${outcome}`,
    decision.reason ? `Reason: ${decision.reason}` : '',
    decision.warning ? `Warning: ${decision.warning}` : '',
    outcome === 'UNCONFIRMED' ? 'Submission is uncertain; it is not proof of a failed or absent order. Reconciliation remains active.' : '',
    outcome === 'DETECTED' ? 'The signal was saved before execution; no order or protective stop is confirmed by this message.' : '',
    outcome === 'NO_POSITION_OBSERVED' ? 'Recovery observed no position. This is not proof that an earlier broadcast was never sent.' : '',
    intent?.error ? `Execution error: ${intent.error}` : '',
    outcome === 'PLACED' && decision.size ? `Actual position size: ${decision.size}` : '',
    outcome === 'PLACED' && decision.entryPrice ? `dYdX entry: ${decision.entryPrice}` : '',
    plan?.stop?.price ? `Planned initial SL (${initialStopLabel}): ${plan.stop.price}` : '',
    plan?.sizingStop?.price ? `Original dYdX fractal sizing stop: ${plan.sizingStop.price}` : '',
    result.dynamicSlSync?.outcome ? `Protection check: ${result.dynamicSlSync.outcome}` : '',
    result.dynamicSlSync?.reason ? `Protection detail: ${result.dynamicSlSync.reason}` : '',
    'TPs: none. Fractal trailing tightens only on later same-direction Yellow intrusion closes, once per candle.',
    'This message reports the signal and the execution result available at notification time.'
  ].filter(Boolean).join('\n');
  return { signature, subject, body, outcome, createdAt: new Date().toISOString(), attempts: 0 };
}
