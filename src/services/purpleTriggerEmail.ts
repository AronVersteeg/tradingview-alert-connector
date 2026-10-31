import { btcManualTriggerTimestampNl } from './btcManualTradeOverride';
import { PurpleLineSignal } from './ethGapDailyEntry';

export type PurpleTriggerEmail = {
  signature: string; subject: string; body: string; outcome: string; createdAt: string;
  attempts: number; sentAt?: string; lastAttemptAt?: string; retryAt?: string; error?: string;
};

export function purpleTriggerEmail(market: string, signal: PurpleLineSignal, entry: any, result: any): PurpleTriggerEmail {
  const asset = market.replace('-USD', '');
  const signature = `${market}|GAP_DAILY|${signal.time}|${signal.direction}`;
  const intent = entry.intent?.signature === signature ? entry.intent : undefined;
  const decision = entry.decision?.signal?.time === signal.time || entry.decision?.outcome === 'ERROR' ? entry.decision : {};
  const outcome = intent?.outcome === 'CONFIRMED' ? 'PLACED'
    : intent?.outcome === 'FLATTENED' ? 'FLATTENED'
    : ['SUBMITTING', 'UNKNOWN'].includes(intent?.outcome) ? 'UNCONFIRMED'
    : decision.outcome || (result.tradePlaced ? 'PLACED' : 'ERROR');
  const plan = intent?.plan?.activePlan;
  const started = new Date(signal.time * 1000).toISOString(), closed = new Date(signal.closedAt * 1000).toISOString();
  const subject = `${asset} PURPLE ${signal.direction.toUpperCase()} TRIGGERED | ${btcManualTriggerTimestampNl(started)}`;
  const body = [
    `${asset} PURPLE ${signal.direction.toUpperCase()} TRIGGERED`,
    `Source: Binance Spot ${asset}USDT 1H intrusion + Daily fractal close break`,
    `Signal candle open: ${btcManualTriggerTimestampNl(started)}`,
    `Signal candle close: ${btcManualTriggerTimestampNl(closed)}`,
    `Binance Spot signal close: ${signal.close}`,
    ...signal.dailyLevels.map(level => `Daily ${level.type}: ${level.price}`),
    `Execution market: dYdX ${market}`,
    `Order result: ${outcome}`,
    decision.reason ? `Reason: ${decision.reason}` : '',
    decision.warning ? `Warning: ${decision.warning}` : '',
    outcome === 'UNCONFIRMED' ? 'Submission is uncertain; it is not proof of a failed or absent order. Reconciliation remains active.' : '',
    outcome === 'PLACED' && decision.size ? `Actual position size: ${decision.size}` : '',
    outcome === 'PLACED' && decision.entryPrice ? `dYdX entry: ${decision.entryPrice}` : '',
    plan?.stop?.price ? `Planned initial SL (dYdX signal-body midpoint): ${plan.stop.price}` : '',
    plan?.sizingStop?.price ? `Original dYdX fractal sizing stop: ${plan.sizingStop.price}` : '',
    result.dynamicSlSync?.outcome ? `Protection check: ${result.dynamicSlSync.outcome}` : '',
    result.dynamicSlSync?.reason ? `Protection detail: ${result.dynamicSlSync.reason}` : '',
    'TPs: none. Management: initial SL and tighten-only dYdX fractal trailing.',
    'This message reports the signal and the execution result available at notification time.'
  ].filter(Boolean).join('\n');
  return { signature, subject, body, outcome, createdAt: new Date().toISOString(), attempts: 0 };
}
