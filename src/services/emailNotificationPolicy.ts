export const EMAIL_NOTIFICATION_POLICY = 'entries-only';

export type EmailNotificationContext = {
  kind: 'entry';
  market: string;
  signature: string;
  outcome: string;
};

export function emailNotificationAllowed(context?: EmailNotificationContext): boolean {
  return context?.kind === 'entry' && context.outcome === 'PLACED' &&
    typeof context.market === 'string' && Boolean(context.market.trim()) &&
    typeof context.signature === 'string' && Boolean(context.signature.trim());
}

export function informationalEmailsEnabled(): boolean {
  return false;
}
