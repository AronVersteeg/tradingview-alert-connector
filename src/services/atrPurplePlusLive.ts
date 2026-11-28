export type DydxPositionIdentity = { createdAt: string; createdAtHeight: string };
export type ClosedPositionResult = { closedAt: number; realizedPnl: number };
export type AtrPurpleParent = {
  direction: 'long' | 'short';
  signalClosedAt: number;
  position?: DydxPositionIdentity;
  result?: ClosedPositionResult;
  consumedAt?: number;
  unverifiedAt?: number;
  previousUnverifiedAt?: number;
};

export function dydxPositionIdentity(row: { createdAt?: unknown; createdAtHeight?: unknown }): DydxPositionIdentity | undefined {
  if (typeof row.createdAt !== 'string' || !Number.isFinite(Date.parse(row.createdAt)) ||
      typeof row.createdAtHeight !== 'string' || !/^[1-9]\d*$/.test(row.createdAtHeight)) return;
  return { createdAt: row.createdAt, createdAtHeight: row.createdAtHeight };
}

export function matchDydxClosedPosition(rows: unknown[], market: string, direction: 'long' | 'short', identity: DydxPositionIdentity): ClosedPositionResult | undefined {
  const opened = Date.parse(identity.createdAt);
  const matches = rows.filter((row: any) => row?.market === market && row.subaccountNumber === 0 && row.status === 'CLOSED' &&
    row.side === direction.toUpperCase() && row.createdAt === identity.createdAt && row.createdAtHeight === identity.createdAtHeight);
  if (matches.length !== 1 || !Number.isFinite(opened)) return;
  const row = matches[0] as { closedAt?: string; realizedPnl?: unknown; size?: unknown };
  if (!(row.size === 0 || typeof row.size === 'string' && row.size.trim() && Number(row.size) === 0) ||
      typeof row.realizedPnl !== 'string' || !row.realizedPnl.trim()) return;
  const closedAt = Date.parse(row.closedAt || ''), realizedPnl = Number(row.realizedPnl);
  if (closedAt >= opened && Number.isFinite(realizedPnl)) return { closedAt, realizedPnl };
}

export function atrPurpleParentMatches(parent: AtrPurpleParent | undefined, direction: 'long' | 'short', close: number): parent is AtrPurpleParent & { position: DydxPositionIdentity } {
  return Boolean(parent?.position && dydxPositionIdentity(parent.position) && parent.direction === direction &&
    parent.signalClosedAt < close && (parent.consumedAt === undefined || parent.consumedAt === close));
}

export function atrPurpleParentLostBy(parent: AtrPurpleParent, signalClose: number): boolean {
  return Boolean(parent.result && Number.isFinite(parent.result.realizedPnl) && parent.result.realizedPnl < 0 &&
    parent.result.closedAt >= parent.signalClosedAt && parent.result.closedAt <= signalClose);
}

export function noteUnverifiedAtrPurpleCandidate(parent: AtrPurpleParent, close: number): boolean {
  if (parent.unverifiedAt !== undefined && close <= parent.unverifiedAt) return false;
  parent.previousUnverifiedAt = parent.unverifiedAt;
  parent.unverifiedAt = close;
  return true;
}

export function priorUnverifiedAtrPurpleCandidate(parent: AtrPurpleParent, close: number): number | undefined {
  const prior = parent.unverifiedAt !== close ? parent.unverifiedAt : parent.previousUnverifiedAt;
  return prior !== undefined && prior < close && atrPurpleParentLostBy(parent, prior) ? prior : undefined;
}
