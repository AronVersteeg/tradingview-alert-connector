import { greenRetestSignals, type RetestContext, type RetestSignal } from './greenRetest';
import { readRetestHistory } from './greenRetestHistory';
import { recentPurpleRetest } from './purpleRetest';
import type { PurpleLineMarket } from '../services/purpleLineSetup';
import { filterGreenContinuation } from '../services/greenContinuationRule';

const HOUR = 3_600_000;
type SpotContext = { green: RetestContext; purple: RetestContext };

const archives = new Map<PurpleLineMarket, SpotContext>();
export function archivedContinuation(market: PurpleLineMarket): SpotContext | undefined {
  const cached = archives.get(market);
  if (cached) return cached;
  const green = readRetestHistory(market, 'green')?.context;
  const purple = readRetestHistory(market, 'purple')?.context;
  if (!green || !purple) return;
  const context = { green, purple };
  archives.set(market, context);
  return context;
}

const recent = new Map<PurpleLineMarket, { key: string; retryAt: number; read: Promise<SpotContext> }>();

export function readRecentContinuation(market: PurpleLineMarket, payload: any, now: number): Promise<SpotContext> {
  const frames = payload.frames;
  const key = `${payload.modelVersion}|${frames?.[0]?.startedAtMs}|${frames?.at(-1)?.startedAtMs}`;
  const cached = recent.get(market);
  if (cached?.key === key && now < cached.retryAt) return cached.read;
  const read = Promise.resolve().then(async () => {
    const green = greenRetestSignals(market, payload, now);
    const purple = await recentPurpleRetest(green, now);
    const entry = recent.get(market);
    if (entry?.read === read) entry.retryAt = Date.now() + 60_000;
    return { green, purple };
  }).catch(error => {
    const failed = recent.get(market);
    if (failed?.read === read) failed.retryAt = Date.now() + 10_000;
    throw error;
  });
  recent.set(market, { key, read, retryAt: Infinity });
  return read;
}

export function canBridgeContinuation(archive: RetestContext, live: RetestContext): boolean {
  if (archive.market !== live.market || live.from < archive.from || live.through <= archive.through) return false;
  const first = live.from + 2 * HOUR;
  if (archive.through - first < 23 * HOUR) return false;
  const evidence = (context: RetestContext) => context.signals
    .filter(row => row.closedAt * 1000 >= first && row.closedAt * 1000 <= archive.through)
    .map(row => `${row.closedAt}|${row.direction}|${row.dailyLevels?.join(',')}`).join(';');
  return evidence(archive) === evidence(live);
}

export function greenContinuationMarkers(archive?: SpotContext, live?: SpotContext) {
  if (!archive && !live) throw new Error('No verified Purple direction history.');
  if (archive && live && archive.green.market !== live.green.market) throw new Error('Cannot combine different Spot markets.');
  const archived = archive ? filterGreenContinuation(archive.green.signals, archive.purple.signals) : [];
  if (!live || archive && live.green.through <= archive.green.through) {
    return { from: archive!.green.from, through: archive!.green.through, archivedThrough: archive!.green.through, signals: archived };
  }
  const bridged = archive && canBridgeContinuation(archive.purple, live.purple);
  if (bridged) {
    const cutoff = archive.green.through / 1000;
    const anchors = [...archive.purple.signals, ...live.purple.signals.filter(row => row.closedAt > cutoff)];
    return { from: archive.green.from, through: live.green.through, archivedThrough: archive.green.through, signals: [...archived,
      ...filterGreenContinuation(live.green.signals.filter(row => row.closedAt > cutoff), anchors)] };
  }
  const cutoff = (archive?.green.through ?? 0) / 1000;
  const latest = filterGreenContinuation(live.green.signals, live.purple.signals).filter(row => row.closedAt > cutoff);
  return { from: archive?.green.from ?? live.green.from, through: live.green.through, archivedThrough: archive?.green.through,
    signals: [...archived, ...latest] };
}

export async function hasGreenContinuation(market: PurpleLineMarket, payload: any, signal: RetestSignal, now: number): Promise<boolean> {
  const live = await readRecentContinuation(market, payload, now);
  if (!live.green.signals.some(row => row.closedAt === signal.closedAt && row.direction === signal.direction &&
    row.close === signal.close && row.edge === signal.edge)) return false;
  const withinRecent = filterGreenContinuation([signal], live.purple.signals);
  if (withinRecent.length) return true;
  if (live.purple.signals.some(row => row.closedAt < signal.closedAt)) return false;
  const archive = archivedContinuation(market);
  return Boolean(archive && canBridgeContinuation(archive.purple, live.purple) &&
    filterGreenContinuation([signal], archive.purple.signals).length);
}
