// Read-only/offline market structure features at each original signal close.
import fs from 'fs';
import path from 'path';
import { parseSpotChartCandles } from '../src/services/binanceSpotChart';
import { parseBinanceDailyCandles, buildDailyFractalHistory } from '../src/services/binanceDailyFractalHistory';
const { projectFractals } = require('../public/btc-spot-chart/fractals.js');
const { buildOrderBlocks, clusterActiveBlocks } = require('../public/btc-spot-chart/order-blocks.js');
const base = process.argv[2], out = process.argv[3], weeklyBase = process.argv[4];
if (!base || !out || !weeklyBase) throw new Error('Supply Daily capture, study output and Weekly capture directories');
const input = JSON.parse(fs.readFileSync(path.join(base, 'inputs.json'), 'utf8'));
const replay = JSON.parse(fs.readFileSync(path.join(base, 'replay.json'), 'utf8'));
const weekly = JSON.parse(fs.readFileSync(path.join(weeklyBase, 'inputs.json'), 'utf8')).weekly;
const monthly = JSON.parse(fs.readFileSync(path.join(out, 'monthly.json'), 'utf8'));
const candles = parseSpotChartCandles(input.hourly, input.cutoff).filter(c => c.closed);
const allBlocks = buildOrderBlocks(candles, { length: 5, mitigation: 'Wick' }, input.cutoff);
const records = replay.signals.map((s: any) => {
  const rows = candles.filter(c => c.time <= s.time), current = rows.at(-1)!, previous = rows.at(-2)!;
  const cutoff = s.closedAt * 1000, sign = s.direction === 'long' ? 1 : -1;
  const bundle: any = { symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: new Date(cutoff).toISOString() };
  for (const [key, raw] of [['daily', input.daily], ['weekly', weekly], ['monthly', monthly]] as any[]) {
    const higher = parseBinanceDailyCandles(raw, cutoff);
    bundle[key] = { venue: 'Binance Spot', symbol: 'BTCUSDT', stale: false,
      latestClosedCandleAt: new Date(higher.at(-1)!.closeTime).toISOString(), records: buildDailyFractalHistory(higher) };
  }
  const projection = projectFractals(bundle, rows);
  const known = projection.levels.filter((l: any) => Date.parse(l.confirmedAt) < s.time * 1000 &&
    l.type === (sign === 1 ? 'HIGH' : 'LOW'));
  const features: any = {}, higherBreaks: any = {};
  const initial = replay.independent.trades.find((t: any) => t.signalTime === s.time && t.direction === s.direction);
  const riskDistance = Math.abs(initial.entryPrice - initial.initialStop);
  for (const timeframe of ['D', 'W', 'M']) {
    const levels = known.filter((l: any) => l.timeframe === timeframe);
    const crosses = levels.filter((l: any) => sign * (previous.close - l.price) <= 0 && sign * (current.close - l.price) > 0);
    const firstClose = crosses.filter((l: any) => !rows.slice(0, -1).some(c => c.time * 1000 > Date.parse(l.confirmedAt) && sign * (c.close - l.price) > 0));
    const freshWick = crosses.filter((l: any) => !l.breakBeforeHistory && (!l.broken || l.brokenTime === s.time));
    higherBreaks[timeframe] = { freshWick, firstClose, recrosses: crosses };
    features[`${timeframe}_fresh_wick_breaks`] = freshWick.length;
    features[`${timeframe}_first_close_breaks`] = firstClose.length;
    features[`${timeframe}_recrosses`] = crosses.length;
    features[`${timeframe}_oldest_fresh_level_days`] = freshWick.length ? Math.max(...freshWick.map((l: any) => (s.time - Date.parse(l.pivotAt) / 1000) / 86400)) : null;
    const ahead = levels.filter((l: any) => !l.breakBeforeHistory && (!l.broken || l.brokenTime > s.time) && sign * (l.price - current.close) > 0)
      .sort((a: any, b: any) => sign * (a.price - b.price));
    features[`${timeframe}_next_intact_distance_pct`] = ahead[0] ? Math.abs(ahead[0].price / current.close - 1) * 100 : null;
    features[`${timeframe}_next_intact_distance_R`] = ahead[0] ? Math.abs(ahead[0].price - current.close) / riskDistance : null;
  }
  const blocks = buildOrderBlocks(rows, { length: 5, mitigation: 'Wick' }, cutoff);
  const knownBlocks = blocks.blocks.filter((b: any) => b.confirmedAt <= s.time && b.direction === (sign === 1 ? 'bearish' : 'bullish'));
  const broken = knownBlocks.filter((b: any) => b.brokenTime === s.time && sign * (previous.close - (sign === 1 ? b.top : b.bottom)) <= 0 &&
    sign * (current.close - (sign === 1 ? b.top : b.bottom)) > 0);
  for (const block of broken) {
    const full = allBlocks.blocks.find((b: any) => b.id === block.id);
    if (JSON.stringify(full) !== JSON.stringify(block)) throw new Error('Block break metadata changed after future candles were removed');
  }
  const enriched = broken.map((b: any) => {
    const prior = rows.filter(c => c.time >= b.confirmedAt && c.time < s.time);
    let touches = 0, episodes = 0, touching = false;
    for (const c of prior) { const hit = c.high >= b.bottom && c.low <= b.top; if (hit) touches++; if (hit && !touching) episodes++; touching = hit; }
    return { ...b, ageHours: (s.time - b.confirmedAt) / 3600, originAgeHours: (s.time - b.originTime) / 3600, touches, touchEpisodes: episodes };
  });
  const simulatedBeforeBreak = blocks.blocks.filter((b: any) => b.confirmedAt <= s.time &&
    (b.brokenAt === null || b.brokenAt > s.time) && b.state !== 'coverage-gap').map((b: any) => ({ ...b, state: 'intact' }));
  const clusters = clusterActiveBlocks(simulatedBeforeBreak);
  const breakingIds = new Set(broken.map((b: any) => b.id));
  const brokenClusters = clusters.filter((cluster: any) => cluster.blockIds.some((id: string) => breakingIds.has(id)));
  const totalPivotVolume = brokenClusters.reduce((sum: number, cluster: any) => sum + (cluster.pivotVolumeBtc || 0), 0);
  features.OB_broken_count = enriched.length;
  features.OB_broken_clusters = brokenClusters.length;
  features.OB_oldest_break_age_hours = enriched.length ? Math.max(...enriched.map((b: any) => b.ageHours)) : null;
  features.OB_max_prior_touch_episodes = enriched.length ? Math.max(...enriched.map((b: any) => b.touchEpisodes)) : null;
  features.OB_best_volume_rank = enriched.length ? Math.min(...enriched.map((b: any) => b.volumeRankAtBreak)) : null;
  features.OB_break_candle_to_cluster_PV = totalPivotVolume > 0 ? current.volume / totalPivotVolume : null;
  features.OB_break_candle_to_largest_origin_volume = enriched.length ? current.volume / Math.max(...enriched.map((b: any) => b.volume)) : null;
  const previousBreaks = knownBlocks.filter((b: any) => b.brokenAt !== null && b.brokenAt <= s.time).sort((a: any, b: any) => b.brokenAt - a.brokenAt);
  features.OB_hours_since_latest_opposing_break = previousBreaks[0] ? (s.time - previousBreaks[0].brokenAt) / 3600 : null;
  for (const hours of [24, 72, 168]) {
    const recent = knownBlocks.filter((b: any) => b.brokenAt !== null && b.brokenAt <= s.closedAt && b.brokenAt >= s.closedAt - hours * 3600);
    features[`OB_recent_${hours}h_oldest_broken_age_hours`] = recent.length ? Math.max(...recent.map((b: any) => (b.brokenAt - b.confirmedAt) / 3600)) : null;
  }
  const intact = knownBlocks.filter((b: any) => b.state === 'intact' && sign * ((sign === 1 ? b.bottom : b.top) - current.close) > 0)
    .sort((a: any, b: any) => sign * (sign === 1 ? a.bottom - b.bottom : a.top - b.top));
  features.OB_nearest_ahead_distance_R = intact[0] ? Math.abs((sign === 1 ? intact[0].bottom : intact[0].top) - current.close) / riskDistance : null;
  return { time: s.time, direction: s.direction, features, higherBreaks, brokenBlocks: enriched, brokenClusters,
    nearestIntactBlockAhead: intact[0] || null, blockBreakPrefixAudit: true };
});
fs.writeFileSync(path.join(out, 'structure.json'), JSON.stringify({ blockSettings: { length: 5, mitigation: 'Wick', breakRequiresClose: true }, records }, null, 2));
console.log(JSON.stringify(records.map((r: any) => ({ time: new Date(r.time * 1000).toISOString(), features: r.features })), null, 2));
