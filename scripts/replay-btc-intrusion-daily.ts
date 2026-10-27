// Offline runner: imports pure evaluators, never the application's bootstrap.
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { buildFractalStop } from '../src/services/decentraderGapMonitor';
import { parseSpotChartCandles } from '../src/services/binanceSpotChart';
import { parseBinanceDailyCandles, buildDailyFractalHistory } from '../src/services/binanceDailyFractalHistory';
import { replayFractalStrategy, ReplayConfig, ReplayTrade } from '../src/research/fractalStrategyReplay';
const { projectDailyFractals } = require('../public/btc-spot-chart/fractals.js');
const { compactIntrusionHistory, buildIntrusionDailyBreakouts } = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');
const directory = process.argv[2];
if (!directory) throw new Error('Supply a directory containing inputs.json');
const inputPath = path.join(directory, 'inputs.json'), bytes = fs.readFileSync(inputPath);
const input = JSON.parse(bytes.toString());
if (input.version !== 1 || input.source !== 'Binance Spot BTCUSDT' || !Number.isSafeInteger(input.cutoff)) throw new Error('Unexpected replay capture');
const candles = parseSpotChartCandles(input.hourly, input.cutoff).filter(c => c.closed);
const daily = parseBinanceDailyCandles(input.daily, input.cutoff);
const bundle = { symbol: 'BTCUSDT', venue: 'Binance Spot', evaluatedThrough: new Date(input.cutoff).toISOString(),
  daily: { venue: 'Binance Spot', symbol: 'BTCUSDT', interval: '1d', stale: false,
    latestClosedCandleAt: new Date(daily.at(-1)!.closeTime).toISOString(), records: buildDailyFractalHistory(daily) } };
const history = compactIntrusionHistory(input.intrusionPayload, input.cutoff);
const projections = projectDailyFractals(bundle, candles);
const model = buildIntrusionDailyBreakouts(candles, history, projections, input.cutoff);
const prefixChecks = model.signals.map((signal: any) => {
  const cutoff = signal.closedAt * 1000, prefixDaily = daily.filter(c => c.closeTime < cutoff);
  const prefixCandles = candles.filter(c => (c.time + 3600) * 1000 <= cutoff);
  const prefixBundle = { ...bundle, evaluatedThrough: new Date(cutoff).toISOString(), daily: { ...bundle.daily,
    latestClosedCandleAt: new Date(prefixDaily.at(-1)!.closeTime).toISOString(), records: buildDailyFractalHistory(prefixDaily) } };
  const prefix = buildIntrusionDailyBreakouts(prefixCandles, history, projectDailyFractals(prefixBundle, prefixCandles), cutoff);
  const expected = model.signals.filter((s: any) => s.closedAt <= signal.closedAt);
  if (JSON.stringify(prefix.signals) !== JSON.stringify(expected)) throw new Error('Daily signal changed when future candles were removed');
  return { cutoff: new Date(cutoff).toISOString(), signals: prefix.signals.length, matchesFullHistory: true };
});
// These match the previously supplied SL settings and the existing live defaults.
// Public status independently verifies risk, maximum distance and fractal delay.
const stopEnvironment = {
  DECENTRADER_SL_FRACTAL_WINDOW: '2', DECENTRADER_SL_LOOKBACK_BARS: '72', DECENTRADER_SL_BUFFER_PCT: '0.001',
  DECENTRADER_SL_MIN_DISTANCE_PCT: '0.0025', DECENTRADER_SL_MAX_DISTANCE_PCT: String(input.settings.slMaxDistancePct ?? 0.3),
  DECENTRADER_SKIP_TRADE_WITHOUT_SL: 'true'
};
Object.assign(process.env, stopEnvironment);
const config: ReplayConfig = { riskUsd: 15, feeBps: 0, slippageBps: 0,
  fractalDelay: input.settings.dynamicSlFractalDelay ?? 0, warmupBars: 76 };
if (input.settings.tradeRiskUsd !== 15) throw new Error('Captured live risk differs from requested $15; review before running');
const primary = replayFractalStrategy(candles, model.signals, config, buildFractalStop);
const independent = replayFractalStrategy(candles, model.signals, config, buildFractalStop, true);
const sensitivity = [{ feeBps: 5, slippageBps: 2 }, { feeBps: 10, slippageBps: 5 }].map(costs =>
  replayFractalStrategy(candles, model.signals, { ...config, ...costs }, buildFractalStop));
const utc = (time: number) => new Date(time * 1000).toISOString();
const nl = (time: number) => new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam',
  dateStyle: 'short', timeStyle: 'short' }).format(time * 1000);
const escape = (v: any) => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
const money = (v: any) => v === null || v === undefined ? 'n.v.t.' : '$' + Number(v).toFixed(2);
const num = (v: any, digits = 2) => v === null || v === undefined ? 'n.v.t.' : Number(v).toFixed(digits);
const delayRecords = input.intrusionPayload.delayHistory.records || [];
const signalAudit = model.signals.map((s: any) => {
  const matching = delayRecords.filter((r: any) => {
    const t = r.intrusionTimestamp?.replace(' ', 'T') + 'Z';
    return Date.parse(t) / 1000 === s.time && (s.direction === 'long' ? /left edge|\|L\|/i : /right edge|\|S\|/i).test(`${r.sideCounts} ${r.signature}`);
  }).map((r: any) => Date.parse(r.smtpSentAt)).filter(Number.isFinite);
  const smtp = matching.length ? Math.min(...matching) : null;
  return { ...s, signalOpenUtc: utc(s.time), signalCloseUtc: utc(s.closedAt), signalOpenAmsterdam: nl(s.time),
    firstObservedAt: null, firstObservationAtCloseProven: false, earliestSmtpSentAt: smtp ? new Date(smtp).toISOString() : null,
    smtpMinutesAfterClose: smtp ? (smtp - s.closedAt * 1000) / 60000 : null };
});
const report = { version: 1, generatedAt: new Date().toISOString(), capturedAt: input.capturedAt,
  inputSha256: crypto.createHash('sha256').update(bytes).digest('hex'), manifest: input.manifest,
  strategy: 'Recorded BTC intrusion + same-candle fresh Daily close break (purple overlay)',
  signalSource: history.source, intrusions: { count: history.records.length, first: history.records[0], last: history.records.at(-1),
    ignored: history.ignoredRecords, completeCoverageProven: false },
  dailyFractals: bundle.daily.records.length, stopEnvironment, settingsEvidence: input.settings,
  validation: { contiguousClosedCandles: true, priceSignalPrefixChecks: prefixChecks,
    caveat: 'This proves price/fractal causality only, not historical intrusion availability.' },
  signals: signalAudit, primary, independent, sensitivity,
  limitations: [
    'Retrospective alert-time hypothesis, not a proven deployable live backtest: intrusion availability at candle close is unknown.',
    'Saved intrusion history is a partial sample. Absence of a record is not proof that no intrusion occurred.',
    'Spot OHLC drives entries and stops; live SL management uses dYdX OHLC and oracle/order execution.',
    'Entry at next candle open; initial/updated stops use only already closed candles. Stops tighten only, no TP.',
    'Intrabar stop time is only known within the exit 1H candle; gaps fill at the adverse open, not the stop.',
    'Base case excludes fees, slippage, funding, spreads and margin limits; cost scenarios are assumptions, not actual exchange fees.',
    'Shorts are synthetic linear exposure on Spot prices, not unborrowed short-selling on a Spot account.',
    'Fixed $15 price-to-initial-stop risk excludes costs/gaps; quantity is not rounded or capped by collateral.',
    'Open trades are marked to market separately, not forced closed. Drawdown metric is realized-only, not intratrade drawdown.',
    'Stop window 2, lookback 72 and buffer 0.1% follow supplied settings/code defaults; these three are not exposed by remote status.'
  ] };
fs.writeFileSync(path.join(directory, 'replay.json'), JSON.stringify(report, null, 2));
const csv = (rows: any[], keys: string[]) => [keys.join(','), ...rows.map(row => keys.map(key => '"' + String(row[key] ?? '').replace(/"/g, '""') + '"').join(','))].join('\r\n');
const exportTrades = (trades: ReplayTrade[]) => trades.map(t => ({ ...t, signalOpenUtc: utc(t.signalTime), entryUtc: utc(t.entryTime),
  signalOpenAmsterdam: nl(t.signalTime), entryAmsterdam: nl(t.entryTime),
  exitCandleUtc: t.exitCandleTime ? utc(t.exitCandleTime) : '', exitCandleAmsterdam: t.exitCandleTime ? nl(t.exitCandleTime) : '',
  exitWindowEndUtc: t.exitWindowEnd ? utc(t.exitWindowEnd) : '', stopUpdates: t.stops.length - 1 }));
const tradeKeys = ['id', 'direction', 'signalOpenUtc', 'signalOpenAmsterdam', 'entryUtc', 'entryAmsterdam', 'entryPrice', 'initialStop',
  'quantityBtc', 'riskUsd', 'status', 'exitCandleUtc', 'exitCandleAmsterdam', 'exitWindowEndUtc', 'exitPrice', 'exitReason',
  'stopUpdates', 'grossPnl', 'fees', 'netPnl', 'rMultiple', 'unrealizedGross'];
fs.writeFileSync(path.join(directory, 'trades-single.csv'), csv(exportTrades(primary.trades), tradeKeys));
fs.writeFileSync(path.join(directory, 'trades-all-signals.csv'), csv(exportTrades(independent.trades), tradeKeys));
fs.writeFileSync(path.join(directory, 'signals.csv'), csv(signalAudit.map((s: any) => ({ ...s,
  levels: s.dailyLevels.map((l: any) => l.price).join(' | ') })), ['signalOpenUtc', 'signalOpenAmsterdam', 'signalCloseUtc',
  'direction', 'close', 'levels', 'firstObservedAt', 'firstObservationAtCloseProven', 'earliestSmtpSentAt', 'smtpMinutesAfterClose']));

function lineChart(points: { x: number; y: number }[], width: number, height: number, color: string, markers: any[] = []) {
  if (!points.length) return '<p>Geen gesloten trades.</p>';
  const left = 66, right = width - 25, top = 16, bottom = height - 32;
  const minX = points[0].x, maxX = points.at(-1)!.x;
  const minY = Math.min(...points.map(p => p.y)), maxY = Math.max(...points.map(p => p.y));
  const x = (v: number) => left + (v - minX) / Math.max(1, maxX - minX) * (right - left);
  const y = (v: number) => bottom - (v - minY) / Math.max(1, maxY - minY) * (bottom - top);
  return `<svg viewBox="0 0 ${width} ${height}" role="img"><path d="${points.map((p, i) => `${i ? 'L' : 'M'}${x(p.x).toFixed(1)},${y(p.y).toFixed(1)}`).join(' ')}" fill="none" stroke="${color}" stroke-width="1.5"/>
    ${markers.map(m => `<line x1="${x(m.time)}" x2="${x(m.time)}" y1="${top}" y2="${bottom}" stroke="#bb86d9" stroke-opacity=".5"/><text x="${x(m.time) + 3}" y="${top + 12}" fill="#c4a0e1">${m.id}</text>`).join('')}
    <text x="4" y="${top + 12}">${num(maxY)}</text><text x="4" y="${bottom}">${num(minY)}</text>
    <text x="${left}" y="${height - 6}">${escape(nl(minX))}</text><text x="${right}" y="${height - 6}" text-anchor="end">${escape(nl(maxX))}</text></svg>`;
}
function tradeChart(trade: ReplayTrade) {
  const entryIndex = candles.findIndex(c => c.time === trade.entryTime);
  const endIndex = trade.exitCandleTime ? candles.findIndex(c => c.time === trade.exitCandleTime) : candles.length - 1;
  const rows = candles.slice(Math.max(0, entryIndex - 14), Math.min(candles.length, endIndex + 9));
  const width = 960, height = 320, left = 64, right = width - 18, top = 20, bottom = height - 36;
  const lo = Math.min(trade.initialStop, ...rows.map(c => c.low)), hi = Math.max(trade.initialStop, ...rows.map(c => c.high));
  const pad = (hi - lo) * .08 || 1;
  const x = (time: number) => left + (time - rows[0].time) / Math.max(1, rows.at(-1)!.time - rows[0].time) * (right - left);
  const y = (price: number) => bottom - (price - lo + pad) / (hi - lo + 2 * pad) * (bottom - top);
  const candleWidth = Math.max(.5, Math.min(7, (right - left) / rows.length * .65));
  const exitTime = trade.exitWindowEnd ?? rows.at(-1)!.time + 3600;
  const stopPath = trade.stops.filter(s => s.effectiveAt <= exitTime).map((s, i, all) => {
    const until = Math.min(exitTime, all[i + 1]?.effectiveAt ?? exitTime);
    return `M${x(s.effectiveAt)},${y(s.price)}H${x(until)}${all[i + 1] && until < exitTime ? `V${y(all[i + 1].price)}` : ''}`;
  }).join(' ');
  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Trade ${trade.id}: candles en causale trailing stop">
    <rect x="${x(trade.entryTime)}" y="${top}" width="${Math.max(1, x(Math.min(exitTime, rows.at(-1)!.time)) - x(trade.entryTime))}" height="${bottom - top}" fill="#2979ff" opacity=".06"/>
    ${rows.map(c => { const color = c.close >= c.open ? '#37bdab' : '#f06475'; return `<line x1="${x(c.time)}" x2="${x(c.time)}" y1="${y(c.high)}" y2="${y(c.low)}" stroke="${color}"/>
      <rect x="${x(c.time) - candleWidth / 2}" y="${Math.min(y(c.open), y(c.close))}" width="${candleWidth}" height="${Math.max(.8, Math.abs(y(c.open) - y(c.close)))}" fill="${color}"/>`; }).join('')}
    <line x1="${x(trade.signalTime)}" x2="${x(trade.signalTime)}" y1="${top}" y2="${bottom}" stroke="#bb86d9" stroke-width="1.3"/>
    <path d="${stopPath}" fill="none" stroke="#ffc857" stroke-width="1.7"/>
    <circle cx="${x(trade.entryTime)}" cy="${y(trade.entryPrice)}" r="4" fill="#69a8ff"/>
    ${trade.exitPrice ? `<circle cx="${x(trade.exitCandleTime!)}" cy="${y(trade.exitPrice)}" r="4" fill="#fff"/>` : ''}
    <text x="${Math.min(right - 48, x(trade.entryTime) + 6)}" y="${Math.max(top + 13, y(trade.entryPrice) - 9)}" fill="#91bfff">ENTRY</text>
    ${trade.exitPrice ? `<text x="${Math.min(right - 48, x(trade.exitCandleTime!) + 7)}" y="${Math.min(bottom - 5, y(trade.exitPrice) + 18)}" fill="#fff">STOP</text>` : ''}
    <text x="3" y="${top + 12}">${num(hi, 0)}</text><text x="3" y="${bottom}">${num(lo, 0)}</text>
    <text x="${left}" y="${height - 8}">${escape(nl(rows[0].time))}</text><text x="${right}" y="${height - 8}" text-anchor="end">${escape(nl(rows.at(-1)!.time))}</text></svg>`;
}
const summaryRow = (name: string, result: any) => `<tr><td>${escape(name)}</td><td>${result.summary.closed} / ${result.summary.open}</td>
  <td>${num(result.summary.winRatePct)}%</td><td>${money(result.summary.netPnlUsd)}</td><td>${num(result.summary.profitFactor)}</td>
  <td>${money(result.summary.maxRealizedDrawdownUsd)}</td></tr>`;
const realizedPoints = [{ x: candles[0].time, y: 0 }];
for (const point of primary.summary.equityCurve) {
  realizedPoints.push({ x: point.time, y: realizedPoints.at(-1)!.y }, { x: point.time, y: point.pnl });
}
realizedPoints.push({ x: input.cutoff / 1000, y: primary.summary.netPnlUsd });
const tradeCards = (trades: ReplayTrade[]) => trades.map(t => `<article class="trade" data-trade-id="${t.id}">
  <header><h3>#${t.id} ${t.direction.toUpperCase()} <span>${escape(nl(t.entryTime))}</span></h3><strong class="${(t.netPnl ?? 0) >= 0 ? 'gain' : 'loss'}">${t.status === 'closed' ? money(t.netPnl) + ' / ' + num(t.rMultiple) + 'R' : 'OPEN'}</strong></header>
  ${tradeChart(t)}<div class="trade-info"><span>Entry ${num(t.entryPrice)} &middot; SL ${num(t.initialStop)} &middot; ${num(t.quantityBtc, 6)} BTC</span>
  <span>${t.stops.length - 1} trailupdates &middot; ${t.exitCandleTime ? 'Exitcandle ' + escape(nl(t.exitCandleTime)) : 'Ongerealiseerd ' + money(t.unrealizedGross)}</span></div></article>`).join('');
const html = `<!doctype html><html lang="nl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC Spot - Intrusion + Daily replay</title><style>
*{box-sizing:border-box}body{margin:0;background:#10141d;color:#e2e7ee;font:14px 'Segoe UI',Arial,sans-serif;letter-spacing:0}main{max-width:2000px;margin:auto;padding:32px}
h1{font-size:28px;margin:0 0 8px}h2{font-size:20px;margin:0 0 14px}h3{font-size:15px;margin:0}p{line-height:1.6;margin:8px 0}a{color:#91bfff}section{border-top:1px solid #303845;padding:24px 0}.muted,small{color:#a0adbe}.warning{border-left:3px solid #ffc857;padding:10px 16px;margin:20px 0;background:#1d2027}.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin:24px 0}.metric strong{display:block;font-size:27px;margin-top:5px}.gain{color:#66d3b0}.loss{color:#ff8991}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{text-align:left;border-bottom:1px solid #2c3340;padding:9px 10px;font-size:13px}th{color:#a0adbe;font-weight:500}.scroll{overflow-x:auto}.trades{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.trade{border:1px solid #303845;border-radius:4px;background:#141924;overflow:hidden}.trade header{display:flex;justify-content:space-between;gap:10px;padding:14px 16px;align-items:center}.trade h3 span{font-weight:400;color:#a0adbe;margin-left:8px}.trade-info{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;color:#a0adbe;font-size:12px;padding:9px 16px 14px}svg{display:block;width:100%;height:auto}svg text{font:11px Arial;fill:#a0adbe}ul{padding-left:20px;line-height:1.6}.legend{display:flex;gap:22px;color:#a0adbe;margin-bottom:12px}.legend b{color:#bb86d9}.legend .stop{color:#ffc857}button{background:#242d3d;color:#e2e7ee;border:1px solid #465066;border-radius:4px;padding:7px 12px;cursor:pointer}footer{color:#a0adbe;font-size:12px;padding:20px 0;word-break:break-word}@media(max-width:900px){main{padding:16px}.metrics{grid-template-columns:repeat(2,1fr)}.trades{grid-template-columns:1fr}.trade header{flex-wrap:wrap}}@media print{button{display:none}.trade{break-inside:avoid}}
</style><main><h1>BTCUSDT Spot &middot; Intrusion + Daily break</h1><p class="muted">Historische replay &middot; $15 prijsrisico per entry &middot; geen TP &middot; fractal-SL + tighten-only trailing &middot; tijden Europe/Amsterdam</p>
<div class="warning"><strong>Hypothesetest, geen bewezen live rendement.</strong> De paarse lijnen worden op hun oorspronkelijke alert-candle getest. Dat de intrusion toen al bij candle-close beschikbaar was, is niet bewezen. De bewaarde intrusionhistorie is niet aantoonbaar volledig.</div>
<div class="metrics"><div class="metric">Spot-candles<strong>${candles.length.toLocaleString('nl-NL')}</strong></div><div class="metric">Paarse signalen<strong>${model.signals.length}</strong></div>
<div class="metric">Gesloten / open<strong>${primary.summary.closed} / ${primary.summary.open}</strong></div><div class="metric">Bruto resultaat<strong class="${primary.summary.netPnlUsd >= 0 ? 'gain' : 'loss'}">${money(primary.summary.netPnlUsd)}</strong></div>
<div class="metric">Winrate<strong>${num(primary.summary.winRatePct)}%</strong></div></div>
<p>Prijsdata: ${escape(nl(candles[0].time))} tot ${escape(nl(input.cutoff / 1000))}. ${history.records.length} bewaarde intrusionrecords na deduplicatie: ${escape(nl(history.records[0].time))} tot ${escape(nl(history.records.at(-1).time))}. Geen ontbrekende uren in deze prijsreeks.</p>
<section id="overview"><h2>Resultaat en kostenafhankelijkheid</h2><div class="scroll"><table><thead><tr><th>Variant</th><th>Gesloten / open</th><th>Winrate</th><th>P&amp;L gesloten</th><th>Profit factor</th><th>Max. gerealiseerde DD</th></tr></thead><tbody>
${summaryRow('Een positie tegelijk - bruto', primary)}${sensitivity.map(r => summaryRow(`Een positie - ${r.config.feeBps}bp fee + ${r.config.slippageBps}bp slippage per zijde`, r)).join('')}
${summaryRow('Ieder signaal onafhankelijk - bruto (overlap toegestaan)', independent)}</tbody></table></div>
<p class="muted">Kosten zijn scenario-aannames, geen geverifieerd tarief. Funding en onderpandbeperkingen ontbreken. De onafhankelijke variant is geen uitvoerbaar nettopositieportfolio. Open trades tellen niet als gerealiseerde winst. P&amp;L is in USD-equivalent met 1 USDT = 1 USD.</p>
<div class="warning"><strong>Concentratierisico:</strong> de beste trade levert ${money(primary.summary.bestTradeUsd)} op (${num(primary.summary.bestTradeShareOfNetPct, 1)}% van het bruto totaal). Zonder deze trade resteert ${money(primary.summary.pnlWithoutBestTradeUsd)} bruto; in het 5bp-fee/2bp-slippagescenario ${money(sensitivity[0].summary.pnlWithoutBestTradeUsd)}. Mediaan per trade: ${money(primary.summary.medianTradeUsd)}. Dit is geen bewijs van een robuust statistisch voordeel.</div>
<h3>Gerealiseerde P&amp;L &middot; &eacute;&eacute;n positie tegelijk, bruto</h3>${lineChart(realizedPoints, 1800, 200, '#66d3b0')}
<h3>Prijscontext &middot; nummers verwijzen naar de trades</h3>${lineChart(candles.map(c => ({ x: c.time, y: c.close })), 1800, 260, '#66a5ed', primary.trades.map(t => ({ time: t.signalTime, id: t.id })))}</section>
<section id="trade-overview"><h2>Trades &middot; &eacute;&eacute;n positie tegelijk</h2><div class="legend"><span><b>|</b> Paarse signaalcandle</span><span>Blauw: entry</span><span class="stop">Geel: actieve SL / trailing</span><span>Wit: stop-exit</span></div>
<p class="muted">Entry op de open van de volgende 1H-candle. Nieuwe stops gelden pas na bevestiging, vanaf de volgende candle. De exacte intrabar exittijd is onbekend.</p><div class="trades">${tradeCards(primary.trades) || '<p>Geen geldige entries.</p>'}</div></section>
<section><h2>Signaalcontrole</h2><div class="scroll"><table><thead><tr><th>Alertcandle (open)</th><th>Close / entrymoment</th><th>Richting</th><th>Gebroken D-level(s)</th><th>Primaire replay</th><th>Eerste SMTP na close</th></tr></thead><tbody>
${signalAudit.map((s: any) => { const trade = primary.trades.find(t => t.signalTime === s.time && t.direction === s.direction), skip = primary.skipped.find(t => t.signalTime === s.time && t.direction === s.direction); return `<tr><td>${escape(nl(s.time))}</td><td>${escape(nl(s.closedAt))}</td><td>${s.direction}</td><td>${s.dailyLevels.map((l: any) => num(l.price)).join(' / ')}</td><td>${trade ? '#' + trade.id : escape(skip?.reason || 'Onbekend')}</td><td>${s.smtpMinutesAfterClose === null ? 'Onbekend' : num(s.smtpMinutesAfterClose) + ' min'}</td></tr>`; }).join('')}
</tbody></table></div><p class="muted">SMTP-acceptatie is geen bewijs van eerste observatie of inboxbezorging. Onbekend is niet nul minuten.</p></section>
<section id="independent"><h2>Ieder signaal afzonderlijk &middot; diagnostisch</h2><p class="muted">Ook signalen tijdens een lopende trade, elk met eigen $15-risico en stop. Niet optellen alsof dit dezelfde live BTC-nettopositie is.</p>
<details><summary>Alle ${independent.trades.length} afzonderlijke trades</summary><div class="trades">${tradeCards(independent.trades)}</div></details></section>
<section><h2>Regels, herkomst en beperkingen</h2><p>Initial SL: meest recente geldige, bevestigde 1H Williams-fractal; 2 candles links/rechts; lookback 72; wick-buffer 0,1%; geen minimumafstand-uitbreiding; maximale afstand ${num(Number(stopEnvironment.DECENTRADER_SL_MAX_DISTANCE_PCT) * 100)}%. Trailing kiest uitsluitend nieuwere fractals, delay ${config.fractalDelay}, en verruimt de stop nooit. Dezelfde <code>buildFractalStop</code> als de live code wordt gebruikt.</p>
<p>Anti-lookahead-controle: voor elk van de ${prefixChecks.length} signalen zijn alle latere 1H- en Daily-candles verwijderd en de Daily-breaksignalen opnieuw berekend. Alle controles komen overeen. Dit bewijst niet dat de intrusion zelf op tijd beschikbaar was.</p>
<ul>${report.limitations.map(s => '<li>' + escape(s) + '</li>').join('')}</ul><p>Geen OI-, Delay-, candlekleur-, delta-, orderblock- of TP-filter toegevoegd. Daily-break en intrusionselectie gebruiken exact de bestaande paarse-overlay-evaluatoren.</p>
<p><a href="https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints">Binance Spot kline-bron</a> &middot; <a href="replay.json">Replay + stoptrajecten (JSON)</a> &middot; <a href="trades-single.csv">Trades (CSV)</a> &middot; <a href="trades-all-signals.csv">Alle signalen (CSV)</a> &middot; <a href="signals.csv">Signaalaudit (CSV)</a></p></section>
<footer>Inputcapture ${escape(input.capturedAt)} &middot; input SHA-256 ${report.inputSha256} &middot; analyse-output, geen live orders of mails</footer></main></html>`;
fs.writeFileSync(path.join(directory, 'rapport.html'), html);
console.log(JSON.stringify({ signals: model.signals.length, primary: primary.summary, independent: independent.summary,
  sensitivity: sensitivity.map(r => ({ config: r.config, summary: r.summary })), output: directory }, null, 2));
