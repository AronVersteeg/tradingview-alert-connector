"""Render the offline study as a self-contained HTML report with SVG figures."""
import datetime as dt
import html
import json
import math
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
study = json.loads((out/"study.json").read_text())
rows = study["rows"]
winners = [r for r in rows if r["target"]]
others = [r for r in rows if not r["target"]]
esc = lambda value: html.escape(str(value))


def number(value, digits=2):
    return "onbekend / n.v.t." if value is None else f"{value:.{digits}f}"


def table(headers, values):
    return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+esc(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+str(c)+'</td>' for c in row)+'</tr>' for row in values)+'</tbody></table></div>'


core = [
    ("Spot-volume / voorafgaand 24u-gemiddelde", "spot_volume_ratio_24", "x"),
    ("Futures-volume / voorafgaand 24u-gemiddelde", "futures_volume_ratio_24", "x"),
    ("Spot taker-delta, in handelsrichting", "spot_delta_pct", "%"),
    ("Futures taker-delta, in handelsrichting", "futures_delta_pct", "%"),
    ("Spot laatste 5m taker-delta, in handelsrichting", "spot_last5m_delta_pct", "%"),
    ("Futures laatste 5m taker-delta, in handelsrichting", "futures_last5m_delta_pct", "%"),
    ("Futures laatste 5m absoluut netto taker-volume", "futures_last5m_delta_quote_usd", " USD"),
    ("Futures aandeel uurvolume in laatste 5m", "futures_last5m_volume_share_pct", "%"),
    ("Futures laatste 15m taker-delta, in handelsrichting", "futures_last15m_delta_pct", "%"),
    ("Bollinger-bandbreedte 20u, VOOR signaal", "spot_pre_BB_width20_pct", "%"),
    ("Ruimte tot volgende intacte D-fractal", "D_next_intact_distance_R", "R"),
    ("Verse Weekly-breaks", "W_fresh_wick_breaks", ""),
    ("Verse Monthly-breaks", "M_fresh_wick_breaks", ""),
    ("Doorbroken tegenliggende orderblocks", "OB_broken_count", ""),
    ("Oudste op dat moment doorbroken OB", "OB_oldest_break_age_hours", " uur"),
    ("Candlevolume / doorbroken cluster-pivotvolume", "OB_break_candle_to_cluster_PV", "x"),
    ("OI-verandering signaaluur, 5m label-lag", "OI_change_1h_pct_lag5", "%"),
    ("OI-verandering voorafgaande 3u, 5m label-lag", "OI_change_3h_pct_lag5", "%"),
    ("1H ADX14", "spot_adx14", ""),
    ("4H ADX14, laatst gesloten 4H-candle", "4h_adx14", ""),
    ("Laatst gepubliceerde funding rate", "funding_last_pct", "%"),
]
comparisons = []
for name, key, unit in core:
    values = [r[key] for r in others if r.get(key) is not None]
    scale = 1e6 if key.endswith('quote_usd') else 1
    suffix = ' mln USD' if scale == 1e6 else unit
    digits = 4 if key == 'funding_last_pct' else 2
    display = lambda value: number(value/scale if value is not None else None, digits)
    comparisons.append([esc(name), display(winners[0].get(key))+suffix, display(winners[1].get(key))+suffix,
        (display(min(values))+" - "+display(max(values))+suffix) if values else "onbekend"])


def scatter():
    width, height = 1100, 360
    x = lambda value: 75+value/8.5*970
    y = lambda value: 305-(value+30)/65*260
    dots = ''
    for r in rows:
        color = '#8ab4ff' if r['target'] else '#61caa8' if r['pnl'] > 0 else '#ed8c99'
        offset = 18 if r['date'] == '2026-05-28' else -6
        label = r['date'][8:10]+'-'+r['date'][5:7]
        dots += f'<circle cx="{x(r["spot_pre_BB_width20_pct"])}" cy="{y(r["futures_last5m_delta_pct"])}" r="{7 if r["target"] else 5}" fill="{color}"/><text x="{x(r["spot_pre_BB_width20_pct"])+9}" y="{y(r["futures_last5m_delta_pct"])+offset}">{label}</text>'
    return f'<svg viewBox="0 0 {width} {height}"><line x1="75" x2="1045" y1="{y(25)}" y2="{y(25)}" stroke="#8893a4" stroke-dasharray="4 5"/><text x="520" y="{y(25)-8}">25%: achteraf bekeken grens, geen gevalideerd filter</text>{dots}<text x="75" y="340">Voorafgaande BB-bandbreedte: links compacter, rechts meer uitgerekt (0 - 8,5%)</text><text x="12" y="25">Laatste 5m directionele futures-delta</text></svg>'


def flow_chart(row):
    tag = dt.datetime.fromtimestamp(row['signalTime'], dt.timezone.utc).strftime('%Y-%m-%d-%H')
    raw = json.loads((out/f'futures-5m-{tag}.json').read_text())
    event = [r for r in raw if row['signalTime']*1000 <= r[0] < (row['signalTime']+3600)*1000]
    sign = 1 if row['direction'] == 'long' else -1
    bars = ''
    for i, r in enumerate(event):
        delta = sign*(2*float(r[9])/float(r[5])-1)*100
        height = abs(delta)*1.5
        y = 130-height if delta >= 0 else 130
        bars += f'<rect x="{52+i*45}" y="{y}" width="28" height="{height}" fill="{"#62cdae" if delta>=0 else "#ee8494"}"/><text x="{52+i*45}" y="{max(20,y-5) if delta>=0 else y+height+15}">{delta:.1f}</text>'
    return f'<article><h3>{row["date"]} {row["direction"]} / ${row["pnl"]:.2f}</h3><svg viewBox="0 0 640 265"><line x1="40" x2="620" y1="130" y2="130" stroke="#596577"/>{bars}<text x="42" y="245">12 x 5 minuten van de signaalcandle; rechts de laatste 5 minuten</text></svg></article>'


rules = [[esc(r['rule']), f'{r["targetsRetained"]}/2', str(r['signals']), str(r['losses']), '$'+number(r['sumIndependentPnl'])] for r in study['fixedRules']]
audit = []
for row in study['contextAudit']:
    date = dt.datetime.fromtimestamp(row['signalTime'], dt.timezone.utc).strftime('%Y-%m-%d')
    audit.append([date, esc(row.get('coinGlassEventObservedAt') or 'ontbreekt'), number(row.get('coinGlassEventAgeMinutes')),
        number(row.get('eventSupportFrictionRatio')), 'ja' if row.get('coinGlassEventFresh15m') else 'nee',
        esc(row.get('coinGlassReviewObservedAt') or 'ontbreekt')])
ranking = [[esc(r['feature']), f'{r["orientation"]} {number(r["targetBoundary"],4)}', str(r['otherSelected']),
    str(r['missingOthers']), number(r['pairwiseAUC'],3)] for r in study['rankedFeatures'][:35]]
summary = f'''<!doctype html><html lang="nl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC impulse-onderzoek</title>
<style>*{{box-sizing:border-box}}body{{margin:0;background:#10151e;color:#e2e8ef;font:14px 'Segoe UI',Arial;letter-spacing:0}}main{{max-width:1650px;margin:auto;padding:30px}}h1{{font-size:28px;margin:0 0 8px}}h2{{font-size:21px;margin:0 0 14px}}h3{{font-size:15px}}p,li{{line-height:1.6}}section{{padding:24px 0;border-top:1px solid #303946}}table{{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}}th,td{{padding:9px 10px;text-align:left;border-bottom:1px solid #2b3542;font-size:13px}}th,.muted{{color:#a2b0c0}}.scroll{{overflow-x:auto}}.warning{{background:#20242b;border-left:3px solid #efc564;padding:12px 16px;margin:16px 0}}.metrics,.charts{{display:grid;grid-template-columns:repeat(4,1fr);gap:15px}}.metrics strong{{display:block;font-size:27px;margin-top:6px}}.charts{{grid-template-columns:1fr 1fr}}article{{border:1px solid #303946;border-radius:4px;padding:12px}}svg{{width:100%;height:auto;display:block}}svg text{{fill:#a9b8ca;font:11px Arial}}a{{color:#8ab4ff}}code{{overflow-wrap:anywhere}}@media(max-width:850px){{main{{padding:16px}}.metrics,.charts{{grid-template-columns:1fr 1fr}}.charts{{grid-template-columns:1fr}}}}</style><main>
<h1>BTC: wat onderscheidt 1 juni en 19 augustus?</h1><p class="muted">Alleen informatie tot de oorspronkelijke entry-cutoff. Vergelijking met alle 11 D+intrusion-signalen, inclusief twee overlappende signalen.</p>
<div class="warning"><strong>Exploratief, niet voorspellend bewezen.</strong> De twee grootste winnaars zijn achteraf gekozen. Veel indicatoren doorzoeken op 11 signalen levert gemakkelijk schijnbaar perfecte onderscheidingen. Geen filter of live strategie is gewijzigd.</div>
<div class="metrics"><div>Onderzochte variabelen<strong>{study['searchCalibration']['candidateVariableCount']}</strong></div><div>Volledig vergelijkbaar<strong>{study['searchCalibration']['completeVariableCount']}</strong></div><div>Historische signalen<strong>11</strong></div><div>Geselecteerde grote winnaars<strong>2</strong></div></div>
<section><h2>Belangrijkste bevindingen</h2><p><strong>De gezamenlijke aanwijzing zit meer in actieve richtingdruk aan het candle-einde, een compacte aanloop en ruimte tot volgende Daily-niveaus dan in maximaal totaalvolume.</strong> Laatste-5m futures-delta is bij beide sterker dan bij de andere negen signalen. Dit is een achteraf gevonden kandidaat, geen bewezen entryregel.</p>
{table(['Variabele','01-06 short','19-08 long','Overige 9: minimum - maximum'],comparisons)}
<p>Delta in handelsrichting = (taker buys - taker sells) / totaalvolume, vermenigvuldigd met +1 voor long of -1 voor short. De ruwe futures-delta was dus -30,44% bij de short en +25,86% bij de long in de laatste vijf minuten. Dit zijn uitgevoerde transacties, geen resting orders.</p></section>
<section><h2>1. Einddruk versus een al uitgerekte beweging</h2>{scatter()}<p>Blauw: de twee gekozen winnaars. Groen: andere positieve trades. Rood: negatieve trades. De laatste-5m futures-delta loopt van -22,19% tot +19,59% bij de negen controles, tegenover +30,44% en +25,86% bij de winnaars. De langere 15m-window scheidt ze niet: 19 augustus komt daar slechts op +3,56%.</p>
<p>Het gaat niet simpelweg om het grootste absolute geldbedrag. De laatste vijf minuten bevatten circa $156,56 mln directionele netto Futures-taker-flow op 1 juni en $44,40 mln op 19 augustus, maar de verliezende 21 augustus had circa $135,73 mln. Ook de volumeverdeling verschilt: 74,2% van het uurvolume viel op 1 juni in de laatste vijf minuten, tegenover 2,4% op 19 augustus.</p>
<div class="charts">{''.join(flow_chart(r) for r in rows if r['target'] or r['date'] in ['2026-05-23','2026-08-21'])}</div></section>
<section><h2>2. Higher timeframes, ruimte en orderblocks</h2><p>1 juni had geen verse W- of M-break; 19 augustus wel een W-break, maar geen M-break. De Monthly-break van 21 september leverde slechts een kleine winst op. Meer hogere-timeframe-bevestigingen waren dus geen monotone maat voor winst.</p>
<p>De volgende intacte Daily-fractal lag bij 1 juni op 1,22R afstand en bij 19 augustus op 2,14R. Een vaste verkenning met minstens 1R ruimte behoudt vier signalen: beide grote winnaars, de kleine winnaar van 21 september en de verliezer van 28 mei. Niet perfect, wel een samenhangende structurele richting om te blijven volgen.</p>
<p>Op de entrycandle van 1 juni werd geen bekend, intact tegenliggend 1H-orderblock nieuw gebroken. Op 19 augustus brak er wel een van 691 uur oud (28,8 dagen). Op 21 augustus brak een block van 2.296 uur oud (95,7 dagen), maar die trade verloor. Lang intact blijven of veel historisch blockvolume was op zichzelf geen betrouwbare scheider.</p>
<p>Op 19 augustus was het candlevolume 3,52x de gezamenlijke pivotvolumes van de betrokken clusters; op de verliezende 24-juni-entry was dit 3,32x. Dit is <strong>geen</strong> bewijs dat vraag/aanbod fysiek is overwonnen: pivotvolume is niet de nog openstaande hoeveelheid orders.</p></section>
<section><h2>3. OI, funding en CoinGlass-whales</h2><p>De gratis Binance metrics-archieven maken historische OI wel analyseerbaar. Met een conservatieve 5m label-lag veranderde OI in het signaaluur circa +0,01% op 1 juni en -0,62% op 19 augustus. Een gezamenlijke diepe rode OI-flush verklaart deze twee entries dus niet. Dit is een andere window dan de oudere Delay-classificatie.</p>
{table(['Datum','OI 1h lag0','OI 1h lag5 (hoofdvariant)','OI 1h lag10','OI 3h lag5'],[[r['date'],number(r.get('OI_change_1h_pct_lag0'))+'%',number(r.get('OI_change_1h_pct_lag5'))+'%',number(r.get('OI_change_1h_pct_lag10'))+'%',number(r.get('OI_change_3h_pct_lag5'))+'%'] for r in rows])}
<p>Archive-timestamps zijn niet zonder meer publicatietijden. Een gemelde labelingwijziging en veranderende waarden nabij de entry maken 0/5/10m gevoeligheidscontrole nodig. Bijvoorbeeld 21 augustus verandert van teken. Funding, RSI, ADX, MACD en ETH-confirmatie leveren hier geen unieke gedeelde verklaring op.</p>
<p>Voor 1 juni ontbreekt een CoinGlass-event-snapshot van voor entry; de bewaarde review komt van 22 juli. Die mag niet als entry-informatie tellen. Voor 19 augustus is er een snapshot van 15:02:48 UTC, ongeveer 57 minuten oud bij de hypothetische entry om 16:00 UTC. De support/friction-ratio was 1,94; bij de verliezende 21 augustus was die gunstiger: 2,78. Een hoge whale-support-ratio alleen onderscheidt de winnaars niet.</p>
{table(['Signaaldatum','CoinGlass-event observatie UTC','Leeftijd bij entry (min)','Support/friction','Vers binnen 15m?','Review-observatie UTC'],audit)}
<p>Verwijderde orders, spoofing en daadwerkelijke absorptie zijn niet met deze losse snapshots te reconstrueren. Er is geen historische L2-orderboekfilm of volledige gratis liquidation-heatmap aan deze analyse toegevoegd.</p></section>
<section><h2>4. Regelchecks, inclusief tegenvoorbeelden</h2>{table(['Regel','Grote winnaars behouden','Signalen','Negatieve trades','Som onafhankelijke bruto P&amp;L'],rules)}<p>Dit zijn sommen van afzonderlijk gesimuleerde trades, geen nieuwe live-portfolio-backtest. De twee regels met "Exploratory" zijn na inspectie gekozen. Geen van deze resultaten is out-of-sample.</p></section>
<section><h2>5. Hoe gemakkelijk is een perfecte scheiding te vinden?</h2><div class="warning">Na het doorzoeken van dezelfde {study['searchCalibration']['completeVariableCount']} complete variabelen zijn <strong>{round(study['searchCalibration']['fractionOfArbitraryPairsAtLeastAsSeparable']*55)} van alle 55 mogelijke tweetallen</strong> afzonderlijk perfect te isoleren met ten minste een eenzijdige grens. Dat is {study['searchCalibration']['fractionOfArbitraryPairsAtLeastAsSeparable']*100:.1f}%. Het feit dat ook onze twee winnaars te isoleren zijn, is dus op zichzelf zwak bewijs. Dit is een beschrijvende zoekkalibratie, geen geldige onafhankelijke significantietoets.</div>
{table(['Achteraf gerangschikte variabele','Grens bij beide winnaars','Overige signalen ook geselecteerd','Ontbrekende controles','Beschrijvende pairwise AUC'],ranking)}</section>
<section><h2>Bronnen en controles</h2><p>Binance BTCUSDT Spot en USD-M Futures 1H en 5m OHLCV/taker-volumes, funding, ETHUSDT Spot, D/W/M-fractals en gratis metrics-archieven. 1H-taker-volumes en OHLC zijn voor elke signaalcandle tegen de 12 onderliggende 5m-bars gecontroleerd. Higher-timeframe-perioden worden pas na echte sluiting gebruikt; orderblock-bevestiging en breekmetadata zijn met historische prefixes gecontroleerd.</p>
<p><a href="https://github.com/binance/binance-public-data">Binance public-data documentatie</a> | <a href="https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics">Binance OI API</a> | <a href="https://github.com/binance/binance-public-data/issues/491">Gemelde metrics-timestampwijziging (onbevestigde issue)</a> | <a href="https://docs.coinglass.com/reference/spot-orderbook-heatmap">CoinGlass historische orderboeken en vereiste abonnementen</a></p>
<ul>{''.join('<li>'+esc(note)+'</li>' for note in study['limitations'])}</ul>
<p><a href="features.csv">Alle features per signaal (CSV)</a> | <a href="study.json">Volledige analyse (JSON)</a> | <a href="structure.json">Fractals en orderblocks (JSON)</a> | <a href="manifest.json">Bronnen, checksums en capture</a></p></section></main></html>'''
(out/'rapport.html').write_text(summary)
print(out/'rapport.html')
