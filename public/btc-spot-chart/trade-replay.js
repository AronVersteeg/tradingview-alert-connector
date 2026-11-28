const root = document.getElementById('btcTradeReplay');
const markets = new Map([['open-v2', 'BTC-USD'], ['open-v2-eth', 'ETH-USD'], ['open-v2-inj', 'INJ-USD'], ['open-v2-sol', 'SOL-USD'], ['open-v2-zec', 'ZEC-USD'], ['open-v2-wld', 'WLD-USD']]);
const market = markets.get(new URLSearchParams(location.search).get('source'));
if (root && market) init(market);

function init(market) {
  const asset = market.split('-')[0];
  root.hidden = false;
  const el = id => document.getElementById(`btcReplay${id}`);
  const title = el('Title'), strategySwitch = el('Strategy');
  const load = el('Load'), select = el('Trade'), summary = el('Summary'), events = el('Events');
  select.setAttribute('aria-label', `Gesimuleerde ${asset}-trade`);
  const picker = el('TradePicker'), totalPnl = el('TotalPnl');
  const riskControls = el('RiskControls'), baseRisk = el('BaseRisk'), testRisk = el('TestRisk');
  const riskError = el('RiskError');
  const status = el('Status'), host = el('Canvas'), cursor = el('Cursor'), play = el('Play');
  const playback = el('Playback'), time = el('Time'), legend = el('Legend');
  const date = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', dateStyle: 'short', timeStyle: 'short' });
  const clock = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', hour: '2-digit', minute: '2-digit' });
  const day = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', month: 'short', day: 'numeric' });
  const priceDigits = asset === 'BTC' ? 2 : 4;
  const price = new Intl.NumberFormat('nl-NL', { maximumFractionDigits: priceDigits });
  const pnlMoney = new Intl.NumberFormat('nl-NL', { style: 'currency', currency: 'USD', currencyDisplay: 'narrowSymbol', signDisplay: 'exceptZero' });
  const quantity = new Intl.NumberFormat('nl-NL', { maximumFractionDigits: 6 });
  const hourMs = 3_600_000;
  let chart, candles, entryPoint, exitPoint, stops, fractalMarkers, annotations, snapshot, timer, nonce = 0;
  let list, riskBudget, riskFactor = 1;
  let strategy = 'green', listRequest = 0;
  const strategies = { green: { name: 'Green', color: '#68e2a6' }, purple: { name: 'Purple', color: '#d5a4fa' },
    atr: { name: 'ATR', color: '#238554' }, 'atr-purple': { name: 'ATR Purple', color: '#f18ebc' },
    'atr-purple-plus': { name: 'ATR Purple +', color: '#376cce' } };
  const strategyName = () => strategies[strategy].name;
  const entryColor = () => strategies[strategy].color;
  function showStrategy() {
    root.dataset.strategy = strategy;
    title.textContent = `${asset} ${strategyName()} Retest`;
    legend.querySelector('.btc-replay-entry').textContent = `${strategyName()}-entry`;
  }
  showStrategy();

  async function read(id) {
    const selectedStrategy = strategy;
    const label = strategyName();
    const query = new URLSearchParams({ market, strategy: selectedStrategy });
    if (id) query.set('id', id);
    const response = await fetch(`/open-liquidity/v2/green-retest?${query}`, { cache: 'no-store' });
    if (!response.ok) throw new Error(`${asset} ${label}-retest tijdelijk niet beschikbaar.`);
    const data = await response.json();
    if (data.market !== market || data.strategy !== selectedStrategy) throw new Error(`Onverwachte markt of strategie in de ${asset} ${label}-retest.`);
    return data;
  }

  async function makeChart() {
    if (chart) return;
    const lib = await import('./vendor/lightweight-charts-5.2.1.mjs');
    if (chart) return;
    chart = lib.createChart(host, {
      autoSize: true,
      layout: { background: { color: '#141a23' }, textColor: '#a9b6c8' },
      grid: { vertLines: { visible: false }, horzLines: { color: '#242e3b' } },
      rightPriceScale: { borderColor: '#35435a' },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#35435a',
        tickMarkFormatter: (stamp, kind) => (kind >= lib.TickMarkType.Time ? clock : day).format(stamp * 1000) },
      localization: { timeFormatter: stamp => date.format(stamp * 1000) }
    });
    const priceFormat = { type: 'price', precision: priceDigits, minMove: 10 ** -priceDigits };
    candles = chart.addSeries(lib.CandlestickSeries, {
      upColor: '#28b9a2', downColor: '#ec6171', wickUpColor: '#28b9a2', wickDownColor: '#ec6171',
      borderVisible: false, lastValueVisible: false, priceLineVisible: false, priceFormat
    });
    const markerOptions = { lineVisible: false, pointMarkersVisible: true, pointMarkersRadius: 5,
      lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false, priceFormat };
    entryPoint = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#68e2a6' });
    exitPoint = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#eea9ce' });
    stops = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#69aaff', lineVisible: true,
      lineWidth: 2, lineType: lib.LineType.WithSteps, pointMarkersRadius: 3 });
    fractalMarkers = lib.createSeriesMarkers(candles, []);
    annotations = {
      labels: [], pivots: [], sizing: null,
      autoscaleInfo() { return this.sizing ? { priceRange: { minValue: this.sizing.price, maxValue: this.sizing.price } } : null; },
      paneViews() { return [{ renderer: () => ({ draw: target => target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
        const x = at => chart.timeScale().timeToCoordinate(Math.floor(at / hourMs) * 3600);
        ctx.save(); ctx.font = '11px sans-serif';
        if (this.sizing) {
          const left = x(this.sizing.from), right = x(this.sizing.to), y = candles.priceToCoordinate(this.sizing.price);
          if (left !== null && right !== null && y !== null && right > left) {
            ctx.strokeStyle = '#a9b6c8'; ctx.setLineDash([3, 4]);
            ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke();
            ctx.setLineDash([]);
          }
        }
        let previousLabel = -100;
        for (const line of this.labels) {
          const pos = x(line.at);
          if (pos === null || pos < 0 || pos > mediaSize.width) continue;
          ctx.strokeStyle = line.color; ctx.globalAlpha = .6;
          ctx.beginPath(); ctx.moveTo(pos, 20); ctx.lineTo(pos, mediaSize.height); ctx.stroke();
          if (pos - previousLabel >= 24) {
            ctx.globalAlpha = 1; ctx.fillStyle = line.color;
            ctx.fillText(line.label, Math.min(pos + 3, mediaSize.width - 25), 14);
            previousLabel = pos;
          }
        }
        ctx.globalAlpha = 1; ctx.strokeStyle = '#f9a365';
        for (const f of this.pivots) {
          const left = x(f.at), right = x(f.confirmedAt), y = candles.priceToCoordinate(f.price);
          if (left === null || right === null || y === null) continue;
          ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke();
          ctx.setLineDash([]); ctx.beginPath(); ctx.arc(right, y, 3.4, 0, Math.PI * 2); ctx.stroke();
        }
        ctx.restore();
      }) }) }]; }
    };
    candles.attachPrimitive(annotations);
  }

  function pause() {
    clearInterval(timer);
    timer = undefined;
    play.textContent = '\u25B6';
    play.title = 'Afspelen';
    play.setAttribute('aria-label', play.title);
  }

  function render(fit) {
    if (!snapshot?.candles.length) return;
    const shown = snapshot.candles.slice(0, Number(cursor.value));
    const through = (shown.at(-1).time + 3600) * 1000;
    const times = new Set(shown.map(row => row.time));
    const candleTime = at => Math.floor(at / hourMs) * 3600;
    const point = (at, value) => Number.isFinite(at) && Number.isFinite(value) && at < through && times.has(candleTime(at))
      ? [{ time: candleTime(at), value }] : [];
    const color = strategy === 'green' ? '#58ce92' : entryColor();
    candles.setData([...shown.map(row => row.time === candleTime(snapshot.trade.signalAt)
      ? { ...row, color, wickColor: color } : row), { time: through / 1000 }]);
    entryPoint.applyOptions({ color: entryColor() });
    entryPoint.setData(point(snapshot.trade.openedAt, snapshot.trade.entryPrice));
    exitPoint.setData(point(snapshot.trade.closedAt, snapshot.trade.exitPrice));
    const trade = snapshot.trade;
    const path = new Map();
    for (const stop of trade.stops) {
      for (const row of point(stop.at, stop.price)) path.set(row.time, row);
      if (stop.at === through && stop.at > trade.openedAt) path.set(through / 1000, { time: through / 1000, value: stop.price });
    }
    const endpoint = Math.min(trade.closedAt ?? Infinity, shown.at(-1).time * 1000);
    const lastStop = trade.stops.filter(step => step.at <= endpoint).at(-1);
    if (lastStop && times.has(candleTime(endpoint))) path.set(candleTime(endpoint), { time: candleTime(endpoint), value: lastStop.price });
    stops.setData([...path.values()].sort((left, right) => left.time - right.time));
    const fractals = window.BtcHourlyFractalBoxes?.buildHourlyFractalBoxes(shown, through).fractals || [];
    fractalMarkers.setMarkers(fractals.map(row => ({ time: row.pivotTime,
      position: row.type === 'HIGH' ? 'aboveBar' : 'belowBar', color: '#a9b6c8',
      shape: row.type === 'HIGH' ? 'arrowDown' : 'arrowUp', size: .55 })));
    annotations.labels = [
      { at: trade.openedAt, label: 'E', color: entryColor() },
      ...trade.yellows.map((row, index) => ({ at: row.at, label: 'Y' + (index + 1), color: '#f3d25c' })),
      ...(trade.closedAt === undefined ? [] : [{ at: trade.closedAt, label: 'X', color: '#eea9ce' }])
    ].filter(row => row.at < through || row.at === through && row.label.startsWith('Y')).sort((a, b) => a.at - b.at);
    annotations.pivots = [...new Map([trade.sizingFractal, ...trade.yellows
      .filter(row => row.at <= through && row.result === 'TIGHTENED').map(row => row.fractal)]
      .filter(f => f && f.confirmedAt <= through).map(f => [f.at + '|' + f.price, f])).values()];
    annotations.sizing = trade.openedAt < through ? {
      from: Math.max(shown[0].time * 1000, trade.sizingFractal?.at ?? trade.signalAt),
      to: endpoint, price: trade.sizingStop
    } : null;
    for (const row of events.children) {
      const at = Number(row.dataset.at);
      row.hidden = at > through || at === through && !row.classList.contains('btc-replay-yellow-event');
    }
    time.textContent = date.format(through);
    if (fit) chart.timeScale().fitContent();
  }

  function pnl(trade) {
    if (trade.status === 'CLOSED' && Number.isFinite(trade.grossPnl)) {
      return { label: 'Geschatte bruto P&L', value: trade.grossPnl * riskFactor, note: 'Gesloten in de retest' };
    }
    if (trade.status === 'OPEN' && Number.isFinite(trade.markToMarket?.grossPnl)) {
      return { label: 'Ongerealiseerde bruto P&L', value: trade.markToMarket.grossPnl * riskFactor,
        note: `dYdX-close ${date.format(trade.markToMarket.at)} @ $${price.format(trade.markToMarket.price)}` };
    }
    return { label: 'Geschatte bruto P&L', note: 'Onbekend' };
  }

  function tradeSummary(trade) {
    const estimate = pnl(trade);
    const entries = [
      [estimate.label, estimate.value === undefined ? '—' : pnlMoney.format(estimate.value), estimate.note],
      ['Begin', `${trade.direction.toUpperCase()} · ${date.format(trade.openedAt)} · $${price.format(trade.entryPrice)}`],
      ['Gebroken gaprand', '$' + price.format(trade.gapEdge)],
      ...(trade.dailyLevels?.length ? [['Gebroken Daily (Spot)', trade.dailyLevels.map(value => '$' + price.format(value)).join(' / ')]] : []),
      ['Sizing-SL', '$' + price.format(trade.sizingStop)],
      ['Initiële SL', '$' + price.format(trade.initialStop)],
      ['Positiegrootte (model)', `${quantity.format(trade.size * riskFactor)} ${asset}`],
      ['Initieel prijsrisico', '$' + price.format(trade.riskUsd * riskFactor)],
      ['TP', 'Geen'],
      ['Exitcandle', trade.closedAt === undefined ? (trade.status === 'INCOMPLETE' ? 'Onbekend: dYdX-data ontbreekt' : 'Open in retest')
        : `${date.format(trade.closedAt)} · $${price.format(trade.exitPrice)}`]
    ];
    summary.replaceChildren(...entries.map(([label, value, note], index) => {
      const pair = document.createElement('div'), title = document.createElement('dt'), data = document.createElement('dd');
      title.textContent = label; data.textContent = value; pair.append(title, data);
      if (index === 0) {
        pair.className = 'btc-replay-pnl';
        if (estimate.value > 0) data.className = 'btc-replay-gain';
        else if (estimate.value < 0) data.className = 'btc-replay-loss';
        const caption = document.createElement('small');
        caption.textContent = note;
        data.append(caption);
      }
      return pair;
    }));
  }

  function details(trade) {
    tradeSummary(trade);
    const steps = [
      [trade.openedAt, 'E', `${strategyName()}-entry · initiële SL op de dYdX-open van ${date.format(trade.signalAt)}`],
      ...trade.yellows.map((row, index) => [row.at, 'Y' + (index + 1),
        (row.result === 'TIGHTENED' ? `SL naar $${price.format(row.stop)}` : `SL blijft $${price.format(row.stop)}`) +
        (row.fractal ? ` · pivot ${date.format(row.fractal.at)}, bevestigd ${date.format(row.fractal.confirmedAt)}` : '')]),
      ...(trade.closedAt === undefined ? [] : [[trade.closedAt, 'X', trade.exitReason === 'GAP_STOP'
        ? 'Uuropen voorbij de stop' : 'Stop geraakt binnen deze candle']])
    ];
    events.replaceChildren(...steps.map(([at, label, description]) => {
      const row = document.createElement('li'), badge = document.createElement('b'), text = document.createElement('span');
      row.dataset.at = String(at); badge.textContent = `${label} · ${date.format(at)}`; text.textContent = description;
      if (label.startsWith('Y')) row.className = 'btc-replay-yellow-event';
      row.append(badge, text);
      return row;
    }));
  }

  async function showTrade() {
    const request = ++nonce;
    pause();
    snapshot = undefined;
    host.hidden = playback.hidden = legend.hidden = summary.hidden = events.hidden = true;
    status.textContent = 'dYdX-candles laden...';
    try {
      const data = await read(select.value);
      if (request !== nonce) return;
      snapshot = data;
      const { trade } = data;
      tradeStatus(data);
      details(trade);
      summary.hidden = events.hidden = false;
      if (!data.candles.length) return;
      host.hidden = false;
      await makeChart();
      if (request !== nonce) return;
      playback.hidden = legend.hidden = false;
      cursor.max = String(data.candles.length);
      cursor.value = cursor.max;
      play.disabled = data.candles.length < 2;
      render(true);
    } catch (error) { if (request === nonce) status.textContent = error.message; }
  }

  function tradeStatus(data) {
    status.textContent = [
      `Binance Spot-signaal · dYdX-uurdata · $${price.format(riskBudget)} modelbudget · vóór kosten`,
      coverageText(data),
      Number.isInteger(data.tradeCount) ? `${data.tradeCount} gesimuleerde trades` : '',
      data.missingDydxHours > 0 ? `${data.missingDydxHours} dYdX-uren ontbreken` : '',
      Number.isFinite(data.skippedWhileOpen) && data.skippedWhileOpen > 0
        ? `${data.skippedWhileOpen} signalen overgeslagen tijdens open trades` : ''
    ].filter(Boolean).join(' | ');
  }

  function coverageText(data) {
    if (!Number.isFinite(data.from) || !Number.isFinite(data.through)) return '';
    const through = data.incompleteAt ?? data.through;
    const range = `${date.format(data.from)} – ${date.format(through)}`;
    if (data.incompleteAt !== undefined) return `${range} (gestopt bij ontbrekende dYdX-candle)`;
    if (data.historyCoverage === 'archive-only') return `${range} (archief, geen recente aanvulling)`;
    if (data.historyCoverage === 'archive-plus-recent') return `${range} (archief + recent)`;
    return range;
  }

  function applyRiskScenario() {
    if (!list) return;
    const raw = testRisk.value.trim();
    const custom = Number(raw.replace(',', '.'));
    const valid = /^(?:\d+(?:[.,]\d{0,2})?|[.,]\d{1,2})$/.test(raw) && custom >= 0.01 && custom <= 1_000_000;
    const invalid = Boolean(raw) && !valid;
    testRisk.setAttribute('aria-invalid', String(invalid));
    if (invalid) testRisk.setAttribute('aria-describedby', riskError.id);
    else testRisk.removeAttribute('aria-describedby');
    riskError.hidden = !invalid;
    riskBudget = valid ? custom : list.riskBudgetUsd;
    // The retest has no size-dependent gates or costs, so size and P&L scale together.
    riskFactor = riskBudget / list.riskBudgetUsd;

    let total = 0, known = 0;
    list.trades.forEach((trade, index) => {
      const state = trade.status === 'OPEN' ? 'open' : trade.status === 'INCOMPLETE' ? 'onvolledig' : 'gesloten';
      const { value } = pnl(trade);
      if (value !== undefined) { total += value; known++; }
      select.options[index].textContent = `${value === undefined ? 'P&L —' : pnlMoney.format(value)} | ${state} | ${trade.direction.toUpperCase()} | ${date.format(trade.openedAt)}`;
    });
    const amount = known ? pnlMoney.format(total) : '\u2014';
    const description = 'Totaalresultaat van de retest, v\u00f3\u00f3r kosten, inclusief bekende ongerealiseerde P&L.' +
      (known < list.trades.length ? ` ${list.trades.length - known} zonder P&L niet meegeteld.` : '');
    totalPnl.value = amount;
    totalPnl.dataset.result = !known || amount === pnlMoney.format(0) ? 'flat' : total > 0 ? 'gain' : 'loss';
    totalPnl.title = description;
    totalPnl.setAttribute('aria-label', `${description} ${amount}`);
    if (snapshot) {
      tradeSummary(snapshot.trade);
      tradeStatus(snapshot);
    }
  }

  async function showList() {
    const request = ++nonce;
    const listId = ++listRequest;
    pause();
    snapshot = list = undefined;
    host.hidden = playback.hidden = legend.hidden = summary.hidden = events.hidden = picker.hidden = riskControls.hidden = true;
    status.textContent = `${strategyName()}-signalen en dYdX-candles laden...`;
    load.disabled = true;
    try {
      const data = await read();
      if (request !== nonce) return;
      if (!Number.isFinite(data.riskBudgetUsd) || data.riskBudgetUsd <= 0) throw new Error('Standaardrisico van de retest ontbreekt.');
      list = { riskBudgetUsd: data.riskBudgetUsd, trades: data.trades.slice().reverse() };
      select.replaceChildren(...list.trades.map(trade => {
        const option = document.createElement('option');
        option.value = trade.id;
        return option;
      }));
      baseRisk.value = '$' + price.format(list.riskBudgetUsd);
      testRisk.placeholder = price.format(list.riskBudgetUsd);
      applyRiskScenario();
      riskControls.hidden = false;
      if (!select.options.length) {
        status.textContent = [`Geen gesimuleerde entries · ${data.candidateCount} ${strategyName()}-signalen`, coverageText(data)].filter(Boolean).join(' | ');
        return;
      }
      picker.hidden = false;
      await showTrade();
    } catch (error) { if (request === nonce) status.textContent = error.message; }
    finally { if (listId === listRequest) load.disabled = false; }
  }

  load.addEventListener('click', showList);
  strategySwitch.addEventListener('change', event => {
    if (!Object.hasOwn(strategies, event.target.value) || strategy === event.target.value) return;
    strategy = event.target.value;
    showStrategy();
    showList();
  });
  select.addEventListener('change', showTrade);
  testRisk.addEventListener('input', applyRiskScenario);
  cursor.addEventListener('input', () => { pause(); render(true); });
  play.addEventListener('click', () => {
    if (timer) return pause();
    if (Number(cursor.value) >= Number(cursor.max)) cursor.value = '1';
    play.textContent = '\u23F8';
    play.title = 'Pauzeren';
    play.setAttribute('aria-label', play.title);
    render(true);
    timer = setInterval(() => {
      cursor.value = String(Number(cursor.value) + 1);
      render(true);
      if (Number(cursor.value) >= Number(cursor.max)) pause();
    }, 420);
  });
  window.addEventListener('pagehide', pause);
}
