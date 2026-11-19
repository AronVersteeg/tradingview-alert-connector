const root = document.getElementById('btcTradeReplay');
if (root && new URLSearchParams(location.search).get('source') === 'open-v2') init();

function init() {
  root.hidden = false;
  const el = id => document.getElementById(`btcReplay${id}`);
  const load = el('Load'), token = el('Token'), select = el('Trade'), windowSelect = el('Window');
  const status = el('Status'), host = el('Canvas'), cursor = el('Cursor'), play = el('Play');
  const playback = el('Playback'), time = el('Time'), legend = el('Legend');
  const date = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', dateStyle: 'short', timeStyle: 'short' });
  const clock = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', hour: '2-digit', minute: '2-digit' });
  const day = new Intl.DateTimeFormat('nl-NL', { timeZone: 'Europe/Amsterdam', month: 'short', day: 'numeric' });
  const price = new Intl.NumberFormat('nl-NL', { maximumFractionDigits: 2 });
  const hourMs = 3_600_000;
  let chart, candles, entryPoint, exitPoint, stops, fractalMarkers, snapshot, timer, nonce = 0;

  function dashboardToken() {
    const entered = token.value.trim() || document.getElementById('manualOverrideToken')?.value.trim();
    if (entered) return entered;
    try { return sessionStorage.getItem('decentraderManualOverrideToken') || ''; } catch { return ''; }
  }

  async function read(search = '') {
    const key = dashboardToken();
    if (!key) throw new Error('Voer het bestaande dashboardtoken in.');
    const response = await fetch('/open-liquidity/v2/btc-trade-replay' + search, {
      headers: { 'X-Webhook-Token': key }, cache: 'no-store'
    });
    if (response.status === 401) throw new Error('Dashboardtoken niet geaccepteerd.');
    if (!response.ok) throw new Error('dYdX-replay tijdelijk niet beschikbaar.');
    return response.json();
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
    candles = chart.addSeries(lib.CandlestickSeries, {
      upColor: '#28b9a2', downColor: '#ec6171', wickUpColor: '#28b9a2', wickDownColor: '#ec6171',
      borderVisible: false, lastValueVisible: false, priceLineVisible: false
    });
    const markerOptions = { lineVisible: false, pointMarkersVisible: true, pointMarkersRadius: 5,
      lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false };
    entryPoint = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#79a9ff' });
    exitPoint = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#eea9ce' });
    stops = chart.addSeries(lib.LineSeries, { ...markerOptions, color: '#ffc469', lineVisible: true,
      lineWidth: 2, lineType: lib.LineType.WithSteps, lineStyle: lib.LineStyle.Dashed, pointMarkersRadius: 3 });
    fractalMarkers = lib.createSeriesMarkers(candles, []);
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
    candles.setData(shown);
    entryPoint.setData(point(snapshot.trade.openedAt, snapshot.trade.entryPrice));
    exitPoint.setData(point(snapshot.trade.closedAt, snapshot.trade.exitPrice));
    const evidence = new Map();
    for (const stop of snapshot.stops || []) {
      for (const row of point(stop.at, stop.price)) evidence.set(row.time, row);
    }
    stops.setData([...evidence.values()]);
    const fractals = window.BtcHourlyFractalBoxes?.buildHourlyFractalBoxes(shown, through).fractals || [];
    fractalMarkers.setMarkers(fractals.map(row => ({ time: row.pivotTime,
      position: row.type === 'HIGH' ? 'aboveBar' : 'belowBar', color: '#a9b6c8',
      shape: row.type === 'HIGH' ? 'arrowDown' : 'arrowUp', size: .55 })));
    time.textContent = date.format(through);
    if (fit) chart.timeScale().fitContent();
  }

  function failed(error) {
    if (/token/i.test(error.message)) { token.hidden = false; token.focus(); }
    status.textContent = error.message;
  }

  async function showTrade() {
    const request = ++nonce;
    pause();
    snapshot = undefined;
    host.hidden = playback.hidden = legend.hidden = true;
    status.textContent = 'dYdX-candles laden...';
    try {
      const data = await read(`?id=${encodeURIComponent(select.value)}&window=${windowSelect.value}`);
      if (request !== nonce) return;
      snapshot = data;
      const { trade, range } = data;
      const opened = `Entry ${date.format(trade.openedAt)} @ ${price.format(trade.entryPrice)}`;
      const closed = trade.closedAt !== null && trade.exitPrice !== null
        ? `Exit ${date.format(trade.closedAt)} @ ${price.format(trade.exitPrice)}`
        : trade.status === 'OPEN' ? 'Open' : 'Exitgegevens ontbreken';
      const coverage = !data.stops?.length ? 'Geen bevestigde SL-stappen beschikbaar' : `${data.stops.length} waargenomen SL-stappen`;
      const gaps = data.candles.length < (range.through - range.from) / hourMs ? 'Candledata onvolledig' : '';
      status.textContent = [opened, closed, coverage, gaps, range.hasLater || range.hasEarlier ? 'Deelvenster' : ''].filter(Boolean).join(' | ');
      if (!data.candles.length) return;
      host.hidden = false;
      await makeChart();
      if (request !== nonce) return;
      playback.hidden = legend.hidden = false;
      windowSelect.hidden = !(range.hasLater || range.hasEarlier);
      cursor.max = String(data.candles.length);
      cursor.value = cursor.max;
      play.disabled = data.candles.length < 2;
      render(true);
    } catch (error) { if (request === nonce) failed(error); }
  }

  async function showList() {
    const request = ++nonce;
    pause();
    host.hidden = playback.hidden = legend.hidden = windowSelect.hidden = select.hidden = true;
    status.textContent = 'BTC-posities laden...';
    load.disabled = true;
    try {
      const data = await read();
      if (request !== nonce) return;
      token.hidden = true;
      select.replaceChildren();
      for (const trade of data.trades) {
        const option = document.createElement('option');
        option.value = trade.id;
        const state = trade.status === 'OPEN' ? 'open' : trade.status === 'LIQUIDATED' ? 'liquidatie' : 'gesloten';
        option.textContent = `${date.format(trade.openedAt)} | ${trade.direction.toUpperCase()} | ${state}`;
        select.append(option);
      }
      if (!select.options.length) { status.textContent = 'Geen recente BTC-posities gevonden.'; return; }
      select.hidden = false;
      windowSelect.value = 'entry';
      await showTrade();
    } catch (error) { if (request === nonce) failed(error); }
    finally { load.disabled = false; }
  }

  load.addEventListener('click', showList);
  token.addEventListener('keydown', event => { if (event.key === 'Enter' && !load.disabled) void showList(); });
  select.addEventListener('change', () => { windowSelect.value = 'entry'; void showTrade(); });
  windowSelect.addEventListener('change', showTrade);
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
