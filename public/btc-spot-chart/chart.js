(function () {
  'use strict';
  if (new URLSearchParams(location.search).get('source') !== 'open-v2') return;
  const panel = document.getElementById('btcSpotChartPanel');
  if (!panel) return;
  panel.hidden = false;
  const assetBase = new URL('.', document.currentScript.src);
  const data = window.BtcSpotChartData;
  const fractals = window.BtcSpotFractals;
  const hourlyBoxes = window.BtcHourlyFractalBoxes;
  const byId = (id) => document.getElementById(id);
  const api = '/open-liquidity/v2/btc-spot-candles';
  const streamUrls = ['wss://stream.binance.com:443/ws/btcusdt@kline_1h', 'wss://stream.binance.com:9443/ws/btcusdt@kline_1h'];
  const money = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const volumeFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
  let zone = 'Europe/Amsterdam';
  let library, chart, candlesSeries, volumeSeries, initialization;
  let candles = [], socket, reconnectTimer, watchdog;
  let active = false, inView = false, destroyed = false, failures = 0;
  let lastTickAt = 0, lastRestAt = 0, lastSourceTime = 0;
  let recentPending, olderPending, olderRetryAt = 0, hasMore = true;
  let initialData = false, hovering = false;
  let fractalLayer, fractalBundle, fractalModel, fractalPending, fractalRetryAt = 0, fractalKey;
  let fitFractals = false;
  let hourlyBoxLayer, hourlyBoxModel;

  function projectionStatus(text, stale = false) {
    byId('spotChartFractalStatus').textContent = text;
    byId('spotChartFractalStatus').dataset.stale = String(stale);
  }
  function projectLoadedCandles() {
    if (!fractalBundle || !fractalLayer) return;
    try {
      fractalModel = fractals.projectFractals(fractalBundle, candles);
      fractalLayer.setData(fractalModel.levels);
      projectionStatus(`D/W closed through ${dateText(Date.parse(fractalModel.evaluatedThrough) / 1000, { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })}`);
      if (fitFractals) candlesSeries.applyOptions({ autoscaleInfoProvider: fractalAutoscale });
    } catch (error) {
      projectionStatus(`D/W pending: ${error.message}`, true);
    }
  }
  function fractalAutoscale(base) {
    const result = base();
    if (!result || !fitFractals || !fractalLayer) return result;
    const prices = fractalLayer.visibleLevels().filter((level) => level.isCurrent && !level.broken).map((level) => level.price);
    return { ...result, priceRange: {
      minValue: Math.min(result.priceRange.minValue, ...prices),
      maxValue: Math.max(result.priceRange.maxValue, ...prices)
    } };
  }
  async function refreshFractals(force = false) {
    if (!initialData || !active || destroyed || fractalPending) return fractalPending;
    const hour = Math.floor(Date.now() / 3_600_000) * 3_600_000;
    if (!force && (Date.parse(fractalModel?.evaluatedThrough || '') >= hour || Date.now() < fractalRetryAt)) return;
    // One shared higher-timeframe read per closed hour, never one read per live tick.
    fractalRetryAt = Date.now() + 5000;
    fractalPending = (async () => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 25_000);
      try {
        const response = await fetch('/open-liquidity/v2/btc-spot-fractal-projections', { signal: controller.signal, cache: 'no-store' });
        if (!response.ok) {
          const retry = Number(response.headers.get('Retry-After'));
          fractalRetryAt = Date.now() + Math.max(60, Number.isFinite(retry) ? retry : 60) * 1000;
          throw new Error(`source unavailable (${response.status})`);
        }
        const bundle = await response.json();
        if (destroyed) return;
        if (Date.parse(bundle.evaluatedThrough) < hour) throw new Error('awaiting current closed hour');
        fractalBundle = bundle;
        const lastClosed = candles.filter((candle) => candle.closed).at(-1);
        if (!lastClosed || (lastClosed.time + 3600) * 1000 < Date.parse(bundle.evaluatedThrough)) await loadRecent();
        if (!destroyed) projectLoadedCandles();
      } catch (error) {
        if (!destroyed) projectionStatus(`D/W stale: ${error.name === 'AbortError' ? 'request timed out' : error.message}`, true);
      } finally { clearTimeout(timeout); fractalPending = undefined; }
    })();
    return fractalPending;
  }

  function dateText(time, options) {
    return new Intl.DateTimeFormat('nl-NL', { timeZone: zone, ...options }).format(new Date(time * 1000));
  }
  function setFeed(state, text) {
    byId('spotChartFeed').dataset.state = state;
    byId('spotChartFeed').textContent = text;
  }
  function feedText() {
    if (!active) { setFeed('paused', 'Paused'); return; }
    const count = `${candles.length.toLocaleString('en-US')} candles`;
    const clock = lastSourceTime ? dateText(lastSourceTime / 1000, { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '-';
    if (lastTickAt && Date.now() - lastTickAt < 12_000) setFeed('live', `Live | ${clock} | ${count}`);
    else if (initialData) setFeed('retry', `REST | ${clock} | ${count}`);
    else setFeed('connecting', 'Connecting');
  }
  function message(text, error = false) {
    const node = byId('spotChartMessage');
    node.hidden = !text;
    node.textContent = text || '';
    node.dataset.error = String(error);
  }
  function legend(candle) {
    if (!candle) return;
    byId('spotChartOhlc').dataset.direction = candle.close >= candle.open ? 'up' : 'down';
    for (const key of ['open', 'high', 'low', 'close']) byId(`spotChart${key[0].toUpperCase()}${key.slice(1)}`).textContent = money.format(candle[key]);
    byId('spotChartVolumeValue').textContent = volumeFormat.format(candle.volume);
    byId('spotChartCandleTime').textContent = dateText(candle.time, { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });
  }
  function bar(candle) {
    return { time: candle.time, open: candle.open, high: candle.high, low: candle.low, close: candle.close };
  }
  function volumeBar(candle) {
    return { time: candle.time, value: candle.volume, color: candle.close >= candle.open ? '#26a69a66' : '#ef535066' };
  }
  function applyCandles(next, singleUpdate) {
    const range = chart.timeScale().getVisibleLogicalRange();
    const oldFirst = candles[0]?.time;
    const following = initialData && range && range.to >= candles.length - 2;
    const wasInitial = initialData;
    candles = next;
    if (singleUpdate && wasInitial && oldFirst === next[0]?.time) {
      candlesSeries.update(bar(singleUpdate));
      volumeSeries.update(volumeBar(singleUpdate));
    } else {
      candlesSeries.setData(candles.map(bar));
      volumeSeries.setData(candles.map(volumeBar));
      if (wasInitial && range && oldFirst !== undefined) {
        const offset = candles.findIndex((candle) => candle.time === oldFirst);
        if (offset >= 0) chart.timeScale().setVisibleLogicalRange({ from: range.from + offset, to: range.to + offset });
      }
    }
    initialData = true;
    if (!wasInitial) {
      selectRange(window.innerWidth < 680 ? '60' : '120');
      window.dispatchEvent(new CustomEvent('btc-spot-chart:ready', { detail: window.btcSpotChart }));
    } else if (following) chart.timeScale().scrollToRealTime();
    if (!hovering) legend(candles[candles.length - 1]);
    message('');
    feedText();
    const lastClosed = candles.filter((candle) => candle.closed).at(-1)?.time;
    const key = `${candles[0]?.time}|${lastClosed}|${candles.length}`;
    if (key !== fractalKey || !singleUpdate) {
      hourlyBoxModel = hourlyBoxes.buildHourlyFractalBoxes(candles);
      hourlyBoxLayer.setData(hourlyBoxModel.boxes, candles);
    }
    if (key !== fractalKey) { fractalKey = key; projectLoadedCandles(); }
    void refreshFractals();
  }
  function selectRange(value) {
    if (!chart || !candles.length) return;
    if (value === 'all') chart.timeScale().fitContent();
    else chart.timeScale().setVisibleLogicalRange({ from: Math.max(0, candles.length - Number(value)), to: candles.length + 5 });
    chart.priceScale('right').applyOptions({ autoScale: true });
    panel.querySelectorAll('[data-spot-range]').forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.spotRange === String(value)));
    });
  }
  function zoom(factor) {
    const range = chart?.timeScale().getVisibleLogicalRange();
    if (!range) return;
    const span = Math.max(12, Math.min(10000, (range.to - range.from) * factor));
    const center = (range.to + range.from) / 2;
    chart.timeScale().setVisibleLogicalRange({ from: center - span / 2, to: center + span / 2 });
    panel.querySelectorAll('[data-spot-range]').forEach((button) => button.setAttribute('aria-pressed', 'false'));
  }

  async function ensureChart() {
    if (initialization) return initialization;
    initialization = (async () => {
      library = await import(new URL('vendor/lightweight-charts-5.2.1.mjs', assetBase).href);
      if (destroyed) return;
      chart = library.createChart(byId('spotChartCanvas'), {
        autoSize: true,
        layout: { background: { type: library.ColorType.Solid, color: '#131722' }, textColor: '#b2b5be', fontSize: 12, fontFamily: '-apple-system, BlinkMacSystemFont, Segoe UI, Arial, sans-serif', attributionLogo: true },
        grid: { vertLines: { color: '#242731', visible: byId('spotChartGrid').checked }, horzLines: { color: '#242731', visible: byId('spotChartGrid').checked } },
        crosshair: { mode: library.CrosshairMode.Normal, vertLine: { color: '#787b86', labelBackgroundColor: '#363a45' }, horzLine: { color: '#787b86', labelBackgroundColor: '#363a45' } },
        rightPriceScale: { borderColor: '#2a2e39', minimumWidth: 80, scaleMargins: { top: .08, bottom: byId('spotChartVolume').checked ? .23 : .08 } },
        timeScale: { borderColor: '#2a2e39', timeVisible: true, secondsVisible: false, rightOffset: 5, barSpacing: 8,
          tickMarkFormatter: (time, type) => dateText(time, type <= library.TickMarkType.DayOfMonth ? { day: 'numeric', month: 'short' } : { hour: '2-digit', minute: '2-digit' }) },
        localization: { locale: 'en-US', priceFormatter: (value) => money.format(value), timeFormatter: (time) => dateText(time, { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) },
        handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
        handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true, axisDoubleClickReset: true }
      });
      candlesSeries = chart.addSeries(library.CandlestickSeries, {
        upColor: '#26a69a', downColor: '#ef5350', borderVisible: false,
        wickUpColor: '#26a69a', wickDownColor: '#ef5350',
        priceFormat: { type: 'price', precision: 2, minMove: .01 }, autoscaleInfoProvider: fractalAutoscale
      });
      fractalLayer = new fractals.FractalPrimitive();
      candlesSeries.attachPrimitive(fractalLayer);
      hourlyBoxLayer = new hourlyBoxes.HourlyFractalBoxPrimitive();
      hourlyBoxLayer.setVisible(byId('spotChartHourlyBoxes').checked);
      candlesSeries.attachPrimitive(hourlyBoxLayer);
      volumeSeries = chart.addSeries(library.HistogramSeries, {
        priceFormat: { type: 'volume' }, priceScaleId: 'volume', lastValueVisible: false, priceLineVisible: false,
        visible: byId('spotChartVolume').checked
      });
      volumeSeries.priceScale().applyOptions({ scaleMargins: { top: .82, bottom: 0 } });
      chart.subscribeCrosshairMove((param) => {
        const value = param.seriesData.get(candlesSeries);
        hovering = Boolean(value && param.point);
        legend(hovering ? candles.find((candle) => candle.time === value.time) : candles[candles.length - 1]);
      });
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (range && range.from < 35 && initialData && active) void loadOlder();
      });
      window.btcSpotChart = Object.freeze({
        symbol: 'BTCUSDT', venue: 'Binance Spot', interval: '1h', chart,
        candleSeries: candlesSeries, volumeSeries,
        getCandles: () => candles.map((candle) => ({ ...candle })),
        getFractalProjections: () => fractalModel ? { ...fractalModel, levels: fractalModel.levels.map((level) => ({ ...level })) } : null,
        getHourlyFractalBoxes: () => hourlyBoxModel ? { evaluatedThrough: hourlyBoxModel.evaluatedThrough,
          boxes: hourlyBoxModel.boxes.map((box) => ({ ...box })), fractals: hourlyBoxModel.fractals.map((fractal) => ({ ...fractal })) } : null
      });
      watchdog = setInterval(() => {
        if (!active) return;
        feedText();
        void refreshFractals();
        if (!lastTickAt || Date.now() - lastTickAt > 12_000) {
          if (Date.now() - lastRestAt >= 30_000) void loadRecent();
          if (socket && Date.now() - (lastTickAt || lastRestAt) > 20_000) socket.close();
        }
      }, 5_000);
    })().catch((error) => {
      initialization = undefined;
      message('Chart could not be loaded. Use refresh to retry.', true);
      setFeed('retry', error.message || 'Chart unavailable');
      throw error;
    });
    return initialization;
  }

  async function fetchPage(before) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20_000);
    try {
      const url = new URL(api, location.origin);
      if (before !== undefined) url.searchParams.set('before', String(before));
      const response = await fetch(url, { signal: controller.signal, cache: 'no-store' });
      if (!response.ok) throw new Error(`Binance chart feed unavailable (${response.status})`);
      const page = await response.json();
      if (page.symbol !== 'BTCUSDT' || page.venue !== 'Binance Spot' || page.interval !== '1h' ||
        !Array.isArray(page.candles) || !Number.isFinite(Date.parse(page.fetchedAt))) throw new Error('Unexpected chart data source');
      const version = Date.parse(page.fetchedAt);
      return { ...page, candles: page.candles.map((candle) => ({ ...candle, version })) };
    } finally { clearTimeout(timeout); }
  }
  async function loadRecent() {
    if (recentPending || destroyed) return recentPending;
    lastRestAt = Date.now();
    byId('spotChartRefresh').disabled = true;
    recentPending = (async () => {
      try {
        const page = await fetchPage();
        if (destroyed) return;
        if (!page.candles.length) throw new Error('No BTCUSDT candles returned');
        const next = data.mergeCandles(candles, page.candles);
        if (!next.length) throw new Error('No valid BTCUSDT candles returned');
        hasMore = hasMore && page.hasMore;
        lastSourceTime = Math.max(lastSourceTime, Date.parse(page.fetchedAt));
        applyCandles(next);
      } catch (error) {
        if (!initialData) message('BTCUSDT candles are unavailable. Use refresh to retry.', true);
        setFeed('retry', error.name === 'AbortError' ? 'Binance request timed out' : error.message);
      } finally {
        recentPending = undefined;
        byId('spotChartRefresh').disabled = false;
      }
    })();
    return recentPending;
  }
  async function loadOlder() {
    if (olderPending || !hasMore || !candles.length || Date.now() < olderRetryAt || candles.length >= 12000) return;
    olderPending = true;
    const before = candles[0].time * 1000;
    try {
      const page = await fetchPage(before);
      if (destroyed) return;
      const next = data.mergeCandles(candles, page.candles);
      hasMore = page.hasMore && next[0]?.time < candles[0].time;
      applyCandles(next);
    } catch (error) {
      olderRetryAt = Date.now() + 30_000;
      setFeed('retry', 'Earlier candles unavailable; retrying later');
    } finally { olderPending = false; }
  }

  function disconnect() {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
    if (socket) { const previous = socket; socket = undefined; previous.onclose = null; previous.onmessage = null; previous.close(); }
  }
  function openStream() {
    if (!active || destroyed || socket) return;
    const current = new WebSocket(streamUrls[failures % streamUrls.length]);
    socket = current;
    current.onmessage = (event) => {
      if (socket !== current || !active) return;
      let candle;
      try { candle = data.streamCandle(JSON.parse(event.data)); } catch { return; }
      if (!candle || candle.version < lastSourceTime - 120_000 || candle.time * 1000 > Date.now() + 60_000) return;
      const previous = candles[candles.length - 1];
      const existing = candles.find((row) => row.time === candle.time);
      if (existing && existing.version > candle.version) return;
      lastTickAt = Date.now();
      lastSourceTime = candle.version;
      failures = 0;
      if (!initialData) candles = data.mergeCandles(candles, [candle]);
      else applyCandles(data.mergeCandles(candles, [candle]), previous && candle.time >= previous.time ? candle : undefined);
      feedText();
    };
    current.onerror = () => { if (socket === current) setFeed('retry', 'Reconnecting'); };
    current.onclose = () => {
      if (socket !== current) return;
      socket = undefined;
      if (!active || destroyed) return;
      failures += 1;
      feedText();
      reconnectTimer = setTimeout(() => { reconnectTimer = undefined; openStream(); }, Math.min(30_000, 1000 * 2 ** Math.min(failures, 5)));
      if (Date.now() - lastRestAt > 15_000) void loadRecent();
    };
  }
  async function syncActivity() {
    active = inView && !document.hidden && navigator.onLine;
    if (!active) { disconnect(); feedText(); return; }
    try {
      await ensureChart();
      if (!active || destroyed) return;
      openStream();
      if (!initialData || Date.now() - lastRestAt > 30_000) void loadRecent();
      feedText();
    } catch { /* The panel displays the library load error. */ }
  }

  byId('spotChartRefresh').addEventListener('click', async () => {
    try { await ensureChart(); await loadRecent(); await refreshFractals(true); if (active) openStream(); } catch { /* Visible error remains retryable. */ }
  });
  for (const [id, key] of [['spotChartDaily', 'daily'], ['spotChartWeekly', 'weekly'], ['spotChartBroken', 'broken']]) {
    byId(id).addEventListener('change', (event) => {
      fractalLayer?.setOptions({ [key]: event.target.checked });
      if (fitFractals) candlesSeries?.applyOptions({ autoscaleInfoProvider: fractalAutoscale });
    });
  }
  byId('spotChartFitFractals').addEventListener('click', () => {
    fitFractals = !fitFractals;
    byId('spotChartFitFractals').setAttribute('aria-pressed', String(fitFractals));
    candlesSeries?.applyOptions({ autoscaleInfoProvider: fractalAutoscale });
    chart?.priceScale('right').applyOptions({ autoScale: true });
  });
  byId('spotChartHourlyBoxes').addEventListener('change', (event) => hourlyBoxLayer?.setVisible(event.target.checked));
  byId('spotChartZoomIn').addEventListener('click', () => zoom(.75));
  byId('spotChartZoomOut').addEventListener('click', () => zoom(1.33));
  byId('spotChartReset').addEventListener('click', () => selectRange(window.innerWidth < 680 ? '60' : '120'));
  panel.querySelectorAll('[data-spot-range]').forEach((button) => button.addEventListener('click', () => selectRange(button.dataset.spotRange)));
  byId('spotChartVolume').addEventListener('change', (event) => {
    if (!chart) return;
    volumeSeries.applyOptions({ visible: event.target.checked });
    chart.priceScale('right').applyOptions({ scaleMargins: { top: .08, bottom: event.target.checked ? .23 : .08 } });
  });
  byId('spotChartGrid').addEventListener('change', (event) => chart?.applyOptions({ grid: { vertLines: { visible: event.target.checked }, horzLines: { visible: event.target.checked } } }));
  byId('spotChartTimezone').addEventListener('change', (event) => {
    zone = event.target.value;
    if (chart) chart.applyOptions({ localization: { locale: 'en-US' } });
    if (!hovering) legend(candles[candles.length - 1]);
    projectLoadedCandles();
    feedText();
  });
  byId('spotChartFullscreen').hidden = !panel.requestFullscreen;
  byId('spotChartFullscreen').addEventListener('click', async () => {
    try {
      if (document.fullscreenElement === panel) await document.exitFullscreen();
      else await panel.requestFullscreen();
    } catch { setFeed('retry', 'Full screen unavailable'); }
  });
  document.addEventListener('fullscreenchange', () => {
    const label = document.fullscreenElement === panel ? 'Exit full screen' : 'Full screen';
    byId('spotChartFullscreen').title = label;
    byId('spotChartFullscreen').setAttribute('aria-label', label);
  });
  byId('spotChartDownload').addEventListener('click', () => {
    if (!chart || !initialData) return;
    chart.takeScreenshot().toBlob((blob) => {
      if (!blob) return;
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url; link.download = `BTCUSDT-Spot-1H-${new Date().toISOString().slice(0, 10)}.png`;
      link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    });
  });
  document.addEventListener('visibilitychange', syncActivity);
  window.addEventListener('online', syncActivity);
  window.addEventListener('offline', syncActivity);
  const observer = new IntersectionObserver((entries) => {
    inView = entries.some((entry) => entry.isIntersecting);
    void syncActivity();
  }, { rootMargin: '250px' });
  observer.observe(panel);
  window.addEventListener('pagehide', () => {
    destroyed = true;
    observer.disconnect(); disconnect(); clearInterval(watchdog); chart?.remove();
  }, { once: true });
})();
