(function (root) {
  'use strict';
  const HOUR = 3600;
  const SOURCE = 'binance-spot-recorded-intrusions';

  function buildIntrusionConfluence(candles, history, daily, orderBlocks, nowMs = Date.now(), market = 'BTC-USD') {
    const symbol = market.replace('-USD', 'USDT');
    if (!['BTC-USD', 'ETH-USD', 'SOL-USD'].includes(market) || history?.market !== market || history.source !== SOURCE ||
        history.timeBasis !== 'alert-candle-open-utc' || !Array.isArray(history.records) ||
        [daily, orderBlocks].some(model => model && (model.symbol !== symbol || model.venue !== 'Binance Spot')) ||
        daily && daily.intrusionSource !== SOURCE || orderBlocks && orderBlocks.interval !== '1h') {
      throw new Error('Unexpected Spot intrusion confluence source');
    }
    const closed = new Set(candles.filter(c => c.closed === true && Number.isSafeInteger(c.time) && c.time % HOUR === 0 &&
      (c.time + HOUR) * 1000 <= nowMs).map(c => c.time));
    const intrusions = new Set(history.records.filter(record => closed.has(record.time) &&
      Date.parse(record.alertTime) === record.time * 1000 && ['long', 'short'].includes(record.direction))
      .map(record => `${record.time}|${record.direction}`));
    const byTime = new Map();
    function add(time, direction, type, blockId) {
      if (!intrusions.has(`${time}|${direction}`)) return;
      if (!byTime.has(time)) byTime.set(time, new Map());
      const matches = byTime.get(time);
      if (!matches.has(direction)) matches.set(direction, { direction, dailyBreak: false, brokenBlockIds: [], newBlockIds: [] });
      const match = matches.get(direction);
      if (type === 'dailyBreak') match.dailyBreak = true;
      else if (!match[type].includes(blockId)) match[type].push(blockId);
    }

    for (const signal of daily?.signals || []) {
      if (signal.closedAt === signal.time + HOUR) add(signal.time, signal.direction, 'dailyBreak');
    }
    for (const block of orderBlocks?.blocks || []) {
      if (!['bullish', 'bearish'].includes(block.direction)) continue;
      const bullish = block.direction === 'bullish';
      // A volume pivot is only known at confirmation, not at its drawn origin.
      add(block.confirmedAt - HOUR, bullish ? 'long' : 'short', 'newBlockIds', block.id);
      if (block.state === 'broken' && block.brokenAt === block.brokenTime + HOUR && block.confirmedAt <= block.brokenAt) {
        add(block.brokenTime, bullish ? 'short' : 'long', 'brokenBlockIds', block.id);
      }
    }
    return { symbol, venue: 'Binance Spot', interval: '1h', signals: [...byTime].sort(([a], [b]) => a - b)
      .map(([time, matches]) => ({ time, closedAt: time + HOUR, matches: [...matches.values()] })) };
  }

  const api = Object.freeze({ buildIntrusionConfluence });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotIntrusionConfluence = api;
})(typeof globalThis === 'object' ? globalThis : this);
