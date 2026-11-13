(function (root) {
  'use strict';
  const HOUR = 3600, LOOKBACK = 72, BUFFER = .001, MAX_DISTANCE = .3;

  function selectStop(fractals, at, direction, price, anchor = -Infinity) {
    const sign = direction === 'long' ? 1 : -1;
    const type = sign === 1 ? 'LOW' : 'HIGH';
    for (let i = fractals.length - 1; i >= 0; i--) {
      const pivot = fractals[i];
      if (pivot.pivotTime < at - LOOKBACK * HOUR || pivot.pivotTime <= anchor) break;
      if (pivot.type !== type || sign * (price - pivot.price) <= 0) continue;
      const stop = pivot.price * (1 - sign * BUFFER);
      return Math.abs(price - stop) / Math.max(1, price) <= MAX_DISTANCE
        ? { price: stop, anchor: pivot.pivotTime } : undefined;
    }
    return undefined;
  }

  function filterGreenPositions(candles, signals, fractals) {
    if (!signals.length) return [];
    const byEntry = new Map(signals.map(signal => [signal.closedAt, signal]));
    const selected = [], confirmed = [];
    let position, last, fractalIndex = 0;
    for (const candle of candles) {
      if (last && candle.time !== last.time + HOUR) {
        // An incomplete history cannot verify how far the earlier trade ran.
        position = undefined;
        confirmed.length = 0;
      }
      while (fractals[fractalIndex]?.confirmedAt <= candle.time) {
        const pivot = fractals[fractalIndex++];
        if (!last || candle.time === last.time + HOUR) confirmed.push(pivot);
      }
      const signal = byEntry.get(candle.time);
      if (!position && signal && last?.closed === true && last.time + HOUR === candle.time) {
        const sign = signal.direction === 'long' ? 1 : -1;
        const stop = selectStop(confirmed, last.time, signal.direction, candle.open);
        if (stop && sign * (candle.open - last.open) > 0 &&
            Math.abs(candle.open - last.open) / Math.max(1, candle.open) <= MAX_DISTANCE) {
          position = { sign, direction: signal.direction, price: last.open, anchor: stop.anchor };
          selected.push(signal);
        }
      }
      // An active position at the hour boundary owns that hour's entry, even if it then stops out.
      if (position && candle.closed === true) {
        const { sign, price } = position;
        if (sign * (candle.open - price) <= 0 || (sign === 1 ? candle.low <= price : candle.high >= price)) {
          position = undefined;
        } else {
          while (fractals[fractalIndex]?.confirmedAt <= candle.time + HOUR) confirmed.push(fractals[fractalIndex++]);
          const stop = selectStop(confirmed, candle.time, position.direction, candle.close, position.anchor);
          if (stop && sign * (stop.price - price) > 0 && sign * (candle.close - stop.price) > 0) {
            Object.assign(position, stop);
          }
        }
      }
      last = candle;
    }
    return selected;
  }

  const api = Object.freeze({ filterGreenPositions });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcGreenPositionReplay = api;
})(typeof globalThis === 'object' ? globalThis : this);
