(function (root) {
  'use strict';
  const RIGHT_BUFFER = 50;
  function rightBoundary(count, lead) { return count - 1 + lead + RIGHT_BUFFER; }
  function keepRightSpace(range, count, lead) {
    const to = Math.max(range.to, rightBoundary(count, lead));
    const span = Math.max(range.to - range.from, lead + RIGHT_BUFFER + 12);
    return { from: to - span, to };
  }
  function presetRange(count, lead, lookback) {
    return { from: Math.max(0, count - lookback), to: rightBoundary(count, lead) };
  }
  function zoomRange(range, count, lead, factor) {
    if (range.to < count - 1) {
      const span = Math.max(12, Math.min(10000, (range.to - range.from) * factor));
      const center = (range.from + range.to) / 2;
      return { from: center - span / 2, to: center + span / 2 };
    }
    const pinned = keepRightSpace(range, count, lead);
    const future = pinned.to - (count - 1);
    const visible = Math.max(12, Math.min(10000, (pinned.to - pinned.from - future) * factor));
    return { from: pinned.to - future - visible, to: pinned.to };
  }
  const api = { RIGHT_BUFFER, rightBoundary, keepRightSpace, presetRange, zoomRange };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotViewport = api;
})(typeof globalThis === 'object' ? globalThis : this);
