(function (root) {
  'use strict';
  const viewport = typeof module === 'object' && module.exports ? require('./viewport.js') : root.BtcSpotViewport;
  const clamp = (value, low, high) => Math.max(low, Math.min(high, value));

  function transformView(view, options = {}) {
    const { factor = 1, dx = 0, dy = 0, x = view.width / 2, y = view.height / 2,
      time = true, price = true, pinRight = false, count, lead } = options;
    const result = {};
    if (time) {
      const span = view.time.to - view.time.from;
      const next = clamp(span * factor, 12, 12000);
      const from = view.time.from + (x / view.width) * (span - next) - (dx / view.width) * next;
      result.time = { from, to: from + next };
      if (pinRight && view.time.to >= count - 1) result.time = viewport.keepRightSpace(result.time, count, lead);
    }
    if (price) {
      const span = view.price.to - view.price.from;
      const next = clamp(span * factor, 1, 1e9);
      const anchor = view.price.to + ((view.priceTopY - y) / view.pricePixels) * span;
      const from = anchor - ((anchor - view.price.from) / span) * next + (dy / view.pricePixels) * next;
      result.price = { from: Math.max(.00000001, from), to: Math.max(.00000001, from) + next };
    }
    return result;
  }

  function install(element, adapter) {
    const pointers = new Map(), cleanups = [];
    let start, frame, pending, lastTap, wasMulti = false;
    const point = (event) => {
      const rect = element.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    };
    const metrics = () => {
      const values = [...pointers.values()];
      if (!values.length) return null;
      const [a, b] = values;
      return b ? { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2, distance: Math.max(1, Math.hypot(a.x - b.x, a.y - b.y)) } : { ...a, distance: 1 };
    };
    const flush = () => {
      cancelAnimationFrame(frame); frame = undefined;
      if (pending) { adapter.apply(pending); pending = undefined; }
    };
    const rebase = (external = false) => {
      const moved = external && start?.moved;
      if (external) { pending = undefined; cancelAnimationFrame(frame); frame = undefined; }
      else flush();
      const view = adapter.read(), center = metrics();
      start = view && center ? { view, center, mode: center.x > view.width ? 'price' : center.y > view.height ? 'time' : 'both', moved: Boolean(moved) } : null;
    };
    const apply = (view, options, kind) => {
      adapter.onChange?.(kind);
      pending = transformView(view, { ...options, ...adapter.constraints() });
      if (!frame) frame = requestAnimationFrame(flush);
    };
    const stop = (event) => { event.preventDefault(); event.stopPropagation(); };
    const down = (event) => {
      if (event.pointerType === 'mouse' && event.button !== 0) return;
      if (!adapter.read()) return;
      stop(event);
      element.setPointerCapture?.(event.pointerId);
      pointers.set(event.pointerId, point(event));
      if (pointers.size > 1) { wasMulti = true; lastTap = null; }
      adapter.onStart?.(); element.classList.add('is-panning'); rebase();
    };
    const move = (event) => {
      if (!pointers.has(event.pointerId) || !start) return;
      stop(event); pointers.set(event.pointerId, point(event));
      const center = metrics(), dx = center.x - start.center.x, dy = center.y - start.center.y;
      if (Math.hypot(dx, dy) > 3 || pointers.size > 1) start.moved = true;
      if (!start.moved) return;
      lastTap = null;
      if (pointers.size > 1) {
        apply(start.view, { factor: clamp(start.center.distance / center.distance, .05, 20), dx, dy,
          x: start.center.x, y: start.center.y, pinRight: true }, 'pinch');
      } else if (start.mode === 'price') {
        apply(start.view, { factor: Math.exp(clamp(dy * .01, -3, 3)), time: false, y: start.center.y }, 'zoom');
      } else if (start.mode === 'time') {
        apply(start.view, { factor: Math.exp(clamp(dx * .01, -3, 3)), price: false, x: start.center.x, pinRight: true }, 'zoom');
      } else apply(start.view, { dx, dy }, 'pan');
    };
    const end = (event) => {
      if (!pointers.has(event.pointerId)) return;
      const tap = !wasMulti && pointers.size === 1 && start && !start.moved && event.type === 'pointerup';
      const location = point(event);
      flush(); pointers.delete(event.pointerId);
      if (element.hasPointerCapture?.(event.pointerId)) element.releasePointerCapture(event.pointerId);
      if (pointers.size) rebase();
      else {
        start = null; wasMulti = false; element.classList.remove('is-panning'); adapter.onEnd?.();
        if (tap) {
          const now = performance.now();
          if (lastTap && now - lastTap.at < 300 && Math.hypot(location.x - lastTap.x, location.y - lastTap.y) < 25) {
            lastTap = null; adapter.reset();
          } else { lastTap = { ...location, at: now }; adapter.onTap?.(location); }
        }
      }
    };
    const wheel = (event) => {
      flush();
      const view = adapter.read();
      if (!view || event.deltaY === 0) return;
      stop(event);
      const anchor = point(event), factor = event.deltaY > 0 ? 1.18 : .84;
      apply(view, { ...anchor, factor, time: anchor.x <= view.width,
        price: anchor.y <= view.height, pinRight: true }, 'zoom');
      flush();
    };
    function listen(target, name, handler, options) {
      target.addEventListener(name, handler, options);
      cleanups.push(() => target.removeEventListener(name, handler, options));
    }
    listen(element, 'pointerdown', down, true);
    listen(element, 'pointermove', move, true);
    for (const type of ['pointerup', 'pointercancel', 'lostpointercapture']) listen(element, type, end, true);
    listen(element, 'wheel', wheel, { capture: true, passive: false });
    // Prevent the chart library's separate touch/long-press handlers from competing.
    for (const type of ['touchstart', 'touchmove']) listen(element, type, stop, { capture: true, passive: false });
    listen(element, 'dblclick', (event) => { stop(event); adapter.reset(); }, true);
    const cancel = () => {
      pending = undefined; cancelAnimationFrame(frame); frame = undefined; start = null; wasMulti = false; lastTap = null;
      const ids = [...pointers.keys()]; pointers.clear();
      for (const id of ids) if (element.hasPointerCapture?.(id)) element.releasePointerCapture(id);
      element.classList.remove('is-panning'); adapter.onEnd?.();
    };
    return { rebase: () => rebase(true), cancel, zoom: (factor) => {
      flush();
      const view = adapter.read(); if (!view) return;
      apply(view, { factor, pinRight: true }, 'zoom'); flush();
    }, destroy: () => { cancel(); cleanups.forEach((cleanup) => cleanup()); } };
  }
  const api = { transformView, install };
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.BtcSpotInteractions = api;
})(typeof globalThis === 'object' ? globalThis : this);
