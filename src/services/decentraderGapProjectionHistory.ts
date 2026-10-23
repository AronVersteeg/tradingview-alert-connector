export interface GapProjectionHistory {
  symbol: 'BTCUSDT';
  venue: 'Decentrader map';
  historicalOnly: true;
  records: Array<{ candleTime: number; availableAt: number; lower: number | null; upper: number | null }>;
}

// Reconstruct the default map's active/all-sides/all-leverages gap, not a
// point-in-time signal archive: upstream active flags can change retrospectively.
export function buildDecentraderGapProjectionHistory(payload: any): GapProjectionHistory {
  if (payload?.source?.method !== 'getOHLCHourlyCalculations' ||
      payload.source.params?.[0]?.toLowerCase() !== 'btcusdt' ||
      !Array.isArray(payload.frames) || !payload.frames.length || !Array.isArray(payload.events)) {
    throw new Error('Stored BTC Decentrader map history is unavailable.');
  }
  const pricesByFrame: number[][] = payload.frames.map(() => []);
  for (const event of payload.events) {
    if (!Number.isInteger(event.i) || event.i < 0 || event.i >= pricesByFrame.length ||
        !['L', 'S'].includes(event.s) || ![3, 5, 10].includes(event.l) ||
        !Number.isFinite(event.p) || event.p <= 0 || ![0, 1].includes(event.a)) {
      throw new Error('Invalid stored Decentrader gap event.');
    }
    if (event.a) pricesByFrame[event.i].push(event.p);
  }
  const active: number[] = [];
  const lowerBound = (price: number) => {
    let left = 0, right = active.length;
    while (left < right) {
      const mid = (left + right) >>> 1;
      if (active[mid] < price) left = mid + 1;
      else right = mid;
    }
    return left;
  };
  let previous = 0;
  const records = payload.frames.map((frame: any, index: number) => {
    const candleTime = typeof frame.t === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:00:00$/.test(frame.t)
      ? Date.parse(frame.t.replace(' ', 'T') + 'Z') / 1000 : NaN;
    if (!Number.isSafeInteger(candleTime) || candleTime <= previous || candleTime % 3600) {
      throw new Error('Invalid stored Decentrader gap timestamp.');
    }
    previous = candleTime;
    for (const price of pricesByFrame[index]) {
      const at = lowerBound(price);
      if (active[at] !== price) active.splice(at, 0, price);
    }
    const at = lowerBound(frame.price);
    const lower = active[at - 1];
    const upper = active[active[at] === frame.price ? at + 1 : at];
    const valid = Number.isFinite(frame.price) && frame.price > 0 && lower !== undefined && upper !== undefined;
    return { candleTime, availableAt: candleTime + 3600, lower: valid ? lower : null, upper: valid ? upper : null };
  });
  return { symbol: 'BTCUSDT', venue: 'Decentrader map', historicalOnly: true, records };
}
