import Decimal from 'decimal.js';

export type EntryRiskLimit = { side: 'BUY' | 'SELL'; price: number };

export type RiskCompatibleEntry = {
  requestedSize: number;
  size: number;
  downsized: boolean;
  limit: EntryRiskLimit;
  depth: ReturnType<typeof assessEntryDepth>;
};

export function entryRiskLimit(side: 'BUY' | 'SELL', size: number, stop: number, budget: number, tickSize: number): EntryRiskLimit {
  if (![size, stop, budget, tickSize].every((value) => Number.isFinite(value) && value > 0)) {
    throw new Error('Entry risk guard requires positive size, stop, risk budget and tick size.');
  }
  const distance = new Decimal(budget).div(size);
  // Reserve the adverse tick rounding of the stop as well as of the entry.
  const trigger = new Decimal(stop).div(tickSize)
    .toDecimalPlaces(0, side === 'BUY' ? Decimal.ROUND_FLOOR : Decimal.ROUND_CEIL).mul(tickSize);
  const raw = side === 'BUY' ? trigger.plus(distance) : trigger.minus(distance);
  const price = raw.div(tickSize).toDecimalPlaces(0, side === 'BUY' ? Decimal.ROUND_FLOOR : Decimal.ROUND_CEIL).mul(tickSize).toNumber();
  if (!(price > 0) || (side === 'BUY' ? price <= stop : price >= stop)) {
    throw new Error('Entry risk budget leaves no valid tick between entry and protective stop.');
  }
  return { side, price };
}

export function constrainEntryPrice(price: number, side: 'BUY' | 'SELL', reduceOnly: boolean, limit?: EntryRiskLimit): number {
  if (reduceOnly || !limit || limit.side !== side) return price;
  return side === 'BUY' ? Math.min(price, limit.price) : Math.max(price, limit.price);
}

export function assessEntryDepth(book: any, side: 'BUY' | 'SELL', size: number, limitPrice: number) {
  if (![size, limitPrice].every((value) => Number.isFinite(value) && value > 0)) {
    throw new Error('Entry risk guard: invalid size or price limit.');
  }
  const levels = (raw: any): Array<{ price: number; size: number }> => {
    if (!Array.isArray(raw)) throw new Error('Entry risk guard: missing orderbook side.');
    return raw.map((level) => ({ price: Number(level.price ?? level[0]), size: Number(level.size ?? level[1]) }))
      .filter((level) => Number.isFinite(level.price) && level.price > 0 && Number.isFinite(level.size) && level.size > 0);
  };
  const bids = levels(book?.bids).sort((a, b) => b.price - a.price);
  const asks = levels(book?.asks).sort((a, b) => a.price - b.price);
  if (!bids.length || !asks.length || bids[0].price >= asks[0].price) {
    throw new Error('Entry risk guard: empty or crossed dYdX orderbook.');
  }
  let remaining = new Decimal(size);
  let notional = new Decimal(0);
  for (const level of side === 'BUY' ? asks : bids) {
    if (side === 'BUY' ? level.price > limitPrice : level.price < limitPrice) break;
    const amount = Decimal.min(remaining, level.size);
    remaining = remaining.minus(amount);
    notional = notional.plus(amount.mul(level.price));
    if (remaining.isZero()) break;
  }
  if (remaining.gt(0)) throw new Error('Entry risk guard: insufficient visible liquidity within the stop-risk price limit.');
  return {
    expectedFillPrice: notional.div(size).toNumber(),
    bestBid: bids[0].price,
    bestAsk: asks[0].price,
    limitPrice,
    size
  };
}

function isInsufficientDepthError(error: unknown): boolean {
  return error instanceof Error && /insufficient visible liquidity/i.test(error.message);
}

export function findRiskCompatibleEntry(
  book: any,
  side: 'BUY' | 'SELL',
  requestedSize: number,
  stop: number,
  budget: number,
  tickSize: number,
  stepSize: number,
  marketPriceLimit: number
): RiskCompatibleEntry {
  if (![requestedSize, stepSize, marketPriceLimit].every((value) => Number.isFinite(value) && value > 0)) {
    throw new Error('Entry risk guard: dynamic sizing requires positive requested size, step size and market price limit.');
  }

  const requestedSteps = Math.floor(new Decimal(requestedSize).div(stepSize).plus('1e-9').toNumber());
  if (requestedSteps < 1) {
    throw new Error('Entry risk guard: requested size is below the dYdX minimum order size.');
  }

  const evaluate = (steps: number): Omit<RiskCompatibleEntry, 'requestedSize' | 'downsized'> => {
    const size = new Decimal(stepSize).mul(steps).toNumber();
    const limit = entryRiskLimit(side, size, stop, budget, tickSize);
    const limitPrice = constrainEntryPrice(marketPriceLimit, side, false, limit);
    const depth = assessEntryDepth(book, side, size, limitPrice);
    return { size, limit, depth };
  };

  try {
    const full = evaluate(requestedSteps);
    return { requestedSize, ...full, downsized: full.size + Number.EPSILON < requestedSize };
  } catch (error) {
    if (!isInsufficientDepthError(error)) throw error;
  }

  let minimum: Omit<RiskCompatibleEntry, 'requestedSize' | 'downsized'>;
  try {
    minimum = evaluate(1);
  } catch (error) {
    if (isInsufficientDepthError(error)) {
      throw new Error(
        'Entry risk guard: insufficient visible liquidity even after downsizing to the dYdX minimum order size.'
      );
    }
    throw error;
  }

  let low = 1;
  let high = requestedSteps - 1;
  let best = minimum;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    try {
      const candidate = evaluate(middle);
      best = candidate;
      low = middle + 1;
    } catch (error) {
      if (!isInsufficientDepthError(error)) throw error;
      high = middle - 1;
    }
  }

  return { requestedSize, ...best, downsized: true };
}
