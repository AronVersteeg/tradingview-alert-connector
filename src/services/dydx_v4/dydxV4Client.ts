import {
  BECH32_PREFIX,
  IndexerClient,
  CompositeClient,
  Network,
  SubaccountClient,
  ValidatorConfig,
  LocalWallet,
  OrderSide,
  OrderTimeInForce,
  OrderType,
  OrderExecution,
  IndexerConfig
} from '@dydxprotocol/v4-client-js';

import { AlertObject } from '../../types';
import 'dotenv/config';
import config from 'config';
import crypto from 'crypto';
import { AbstractDexClient } from '../abstractDexClient';

type ProgressResult =
  | { kind: 'target'; currentSize: number }
  | { kind: 'flat'; currentSize: number }
  | { kind: 'progress'; currentSize: number }
  | { kind: 'unchanged'; currentSize: number }
  | { kind: 'flipped'; currentSize: number };

type ManagedStop = {
  market: string;
  side: OrderSide;
  triggerPrice: number;
  clientId: number;
  size: number;
  source: 'STATIC' | 'TRAIL' | 'MANUAL';
  updatedAt: number;
  goodTilBlockTime?: number;
};

type ManagedTakeProfit = {
  market: string;
  side: OrderSide;
  triggerPrice: number;
  executionPrice: number;
  clientId: number;
  size: number;
  source: 'SAFETY' | 'EXTRA_TP' | 'EXPLICIT_TP';
  levelName: string;
  updatedAt: number;
  goodTilBlockTime?: number;
};

type ExtraTakeProfitLevel = {
  name: string;
  distancePct: number;
  remainingSizePct: number;
};

type ExplicitTakeProfitLevel = {
  name: string;
  price: number;
  size?: number;
  sizeFraction?: number;
};

type PlacedConditionalOrder = {
  clientId: number;
  size: number;
  goodTilBlockTime?: number;
};

type PlacedCorrectionOrder = {
  market: string;
  side: OrderSide;
  size: number;
  price: number;
  referencePrice?: number;
  priceSource: string;
  slippagePct: number;
  usedFallbackWorstPrice: boolean;
  minOrderSize?: number;
  roundedOrderSize?: number;
  stepSize?: number;
  goodTilBlockBuffer: number;
  goodTilBlock?: number;
  currentHeight?: number;
  clientId: number;
  reduceOnly: boolean;
  submittedAt: number;
  submitResult?: any;
};

type PositionSnapshot = {
  size: number;
  entryPrice?: number;
};

type TradingViewTelemetry = {
  signal: string;
  desiredPosition?: string;
  alertPrice?: number;
  currentPrice?: number;
  entryPrice?: number;
  trailStop?: number;
  longTrailStop?: number;
  shortTrailStop?: number;
  staticStop?: number;
  longStaticStop?: number;
  shortStaticStop?: number;
  topFractal?: number;
  bottomFractal?: number;
  floatingLongEntry?: number;
  floatingShortEntry?: number;
  barTime?: string;
  rawTime?: string;
};

type MarketSnapshot = {
  position: PositionSnapshot;
  allMarketOrders: any[];
  openOrders: any[];
  protectiveStops: any[];
  managedStop?: ManagedStop;
  managedTakeProfits: ManagedTakeProfit[];
};

type StopResolution = {
  triggerPrice?: number;
  source: 'DYDX_OPEN_ORDER' | 'MANAGED_MEMORY' | 'UNKNOWN';
  reason?: string;
  order?: any;
  matchingOrders: any[];
};

type PriceCandidate = {
  path: string;
  value: unknown;
  parsed?: number;
};

type CorrectionOrderPriceReference = {
  price?: number;
  source: string;
};

type CorrectionOrderPricing = {
  price: number;
  referencePrice?: number;
  priceSource: string;
  slippagePct: number;
  usedFallbackWorstPrice: boolean;
};

type CorrectionOrderSizeCheck = {
  requestedSize: number;
  minOrderSize?: number;
  roundedOrderSize?: number;
  stepSize?: number;
  source: string;
};

type MarketOrderGoodTilBlock = {
  currentHeight?: number;
  goodTilBlock?: number;
  goodTilBlockForward: number;
};

type ExecutionProfileName = 'MANAGED' | 'SIGNAL_ONLY' | 'NO_TP';

type ExecutionProfile = {
  name: ExecutionProfileName;
  placeStaticStop: boolean;
  placeSafetyTp: boolean;
  placeExtraTps: boolean;
  allowFractalTrail: boolean;
  allowManualStopSync: boolean;
};

export type DydxV4AccountSnapshot = {
  wallet: string;
  subaccount: number;
  equity: number;
  freeCollateral: number;
  marginEnabled: boolean;
  openPositionsCount: number;
  openPositions: Array<{
    market: string;
    side: string;
    size: number;
    entryPrice?: number;
    realizedPnl?: number;
    unrealizedPnl?: number;
  }>;
  markets: Record<
    string,
    {
      oraclePrice?: number;
      initialMarginFraction: number;
      maintenanceMarginFraction: number;
      stepSize: number;
      status?: string;
    }
  >;
  updatedAt: string;
};

const parseEnvFraction = (value: unknown, fallback = 0): number => {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed > 1
    ? parsed / 100
    : parsed;
};

const parseEnvPositiveNumber = (value: unknown, fallback: number): number => {
  const parsed = Number(value);

  return Number.isFinite(parsed) && parsed > 0
    ? parsed
    : fallback;
};

export class DydxV4Client extends AbstractDexClient {
  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  private txQueue: Promise<unknown> = Promise.resolve();
  private readonly marketQueues = new Map<string, Promise<unknown>>();

  private readonly managedStops = new Map<string, ManagedStop>();
  private readonly managedTakeProfits = new Map<string, ManagedTakeProfit[]>();

  private readonly TOLERANCE = parseEnvPositiveNumber(
    process.env.DYDX_V4_SIZE_TOLERANCE,
    0.00000001
  );

  private readonly MAX_ATTEMPTS = 5;
  private readonly FLAT_MAX_ATTEMPTS = 5;

  private readonly TARGET_PROGRESS_POLLS = 8;
  private readonly FLAT_PROGRESS_POLLS = 10;

  private readonly TARGET_POLL_DELAY_MS = 1500;
  private readonly FLAT_POLL_DELAY_MS = 1000;

  private readonly POST_ORDER_SETTLE_MS = 2000;

  private readonly SAFETY_STOP_PCT = Number(
    process.env.DYDX_V4_STATIC_STOP_PCT ?? '0.003'
  );

  private readonly SAFETY_STOP_SLIPPAGE_PCT = Number(
    process.env.DYDX_V4_SAFETY_STOP_SLIPPAGE_PCT ?? '0.01'
  );

  private readonly SAFETY_STOP_LIFETIME_SECONDS = Number(
    process.env.DYDX_V4_SAFETY_STOP_LIFETIME_SECONDS ?? `${60 * 60 * 24 * 30}`
  );

  private readonly SAFETY_STOP_VERIFY_POLLS = Number(
    process.env.DYDX_V4_SAFETY_STOP_VERIFY_POLLS ?? '10'
  );

  private readonly SAFETY_STOP_VERIFY_DELAY_MS = Number(
    process.env.DYDX_V4_SAFETY_STOP_VERIFY_DELAY_MS ?? '1000'
  );

  private readonly STOP_TRIGGER_MATCH_TOLERANCE_PCT = Number(
    process.env.DYDX_V4_STOP_TRIGGER_MATCH_TOLERANCE_PCT ?? '0.002'
  );

  private readonly MIN_TRAIL_IMPROVEMENT_PCT = Number(
    process.env.DYDX_V4_MIN_TRAIL_IMPROVEMENT_PCT ?? '0'
  );

  private readonly FRACTAL_TRAIL_BUFFER_PCT = Number(
    process.env.DYDX_V4_FRACTAL_TRAIL_BUFFER_PCT ??
    process.env.DYDX_V4_TRAIL_BUFFER_PCT ??
    '0.001'
  );

  private readonly FRACTAL_TRAIL_ENABLED =
    String(process.env.DYDX_V4_FRACTAL_TRAIL_ENABLED ?? 'false').toLowerCase() === 'true';

  private readonly FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP =
    String(process.env.DYDX_V4_FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP ?? 'false').toLowerCase() === 'true';

  private readonly CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT =
    String(process.env.DYDX_V4_CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT ?? 'true').toLowerCase() !== 'false';

  private readonly SAFETY_TP_ENABLED =
    String(process.env.DYDX_V4_SAFETY_TP_ENABLED ?? 'false').toLowerCase() === 'true';

  private readonly SAFETY_TP_SIZE_FRACTION = (() => {
    const raw = Number(process.env.DYDX_V4_SAFETY_TP_SIZE_PCT ?? '0.5');

    if (!Number.isFinite(raw) || raw <= 0) {
      return 0.5;
    }

    return raw > 1
      ? raw / 100
      : raw;
  })();

  private readonly SAFETY_TP_RISK_MULTIPLIER = Number(
    process.env.DYDX_V4_SAFETY_TP_RISK_MULTIPLIER ?? '1'
  );

  private readonly SAFETY_TP_LIFETIME_SECONDS = Number(
    process.env.DYDX_V4_SAFETY_TP_LIFETIME_SECONDS ?? `${60 * 60 * 24 * 30}`
  );

  private readonly SAFETY_TP_SLIPPAGE_PCT = Number(
    process.env.DYDX_V4_SAFETY_TP_SLIPPAGE_PCT ??
    process.env.DYDX_V4_SAFETY_STOP_SLIPPAGE_PCT ??
    '0.01'
  );

  private readonly EXTRA_TP_ENABLED =
    String(process.env.DYDX_V4_EXTRA_TP_ENABLED ?? 'false').toLowerCase() === 'true';

  private readonly EXTRA_TP_LIFETIME_SECONDS = Number(
    process.env.DYDX_V4_EXTRA_TP_LIFETIME_SECONDS ??
    process.env.DYDX_V4_SAFETY_TP_LIFETIME_SECONDS ??
    `${60 * 60 * 24 * 30}`
  );

  private readonly EXTRA_TP_SLIPPAGE_PCT = Number(
    process.env.DYDX_V4_EXTRA_TP_SLIPPAGE_PCT ??
    process.env.DYDX_V4_SAFETY_TP_SLIPPAGE_PCT ??
    process.env.DYDX_V4_SAFETY_STOP_SLIPPAGE_PCT ??
    '0.01'
  );

  private readonly EXTRA_TP_LEVELS: ExtraTakeProfitLevel[] = [
    {
      name: 'TP2',
      distancePct: parseEnvFraction(process.env.DYDX_V4_EXTRA_TP2_DISTANCE_PCT, 0),
      remainingSizePct: parseEnvFraction(
        process.env.DYDX_V4_EXTRA_TP2_SIZE_PCT_OF_REMAINING,
        0
      )
    },
    {
      name: 'TP3',
      distancePct: parseEnvFraction(process.env.DYDX_V4_EXTRA_TP3_DISTANCE_PCT, 0),
      remainingSizePct: parseEnvFraction(
        process.env.DYDX_V4_EXTRA_TP3_SIZE_PCT_OF_REMAINING,
        0
      )
    }
  ];

  private readonly LOG_RAW_ORDER_SNAPSHOTS =
    String(process.env.DYDX_V4_LOG_RAW_ORDER_SNAPSHOTS ?? 'true').toLowerCase() !== 'false';

  private readonly MARKET_BUY_WORST_PRICE = parseEnvPositiveNumber(
    process.env.DYDX_V4_MARKET_BUY_WORST_PRICE,
    999999
  );

  private readonly MARKET_SELL_WORST_PRICE = parseEnvPositiveNumber(
    process.env.DYDX_V4_MARKET_SELL_WORST_PRICE,
    0.000001
  );

  private readonly MARKET_ORDER_SLIPPAGE_PCT = parseEnvFraction(
    process.env.DYDX_V4_MARKET_ORDER_SLIPPAGE_PCT,
    0.03
  );

  private readonly MARKET_ORDER_GOOD_TIL_BLOCKS = parseEnvPositiveNumber(
    process.env.DYDX_V4_MARKET_ORDER_GOOD_TIL_BLOCKS,
    20
  );

  private readonly MAX_SHORT_TERM_GOOD_TIL_BLOCKS = 20;

  private readonly FAILSAFE_FLATTEN_ON_TARGET_FAILURE =
    String(process.env.DYDX_V4_FAILSAFE_FLATTEN_ON_TARGET_FAILURE ?? 'true').toLowerCase() !== 'false';

  private readonly DEFAULT_EXECUTION_PROFILE = this.normalizeExecutionProfileName(
    process.env.DYDX_V4_DEFAULT_EXECUTION_PROFILE ?? 'MANAGED',
    'MANAGED'
  );

  private readonly EXECUTION_PROFILES: Record<ExecutionProfileName, ExecutionProfile> = {
    MANAGED: {
      name: 'MANAGED',
      placeStaticStop: true,
      placeSafetyTp: this.SAFETY_TP_ENABLED,
      placeExtraTps: this.EXTRA_TP_ENABLED,
      allowFractalTrail: this.FRACTAL_TRAIL_ENABLED,
      allowManualStopSync: true
    },

    SIGNAL_ONLY: {
      name: 'SIGNAL_ONLY',
      placeStaticStop: false,
      placeSafetyTp: false,
      placeExtraTps: false,
      allowFractalTrail: false,
      allowManualStopSync: false
    },

    NO_TP: {
      name: 'NO_TP',
      placeStaticStop: true,
      placeSafetyTp: false,
      placeExtraTps: false,
      allowFractalTrail: this.FRACTAL_TRAIL_ENABLED,
      allowManualStopSync: true
    }
  };

  private readonly CONDITIONAL_ORDER_FLAGS = 32;
  private readonly LONG_TERM_ORDER_FLAGS = 64;

  async init(): Promise<void> {
    this.wallet = await LocalWallet.fromMnemonic(
      process.env.DYDX_V4_MNEMONIC!,
      BECH32_PREFIX
    );

    await this.connectDydxClients('init');

    this.initialized = true;
  }

  async getIsAccountReady(): Promise<boolean> {
    return this.initialized;
  }

  async getAccountSnapshot(markets: string[] = ['BTC-USD']): Promise<DydxV4AccountSnapshot> {
    if (!this.initialized) {
      throw new Error('dYdX v4 client is not initialized.');
    }

    const snapshot = await this.getSubaccountSnapshotBestEffort();
    const subaccount = snapshot?.subaccount ?? snapshot;
    const positions =
      subaccount?.openPerpetualPositions ??
      subaccount?.perpetualPositions ??
      snapshot?.perpetualPositions?.positions ??
      {};

    const openPositions = this.normalizeOpenPositions(positions);
    const requestedMarkets = Array.from(
      new Set(markets.map((market) => this.normalizeMarket(market)).filter(Boolean))
    );
    const marketSnapshots: DydxV4AccountSnapshot['markets'] = {};

    for (const market of requestedMarkets) {
      const marketInfo = await this.getMarketInfoBestEffort(market);
      marketSnapshots[market] = this.toPublicMarketSnapshot(marketInfo);
    }

    return {
      wallet: this.wallet.address,
      subaccount: 0,
      equity: this.parseNumberOrZero(subaccount?.equity),
      freeCollateral: this.parseNumberOrZero(subaccount?.freeCollateral),
      marginEnabled: Boolean(subaccount?.marginEnabled ?? true),
      openPositionsCount: openPositions.length,
      openPositions,
      markets: marketSnapshots,
      updatedAt: new Date().toISOString()
    };
  }

  async placeOrder(alert: AlertObject): Promise<void> {
    const market = this.normalizeMarket((alert as any).market);

    return this.withMarketQueue(market, () =>
      this.placeOrderForMarket(market, alert)
    );
  }

  async syncTakeProfits(alert: AlertObject): Promise<any> {
    const market = this.normalizeMarket((alert as any).market);

    return this.withMarketQueue(market, () =>
      this.syncTakeProfitsForMarket(market, alert)
    );
  }

  async syncTrailingStop(alert: AlertObject): Promise<any> {
    const market = this.normalizeMarket((alert as any).market);

    return this.withMarketQueue(market, () =>
      this.syncTrailingStopForMarket(market, alert)
    );
  }

  private async syncTrailingStopForMarket(market: string, alert: AlertObject): Promise<any> {
    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      return {
        outcome: 'SKIPPED',
        reason: `${market} is flat; dynamic SL sync has nothing to manage.`
      };
    }

    const expectedSize = this.getTargetSize(alert, Math.abs(position.size));
    if (Math.sign(expectedSize) !== Math.sign(position.size)) {
      throw new Error(
        `Dynamic SL sync direction mismatch for ${market}. Current=${position.size}, requested=${expectedSize}.`
      );
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const trailStop = this.getFirstPositiveNumber([
      (alert as any).trail_stop,
      (alert as any).trailStop,
      (alert as any).static_sl,
      (alert as any).staticSL,
      (alert as any).stop_loss,
      (alert as any).stopLoss
    ]);

    if (!trailStop) {
      return {
        outcome: 'SKIPPED',
        reason: 'No valid trailing stop price supplied.'
      };
    }

    const managedStop = this.managedStops.get(market);
    const positionAbsSize = Math.abs(position.size);
    const managedStopCoversPosition =
      managedStop &&
      managedStop.size + this.TOLERANCE >= positionAbsSize;
    if (
      managedStop &&
      String(managedStop.side).toUpperCase() === String(side).toUpperCase() &&
      managedStopCoversPosition &&
      Math.abs(managedStop.triggerPrice - trailStop) / trailStop < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT
    ) {
      return {
        outcome: 'UNCHANGED',
        reason: 'Render-managed stop memory already matches the latest fractal trailing stop.',
        positionSize: position.size,
        trailStop,
        visibility: 'MANAGED_MEMORY_ONLY'
      };
    }

    const executionPrice = this.getStopExecutionPrice(isLong, trailStop);
    const size = Math.max(positionAbsSize, managedStop?.size ?? 0);
    const openOrdersBefore = await this.getOpenOrdersForMarket(market);
    const protectiveStopOrders = this.getProtectiveStopOrdersFromOrders(openOrdersBefore)
      .filter((order: any) => this.orderSideMatches(order, side));
    const visibleCoveredStop = protectiveStopOrders.find((order: any) => {
      const existingTrigger = this.getOrderTriggerPrice(order);
      const existingSize = this.parsePositiveNumber(
        order.size ??
        order.remainingSize ??
        order.remaining_size ??
        order.quantity ??
        order.qty
      );

      return (
        existingTrigger !== undefined &&
        existingSize !== undefined &&
        Math.abs(existingTrigger - trailStop) / trailStop < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT &&
        existingSize + this.TOLERANCE >= positionAbsSize
      );
    });

    if (visibleCoveredStop) {
      const visibleSize = this.parsePositiveNumber(
        visibleCoveredStop.size ??
        visibleCoveredStop.remainingSize ??
        visibleCoveredStop.remaining_size ??
        visibleCoveredStop.quantity ??
        visibleCoveredStop.qty
      ) ?? positionAbsSize;
      const visibleClientId = this.getOrderClientId(visibleCoveredStop);

      if (Number.isFinite(visibleClientId)) {
        this.managedStops.set(market, {
          market,
          side,
          triggerPrice: trailStop,
          clientId: visibleClientId,
          size: Math.max(visibleSize, positionAbsSize),
          source: managedStop?.source ?? 'TRAIL',
          updatedAt: Date.now(),
          goodTilBlockTime: this.getOrderGoodTilBlockTime(visibleCoveredStop)
        });
      }

      return {
        outcome: 'UNCHANGED',
        reason: 'A visible dYdX protective stop already covers the full current position; Render-managed stop memory was refreshed.',
        positionSize: position.size,
        trailStop,
        stopSize: visibleSize,
        visibility: 'DYDX_OPEN_ORDER',
        matchedOrder: this.summarizeOrder(visibleCoveredStop)
      };
    }

    const oldStopOrders = protectiveStopOrders
      .filter((order: any) => {
        const existingTrigger = this.getOrderTriggerPrice(order);
        const existingSize = this.parsePositiveNumber(
          order.size ??
          order.remainingSize ??
          order.remaining_size ??
          order.quantity ??
          order.qty
        );
        const triggerMatches =
          existingTrigger !== undefined &&
          Math.abs(existingTrigger - trailStop) / trailStop < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT;
        const sizeMatches =
          existingSize !== undefined &&
          Math.abs(existingSize - size) < this.TOLERANCE;

        return (
          existingTrigger === undefined ||
          !triggerMatches ||
          !sizeMatches
        );
      });

    console.log('Submitting add-only Decentrader fractal trailing stop:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      positionSize: position.size,
      trailStop,
      executionPrice,
      side,
      size,
      previousManagedStop: managedStop || null,
      oldVisibleStopCount: oldStopOrders.length,
      cancelOldStopsAfterTrailSubmit: this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT
    });

    const placedStop = await this.placeSafetyStopOrder(
      market,
      side,
      size,
      trailStop,
      executionPrice
    );

    this.managedStops.set(market, {
      market,
      side,
      triggerPrice: trailStop,
      clientId: placedStop.clientId,
      size: placedStop.size,
      source: 'TRAIL',
      updatedAt: Date.now(),
      goodTilBlockTime: placedStop.goodTilBlockTime
    });

    const verified = await this.waitForSafetyStopVisibleBestEffort(
      market,
      side,
      trailStop,
      placedStop.clientId
    );
    const shouldCancelOldStops = verified || this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT;
    let oldManagedStopCancelledBestEffort = false;
    let remainingOldStops: any[] = [];

    if (shouldCancelOldStops) {
      if (!verified && this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT) {
        console.warn('Cancelling old stops after unverified Decentrader trailing stop submit because config allows it.', {
          market,
          trailStop,
          placedClientId: placedStop.clientId
        });
      }

      await this.cancelSpecificOrders(market, oldStopOrders);
      remainingOldStops = await this.cancelOtherProtectiveStopsBestEffort(
        market,
        side,
        placedStop.clientId,
        'Replacing old Decentrader trailing stop.'
      );

      const oldManagedAlreadyVisible = managedStop
        ? oldStopOrders.some((order: any) =>
            this.orderClientIdMatches(order, managedStop.clientId)
          )
        : false;

      if (managedStop && !oldManagedAlreadyVisible) {
        await this.cancelManagedStopBestEffort(
          market,
          managedStop,
          'Replacing old Decentrader trailing stop.'
        );
        oldManagedStopCancelledBestEffort = true;
      }
    } else {
      console.warn('Old stops left in place because new Decentrader trailing stop was not verified and cancel-on-submit is disabled.', {
        market,
        trailStop,
        placedClientId: placedStop.clientId
      });
    }

    return {
      outcome: 'UPDATED',
      reason: shouldCancelOldStops
        ? 'Submitted fractal trailing stop and cancelled older visible/managed stops best-effort.'
        : 'Submitted add-only fractal trailing stop. Older stops were preserved as fallback.',
      positionSize: position.size,
      trailStop,
      executionPrice,
      clientId: placedStop.clientId,
      goodTilBlockTime: placedStop.goodTilBlockTime,
      verified,
      previousManagedStop: managedStop || null,
      oldVisibleStopCount: oldStopOrders.length,
      remainingOldStopCount: remainingOldStops?.length ?? 0,
      oldManagedStopCancelledBestEffort
    };
  }

  private async syncTakeProfitsForMarket(market: string, alert: AlertObject): Promise<any> {
    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      return {
        outcome: 'SKIPPED',
        reason: `${market} is flat; dynamic TP sync has nothing to manage.`
      };
    }

    const expectedSize = this.getTargetSize(alert, Math.abs(position.size));
    if (Math.sign(expectedSize) !== Math.sign(position.size)) {
      throw new Error(
        `Dynamic TP sync direction mismatch for ${market}. Current=${position.size}, requested=${expectedSize}.`
      );
    }

    const requestedLevels = this.getExplicitTakeProfitLevels(alert);
    if (!requestedLevels.length) {
      return {
        outcome: 'SKIPPED',
        reason: 'No valid map-derived TP levels supplied; existing TP orders were preserved.'
      };
    }

    const isLong = position.size > 0;
    const marketInfo = await this.getMarketInfoBestEffort(market);
    const currentPrice = this.getMarketInfoReferencePrice(marketInfo);
    const levels = requestedLevels.filter((level) =>
      !currentPrice || (isLong ? level.price > currentPrice : level.price < currentPrice)
    );

    if (!levels.length) {
      return {
        outcome: 'SKIPPED',
        reason: 'No supplied map-derived TP levels remain beyond the current market price.'
      };
    }

    const openOrders = await this.getOpenOrdersForMarket(market);
    const existingTakeProfits = openOrders.filter((order: any) => this.isTakeProfitOrder(order));
    const managedTakeProfits = this.managedTakeProfits.get(market) ?? [];

    if (this.takeProfitOrdersMatchLevels(existingTakeProfits, levels, market, marketInfo)) {
      return {
        outcome: 'UNCHANGED',
        reason: 'Live dYdX TP orders already match the latest map-derived TP ladder.',
        positionSize: position.size,
        takeProfitCount: existingTakeProfits.length
      };
    }

    if (!existingTakeProfits.length && this.managedTakeProfitsMatchLevels(managedTakeProfits, levels)) {
      return {
        outcome: 'UNCHANGED',
        reason: 'Render-managed TP memory already matches the latest map-derived TP ladder; dYdX indexer did not expose the conditional order.',
        positionSize: position.size,
        takeProfitCount: managedTakeProfits.length,
        visibility: 'MANAGED_MEMORY_ONLY'
      };
    }

    if (!existingTakeProfits.length && managedTakeProfits.length) {
      return {
        outcome: 'SKIPPED',
        reason: 'Render-managed TP memory differs from the latest map ladder, but dYdX indexer did not expose the live conditional order; dynamic TP replacement skipped to avoid duplicates.',
        positionSize: position.size,
        managedTakeProfitCount: managedTakeProfits.length,
        requestedTakeProfitCount: levels.length,
        visibility: 'MANAGED_MEMORY_ONLY'
      };
    }

    if (!existingTakeProfits.length && !managedTakeProfits.length) {
      return {
        outcome: 'SKIPPED',
        reason: 'No visible or Render-managed TP orders were available to replace; dynamic TP sync skipped to avoid duplicate dYdX conditional orders.',
        positionSize: position.size,
        requestedTakeProfitCount: levels.length,
        visibility: 'UNVERIFIED_INDEXER'
      };
    }

    console.log('Dynamic map TP sync replacing TP-only ladder:', {
      market,
      position,
      currentPrice,
      existingTakeProfits: existingTakeProfits.map((order: any) => this.summarizeOrder(order)),
      managedTakeProfits,
      requestedLevels: levels
    });

    await this.cancelSpecificOrders(market, existingTakeProfits);
    const visibleManagedClientIds = new Set(
      existingTakeProfits.map((order: any) => this.getOrderClientId(order))
    );

    for (const managedTakeProfit of managedTakeProfits) {
      if (visibleManagedClientIds.has(managedTakeProfit.clientId)) continue;
      await this.cancelManagedTakeProfitBestEffort(
        market,
        managedTakeProfit,
        'Replacing dynamic map-derived TP ladder.'
      );
    }

    const remainingTakeProfits = existingTakeProfits.length
      ? await this.waitForTakeProfitsCleared(market)
      : [];

    if (remainingTakeProfits.length) {
      throw new Error(
        `Dynamic TP sync stopped for ${market}: ${remainingTakeProfits.length} old TP order(s) remain visible after cancellation.`
      );
    }

    const positionAfterCancel = await this.getCurrentPosition(market);
    if (
      Math.sign(positionAfterCancel.size) !== Math.sign(position.size) ||
      Math.abs(positionAfterCancel.size - position.size) >= this.TOLERANCE
    ) {
      throw new Error(
        `Dynamic TP sync stopped for ${market}: position changed during TP replacement. Before=${position.size}, after=${positionAfterCancel.size}.`
      );
    }

    this.managedTakeProfits.delete(market);
    await this.placeExplicitTakeProfitsAfterEntry(
      market,
      position.size,
      {
        ...(alert as any),
        take_profits: levels
      } as AlertObject,
      levels
    );

    return {
      outcome: 'UPDATED',
      reason: 'Replaced dYdX TP-only ladder with latest map-derived liquidity zones.',
      positionSize: position.size,
      previousTakeProfitCount: Math.max(existingTakeProfits.length, managedTakeProfits.length),
      takeProfitCount: levels.length,
      levels
    };
  }

  private async waitForTakeProfitsCleared(market: string): Promise<any[]> {
    let remainingTakeProfits: any[] = [];

    for (let poll = 1; poll <= this.SAFETY_STOP_VERIFY_POLLS; poll += 1) {
      await this.sleep(this.SAFETY_STOP_VERIFY_DELAY_MS);
      remainingTakeProfits = (await this.getOpenOrdersForMarket(market))
        .filter((order: any) => this.isTakeProfitOrder(order));

      console.log(`Dynamic TP cancel verify ${poll}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        remainingTakeProfitCount: remainingTakeProfits.length
      });

      if (!remainingTakeProfits.length) return [];
    }

    return remainingTakeProfits;
  }

  private async placeOrderForMarket(
    market: string,
    alert: AlertObject
  ): Promise<void> {
    const signal = this.getSignal(alert);
    const telemetry = this.getTradingViewTelemetry(alert);
    const profile = this.getExecutionProfile(alert);

    console.log('dYdX alert intake:', {
      market,
      signal,
      profile: profile.name,
      desiredPosition: telemetry.desiredPosition,
      telemetry: this.compactObject(telemetry as Record<string, unknown>)
    });

    if (this.isManualStopSyncSignal(signal)) {
      if (!profile.allowManualStopSync) {
        console.log('Manual stop sync ignored for execution profile.', { market, signal, profile: profile.name });
        return;
      }

      await this.handleManualStopSync(market, alert);
      return;
    }

    if (this.isFractalMovementSignal(signal)) {
      if (!profile.allowFractalTrail) {
        console.log('Fractal movement ignored for execution profile.', { market, signal, profile: profile.name });
        return;
      }

      await this.handleFractalMovement(market, alert);
      return;
    }

    if (signal === 'TRAIL_UPDATE') {
      console.log('TRAIL_UPDATE ignored because legacy Render trailing is disabled.', {
        market,
        signal,
        profile: profile.name
      });
      return;
    }

    const targetSize = this.getTargetSize(alert, Number((alert as any).size ?? 0));

    console.log('Target position:', { market, targetSize, profile: profile.name });

    await this.cancelOpenOrders(market);

    if (Math.abs(targetSize) < this.TOLERANCE) {
      await this.clearManagedOrdersForFlatMarket(market, 'Target position is FLAT.');
      await this.flattenPositionSafely(market);
      return;
    }

    const targetReached = await this.reachTargetPositionOrFailsafeFlat(
      market,
      targetSize,
      this.getAlertPriceReference(alert, telemetry)
    );

    if (!targetReached) {
      console.warn('Target was not reached. Position was fail-safe flattened; skipping SL/TP setup.', {
        market,
        targetSize,
        profile: profile.name
      });
      return;
    }

    const explicitTakeProfits = this.getExplicitTakeProfitLevels(alert);
    const skipStaticStop = this.shouldSkipStaticStop(alert);

    if (skipStaticStop) {
      if (explicitTakeProfits.length) {
        await this.placeExplicitTakeProfitsAfterEntry(
          market,
          targetSize,
          alert,
          explicitTakeProfits
        );
      } else {
        console.log('Static stop skipped and no explicit TPs supplied.', {
          market,
          targetSize,
          profile: profile.name
        });
      }
      return;
    }

    if (!profile.placeStaticStop) {
      if (explicitTakeProfits.length) {
        await this.placeExplicitTakeProfitsAfterEntry(
          market,
          targetSize,
          alert,
          explicitTakeProfits
        );
        return;
      }

      console.log('Signal-only execution complete: no SL/TP/trailing orders placed.', {
        market,
        targetSize,
        profile: profile.name
      });
      return;
    }

    const safetyStop = await this.placeStaticSafetyStopAfterEntry(
      market,
      targetSize,
      alert
    );

    if (explicitTakeProfits.length) {
      await this.placeExplicitTakeProfitsAfterEntry(
        market,
        targetSize,
        alert,
        explicitTakeProfits
      );
      return;
    }

    if (!profile.placeSafetyTp) {
      console.log('Safety TP skipped for execution profile.', { market, profile: profile.name });
      return;
    }

    const safetyTakeProfit = await this.placeSafetyTakeProfitAfterEntry(
      market,
      targetSize,
      alert,
      safetyStop
    );

    if (!profile.placeExtraTps) {
      console.log('Extra TPs skipped for execution profile.', { market, profile: profile.name });
      return;
    }

    await this.placeExtraTakeProfitsAfterEntry(
      market,
      targetSize,
      alert,
      safetyTakeProfit
    );
  }

  private getExecutionProfile(alert: AlertObject): ExecutionProfile {
    const data = alert as any;
    const rawProfile =
      data.profile ??
      data.execution_profile ??
      data.executionProfile ??
      this.DEFAULT_EXECUTION_PROFILE;

    const profileName = this.normalizeExecutionProfileName(rawProfile);

    return this.EXECUTION_PROFILES[profileName];
  }

  private normalizeExecutionProfileName(
    value: unknown,
    fallback?: ExecutionProfileName
  ): ExecutionProfileName {
    const normalized = String(value ?? '')
      .trim()
      .toUpperCase()
      .replace(/[\s-]+/g, '_');

    switch (normalized) {
      case '':
      case 'DEFAULT':
        if (fallback) return fallback;
        break;

      case 'MANAGED':
      case 'FULL':
      case 'WITH_RISK':
        return 'MANAGED';

      case 'SIGNAL':
      case 'SIGNALS':
      case 'SIGNAL_ONLY':
      case 'LONG_SHORT_ONLY':
      case 'ENTRY_ONLY':
      case 'NO_SL_TP':
        return 'SIGNAL_ONLY';

      case 'NO_TP':
      case 'STOP_ONLY':
        return 'NO_TP';
    }

    if (fallback) return fallback;

    throw new Error(
      `Invalid dYdX execution profile "${String(value)}". Use MANAGED, SIGNAL_ONLY, or NO_TP.`
    );
  }

  private isFractalMovementSignal(signal: string): boolean {
    return (
      signal === 'BOTTOM_FRACTAL_MOVING_UP' ||
      signal === 'TOP_FRACTAL_MOVING_DOWN'
    );
  }

  private isManualStopSyncSignal(signal: string): boolean {
    return (
      signal === 'MANUAL_STOP_SYNC' ||
      signal === 'SYNC_MANUAL_STOP'
    );
  }

  private async handleManualStopSync(
    market: string,
    alert: AlertObject
  ): Promise<void> {
    const snapshot = await this.getMarketSnapshot(market);
    const position = snapshot.position;
    const telemetry = this.getTradingViewTelemetry(alert);
    const manualStop = this.getFirstPositiveNumber([
      (alert as any).manual_stop,
      (alert as any).manualStop,
      (alert as any).stop,
      (alert as any).stop_loss,
      (alert as any).stopLoss,
      (alert as any).static_sl,
      (alert as any).staticSL,
      (alert as any).trail_stop,
      (alert as any).trailStop,
      (alert as any).price
    ]);

    this.logMarketSnapshot('MANUAL_STOP_SYNC received', market, snapshot, telemetry);

    if (Math.abs(position.size) < this.TOLERANCE) {
      await this.clearManagedOrdersForFlatMarket(market, 'Manual stop sync received while flat.');

      console.warn('Manual stop sync ignored because position is flat.', {
        market,
        position,
        manualStop
      });
      return;
    }

    if (!manualStop) {
      console.warn('Manual stop sync ignored because no valid stop price was supplied.', {
        market,
        telemetry: this.compactObject(telemetry as Record<string, unknown>)
      });
      return;
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;

    if (isLong && manualStop >= (position.entryPrice ?? Number.POSITIVE_INFINITY)) {
      console.warn('Manual LONG stop is above/equal entry reference. Syncing anyway, but please verify.', {
        market,
        position,
        manualStop
      });
    }

    if (!isLong && manualStop <= (position.entryPrice ?? 0)) {
      console.warn('Manual SHORT stop is below/equal entry reference. Syncing anyway, but please verify.', {
        market,
        position,
        manualStop
      });
    }

    this.managedStops.set(market, {
      market,
      side,
      triggerPrice: manualStop,
      clientId: this.createClientId(),
      size: Math.abs(position.size),
      source: 'MANUAL',
      updatedAt: Date.now()
    });

    console.log('Manual stop synced into Render memory. No dYdX order was placed or cancelled.', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      side,
      triggerPrice: manualStop,
      position
    });
  }

  private async handleFractalMovement(
    market: string,
    alert: AlertObject
  ): Promise<void> {
    const signal = this.getSignal(alert);
    const telemetry = this.getTradingViewTelemetry(alert);
    const bufferPct = this.getFractalTrailBufferPct(alert);

    const bottomFractal =
      telemetry.bottomFractal ??
      (signal === 'BOTTOM_FRACTAL_MOVING_UP' ? telemetry.alertPrice : undefined);

    const topFractal =
      telemetry.topFractal ??
      (signal === 'TOP_FRACTAL_MOVING_DOWN' ? telemetry.alertPrice : undefined);

    const longTrailPreview =
      signal === 'BOTTOM_FRACTAL_MOVING_UP' && bottomFractal
        ? bottomFractal * (1 - bufferPct)
        : undefined;

    const shortTrailPreview =
      signal === 'TOP_FRACTAL_MOVING_DOWN' && topFractal
        ? topFractal * (1 + bufferPct)
        : undefined;

    const snapshot = await this.getMarketSnapshot(market);
    const position = snapshot.position;

    this.logMarketSnapshot('FRACTAL_TRAIL received', market, snapshot, telemetry);

    if (!this.FRACTAL_TRAIL_ENABLED) {
      console.log('Fractal trail debug only: live trailing is disabled.', {
        market,
        signal,
        bottomFractal,
        topFractal,
        bufferPct,
        longTrailPreview,
        shortTrailPreview,
        position
      });
      return;
    }

    if (Math.abs(position.size) < this.TOLERANCE) {
      await this.clearManagedOrdersForFlatMarket(market, 'Fractal movement received while flat.');

      console.log(
        signal === 'BOTTOM_FRACTAL_MOVING_UP'
          ? 'FB up ignored: dYdX position is not LONG.'
          : 'FT down ignored: dYdX position is not SHORT.',
        {
          market,
          position,
          trailStop: telemetry.trailStop,
          managedOrdersCleared: true
        }
      );
      return;
    }

    const isLongSignal = signal === 'BOTTOM_FRACTAL_MOVING_UP';
    const isShortSignal = signal === 'TOP_FRACTAL_MOVING_DOWN';

    if (isLongSignal && position.size <= this.TOLERANCE) {
      console.log('FB up ignored: dYdX position is not LONG.', {
        market,
        position,
        trailStop: telemetry.trailStop
      });
      return;
    }

    if (isShortSignal && position.size >= -this.TOLERANCE) {
      console.log('FT down ignored: dYdX position is not SHORT.', {
        market,
        position,
        trailStop: telemetry.trailStop
      });
      return;
    }

    const isLong = position.size > 0;
    const direction = isLong ? 'LONG' : 'SHORT';
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const trailStop = telemetry.trailStop;

    if (!trailStop) {
      console.warn('Fractal trail ignored because no valid trail_stop was supplied.', {
        market,
        signal,
        direction,
        bottomFractal,
        topFractal,
        longTrailPreview,
        shortTrailPreview,
        telemetry: this.compactObject(telemetry as Record<string, unknown>)
      });
      return;
    }

    const stopResolution = this.resolveCurrentProtectiveStop(snapshot, side, isLong);
    const currentKnownStop = stopResolution.triggerPrice;
    const oldManagedStop =
      snapshot.managedStop &&
      String(snapshot.managedStop.side).toUpperCase() === String(side).toUpperCase()
        ? snapshot.managedStop
        : undefined;

    const oldStopOrders = snapshot.protectiveStops.filter((order: any) =>
      this.orderSideMatches(order, side)
    );

    if (currentKnownStop === undefined && !this.FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP) {
      console.warn('Fractal trail ignored because current protective stop could not be determined safely.', {
        market,
        signal,
        direction,
        trailStop,
        reason: stopResolution.reason,
        bootstrapIfNoStop: this.FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP,
        matchingStopCount: stopResolution.matchingOrders.length
      });
      return;
    }

    if (
      currentKnownStop !== undefined &&
      !this.stopImproves(isLong, trailStop, currentKnownStop)
    ) {
      console.log('Fractal trail ignored: new stop does not improve current stop.', {
        market,
        signal,
        direction,
        currentStop: currentKnownStop,
        currentStopSource: stopResolution.source,
        trailStop,
        minTrailImprovementPct: this.MIN_TRAIL_IMPROVEMENT_PCT
      });
      return;
    }

    const size = Math.abs(position.size);
    const executionPrice = this.getStopExecutionPrice(isLong, trailStop);

    console.log('Applying fractal trail update:', {
      market,
      signal,
      direction,
      positionSize: position.size,
      currentKnownStop,
      currentStopSource: stopResolution.source,
      trailStop,
      executionPrice,
      side,
      size,
      oldStopCount: oldStopOrders.length,
      oldManagedStop,
      bootstrapIfNoStop: this.FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP,
      cancelOldStopsAfterTrailSubmit: this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT
    });

    const placedStop = await this.placeSafetyStopOrder(
      market,
      side,
      size,
      trailStop,
      executionPrice
    );

    this.managedStops.set(market, {
      market,
      side,
      triggerPrice: trailStop,
      clientId: placedStop.clientId,
      size: placedStop.size,
      source: 'TRAIL',
      updatedAt: Date.now(),
      goodTilBlockTime: placedStop.goodTilBlockTime
    });

    const verified = await this.waitForSafetyStopVisibleBestEffort(
      market,
      side,
      trailStop,
      placedStop.clientId
    );

    if (!verified) {
      console.warn('Fractal trail stop submitted but not verified through indexer.', {
        market,
        signal,
        direction,
        clientId: placedStop.clientId,
        trailStop,
        oldStopCount: oldStopOrders.length,
        oldManagedStop,
        cancelOldStopsAfterTrailSubmit: this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT
      });
    }

    const shouldCancelOldStops = verified || this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT;

    if (!shouldCancelOldStops) {
      console.warn('Old stops left in place because new trail was not verified and cancel-on-submit is disabled.', {
        market,
        signal,
        direction,
        trailStop,
        placedClientId: placedStop.clientId
      });
      return;
    }

    if (!verified && this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT) {
      console.warn('Cancelling old Render-managed stops after unverified trail submit because config allows it.', {
        market,
        signal,
        direction,
        trailStop,
        placedClientId: placedStop.clientId
      });
    }

    await this.cancelSpecificOrders(market, oldStopOrders);
    const remainingOldStops = await this.cancelOtherProtectiveStopsBestEffort(
      market,
      side,
      placedStop.clientId,
      'Replacing old Render-managed stop with new fractal trail stop.'
    );

    const oldManagedAlreadyVisible = oldManagedStop
      ? oldStopOrders.some((order: any) =>
          this.orderClientIdMatches(order, oldManagedStop.clientId)
        )
      : false;

    if (oldManagedStop && !oldManagedAlreadyVisible) {
      await this.cancelManagedStopBestEffort(
        market,
        oldManagedStop,
        'Replacing old Render-managed stop with new fractal trail stop.'
      );
    }

    console.log('Fractal trail update complete.', {
      market,
      signal,
      direction,
      trailStop,
      placedClientId: placedStop.clientId,
      verified,
      oldVisibleStopCount: oldStopOrders.length,
      remainingOldStopCount: remainingOldStops.length,
      oldManagedStopCancelledBestEffort: Boolean(oldManagedStop && !oldManagedAlreadyVisible)
    });
  }

  private async clearManagedOrdersForFlatMarket(
    market: string,
    reason: string
  ): Promise<void> {
    const managedStop = this.managedStops.get(market);
    const managedTakeProfits = this.managedTakeProfits.get(market) ?? [];

    if (managedStop) {
      await this.cancelManagedStopBestEffort(market, managedStop, reason);
    }

    for (const managedTakeProfit of managedTakeProfits) {
      await this.cancelManagedTakeProfitBestEffort(market, managedTakeProfit, reason);
    }

    this.managedStops.delete(market);
    this.managedTakeProfits.delete(market);
  }

  private async cancelManagedStopBestEffort(
    market: string,
    stop: ManagedStop,
    reason: string
  ): Promise<void> {
    if (stop.source === 'MANUAL') {
      console.log('Skipping managed stop cancel because stop source is MANUAL.', {
        market,
        reason,
        triggerPrice: stop.triggerPrice
      });
      return;
    }

    if (stop.goodTilBlockTime === undefined) {
      console.warn('Render-managed stop cannot be cancelled because goodTilBlockTime is missing.', {
        market,
        clientId: stop.clientId,
        reason,
        stop
      });
      return;
    }

    try {
      console.log('Cancelling Render-managed stop best-effort:', {
        market,
        clientId: stop.clientId,
        orderFlags: this.CONDITIONAL_ORDER_FLAGS,
        goodTilBlockTime: stop.goodTilBlockTime,
        triggerPrice: stop.triggerPrice,
        reason
      });

      await this.cancelOrderByFlags(
        market,
        stop.clientId,
        this.CONDITIONAL_ORDER_FLAGS,
        undefined,
        stop.goodTilBlockTime
      );

      console.log('Render-managed stop cancel submitted:', {
        market,
        clientId: stop.clientId,
        goodTilBlockTime: stop.goodTilBlockTime,
        reason
      });
    } catch (error) {
      console.warn('Render-managed stop could not be cancelled best-effort. Check dYdX UI.', {
        market,
        clientId: stop.clientId,
        goodTilBlockTime: stop.goodTilBlockTime,
        reason,
        error: this.serializeError(error)
      });
    }
  }

  private async cancelManagedTakeProfitBestEffort(
    market: string,
    takeProfit: ManagedTakeProfit,
    reason: string
  ): Promise<void> {
    if (takeProfit.goodTilBlockTime === undefined) {
      console.warn('Render-managed TP cannot be cancelled because goodTilBlockTime is missing.', {
        market,
        clientId: takeProfit.clientId,
        reason,
        takeProfit
      });
      return;
    }

    try {
      console.log('Cancelling Render-managed TP best-effort:', {
        market,
        levelName: takeProfit.levelName,
        source: takeProfit.source,
        clientId: takeProfit.clientId,
        orderFlags: this.CONDITIONAL_ORDER_FLAGS,
        goodTilBlockTime: takeProfit.goodTilBlockTime,
        triggerPrice: takeProfit.triggerPrice,
        reason
      });

      await this.cancelOrderByFlags(
        market,
        takeProfit.clientId,
        this.CONDITIONAL_ORDER_FLAGS,
        undefined,
        takeProfit.goodTilBlockTime
      );

      console.log('Render-managed TP cancel submitted:', {
        market,
        levelName: takeProfit.levelName,
        clientId: takeProfit.clientId,
        goodTilBlockTime: takeProfit.goodTilBlockTime,
        reason
      });
    } catch (error) {
      console.warn('Render-managed TP could not be cancelled best-effort. Check dYdX UI.', {
        market,
        levelName: takeProfit.levelName,
        clientId: takeProfit.clientId,
        goodTilBlockTime: takeProfit.goodTilBlockTime,
        reason,
        error: this.serializeError(error)
      });
    }
  }

  private async flattenPositionSafely(market: string): Promise<void> {
    let currentSize = await this.getCurrentSize(market);

    console.log('Flatten requested:', { market, currentSize });

    if (Math.abs(currentSize) < this.TOLERANCE) {
      console.log('Already flat.', { market });
      await this.clearManagedOrdersForFlatMarket(market, 'Flatten requested while already flat.');
      return;
    }

    for (let attempt = 1; attempt <= this.FLAT_MAX_ATTEMPTS; attempt++) {
      const startSize = currentSize;
      const side = startSize > 0 ? OrderSide.SELL : OrderSide.BUY;
      const size = Math.abs(startSize);

      console.log('Flatten attempt:', {
        market,
        attempt,
        maxAttempts: this.FLAT_MAX_ATTEMPTS,
        startSize,
        side,
        size,
        reduceOnly: true
      });

      await this.placeCorrectionOrder(market, side, size, true);
      await this.sleep(this.POST_ORDER_SETTLE_MS);

      const progress = await this.waitForFlattenProgress(market, startSize);

      if (progress.kind === 'flat') {
        await this.clearManagedOrdersForFlatMarket(market, 'Position fully flattened.');
        console.log('Position fully flattened.', { market });
        return;
      }

      if (progress.kind === 'flipped') {
        console.error('Position flipped during flatten. Starting emergency flatten.', {
          market,
          previousSize: startSize,
          currentSize: progress.currentSize
        });

        await this.emergencyFlattenOppositePosition(market, progress.currentSize);
        return;
      }

      currentSize = progress.currentSize;
      console.log('Flatten not complete yet; retrying.', { market, currentSize, kind: progress.kind });
    }

    throw new Error(`Flatten failed for ${market}: max attempts reached without reaching flat.`);
  }

  private async emergencyFlattenOppositePosition(market: string, currentSize: number): Promise<void> {
    if (Math.abs(currentSize) < this.TOLERANCE) {
      await this.clearManagedOrdersForFlatMarket(market, 'Emergency flatten found position already flat.');
      console.log('Emergency check: already flat.', { market });
      return;
    }

    const side = currentSize > 0 ? OrderSide.SELL : OrderSide.BUY;
    const size = Math.abs(currentSize);

    console.warn('Emergency flatten order:', {
      market,
      side,
      size,
      currentSize,
      reduceOnly: true
    });

    await this.placeCorrectionOrder(market, side, size, true);
    await this.sleep(this.POST_ORDER_SETTLE_MS);

    const finalSize = await this.getCurrentSize(market);

    if (Math.abs(finalSize) < this.TOLERANCE) {
      await this.clearManagedOrdersForFlatMarket(market, 'Emergency flatten successful.');
      console.log('Emergency flatten successful.', { market });
      return;
    }

    throw new Error(
      `Emergency flatten failed for ${market}. Current size after emergency attempt: ${finalSize}`
    );
  }

  private async waitForFlattenProgress(
    market: string,
    initialSize: number
  ): Promise<ProgressResult> {
    let lastSeen = initialSize;

    for (let i = 1; i <= this.FLAT_PROGRESS_POLLS; i++) {
      await this.sleep(this.FLAT_POLL_DELAY_MS);

      const currentSize = await this.getCurrentSize(market);
      lastSeen = currentSize;

      console.log('Flatten poll:', {
        market,
        poll: i,
        maxPolls: this.FLAT_PROGRESS_POLLS,
        initialSize,
        currentSize
      });

      if (Math.abs(currentSize) < this.TOLERANCE) {
        return { kind: 'flat', currentSize };
      }

      const initialSign = Math.sign(initialSize);
      const currentSign = Math.sign(currentSize);

      if (currentSign !== 0 && initialSign !== 0 && currentSign !== initialSign) {
        return { kind: 'flipped', currentSize };
      }

      if (Math.abs(currentSize) < Math.abs(initialSize) - this.TOLERANCE) {
        return { kind: 'progress', currentSize };
      }
    }

    return { kind: 'unchanged', currentSize: lastSeen };
  }

  private async reachTargetPositionSafely(
    market: string,
    targetSize: number,
    priceReference?: CorrectionOrderPriceReference
  ): Promise<void> {
    for (let attempt = 1; attempt <= this.MAX_ATTEMPTS; attempt++) {
      await this.sleep(this.TARGET_POLL_DELAY_MS);

      const currentSize = await this.getCurrentSize(market);
      const diff = targetSize - currentSize;

      console.log('Target attempt:', {
        market,
        attempt,
        maxAttempts: this.MAX_ATTEMPTS,
        currentSize,
        targetSize,
        diff
      });

      if (Math.abs(diff) < this.TOLERANCE) {
        console.log('Target reached within tolerance.', { market, targetSize });
        return;
      }

      const side = diff > 0 ? OrderSide.BUY : OrderSide.SELL;
      const size = Math.abs(diff);

      const placedOrder = await this.placeCorrectionOrder(
        market,
        side,
        size,
        false,
        priceReference
      );
      await this.sleep(this.POST_ORDER_SETTLE_MS);

      const progress = await this.waitForTargetProgress(market, currentSize, targetSize);

      if (progress.kind === 'target') {
        console.log('Target reached after correction.', { market, targetSize });
        return;
      }

      if (progress.kind === 'progress') {
        console.log('Position moved toward target, continuing if needed.', {
          market,
          currentSize: progress.currentSize
        });
        continue;
      }

      if (progress.kind === 'flipped') {
        throw new Error(
          `Dangerous overshoot detected for ${market}. Current size flipped unexpectedly to ${progress.currentSize}`
        );
      }

      await this.logOrderDiagnostics(
        'Target correction made no visible progress.',
        market,
        placedOrder,
        {
          attempt,
          maxAttempts: this.MAX_ATTEMPTS,
          currentSize,
          targetSize,
          diff
        }
      );

      if (this.isAcceptedBroadcastResult(placedOrder.submitResult)) {
        console.warn('Correction IOC was accepted by dYdX but produced no visible fill before expiry.', {
          market,
          side,
          size,
          price: placedOrder.price,
          referencePrice: placedOrder.referencePrice,
          priceSource: placedOrder.priceSource,
          slippagePct: placedOrder.slippagePct,
          goodTilBlock: placedOrder.goodTilBlock,
          currentHeight: placedOrder.currentHeight,
          attempt,
          currentSize: progress.currentSize,
          targetSize
        });
      }

      console.warn('No visible progress yet after correction; retrying cautiously.', { market });
    }

    throw new Error(`Target correction failed for ${market}: max attempts reached.`);
  }

  private async reachTargetPositionOrFailsafeFlat(
    market: string,
    targetSize: number,
    priceReference?: CorrectionOrderPriceReference
  ): Promise<boolean> {
    try {
      await this.reachTargetPositionSafely(market, targetSize, priceReference);
      return true;
    } catch (error) {
      if (!this.FAILSAFE_FLATTEN_ON_TARGET_FAILURE) {
        throw error;
      }

      console.error('Target position failed. Starting fail-safe flatten.', {
        market,
        targetSize,
        error: this.serializeError(error)
      });

      try {
        await this.cancelOpenOrders(market);
        await this.clearManagedOrdersForFlatMarket(
          market,
          'Target position failed before fail-safe flatten.'
        );
        await this.flattenPositionSafely(market);

        console.error('Fail-safe flatten completed after target failure.', {
          market,
          targetSize
        });

        return false;
      } catch (flattenError) {
        console.error('Fail-safe flatten failed after target failure.', {
          market,
          targetSize,
          originalError: this.serializeError(error),
          flattenError: this.serializeError(flattenError)
        });

        throw flattenError;
      }
    }
  }

  private async waitForTargetProgress(
    market: string,
    initialSize: number,
    targetSize: number
  ): Promise<ProgressResult> {
    let lastSeen = initialSize;
    const initialDistance = Math.abs(targetSize - initialSize);

    for (let i = 1; i <= this.TARGET_PROGRESS_POLLS; i++) {
      await this.sleep(this.TARGET_POLL_DELAY_MS);

      const currentSize = await this.getCurrentSize(market);
      lastSeen = currentSize;

      const currentDistance = Math.abs(targetSize - currentSize);

      console.log('Target poll:', {
        market,
        poll: i,
        maxPolls: this.TARGET_PROGRESS_POLLS,
        initialSize,
        currentSize,
        targetSize
      });

      if (currentDistance < this.TOLERANCE) {
        return { kind: 'target', currentSize };
      }

      if (Math.sign(targetSize - initialSize) !== 0 && Math.sign(targetSize - currentSize) !== 0) {
        const initialDiffSign = Math.sign(targetSize - initialSize);
        const currentDiffSign = Math.sign(targetSize - currentSize);

        if (initialDiffSign !== currentDiffSign && currentDistance > this.TOLERANCE) {
          return { kind: 'flipped', currentSize };
        }
      }

      if (currentDistance < initialDistance - this.TOLERANCE) {
        return { kind: 'progress', currentSize };
      }
    }

    return { kind: 'unchanged', currentSize: lastSeen };
  }

  private async placeCorrectionOrder(
    market: string,
    side: OrderSide,
    size: number,
    reduceOnly: boolean,
    priceReference?: CorrectionOrderPriceReference
  ): Promise<PlacedCorrectionOrder> {
    const marketInfo = await this.getMarketInfoBestEffort(market);
    const sizeCheck = this.getCorrectionOrderSizeCheck(market, size, marketInfo);

    this.assertCorrectionOrderSizeSupported(market, sizeCheck);

    const pricing = await this.resolveCorrectionOrderPricing(
      market,
      side,
      priceReference,
      marketInfo
    );

    const clientId = this.createClientId();
    const submittedAt = Date.now();
    const goodTilBlockForward = this.getMarketOrderGoodTilBlockForward();

    console.log('Correction order:', {
      market,
      side,
      size,
      price: pricing.price,
      referencePrice: pricing.referencePrice,
      priceSource: pricing.priceSource,
      slippagePct: pricing.slippagePct,
      usedFallbackWorstPrice: pricing.usedFallbackWorstPrice,
      minOrderSize: sizeCheck.minOrderSize,
      roundedOrderSize: sizeCheck.roundedOrderSize,
      stepSize: sizeCheck.stepSize,
      goodTilBlockBuffer: goodTilBlockForward,
      clientId,
      reduceOnly
    });

    const placedOrder: PlacedCorrectionOrder = {
      market,
      side,
      size,
      price: pricing.price,
      referencePrice: pricing.referencePrice,
      priceSource: pricing.priceSource,
      slippagePct: pricing.slippagePct,
      usedFallbackWorstPrice: pricing.usedFallbackWorstPrice,
      minOrderSize: sizeCheck.minOrderSize,
      roundedOrderSize: sizeCheck.roundedOrderSize,
      stepSize: sizeCheck.stepSize,
      goodTilBlockBuffer: goodTilBlockForward,
      clientId,
      reduceOnly,
      submittedAt
    };

    try {
      const submitResult = await this.submitDydxTransaction(
        'Correction order',
        async () => {
          const goodTilBlock = await this.getMarketOrderGoodTilBlock();

          placedOrder.currentHeight = goodTilBlock.currentHeight;
          placedOrder.goodTilBlock = goodTilBlock.goodTilBlock;
          placedOrder.goodTilBlockBuffer = goodTilBlock.goodTilBlockForward;

          return this.client.placeOrder(
            this.subaccount,
            market,
            OrderType.MARKET,
            side,
            pricing.price,
            size,
            clientId,
            OrderTimeInForce.IOC,
            undefined,
            OrderExecution.IOC,
            false,
            reduceOnly,
            undefined,
            undefined,
            goodTilBlock.currentHeight,
            goodTilBlock.goodTilBlock
          );
        },
        placedOrder
      );

      console.log('Correction order submit result:', {
        market,
        side,
        size,
        price: pricing.price,
        referencePrice: pricing.referencePrice,
        priceSource: pricing.priceSource,
        slippagePct: pricing.slippagePct,
        usedFallbackWorstPrice: pricing.usedFallbackWorstPrice,
        minOrderSize: sizeCheck.minOrderSize,
        roundedOrderSize: sizeCheck.roundedOrderSize,
        stepSize: sizeCheck.stepSize,
        goodTilBlockBuffer: placedOrder.goodTilBlockBuffer,
        goodTilBlock: placedOrder.goodTilBlock,
        currentHeight: placedOrder.currentHeight,
        clientId,
        reduceOnly,
        submitResult
      });

      return {
        ...placedOrder,
        submitResult
      };
    } catch (error) {
      console.error('Correction order placement rejected before polling:', {
        ...placedOrder,
        error: this.serializeError(error)
      });

      await this.logOrderDiagnostics(
        'Correction order placement rejected before polling.',
        market,
        placedOrder,
        { error: this.serializeError(error) }
      );

      throw error;
    }
  }

  private getAlertPriceReference(
    alert: AlertObject,
    telemetry: TradingViewTelemetry
  ): CorrectionOrderPriceReference | undefined {
    const data = alert as any;
    const candidates = [
      { source: 'alert.current_price', value: data.current_price },
      { source: 'alert.currentPrice', value: data.currentPrice },
      { source: 'alert.close', value: data.close },
      { source: 'alert.close_price', value: data.close_price },
      { source: 'alert.closePrice', value: data.closePrice },
      { source: 'alert.price', value: data.price },
      { source: 'alert.entry_price', value: data.entry_price },
      { source: 'alert.entryPrice', value: data.entryPrice },
      { source: 'telemetry.currentPrice', value: telemetry.currentPrice },
      { source: 'telemetry.alertPrice', value: telemetry.alertPrice },
      { source: 'telemetry.entryPrice', value: telemetry.entryPrice }
    ];

    for (const candidate of candidates) {
      const price = this.parsePositiveNumber(candidate.value);

      if (price !== undefined) {
        return {
          price,
          source: candidate.source
        };
      }
    }

    return undefined;
  }

  private async resolveCorrectionOrderPricing(
    market: string,
    side: OrderSide,
    alertReference?: CorrectionOrderPriceReference,
    marketInfo?: any
  ): Promise<CorrectionOrderPricing> {
    const marketReference = await this.getMarketPriceReferenceBestEffort(market, marketInfo);
    const reference = marketReference ?? alertReference;
    const slippagePct = this.getMarketOrderSlippagePct();

    if (reference?.price && Number.isFinite(slippagePct) && slippagePct > 0) {
      return {
        price:
          side === OrderSide.BUY
            ? reference.price * (1 + slippagePct)
            : reference.price * (1 - slippagePct),
        referencePrice: reference.price,
        priceSource: reference.source,
        slippagePct,
        usedFallbackWorstPrice: false
      };
    }

    const fallbackPrice = side === OrderSide.BUY
      ? this.MARKET_BUY_WORST_PRICE
      : this.MARKET_SELL_WORST_PRICE;

    console.warn('Correction order using legacy worst-price fallback because no reference price was available.', {
      market,
      side,
      fallbackPrice,
      alertReference
    });

    return {
      price: fallbackPrice,
      referencePrice: reference?.price,
      priceSource: reference?.source ?? 'LEGACY_WORST_PRICE_FALLBACK',
      slippagePct,
      usedFallbackWorstPrice: true
    };
  }

  private async getMarketPriceReferenceBestEffort(
    market: string,
    marketInfo?: any
  ): Promise<CorrectionOrderPriceReference | undefined> {
    try {
      const price = this.getMarketInfoReferencePrice(marketInfo);

      if (price !== undefined) {
        return {
          price,
          source: 'dydx_indexer_market'
        };
      }

      const fetchedMarketInfo = await this.getMarketInfoBestEffort(market);
      const fetchedPrice = this.getMarketInfoReferencePrice(fetchedMarketInfo);

      if (fetchedPrice !== undefined) {
        return {
          price: fetchedPrice,
          source: 'dydx_indexer_market'
        };
      }
    } catch (error) {
      console.warn('Could not fetch dYdX market reference price; falling back to alert price.', {
        market,
        error: this.serializeError(error)
      });
    }

    return undefined;
  }

  private async getMarketInfoBestEffort(market: string): Promise<any | undefined> {
    try {
      const marketsApi = (this.indexer as any).markets;

      if (typeof marketsApi?.getPerpetualMarkets !== 'function') {
        return undefined;
      }

      const response = await marketsApi.getPerpetualMarkets(market);

      return this.getMarketInfoFromResponse(response, market);
    } catch (error) {
      console.warn('Could not fetch dYdX market info.', {
        market,
        error: this.serializeError(error)
      });

      return undefined;
    }
  }

  private getMarketInfoFromResponse(response: any, market: string): any | undefined {
    const markets = response?.markets;

    if (!markets || typeof markets !== 'object') {
      return undefined;
    }

    const normalizedMarket = this.normalizeMarket(market);

    return (
      markets[market] ??
      markets[normalizedMarket] ??
      Object.keys(markets)
        .map(key => markets[key])
        .find((item: any) =>
          this.normalizeMarket(item?.ticker ?? item?.market ?? item?.id) === normalizedMarket
        )
    );
  }

  private getMarketInfoReferencePrice(marketInfo: any): number | undefined {
    if (!marketInfo) {
      return undefined;
    }

    return this.getFirstPositiveNumber([
      marketInfo.oraclePrice,
      marketInfo.oracle_price,
      marketInfo.indexPrice,
      marketInfo.index_price,
      marketInfo.price,
      marketInfo.midPrice,
      marketInfo.mid_price
    ]);
  }

  private getCorrectionOrderSizeCheck(
    market: string,
    size: number,
    marketInfo?: any
  ): CorrectionOrderSizeCheck {
    const atomicResolution = Number(marketInfo?.atomicResolution);
    const stepBaseQuantums = Number(marketInfo?.stepBaseQuantums);

    if (
      !Number.isFinite(atomicResolution) ||
      !Number.isFinite(stepBaseQuantums) ||
      stepBaseQuantums <= 0
    ) {
      return {
        requestedSize: size,
        source: 'UNKNOWN'
      };
    }

    const baseQuantumMultiplier = 10 ** (-1 * atomicResolution);
    const rawQuantums = size * baseQuantumMultiplier;
    const roundedQuantums =
      Math.floor((rawQuantums + stepBaseQuantums * 1e-9) / stepBaseQuantums) * stepBaseQuantums;
    const finalQuantums = Math.max(roundedQuantums, stepBaseQuantums);
    const stepSize = stepBaseQuantums / baseQuantumMultiplier;
    const roundedOrderSize = finalQuantums / baseQuantumMultiplier;

    return {
      requestedSize: size,
      minOrderSize: stepSize,
      roundedOrderSize,
      stepSize,
      source: `DYDX_MARKET_INFO:${this.normalizeMarket(market)}`
    };
  }

  private assertCorrectionOrderSizeSupported(
    market: string,
    sizeCheck: CorrectionOrderSizeCheck
  ): void {
    if (
      sizeCheck.minOrderSize !== undefined &&
      sizeCheck.requestedSize + this.TOLERANCE < sizeCheck.minOrderSize
    ) {
      throw new Error(
        `Order size ${sizeCheck.requestedSize} is below dYdX minimum ${sizeCheck.minOrderSize} for ${market}. Increase the alert size; otherwise dYdX would round it up to ${sizeCheck.roundedOrderSize}.`
      );
    }

    if (
      sizeCheck.roundedOrderSize !== undefined &&
      Math.abs(sizeCheck.roundedOrderSize - sizeCheck.requestedSize) > this.TOLERANCE
    ) {
      console.warn('dYdX will quantize correction order size to market step size.', {
        market,
        requestedSize: sizeCheck.requestedSize,
        roundedOrderSize: sizeCheck.roundedOrderSize,
        minOrderSize: sizeCheck.minOrderSize,
        stepSize: sizeCheck.stepSize,
        source: sizeCheck.source
      });
    }
  }

  private getMarketOrderSlippagePct(): number {
    if (!Number.isFinite(this.MARKET_ORDER_SLIPPAGE_PCT) || this.MARKET_ORDER_SLIPPAGE_PCT <= 0) {
      return 0.03;
    }

    return Math.min(this.MARKET_ORDER_SLIPPAGE_PCT, 0.5);
  }

  private getMarketOrderGoodTilBlockForward(): number {
    const configured = Math.floor(this.MARKET_ORDER_GOOD_TIL_BLOCKS);

    if (!Number.isFinite(configured) || configured <= 0) {
      return this.MAX_SHORT_TERM_GOOD_TIL_BLOCKS;
    }

    return Math.min(configured, this.MAX_SHORT_TERM_GOOD_TIL_BLOCKS);
  }

  private async getMarketOrderGoodTilBlock(): Promise<MarketOrderGoodTilBlock> {
    const goodTilBlockForward = this.getMarketOrderGoodTilBlockForward();

    try {
      const currentHeightRaw = await (this.client as any).validatorClient.get.latestBlockHeight();
      const currentHeight = Number(currentHeightRaw);

      if (!Number.isFinite(currentHeight) || currentHeight <= 0) {
        throw new Error(`Invalid latest block height: ${String(currentHeightRaw)}`);
      }

      return {
        currentHeight,
        goodTilBlock: currentHeight + goodTilBlockForward,
        goodTilBlockForward
      };
    } catch (error) {
      console.warn('Could not calculate explicit dYdX GoodTilBlock; client default will be used.', {
        goodTilBlockForward,
        error: this.serializeError(error)
      });

      return {
        goodTilBlockForward
      };
    }
  }

  private isAcceptedBroadcastResult(result: any): boolean {
    const code = Number(result?.code ?? result?.result?.code);

    return Number.isFinite(code) && code === 0;
  }

  private async logOrderDiagnostics(
    label: string,
    market: string,
    placedOrder?: PlacedCorrectionOrder,
    context: Record<string, unknown> = {}
  ): Promise<void> {
    const diagnostics: Record<string, unknown> = {
      label,
      market,
      placedOrder,
      context
    };

    try {
      diagnostics.currentPosition = await this.getCurrentPosition(market);
    } catch (error) {
      diagnostics.currentPositionError = this.serializeError(error);
    }

    try {
      diagnostics.market = await this.getMarketSnapshotBestEffort(market);
    } catch (error) {
      diagnostics.marketError = this.serializeError(error);
    }

    try {
      diagnostics.orderbook = await this.getOrderbookSnapshotBestEffort(market);
    } catch (error) {
      diagnostics.orderbookError = this.serializeError(error);
    }

    try {
      const allMarketOrders = await this.getAllOrdersForMarket(market);
      const matchingOrders = placedOrder
        ? allMarketOrders.filter((order: any) =>
            this.orderClientIdMatches(order, placedOrder.clientId)
          )
        : [];

      diagnostics.allMarketOrderCount = allMarketOrders.length;
      diagnostics.matchingOrders = matchingOrders.map((order: any) => this.summarizeOrder(order));
      diagnostics.recentMarketOrders = allMarketOrders
        .slice(0, 10)
        .map((order: any) => this.summarizeOrder(order));

      if (this.LOG_RAW_ORDER_SNAPSHOTS) {
        diagnostics.rawMatchingOrders = matchingOrders;
      }
    } catch (error) {
      diagnostics.ordersError = this.serializeError(error);
    }

    try {
      diagnostics.subaccount = await this.getSubaccountSnapshotBestEffort();
    } catch (error) {
      diagnostics.subaccountError = this.serializeError(error);
    }

    console.warn('dYdX order diagnostics:', diagnostics);
  }

  private async getMarketSnapshotBestEffort(market: string): Promise<any> {
    const marketsApi = (this.indexer as any).markets;

    if (typeof marketsApi?.getPerpetualMarkets !== 'function') {
      return { available: false, reason: 'Indexer markets.getPerpetualMarkets is unavailable.' };
    }

    const marketInfo = await this.getMarketInfoBestEffort(market);

    if (!marketInfo) {
      return {
        available: false,
        reason: 'Market was not found in dYdX indexer response.'
      };
    }

    return this.summarizeMarketInfo(marketInfo);
  }

  private async getOrderbookSnapshotBestEffort(market: string): Promise<any> {
    const marketsApi = (this.indexer as any).markets;

    if (typeof marketsApi?.getPerpetualMarketOrderbook !== 'function') {
      return { available: false, reason: 'Indexer markets.getPerpetualMarketOrderbook is unavailable.' };
    }

    const response = await marketsApi.getPerpetualMarketOrderbook(market);
    const bids = Array.isArray(response?.bids) ? response.bids : [];
    const asks = Array.isArray(response?.asks) ? response.asks : [];
    const bestBid = bids[0];
    const bestAsk = asks[0];
    const bestBidPrice = this.parsePositiveNumber(bestBid?.price ?? bestBid?.[0]);
    const bestAskPrice = this.parsePositiveNumber(bestAsk?.price ?? bestAsk?.[0]);

    return {
      bidCount: bids.length,
      askCount: asks.length,
      bestBid: bestBid
        ? {
            price: bestBidPrice,
            size: this.parsePositiveNumber(bestBid.size ?? bestBid[1])
          }
        : undefined,
      bestAsk: bestAsk
        ? {
            price: bestAskPrice,
            size: this.parsePositiveNumber(bestAsk.size ?? bestAsk[1])
          }
        : undefined,
      spreadPct:
        bestBidPrice !== undefined && bestAskPrice !== undefined
          ? (bestAskPrice - bestBidPrice) / ((bestAskPrice + bestBidPrice) / 2)
          : undefined
    };
  }

  private summarizeMarketInfo(marketInfo: any): any {
    return this.compactObject({
      ticker: marketInfo.ticker ?? marketInfo.market ?? marketInfo.id,
      status: marketInfo.status,
      clobPairId: marketInfo.clobPairId,
      atomicResolution: marketInfo.atomicResolution,
      quantumConversionExponent: marketInfo.quantumConversionExponent,
      subticksPerTick: marketInfo.subticksPerTick,
      stepBaseQuantums: marketInfo.stepBaseQuantums,
      oraclePrice: this.parsePositiveNumber(marketInfo.oraclePrice ?? marketInfo.oracle_price),
      indexPrice: this.parsePositiveNumber(marketInfo.indexPrice ?? marketInfo.index_price),
      initialMarginFraction: marketInfo.initialMarginFraction ?? marketInfo.initial_margin_fraction,
      maintenanceMarginFraction: marketInfo.maintenanceMarginFraction ?? marketInfo.maintenance_margin_fraction,
      baseOpenInterest: marketInfo.baseOpenInterest ?? marketInfo.base_open_interest,
      nextFundingRate: marketInfo.nextFundingRate ?? marketInfo.next_funding_rate,
      openInterest: marketInfo.openInterest ?? marketInfo.open_interest
    });
  }

  private async getSubaccountSnapshotBestEffort(): Promise<any> {
    const accountApi = this.indexer.account as any;

    if (typeof accountApi.getSubaccount === 'function') {
      return accountApi.getSubaccount(this.wallet.address, 0);
    }

    const [perpetualPositions, assetPositions] = await Promise.allSettled([
      accountApi.getSubaccountPerpetualPositions?.(this.wallet.address, 0),
      accountApi.getSubaccountAssetPositions?.(this.wallet.address, 0)
    ]);

    return {
      perpetualPositions:
        perpetualPositions.status === 'fulfilled'
          ? perpetualPositions.value
          : this.serializeError(perpetualPositions.reason),
      assetPositions:
        assetPositions.status === 'fulfilled'
          ? assetPositions.value
          : this.serializeError(assetPositions.reason)
    };
  }

  private async placeStaticSafetyStopAfterEntry(
    market: string,
    targetSize: number,
    alert: AlertObject
  ): Promise<ManagedStop> {
    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      throw new Error(`Safety stop not placed for ${market}: position is flat after entry.`);
    }

    if (Math.sign(position.size) !== Math.sign(targetSize)) {
      throw new Error(
        `Safety stop not placed for ${market}: position direction mismatch. Current=${position.size}, target=${targetSize}`
      );
    }

    const telemetry = this.getTradingViewTelemetry(alert, position.size > 0);
    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const isLong = position.size > 0;
    const staticStopFromAlert = this.getStaticStopFromAlert(alert, isLong);
    const triggerPrice = this.getInitialSafetyStopTriggerPrice(
      alert,
      isLong,
      entryReferencePrice,
      staticStopFromAlert
    );
    const executionPrice = this.getStopExecutionPrice(isLong, triggerPrice);
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const size = Math.abs(position.size);

    console.log('Placing static safety stop:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      positionSize: position.size,
      entryReferencePrice,
      triggerPrice,
      executionPrice,
      side,
      size,
      source: staticStopFromAlert ? 'TRADINGVIEW_STATIC_SL' : 'ENV_FALLBACK'
    });

    const placedStop = await this.placeSafetyStopOrder(
      market,
      side,
      size,
      triggerPrice,
      executionPrice
    );

    const managedStop: ManagedStop = {
      market,
      side,
      triggerPrice,
      clientId: placedStop.clientId,
      size: placedStop.size,
      source: 'STATIC',
      updatedAt: Date.now(),
      goodTilBlockTime: placedStop.goodTilBlockTime
    };

    this.managedStops.set(market, managedStop);

    await this.waitForSafetyStopVisibleBestEffort(
      market,
      side,
      triggerPrice,
      placedStop.clientId
    );

    return managedStop;
  }

  private async placeSafetyTakeProfitAfterEntry(
    market: string,
    targetSize: number,
    alert: AlertObject,
    safetyStop: ManagedStop
  ): Promise<ManagedTakeProfit | undefined> {
    if (!this.SAFETY_TP_ENABLED) {
      return undefined;
    }

    const sizeFraction = Math.min(this.SAFETY_TP_SIZE_FRACTION, 1);
    const riskMultiplier =
      Number.isFinite(this.SAFETY_TP_RISK_MULTIPLIER) && this.SAFETY_TP_RISK_MULTIPLIER > 0
        ? this.SAFETY_TP_RISK_MULTIPLIER
        : 1;

    if (!Number.isFinite(sizeFraction) || sizeFraction <= 0) {
      console.warn('Safety TP skipped: invalid size fraction.', {
        market,
        safetyTpSizeFraction: this.SAFETY_TP_SIZE_FRACTION
      });
      return undefined;
    }

    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      console.log('Safety TP skipped: position is flat after entry.', { market });
      return undefined;
    }

    if (Math.sign(position.size) !== Math.sign(targetSize)) {
      throw new Error(
        `Safety TP not placed for ${market}: position direction mismatch. Current=${position.size}, target=${targetSize}`
      );
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const positionSize = Math.abs(position.size);
    const tpSize = Number((positionSize * sizeFraction).toFixed(6));
    const riskPerUnit = Math.abs(entryReferencePrice - safetyStop.triggerPrice);
    const riskToCover = riskPerUnit * positionSize * riskMultiplier;
    const profitPerUnitNeeded = riskToCover / tpSize;
    const triggerPrice = isLong
      ? entryReferencePrice + profitPerUnitNeeded
      : entryReferencePrice - profitPerUnitNeeded;

    if (tpSize < this.TOLERANCE) {
      console.log('Safety TP skipped: TP size is below tolerance.', {
        market,
        positionSize,
        sizeFraction,
        tpSize
      });
      return undefined;
    }

    if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) {
      throw new Error(`Safety TP trigger price invalid for ${market}. Calculated price=${triggerPrice}`);
    }

    console.log('Placing safety take profit:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      positionSize,
      entryReferencePrice,
      safetyStopPrice: safetyStop.triggerPrice,
      safetyStopDistance: riskPerUnit,
      riskToCover,
      riskMultiplier,
      sizeFraction,
      tpSize,
      side,
      triggerPrice,
      reduceOnly: true,
      orderType: 'TAKE_PROFIT_MARKET'
    });

    const placedTakeProfit = await this.placeTakeProfitMarketOrder(
      market,
      side,
      tpSize,
      triggerPrice,
      'Safety TP',
      this.SAFETY_TP_LIFETIME_SECONDS,
      this.SAFETY_TP_SLIPPAGE_PCT
    );

    const executionPrice = this.getTakeProfitExecutionPrice(
      side,
      triggerPrice,
      this.SAFETY_TP_SLIPPAGE_PCT
    );

    const managedTakeProfit: ManagedTakeProfit = {
      market,
      side,
      triggerPrice,
      executionPrice,
      clientId: placedTakeProfit.clientId,
      size: tpSize,
      source: 'SAFETY',
      levelName: 'SAFETY_TP',
      updatedAt: Date.now(),
      goodTilBlockTime: placedTakeProfit.goodTilBlockTime
    };

    this.rememberManagedTakeProfit(market, managedTakeProfit);

    await this.waitForTakeProfitVisibleBestEffort(
      'Safety TP',
      market,
      side,
      triggerPrice,
      placedTakeProfit.clientId
    );

    return managedTakeProfit;
  }

  private async placeExtraTakeProfitsAfterEntry(
    market: string,
    targetSize: number,
    alert: AlertObject,
    safetyTakeProfit?: ManagedTakeProfit
  ): Promise<void> {
    if (!this.EXTRA_TP_ENABLED) {
      return;
    }

    if (!safetyTakeProfit) {
      console.warn('Extra TPs skipped because Safety TP was not placed.', {
        market,
        extraTpEnabled: this.EXTRA_TP_ENABLED,
        safetyTpEnabled: this.SAFETY_TP_ENABLED
      });
      return;
    }

    const levels = this.EXTRA_TP_LEVELS.filter(level =>
      Number.isFinite(level.distancePct) &&
      Number.isFinite(level.remainingSizePct) &&
      level.distancePct > 0 &&
      level.remainingSizePct > 0
    );

    if (levels.length === 0) {
      console.log('Extra TPs skipped: no valid extra TP levels configured.', { market });
      return;
    }

    const totalRemainingSizePct = levels.reduce(
      (sum, level) => sum + level.remainingSizePct,
      0
    );

    if (totalRemainingSizePct > 1 + this.TOLERANCE) {
      throw new Error(
        `Extra TP percentages exceed 100% of remaining size for ${market}. Total=${totalRemainingSizePct}`
      );
    }

    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      console.log('Extra TPs skipped: position is flat after Safety TP setup.', { market });
      return;
    }

    if (Math.sign(position.size) !== Math.sign(targetSize)) {
      throw new Error(
        `Extra TPs not placed for ${market}: position direction mismatch. Current=${position.size}, target=${targetSize}`
      );
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const originalPositionSize = Math.abs(targetSize);
    const remainingReferenceSize = Math.max(
      originalPositionSize - safetyTakeProfit.size,
      0
    );

    if (remainingReferenceSize < this.TOLERANCE) {
      console.log('Extra TPs skipped: no remaining reference size after Safety TP.', {
        market,
        originalPositionSize,
        safetyTpSize: safetyTakeProfit.size
      });
      return;
    }

    console.log('Placing extra take profits:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      entryReferencePrice,
      originalPositionSize,
      safetyTpSize: safetyTakeProfit.size,
      remainingReferenceSize,
      levels,
      manualTuningNote: 'Render places these once at entry. Manual dYdX edits will not be re-managed unless a new entry/flat cleanup happens.'
    });

    for (const level of levels) {
      const size = Number((remainingReferenceSize * level.remainingSizePct).toFixed(6));
      const triggerPrice = isLong
        ? entryReferencePrice * (1 + level.distancePct)
        : entryReferencePrice * (1 - level.distancePct);

      if (size < this.TOLERANCE) {
        console.log('Extra TP skipped: size is below tolerance.', {
          market,
          level,
          size
        });
        continue;
      }

      if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) {
        throw new Error(
          `Extra TP trigger price invalid for ${market} ${level.name}. Calculated price=${triggerPrice}`
        );
      }

      const placedTakeProfit = await this.placeTakeProfitMarketOrder(
        market,
        side,
        size,
        triggerPrice,
        level.name,
        this.EXTRA_TP_LIFETIME_SECONDS,
        this.EXTRA_TP_SLIPPAGE_PCT
      );

      const managedTakeProfit: ManagedTakeProfit = {
        market,
        side,
        triggerPrice,
        executionPrice: this.getTakeProfitExecutionPrice(
          side,
          triggerPrice,
          this.EXTRA_TP_SLIPPAGE_PCT
        ),
        clientId: placedTakeProfit.clientId,
        size,
        source: 'EXTRA_TP',
        levelName: level.name,
        updatedAt: Date.now(),
        goodTilBlockTime: placedTakeProfit.goodTilBlockTime
      };

      this.rememberManagedTakeProfit(market, managedTakeProfit);

      await this.waitForTakeProfitVisibleBestEffort(
        level.name,
        market,
        side,
        triggerPrice,
        placedTakeProfit.clientId
      );
    }
  }

  private async placeExplicitTakeProfitsAfterEntry(
    market: string,
    targetSize: number,
    alert: AlertObject,
    levels: ExplicitTakeProfitLevel[]
  ): Promise<void> {
    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      console.log('Explicit TPs skipped: position is flat after entry.', { market });
      return;
    }

    if (Math.sign(position.size) !== Math.sign(targetSize)) {
      throw new Error(
        `Explicit TPs not placed for ${market}: position direction mismatch. Current=${position.size}, target=${targetSize}`
      );
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const positionSize = Math.abs(position.size);
    const marketInfo = await this.getMarketInfoBestEffort(market);

    console.log('Placing explicit take profits:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      entryReferencePrice,
      positionSize,
      levels
    });

    for (const level of levels) {
      const triggerPrice = level.price;
      const requestedSize = Number(
        (
          level.size && level.size > 0
            ? level.size
            : positionSize * (level.sizeFraction ?? 0)
        ).toFixed(6)
      );
      const sizeCheck = this.getCorrectionOrderSizeCheck(market, requestedSize, marketInfo);
      const minOrderSize = sizeCheck.minOrderSize ?? this.TOLERANCE;
      const size = sizeCheck.roundedOrderSize ?? requestedSize;

      if (requestedSize <= 0 || requestedSize + Number.EPSILON < minOrderSize || size <= 0) {
        console.log('Explicit TP skipped: size is below dYdX market minimum.', {
          market,
          level,
          requestedSize,
          minOrderSize,
          roundedOrderSize: sizeCheck.roundedOrderSize,
          stepSize: sizeCheck.stepSize,
          source: sizeCheck.source
        });
        continue;
      }

      if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) {
        throw new Error(`Explicit TP trigger price invalid for ${market} ${level.name}. Price=${triggerPrice}`);
      }

      if (isLong && triggerPrice <= entryReferencePrice) {
        console.warn('Explicit LONG TP skipped because it is not above entry reference.', {
          market,
          level,
          entryReferencePrice
        });
        continue;
      }

      if (!isLong && triggerPrice >= entryReferencePrice) {
        console.warn('Explicit SHORT TP skipped because it is not below entry reference.', {
          market,
          level,
          entryReferencePrice
        });
        continue;
      }

      const placedTakeProfit = await this.placeTakeProfitMarketOrder(
        market,
        side,
        size,
        triggerPrice,
        level.name,
        this.EXTRA_TP_LIFETIME_SECONDS,
        this.EXTRA_TP_SLIPPAGE_PCT
      );

      const managedTakeProfit: ManagedTakeProfit = {
        market,
        side,
        triggerPrice,
        executionPrice: this.getTakeProfitExecutionPrice(
          side,
          triggerPrice,
          this.EXTRA_TP_SLIPPAGE_PCT
        ),
        clientId: placedTakeProfit.clientId,
        size,
        source: 'EXPLICIT_TP',
        levelName: level.name,
        updatedAt: Date.now(),
        goodTilBlockTime: placedTakeProfit.goodTilBlockTime
      };

      this.rememberManagedTakeProfit(market, managedTakeProfit);

      await this.waitForTakeProfitVisibleBestEffort(
        level.name,
        market,
        side,
        triggerPrice,
        placedTakeProfit.clientId
      );
    }
  }

  private async placeTakeProfitMarketOrder(
    market: string,
    side: OrderSide,
    size: number,
    triggerPrice: number,
    levelName: string,
    lifetimeSeconds: number,
    slippagePct: number
  ): Promise<PlacedConditionalOrder> {
    const clientId = this.createClientId();
    const goodTilBlockTime = Math.floor(Date.now() / 1000) + lifetimeSeconds;
    const executionPrice = this.getTakeProfitExecutionPrice(side, triggerPrice, slippagePct);
    const orderType = this.getTakeProfitMarketOrderType();

    console.log('Submitting take profit order:', {
      market,
      levelName,
      side,
      size,
      triggerPrice,
      executionPrice,
      clientId,
      reduceOnly: true,
      orderType,
      goodTilBlockTime
    });

    await this.submitDydxTransaction(
      'Take profit order',
      () => this.client.placeOrder(
        this.subaccount,
        market,
        orderType,
        side,
        executionPrice,
        size,
        clientId,
        OrderTimeInForce.IOC,
        lifetimeSeconds,
        OrderExecution.IOC,
        false,
        true,
        triggerPrice
      ),
      {
        market,
        levelName,
        side,
        size,
        triggerPrice,
        executionPrice,
        clientId,
        reduceOnly: true,
        orderType,
        goodTilBlockTime
      }
    );

    return {
      clientId,
      size,
      goodTilBlockTime
    };
  }

  private getTakeProfitMarketOrderType(): OrderType {
    const orderType = (OrderType as any).TAKE_PROFIT_MARKET;

    if (orderType === undefined) {
      throw new Error(
        'OrderType.TAKE_PROFIT_MARKET is not available in this dYdX v4 client version. Update @dydxprotocol/v4-client-js or disable TP features.'
      );
    }

    return orderType as OrderType;
  }

  private getTakeProfitExecutionPrice(
    side: OrderSide,
    triggerPrice: number,
    slippagePct = this.SAFETY_TP_SLIPPAGE_PCT
  ): number {
    return side === OrderSide.SELL
      ? triggerPrice * (1 - slippagePct)
      : triggerPrice * (1 + slippagePct);
  }

  private async waitForTakeProfitVisibleBestEffort(
    label: string,
    market: string,
    side: OrderSide,
    triggerPrice: number,
    expectedClientId?: number
  ): Promise<boolean> {
    for (let i = 1; i <= this.SAFETY_STOP_VERIFY_POLLS; i++) {
      await this.sleep(this.SAFETY_STOP_VERIFY_DELAY_MS);

      const orders = await this.getOpenOrdersForMarket(market);
      const takeProfitOrder = this.findTakeProfitOrderInOrders(
        orders,
        side,
        triggerPrice,
        expectedClientId
      );

      console.log(`${label} verify ${i}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        found: Boolean(takeProfitOrder),
        expectedSide: side,
        expectedTriggerPrice: triggerPrice,
        expectedClientId,
        openOrderCount: orders.length
      });

      if (takeProfitOrder) {
        console.log(`${label} visible on dYdX:`, {
          market,
          side,
          triggerPrice,
          expectedClientId,
          matchedOrder: this.summarizeOrder(takeProfitOrder)
        });
        return true;
      }
    }

    console.warn(
      `${label} was submitted but could not be verified through indexer for ${market} at trigger ${triggerPrice}. Check dYdX UI.`
    );

    return false;
  }

  private findTakeProfitOrderInOrders(
    orders: any[],
    side: OrderSide,
    triggerPrice: number,
    expectedClientId?: number
  ): any | undefined {
    return orders.find((order: any) => {
      if (!this.orderSideMatches(order, side)) {
        return false;
      }

      if (!this.getOrderReduceOnly(order)) {
        return false;
      }

      const orderType = this.getOrderTypeText(order);

      if (!orderType.includes('TAKE_PROFIT') && !orderType.includes('TAKE-PROFIT')) {
        return false;
      }

      if (expectedClientId !== undefined && this.orderClientIdMatches(order, expectedClientId)) {
        return true;
      }

      const parsedTrigger = this.getOrderTriggerPrice(order) ?? this.getOrderPrice(order);

      return (
        parsedTrigger !== undefined &&
        Math.abs(parsedTrigger - triggerPrice) / triggerPrice < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT
      );
    });
  }

  private rememberManagedTakeProfit(
    market: string,
    takeProfit: ManagedTakeProfit
  ): void {
    const existing = this.managedTakeProfits.get(market) ?? [];

    this.managedTakeProfits.set(market, [
      ...existing,
      takeProfit
    ]);
  }

  private async placeSafetyStopOrder(
    market: string,
    side: OrderSide,
    size: number,
    triggerPrice: number,
    executionPrice: number
  ): Promise<PlacedConditionalOrder> {
    const marketInfo = await this.getMarketInfoBestEffort(market);
    const sizeCheck = this.getCorrectionOrderSizeCheck(market, size, marketInfo);
    const submittedSize = sizeCheck.roundedOrderSize ?? size;
    const clientId = this.createClientId();
    const goodTilBlockTime = Math.floor(Date.now() / 1000) + this.SAFETY_STOP_LIFETIME_SECONDS;

    console.log('Submitting safety stop order:', {
      market,
      side,
      requestedSize: size,
      size: submittedSize,
      triggerPrice,
      executionPrice,
      clientId,
      reduceOnly: true,
      orderType: OrderType.STOP_MARKET,
      goodTilBlockTime,
      minOrderSize: sizeCheck.minOrderSize,
      roundedOrderSize: sizeCheck.roundedOrderSize,
      stepSize: sizeCheck.stepSize,
      sizeSource: sizeCheck.source
    });

    await this.submitDydxTransaction(
      'Safety stop order',
      () => this.client.placeOrder(
        this.subaccount,
        market,
        OrderType.STOP_MARKET,
        side,
        executionPrice,
        submittedSize,
        clientId,
        OrderTimeInForce.IOC,
        this.SAFETY_STOP_LIFETIME_SECONDS,
        OrderExecution.IOC,
        false,
        true,
        triggerPrice
      ),
      {
        market,
        side,
        requestedSize: size,
        size: submittedSize,
        triggerPrice,
        executionPrice,
        clientId,
        reduceOnly: true,
        orderType: OrderType.STOP_MARKET,
        goodTilBlockTime,
        minOrderSize: sizeCheck.minOrderSize,
        roundedOrderSize: sizeCheck.roundedOrderSize,
        stepSize: sizeCheck.stepSize,
        sizeSource: sizeCheck.source
      }
    );

    return {
      clientId,
      size: submittedSize,
      goodTilBlockTime
    };
  }

  private async waitForSafetyStopVisibleBestEffort(
    market: string,
    side: OrderSide,
    triggerPrice: number,
    expectedClientId?: number
  ): Promise<boolean> {
    for (let i = 1; i <= this.SAFETY_STOP_VERIFY_POLLS; i++) {
      await this.sleep(this.SAFETY_STOP_VERIFY_DELAY_MS);

      const orders = await this.getOpenOrdersForMarket(market);
      const stopOrder = this.findSafetyStopOrderInOrders(
        orders,
        side,
        triggerPrice,
        expectedClientId
      );

      console.log(`Safety stop verify ${i}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        found: Boolean(stopOrder),
        expectedSide: side,
        expectedTriggerPrice: triggerPrice,
        expectedClientId,
        openOrderCount: orders.length
      });

      if (stopOrder) {
        console.log('Safety stop visible on dYdX:', {
          market,
          side,
          triggerPrice,
          expectedClientId,
          matchedOrder: this.summarizeOrder(stopOrder)
        });
        return true;
      }
    }

    console.warn(
      `Safety stop was submitted but could not be verified through indexer for ${market} at trigger ${triggerPrice}. Check dYdX UI.`
    );

    return false;
  }

  private async getMarketSnapshot(market: string): Promise<MarketSnapshot> {
    const [position, allMarketOrders] = await Promise.all([
      this.getCurrentPosition(market),
      this.getAllOrdersForMarket(market)
    ]);

    const openOrders = allMarketOrders.filter((order: any) => this.isVisibleOpenOrder(order));
    const protectiveStops = this.getProtectiveStopOrdersFromOrders(openOrders);
    const managedStop = this.managedStops.get(market);
    const managedTakeProfits = this.managedTakeProfits.get(market) ?? [];

    return {
      position,
      allMarketOrders,
      openOrders,
      protectiveStops,
      managedStop,
      managedTakeProfits
    };
  }

  private logMarketSnapshot(
    label: string,
    market: string,
    snapshot: MarketSnapshot,
    telemetry?: TradingViewTelemetry
  ): void {
    const logPayload: any = {
      label,
      market,
      position: snapshot.position,
      managedStop: snapshot.managedStop,
      managedTakeProfits: snapshot.managedTakeProfits,
      telemetry: telemetry
        ? this.compactObject(telemetry as Record<string, unknown>)
        : undefined,
      allMarketOrderCount: snapshot.allMarketOrders.length,
      openOrderCount: snapshot.openOrders.length,
      protectiveStopCount: snapshot.protectiveStops.length,
      openOrders: snapshot.openOrders.map((order: any) => this.summarizeOrder(order)),
      protectiveStops: snapshot.protectiveStops.map((order: any) => this.summarizeOrder(order))
    };

    if (this.LOG_RAW_ORDER_SNAPSHOTS) {
      logPayload.rawOpenOrders = snapshot.openOrders;
      logPayload.rawProtectiveStops = snapshot.protectiveStops;
    }

    console.log('dYdX market snapshot:', logPayload);
  }

  private resolveCurrentProtectiveStop(
    snapshot: MarketSnapshot,
    side: OrderSide,
    isLong: boolean
  ): StopResolution {
    const matchingOrders = snapshot.protectiveStops.filter((order: any) =>
      this.orderSideMatches(order, side)
    );

    const ordersWithTriggers = matchingOrders
      .map((order: any) => ({
        order,
        triggerPrice: this.getOrderTriggerPrice(order)
      }))
      .filter((item: { order: any; triggerPrice?: number }) => item.triggerPrice !== undefined);

    if (ordersWithTriggers.length > 0) {
      const selected = ordersWithTriggers.reduce((best, current) => {
        if (best.triggerPrice === undefined) {
          return current;
        }

        if (current.triggerPrice === undefined) {
          return best;
        }

        return isLong
          ? current.triggerPrice > best.triggerPrice ? current : best
          : current.triggerPrice < best.triggerPrice ? current : best;
      });

      return {
        triggerPrice: selected.triggerPrice,
        source: 'DYDX_OPEN_ORDER',
        order: selected.order,
        matchingOrders
      };
    }

    const managedStop = snapshot.managedStop;

    if (managedStop && String(managedStop.side).toUpperCase() === String(side).toUpperCase()) {
      return {
        triggerPrice: managedStop.triggerPrice,
        source: 'MANAGED_MEMORY',
        reason: 'No live protective stop was visible through indexer, using Render-managed memory as fallback.',
        matchingOrders
      };
    }

    return {
      source: 'UNKNOWN',
      reason:
        matchingOrders.length === 0
          ? 'No matching protective stop orders found for this market/side.'
          : 'Matching protective stop orders found, but no trigger price could be parsed.',
      matchingOrders
    };
  }

  private async getAllOrdersForMarket(market: string): Promise<any[]> {
    const res = await this.indexer.account.getSubaccountOrders(
      this.wallet.address,
      0
    );

    return res.orders?.filter((order: any) => this.orderMarketMatches(order, market)) || [];
  }

  private async getOpenOrdersForMarket(market: string): Promise<any[]> {
    const orders = await this.getAllOrdersForMarket(market);
    return orders.filter((order: any) => this.isVisibleOpenOrder(order));
  }

  private getProtectiveStopOrdersFromOrders(orders: any[]): any[] {
    return orders.filter((order: any) => this.isProtectiveStopOrder(order));
  }

  private isVisibleOpenOrder(order: any): boolean {
    const visibleStatuses = new Set([
      'OPEN',
      'UNTRIGGERED',
      'OPEN_UNTRIGGERED',
      'PENDING',
      'BEST_EFFORT_OPENED'
    ]);

    return visibleStatuses.has(String(order.status).toUpperCase());
  }

  private isProtectiveStopOrder(order: any): boolean {
    const orderType = this.getOrderTypeText(order);
    const reduceOnly = this.getOrderReduceOnly(order);
    const hasTriggerPrice = this.getOrderTriggerPrice(order) !== undefined;

    if (!reduceOnly) {
      return false;
    }

    if (orderType.includes('TAKE_PROFIT') || orderType.includes('TAKE-PROFIT')) {
      return false;
    }

    if (orderType.includes('STOP')) {
      return true;
    }

    return orderType === '' && hasTriggerPrice;
  }

  private isTakeProfitOrder(order: any): boolean {
    const orderType = this.getOrderTypeText(order);

    return (
      this.getOrderReduceOnly(order) &&
      (orderType.includes('TAKE_PROFIT') || orderType.includes('TAKE-PROFIT'))
    );
  }

  private takeProfitOrdersMatchLevels(
    orders: any[],
    levels: ExplicitTakeProfitLevel[],
    market: string,
    marketInfo?: any
  ): boolean {
    if (orders.length !== levels.length) return false;

    const unmatchedOrders = [...orders];

    for (const level of levels) {
      const requestedSize =
        level.size ??
        0;
      const sizeCheck = this.getCorrectionOrderSizeCheck(market, requestedSize, marketInfo);
      const expectedSize = sizeCheck.roundedOrderSize ?? requestedSize;
      const matchIndex = unmatchedOrders.findIndex((order: any) => {
        const triggerPrice = this.getOrderTriggerPrice(order) ?? this.getOrderPrice(order);
        const orderSize = this.parsePositiveNumber(
          order.size ??
          order.remainingSize ??
          order.remaining_size ??
          order.quantity ??
          order.qty
        );

        return (
          triggerPrice !== undefined &&
          orderSize !== undefined &&
          Math.abs(triggerPrice - level.price) / level.price < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT &&
          Math.abs(orderSize - expectedSize) < this.TOLERANCE
        );
      });

      if (matchIndex < 0) return false;
      unmatchedOrders.splice(matchIndex, 1);
    }

    return unmatchedOrders.length === 0;
  }

  private managedTakeProfitsMatchLevels(
    managedTakeProfits: ManagedTakeProfit[],
    levels: ExplicitTakeProfitLevel[]
  ): boolean {
    if (managedTakeProfits.length !== levels.length) return false;

    const unmatched = [...managedTakeProfits];

    for (const level of levels) {
      const expectedSize = level.size ?? 0;
      const matchIndex = unmatched.findIndex((takeProfit) =>
        Math.abs(takeProfit.triggerPrice - level.price) / level.price < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT &&
        Math.abs(takeProfit.size - expectedSize) < this.TOLERANCE
      );

      if (matchIndex < 0) return false;
      unmatched.splice(matchIndex, 1);
    }

    return unmatched.length === 0;
  }

  private findSafetyStopOrderInOrders(
    orders: any[],
    side: OrderSide,
    triggerPrice: number,
    expectedClientId?: number
  ): any | undefined {
    return orders.find((order: any) => {
      if (!this.orderSideMatches(order, side)) {
        return false;
      }

      if (!this.isProtectiveStopOrder(order)) {
        return false;
      }

      if (expectedClientId !== undefined && this.orderClientIdMatches(order, expectedClientId)) {
        return true;
      }

      const parsedTrigger = this.getOrderTriggerPrice(order);

      return (
        parsedTrigger !== undefined &&
        Math.abs(parsedTrigger - triggerPrice) / triggerPrice < this.STOP_TRIGGER_MATCH_TOLERANCE_PCT
      );
    });
  }

  private async cancelOpenOrders(market: string): Promise<void> {
    const orders = await this.getOpenOrdersForMarket(market);
    const managedStop = this.managedStops.get(market);
    const managedTakeProfits = this.managedTakeProfits.get(market) ?? [];

    await this.cancelSpecificOrders(market, orders);

    const managedStopAlreadyVisible = managedStop
      ? orders.some((order: any) => this.orderClientIdMatches(order, managedStop.clientId))
      : false;

    if (managedStop && !managedStopAlreadyVisible) {
      await this.cancelManagedStopBestEffort(
        market,
        managedStop,
        'Cancelling open orders before new target order.'
      );
    }

    for (const managedTakeProfit of managedTakeProfits) {
      const managedTakeProfitAlreadyVisible = orders.some((order: any) =>
        this.orderClientIdMatches(order, managedTakeProfit.clientId)
      );

      if (!managedTakeProfitAlreadyVisible) {
        await this.cancelManagedTakeProfitBestEffort(
          market,
          managedTakeProfit,
          'Cancelling open orders before new target order.'
        );
      }
    }

    this.managedStops.delete(market);
    this.managedTakeProfits.delete(market);
  }

  private async cancelSpecificOrders(market: string, orders: any[]): Promise<void> {
    for (const order of orders) {
      const clientId = this.getOrderClientId(order);

      if (!Number.isFinite(clientId)) {
        console.warn('Skipping cancel because clientId is invalid:', this.summarizeOrder(order));
        continue;
      }

      const orderFlags = this.getOrderFlags(order);
      const goodTilBlock = this.getOrderGoodTilBlock(order);
      const goodTilBlockTime = this.getOrderGoodTilBlockTime(order);

      console.log('Cancelling open order:', {
        market,
        clientId,
        orderFlags,
        status: order.status,
        type: order.type,
        goodTilBlock,
        goodTilBlockTime,
        order: this.summarizeOrder(order)
      });

      try {
        await this.cancelOrderByFlags(
          market,
          clientId,
          orderFlags,
          goodTilBlock,
          goodTilBlockTime
        );
      } catch (error) {
        console.warn('Open order cancel failed. Check dYdX UI if this order should be gone.', {
          market,
          clientId,
          orderFlags,
          goodTilBlock,
          goodTilBlockTime,
          error: this.serializeError(error)
        });
      }
    }
  }

  private async cancelOtherProtectiveStopsBestEffort(
    market: string,
    side: OrderSide,
    keepClientId: number,
    reason: string
  ): Promise<any[]> {
    let remaining: any[] = [];

    for (let poll = 1; poll <= this.SAFETY_STOP_VERIFY_POLLS; poll += 1) {
      const orders = await this.getOpenOrdersForMarket(market);
      remaining = this.getProtectiveStopOrdersFromOrders(orders)
        .filter((order: any) => this.orderSideMatches(order, side))
        .filter((order: any) => !this.orderClientIdMatches(order, keepClientId));

      console.log(`Protective stop replacement verify ${poll}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        side,
        keepClientId,
        remainingOldStopCount: remaining.length,
        reason
      });

      if (!remaining.length) {
        return [];
      }

      await this.cancelSpecificOrders(market, remaining);
      await this.sleep(this.SAFETY_STOP_VERIFY_DELAY_MS);
    }

    console.warn('Protective stop replacement left older stop orders visible after retries. Check dYdX UI.', {
      market,
      side,
      keepClientId,
      remainingOldStopCount: remaining.length,
      remainingOldStops: remaining.map((order: any) => this.summarizeOrder(order)),
      reason
    });

    return remaining;
  }

  private async cancelOrderByFlags(
    market: string,
    clientId: number,
    orderFlags: number,
    goodTilBlock?: number,
    goodTilBlockTime?: number
  ): Promise<void> {
    const usesGoodTilTime =
      orderFlags === this.CONDITIONAL_ORDER_FLAGS ||
      orderFlags === this.LONG_TERM_ORDER_FLAGS;

    if (usesGoodTilTime && goodTilBlockTime === undefined) {
      throw new Error(
        `Cannot cancel conditional/long-term order ${clientId} for ${market}: missing goodTilTimeInSeconds.`
      );
    }

    await this.submitDydxTransaction(
      'Cancel order',
      () => (this.client as any).cancelOrder(
        this.subaccount,
        clientId,
        orderFlags,
        market,
        usesGoodTilTime ? undefined : goodTilBlock,
        usesGoodTilTime ? goodTilBlockTime : undefined
      ),
      {
        market,
        clientId,
        orderFlags,
        goodTilBlock,
        goodTilBlockTime
      }
    );
  }

  private stopImproves(isLong: boolean, newStop: number, currentStop: number): boolean {
    if (this.MIN_TRAIL_IMPROVEMENT_PCT <= 0) {
      return isLong
        ? newStop > currentStop
        : newStop < currentStop;
    }

    return isLong
      ? newStop > currentStop * (1 + this.MIN_TRAIL_IMPROVEMENT_PCT)
      : newStop < currentStop * (1 - this.MIN_TRAIL_IMPROVEMENT_PCT);
  }

  private getInitialSafetyStopTriggerPrice(
    alert: AlertObject,
    isLong: boolean,
    entryReferencePrice: number,
    staticStopFromAlert?: number
  ): number {
    if (staticStopFromAlert) {
      if (isLong && staticStopFromAlert >= entryReferencePrice) {
        throw new Error(
          `Invalid static_sl for LONG: ${staticStopFromAlert} must be below entry reference ${entryReferencePrice}.`
        );
      }

      if (!isLong && staticStopFromAlert <= entryReferencePrice) {
        throw new Error(
          `Invalid static_sl for SHORT: ${staticStopFromAlert} must be above entry reference ${entryReferencePrice}.`
        );
      }

      return staticStopFromAlert;
    }

    return isLong
      ? entryReferencePrice * (1 - this.SAFETY_STOP_PCT)
      : entryReferencePrice * (1 + this.SAFETY_STOP_PCT);
  }

  private getStopExecutionPrice(isLong: boolean, triggerPrice: number): number {
    return isLong
      ? triggerPrice * (1 - this.SAFETY_STOP_SLIPPAGE_PCT)
      : triggerPrice * (1 + this.SAFETY_STOP_SLIPPAGE_PCT);
  }

  private getFractalTrailBufferPct(alert: AlertObject): number {
    const data = alert as any;

    const fractionBuffer = this.getFirstPositiveNumber([
      data.trail_buffer_fraction,
      data.trailBufferFraction,
      data.fractal_trail_buffer_fraction,
      data.fractalTrailBufferFraction
    ]);

    if (fractionBuffer !== undefined) {
      return fractionBuffer;
    }

    const pctLikeBuffer = this.getFirstPositiveNumber([
      data.trail_buffer_pct,
      data.trailBufferPct,
      data.fractal_trail_buffer_pct,
      data.fractalTrailBufferPct,
      data.stop_buffer_pct,
      data.stopBufferPct,
      data.buffer_pct,
      data.bufferPct
    ]);

    if (pctLikeBuffer !== undefined) {
      return pctLikeBuffer > 0.05
        ? pctLikeBuffer / 100
        : pctLikeBuffer;
    }

    const percentBuffer = this.getFirstPositiveNumber([
      data.trail_buffer_percent,
      data.trailBufferPercent,
      data.trail_buffer_perc,
      data.trailBufferPerc,
      data.fractal_trail_buffer_percent,
      data.fractalTrailBufferPercent,
      data.fractal_trail_buffer_perc,
      data.fractalTrailBufferPerc
    ]);

    if (percentBuffer !== undefined) {
      return percentBuffer / 100;
    }

    return this.FRACTAL_TRAIL_BUFFER_PCT;
  }

  private getTradingViewTelemetry(alert: AlertObject, isLong?: boolean): TradingViewTelemetry {
    const data = alert as any;

    const longTrailStop = this.getFirstPositiveNumber([
      data.trailing_sl_long,
      data.trailingSLLong,
      data.trail_sl_long,
      data.trailSLLong,
      data.long_trailing_sl,
      data.longTrailingSL,
      data.long_trail_stop,
      data.longTrailStop,
      data.trail_stop_long,
      data.trailStopLong
    ]);

    const shortTrailStop = this.getFirstPositiveNumber([
      data.trailing_sl_short,
      data.trailingSLShort,
      data.trail_sl_short,
      data.trailSLShort,
      data.short_trailing_sl,
      data.shortTrailingSL,
      data.short_trail_stop,
      data.shortTrailStop,
      data.trail_stop_short,
      data.trailStopShort
    ]);

    const genericTrailStop = this.getFirstPositiveNumber([
      data.trailing_sl,
      data.trailingSL,
      data.trail_sl,
      data.trailSL,
      data.trail_stop,
      data.trailStop,
      data.trail,
      data.trail_stop_price,
      data.trailStopPrice
    ]);

    const alertPrice = this.parsePositiveNumber(data.price);

    return {
      signal: this.getSignal(alert),
      desiredPosition: String(data.desired_position ?? data.position ?? ''),
      alertPrice,
      currentPrice: this.getFirstPositiveNumber([
        data.current_price,
        data.currentPrice,
        data.close,
        data.close_price,
        data.closePrice
      ]),
      entryPrice: this.getFirstPositiveNumber([
        data.entry_price,
        data.entryPrice,
        data.entry
      ]),
      trailStop:
        isLong === true
          ? longTrailStop ?? genericTrailStop ?? alertPrice
          : isLong === false
            ? shortTrailStop ?? genericTrailStop ?? alertPrice
            : genericTrailStop ?? longTrailStop ?? shortTrailStop ?? alertPrice,
      longTrailStop: longTrailStop ?? genericTrailStop,
      shortTrailStop: shortTrailStop ?? genericTrailStop,
      staticStop: this.getFirstPositiveNumber([
        data.static_sl,
        data.staticSL,
        data.stop_loss,
        data.stopLoss,
        data.stopPrice
      ]),
      longStaticStop: this.getFirstPositiveNumber([
        data.static_sl_long,
        data.staticSLLong,
        data.staticLongSL,
        data.static_long_sl,
        data.long_static_sl,
        data.longStaticSL
      ]),
      shortStaticStop: this.getFirstPositiveNumber([
        data.static_sl_short,
        data.staticSLShort,
        data.static_short_sl,
        data.staticSLShort,
        data.short_static_sl,
        data.shortStaticSL
      ]),
      topFractal: this.getFirstPositiveNumber([
        data.top_fractal,
        data.topFractal,
        data.top_fractal_level,
        data.topFractalLevel,
        data.fractal_top,
        data.fractalTop
      ]),
      bottomFractal: this.getFirstPositiveNumber([
        data.bottom_fractal,
        data.bottomFractal,
        data.bottom_fractal_level,
        data.bottomFractalLevel,
        data.fractal_bottom,
        data.fractalBottom
      ]),
      floatingLongEntry: this.getFirstPositiveNumber([
        data.floating_long_entry,
        data.floatingLongEntry,
        data.float_long_entry,
        data.floatLongEntry,
        data.long_float,
        data.longFloat
      ]),
      floatingShortEntry: this.getFirstPositiveNumber([
        data.floating_short_entry,
        data.floatingShortEntry,
        data.float_short_entry,
        data.floatShortEntry,
        data.short_float,
        data.shortFloat
      ]),
      barTime: data.bar_time ?? data.barTime,
      rawTime: data.time
    };
  }

  private getStaticStopFromAlert(alert: AlertObject, isLong: boolean): number | undefined {
    const telemetry = this.getTradingViewTelemetry(alert, isLong);

    return isLong
      ? telemetry.longStaticStop ?? telemetry.staticStop
      : telemetry.shortStaticStop ?? telemetry.staticStop;
  }

  private shouldSkipStaticStop(alert: AlertObject): boolean {
    const data = alert as any;

    return (
      data.skip_static_stop === true ||
      data.skipStaticStop === true ||
      String(data.skip_static_stop).toLowerCase() === 'true' ||
      String(data.skipStaticStop).toLowerCase() === 'true'
    );
  }

  private getExplicitTakeProfitLevels(alert: AlertObject): ExplicitTakeProfitLevel[] {
    const data = alert as any;
    const raw =
      data.take_profits ??
      data.takeProfits ??
      data.take_profit_levels ??
      data.takeProfitLevels ??
      data.tp_levels ??
      data.tpLevels ??
      data.tp_prices ??
      data.tpPrices;

    const values = Array.isArray(raw)
      ? raw
      : typeof raw === 'string'
        ? raw.split(',').map((item) => item.trim()).filter(Boolean)
        : [];

    const parsed = values
      .map((value: any, index: number): ExplicitTakeProfitLevel | undefined => {
        const price = this.parsePositiveNumber(
          typeof value === 'object' && value !== null
            ? value.price ?? value.triggerPrice ?? value.trigger_price
            : value
        );

        if (!price) {
          return undefined;
        }

        const size = this.parsePositiveNumber(
          typeof value === 'object' && value !== null
            ? value.size ?? value.quantity ?? value.qty
            : undefined
        );
        const sizeFraction = this.parseTakeProfitFraction(
          typeof value === 'object' && value !== null
            ? value.sizeFraction ??
              value.size_fraction ??
              value.sizePct ??
              value.size_pct ??
              value.fraction
            : undefined
        );

        return {
          name: String(
            typeof value === 'object' && value !== null
              ? value.label ?? value.name ?? `TP${index + 1}`
              : `TP${index + 1}`
          ),
          price,
          size,
          sizeFraction
        };
      })
      .filter((level): level is ExplicitTakeProfitLevel => level !== undefined)
      .slice(0, 6);

    const missingFraction = parsed.filter(
      (level) => !level.size && (!level.sizeFraction || level.sizeFraction <= 0)
    );

    if (parsed.length && missingFraction.length === parsed.length) {
      const equalFraction = 1 / parsed.length;
      return parsed.map((level) => ({
        ...level,
        sizeFraction: equalFraction
      }));
    }

    if (missingFraction.length) {
      const usedFraction = parsed.reduce((sum, level) => sum + (level.sizeFraction || 0), 0);
      const remainingFraction = Math.max(0, 1 - usedFraction);
      const fallbackFraction = remainingFraction / missingFraction.length;

      const withFallbacks = parsed.map((level) =>
        level.size || level.sizeFraction
          ? level
          : { ...level, sizeFraction: fallbackFraction }
      );

      return this.normalizeTakeProfitFractions(withFallbacks);
    }

    return this.normalizeTakeProfitFractions(parsed);
  }

  private parseTakeProfitFraction(value: unknown): number | undefined {
    const parsed = this.parsePositiveNumber(value);

    if (parsed === undefined) {
      return undefined;
    }

    return parsed > 1
      ? parsed / 100
      : parsed;
  }

  private normalizeTakeProfitFractions(levels: ExplicitTakeProfitLevel[]): ExplicitTakeProfitLevel[] {
    const fractionTotal = levels.reduce((sum, level) => sum + (level.sizeFraction || 0), 0);

    if (fractionTotal <= 1 + this.TOLERANCE) {
      return levels;
    }

    return levels.map((level) => ({
      ...level,
      sizeFraction: level.sizeFraction
        ? level.sizeFraction / fractionTotal
        : level.sizeFraction
    }));
  }

  private getOrderTriggerPrice(order: any): number | undefined {
    const candidates = this.getOrderTriggerPriceCandidates(order);

    for (const candidate of candidates) {
      if (candidate.parsed !== undefined) {
        return candidate.parsed;
      }
    }

    return undefined;
  }

  private getOrderTriggerPriceCandidates(order: any): PriceCandidate[] {
    const candidates: PriceCandidate[] = [];
    const seen = new Set<string>();

    const pushCandidate = (path: string, value: unknown) => {
      if (seen.has(path)) {
        return;
      }

      seen.add(path);

      if (value === undefined || value === null || value === '') {
        return;
      }

      candidates.push({
        path,
        value,
        parsed: this.parsePositiveNumber(value)
      });
    };

    pushCandidate('triggerPrice', order.triggerPrice);
    pushCandidate('trigger_price', order.trigger_price);
    pushCandidate('conditionalOrderTriggerPrice', order.conditionalOrderTriggerPrice);
    pushCandidate('conditional_order_trigger_price', order.conditional_order_trigger_price);
    pushCandidate('stopPrice', order.stopPrice);
    pushCandidate('stop_price', order.stop_price);
    pushCandidate('activationPrice', order.activationPrice);
    pushCandidate('activation_price', order.activation_price);
    pushCandidate('order.triggerPrice', order.order?.triggerPrice);
    pushCandidate('order.trigger_price', order.order?.trigger_price);
    pushCandidate('order.conditionalOrderTriggerPrice', order.order?.conditionalOrderTriggerPrice);
    pushCandidate('order.conditional_order_trigger_price', order.order?.conditional_order_trigger_price);
    pushCandidate('order.stopPrice', order.order?.stopPrice);
    pushCandidate('order.stop_price', order.order?.stop_price);

    this.collectTriggerPriceCandidates(order, '', 0, candidates, seen, new Set<object>());

    return candidates;
  }

  private collectTriggerPriceCandidates(
    value: unknown,
    path: string,
    depth: number,
    candidates: PriceCandidate[],
    seenPaths: Set<string>,
    seenObjects: Set<object>
  ): void {
    if (depth > 4 || value === null || typeof value !== 'object') {
      return;
    }

    if (seenObjects.has(value as object)) {
      return;
    }

    seenObjects.add(value as object);

    for (const [key, childValue] of Object.entries(value as Record<string, unknown>)) {
      const childPath = path ? `${path}.${key}` : key;
      const normalizedKey = key.toLowerCase();

      const looksLikeTriggerPrice =
        (
          normalizedKey.includes('trigger') ||
          normalizedKey.includes('stop') ||
          normalizedKey.includes('activation')
        ) &&
        normalizedKey.includes('price');

      if (looksLikeTriggerPrice && !seenPaths.has(childPath)) {
        seenPaths.add(childPath);

        if (childValue !== undefined && childValue !== null && childValue !== '') {
          candidates.push({
            path: childPath,
            value: childValue,
            parsed: this.parsePositiveNumber(childValue)
          });
        }
      }

      if (typeof childValue === 'object' && childValue !== null) {
        this.collectTriggerPriceCandidates(
          childValue,
          childPath,
          depth + 1,
          candidates,
          seenPaths,
          seenObjects
        );
      }
    }
  }

  private summarizeOrder(order: any): any {
    return {
      id: order.id ?? order.orderId?.id ?? order.order_id?.id,
      clientId: order.clientId ?? order.client_id ?? order.orderId?.clientId ?? order.order_id?.client_id,
      market: order.market ?? order.ticker,
      side: order.side,
      type: order.type ?? order.orderType ?? order.order_type,
      status: order.status,
      reduceOnly: order.reduceOnly ?? order.reduce_only,
      size: order.size,
      totalFilled: order.totalFilled ?? order.total_filled,
      price: order.price,
      triggerPrice: this.getOrderTriggerPrice(order),
      orderFlags: this.getRawOrderFlags(order),
      inferredOrderFlags: this.getOrderFlags(order),
      goodTilBlockTime: this.getOrderGoodTilBlockTime(order),
      goodTilBlock: this.getOrderGoodTilBlock(order),
      timeInForce: order.timeInForce ?? order.time_in_force,
      execution: order.execution,
      postOnly: order.postOnly ?? order.post_only,
      createdAt: order.createdAt ?? order.created_at,
      updatedAt: order.updatedAt ?? order.updated_at,
      diagnostics: this.getOrderDiagnosticFields(order)
    };
  }

  private getOrderPrice(order: any): number | undefined {
    return this.parsePositiveNumber(
      order.price ??
      order.limitPrice ??
      order.limit_price ??
      order.order?.price ??
      order.order?.limitPrice ??
      order.order?.limit_price
    );
  }

  private async getCurrentSize(market: string): Promise<number> {
    const position = await this.getCurrentPosition(market);
    return position.size;
  }

  private async getCurrentPosition(market: string): Promise<PositionSnapshot> {
    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const pos = response.positions.find((p: any) => this.normalizeMarket(p.market) === market);

    if (!pos) {
      return { size: 0 };
    }

    return {
      size: Number(pos.size),
      entryPrice: this.parsePositiveNumber(
        pos.entryPrice ??
        pos.entry_price ??
        pos.averageEntryPrice ??
        pos.average_entry_price
      )
    };
  }

  private getTargetSize(alert: AlertObject, baseSize: number): number {
    const dir = String((alert as any).desired_position ?? (alert as any).position ?? '')
      .toUpperCase();

    switch (dir) {
      case 'BUY':
      case 'LONG':
        return Math.abs(baseSize);
      case 'SELL':
      case 'SHORT':
        return -Math.abs(baseSize);
      case 'FLAT':
      default:
        return 0;
    }
  }

  private getSafetyStopReferencePrice(alert: AlertObject, positionEntryPrice?: number): number {
    if (positionEntryPrice && positionEntryPrice > 0) {
      return positionEntryPrice;
    }

    const alertPrice = this.parsePositiveNumber(
      (alert as any).entry_price ??
      (alert as any).entryPrice ??
      (alert as any).price
    );

    if (alertPrice && alertPrice > 0) {
      return alertPrice;
    }

    throw new Error('Cannot place safety stop: missing entry reference price.');
  }

  private getSignal(alert: AlertObject): string {
    return String((alert as any).signal ?? '')
      .trim()
      .toUpperCase()
      .replace(/[\s-]+/g, '_');
  }

  private parsePositiveNumber(value: unknown): number | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    const normalized =
      typeof value === 'string'
        ? value.replace(/[$,\s]/g, '')
        : value;

    const parsed = Number(normalized);

    return Number.isFinite(parsed) && parsed > 0
      ? parsed
      : undefined;
  }

  private getFirstPositiveNumber(values: unknown[]): number | undefined {
    for (const value of values) {
      const parsed = this.parsePositiveNumber(value);

      if (parsed !== undefined) {
        return parsed;
      }
    }

    return undefined;
  }

  private parseNumberOrZero(value: unknown): number {
    const normalized =
      typeof value === 'string'
        ? value.replace(/[$,\s]/g, '')
        : value;
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private parseSignedNumber(value: unknown): number | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    const normalized =
      typeof value === 'string'
        ? value.replace(/[$,\s]/g, '')
        : value;
    const parsed = Number(normalized);

    return Number.isFinite(parsed) ? parsed : undefined;
  }

  private normalizeOpenPositions(positions: any): DydxV4AccountSnapshot['openPositions'] {
    const entries = Array.isArray(positions)
      ? positions.map((position: any) => [position?.market ?? position?.ticker, position])
      : Object.entries(positions || {});

    return entries
      .map(([market, position]: [string, any]) => ({
        market: this.normalizeMarket(position?.market ?? position?.ticker ?? market),
        side: String(position?.side ?? ''),
        size: this.parseNumberOrZero(position?.size),
        entryPrice: this.parsePositiveNumber(position?.entryPrice ?? position?.entry_price),
        realizedPnl: this.parseSignedNumber(position?.realizedPnl ?? position?.realized_pnl),
        unrealizedPnl: this.parseSignedNumber(position?.unrealizedPnl ?? position?.unrealized_pnl)
      }))
      .filter((position) => position.market && Math.abs(position.size) > 0);
  }

  private toPublicMarketSnapshot(marketInfo: any): DydxV4AccountSnapshot['markets'][string] {
    const computedStep = this.getCorrectionOrderSizeCheck(
      this.normalizeMarket(marketInfo?.ticker ?? marketInfo?.market ?? marketInfo?.id),
      1,
      marketInfo
    ).stepSize;

    return {
      oraclePrice: this.getMarketInfoReferencePrice(marketInfo),
      initialMarginFraction:
        this.parsePositiveNumber(
          marketInfo?.initialMarginFraction ?? marketInfo?.initial_margin_fraction
        ) ?? 0.1,
      maintenanceMarginFraction:
        this.parsePositiveNumber(
          marketInfo?.maintenanceMarginFraction ?? marketInfo?.maintenance_margin_fraction
        ) ?? 0.05,
      stepSize:
        this.parsePositiveNumber(marketInfo?.stepSize ?? marketInfo?.step_size) ??
        computedStep ??
        0.0001,
      status: marketInfo?.status
    };
  }

  private normalizeMarket(value: unknown): string {
    return String(value ?? '')
      .replace(/_/g, '-')
      .toUpperCase();
  }

  private orderMarketMatches(order: any, market: string): boolean {
    return this.normalizeMarket(order.market ?? order.ticker) === market;
  }

  private orderSideMatches(order: any, side: OrderSide): boolean {
    return String(order.side).toUpperCase() === String(side).toUpperCase();
  }

  private orderClientIdMatches(order: any, clientId: number): boolean {
    return this.getOrderClientId(order) === clientId;
  }

  private getOrderClientId(order: any): number {
    const raw =
      order.clientId ??
      order.client_id ??
      order.orderId?.clientId ??
      order.order_id?.client_id;

    const parsed = Number(raw);

    return Number.isFinite(parsed)
      ? parsed
      : NaN;
  }

  private getOrderTypeText(order: any): string {
    return String(order.type ?? order.orderType ?? order.order_type ?? '')
      .toUpperCase();
  }

  private getOrderReduceOnly(order: any): boolean {
    return (
      order.reduceOnly === true ||
      order.reduce_only === true ||
      String(order.reduceOnly).toLowerCase() === 'true' ||
      String(order.reduce_only).toLowerCase() === 'true' ||
      (order.reduceOnly === undefined && order.reduce_only === undefined)
    );
  }

  private getRawOrderFlags(order: any): number | undefined {
    const raw =
      order.orderFlags ??
      order.order_flags ??
      order.orderId?.orderFlags ??
      order.order_id?.order_flags;

    const parsed = Number(raw);

    return Number.isFinite(parsed)
      ? parsed
      : undefined;
  }

  private getOrderFlags(order: any): number {
    const rawFlags = this.getRawOrderFlags(order);

    if (rawFlags !== undefined) {
      return rawFlags;
    }

    const type = this.getOrderTypeText(order);

    if (
      type.includes('STOP') ||
      type.includes('TAKE_PROFIT') ||
      type.includes('TAKE-PROFIT') ||
      this.getOrderTriggerPrice(order) !== undefined
    ) {
      return this.CONDITIONAL_ORDER_FLAGS;
    }

    return 0;
  }

  private getOrderGoodTilBlock(order: any): number | undefined {
    const raw =
      order.goodTilBlock ??
      order.good_til_block ??
      order.orderId?.goodTilBlock ??
      order.order_id?.good_til_block;

    const parsed = Number(raw);

    return Number.isFinite(parsed) && parsed > 0
      ? parsed
      : undefined;
  }

  private getOrderGoodTilBlockTime(order: any): number | undefined {
    const raw =
      order.goodTilBlockTime ??
      order.good_til_block_time ??
      order.goodTilBlockTimeSeconds ??
      order.good_til_block_time_seconds ??
      order.goodTilTimeInSeconds ??
      order.good_til_time_in_seconds ??
      order.orderId?.goodTilBlockTime ??
      order.orderId?.goodTilBlockTimeSeconds ??
      order.order_id?.good_til_block_time ??
      order.order_id?.good_til_block_time_seconds;

    const parsed = Number(raw);

    return Number.isFinite(parsed) && parsed > 0
      ? parsed
      : undefined;
  }

  private compactObject(value: Record<string, unknown>): Record<string, unknown> {
    return Object.fromEntries(
      Object.entries(value).filter(([, childValue]) =>
        childValue !== undefined &&
        childValue !== null &&
        childValue !== ''
      )
    );
  }

  private createClientId(): number {
    return parseInt(
      crypto.randomBytes(4).toString('hex'),
      16
    );
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private async withMarketQueue<T>(
    market: string,
    fn: () => Promise<T>
  ): Promise<T> {
    const previous = this.marketQueues.get(market) ?? Promise.resolve();
    const run = previous.then(fn, fn);
    const cleanup = run.catch(() => undefined);

    this.marketQueues.set(market, cleanup);

    cleanup.then(() => {
      if (this.marketQueues.get(market) === cleanup) {
        this.marketQueues.delete(market);
      }
    });

    return run;
  }

  private async withTxQueue<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.txQueue.then(fn, fn);
    this.txQueue = run.catch(() => undefined);

    return run;
  }

  private async submitDydxTransaction<T>(
    label: string,
    fn: () => Promise<T>,
    context: Record<string, unknown> = {}
  ): Promise<T> {
    return this.withTxQueue(async () => {
      try {
        return await fn();
      } catch (error) {
        if (!this.isRetriableDydxBroadcastError(error)) {
          throw error;
        }

        console.warn(`${label} failed with retriable dYdX broadcast error. Reconnecting dYdX clients and retrying once.`, {
          context,
          error: this.serializeError(error)
        });

        await this.reconnectDydxClients(`${label} broadcast retry`);

        try {
          return await fn();
        } catch (retryError) {
          console.error(`${label} retry failed after reconnect.`, {
            context,
            originalError: this.serializeError(error),
            retryError: this.serializeError(retryError)
          });

          throw retryError;
        }
      }
    });
  }

  private async connectDydxClients(reason: string): Promise<void> {
    const network = this.createNetwork();

    this.client = await CompositeClient.connect(network);
    this.subaccount = new SubaccountClient(this.wallet, 0);
    this.indexer = this.createIndexerClient();

    console.log('dYdX clients connected:', {
      reason,
      address: this.wallet.address,
      nodeEnv: process.env.NODE_ENV ?? ''
    });
  }

  private async reconnectDydxClients(reason: string): Promise<void> {
    console.warn('Reconnecting dYdX clients.', {
      reason,
      address: this.wallet.address
    });

    await this.connectDydxClients(reason);
  }

  private createNetwork(): Network {
    if (process.env.NODE_ENV === 'production') {
      return new Network(
        'mainnet',
        this.getIndexerConfig(),
        this.getValidatorConfig()
      );
    }

    return Network.testnet();
  }

  private createIndexerClient(): IndexerClient {
    return new IndexerClient(
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig
    );
  }

  private getValidatorConfig(): ValidatorConfig {
    return new ValidatorConfig(
      config.get('DydxV4.ValidatorConfig.restEndpoint'),
      'dydx-mainnet-1',
      {
        CHAINTOKEN_DENOM: 'adydx',
        CHAINTOKEN_DECIMALS: 18,
        USDC_DENOM: 'uusdc',
        USDC_GAS_DENOM: 'uusdc',
        USDC_DECIMALS: 6
      }
    );
  }

  private getIndexerConfig(): IndexerConfig {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }

  private isRetriableDydxBroadcastError(error: unknown): boolean {
    const text = this.getErrorSearchText(error).toLowerCase();

    return (
      text.includes('signature verification failed') ||
      text.includes('unable to verify single signer signature') ||
      text.includes('unauthorized') ||
      text.includes('account number') ||
      text.includes('chain-id') ||
      text.includes('goodtilblock') ||
      text.includes('next block height is greater than the goodtilblock')
    );
  }

  private getErrorSearchText(value: unknown, depth = 0): string {
    if (depth > 6 || value === undefined || value === null) {
      return '';
    }

    if (typeof value === 'string') {
      return value;
    }

    if (
      typeof value === 'number' ||
      typeof value === 'boolean' ||
      typeof value === 'bigint'
    ) {
      return String(value);
    }

    if (value instanceof Error) {
      return [
        value.name,
        value.message,
        value.stack,
        ...Object.getOwnPropertyNames(value).map(key =>
          this.getErrorSearchText((value as any)[key], depth + 1)
        )
      ].join(' ');
    }

    if (typeof value === 'object') {
      return Object.entries(value as Record<string, unknown>)
        .map(([key, childValue]) =>
          `${key} ${this.getErrorSearchText(childValue, depth + 1)}`
        )
        .join(' ');
    }

    return '';
  }

  private serializeError(error: unknown): any {
    if (!(error instanceof Error)) {
      return error;
    }

    const serialized: Record<string, unknown> = {
      name: error.name,
      message: error.message,
      stack: error.stack
    };

    for (const key of Object.getOwnPropertyNames(error)) {
      serialized[key] = (error as any)[key];
    }

    const anyError = error as any;

    for (const key of ['cause', 'code', 'status', 'response', 'data', 'details']) {
      if (anyError[key] !== undefined) {
        serialized[key] = anyError[key];
      }
    }

    return serialized;
  }

  private getOrderDiagnosticFields(order: any): Record<string, unknown> {
    const keys = [
      'reason',
      'rejectionReason',
      'rejectReason',
      'failureReason',
      'cancelReason',
      'removalReason',
      'statusReason',
      'detailedStatus',
      'error',
      'message'
    ];

    const diagnostics: Record<string, unknown> = {};

    for (const key of keys) {
      const value = order[key] ?? order.order?.[key];

      if (value !== undefined && value !== null && value !== '') {
        diagnostics[key] = value;
      }
    }

    return diagnostics;
  }
}
