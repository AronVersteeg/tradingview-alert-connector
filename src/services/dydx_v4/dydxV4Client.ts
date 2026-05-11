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
  source: 'SAFETY' | 'EXTRA_TP';
  levelName: string;
  updatedAt: number;
  goodTilBlockTime?: number;
};

type ExtraTakeProfitLevel = {
  name: string;
  distancePct: number;
  remainingSizePct: number;
};

type PlacedConditionalOrder = {
  clientId: number;
  goodTilBlockTime?: number;
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

type ExecutionProfileName = 'MANAGED' | 'SIGNAL_ONLY' | 'NO_TP';

type ExecutionProfile = {
  name: ExecutionProfileName;
  placeStaticStop: boolean;
  placeSafetyTp: boolean;
  placeExtraTps: boolean;
  allowFractalTrail: boolean;
  allowManualStopSync: boolean;
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

export class DydxV4Client extends AbstractDexClient {
  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  private readonly managedStops = new Map<string, ManagedStop>();
  private readonly managedTakeProfits = new Map<string, ManagedTakeProfit[]>();

  private readonly TOLERANCE = 0.001;

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

    const validatorConfig = new ValidatorConfig(
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

    const network =
      process.env.NODE_ENV === 'production'
        ? new Network('mainnet', this.getIndexerConfig(), validatorConfig)
        : Network.testnet();

    this.client = await CompositeClient.connect(network);
    this.subaccount = new SubaccountClient(this.wallet, 0);

    this.indexer = new IndexerClient(
      process.env.NODE_ENV === 'production'
        ? this.getIndexerConfig()
        : Network.testnet().indexerConfig
    );

    this.initialized = true;
  }

  async getIsAccountReady(): Promise<boolean> {
    return this.initialized;
  }

  async placeOrder(alert: AlertObject): Promise<void> {
    const market = this.normalizeMarket((alert as any).market);
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

    await this.reachTargetPositionSafely(market, targetSize);

    if (!profile.placeStaticStop) {
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
      size,
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
        error
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
        error
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

  private async reachTargetPositionSafely(market: string, targetSize: number): Promise<void> {
    for (let attempt = 1; attempt <= this.MAX_ATTEMPTS; attempt++) {
      await this.sleep(this.TARGET_POLL_DELAY_MS);

      const currentSize = await this.getCurrentSize(market);
      const diffRaw = targetSize - currentSize;
      const diff = Number(diffRaw.toFixed(3));

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

      await this.placeCorrectionOrder(market, side, size, false);
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

      console.warn('No visible progress yet after correction; retrying cautiously.', { market });
    }

    throw new Error(`Target correction failed for ${market}: max attempts reached.`);
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
    reduceOnly: boolean
  ): Promise<void> {
    const price = side === OrderSide.BUY ? 999999 : 1;
    const clientId = this.createClientId();

    console.log('Correction order:', {
      market,
      side,
      size,
      clientId,
      reduceOnly
    });

    await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.MARKET,
      side,
      price,
      size,
      clientId,
      OrderTimeInForce.IOC,
      20,
      OrderExecution.DEFAULT,
      false,
      reduceOnly,
      undefined
    );
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
      size,
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

    await this.client.placeOrder(
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
    );

    return {
      clientId,
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
    const clientId = this.createClientId();
    const goodTilBlockTime = Math.floor(Date.now() / 1000) + this.SAFETY_STOP_LIFETIME_SECONDS;

    console.log('Submitting safety stop order:', {
      market,
      side,
      size,
      triggerPrice,
      executionPrice,
      clientId,
      reduceOnly: true,
      orderType: OrderType.STOP_MARKET,
      goodTilBlockTime
    });

    await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.STOP_MARKET,
      side,
      executionPrice,
      size,
      clientId,
      OrderTimeInForce.IOC,
      this.SAFETY_STOP_LIFETIME_SECONDS,
      OrderExecution.IOC,
      false,
      true,
      triggerPrice
    );

    return {
      clientId,
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
          error
        });
      }
    }
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

    await (this.client as any).cancelOrder(
      this.subaccount,
      clientId,
      orderFlags,
      market,
      usesGoodTilTime ? undefined : goodTilBlock,
      usesGoodTilTime ? goodTilBlockTime : undefined
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
      updatedAt: order.updatedAt ?? order.updated_at
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

  private getIndexerConfig(): IndexerConfig {
    return new IndexerConfig(
      config.get('DydxV4.IndexerConfig.httpsEndpoint'),
      config.get('DydxV4.IndexerConfig.wssEndpoint')
    );
  }
}
