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

type PlacedSafetyStop = {
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

type TakeProfitLevel = {
  name: string;
  distancePct: number;
  sizePct: number;
};

export class DydxV4Client extends AbstractDexClient {
  private wallet!: LocalWallet;
  private client!: CompositeClient;
  private subaccount!: SubaccountClient;
  private indexer!: IndexerClient;
  private initialized = false;

  private readonly managedStops = new Map<string, ManagedStop>();

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

  private readonly TAKE_PROFIT_ENABLED =
    String(process.env.DYDX_V4_TP_ENABLED ?? 'false').toLowerCase() === 'true';

  private readonly TAKE_PROFIT_STRATEGY_ALLOWLIST = String(
    process.env.DYDX_V4_TP_STRATEGY_ALLOWLIST ?? ''
  )
    .split(',')
    .map(value => value.trim().toLowerCase())
    .filter(Boolean);

  private readonly TAKE_PROFIT_LIFETIME_SECONDS = Number(
    process.env.DYDX_V4_TP_LIFETIME_SECONDS ?? `${60 * 60 * 24 * 30}`
  );

  private readonly TAKE_PROFIT_LEVELS: TakeProfitLevel[] = [
    {
      name: 'TP1',
      distancePct: Number(process.env.DYDX_V4_TP1_DISTANCE_PCT ?? '0'),
      sizePct: Number(process.env.DYDX_V4_TP1_SIZE_PCT ?? '0')
    },
    {
      name: 'TP2',
      distancePct: Number(process.env.DYDX_V4_TP2_DISTANCE_PCT ?? '0'),
      sizePct: Number(process.env.DYDX_V4_TP2_SIZE_PCT ?? '0')
    },
    {
      name: 'TP3',
      distancePct: Number(process.env.DYDX_V4_TP3_DISTANCE_PCT ?? '0'),
      sizePct: Number(process.env.DYDX_V4_TP3_SIZE_PCT ?? '0')
    }
  ];

  private readonly LOG_RAW_ORDER_SNAPSHOTS =
    String(process.env.DYDX_V4_LOG_RAW_ORDER_SNAPSHOTS ?? 'true').toLowerCase() !== 'false';

  private readonly CONDITIONAL_ORDER_FLAGS = 32;

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

    console.log('dYdX alert intake:', {
      market,
      signal,
      desiredPosition: telemetry.desiredPosition,
      telemetry
    });

    if (this.isManualStopSyncSignal(signal)) {
      await this.handleManualStopSync(market, alert);
      return;
    }

    if (this.isFractalMovementSignal(signal)) {
      await this.handleFractalMovement(market, alert);
      return;
    }

    if (signal === 'TRAIL_UPDATE') {
      console.log('TRAIL_UPDATE ignored because legacy Render trailing is disabled.', {
        market,
        signal,
        telemetry
      });
      return;
    }

    const targetSize = this.getTargetSize(alert, Number((alert as any).size ?? 0));

    console.log('Target position:', targetSize);

    await this.cancelOpenOrders(market);

    if (Math.abs(targetSize) < this.TOLERANCE) {
      this.managedStops.delete(market);
      await this.flattenPositionSafely(market);
      return;
    }

    await this.reachTargetPositionSafely(market, targetSize);
    await this.placeStaticSafetyStopAfterEntry(market, targetSize, alert);
    await this.placeTakeProfitOrdersAfterEntry(market, targetSize, alert);
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
      this.managedStops.delete(market);
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
        telemetry
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
        bufferPercent: bufferPct * 100,
        longTrailPreview,
        shortTrailPreview,
        position,
        message:
          signal === 'BOTTOM_FRACTAL_MOVING_UP'
            ? `FB up triggered. Price level=${bottomFractal}. Long trail preview=${longTrailPreview}. No order placed.`
            : `FT down triggered. Price level=${topFractal}. Short trail preview=${shortTrailPreview}. No order placed.`
      });
      return;
    }

    if (Math.abs(position.size) < this.TOLERANCE) {
      this.managedStops.delete(market);

      console.log(
        signal === 'BOTTOM_FRACTAL_MOVING_UP'
          ? 'FB up ignored: dYdX position is not LONG.'
          : 'FT down ignored: dYdX position is not SHORT.',
        {
          market,
          position,
          trailStop: telemetry.trailStop,
          managedStopCleared: true
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
        telemetry,
        bottomFractal,
        topFractal,
        longTrailPreview,
        shortTrailPreview
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
        matchingStopOrders: stopResolution.matchingOrders.map((order: any) =>
          this.summarizeOrder(order)
        )
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
      oldStops: oldStopOrders.map((order: any) => this.summarizeOrder(order)),
      oldManagedStop,
      bootstrapIfNoStop: this.FRACTAL_TRAIL_BOOTSTRAP_IF_NO_STOP,
      cancelOldStopsAfterTrailSubmit: this.CANCEL_OLD_STOPS_AFTER_TRAIL_SUBMIT,
      telemetry
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

  private async cancelManagedStopBestEffort(
    market: string,
    stop: ManagedStop,
    reason: string
  ): Promise<void> {
    if (stop.source === 'MANUAL') {
      console.log('Skipping managed stop cancel because stop source is MANUAL.', {
        market,
        stop,
        reason
      });
      return;
    }

    const goodTilCandidates: Array<number | undefined> = [];

    if (stop.goodTilBlockTime !== undefined) {
      goodTilCandidates.push(stop.goodTilBlockTime);
    }

    goodTilCandidates.push(undefined);

    for (const goodTilBlockTime of goodTilCandidates) {
      try {
        console.log('Cancelling Render-managed stop best-effort:', {
          market,
          clientId: stop.clientId,
          orderFlags: this.CONDITIONAL_ORDER_FLAGS,
          goodTilBlockTime,
          stop,
          reason
        });

        await this.client.cancelOrder(
          this.subaccount,
          stop.clientId,
          this.CONDITIONAL_ORDER_FLAGS,
          market,
          goodTilBlockTime
        );

        console.log('Render-managed stop cancel submitted:', {
          market,
          clientId: stop.clientId,
          goodTilBlockTime,
          reason
        });

        return;
      } catch (error) {
        console.warn('Render-managed stop cancel attempt failed:', {
          market,
          clientId: stop.clientId,
          goodTilBlockTime,
          reason,
          error
        });
      }
    }

    console.warn('Render-managed stop could not be cancelled best-effort. Check dYdX UI.', {
      market,
      stop,
      reason
    });
  }

  private async flattenPositionSafely(market: string): Promise<void> {
    let currentSize = await this.getCurrentSize(market);

    console.log('Flatten requested | Current size:', currentSize);

    if (Math.abs(currentSize) < this.TOLERANCE) {
      console.log('Already flat.');
      this.managedStops.delete(market);
      return;
    }

    for (let attempt = 1; attempt <= this.FLAT_MAX_ATTEMPTS; attempt++) {
      const startSize = currentSize;
      const side = startSize > 0 ? OrderSide.SELL : OrderSide.BUY;
      const size = Math.abs(startSize);

      console.log(
        `Flatten attempt ${attempt}/${this.FLAT_MAX_ATTEMPTS} | Start: ${startSize} | Sending: ${side} ${size} | reduceOnly=true`
      );

      await this.placeCorrectionOrder(market, side, size, true);
      await this.sleep(this.POST_ORDER_SETTLE_MS);

      const progress = await this.waitForFlattenProgress(market, startSize);

      if (progress.kind === 'flat') {
        this.managedStops.delete(market);
        console.log('Position fully flattened.');
        return;
      }

      if (progress.kind === 'flipped') {
        console.error('Position flipped during flatten. Starting emergency flatten.', {
          previousSize: startSize,
          currentSize: progress.currentSize
        });

        await this.emergencyFlattenOppositePosition(market, progress.currentSize);
        return;
      }

      currentSize = progress.currentSize;
      console.log('Flatten not complete yet; retrying.', { currentSize, kind: progress.kind });
    }

    throw new Error(`Flatten failed for ${market}: max attempts reached without reaching flat.`);
  }

  private async emergencyFlattenOppositePosition(market: string, currentSize: number): Promise<void> {
    if (Math.abs(currentSize) < this.TOLERANCE) {
      this.managedStops.delete(market);
      console.log('Emergency check: already flat.');
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
      this.managedStops.delete(market);
      console.log('Emergency flatten successful.');
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

      console.log(
        `Flatten poll ${i}/${this.FLAT_PROGRESS_POLLS} | Initial: ${initialSize} | Current: ${currentSize}`
      );

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

      console.log(
        `Attempt ${attempt}/${this.MAX_ATTEMPTS} | Current: ${currentSize} | Target: ${targetSize} | Diff: ${diff}`
      );

      if (Math.abs(diff) < this.TOLERANCE) {
        console.log('Target reached within tolerance.');
        return;
      }

      const side = diff > 0 ? OrderSide.BUY : OrderSide.SELL;
      const size = Math.abs(diff);

      await this.placeCorrectionOrder(market, side, size, false);
      await this.sleep(this.POST_ORDER_SETTLE_MS);

      const progress = await this.waitForTargetProgress(market, currentSize, targetSize);

      if (progress.kind === 'target') {
        console.log('Target reached after correction.');
        return;
      }

      if (progress.kind === 'progress') {
        console.log('Position moved toward target, continuing if needed.', {
          currentSize: progress.currentSize
        });
        continue;
      }

      if (progress.kind === 'flipped') {
        throw new Error(
          `Dangerous overshoot detected for ${market}. Current size flipped unexpectedly to ${progress.currentSize}`
        );
      }

      console.warn('No visible progress yet after correction; retrying cautiously.');
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

      console.log(
        `Target poll ${i}/${this.TARGET_PROGRESS_POLLS} | Initial: ${initialSize} | Current: ${currentSize} | Target: ${targetSize}`
      );

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
  ): Promise<void> {
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
      positionSize: position.size,
      entryReferencePrice,
      triggerPrice,
      executionPrice,
      side,
      size,
      reduceOnly: true,
      source: staticStopFromAlert ? 'TRADINGVIEW_STATIC_SL' : 'ENV_FALLBACK',
      telemetry
    });

    const placedStop = await this.placeSafetyStopOrder(
      market,
      side,
      size,
      triggerPrice,
      executionPrice
    );

    this.managedStops.set(market, {
      market,
      side,
      triggerPrice,
      clientId: placedStop.clientId,
      size,
      source: 'STATIC',
      updatedAt: Date.now(),
      goodTilBlockTime: placedStop.goodTilBlockTime
    });

    await this.waitForSafetyStopVisibleBestEffort(
      market,
      side,
      triggerPrice,
      placedStop.clientId
    );
  }

  private async placeTakeProfitOrdersAfterEntry(
    market: string,
    targetSize: number,
    alert: AlertObject
  ): Promise<void> {
    if (!this.shouldPlaceTakeProfitOrders(alert)) {
      return;
    }

    const levels = this.getConfiguredTakeProfitLevels();

    if (levels.length === 0) {
      console.log('Take profit orders skipped: no valid TP levels configured.');
      return;
    }

    const totalSizePct = levels.reduce((sum, level) => sum + level.sizePct, 0);

    if (totalSizePct > 1 + this.TOLERANCE) {
      throw new Error(`Take profit size percentages exceed 100%. Total=${totalSizePct}`);
    }

    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      console.log('Take profit orders skipped: position is flat after entry.', { market });
      return;
    }

    if (Math.sign(position.size) !== Math.sign(targetSize)) {
      throw new Error(
        `Take profit orders not placed for ${market}: position direction mismatch. Current=${position.size}, target=${targetSize}`
      );
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const positionSize = Math.abs(position.size);

    console.log('Placing take profit orders:', {
      market,
      positionSize: position.size,
      entryReferencePrice,
      side,
      levels
    });

    for (const level of levels) {
      const price = isLong
        ? entryReferencePrice * (1 + level.distancePct)
        : entryReferencePrice * (1 - level.distancePct);

      const size = Number((positionSize * level.sizePct).toFixed(6));

      if (size < this.TOLERANCE) {
        console.log('Skipping TP level because size is below tolerance.', {
          market,
          level,
          size
        });
        continue;
      }

      await this.placeTakeProfitOrder(market, side, size, price, level.name);
    }
  }

  private shouldPlaceTakeProfitOrders(alert: AlertObject): boolean {
    if (!this.TAKE_PROFIT_ENABLED) {
      return false;
    }

    if (this.TAKE_PROFIT_STRATEGY_ALLOWLIST.length === 0) {
      return true;
    }

    const strategy = String((alert as any).strategy ?? '')
      .trim()
      .toLowerCase();

    return this.TAKE_PROFIT_STRATEGY_ALLOWLIST.includes(strategy);
  }

  private getConfiguredTakeProfitLevels(): TakeProfitLevel[] {
    return this.TAKE_PROFIT_LEVELS.filter(level =>
      Number.isFinite(level.distancePct) &&
      Number.isFinite(level.sizePct) &&
      level.distancePct > 0 &&
      level.sizePct > 0
    );
  }

  private async placeTakeProfitOrder(
    market: string,
    side: OrderSide,
    size: number,
    price: number,
    levelName: string
  ): Promise<void> {
    const clientId = this.createClientId();

    console.log('Submitting take profit order:', {
      market,
      side,
      size,
      price,
      levelName,
      clientId,
      reduceOnly: true,
      orderType: OrderType.LIMIT
    });

    await this.client.placeOrder(
      this.subaccount,
      market,
      OrderType.LIMIT,
      side,
      price,
      size,
      clientId,
      this.getRestingLimitTimeInForce(),
      this.TAKE_PROFIT_LIFETIME_SECONDS,
      OrderExecution.DEFAULT,
      false,
      true,
      undefined
    );
  }

  private getRestingLimitTimeInForce(): OrderTimeInForce {
    const timeInForce = OrderTimeInForce as any;

    return (
      timeInForce.UNSPECIFIED ??
      timeInForce.TIME_IN_FORCE_UNSPECIFIED ??
      0
    ) as OrderTimeInForce;
  }

  private async placeSafetyStopOrder(
    market: string,
    side: OrderSide,
    size: number,
    triggerPrice: number,
    executionPrice: number
  ): Promise<PlacedSafetyStop> {
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

      console.log(`Safety stop verify poll ${i}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        expectedSide: side,
        expectedTriggerPrice: triggerPrice,
        expectedClientId,
        openOrders: orders.map((order: any) => this.summarizeOrder(order))
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

    return {
      position,
      allMarketOrders,
      openOrders,
      protectiveStops,
      managedStop
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
      telemetry,
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
    await this.cancelSpecificOrders(market, orders);
    this.managedStops.delete(market);
  }

  private async cancelSpecificOrders(market: string, orders: any[]): Promise<void> {
    for (const order of orders) {
      const clientId = this.getOrderClientId(order);

      if (!Number.isFinite(clientId)) {
        console.warn('Skipping cancel because clientId is invalid:', this.summarizeOrder(order));
        continue;
      }

      const orderFlags = this.getOrderFlags(order);
      const goodTilBlockTime = this.getOrderGoodTilBlockTime(order);

      console.log('Cancelling open order:', {
        market,
        clientId,
        orderFlags,
        status: order.status,
        type: order.type,
        goodTilBlockTime,
        order: this.summarizeOrder(order)
      });

      await this.client.cancelOrder(
        this.subaccount,
        clientId,
        orderFlags,
        market,
        goodTilBlockTime
      );
    }
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
        data.staticShortSL,
        data.static_short_sl,
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
      triggerCandidates: this.getOrderTriggerPriceCandidates(order),
      orderFlags: this.getRawOrderFlags(order),
      inferredOrderFlags: this.getOrderFlags(order),
      goodTilBlockTime: this.getOrderGoodTilBlockTime(order),
      goodTilBlock: order.goodTilBlock ?? order.good_til_block ?? order.orderId?.goodTilBlock,
      timeInForce: order.timeInForce ?? order.time_in_force,
      execution: order.execution,
      postOnly: order.postOnly ?? order.post_only,
      createdAt: order.createdAt ?? order.created_at,
      updatedAt: order.updatedAt ?? order.updated_at
    };
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

  private getOrderGoodTilBlockTime(order: any): number | undefined {
    const raw =
      order.goodTilBlockTime ??
      order.good_til_block_time ??
      order.goodTilBlockTimeSeconds ??
      order.good_til_block_time_seconds;

    const parsed = Number(raw);

    return Number.isFinite(parsed) && parsed > 0
      ? parsed
      : undefined;
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
