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
  source: 'STATIC' | 'TRAIL';
  updatedAt: number;
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
    const market = alert.market.replace(/_/g, '-');
    const signal = this.getSignal(alert);

    if (signal === 'TRAIL_UPDATE') {
      await this.handleTrailUpdate(market, alert);
      return;
    }

    const targetSize = this.getTargetSize(alert, alert.size);

    console.log('Target position:', targetSize);

    await this.cancelOpenOrders(market);

    if (Math.abs(targetSize) < this.TOLERANCE) {
      this.managedStops.delete(market);
      await this.flattenPositionSafely(market);
      return;
    }

    await this.reachTargetPositionSafely(market, targetSize);
    await this.placeStaticSafetyStopAfterEntry(market, targetSize, alert);
  }

  private async handleTrailUpdate(market: string, alert: AlertObject): Promise<void> {
    const position = await this.getCurrentPosition(market);

    if (Math.abs(position.size) < this.TOLERANCE) {
      console.log('Trail update ignored because position is flat.', { market });
      this.managedStops.delete(market);
      return;
    }

    const isLong = position.size > 0;
    const side = isLong ? OrderSide.SELL : OrderSide.BUY;
    const trailTriggerPrice = this.getTrailStopFromAlert(alert, isLong);

    if (!trailTriggerPrice) {
      console.warn('Trail update ignored because no valid trail stop was supplied.', {
        market,
        isLong
      });
      return;
    }

    const currentKnownStop = await this.getCurrentKnownStopTrigger(market, side, isLong);

    if (currentKnownStop === undefined) {
      console.warn('Trail update ignored because current dYdX stop could not be determined safely.', {
        market,
        direction: isLong ? 'LONG' : 'SHORT',
        trailTriggerPrice
      });
      return;
    }

    const improvesLong = isLong && trailTriggerPrice > currentKnownStop;
    const improvesShort = !isLong && trailTriggerPrice < currentKnownStop;

    if (!improvesLong && !improvesShort) {
      console.log('Trail update ignored because it does not improve current stop.', {
        market,
        direction: isLong ? 'LONG' : 'SHORT',
        currentKnownStop,
        trailTriggerPrice
      });
      return;
    }

    const size = Math.abs(position.size);
    const executionPrice = this.getStopExecutionPrice(isLong, trailTriggerPrice);
    const oldStopOrders = await this.getProtectiveStopOrders(market, side);

    console.log('Applying trail update:', {
      market,
      direction: isLong ? 'LONG' : 'SHORT',
      positionSize: position.size,
      currentKnownStop,
      trailTriggerPrice,
      executionPrice,
      side,
      size,
      oldStopCount: oldStopOrders.length
    });

    const clientId = await this.placeSafetyStopOrder(
      market,
      side,
      size,
      trailTriggerPrice,
      executionPrice
    );

    this.managedStops.set(market, {
      market,
      side,
      triggerPrice: trailTriggerPrice,
      clientId,
      size,
      source: 'TRAIL',
      updatedAt: Date.now()
    });

    await this.waitForSafetyStopVisibleBestEffort(market, side, trailTriggerPrice);
    await this.cancelSpecificOrders(market, oldStopOrders);
  }

  private async flattenPositionSafely(market: string): Promise<void> {
    let currentSize = await this.getCurrentSize(market);

    console.log('Flatten requested | Current size:', currentSize);

    if (Math.abs(currentSize) < this.TOLERANCE) {
      console.log('Already flat.');
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

    const entryReferencePrice = this.getSafetyStopReferencePrice(alert, position.entryPrice);
    const isLong = position.size > 0;
    const triggerPrice = this.getInitialSafetyStopTriggerPrice(alert, isLong, entryReferencePrice);
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
      source: this.getStaticStopFromAlert(alert, isLong) ? 'TRADINGVIEW_STATIC_SL' : 'ENV_FALLBACK'
    });

    const clientId = await this.placeSafetyStopOrder(
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
      clientId,
      size,
      source: 'STATIC',
      updatedAt: Date.now()
    });

    await this.waitForSafetyStopVisibleBestEffort(market, side, triggerPrice);
  }

  private async placeSafetyStopOrder(
    market: string,
    side: OrderSide,
    size: number,
    triggerPrice: number,
    executionPrice: number
  ): Promise<number> {
    const clientId = this.createClientId();

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

    return clientId;
  }

  private async waitForSafetyStopVisibleBestEffort(
    market: string,
    side: OrderSide,
    triggerPrice: number
  ): Promise<void> {
    for (let i = 1; i <= this.SAFETY_STOP_VERIFY_POLLS; i++) {
      await this.sleep(this.SAFETY_STOP_VERIFY_DELAY_MS);

      const orders = await this.getOpenOrdersForMarket(market);
      const stopOrder = this.findSafetyStopOrderInOrders(orders, side, triggerPrice);

      console.log(`Safety stop verify poll ${i}/${this.SAFETY_STOP_VERIFY_POLLS}`, {
        market,
        expectedSide: side,
        expectedTriggerPrice: triggerPrice,
        openOrders: orders.map((order: any) => ({
          clientId: order.clientId,
          market: order.market,
          side: order.side,
          type: order.type ?? order.orderType,
          status: order.status,
          reduceOnly: order.reduceOnly,
          triggerPrice:
            order.triggerPrice ??
            order.trigger_price ??
            order.conditionalOrderTriggerPrice ??
            order.conditional_order_trigger_price,
          orderFlags:
            order.orderFlags ??
            order.order_flags ??
            order.orderId?.orderFlags ??
            order.order_id?.order_flags
        }))
      });

      if (stopOrder) {
        console.log('Safety stop visible on dYdX:', {
          market,
          side,
          triggerPrice,
          status: stopOrder.status,
          clientId: stopOrder.clientId
        });
        return;
      }
    }

    console.warn(
      `Safety stop was placed but could not be verified through indexer for ${market} at trigger ${triggerPrice}. Check dYdX UI.`
    );
  }

  private async getOpenOrdersForMarket(market: string): Promise<any[]> {
    const res = await this.indexer.account.getSubaccountOrders(
      this.wallet.address,
      0
    );

    const visibleStatuses = new Set([
      'OPEN',
      'UNTRIGGERED',
      'OPEN_UNTRIGGERED',
      'PENDING',
      'BEST_EFFORT_OPENED'
    ]);

    return res.orders?.filter((order: any) =>
      order.market === market &&
      visibleStatuses.has(String(order.status).toUpperCase())
    ) || [];
  }

  private async getProtectiveStopOrders(market: string, side?: OrderSide): Promise<any[]> {
    const orders = await this.getOpenOrdersForMarket(market);

    return orders.filter((order: any) => {
      const orderSideMatches =
        side === undefined ||
        String(order.side).toUpperCase() === String(side).toUpperCase();

      const orderType = String(order.type || order.orderType || '').toUpperCase();

      const reduceOnly =
        order.reduceOnly === true ||
        String(order.reduceOnly).toLowerCase() === 'true' ||
        order.reduceOnly === undefined;

      return orderSideMatches && reduceOnly && orderType.includes('STOP');
    });
  }

  private findSafetyStopOrderInOrders(
    orders: any[],
    side: OrderSide,
    triggerPrice: number
  ): any | undefined {
    return orders.find((order: any) => {
      const orderSide = String(order.side).toUpperCase() === String(side).toUpperCase();
      const orderType = String(order.type || order.orderType || '').toUpperCase();

      const reduceOnly =
        order.reduceOnly === true ||
        String(order.reduceOnly).toLowerCase() === 'true' ||
        order.reduceOnly === undefined;

      const parsedTrigger = this.getOrderTriggerPrice(order);

      const triggerMatches =
        parsedTrigger === undefined ||
        Math.abs(parsedTrigger - triggerPrice) / triggerPrice < 0.002;

      return (
        orderSide &&
        reduceOnly &&
        orderType.includes('STOP') &&
        triggerMatches
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
      const clientId =
        typeof order.clientId === 'number'
          ? order.clientId
          : parseInt(order.clientId, 10);

      if (!Number.isFinite(clientId)) {
        console.warn('Skipping cancel because clientId is invalid:', order);
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
        goodTilBlockTime
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

  private async getCurrentKnownStopTrigger(
    market: string,
    side: OrderSide,
    isLong: boolean
  ): Promise<number | undefined> {
    const managedStop = this.managedStops.get(market);

    if (managedStop && String(managedStop.side).toUpperCase() === String(side).toUpperCase()) {
      return managedStop.triggerPrice;
    }

    const protectiveStops = await this.getProtectiveStopOrders(market, side);
    const triggerPrices = protectiveStops
      .map((order: any) => this.getOrderTriggerPrice(order))
      .filter((value: number | undefined): value is number => value !== undefined);

    if (triggerPrices.length === 0) {
      return undefined;
    }

    return isLong
      ? Math.max(...triggerPrices)
      : Math.min(...triggerPrices);
  }

  private getInitialSafetyStopTriggerPrice(
    alert: AlertObject,
    isLong: boolean,
    entryReferencePrice: number
  ): number {
    const staticStopFromAlert = this.getStaticStopFromAlert(alert, isLong);

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

  private getStaticStopFromAlert(alert: AlertObject, isLong: boolean): number | undefined {
    const data = alert as any;

    const candidates = isLong
      ? [
          data.static_sl_long,
          data.staticSLLong,
          data.staticLongSL,
          data.static_long_sl,
          data.long_static_sl,
          data.longStaticSL,
          data.static_sl,
          data.staticSL,
          data.stop_loss,
          data.stopLoss,
          data.stopPrice
        ]
      : [
          data.static_sl_short,
          data.staticSLShort,
          data.staticShortSL,
          data.static_short_sl,
          data.short_static_sl,
          data.shortStaticSL,
          data.static_sl,
          data.staticSL,
          data.stop_loss,
          data.stopLoss,
          data.stopPrice
        ];

    for (const candidate of candidates) {
      const parsed = this.parsePositiveNumber(candidate);

      if (parsed) {
        return parsed;
      }
    }

    return undefined;
  }

  private getTrailStopFromAlert(alert: AlertObject, isLong: boolean): number | undefined {
    const data = alert as any;

    const candidates = isLong
      ? [
          data.trailing_sl_long,
          data.trailingSLLong,
          data.trail_sl_long,
          data.trailSLLong,
          data.long_trailing_sl,
          data.longTrailingSL,
          data.long_trail_stop,
          data.longTrailStop,
          data.trail_stop_long,
          data.trailStopLong,
          data.trailing_sl,
          data.trailingSL,
          data.trail_stop,
          data.trailStop,
          data.trail,
          data.price
        ]
      : [
          data.trailing_sl_short,
          data.trailingSLShort,
          data.trail_sl_short,
          data.trailSLShort,
          data.short_trailing_sl,
          data.shortTrailingSL,
          data.short_trail_stop,
          data.shortTrailStop,
          data.trail_stop_short,
          data.trailStopShort,
          data.trailing_sl,
          data.trailingSL,
          data.trail_stop,
          data.trailStop,
          data.trail,
          data.price
        ];

    for (const candidate of candidates) {
      const parsed = this.parsePositiveNumber(candidate);

      if (parsed) {
        return parsed;
      }
    }

    return undefined;
  }

  private getOrderTriggerPrice(order: any): number | undefined {
    return this.parsePositiveNumber(
      order.triggerPrice ??
      order.trigger_price ??
      order.conditionalOrderTriggerPrice ??
      order.conditional_order_trigger_price
    );
  }

  private async getCurrentSize(market: string): Promise<number> {
    const position = await this.getCurrentPosition(market);
    return position.size;
  }

  private async getCurrentPosition(
    market: string
  ): Promise<{ size: number; entryPrice?: number }> {
    const response = await this.indexer.account.getSubaccountPerpetualPositions(
      this.wallet.address,
      0
    );

    const pos = response.positions.find((p: any) => p.market === market);

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
    const dir = alert.desired_position?.toUpperCase();

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

    const alertPrice = this.parsePositiveNumber((alert as any).entry_price ?? (alert as any).price);

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

    const parsed = Number(value);

    return Number.isFinite(parsed) && parsed > 0
      ? parsed
      : undefined;
  }

  private getOrderFlags(order: any): number {
    const raw =
      order.orderFlags ??
      order.order_flags ??
      order.orderId?.orderFlags ??
      order.order_id?.order_flags;

    const parsed = Number(raw);

    if (Number.isFinite(parsed)) {
      return parsed;
    }

    const type = String(order.type || order.orderType || '').toUpperCase();

    if (
      type.includes('STOP') ||
      type.includes('TAKE_PROFIT') ||
      type.includes('TAKE-PROFIT')
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


