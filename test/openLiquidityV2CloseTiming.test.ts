import {
  OpenLiquidityV2EthTradeMonitor,
  openLiquidityV2BtcIntrusionMonitor,
  openLiquidityV2EthTradeMonitor,
  openLiquidityV2InjTradeMonitor,
  openLiquidityV2SolTradeMonitor,
  openLiquidityV2ZecTradeMonitor,
  openLiquidityV2GoldIntrusionMonitor,
  openLiquidityV2SilverIntrusionMonitor
} from '../src/services/openLiquidityV2EthTradeMonitor';

const pairs: Array<[string, OpenLiquidityV2EthTradeMonitor]> = [
  ['BTC', openLiquidityV2BtcIntrusionMonitor],
  ['ETH', openLiquidityV2EthTradeMonitor],
  ['INJ', openLiquidityV2InjTradeMonitor],
  ['SOL', openLiquidityV2SolTradeMonitor],
  ['ZEC', openLiquidityV2ZecTradeMonitor],
  ['GOLD', openLiquidityV2GoldIntrusionMonitor],
  ['SILVER', openLiquidityV2SilverIntrusionMonitor]
];

describe('Public V2 close-aligned intrusion checks', () => {
  const originalEnv = { ...process.env };
  let monitor: OpenLiquidityV2EthTradeMonitor;
  let check: jest.SpyInstance;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-08-27T09:37:00.000Z'));
    process.env.DECENTRADER_GAP_POLL_MINUTES = '60';
    process.env.OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED = 'true';
    process.env.OPEN_LIQUIDITY_V2_ETH_AUTO_TRADE_ENABLED = 'false';
    monitor = new OpenLiquidityV2EthTradeMonitor({} as any);
    check = jest.spyOn(monitor, 'check').mockResolvedValue({ ok: true });
    jest.spyOn(monitor, 'checkManagedPosition').mockResolvedValue({ ok: true });
  });

  afterEach(() => {
    monitor.stop();
    jest.restoreAllMocks();
    jest.useRealTimers();
    process.env = { ...originalEnv };
  });

  test.each(pairs)('%s refreshes the latest closed hour without enabling live trading', (_asset, configuredMonitor) => {
    const config = (configuredMonitor as any).config;
    process.env[config.autoTradeEnv] = 'false';
    expect(configuredMonitor.getStatus()).toMatchObject({
      closedHourRefreshEnabled: true,
      autoTradeEnabled: false
    });
  });

  test.each(pairs)('%s checks five seconds after close instead of waiting for its startup-relative poll', async (_asset, configuredMonitor) => {
    const config = (configuredMonitor as any).config;
    process.env[config.enabledEnv] = 'true';
    process.env[config.pollMinutesEnv || 'DECENTRADER_GAP_POLL_MINUTES'] = '60';
    monitor = new OpenLiquidityV2EthTradeMonitor({} as any, config);
    check = jest.spyOn(monitor, 'check').mockResolvedValue({ ok: true });
    jest.spyOn(monitor, 'checkManagedPosition').mockResolvedValue({ ok: true });
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    expect(check).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 4_999);
    expect(check).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(1);
    expect(check).toHaveBeenCalledTimes(2);
  });

  test('retries unavailable closed-hour data after five seconds and stops retrying on success', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockResolvedValueOnce({ ok: false });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 5_000);
    expect(check).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(5_000);
    expect(check).toHaveBeenCalledTimes(3);
    await jest.advanceTimersByTimeAsync(5 * 60_000);
    expect(check).toHaveBeenCalledTimes(3);
  });

  test('bounds close-window retries during an outage', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockResolvedValue({ ok: false });
    await jest.advanceTimersByTimeAsync(30 * 60_000);
    // Startup, six five-second close checks, then four minute retries.
    expect(check).toHaveBeenCalledTimes(11);
    await jest.advanceTimersByTimeAsync(10 * 60_000);
    expect(check).toHaveBeenCalledTimes(11);
  });

  test('starting just after the hour still checks at the upcoming five-second boundary', async () => {
    jest.setSystemTime(new Date('2026-08-27T10:00:02.000Z'));
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    expect(check).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(3_000);
    expect(check).toHaveBeenCalledTimes(2);
  });

  test('retries a pending filter even when the map check succeeded', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockResolvedValueOnce({ ok: true, pendingAlertCount: 1 });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 5_000);
    expect(check).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(5_000);
    expect(check).toHaveBeenCalledTimes(3);
  });

  test('does not schedule intrusion checks when the monitor is disabled', async () => {
    process.env.OPEN_LIQUIDITY_V2_ETH_INTRUSION_MONITOR_ENABLED = 'false';
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(30 * 60_000);
    expect(check).not.toHaveBeenCalled();
  });

  test('stop cancels the close timer and an in-flight check cannot restart it', async () => {
    let resolve: (value: any) => void = () => undefined;
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockImplementationOnce(() => new Promise((done) => { resolve = done; }));
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 5_000);
    monitor.stop();
    resolve({ ok: false });
    await jest.advanceTimersByTimeAsync(65 * 60_000);
    expect(check).toHaveBeenCalledTimes(2);
  });

  test('records candle-close latency separately from the normal hour of candle formation', () => {
    const state: any = { pendingAlerts: { test: { firstObservedAt: '2026-08-27T10:00:20.000Z' } } };
    const alert: any = { timestamp: '2026-08-27 09:00:00', left: [], right: [], entrants: [] };
    (monitor as any).addDelayRecord(state, alert, 'test', 'filtered', '2026-08-27T10:00:35.000Z');
    expect(state.delayRecords[0]).toMatchObject({
      candleClosedAt: '2026-08-27T10:00:00.000Z',
      firstObservedAt: '2026-08-27T10:00:20.000Z',
      completedCandles1h: 1
    });
    expect(state.delayRecords[0].delayMinutes).toBeCloseTo(60 + 35 / 60);
    expect(state.delayRecords[0].afterCloseDelayMinutes).toBeCloseTo(35 / 60);
    expect(state.delayRecords[0].detectionDelayMinutes).toBeCloseTo(20 / 60);
    expect(state.delayRecords[0].processingDelayMinutes).toBeCloseTo(15 / 60);
  });
});
