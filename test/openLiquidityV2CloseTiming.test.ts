import {
  OpenLiquidityV2EthTradeMonitor,
  openLiquidityV2EthTradeMonitor
} from '../src/services/openLiquidityV2EthTradeMonitor';

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

  test('ETH refreshes the latest closed hour without enabling live trading', () => {
    expect(openLiquidityV2EthTradeMonitor.getStatus()).toMatchObject({
      closedHourRefreshEnabled: true,
      autoTradeEnabled: false
    });
  });

  test('checks five seconds after close instead of waiting for its startup-relative poll', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    expect(check).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 4_999);
    expect(check).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(1);
    expect(check).toHaveBeenCalledTimes(2);
  });

  test('retries unavailable closed-hour data after a minute and stops retrying on success', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockResolvedValueOnce({ ok: false });
    await jest.advanceTimersByTimeAsync(23 * 60_000 + 5_000);
    expect(check).toHaveBeenCalledTimes(2);
    await jest.advanceTimersByTimeAsync(60_000);
    expect(check).toHaveBeenCalledTimes(3);
    await jest.advanceTimersByTimeAsync(5 * 60_000);
    expect(check).toHaveBeenCalledTimes(3);
  });

  test('bounds close-window retries during an outage', async () => {
    monitor.start(0);
    await jest.advanceTimersByTimeAsync(0);
    check.mockResolvedValue({ ok: false });
    await jest.advanceTimersByTimeAsync(30 * 60_000);
    expect(check).toHaveBeenCalledTimes(6);
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
    await jest.advanceTimersByTimeAsync(60_000);
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
