import net from 'net';
import tls from 'tls';
import { emailNotificationAllowed, informationalEmailsEnabled } from '../src/services/emailNotificationPolicy';
import * as gap from '../src/services/decentraderGapMonitor';
import * as v2 from '../src/services/openLiquidityV2EthTradeMonitor';

const smtp: any = { host: 'fixture.invalid', port: 587, sender: 'fixture@example.invalid',
  recipients: ['fixture@example.invalid'], useTls: false, useSsl: false, timeoutMs: 10, jobName: 'fixture' };
const templates: Record<string, any> = { BTC: v2.openLiquidityV2BtcIntrusionMonitor,
  ETH: v2.openLiquidityV2EthTradeMonitor, SOL: v2.openLiquidityV2SolTradeMonitor, ZEC: v2.openLiquidityV2ZecTradeMonitor };

describe('entry-only email policy', () => {
  afterEach(() => jest.restoreAllMocks());

  test('never allows informational mail', () => {
    expect(informationalEmailsEnabled()).toBe(false);
    expect(emailNotificationAllowed()).toBe(false);
  });

  test.each(['DETECTED', 'SKIPPED', 'ERROR', 'UNCONFIRMED', 'FLATTENED', 'NOT_SUBMITTED', 'NO_POSITION_OBSERVED'])(
    'rejects unconfirmed/non-entry outcome %s', outcome => {
      expect(emailNotificationAllowed({ kind: 'entry', market: 'BTC-USD', signature: 'fixture', outcome })).toBe(false);
    });

  test.each([{}, { kind: 'informational' }, { kind: 'entry', outcome: 'PLACED' },
    { kind: 'entry', outcome: 'PLACED', market: 42, signature: 'fixture' },
    { kind: 'entry', outcome: 'PLACED', market: 'BTC-USD', signature: ' ' }])('fails closed for incomplete metadata %j', context => {
    expect(emailNotificationAllowed(context as any)).toBe(false);
  });

  test('does not open any SMTP socket for informational mail, regardless of its subject', async () => {
    const connect = jest.spyOn(net, 'connect').mockImplementation(() => { throw new Error('Unexpected SMTP'); });
    const secure = jest.spyOn(tls, 'connect').mockImplementation(() => { throw new Error('Unexpected SMTP'); });
    for (const subject of ['BTC intrusion', 'FILTERED ETH', 'BTC Shadow Long', 'TP hit', 'Health warning', 'BTC GREEN LONG TRIGGERED']) {
      await expect(gap.sendEmailBestEffort(smtp, subject, 'fixture')).resolves.toEqual({ sent: false, suppressed: true });
    }
    expect(connect).not.toHaveBeenCalled(); expect(secure).not.toHaveBeenCalled();
  });

  test.each(['BTC', 'ETH', 'SOL', 'ZEC'])('%s confirmed entries reach the sender, without using a real SMTP server', async asset => {
    const connect = jest.spyOn(net, 'connect').mockImplementation(() => { throw new Error('Offline SMTP fixture'); });
    jest.spyOn(console, 'error').mockImplementation(() => {});
    await expect(gap.sendEmailBestEffort(smtp, `${asset} entry`, 'fixture', {
      kind: 'entry', market: `${asset}-USD`, signature: `${asset}|fixture`, outcome: 'PLACED'
    })).resolves.toEqual({ sent: false, error: 'Offline SMTP fixture' });
    expect(connect).toHaveBeenCalledTimes(1);
  });

  test('retains the legacy BTC manual entry mail only after an actual entry', async () => {
    const monitor = new gap.DecentraderGapMonitor();
    const request: any = { market: 'BTC-USD', direction: 'short', signature: 'BTC|legacy-manual-fixture',
      signalCandleStartedAt: '2026-11-19T10:00:00.000Z', signalClose: 120, closeTrigger: 121 };
    jest.spyOn(gap, 'smtpSettingsFromEnv').mockReturnValue(smtp);
    const connect = jest.spyOn(net, 'connect').mockImplementation(() => { throw new Error('Offline SMTP fixture'); });
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    await expect(monitor.sendManualBtcTriggerEmail(request, { tradePlaced: false })).resolves.toEqual({ sent: false });
    expect(connect).not.toHaveBeenCalled();
    // Same-module SMTP settings are lexical; pass configuration through a scoped env instead.
    const previous = { ...process.env };
    try {
      Object.assign(process.env, { SMTP_HOST: smtp.host, SMTP_PORT: '587', SMTP_FROM: smtp.sender,
        SMTP_TO: smtp.recipients[0], SMTP_USE_SSL: 'false', SMTP_USE_TLS: 'false' });
      await expect(monitor.sendManualBtcTriggerEmail(request, { tradePlaced: true }))
        .resolves.toEqual({ sent: false, error: 'Offline SMTP fixture' });
      expect(connect).toHaveBeenCalledTimes(1);
    } finally {
      for (const key of Object.keys(process.env)) if (!(key in previous)) delete process.env[key];
      Object.assign(process.env, previous);
    }
  });

  describe.each(['BTC', 'ETH', 'SOL', 'ZEC'])('%s manual notification routing', asset => {
    const market = `${asset}-USD`;
    const request: any = { market, direction: 'long', signature: `${asset}|manual-fixture`,
      signalCandleStartedAt: '2026-11-19T10:00:00.000Z', signalClose: 120, closeTrigger: 119 };

    test.each(['long', 'short'])('preserves a confirmed %s entry mail with explicit entry metadata', async direction => {
      const monitor = new v2.OpenLiquidityV2EthTradeMonitor({} as any, templates[asset].config);
      jest.spyOn(gap, 'smtpSettingsFromEnv').mockReturnValue(smtp);
      const send = jest.spyOn(gap, 'sendEmailBestEffort').mockResolvedValue({ sent: true });
      jest.spyOn(console, 'log').mockImplementation(() => {});
      await expect(monitor.sendManualTriggerEmail({ ...request, direction }, { tradePlaced: true,
        tradeDecision: { entryPrice: 120, size: 1, stop: 110, takeProfits: [] } })).resolves.toEqual({ sent: true });
      expect(send).toHaveBeenCalledWith(smtp, expect.stringContaining(`${asset} MANUAL ${direction.toUpperCase()} TRIGGERED`),
        expect.stringContaining('Order result: PLACED'), { kind: 'entry', market, signature: request.signature, outcome: 'PLACED' });
    });

    test.each([{ tradeSkipped: 'Existing position' }, { tradeError: 'Order rejected' }, { tradePlaced: false }, {}])(
      'does not send for unsuccessful manual result %j', async result => {
        const monitor = new v2.OpenLiquidityV2EthTradeMonitor({} as any, templates[asset].config);
        const send = jest.spyOn(gap, 'sendEmailBestEffort').mockResolvedValue({ sent: true });
        await expect(monitor.sendManualTriggerEmail(request, result)).resolves.toEqual({ sent: false });
        expect(send).not.toHaveBeenCalled();
      });
  });
});
