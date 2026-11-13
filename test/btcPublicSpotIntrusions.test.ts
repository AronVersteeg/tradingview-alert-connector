import fs from 'fs';
import path from 'path';
import vm from 'vm';
const engine = require('../public/btc-spot-chart/intrusion-daily-breakouts.js');

describe('BTC Public V2 always uses native Binance Spot intrusion history', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/decentrader_liquidity_timelapse.html'), 'utf8');
  const start = html.indexOf('    function publishSpotIntrusions() {');
  const end = html.indexOf("    window.addEventListener('btc-spot-chart:intrusion-source-ready'", start);
  const publisher = html.slice(start, end);
  test.each(['manual', 'purple', 'off'])('mode %s keeps the same Spot source', mode => {
    const events: any[] = [];
    const context: any = { activeSource: 'open-v2', openV2Market: 'BTC-USD', Date: { now: () => Date.parse('2026-11-04T10:00:00Z') },
      payload: { status: { entryMode: { mode } },
        source: { market: 'BTC-USD', sourceStatuses: [{ name: 'Binance Spot BTCUSDT 1H', ok: true }] },
        intrusionBenchmarks: { history: [{ timestamp: '2026-10-19 02:00:00', direction: 'long' }] } },
      window: { BtcSpotIntrusionDailyBreakouts: engine, BTC_SPOT_INTRUSION_HISTORY: { source: 'decentrader-recorded-intrusions' },
        dispatchEvent: (event: any) => events.push(event) },
      CustomEvent: class { constructor(public type: string, public options: any) {} } };
    vm.runInNewContext(`${publisher}\npublishSpotIntrusions();`, context);
    expect(events).toHaveLength(1); expect(events[0].type).toBe('btc-spot-chart:intrusion-history');
    expect(context.window.BTC_SPOT_INTRUSION_HISTORY).toMatchObject({ market: 'BTC-USD', source: 'binance-spot-recorded-intrusions',
      gapHours: [Date.parse('2026-10-19T02:00:00Z') / 1000] });
  });
  test('does not publish changes into the Decentrader map tab', () => {
    const events: any[] = [];
    vm.runInNewContext(`${publisher}\npublishSpotIntrusions();`, { activeSource: 'decentrader', payload: {},
      window: { BtcSpotIntrusionDailyBreakouts: engine, dispatchEvent: (event: any) => events.push(event) } });
    expect(events).toEqual([]);
  });
  test('the public Spot chart cannot silently read Decentrader or accept its saved history', () => {
    const chart = fs.readFileSync(path.join(__dirname, '../public/btc-spot-chart/chart.js'), 'utf8');
    expect(chart).not.toContain("fetch('/decentrader/liquidity-timelapse'");
    const start = chart.indexOf("  window.addEventListener('btc-spot-chart:intrusion-history'");
    const end = chart.indexOf("  window.addEventListener('btc-spot-chart:intrusion-error'", start);
    let listener: (event: any) => void = () => { throw new Error('Missing history listener'); };
    const update = jest.fn(), updateMarkers = jest.fn();
    const context: any = { market: 'BTC-USD', intrusionHistory: undefined, intrusionReadFailed: false,
      updateIntrusionDailyBreakouts: update, updateGapIntrusionMarkers: updateMarkers,
      window: { addEventListener: (_name: string, fn: any) => { listener = fn; } } };
    vm.runInNewContext(chart.slice(start, end), context);
    listener({ detail: { market: 'BTC-USD', source: 'decentrader-recorded-intrusions' } });
    listener({ detail: { market: 'ETH-USD', source: 'binance-spot-recorded-intrusions' } });
    expect(update).not.toHaveBeenCalled();
    expect(updateMarkers).not.toHaveBeenCalled();
    const history = { market: 'BTC-USD', source: 'binance-spot-recorded-intrusions', records: [] };
    listener({ detail: history }); expect(context.intrusionHistory).toBe(history); expect(update).toHaveBeenCalledTimes(1);
    expect(updateMarkers).toHaveBeenCalledTimes(1);
  });
});
