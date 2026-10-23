import fs from 'fs';
import path from 'path';

const html = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'decentrader_liquidity_timelapse.html'),
  'utf8'
);

function inlineFunction(name: string, nextName: string, bindings: Record<string, unknown>): any {
  const start = html.indexOf(`    function ${name}(`);
  const end = html.indexOf(`    function ${nextName}(`, start);
  if (start < 0 || end < 0) throw new Error(`Cannot find ${name} in the timelapse viewer.`);
  const names = Object.keys(bindings);
  return new Function(...names, `${html.slice(start, end)} return ${name};`)(...Object.values(bindings));
}

describe('Decentrader historical gap alerts', () => {
  test('quarantines past current-active gaps while keeping the latest frame and Public V2 available', () => {
    const payload = { frames: [{ i: 0 }, { i: 1 }] };
    const historical = inlineFunction('unverifiedDecentraderFrame', 'updateGapMonitor', { isOpenV2: false, payload });
    expect(historical({ i: 0 })).toBe(true);
    expect(historical({ i: 1 })).toBe(false);
    const causal = inlineFunction('unverifiedDecentraderFrame', 'updateGapMonitor', { isOpenV2: true, payload });
    expect(causal({ i: 0 })).toBe(false);
    const cell = () => ({ textContent: '', classList: { remove: jest.fn() } });
    const gapWidth = cell(), gapLeft = cell(), gapRight = cell(), gapPrice = cell(), gapStatus = cell(), gapAlert = cell();
    const update = inlineFunction('updateGapMonitor', 'updateTradeZoneMonitor', {
      unverifiedDecentraderFrame: historical, recordedIntrusionLabel: () => '',
      gapWidth, gapLeft, gapRight, gapPrice, gapStatus, gapAlert, money: (value: number) => String(value)
    });
    update({ i: 0, price: 64000 }, { left: 61000, right: 87400 });
    expect(gapRight.textContent).toBe('-');
    expect(gapStatus.textContent).toContain('Historical gap unverified');
    expect(gapAlert.textContent).toContain('Unknown');
    expect(gapAlert.textContent).not.toBe('No intrusions');
  });

  test('keeps a price gap even when its edges overlap visually on a narrow chart', () => {
    const getGap = inlineFunction('getVisualGapForBars', 'liquiditySourcePriceStep', {
      isOpenV2: false,
      leverageOffset: () => 0,
      BAR_WIDTH: 4,
      GAP_EDGE_PADDING: 3
    });
    const gap = getGap(
      { price: 100 },
      [{ price: 95, side: 'L', leverage: 10 }, { price: 105, side: 'S', leverage: 10 }],
      (price: number) => (price - 50) * 0.8,
      () => 0
    );

    expect(gap).toMatchObject({ left: 95, right: 105, width: 10 });
    expect(gap.leftX).toBeGreaterThan(gap.rightX);
  });

  test('shows the saved intrusion once when the current map no longer has its zone active', () => {
    const signature = '2026-06-01 22:00:00|S|10|78600:1';
    const labelFor = inlineFunction('recordedIntrusionLabel', 'updateGapMonitor', {
      payload: {
        intrusionDomStudy: {
          history: [{ timestamp: '2026-06-01 22:00:00', signature, sideCounts: '1 right edge' }]
        },
        delayHistory: {
          records: [{ intrusionTimestamp: '2026-06-01 22:00:00', signature: `FILTERED|${signature}` }]
        }
      },
      nlTime: (timestamp: string) => timestamp,
      money: (price: number) => `$${price}`
    });

    expect(labelFor({ t: '2026-06-01 22:00:00' })).toContain('1 recorded intrusion');
    expect(labelFor({ t: '2026-06-01 22:00:00' })).toContain('1 right edge @$78600');
    expect(labelFor({ t: '2026-06-01 22:00:00' })).toContain('historical record');
    expect(labelFor({ t: '2026-06-02 00:00:00' })).toBe('');
  });

  test('shows only unbroken daily and weekly fractals in the intact overview', () => {
    const intactFractalRecords = inlineFunction('intactFractalRecords', 'updateFractalHistory', {});
    const records = [
      { id: 'high-intact', type: 'HIGH' },
      { id: 'low-broken', type: 'LOW', firstBrokenAt: '2026-09-01T00:00:00.000Z' },
      { id: 'low-intact', type: 'LOW', firstBrokenAt: undefined }
    ];

    expect(intactFractalRecords(records).map((record: any) => record.id)).toEqual([
      'high-intact',
      'low-intact'
    ]);
    expect(html).toContain('id="dailyIntactFractalRows"');
    expect(html).toContain('id="weeklyIntactFractalRows"');
  });
});
