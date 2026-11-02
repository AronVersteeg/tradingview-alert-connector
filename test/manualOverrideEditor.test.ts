import fs from 'fs';
import path from 'path';
import vm from 'vm';

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'decentrader_liquidity_timelapse.html'), 'utf8');

function inlineFunction(name: string): string {
  const start = html.search(new RegExp(`^    (?:async )?function ${name}\\(`, 'm'));
  if (start < 0) throw new Error(`Missing editor function ${name}`);
  const rest = html.slice(start + 1);
  const next = rest.search(/^    (?:async )?function \w+\(/m);
  return html.slice(start, next < 0 ? html.length : start + 1 + next);
}

function editor() {
  const feedback = { textContent: '', hidden: true, classList: { toggle: jest.fn() } };
  const plan = {
    id: 'btc-manual-short-1793379783310-74970', direction: 'short', status: 'ARMED', closeTrigger: 74970,
    takeProfits: [72300, 70800, 67690, 62700].map(price => ({ price, allocationPct: 25 }))
  };
  const other = { id: 'other-plan', direction: 'long', status: 'ARMED', closeTrigger: 84900 };
  const context: any = {
    document: { getElementById: () => feedback },
    manualOverrideStatus: { textContent: '' },
    manualOverrideTrigger: { value: '76300' },
    manualOverrideDirection: { value: 'short' },
    manualOverrideToken: { focus: jest.fn() },
    manualOverrideArm: { disabled: false, textContent: 'Save changes' },
    manualOverrideSaving: false,
    manualEditingPlanId: plan.id,
    manualOverrideMarket: 'BTC-USD',
    CLOUD_MANUAL_OVERRIDE_URL: '/decentrader/manual-override',
    manualOverrideSnapshot: { enabled: true, overrides: [plan, other] },
    manualToken: jest.fn(() => 'test-only-token'),
    manualTakeProfitsFromForm: () => plan.takeProfits,
    manualOverrides: () => context.manualOverrideSnapshot.overrides,
    updateManualOverrideMonitor: jest.fn(snapshot => { context.manualOverrideSnapshot = snapshot; }),
    resetManualOverrideEditor: jest.fn(() => { context.manualEditingPlanId = null; }),
    refreshManualOverride: jest.fn(async () => context.manualOverrideSnapshot),
    money: (price: number) => String(price),
    fetch: jest.fn(async () => ({ ok: true, json: async () => ({
      ok: true, override: { ...plan, closeTrigger: 76300 }
    }) }))
  };
  vm.createContext(context);
  vm.runInContext(['manualOverrideFeedback', 'manualOverrideInvalid', 'armManualOverride'].map(inlineFunction).join('\n'), context);
  return { context, feedback, plan, other, submit: () => context.armManualOverride({ preventDefault: jest.fn() }) };
}

describe('manual override save feedback', () => {
  test('sends one PATCH for the existing BTC short, preserving its four TP allocations and other plans', async () => {
    const { context, feedback, plan, other, submit } = editor();
    await submit();
    expect(context.fetch).toHaveBeenCalledTimes(1);
    const [url, options] = context.fetch.mock.calls[0];
    expect(url).toContain(`id=${plan.id}`);
    expect(options.method).toBe('PATCH');
    expect(JSON.parse(options.body)).toEqual({
      market: 'BTC-USD', direction: 'short', closeTrigger: 76300, takeProfits: plan.takeProfits
    });
    expect(context.manualOverrideSnapshot.overrides.find((item: any) => item.id === other.id)).toEqual(other);
    expect(feedback.textContent).toBe('Saved: SHORT < 76300.');
    expect(context.manualOverrideArm.disabled).toBe(false);
  });

  test('shows a missing token next to Save without sending a request', async () => {
    const { context, feedback, submit } = editor();
    context.manualToken.mockReturnValue('');
    await submit();
    expect(context.fetch).not.toHaveBeenCalled();
    expect(feedback.textContent).toBe('API token required. No changes saved.');
    expect(feedback.hidden).toBe(false);
    expect(context.manualOverrideToken.focus).toHaveBeenCalled();
  });

  test('shows browser validation failures even when native validation prevents the submit event', () => {
    const { context, feedback } = editor();
    context.manualOverrideInvalid({ target: {
      getAttribute: () => 'TP1 allocation percentage', validationMessage: 'Please enter a valid value.'
    } });
    expect(feedback.textContent).toBe('TP1 allocation percentage: Please enter a valid value.');
    expect(feedback.hidden).toBe(false);
    expect(html).toContain("addEventListener('invalid', manualOverrideInvalid, true)");
  });

  test('keeps the editor and explicit server error after an unauthorized save', async () => {
    const { context, feedback, plan, submit } = editor();
    context.fetch.mockResolvedValue({ ok: false, json: async () => ({ error: 'Unauthorized' }) });
    await submit();
    expect(feedback.textContent).toBe('Unauthorized');
    expect(context.manualEditingPlanId).toBe(plan.id);
    expect(context.resetManualOverrideEditor).not.toHaveBeenCalled();
    expect(context.manualOverrideArm.textContent).toBe('Save changes');
  });

  test('distinguishes a confirmed save from a subsequent failed refresh without resubmitting', async () => {
    const { context, feedback, submit } = editor();
    context.refreshManualOverride.mockRejectedValue(new Error('Network unavailable'));
    await submit();
    expect(feedback.textContent).toContain('Saved: SHORT 76300.');
    expect(feedback.textContent).toContain('Plan refresh unavailable');
    expect(context.fetch).toHaveBeenCalledTimes(1);
    expect(context.manualOverrideSnapshot.overrides.find((item: any) => item.direction === 'short').closeTrigger).toBe(76300);
  });

  test('does not report success when the response refers to another plan', async () => {
    const { context, feedback, submit } = editor();
    context.fetch.mockResolvedValue({ ok: true, json: async () => ({ override: {
      id: 'another-plan', direction: 'short', closeTrigger: 76300
    } }) });
    await submit();
    expect(feedback.textContent).toContain('could not be verified');
    expect(context.resetManualOverrideEditor).not.toHaveBeenCalled();
  });

  test('does not send duplicate requests while Save is pending', async () => {
    const { context, feedback, plan, submit } = editor();
    let resolve: (value: unknown) => void = () => undefined;
    context.fetch.mockImplementation(() => new Promise(done => { resolve = done; }));
    const pending = submit();
    await submit();
    expect(context.fetch).toHaveBeenCalledTimes(1);
    expect(feedback.textContent).toBe('Saving...');
    expect(context.manualOverrideArm.disabled).toBe(true);
    resolve({ ok: true, json: async () => ({ override: { ...plan, closeTrigger: 76300 } }) });
    await pending;
    expect(context.manualOverrideSaving).toBe(false);
  });
});
