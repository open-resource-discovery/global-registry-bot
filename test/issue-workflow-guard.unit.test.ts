/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import {
  detectSingleRoutingLabel,
  ensureRoutingLockMarker,
  enforceRoutingLabelLock,
  handleClosedIssueWorkflowGuard,
  handleIssueLabelChangeWorkflowGuard,
} from '../src/handlers/request/application/issue-workflow-guard.js';

const ctx = {};
const template = { _type: 'namespace' } as any;
const formData: Record<string, string> = {};

let issueCounter = 1000;
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function nextParams() {
  return { owner: 'org', repo: 'repo', issue_number: issueCounter++ };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function baseIssue(overrides: Record<string, unknown> = {}) {
  return { number: 1, labels: [], state: 'open', body: 'issue body', ...overrides };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, unknown> = {}) {
  return {
    tryLoadTemplateForLabels: jest.fn().mockResolvedValue(template),
    normalizeKey: jest.fn((v: unknown) =>
      String(v ?? '')
        .toLowerCase()
        .trim()
    ),
    postOnce: jest.fn().mockResolvedValue(undefined),
    updateIssueBody: jest.fn().mockResolvedValue(undefined),
    fetchIssueLabels: jest.fn().mockResolvedValue([]),
    toLabelNames: jest.fn((labels: unknown) => (Array.isArray(labels) ? labels : [])),
    removeExactLabelsFromIssue: jest.fn().mockResolvedValue(undefined),
    addLabels: jest.fn().mockResolvedValue(undefined),
    labelsMatching: jest.fn((labels: string[], expected: string) =>
      labels.filter((l) => l.toLowerCase() === expected.toLowerCase())
    ),
    loadTemplateWithLabelRefresh: jest.fn().mockResolvedValue(template),
    parseForm: jest.fn().mockReturnValue(formData),
    createEmptyFormData: jest.fn().mockReturnValue(formData),
    readIssueBodyForProcessing: jest.fn((body: unknown) => String(body ?? '')),
    isRequestIssue: jest.fn().mockReturnValue(true),
    resolveEffectiveConstants: jest.fn().mockReturnValue({
      labelOnApproved: 'Approved',
      approverUsernames: ['approver'],
      approverPoolUsernames: [],
    }),
    resolveLockedWorkflowLabelKeys: jest.fn().mockReturnValue(new Set<string>()),
    resolveWorkflowLabel: jest.fn((_ctx: unknown, _key: string, fallback: string) => fallback),
    resolveEffectiveRequestType: jest.fn().mockReturnValue('namespace-type'),
    resolveApproverRoutingForRequestType: jest.fn().mockReturnValue({
      approvalUsernames: ['approver'],
      autoAssigneePoolUsernames: [],
    }),
    uniqLogins: jest.fn((v: string[]) => [...new Set(v)]),
    isConfiguredApprover: jest.fn().mockReturnValue(false),
    setStateLabel: jest.fn().mockResolvedValue(undefined),
    removeRejectedStatusLabel: jest.fn().mockResolvedValue(undefined),
    removeProgressStatusLabels: jest.fn().mockResolvedValue(undefined),
    log: jest.fn(),
    getErrorMessage: jest.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
    ...overrides,
  } as any;
}

const LOCK_BODY = '<!-- nsreq:routing-lock = {"v":1,"expected":"my-template"} -->';

describe('detectSingleRoutingLabel', () => {
  test('returns empty string when no labels match a template', async () => {
    const cbs = makeCallbacks({ tryLoadTemplateForLabels: jest.fn().mockResolvedValue(null) });
    const result = await detectSingleRoutingLabel(ctx, nextParams(), baseIssue(), ['label-a', 'label-b'], cbs);
    expect(result).toBe('');
  });

  test('returns the label when exactly one matches', async () => {
    const cbs = makeCallbacks({
      tryLoadTemplateForLabels: jest.fn().mockResolvedValueOnce(null).mockResolvedValueOnce(template),
    });
    const result = await detectSingleRoutingLabel(ctx, nextParams(), baseIssue(), ['label-a', 'label-b'], cbs);
    expect(result).toBe('label-b');
  });

  test('empty-string label is skipped via normalizeKey empty check', async () => {
    const cbs = makeCallbacks({ tryLoadTemplateForLabels: jest.fn().mockResolvedValue(null) });
    const result = await detectSingleRoutingLabel(ctx, nextParams(), baseIssue(), ['', 'real-label'], cbs);
    expect(result).toBe('');
    expect(cbs.tryLoadTemplateForLabels).toHaveBeenCalledTimes(1);
  });

  test('duplicate label is skipped (seen.has check)', async () => {
    const cbs = makeCallbacks({ tryLoadTemplateForLabels: jest.fn().mockResolvedValue(template) });
    const result = await detectSingleRoutingLabel(ctx, nextParams(), baseIssue(), ['label-a', 'Label-A'], cbs);
    expect(result).toBe('label-a');
    expect(cbs.tryLoadTemplateForLabels).toHaveBeenCalledTimes(1);
  });

  test('returns empty string when multiple labels match (routing.length !== 1)', async () => {
    const cbs = makeCallbacks({ tryLoadTemplateForLabels: jest.fn().mockResolvedValue(template) });
    const result = await detectSingleRoutingLabel(ctx, nextParams(), baseIssue(), ['label-a', 'label-b'], cbs);
    expect(result).toBe('');
  });
});

describe('ensureRoutingLockMarker', () => {
  test('returns false for empty expectedLabel', async () => {
    const cbs = makeCallbacks();
    expect(await ensureRoutingLockMarker(ctx, nextParams(), baseIssue(), '', cbs)).toBe(false);
    expect(cbs.updateIssueBody).not.toHaveBeenCalled();
  });

  test('returns false when lock already matches expected label', async () => {
    const cbs = makeCallbacks();
    const issue = baseIssue({ body: LOCK_BODY });
    expect(await ensureRoutingLockMarker(ctx, nextParams(), issue, 'my-template', cbs)).toBe(false);
    expect(cbs.updateIssueBody).not.toHaveBeenCalled();
  });

  test('returns true and calls updateIssueBody when lock needs to be set', async () => {
    const cbs = makeCallbacks();
    expect(await ensureRoutingLockMarker(ctx, nextParams(), baseIssue(), 'new-template', cbs)).toBe(true);
    expect(cbs.updateIssueBody).toHaveBeenCalled();
  });

  test('returns false when updateIssueBody throws (catch block)', async () => {
    const cbs = makeCallbacks({
      updateIssueBody: jest.fn().mockRejectedValue(new Error('network error')),
    });
    expect(await ensureRoutingLockMarker(ctx, nextParams(), baseIssue(), 'new-template', cbs)).toBe(false);
  });
});

describe('enforceRoutingLabelLock', () => {
  test('returns false for empty expectedLabel', async () => {
    expect(await enforceRoutingLabelLock(ctx, nextParams(), baseIssue(), '', makeCallbacks())).toBe(false);
  });

  test('uses toLabelNames fallback when fetchIssueLabels fails', async () => {
    const issue = baseIssue({ labels: ['my-template'] });
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockRejectedValue(new Error('api error')),
      toLabelNames: jest.fn().mockReturnValue(['my-template']),
      tryLoadTemplateForLabels: jest.fn().mockResolvedValue(null),
    });
    await enforceRoutingLabelLock(ctx, nextParams(), issue, 'my-template', cbs);
    expect(cbs.toLabelNames).toHaveBeenCalledWith(issue.labels);
  });

  test('removes wrong routing labels and adds expected; notifies with no opts (shouldNotify via !touchedLabel)', async () => {
    const p = nextParams();
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValue(['old-template']),
      tryLoadTemplateForLabels: jest
        .fn()
        .mockImplementation((_c: unknown, _p: unknown, _i: unknown, labels: string[]) =>
          Promise.resolve(labels.includes('old-template') ? template : null)
        ),
    });
    const result = await enforceRoutingLabelLock(ctx, p, baseIssue(), 'new-template', cbs);
    expect(result).toBe(true);
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalledWith(ctx, p, ['old-template']);
    expect(cbs.addLabels).toHaveBeenCalledWith(ctx, p, ['new-template']);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('addLabels failure is caught silently when toRemove is empty', async () => {
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValue([]),
      tryLoadTemplateForLabels: jest.fn().mockResolvedValue(null),
      addLabels: jest.fn().mockRejectedValue(new Error('label quota exceeded')),
    });
    const result = await enforceRoutingLabelLock(ctx, nextParams(), baseIssue(), 'new-template', cbs);
    expect(result).toBe(false);
  });

  test('shouldNotify true via touchedLabel matching expectedKey (arm 1)', async () => {
    const p = nextParams();
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValue(['old-template']),
      tryLoadTemplateForLabels: jest
        .fn()
        .mockImplementation((_c: unknown, _p: unknown, _i: unknown, labels: string[]) =>
          Promise.resolve(labels.includes('old-template') ? template : null)
        ),
    });
    const result = await enforceRoutingLabelLock(ctx, p, baseIssue(), 'new-template', cbs, {
      changedLabel: 'new-template',
    });
    expect(result).toBe(true);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('shouldNotify via isRoutingLabelName when touchedLabel does not match expected (arm 2)', async () => {
    const p = nextParams();
    let detectCall = 0;
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValue(['old-template']),
      tryLoadTemplateForLabels: jest
        .fn()
        .mockImplementation((_c: unknown, _p: unknown, _i: unknown, labels: string[]) => {
          detectCall++;
          if (detectCall <= 1) return Promise.resolve(labels.includes('old-template') ? template : null);
          return Promise.resolve(template);
        }),
    });
    const result = await enforceRoutingLabelLock(ctx, p, baseIssue(), 'new-template', cbs, {
      changedLabel: 'unrelated-label',
    });
    expect(result).toBe(true);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('isRoutingLabelName catch block: shouldNotify false when template load throws', async () => {
    const p = nextParams();
    let detectCall = 0;
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValue(['old-template']),
      tryLoadTemplateForLabels: jest
        .fn()
        .mockImplementation((_c: unknown, _p: unknown, _i: unknown, labels: string[]) => {
          detectCall++;
          if (detectCall <= 1) return Promise.resolve(labels.includes('old-template') ? template : null);
          return Promise.reject(new Error('template load failed'));
        }),
    });
    const result = await enforceRoutingLabelLock(ctx, p, baseIssue(), 'new-template', cbs, {
      changedLabel: 'unrelated-label',
    });
    expect(result).toBe(true);
    expect(cbs.postOnce).not.toHaveBeenCalled();
  });
});

describe('handleClosedIssueWorkflowGuard', () => {
  test('returns early when loadTemplateWithLabelRefresh throws (catch block)', async () => {
    const cbs = makeCallbacks({
      loadTemplateWithLabelRefresh: jest.fn().mockRejectedValue(new Error('not found')),
    });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.isRequestIssue).not.toHaveBeenCalled();
  });

  test('uses createEmptyFormData when template resolves to null', async () => {
    const cbs = makeCallbacks({
      loadTemplateWithLabelRefresh: jest.fn().mockResolvedValue(null),
      isRequestIssue: jest.fn().mockReturnValue(false),
    });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.createEmptyFormData).toHaveBeenCalled();
  });

  test('returns early when issue is not a request issue', async () => {
    const cbs = makeCallbacks({ isRequestIssue: jest.fn().mockReturnValue(false) });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.fetchIssueLabels).not.toHaveBeenCalled();
  });

  test('uses toLabelNames when first fetchIssueLabels fails', async () => {
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockRejectedValueOnce(new Error('api error')).mockResolvedValue([]),
      toLabelNames: jest.fn().mockReturnValue([]),
    });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.toLabelNames).toHaveBeenCalled();
  });

  test('removes rejected and progress labels when Approved label is present', async () => {
    const p = nextParams();
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue(['Approved']) });
    await handleClosedIssueWorkflowGuard(ctx, p, baseIssue(), cbs);
    expect(cbs.removeRejectedStatusLabel).toHaveBeenCalledWith(ctx, p, ['Approved']);
    expect(cbs.removeProgressStatusLabels).toHaveBeenCalledWith(ctx, p, ['Approved']);
  });

  test('adds Rejected label when not already present', async () => {
    const p = nextParams();
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue([]) });
    await handleClosedIssueWorkflowGuard(ctx, p, baseIssue(), cbs);
    expect(cbs.addLabels).toHaveBeenCalledWith(ctx, p, ['Rejected']);
  });

  test('skips addLabels when Rejected label already present', async () => {
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue(['Rejected']) });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.addLabels).not.toHaveBeenCalled();
    expect(cbs.removeProgressStatusLabels).toHaveBeenCalled();
  });

  test('second fetchIssueLabels failure is swallowed silently', async () => {
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValueOnce([]).mockRejectedValueOnce(new Error('2nd api error')),
    });
    await expect(handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs)).resolves.not.toThrow();
  });

  test('removes Approved labels found after rejection is applied', async () => {
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValueOnce(['Rejected']).mockResolvedValueOnce(['Rejected', 'Approved']),
    });
    await handleClosedIssueWorkflowGuard(ctx, nextParams(), baseIssue(), cbs);
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalled();
  });
});

describe('handleIssueLabelChangeWorkflowGuard', () => {
  test('returns early when no routing lock and not a request issue', async () => {
    const cbs = makeCallbacks({ isRequestIssue: jest.fn().mockReturnValue(false) });
    await handleIssueLabelChangeWorkflowGuard(ctx, nextParams(), baseIssue(), 'labeled', 'some-label', null, cbs);
    expect(cbs.fetchIssueLabels).not.toHaveBeenCalled();
  });

  test('uses fallback approver routing when effectiveRequestType is empty; labelOnApproved empty falls back to Approved', async () => {
    const cbs = makeCallbacks({
      resolveEffectiveRequestType: jest.fn().mockReturnValue(''),
      resolveEffectiveConstants: jest.fn().mockReturnValue({
        labelOnApproved: '',
        approverUsernames: ['approver'],
        approverPoolUsernames: ['pool-user'],
      }),
    });
    await handleIssueLabelChangeWorkflowGuard(ctx, nextParams(), baseIssue(), 'labeled', 'other-label', null, cbs);
    expect(cbs.resolveApproverRoutingForRequestType).not.toHaveBeenCalled();
    expect(cbs.uniqLogins).toHaveBeenCalled();
  });

  test('returns early when sender is a configured approver and changedKey is in managed keys', async () => {
    const cbs = makeCallbacks({ isConfiguredApprover: jest.fn().mockReturnValue(true) });
    await handleIssueLabelChangeWorkflowGuard(
      ctx,
      nextParams(),
      baseIssue(),
      'labeled',
      'Requester Action',
      'approver',
      cbs
    );
    expect(cbs.postOnce).not.toHaveBeenCalled();
    expect(cbs.removeExactLabelsFromIssue).not.toHaveBeenCalled();
  });

  test('reverts manually added Approved label and posts notice', async () => {
    const p = nextParams();
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue(['Approved']) });
    await handleIssueLabelChangeWorkflowGuard(ctx, p, baseIssue(), 'labeled', 'Approved', null, cbs);
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalled();
    expect(cbs.setStateLabel).toHaveBeenCalled();
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('setStateLabel called with author when Requester Action label is present', async () => {
    const p = nextParams();
    const issue = baseIssue({ labels: ['Approved', 'Requester Action'] });
    const cbs = makeCallbacks();
    await handleIssueLabelChangeWorkflowGuard(ctx, p, issue, 'labeled', 'Approved', null, cbs);
    expect(cbs.setStateLabel).toHaveBeenCalledWith(ctx, p, expect.anything(), 'author');
  });

  test('reverts manually added Rejected label on open issue', async () => {
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue(['Rejected']) });
    await handleIssueLabelChangeWorkflowGuard(
      ctx,
      nextParams(),
      baseIssue({ state: 'open' }),
      'labeled',
      'Rejected',
      null,
      cbs
    );
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalled();
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('closed issue with Approved label: removes rejected and progress labels', async () => {
    const cbs = makeCallbacks({ fetchIssueLabels: jest.fn().mockResolvedValue(['Approved']) });
    await handleIssueLabelChangeWorkflowGuard(
      ctx,
      nextParams(),
      baseIssue({ state: 'closed' }),
      'labeled',
      'other-label',
      null,
      cbs
    );
    expect(cbs.removeRejectedStatusLabel).toHaveBeenCalled();
    expect(cbs.removeProgressStatusLabels).toHaveBeenCalled();
  });

  test('routing lock enforced: refreshes labels after enforcement', async () => {
    const issue = baseIssue({ body: LOCK_BODY });
    const cbs = makeCallbacks({
      fetchIssueLabels: jest.fn().mockResolvedValueOnce(['old-template']).mockResolvedValueOnce(['my-template']),
      tryLoadTemplateForLabels: jest
        .fn()
        .mockImplementation((_c: unknown, _p: unknown, _i: unknown, labels: string[]) =>
          Promise.resolve(labels.includes('old-template') ? template : null)
        ),
    });
    await handleIssueLabelChangeWorkflowGuard(ctx, nextParams(), issue, 'labeled', 'my-template', null, cbs);
    expect(cbs.fetchIssueLabels).toHaveBeenCalledTimes(2);
  });

  test('unlabeled action on locked key re-adds the label', async () => {
    const p = nextParams();
    const lockedKeys = new Set<string>(['my-locked-label']);
    const cbs = makeCallbacks({
      resolveLockedWorkflowLabelKeys: jest.fn().mockReturnValue(lockedKeys),
    });
    await handleIssueLabelChangeWorkflowGuard(ctx, p, baseIssue(), 'unlabeled', 'My-Locked-Label', null, cbs);
    expect(cbs.addLabels).toHaveBeenCalledWith(ctx, p, ['My-Locked-Label']);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('unlabeled action on locked key: addLabels failure is silently ignored', async () => {
    const lockedKeys = new Set<string>(['my-locked-label']);
    const cbs = makeCallbacks({
      resolveLockedWorkflowLabelKeys: jest.fn().mockReturnValue(lockedKeys),
      addLabels: jest.fn().mockRejectedValue(new Error('rate limited')),
    });
    await expect(
      handleIssueLabelChangeWorkflowGuard(ctx, nextParams(), baseIssue(), 'unlabeled', 'My-Locked-Label', null, cbs)
    ).resolves.not.toThrow();
    expect(cbs.postOnce).toHaveBeenCalled();
  });
});
