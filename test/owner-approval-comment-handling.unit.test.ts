/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import {
  handleSystemContactOwnerApprovalIfNeeded,
  handleParentOwnerApprovalIfNeeded,
} from '../src/handlers/request/application/owner-approval-comment-handling.js';

const ctx = {};
const params = { owner: 'org', repo: 'repo', issue_number: 1 };
const issue = { body: '## issue body' };
const template = {};
const formData = {};

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, unknown> = {}) {
  return {
    readContactApprovalMeta: jest.fn().mockReturnValue(null),
    readParentApprovalMeta: jest.fn().mockReturnValue(null),
    normalizeLogin: jest.fn((v: unknown) => String(v ?? '').trim()),
    uniqLogins: jest.fn((arr: string[]) => [...new Set(arr)]),
    normalizeKey: jest.fn((v: unknown) => String(v ?? '').trim()),
    postOnce: jest.fn().mockResolvedValue(undefined),
    validateRequestIssue: jest.fn().mockResolvedValue({
      errors: [],
      namespace: 'test.ns',
      nsType: 'system',
      validationIssues: [],
    }),
    setStateLabel: jest.fn().mockResolvedValue(undefined),
    parseForm: jest.fn().mockReturnValue({}),
    calcSnapshotHash: jest.fn().mockReturnValue('hash123'),
    resolveEffectiveRequestType: jest.fn().mockReturnValue('systemNamespace'),
    ensureContactApprovalMarker: jest.fn().mockResolvedValue(true),
    buildApprovedContactApprovalMeta: jest.fn().mockReturnValue({ approvedBy: 'alice' }),
    maybeHandleApprovalDecision: jest.fn().mockResolvedValue('continue'),
    buildApprovalDecisionDispatchOptions: jest.fn().mockReturnValue({}),
    resolveManualReviewApproverOverrideFromApprovalHook: jest.fn().mockResolvedValue([]),
    resolveAdditionalIssueApproversFromApprovalHook: jest.fn().mockResolvedValue([]),
    handoverToCpa: jest.fn().mockResolvedValue(undefined),
    buildReviewHandoverOptions: jest.fn().mockReturnValue({}),
    ensureParentApprovalMarker: jest.fn().mockResolvedValue(true),
    buildApprovedParentApprovalMeta: jest.fn().mockReturnValue({ approvedBy: 'alice' }),
    setParentOwnerActionState: jest.fn().mockResolvedValue(undefined),
    assignParentOwnersForApproval: jest.fn().mockResolvedValue(undefined),
    clearParentOwnerActionState: jest.fn().mockResolvedValue(undefined),
    isSubContextRequestType: jest.fn().mockReturnValue(false),
    finalizeApprovedRequest: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  } as any;
}

// ---------------------------------------------------------------------------
// handleSystemContactOwnerApprovalIfNeeded
// ---------------------------------------------------------------------------

describe('handleSystemContactOwnerApprovalIfNeeded', () => {
  it('returns false when readContactApprovalMeta returns null (L207 true arm)', async () => {
    const cbs = makeCallbacks({ readContactApprovalMeta: jest.fn().mockReturnValue(null) });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(false);
  });

  it('returns false when approvedBy is already set (L208 true arm)', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: 'alice',
      }),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(false);
    expect(cbs.validateRequestIssue).not.toHaveBeenCalled();
  });

  it('returns true and posts error when owner + reval has errors (L232-244, L171 || [])', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['Field missing'],
        namespace: 'test.ns',
        nsType: 'system',
        validationIssues: undefined,
      }),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.postOnce).toHaveBeenCalled();
    expect(cbs.setStateLabel).toHaveBeenCalledWith(ctx, params, issue, 'author');
  });

  it('covers L172 || "details" arm when validationIssue has no path', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['some error'],
        namespace: 'test.ns',
        nsType: 'system',
        validationIssues: [{ message: 'some error' }],
      }),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  it('covers happy path with approvalOutcome=continue + empty manualApproversOverride (L246-308)', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: null,
      }),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.handoverToCpa).toHaveBeenCalled();
  });

  it('returns true when approvalOutcome is not "continue" (L276 true arm)', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      maybeHandleApprovalDecision: jest.fn().mockResolvedValue('approved'),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.handoverToCpa).not.toHaveBeenCalled();
  });

  it('covers L297 manualApproversOverride truthy arm (hookApprovers = [])', async () => {
    const cbs = makeCallbacks({
      readContactApprovalMeta: jest.fn().mockReturnValue({
        target: 'ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      resolveManualReviewApproverOverrideFromApprovalHook: jest.fn().mockResolvedValue(['bob']),
    });
    const result = await handleSystemContactOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.resolveAdditionalIssueApproversFromApprovalHook).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// handleParentOwnerApprovalIfNeeded
// ---------------------------------------------------------------------------

describe('handleParentOwnerApprovalIfNeeded', () => {
  it('returns false when readParentApprovalMeta returns null (L339 true arm)', async () => {
    const cbs = makeCallbacks({ readParentApprovalMeta: jest.fn().mockReturnValue(null) });
    const result = await handleParentOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(false);
  });

  it('returns false when parent approvedBy is already set (L340 true arm)', async () => {
    const cbs = makeCallbacks({
      readParentApprovalMeta: jest.fn().mockReturnValue({
        parent: 'parent.ns',
        target: 'child.ns',
        owners: ['alice'],
        approvedBy: 'alice',
      }),
    });
    const result = await handleParentOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(false);
  });

  it('returns true and posts validation error when owner + reval has errors (L364-378)', async () => {
    const cbs = makeCallbacks({
      readParentApprovalMeta: jest.fn().mockReturnValue({
        parent: 'parent.ns',
        target: 'child.ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['validation error'],
        namespace: 'child.ns',
        nsType: 'system',
        validationIssues: [{ message: 'validation error' }],
      }),
    });
    const result = await handleParentOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.postOnce).toHaveBeenCalled();
    expect(cbs.clearParentOwnerActionState).toHaveBeenCalled();
    expect(cbs.setStateLabel).toHaveBeenCalledWith(ctx, params, issue, 'author');
  });

  it('covers happy path and L439 manualApproversOverride truthy arm', async () => {
    const cbs = makeCallbacks({
      readParentApprovalMeta: jest.fn().mockReturnValue({
        parent: 'parent.ns',
        target: 'child.ns',
        owners: ['alice'],
        approvedBy: null,
      }),
      resolveManualReviewApproverOverrideFromApprovalHook: jest.fn().mockResolvedValue(['carol']),
    });
    const result = await handleParentOwnerApprovalIfNeeded(ctx, params, issue, template, formData, 'alice', cbs);
    expect(result).toBe(true);
    expect(cbs.resolveAdditionalIssueApproversFromApprovalHook).not.toHaveBeenCalled();
    expect(cbs.handoverToCpa).toHaveBeenCalled();
  });
});
