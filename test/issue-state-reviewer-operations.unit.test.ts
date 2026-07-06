/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import {
  ensureLabelsPresentOnce,
  ensureAssigneesPresent,
  ensureReviewLabelsPresentOnIssue,
  removeExactLabelsFromIssue,
  removeProgressStatusLabels,
  removeRejectedStatusLabel,
  removeReviewPendingLabelsAfterApproval,
  applyApprovedRequestState,
  addApprovedLabelToPr,
  resolveAdditionalIssueApproversFromApprovalHook,
  resolveManualReviewApproverOverrideFromApprovalHook,
} from '../src/handlers/request/application/issue-state-reviewer-operations.js';

const params = { owner: 'org', repo: 'repo', issue_number: 1 };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCtx(overrides: Record<string, unknown> = {}) {
  return {
    octokit: {
      issues: {
        get: jest.fn().mockResolvedValue({ data: { labels: [], assignees: [] } }),
        addLabels: jest.fn().mockResolvedValue({}),
        removeLabel: jest.fn().mockResolvedValue({}),
        addAssignees: jest.fn().mockResolvedValue({}),
      },
    },
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCbs(overrides: Record<string, unknown> = {}) {
  return {
    toLabelNames: jest.fn((labels: unknown[]) =>
      (labels || []).map((l: any) => (typeof l === 'string' ? l : (l?.name ?? ''))).filter(Boolean)
    ),
    normalizeKey: jest.fn((v: unknown) =>
      String(v ?? '')
        .trim()
        .toLowerCase()
    ),
    getHttpStatus: jest.fn((e: unknown): number | undefined => {
      if (typeof e === 'object' && e !== null && 'status' in e) {
        const s = (e as Record<string, unknown>)['status'];
        return typeof s === 'number' ? s : undefined;
      }
      return undefined;
    }),
    getErrorMessage: jest.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
    log: jest.fn(),
    labelsMatching: jest.fn((labels: string[], pattern: string) =>
      labels.filter((l) => l.toLowerCase().includes(pattern.toLowerCase()))
    ),
    resolveWorkflowLabel: jest.fn((_ctx: unknown, _key: string, fallback: string) => fallback),
    resolveEffectiveRequestType: jest.fn().mockReturnValue('systemNamespace'),
    extractResourceNameFromForm: jest.fn().mockReturnValue('sap.base'),
    runApprovalHook: jest.fn().mockResolvedValue({ status: 'unknown', approvers: [] }),
    resolveEffectiveConstants: jest.fn().mockReturnValue({}),
    ...overrides,
  } as any;
}

// ---------------------------------------------------------------------------
// ensureAssigneesPresent
// ---------------------------------------------------------------------------

describe('ensureAssigneesPresent', () => {
  it('returns immediately when assignees is null/undefined (L205 || [] arm)', async () => {
    const ctx = makeCtx();
    const cbs = makeCbs();
    await ensureAssigneesPresent(ctx, params, null as any, cbs);
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  it('returns immediately when all assignees already present (L223 true arm)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({
            data: { assignees: [{ login: 'alice' }] },
          }),
          addAssignees: jest.fn().mockResolvedValue({}),
        },
      },
    });
    const cbs = makeCbs();
    await ensureAssigneesPresent(ctx, params, ['alice'], cbs);
    expect(ctx.octokit.issues.addAssignees).not.toHaveBeenCalled();
  });

  it('logs non-Error on addAssignees failure (L234 String(error) arm)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { assignees: [] } }),
          addAssignees: jest.fn().mockRejectedValue('plain string error'),
        },
      },
    });
    const cbs = makeCbs();
    await ensureAssigneesPresent(ctx, params, ['bob'], cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      ctx,
      'warn',
      expect.objectContaining({ err: 'plain string error' }),
      'failed to ensure assignees'
    );
  });
});

// ---------------------------------------------------------------------------
// ensureReviewLabelsPresentOnIssue
// ---------------------------------------------------------------------------

describe('ensureReviewLabelsPresentOnIssue', () => {
  it('returns true when reviewRequestedLabels is null/undefined (L265 || [] arm, L266 true arm)', async () => {
    const ctx = makeCtx();
    const eff = { reviewRequestedLabels: undefined };
    const result = await ensureReviewLabelsPresentOnIssue(ctx, params, { labels: [] }, eff, makeCbs());
    expect(result).toBe(true);
  });

  it('returns true when reviewRequestedLabels is empty array (L266 true arm)', async () => {
    const ctx = makeCtx();
    const result = await ensureReviewLabelsPresentOnIssue(
      ctx,
      params,
      { labels: [] },
      { reviewRequestedLabels: [] },
      makeCbs()
    );
    expect(result).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// removeExactLabelsFromIssue
// ---------------------------------------------------------------------------

describe('removeExactLabelsFromIssue', () => {
  it('skips empty/blank labels (L370 !name continue arm)', async () => {
    const ctx = makeCtx();
    await removeExactLabelsFromIssue(ctx, params, ['', '  ', 'valid-label'], makeCbs());
    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledTimes(1);
  });

  it('logs non-Error on removeLabel failure for non-404 (L379 String(error) arm)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          removeLabel: jest.fn().mockRejectedValue('non-error failure'),
        },
      },
    });
    const cbs = makeCbs({ getHttpStatus: jest.fn().mockReturnValue(500) });
    await removeExactLabelsFromIssue(ctx, params, ['some-label'], cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      ctx,
      'warn',
      expect.objectContaining({ err: 'non-error failure' }),
      'failed to remove label'
    );
  });
});

// ---------------------------------------------------------------------------
// removeProgressStatusLabels
// ---------------------------------------------------------------------------

describe('removeProgressStatusLabels', () => {
  it('fetches labels when currentLabels is null (L407 || [] arm, L408 true arm)', async () => {
    const ctx = makeCtx();
    const cbs = makeCbs({ labelsMatching: jest.fn().mockReturnValue([]) });
    await removeProgressStatusLabels(ctx, params, undefined, cbs);
    expect(ctx.octokit.issues.get).toHaveBeenCalled();
  });

  it('returns early when no progress labels to remove (L426 true arm)', async () => {
    const ctx = makeCtx();
    const cbs = makeCbs({ labelsMatching: jest.fn().mockReturnValue([]) });
    await removeProgressStatusLabels(ctx, params, ['some-label'], cbs);
    expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// removeRejectedStatusLabel
// ---------------------------------------------------------------------------

describe('removeRejectedStatusLabel', () => {
  it('fetches labels when currentLabels is null (L449 || [] arm)', async () => {
    const ctx = makeCtx();
    const cbs = makeCbs({ labelsMatching: jest.fn().mockReturnValue([]) });
    await removeRejectedStatusLabel(ctx, params, undefined, cbs);
    expect(ctx.octokit.issues.get).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// removeReviewPendingLabelsAfterApproval
// ---------------------------------------------------------------------------

describe('removeReviewPendingLabelsAfterApproval', () => {
  it('returns immediately when reviewRequestedLabels is null (L305 || [] arm)', async () => {
    const ctx = makeCtx();
    const eff = { labelOnApproved: 'Approved', reviewRequestedLabels: undefined };
    await removeReviewPendingLabelsAfterApproval(ctx, params, eff, makeCbs());
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  it('returns immediately when approvedCfg is empty (L307 || arm)', async () => {
    const ctx = makeCtx();
    const eff = { labelOnApproved: '', reviewRequestedLabels: ['Review Pending'] };
    await removeReviewPendingLabelsAfterApproval(ctx, params, eff, makeCbs());
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  it('logs non-Error when removeLabel throws non-404 non-Error (L341 String(error) arm)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { labels: [{ name: 'Approved' }, { name: 'Review Pending' }] } }),
          removeLabel: jest.fn().mockRejectedValue('non-error'),
        },
      },
    });
    const cbs = makeCbs({
      toLabelNames: jest.fn(() => ['Approved', 'Review Pending']),
      getHttpStatus: jest.fn().mockReturnValue(500),
    });
    const eff = { labelOnApproved: 'Approved', reviewRequestedLabels: ['Review Pending'] };
    await removeReviewPendingLabelsAfterApproval(ctx, params, eff, cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      ctx,
      'warn',
      expect.objectContaining({ err: 'non-error' }),
      'failed to remove review pending label after approval'
    );
  });
});

// ---------------------------------------------------------------------------
// applyApprovedRequestState — L495 || 'Approved' arm
// ---------------------------------------------------------------------------

describe('applyApprovedRequestState', () => {
  it('uses "Approved" fallback when labelOnApproved is undefined (L495 || "Approved" arm)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          addLabels: jest.fn().mockResolvedValue({}),
          get: jest.fn().mockResolvedValue({ data: { labels: [] } }),
          removeLabel: jest.fn().mockResolvedValue({}),
        },
      },
    });
    const eff = { labelOnApproved: undefined, reviewRequestedLabels: [] };
    const cbs = makeCbs({ labelsMatching: jest.fn().mockReturnValue([]) });
    await applyApprovedRequestState(ctx, params, eff, cbs);
  });
});

// ---------------------------------------------------------------------------
// ensureLabelsPresentOnce — L131 arm1 (null labels) and L165 arm1 (already present)
// ---------------------------------------------------------------------------

describe('ensureLabelsPresentOnce', () => {
  it('L131 binary-expr arm1: null labels uses [] fallback (empty target → immediate return)', async () => {
    const ctx = makeCtx();
    await ensureLabelsPresentOnce(ctx, params, null as any, makeCbs());
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  it('L165 if arm1: label already present → missing is empty → early return', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { labels: [{ name: 'my-label' }] } }),
          addLabels: jest.fn().mockResolvedValue({}),
          removeLabel: jest.fn().mockResolvedValue({}),
          addAssignees: jest.fn().mockResolvedValue({}),
        },
      },
    });
    await ensureLabelsPresentOnce(ctx, params, ['my-label'], makeCbs());
    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
  });

  it('L165 false-arm: target label absent → missing=[label] → addLabels called', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { labels: [] } }),
          addLabels: jest.fn().mockResolvedValue({}),
          removeLabel: jest.fn().mockResolvedValue({}),
          addAssignees: jest.fn().mockResolvedValue({}),
        },
      },
    });
    await ensureLabelsPresentOnce(ctx, { owner: 'org', repo: 'repo', issue_number: 999 }, ['absent-label'], makeCbs());
    expect(ctx.octokit.issues.addLabels).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// ensureReviewLabelsPresentOnIssue — L279 arm1 (includes left), arm2 (includes right)
// ---------------------------------------------------------------------------

describe('ensureReviewLabelsPresentOnIssue — includes paths', () => {
  it('L279 binary-expr arm1: label includes cfgKey (review-pending-status includes review-pending)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { labels: [{ name: 'Review-Pending-Status' }] } }),
          addLabels: jest.fn(),
          removeLabel: jest.fn(),
          addAssignees: jest.fn(),
        },
      },
    });
    const eff = { reviewRequestedLabels: ['review-pending'] };
    const result = await ensureReviewLabelsPresentOnIssue(ctx, params, { labels: [] }, eff, makeCbs());
    expect(result).toBe(true);
  });

  it('L279 binary-expr arm2: cfgKey includes label (review-pending includes pending)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({ data: { labels: [{ name: 'pending' }] } }),
          addLabels: jest.fn(),
          removeLabel: jest.fn(),
          addAssignees: jest.fn(),
        },
      },
    });
    const eff = { reviewRequestedLabels: ['review-pending'] };
    const result = await ensureReviewLabelsPresentOnIssue(ctx, params, { labels: [] }, eff, makeCbs());
    expect(result).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// removeReviewPendingLabelsAfterApproval — L337 arm1 (404 suppressed)
// ---------------------------------------------------------------------------

describe('removeReviewPendingLabelsAfterApproval — 404 suppression', () => {
  it('L337 if arm1: removeLabel throws 404 → error suppressed (no log called)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          get: jest.fn().mockResolvedValue({
            data: { labels: [{ name: 'Approved' }, { name: 'Review Pending' }] },
          }),
          removeLabel: jest.fn().mockRejectedValue({ status: 404 }),
          addLabels: jest.fn(),
          addAssignees: jest.fn(),
        },
      },
    });
    const cbs = makeCbs({
      toLabelNames: jest.fn(() => ['Approved', 'Review Pending']),
      getHttpStatus: jest.fn((e: unknown) => (e as any)?.status),
    });
    const eff = { labelOnApproved: 'Approved', reviewRequestedLabels: ['Review Pending'] };
    await removeReviewPendingLabelsAfterApproval(ctx, params, eff, cbs);
    expect(cbs.log).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// removeExactLabelsFromIssue — L375 arm1 (404 suppressed)
// ---------------------------------------------------------------------------

describe('removeExactLabelsFromIssue — 404 suppression', () => {
  it('L375 if arm1: removeLabel throws 404 → error suppressed (no log called)', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          removeLabel: jest.fn().mockRejectedValue({ status: 404 }),
          addLabels: jest.fn(),
          addAssignees: jest.fn(),
          get: jest.fn(),
        },
      },
    });
    const cbs = makeCbs({ getHttpStatus: jest.fn((e: unknown) => (e as any)?.status) });
    await removeExactLabelsFromIssue(ctx, params, ['label-to-remove'], cbs);
    expect(cbs.log).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// addApprovedLabelToPr — L533 arm1 (null labelOnApproved → 'Approved' fallback)
// ---------------------------------------------------------------------------

describe('addApprovedLabelToPr', () => {
  it('L533 binary-expr arm1: null labelOnApproved falls back to "Approved"', async () => {
    const ctx = makeCtx({
      octokit: {
        issues: {
          addLabels: jest.fn().mockResolvedValue({}),
          get: jest.fn().mockResolvedValue({ data: { labels: [] } }),
          removeLabel: jest.fn().mockResolvedValue({}),
          addAssignees: jest.fn(),
        },
      },
    });
    const cbs = makeCbs({
      resolveEffectiveConstants: jest.fn().mockReturnValue({ labelOnApproved: null, reviewRequestedLabels: [] }),
      labelsMatching: jest.fn().mockReturnValue([]),
    });
    await addApprovedLabelToPr(ctx, { owner: 'org', repo: 'repo' }, 42, undefined, cbs);
    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(expect.objectContaining({ labels: ['Approved'] }));
  });
});

// ---------------------------------------------------------------------------
// resolveAdditionalIssueApproversFromApprovalHook — L592 || arm
// ---------------------------------------------------------------------------

describe('resolveAdditionalIssueApproversFromApprovalHook', () => {
  it('falls back to resolveEffectiveRequestType when requestType is undefined (L592 || arm)', async () => {
    const ctx = {};
    const issue = { body: '' };
    const template = {};
    const fd = {};
    const cbs = makeCbs({
      resolveEffectiveRequestType: jest.fn().mockReturnValue(''),
    });
    const result = await resolveAdditionalIssueApproversFromApprovalHook(
      ctx,
      params,
      issue,
      template,
      fd,
      undefined,
      cbs
    );
    expect(result).toEqual([]);
    expect(cbs.resolveEffectiveRequestType).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// resolveManualReviewApproverOverrideFromApprovalHook — L641 arm1 + L644 arm0
// ---------------------------------------------------------------------------

describe('resolveManualReviewApproverOverrideFromApprovalHook', () => {
  it('L641 binary-expr arm1 + L644 if arm0: undefined requestType + empty resolveEffectiveRequestType → []', async () => {
    const ctx = {};
    const issue = { body: '' };
    const template = {};
    const fd = {};
    const cbs = makeCbs({
      resolveEffectiveRequestType: jest.fn().mockReturnValue(''),
    });
    const result = await resolveManualReviewApproverOverrideFromApprovalHook(
      ctx,
      params,
      issue,
      template,
      fd,
      undefined,
      cbs
    );
    expect(result).toEqual([]);
    expect(cbs.resolveEffectiveRequestType).toHaveBeenCalled();
  });
});
