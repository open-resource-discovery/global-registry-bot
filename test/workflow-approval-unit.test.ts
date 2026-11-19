/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest, beforeAll } from '@jest/globals';

type WfMod = typeof import('../src/handlers/request/application/workflow-approval.js');
let maybeApprovePendingWorkflowRunsForPrNumbersApplication: WfMod['maybeApprovePendingWorkflowRunsForPrNumbersApplication'];
let maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication: WfMod['maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication'];

beforeAll(async () => {
  const mod = await import('../src/handlers/request/application/workflow-approval.js');
  maybeApprovePendingWorkflowRunsForPrNumbersApplication = mod.maybeApprovePendingWorkflowRunsForPrNumbersApplication;
  maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication =
    mod.maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication;
});

const repoInfo = { owner: 'org', repo: 'repo' };

function mkToStringTrim(): (v: unknown) => string {
  return (v: unknown): string => (v === null || v === undefined ? '' : typeof v === 'string' ? v.trim() : String(v));
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkCbs(overrides: Record<string, unknown> = {}) {
  return {
    isPullRequestOpen: jest.fn((pr: any) => pr?.state === 'open'),
    isSafeRegistryWorkflowApprovalFile: jest.fn(() => true),
    listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'data/ns/foo.yaml', status: 'modified' }])),
    parseLinkedIssueNumberFromPr: jest.fn(() => null),
    isSnapshotManagedRequestPr: jest.fn(() => false),
    evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
    hasAllowedStandaloneDirectPrApprovalForCurrentHead: jest.fn(() => Promise.resolve(false)),
    readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
    isPlainObject: jest.fn((v: unknown) => v !== null && typeof v === 'object' && !Array.isArray(v)),
    log: jest.fn(),
    getErrorMessage: jest.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
    getHttpStatus: jest.fn(() => undefined as number | undefined),
    toStringTrim: jest.fn(mkToStringTrim()),
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkCtx(requestImpl?: (route: string, args: Record<string, unknown>) => Promise<unknown>) {
  const impl =
    requestImpl ??
    ((): Promise<{ data: { workflow_runs: never[] } }> => Promise.resolve({ data: { workflow_runs: [] } }));
  return { octokit: { request: jest.fn(impl) } };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function openPr(sha = 'pr-sha-abc') {
  return { number: 1, state: 'open', draft: false, head: { sha, ref: 'feature' }, base: { ref: 'main' } };
}

// ── maybeApprovePendingWorkflowRunsForPrNumbersApplication ──────────────────

describe('maybeApprovePendingWorkflowRunsForPrNumbersApplication', () => {
  test('returns false immediately for empty prNumbers — covers line 566', async () => {
    const cbs = mkCbs();
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      [],
      'sha',
      'reason',
      cbs
    );
    expect(result).toBe(false);
    expect(cbs.readFreshPullRequest).not.toHaveBeenCalled();
  });

  test('returns false when fresh PR is null', async () => {
    const cbs = mkCbs({ readFreshPullRequest: jest.fn(() => Promise.resolve(null)) });
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      [1],
      'sha',
      'reason',
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when fresh PR is not open', async () => {
    const pr = { number: 1, state: 'closed', head: { sha: 'sha' }, draft: false };
    const cbs = mkCbs({ readFreshPullRequest: jest.fn(() => Promise.resolve(pr)) });
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      [1],
      'sha',
      'reason',
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when PR head sha does not match requested sha', async () => {
    const pr = { number: 1, state: 'open', head: { sha: 'other-sha' }, draft: false };
    const cbs = mkCbs({ readFreshPullRequest: jest.fn(() => Promise.resolve(pr)) });
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      [1],
      'target-sha',
      'reason',
      cbs
    );
    expect(result).toBe(false);
  });
});

// ── maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication ─────────

describe('maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication', () => {
  test('returns false when PR is a draft', async () => {
    const pr = { number: 1, state: 'open', draft: true, head: { sha: 'sha' }, base: { ref: 'main' } };
    const cbs = mkCbs({ isPullRequestOpen: jest.fn(() => true) });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      mkCtx(),
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(result).toBe(false);
  });

  test('catches and logs error from listWorkflowRunsForPullRequestHead — covers lines 175-189', async () => {
    // octokit.request throws → catch in listWorkflowRunsForPullRequestHead → logs runs-read-failed
    const ctx = {
      octokit: {
        request: jest.fn((): Promise<never> => Promise.reject(new Error('api down'))),
      },
    };
    const pr = openPr();
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      getErrorMessage: jest.fn(() => 'api down'),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:runs-read-failed')
    ).toBe(true);
    expect(result).toBe(false);
  });

  test('catches and logs error from approveWorkflowRun — covers lines 222-235', async () => {
    // GET runs succeeds with a waiting run, POST approve throws
    const ctx = mkCtx((route: string): Promise<unknown> => {
      if (route.includes('approve')) return Promise.reject(new Error('approval blocked'));
      return Promise.resolve({
        data: {
          workflow_runs: [{ id: 42, status: 'waiting', conclusion: null, head_sha: 'pr-sha-abc', pull_requests: [] }],
        },
      });
    });
    const pr = openPr('pr-sha-abc');
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      getErrorMessage: jest.fn(() => 'approval blocked'),
    });
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:approve-run-failed')
    ).toBe(true);
  });

  test('covers workflowRunTargetsPullRequest PR-number fallback — lines 137-138', async () => {
    // Run has different head_sha but matches via pull_requests[].number → included in filter
    const pr = openPr('pr-sha-abc');
    const ctx = mkCtx((route: string): Promise<unknown> => {
      if (route.includes('approve')) return Promise.resolve({});
      return Promise.resolve({
        data: {
          workflow_runs: [
            {
              id: 99,
              status: 'waiting',
              conclusion: null,
              head_sha: 'different-sha', // different from pr.head.sha
              pull_requests: [{ number: 1 }], // but matches pr.number → line 138 returns true
            },
          ],
        },
      });
    });
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
    });
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    // The run was found via PR number and processed (approved)
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:run-approved')
    ).toBe(true);
  });

  test('logs trust-check-failed when evaluateDirectPrOnApproval throws — covers lines 310-323', async () => {
    const pr = openPr();
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn((): Promise<never> => Promise.reject(new Error('trust check failed'))),
      getErrorMessage: jest.fn(() => 'trust check failed'),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      mkCtx(),
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:trust-check-failed')
    ).toBe(true);
    expect(result).toBe(false);
  });

  test('returns trusted when hasAllowedStandaloneDirectPrApprovalForCurrentHead is true — covers line 301', async () => {
    // decision.status !== 'approved' but hasCurrentHeadApproval is true → trusted:true, reason:'allowed-current-head-approval'
    const pr = openPr();
    const ctx = mkCtx((route: string): Promise<unknown> => {
      if (route.includes('approve')) return Promise.resolve({});
      return Promise.resolve({
        data: {
          workflow_runs: [{ id: 10, status: 'waiting', conclusion: null, head_sha: 'pr-sha-abc', pull_requests: [] }],
        },
      });
    });
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'pending' })),
      hasAllowedStandaloneDirectPrApprovalForCurrentHead: jest.fn(() => Promise.resolve(true)),
    });
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    // Should proceed to approve runs (trusted:true via allowed-current-head-approval)
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:run-approved')
    ).toBe(true);
  });

  test('logs not-safe-registry-only-pr when changed files are empty', async () => {
    const pr = openPr();
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      mkCtx(),
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'workflow-approval:skip-not-safe-registry-only-pr'
      )
    ).toBe(true);
    expect(result).toBe(false);
  });

  test('logs skip-missing-trust-signal when PR has linked issue (not-standalone-direct-pr)', async () => {
    // parseLinkedIssueNumberFromPr returns a number → not-standalone-direct-pr trust signal
    const pr = openPr();
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      parseLinkedIssueNumberFromPr: jest.fn(() => 42),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      mkCtx(),
      repoInfo,
      pr as any,
      'reason',
      cbs
    );
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'workflow-approval:skip-missing-trust-signal'
      )
    ).toBe(true);
    expect(result).toBe(false);
  });
});
