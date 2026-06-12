/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest, beforeAll, beforeEach, afterEach } from '@jest/globals';

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

// ── Additional branch coverage ─────────────────────────────────────────────

describe('workflow-approval — additional branch coverage', () => {
  test('L137: non-array pull_requests → false arm, run excluded from filter', async () => {
    // Run has a different headSha AND pull_requests=null → prs=[] → filtered out → no waiting runs
    const pr = openPr('sha-target-L137');
    const ctx = mkCtx(() =>
      Promise.resolve({
        data: {
          workflow_runs: [{ id: 5, status: 'waiting', head_sha: 'sha-other', pull_requests: null }],
        },
      })
    );
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      { owner: 'orgL137', repo: 'repoL137' },
      pr as any,
      'test',
      cbs
    );
    expect(result).toBe(false);
    expect(
      (cbs.log as jest.Mock).mock.calls.some(([, , , msg]: unknown[]) => msg === 'workflow-approval:no-waiting-runs')
    ).toBe(true);
  });

  test('L154: empty headSha → listWorkflowRunsForPullRequestHead returns early (no request)', async () => {
    const pr = { number: 51, state: 'open', draft: false, head: { sha: '' }, base: { ref: 'main' } };
    const request = jest.fn();
    const ctx = { octokit: { request } };
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      { owner: 'orgL154', repo: 'repoL154' },
      pr as any,
      'test',
      cbs
    );
    expect(request).not.toHaveBeenCalled();
  });

  test('L170: non-object response data → data={}, workflow_runs=[]', async () => {
    const pr = openPr('sha-L170');
    const ctx = mkCtx(() => Promise.resolve({ data: 'not-an-object' }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      { owner: 'orgL170', repo: 'repoL170' },
      pr as any,
      'test',
      cbs
    );
    expect(result).toBe(false);
  });

  test('L171: non-array workflow_runs → runs=[]', async () => {
    const pr = openPr('sha-L171');
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: 'bad' } }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      { owner: 'orgL171', repo: 'repoL171' },
      pr as any,
      'test',
      cbs
    );
    expect(result).toBe(false);
  });

  test('L306: empty decision status → || "none" fallback in reason string', async () => {
    const pr = openPr('sha-L306');
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: '' })),
      hasAllowedStandaloneDirectPrApprovalForCurrentHead: jest.fn(() => Promise.resolve(false)),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      mkCtx(),
      { owner: 'orgL306', repo: 'repoL306' },
      pr as any,
      'test',
      cbs
    );
    expect(result).toBe(false);
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'workflow-approval:skip-missing-trust-signal'
      )
    ).toBe(true);
  });

  test('L395-396: waiting run with non-numeric id is skipped (runId=0, continue)', async () => {
    const pr = openPr('sha-L395');
    const ctx = mkCtx((route: string) => {
      if (route.includes('approve')) return Promise.resolve({});
      return Promise.resolve({
        data: { workflow_runs: [{ id: 'not-a-number', status: 'waiting', head_sha: 'sha-L395' }] },
      });
    });
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
    });
    const result = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      { owner: 'orgL395', repo: 'repoL395' },
      pr as any,
      'test',
      cbs
    );
    expect(result).toBe(false);
  });

  test('L547: null prNumbers → || [] fallback, loop skipped', async () => {
    const cbs = mkCbs();
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      null as any,
      'sha',
      'reason',
      cbs
    );
    expect(result).toBe(false);
    expect(cbs.readFreshPullRequest as jest.Mock).not.toHaveBeenCalled();
  });

  test('L563: approved=false in loop (PR open, sha matches, but nothing to approve)', async () => {
    const pr = { number: 3, state: 'open', draft: false, head: { sha: 'sha-match' }, base: { ref: 'main' } };
    const cbs = mkCbs({
      readFreshPullRequest: jest.fn(() => Promise.resolve(pr)),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([])), // empty → not safe → false
    });
    const result = await maybeApprovePendingWorkflowRunsForPrNumbersApplication(
      mkCtx(),
      repoInfo,
      [3],
      'sha-match',
      'reason',
      cbs
    );
    expect(result).toBe(false);
  });
});

// ── Timer-based branch coverage ──────────────────────────────────────────────

describe('scheduleWorkflowApprovalRetry — timer-based branches', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });
  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
  });

  test('L436: inflight key already set → early return, no second timer scheduled', async () => {
    const uniquePr = { ...openPr('sha-inflight-400'), number: 400 };
    const ctx = mkCtx();
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(uniquePr)),
    });
    const ri = { owner: 'orgL436', repo: 'repoL436' };
    // First call: schedules attempt 0 timer
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    // Second call: attempt 0 key is still inflight → L436 return
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    const scheduled = (cbs.log as jest.Mock).mock.calls.filter(
      ([, , , msg]: unknown[]) => msg === 'workflow-approval:retry-scheduled'
    );
    expect(scheduled.length).toBe(1);
  });

  test('L443 arm 1: readFreshPullRequest returns null → || pr fallback', async () => {
    const uniquePr = { ...openPr('sha-timer-401'), number: 401 };
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: [] } }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
    });
    const ri = { owner: 'orgL443', repo: 'repoL443' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000);
    expect(cbs.readFreshPullRequest as jest.Mock).toHaveBeenCalled();
  });

  test('L446 arm 0: timer fires, freshPr is closed → early return', async () => {
    const uniquePr = { ...openPr('sha-timer-402'), number: 402 };
    const closedPr = { ...uniquePr, state: 'closed' };
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: [] } }));
    const cbs = mkCbs({
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(closedPr)),
    });
    const ri = { owner: 'orgL446', repo: 'repoL446' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000);
    // closed PR causes L446 arm 0 (isPullRequestOpen=false → return)
    expect(cbs.readFreshPullRequest as jest.Mock).toHaveBeenCalled();
  });

  test('L447 arm 0: timer fires, sha changed → early return', async () => {
    const uniquePr = { ...openPr('sha-original-403'), number: 403 };
    const freshPr = { ...uniquePr, head: { sha: 'sha-changed-403', ref: 'feature' } };
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: [] } }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
    });
    const ri = { owner: 'orgL447', repo: 'repoL447' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000);
    // originalSha=sha-original-403, freshSha=sha-changed-403 → L447 arm 0 (return)
    expect(cbs.readFreshPullRequest as jest.Mock).toHaveBeenCalled();
  });

  test('L447 binary arm 1: freshHeadSha empty → short-circuit at second operand', async () => {
    const uniquePr = { ...openPr('sha-original-404x'), number: 4040 };
    const freshPr = { ...uniquePr, head: { sha: '', ref: 'feature' } };
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: [] } }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
    });
    const ri = { owner: 'orgL447b', repo: 'repoL447b' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000);
    // freshSha='' → L447 binary arm 1 (second operand falsy → condition false → continue)
    expect(cbs.readFreshPullRequest as jest.Mock).toHaveBeenCalled();
  });

  test('L457 arm 1: retry succeeds (approved=true), no further rescheduling', async () => {
    const uniquePr = { ...openPr('sha-timer-404'), number: 404 };
    let requestCallCount = 0;
    const request = jest.fn().mockImplementation((route: string) => {
      if (route.includes('approve')) return Promise.resolve({});
      requestCallCount++;
      if (requestCallCount === 1) return Promise.resolve({ data: { workflow_runs: [] } });
      return Promise.resolve({
        data: { workflow_runs: [{ id: 10, status: 'waiting', head_sha: 'sha-timer-404', pull_requests: [] }] },
      });
    });
    const ctx = { octokit: { request } };
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(uniquePr)),
    });
    const ri = { owner: 'orgL457', repo: 'repoL457' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000);
    // Retry found a waiting run and approved it → L457 arm 1 (approved=true → no further reschedule)
    const approvedMsgs = (cbs.log as jest.Mock).mock.calls.filter(
      ([, , , msg]: unknown[]) => msg === 'workflow-approval:run-approved'
    );
    expect(approvedMsgs.length).toBeGreaterThan(0);
  });

  test('L433: delay undefined after max retries (attempt >= retry list length)', async () => {
    const uniquePr = { ...openPr('sha-timer-405'), number: 405 };
    const ctx = mkCtx(() => Promise.resolve({ data: { workflow_runs: [] } }));
    const cbs = mkCbs({
      isPullRequestOpen: jest.fn(() => true),
      evaluateDirectPrOnApproval: jest.fn(() => Promise.resolve({ status: 'approved' })),
      listChangedFilesForPr: jest.fn(() => Promise.resolve([{ filename: 'a.yaml' }])),
      readFreshPullRequest: jest.fn(() => Promise.resolve(uniquePr)),
    });
    const ri = { owner: 'orgL433', repo: 'repoL433' };
    await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      ctx as any,
      ri,
      uniquePr as any,
      'test',
      cbs
    );
    await jest.advanceTimersByTimeAsync(11000); // fires attempt 0
    await jest.advanceTimersByTimeAsync(31000); // fires attempt 1 → schedules attempt 2 → delay undefined → L433
    // No more timers; test succeeds without hanging
  });
});
