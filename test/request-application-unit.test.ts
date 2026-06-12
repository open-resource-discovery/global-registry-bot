/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';

// ---- branch-update-sequential-handoff ------------------------------------------
import { requestPullRequestBranchUpdateRespectingSequentialRegistryQueue } from '../src/handlers/request/application/branch-update-sequential-handoff.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkBranchUpdateCallbacks(overrides: Partial<Record<string, jest.Mock>> = {}) {
  return {
    isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(false)),
    requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(true)),
    getSequentialRegistryPrActive: jest.fn(() => null),
    markSequentialRegistryPrActive: jest.fn(),
    runOneSequentialDirectRegistryPrMaintenance: jest.fn(() => Promise.resolve({ updated: true })),
    ...overrides,
  } as any;
}

const ctx = {};
const repoInfo = { owner: 'o', repo: 'r' };

describe('requestPullRequestBranchUpdateRespectingSequentialRegistryQueue', () => {
  test('non-sequential PR: delegates directly to requestPullRequestBranchUpdate', async () => {
    const cbs = mkBranchUpdateCallbacks({ isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(false)) });
    const pr = { number: 1, base: { ref: 'main' } };
    const result = await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      ctx,
      repoInfo,
      pr,
      'main',
      'reason',
      cbs
    );
    expect(cbs.requestPullRequestBranchUpdate).toHaveBeenCalledTimes(1);
    expect(result).toBe(true);
  });

  test('sequential PR with no active state: runs maintenance and returns updated', async () => {
    // Lines 72-90 — the sequential path
    const cbs = mkBranchUpdateCallbacks({
      isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(true)),
      getSequentialRegistryPrActive: jest.fn(() => null),
      runOneSequentialDirectRegistryPrMaintenance: jest.fn(() => Promise.resolve({ updated: true })),
    });
    const pr = { number: 99, base: { ref: 'main' } };
    const result = await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      ctx,
      repoInfo,
      pr,
      'main',
      'reason',
      cbs
    );
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalled();
    expect(cbs.requestPullRequestBranchUpdate).not.toHaveBeenCalled();
    expect(result).toBe(true);
  });

  test('sequential PR with different active PR: runs maintenance', async () => {
    const cbs = mkBranchUpdateCallbacks({
      isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(true)),
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 55 })),
      runOneSequentialDirectRegistryPrMaintenance: jest.fn(() => Promise.resolve({ updated: false })),
    });
    const pr = { number: 99, base: { ref: 'main' } };
    const result = await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      ctx,
      repoInfo,
      pr,
      'main',
      'reason',
      cbs
    );
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalled();
    expect(result).toBe(false);
  });

  test('sequential PR where this PR is the active one: requests update and marks active', async () => {
    // Lines 74-82 — active PR matches current PR
    const cbs = mkBranchUpdateCallbacks({
      isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(true)),
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(true)),
      markSequentialRegistryPrActive: jest.fn(),
    });
    const pr = { number: 42, base: { ref: 'main' } };
    const result = await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      ctx,
      repoInfo,
      pr,
      'main',
      'reason',
      cbs
    );
    expect(cbs.requestPullRequestBranchUpdate).toHaveBeenCalled();
    expect(cbs.markSequentialRegistryPrActive).toHaveBeenCalled();
    expect(result).toBe(true);
  });

  test('sequential PR where this PR is active but update fails: does not mark active', async () => {
    const cbs = mkBranchUpdateCallbacks({
      isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(true)),
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(false)),
      markSequentialRegistryPrActive: jest.fn(),
    });
    const pr = { number: 42, base: { ref: 'main' } };
    const result = await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      ctx,
      repoInfo,
      pr,
      'main',
      'reason',
      cbs
    );
    expect(cbs.requestPullRequestBranchUpdate).toHaveBeenCalled();
    expect(cbs.markSequentialRegistryPrActive).not.toHaveBeenCalled();
    expect(result).toBe(false);
  });

  test('uses pr.base.ref when baseBranch arg is empty', async () => {
    const cbs = mkBranchUpdateCallbacks({ isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(false)) });
    const pr = { number: 1, base: { ref: 'develop' } };
    await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(ctx, repoInfo, pr, '', 'reason', cbs);
    expect(cbs.requestPullRequestBranchUpdate).toHaveBeenCalled();
  });
});

// ---- merge-approved-pr-or-update-branch ----------------------------------------
import { runMergeApprovedPrOrUpdateBranch } from '../src/handlers/request/application/merge-approved-pr-or-update-branch.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkMergeCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    waitForPullRequestMergeability: jest.fn((_ctx: any, _repo: any, pr: any) => Promise.resolve(pr)),
    shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(false)),
    requestPullRequestBranchUpdateRespectingSequentialRegistryQueue: jest.fn(() => Promise.resolve(true)),
    hasAutoApprovedPrHead: jest.fn(() => false),
    isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(true)),
    isCrossRepositoryPullRequest: jest.fn(() => false),
    evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
      Promise.resolve({
        green: true,
        reason: 'all-green',
        latestRuns: [],
        blockingRuns: [],
      })
    ),
    tryMergeIfGreen: jest.fn(() => Promise.resolve(true)),
    readFreshPullRequest: jest.fn((_ctx: any, _repo: any, prNumber: number) =>
      Promise.resolve({
        number: prNumber,
        state: 'closed',
        head: { sha: 'sha1' },
        base: { ref: 'main' },
        mergeable: true,
        mergeable_state: 'clean',
      })
    ),
    log: jest.fn(),
    getErrorMessage: jest.fn((e: unknown) => String(e)),
    getHttpStatus: jest.fn(() => undefined),
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function openPr(sha = 'sha1') {
  return { number: 10, state: 'open', head: { sha }, base: { ref: 'main' }, mergeable: true, mergeable_state: 'clean' };
}

describe('runMergeApprovedPrOrUpdateBranch', () => {
  test('returns early when PR is not open after mergeability wait', async () => {
    const closedPr = { number: 10, state: 'closed', head: { sha: 'sha1' }, base: { ref: 'main' } };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(closedPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, closedPr as any, 'reason', cbs);
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('logs and returns when head SHA changed before merge', async () => {
    // Covers lines 117-129
    const pr = { ...openPr('sha-original'), number: 10 };
    const changedPr = { ...pr, head: { sha: 'sha-new' }, state: 'open' };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(changedPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.objectContaining({ originalHeadSha: 'sha-original' }),
      'pull-request head changed before merge, waiting for new CI'
    );
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('logs and returns when PR is dirty (merge conflicts)', async () => {
    // Covers lines 133-144
    const dirtyPr = { ...openPr(), mergeable_state: 'dirty', mergeable: false };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(dirtyPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, dirtyPr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'pull-request has merge conflicts, auto-merge skipped'
    );
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('requests branch update when PR is behind', async () => {
    // Covers lines 147-155
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(true)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.requestPullRequestBranchUpdateRespectingSequentialRegistryQueue).toHaveBeenCalled();
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('skips merge when no approval', async () => {
    // Covers lines 168-181
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(false)),
      hasAutoApprovedPrHead: jest.fn(() => false),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge skipped: no qualifying approval'
    );
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('skips merge when head checks are not green', async () => {
    // Covers lines 187-201
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
        Promise.resolve({
          green: false,
          reason: 'failing-ci',
          latestRuns: [],
          blockingRuns: [],
        })
      ),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge skipped: current head checks are not green'
    );
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('skips merge when head checks still pending', async () => {
    // Covers lines 207-219
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
        Promise.resolve({
          green: true,
          reason: 'all-green',
          latestRuns: [{ status: 'in_progress' }],
          blockingRuns: [],
        })
      ),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge skipped: current head checks are still pending'
    );
    expect(cbs.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('merges successfully and returns when tryMergeIfGreen returns true', async () => {
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(true)),
      readFreshPullRequest: jest.fn(() => Promise.resolve({ ...pr, state: 'closed' })),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.tryMergeIfGreen).toHaveBeenCalled();
  });

  test('logs when tryMergeIfGreen returns false', async () => {
    // Covers lines 260-275 — merge returned false
    const pr = openPr();
    const freshPr = { ...pr, state: 'open' };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(false)),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge returned false, branch update not requested'
    );
  });

  test('head changed after merge attempt returns early', async () => {
    // Covers lines 243-255
    const pr = openPr('sha-before');
    const afterPr = { ...pr, state: 'open', head: { sha: 'sha-after' } };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(undefined)), // returns undefined = retry needed
      readFreshPullRequest: jest.fn(() => Promise.resolve(afterPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.objectContaining({ beforeHeadSha: 'sha-before' }),
      'pull-request head changed after merge attempt'
    );
  });

  test('returns null fresh PR early', async () => {
    const pr = openPr();
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(true)),
      readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    // No error thrown — null PR handled
    expect(cbs.tryMergeIfGreen).toHaveBeenCalled();
  });

  test('handles branch protection error by logging and returning', async () => {
    // Covers lines 330-345
    const pr = openPr();
    const branchProtectionError = new Error('protected branch hook declined');
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn((): Promise<never> => Promise.reject(branchProtectionError)),
      getErrorMessage: jest.fn(() => 'protected branch hook declined'),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge blocked by branch protection'
    );
  });

  test('handles outdated branch error by requesting branch update', async () => {
    // Covers lines 347-376: shouldUpdatePullRequestBranch returns false before merge,
    // then true after the outdated-branch error so we request a branch update
    const pr = openPr();
    const outdatedError = new Error('branch is out-of-date');
    const freshPr = { ...pr, state: 'open' };
    const shouldUpdateMock = jest.fn((): Promise<boolean> => Promise.resolve(false));
    shouldUpdateMock.mockResolvedValueOnce(false); // call 1 — before merge attempt
    shouldUpdateMock.mockResolvedValueOnce(true); // call 2 — after merge error
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      shouldUpdatePullRequestBranch: shouldUpdateMock,
      tryMergeIfGreen: jest.fn((): Promise<never> => Promise.reject(outdatedError)),
      getErrorMessage: jest.fn(() => 'branch is out-of-date'),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.requestPullRequestBranchUpdateRespectingSequentialRegistryQueue).toHaveBeenCalled();
  });

  test('handles outdated branch error when not behind — logs merge failed', async () => {
    const pr = openPr();
    const outdatedError = new Error('must be up to date before merging');
    const freshPr = { ...pr, state: 'open' };
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn((): Promise<never> => Promise.reject(outdatedError)),
      getErrorMessage: jest.fn(() => 'must be up to date before merging'),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
      shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(false)),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'pull-request merge failed, branch update not requested'
    );
  });

  test('rethrows unknown errors', async () => {
    const pr = openPr();
    const unknownError = new Error('Something completely unexpected');
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      tryMergeIfGreen: jest.fn((): Promise<never> => Promise.reject(unknownError)),
      getErrorMessage: jest.fn(() => 'Something completely unexpected'),
    });
    await expect(runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs)).rejects.toThrow(
      unknownError
    );
  });

  test('uses hasAutoApprovedPrHead when it returns true', async () => {
    const pr = openPr('sha-auto');
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(pr)),
      hasAutoApprovedPrHead: jest.fn(() => true),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(true)),
      readFreshPullRequest: jest.fn(() => Promise.resolve({ ...pr, state: 'closed' })),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    // hasAutoApprovedPrHead = true → skips isPullRequestApprovedForBranchMaintenance
    expect(cbs.isPullRequestApprovedForBranchMaintenance).not.toHaveBeenCalled();
    expect(cbs.tryMergeIfGreen).toHaveBeenCalled();
  });

  test('dirty PR after mergeability refresh is logged', async () => {
    // Covers lines 286-298
    const pr = openPr();
    let callCount = 0;
    const cbs = mkMergeCallbacks({
      waitForPullRequestMergeability: jest.fn((): Promise<unknown> => {
        callCount++;
        if (callCount === 1) return Promise.resolve(pr);
        return Promise.resolve({ ...pr, mergeable_state: 'dirty', mergeable: false });
      }),
      tryMergeIfGreen: jest.fn(() => Promise.resolve(undefined)), // returns undefined → retry mergeability
      readFreshPullRequest: jest.fn(() => Promise.resolve({ ...pr, state: 'open' })),
    });
    await runMergeApprovedPrOrUpdateBranch(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'pull-request has merge conflicts after mergeability refresh'
    );
  });
});

// ---- sequential-registry-pr-terminal -------------------------------------------
import {
  advanceSequentialRegistryPrQueueAfterTerminalState,
  handleBlockingRegistryHeadConclusion,
  releaseSequentialRegistryPrIfNotApprovedAfterGreen,
} from '../src/handlers/request/application/sequential-registry-pr-terminal.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkTerminalCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
    isPullRequestOpen: jest.fn((pr: any) => pr?.state === 'open'),
    getSequentialRegistryPrActive: jest.fn(() => null),
    clearSequentialRegistryPrActive: jest.fn(),
    markSequentialRegistryPrHeadSkipped: jest.fn(),
    listOpenPullRequests: jest.fn(() => Promise.resolve([])),
    pullRequestTargetsBranch: jest.fn(() => true),
    listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve([])),
    runOneSequentialDirectRegistryPrMaintenance: jest.fn(() => Promise.resolve({})),
    evaluateHeadGreenForApprovalReevaluation: jest.fn(() => Promise.resolve({ green: false, reason: 'not-green' })),
    isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
    log: jest.fn(),
    ...overrides,
  } as any;
}

describe('advanceSequentialRegistryPrQueueAfterTerminalState', () => {
  test('returns early when no active PR', async () => {
    const cbs = mkTerminalCallbacks({ getSequentialRegistryPrActive: jest.fn(() => null) });
    const pr = { number: 1, base: { ref: 'main' }, head: { sha: 'sha1' } };
    await advanceSequentialRegistryPrQueueAfterTerminalState(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.readFreshPullRequest).not.toHaveBeenCalled();
  });

  test('returns early when active PR does not match', async () => {
    const cbs = mkTerminalCallbacks({ getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 99 })) });
    const pr = { number: 1, base: { ref: 'main' }, head: { sha: 'sha1' } };
    await advanceSequentialRegistryPrQueueAfterTerminalState(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.readFreshPullRequest).not.toHaveBeenCalled();
  });

  test('returns early when fresh PR is still open — does not advance', async () => {
    // Covers lines 196-207 — PR is open, skip advancement
    const pr = { number: 42, state: 'closed', base: { ref: 'main' }, head: { sha: 'sha1' } };
    const freshOpenPr = { number: 42, state: 'open', base: { ref: 'main' }, head: { sha: 'sha1' } };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshOpenPr)),
      isPullRequestOpen: jest.fn((p: any) => p?.state === 'open'),
    });
    await advanceSequentialRegistryPrQueueAfterTerminalState(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.clearSequentialRegistryPrActive).not.toHaveBeenCalled();
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).not.toHaveBeenCalled();
  });

  test('clears active and runs maintenance when PR is closed', async () => {
    const pr = { number: 42, state: 'closed', base: { ref: 'main' }, head: { sha: 'sha1' } };
    const freshClosedPr = { number: 42, state: 'closed', base: { ref: 'main' } };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshClosedPr)),
      isPullRequestOpen: jest.fn(() => false),
    });
    await advanceSequentialRegistryPrQueueAfterTerminalState(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalled();
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalled();
  });

  test('clears active but skips maintenance when no base branch found', async () => {
    const pr = { number: 42, state: 'closed', base: { ref: '' }, head: { sha: 'sha1' } };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
      isPullRequestOpen: jest.fn(() => false),
    });
    await advanceSequentialRegistryPrQueueAfterTerminalState(ctx, repoInfo, pr as any, 'reason', cbs);
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalled();
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).not.toHaveBeenCalled();
  });
});

describe('handleBlockingRegistryHeadConclusion', () => {
  test('returns false when sha is empty', async () => {
    const cbs = mkTerminalCallbacks();
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, '', 'main', 'reason', cbs);
    expect(result).toBe(false);
  });

  test('marks failed PRs and runs maintenance', async () => {
    const openPrForHead = { number: 7, state: 'open', base: { ref: 'main' }, head: { sha: 'abc123' } };
    const cbs = mkTerminalCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([openPrForHead])),
      pullRequestTargetsBranch: jest.fn(() => true),
      listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve(['data/ns/foo.yaml'])),
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 7 })),
      clearSequentialRegistryPrActive: jest.fn(),
    });
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, 'abc123', 'main', 'reason', cbs);
    expect(result).toBe(true);
    expect(cbs.markSequentialRegistryPrHeadSkipped).toHaveBeenCalled();
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalled();
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalled();
  });

  test('logs blocking-head-not-marked when no PRs match', async () => {
    const cbs = mkTerminalCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([])),
    });
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, 'abc123', 'main', 'reason', cbs);
    expect(result).toBe(false);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'sequential-registry-pr:blocking-head-not-marked'
    );
  });

  test('logs advance-skipped when no base branch resolved', async () => {
    // Covers lines 259-272 — advance-skipped-missing-base-branch
    const openPrForHead = { number: 7, state: 'open', base: { ref: '' }, head: { sha: 'abc123' } };
    const cbs = mkTerminalCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([openPrForHead])),
      pullRequestTargetsBranch: jest.fn(() => true),
      listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve(['data/ns/foo.yaml'])),
      getSequentialRegistryPrActive: jest.fn(() => null),
    });
    // baseBranch is empty AND no PR base branch
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, 'abc123', '', 'reason', cbs);
    expect(result).toBe(true);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'sequential-registry-pr:advance-skipped-missing-base-branch'
    );
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).not.toHaveBeenCalled();
  });

  test('skips PRs not targeting the base branch', async () => {
    const pr = { number: 7, state: 'open', base: { ref: 'feature' }, head: { sha: 'abc123' } };
    const cbs = mkTerminalCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      pullRequestTargetsBranch: jest.fn(() => false),
    });
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, 'abc123', 'main', 'reason', cbs);
    expect(result).toBe(false);
  });

  test('skips PRs with no changed registry files', async () => {
    const pr = { number: 7, state: 'open', base: { ref: 'main' }, head: { sha: 'abc123' } };
    const cbs = mkTerminalCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      pullRequestTargetsBranch: jest.fn(() => true),
      listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve([])),
    });
    const result = await handleBlockingRegistryHeadConclusion(ctx, repoInfo, 'abc123', 'main', 'reason', cbs);
    expect(result).toBe(false);
  });
});

describe('releaseSequentialRegistryPrIfNotApprovedAfterGreen', () => {
  test('returns early when no active PR', async () => {
    const cbs = mkTerminalCallbacks({ getSequentialRegistryPrActive: jest.fn(() => null) });
    const pr = { number: 1, head: { sha: 'sha1' }, base: { ref: 'main' } };
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.readFreshPullRequest).not.toHaveBeenCalled();
  });

  test('clears active when fresh PR is closed', async () => {
    // Covers lines 307-309 — closed fresh PR clears active
    const pr = { number: 42, head: { sha: 'sha1' }, base: { ref: 'main' } };
    const closedFreshPr = { number: 42, state: 'closed', head: { sha: 'sha1' }, base: { ref: 'main' } };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(closedFreshPr)),
      isPullRequestOpen: jest.fn(() => false),
    });
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalled();
  });

  test('returns early when fresh PR has no head sha', async () => {
    const pr = { number: 42, head: { sha: '' }, base: { ref: 'main' } };
    const freshPr = { number: 42, state: 'open', head: { sha: '' }, base: { ref: 'main' } };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
      isPullRequestOpen: jest.fn(() => true),
    });
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.evaluateHeadGreenForApprovalReevaluation).not.toHaveBeenCalled();
  });

  test('returns early when PR is approved for maintenance', async () => {
    const pr = { number: 42, head: { sha: 'sha1' }, base: { ref: 'main' } };
    const freshPr = { ...pr, state: 'open' };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
      isPullRequestOpen: jest.fn(() => true),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(true)),
    });
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.evaluateHeadGreenForApprovalReevaluation).not.toHaveBeenCalled();
  });

  test('returns early when head is not green', async () => {
    // Covers line 317 — not green → return
    const pr = { number: 42, head: { sha: 'sha1' }, base: { ref: 'main' } };
    const freshPr = { ...pr, state: 'open' };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
      isPullRequestOpen: jest.fn(() => true),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() => Promise.resolve({ green: false, reason: 'failing' })),
    });
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.markSequentialRegistryPrHeadSkipped).not.toHaveBeenCalled();
  });

  test('marks skipped and advances queue when head is green but not approved', async () => {
    const pr = { number: 42, head: { sha: 'sha1' }, base: { ref: 'main' } };
    const freshPr = { ...pr, state: 'open' };
    const cbs = mkTerminalCallbacks({
      getSequentialRegistryPrActive: jest.fn(() => ({ prNumber: 42 })),
      readFreshPullRequest: jest.fn(() => Promise.resolve(freshPr)),
      isPullRequestOpen: jest.fn(() => true),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() => Promise.resolve({ green: true, reason: 'all-good' })),
    });
    await releaseSequentialRegistryPrIfNotApprovedAfterGreen(ctx, repoInfo, pr as any, cbs);
    expect(cbs.markSequentialRegistryPrHeadSkipped).toHaveBeenCalled();
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalled();
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalled();
  });
});

// ---- local-validation-checks (pure domain) ------------------------------------
import {
  applyRequiredFieldValidation,
  applySchemaIdentifierConsistencyCheck,
} from '../src/handlers/request/validation/local-validation-checks.js';

describe('applyRequiredFieldValidation', () => {
  const isEmpty = (v: unknown): boolean => !v;

  test('adds error when required field is missing and no issue-form sections in body', () => {
    // covers shouldEnforceRequiredTemplateField returning true via !hasAnyIssueFormSection path (line 53)
    const template = { body: [{ id: 'title', attributes: { label: 'Title' }, validations: { required: true } }] };
    const errors: string[] = [];
    applyRequiredFieldValidation({ template, formData: {}, issueBody: 'plain body', formBucket: [], errors, isEmpty });
    expect(errors.some((e) => e.includes('Title'))).toBe(true);
  });

  test('adds error when required field is missing and specific section IS present — covers lines 16-27, 59', () => {
    // issueBody has ### Title (issue-form section) AND 'Title' is the required field label
    // → hasAnyIssueFormSection=true → issueBodyHasTemplateFieldSection=true → enforce required
    const template = { body: [{ id: 'title', attributes: { label: 'Title' }, validations: { required: true } }] };
    const issueBody = '### Title\n\nsome content';
    const errors: string[] = [];
    applyRequiredFieldValidation({ template, formData: {}, issueBody, formBucket: [], errors, isEmpty });
    expect(errors.some((e) => e.includes('Title'))).toBe(true);
  });

  test('skips required enforcement when section is absent (backwards compat)', () => {
    // issueBody has issue-form sections but NOT the specific 'Title' section
    const template = { body: [{ id: 'title', attributes: { label: 'Title' }, validations: { required: true } }] };
    const issueBody = '### Other Section\n\nsome content';
    const errors: string[] = [];
    applyRequiredFieldValidation({ template, formData: {}, issueBody, formBucket: [], errors, isEmpty });
    expect(errors).toHaveLength(0);
  });

  test('skips field that is filled in formData', () => {
    const template = { body: [{ id: 'title', attributes: { label: 'Title' }, validations: { required: true } }] };
    const errors: string[] = [];
    applyRequiredFieldValidation({
      template,
      formData: { title: 'My Title' },
      issueBody: '',
      formBucket: [],
      errors,
      isEmpty,
    });
    expect(errors).toHaveLength(0);
  });

  test('skips field with no id', () => {
    const template = { body: [{ attributes: { label: 'Title' }, validations: { required: true } }] };
    const errors: string[] = [];
    applyRequiredFieldValidation({ template, formData: {}, issueBody: '', formBucket: [], errors, isEmpty });
    expect(errors).toHaveLength(0);
  });

  test('handles template with no body property (L70 || [] null arm)', () => {
    const errors: string[] = [];
    applyRequiredFieldValidation({ template: {}, formData: {}, issueBody: '', formBucket: [], errors, isEmpty });
    expect(errors).toHaveLength(0);
  });

  test('uses field.id as label when field has no attributes.label (L74 label-falsy arm)', () => {
    const template = { body: [{ id: 'myfield', validations: { required: true } }] };
    const errors: string[] = [];
    applyRequiredFieldValidation({ template, formData: {}, issueBody: 'plain body', formBucket: [], errors, isEmpty });
    expect(errors.some((e) => e.includes('myfield'))).toBe(true);
  });
});

describe('applySchemaIdentifierConsistencyCheck', () => {
  const isPlainObject = (v: unknown): v is Record<string, unknown> =>
    v !== null && typeof v === 'object' && !Array.isArray(v);
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  const getObjectProp = (obj: unknown, key: string) => {
    const val = (obj as Record<string, unknown>)?.[key];
    return isPlainObject(val) ? val : null;
  };

  test('adds error when schema has x-form-field=identifier but template has no identifier field', () => {
    const schemaObj = { properties: { id: { 'x-form-field': 'identifier' } } };
    const template = { body: [{ id: 'other', attributes: { label: 'Other' }, validations: { required: false } }] };
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template,
      schemaObj,
      schemaBucket: [],
      errors,
      isPlainObject,
      getObjectProp,
    });
    expect(errors.some((e) => e.includes('identifier'))).toBe(true);
  });

  test('no error when schema identifier field matches template identifier field', () => {
    const schemaObj = { properties: { id: { 'x-form-field': 'identifier' } } };
    const template = { body: [{ id: 'identifier', attributes: { label: 'ID' } }] };
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template,
      schemaObj,
      schemaBucket: [],
      errors,
      isPlainObject,
      getObjectProp,
    });
    expect(errors).toHaveLength(0);
  });

  test('no error when schema has no identifier x-form-field', () => {
    const schemaObj = { properties: { name: { 'x-form-field': 'name' } } };
    const template = { body: [] };
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template,
      schemaObj,
      schemaBucket: [],
      errors,
      isPlainObject,
      getObjectProp,
    });
    expect(errors).toHaveLength(0);
  });

  test('uses false for hasIdentifierFieldInTemplate when template.body is not an array (L97 false arm)', () => {
    const schemaObj = { properties: { id: { 'x-form-field': 'identifier' } } };
    const errors: string[] = [];
    applySchemaIdentifierConsistencyCheck({
      template: {},
      schemaObj,
      schemaBucket: [],
      errors,
      isPlainObject,
      getObjectProp,
    });
    expect(errors.some((e) => e.includes('identifier'))).toBe(true);
  });
});

// ---- default-branch-approved-pr-branch-update ---------------------------------
import { updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush } from '../src/handlers/request/application/default-branch-approved-pr-branch-update.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkDefaultBranchCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    isSequentialRegistryPrActiveBlocking: jest.fn(() => Promise.resolve(false)),
    listOpenPullRequests: jest.fn(() => Promise.resolve([])),
    isSequentialRegistryPrHeadSkipped: jest.fn(() => false),
    listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve(['data/ns/foo.yaml'])),
    isSnapshotManagedRequestPr: jest.fn(() => true),
    isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(true)),
    waitForPullRequestMergeability: jest.fn((_ctx: any, _ri: any, p: any) => Promise.resolve(p)),
    isPullRequestOpen: jest.fn(() => true),
    isPullRequestDirty: jest.fn(() => false),
    readMergeableState: jest.fn(() => 'clean'),
    shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(true)),
    requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(true)),
    markSequentialRegistryPrHeadSkipped: jest.fn(),
    getErrorMessage: jest.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
    log: jest.fn(),
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkOpenPr(num = 1) {
  return { number: num, head: { sha: `sha-${num}`, ref: 'main' }, base: { ref: 'main' } };
}

const defaultBranchCtx = {};
const defaultBranchRepoInfo = { owner: 'org', repo: 'repo' };

describe('updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush', () => {
  test('returns false immediately when sequential PR is blocking — covers line 94', async () => {
    const cbs = mkDefaultBranchCallbacks({
      isSequentialRegistryPrActiveBlocking: jest.fn(() => Promise.resolve(true)),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(result).toBe(false);
    expect(cbs.listOpenPullRequests).not.toHaveBeenCalled();
  });

  test('returns false when no open PRs', async () => {
    const cbs = mkDefaultBranchCallbacks({ listOpenPullRequests: jest.fn(() => Promise.resolve([])) });
    expect(
      await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
        defaultBranchCtx,
        defaultBranchRepoInfo,
        'main',
        cbs
      )
    ).toBe(false);
  });

  test('logs skip when PR has no changed registry files — covers lines 115-121', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      listChangedYamlFilesForPrWithFallback: jest.fn(() => Promise.resolve([])),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'skip branch update: no registry yaml files changed'
    );
    expect(result).toBe(false);
  });

  test('logs skip for direct registry PR handled by sequential queue — covers lines 125-134', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      isSnapshotManagedRequestPr: jest.fn(() => false), // not snapshot → sequential queue
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'skip branch update: direct registry PR handled by sequential queue'
    );
    expect(result).toBe(false);
  });

  test('logs skip when PR is not approved', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      isPullRequestApprovedForBranchMaintenance: jest.fn(() => Promise.resolve(false)),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'skip branch update: PR is not approved'
    );
    expect(result).toBe(false);
  });

  test('logs skip when fresh PR is not open', async () => {
    const pr = mkOpenPr(1);
    const closedPr = { ...pr, state: 'closed' };
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(closedPr)),
      isPullRequestOpen: jest.fn(() => false),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(result).toBe(false);
  });

  test('logs skip when PR has merge conflicts — covers lines 153-163', async () => {
    const pr = mkOpenPr(1);
    const dirtyPr = { ...pr, mergeable_state: 'dirty' };
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      waitForPullRequestMergeability: jest.fn(() => Promise.resolve(dirtyPr)),
      isPullRequestDirty: jest.fn(() => true),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'skip branch update: PR has merge conflicts'
    );
    expect(result).toBe(false);
  });

  test('logs skip when PR is not behind current base', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      shouldUpdatePullRequestBranch: jest.fn(() => Promise.resolve(false)),
    });
    await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'info',
      expect.anything(),
      'skip branch update: PR is not behind current base'
    );
  });

  test('returns true when branch update is requested', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(true)),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(result).toBe(true);
  });

  test('marks head skipped when requestPullRequestBranchUpdate returns false', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      requestPullRequestBranchUpdate: jest.fn(() => Promise.resolve(false)),
    });
    await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.markSequentialRegistryPrHeadSkipped).toHaveBeenCalled();
  });

  test('logs warning and marks skipped when exception thrown — covers lines 196-207', async () => {
    const pr = mkOpenPr(1);
    const cbs = mkDefaultBranchCallbacks({
      listOpenPullRequests: jest.fn(() => Promise.resolve([pr])),
      listChangedYamlFilesForPrWithFallback: jest.fn(
        (): Promise<never> => Promise.reject(new Error('unexpected error'))
      ),
      getErrorMessage: jest.fn(() => 'unexpected error'),
    });
    const result = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      defaultBranchCtx,
      defaultBranchRepoInfo,
      'main',
      cbs
    );
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'failed to update approved pull request branch after default branch push'
    );
    expect(cbs.markSequentialRegistryPrHeadSkipped).toHaveBeenCalled();
    expect(result).toBe(false);
  });
});

// ---- branch-update-decision ---------------------------------------------------
import {
  readBranchHeadSha,
  isPullRequestBehindCurrentBase,
  shouldUpdatePullRequestBranch,
} from '../src/handlers/request/application/branch-update-decision.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkBranchDecisionCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: '' } } })),
    compareCommitsWithBasehead: jest.fn(() => Promise.resolve({ data: { status: 'identical', ahead_by: 0 } })),
    log: jest.fn(),
    getErrorMessage: jest.fn((e: unknown) => (e instanceof Error ? e.message : String(e))),
    getHttpStatus: jest.fn(() => undefined as number | undefined),
    ...overrides,
  } as any;
}

const branchCtx = {};
const branchRepoInfo = { owner: 'org', repo: 'repo' };

describe('readBranchHeadSha', () => {
  test('returns empty string when branch name is empty', async () => {
    const cbs = mkBranchDecisionCallbacks();
    expect(await readBranchHeadSha(branchCtx, branchRepoInfo, '', cbs)).toBe('');
    expect(cbs.getBranch).not.toHaveBeenCalled();
  });

  test('returns sha from getBranch response', async () => {
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'abc123' } } })),
    });
    expect(await readBranchHeadSha(branchCtx, branchRepoInfo, 'main', cbs)).toBe('abc123');
  });

  test('returns empty and logs when getBranch throws', async () => {
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn((): Promise<never> => Promise.reject(new Error('not found'))),
      getErrorMessage: jest.fn(() => 'not found'),
    });
    const result = await readBranchHeadSha(branchCtx, branchRepoInfo, 'main', cbs);
    expect(result).toBe('');
    expect(cbs.log).toHaveBeenCalledWith(expect.anything(), 'warn', expect.anything(), 'branch-head-sha:read-failed');
  });
});

describe('isPullRequestBehindCurrentBase', () => {
  test('returns false when headSha is empty', async () => {
    const pr = { number: 1, head: { sha: '', ref: 'feature' }, base: { ref: 'main' } };
    const cbs = mkBranchDecisionCallbacks();
    expect(await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(false);
    expect(cbs.getBranch).not.toHaveBeenCalled();
  });

  test('returns false when baseHeadSha equals headSha', async () => {
    const pr = { number: 1, head: { sha: 'same', ref: 'feature' }, base: { ref: 'main' } };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'same' } } })),
    });
    expect(await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(false);
  });

  test('returns true when compare result is ahead (behind current base) — covers line 147', async () => {
    const pr = { number: 1, head: { sha: 'pr-sha', ref: 'feature' }, base: { ref: 'main' } };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'base-sha' } } })),
      compareCommitsWithBasehead: jest.fn(() => Promise.resolve({ data: { status: 'ahead', ahead_by: 1 } })),
    });
    expect(await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(true);
  });

  test('returns false when compare result is identical (not behind) — covers line 148', async () => {
    const pr = { number: 1, head: { sha: 'pr-sha', ref: 'feature' }, base: { ref: 'main' } };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'base-sha' } } })),
      compareCommitsWithBasehead: jest.fn(() => Promise.resolve({ data: { status: 'identical', ahead_by: 0 } })),
    });
    expect(await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(false);
  });

  test('logs warning and falls back when compareCommitsWithBasehead throws — covers lines 149-164', async () => {
    const pr = { number: 1, head: { sha: 'pr-sha', ref: 'feature' }, base: { ref: 'main' }, mergeable_state: null };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'base-sha' } } })),
      compareCommitsWithBasehead: jest.fn((): Promise<never> => Promise.reject(new Error('compare failed'))),
      getErrorMessage: jest.fn(() => 'compare failed'),
    });
    const result = await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs);
    expect(cbs.log).toHaveBeenCalledWith(
      expect.anything(),
      'warn',
      expect.anything(),
      'pull-request behind-current-base compare failed'
    );
    expect(result).toBe(false);
  });

  test('falls back to isPullRequestBehindBase when all compares return null status — covers line 167', async () => {
    // 'behind' status is unknown → isBehindCurrentBase = null → no early return → fallback
    const pr = { number: 1, head: { sha: 'pr-sha', ref: 'feature' }, base: { ref: 'main' }, mergeable_state: 'behind' };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'base-sha' } } })),
      compareCommitsWithBasehead: jest.fn(() => Promise.resolve({ data: { status: 'behind', ahead_by: 0 } })),
    });
    const result = await isPullRequestBehindCurrentBase(branchCtx, branchRepoInfo, pr as any, 'main', cbs);
    // isPullRequestBehindBase checks mergeable_state === 'behind' → true
    expect(result).toBe(true);
  });
});

describe('shouldUpdatePullRequestBranch', () => {
  test('returns true immediately when PR has behind mergeable_state — covers line 177', async () => {
    const pr = { number: 1, head: { sha: 'sha', ref: 'feature' }, base: { ref: 'main' }, mergeable_state: 'behind' };
    const cbs = mkBranchDecisionCallbacks();
    expect(await shouldUpdatePullRequestBranch(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(true);
    expect(cbs.getBranch).not.toHaveBeenCalled();
  });

  test('delegates to isPullRequestBehindCurrentBase when not obviously behind — covers line 178', async () => {
    const pr = { number: 1, head: { sha: 'pr-sha', ref: 'feature' }, base: { ref: 'main' }, mergeable_state: 'clean' };
    const cbs = mkBranchDecisionCallbacks({
      getBranch: jest.fn(() => Promise.resolve({ data: { commit: { sha: 'base-sha' } } })),
      compareCommitsWithBasehead: jest.fn(() => Promise.resolve({ data: { status: 'ahead', ahead_by: 1 } })),
    });
    expect(await shouldUpdatePullRequestBranch(branchCtx, branchRepoInfo, pr as any, 'main', cbs)).toBe(true);
    expect(cbs.getBranch).toHaveBeenCalled();
  });
});

// ---- branch-maintenance-approval -------------------------------------------
import { isPullRequestApprovedForBranchMaintenance } from '../src/handlers/request/application/branch-maintenance-approval.js';
import { markAutoApprovedPrHead } from '../src/handlers/request/application/auto-approved-head-tracking.js';

const maintenanceRepoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkMaintenanceCtx(reviews: { state?: string; body?: string; user?: { login: string } }[] = []) {
  return {
    octokit: {
      pulls: {
        listReviews: jest.fn(() => Promise.resolve({ data: reviews })),
      },
    },
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkMaintenanceCbs(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    hasApprovedLabelOnPr: jest.fn(() => Promise.resolve(false)),
    isSnapshotManagedRequestPr: jest.fn(() => false),
    ...overrides,
  } as any;
}

describe('isPullRequestApprovedForBranchMaintenance', () => {
  test('returns false when CHANGES_REQUESTED review exists', async () => {
    const reviews = [{ state: 'CHANGES_REQUESTED', body: '', user: { login: 'reviewer' } }];
    const ctx = mkMaintenanceCtx(reviews);
    const pr = { number: 1, head: { sha: 'sha1' }, body: '' };
    expect(
      await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, mkMaintenanceCbs())
    ).toBe(false);
  });

  test('returns true for snapshot managed PR — covers line 49', async () => {
    const ctx = mkMaintenanceCtx([]);
    const pr = { number: 2, head: { sha: 'sha2' }, body: '' };
    const cbs = mkMaintenanceCbs({ isSnapshotManagedRequestPr: jest.fn(() => true) });
    expect(await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, cbs)).toBe(true);
  });

  test('returns true when APPROVED review contains auto-approval marker — covers lines 54-62', async () => {
    const sha = 'approved-sha-marker-test';
    const marker = `<!-- nsreq:auto-approval:${sha} -->`;
    const reviews = [{ state: 'APPROVED', body: marker, user: { login: 'bot' } }];
    const ctx = mkMaintenanceCtx(reviews);
    const pr = { number: 3, head: { sha }, body: '' };
    expect(
      await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, mkMaintenanceCbs())
    ).toBe(true);
  });

  test('returns true when latest actionable review is APPROVED (no marker) — covers lines 64-65', async () => {
    const reviews = [{ state: 'APPROVED', body: 'lgtm', user: { login: 'reviewer' } }];
    const ctx = mkMaintenanceCtx(reviews);
    const pr = { number: 4, head: { sha: 'sha-no-marker' }, body: '' };
    expect(
      await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, mkMaintenanceCbs())
    ).toBe(true);
  });

  test('returns true when hasAutoApprovedPrHead is true — covers lines 68-70', async () => {
    const sha = 'auto-approved-sha-unique-test-abc';
    markAutoApprovedPrHead(maintenanceRepoInfo, 999, sha);
    const ctx = mkMaintenanceCtx([]);
    const pr = { number: 999, head: { sha }, body: '' };
    expect(
      await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, mkMaintenanceCbs())
    ).toBe(true);
  });

  test('returns true when hasApprovedLabelOnPr is true — covers lines 72-74', async () => {
    const ctx = mkMaintenanceCtx([]);
    const pr = { number: 5, head: { sha: 'sha-label' }, body: '' };
    const cbs = mkMaintenanceCbs({ hasApprovedLabelOnPr: jest.fn(() => Promise.resolve(true)) });
    expect(
      await isPullRequestApprovedForBranchMaintenance(
        ctx,
        maintenanceRepoInfo,
        pr as any,
        { allowLabelFallback: true },
        cbs
      )
    ).toBe(true);
  });

  test('returns false when no approval criteria met', async () => {
    const ctx = mkMaintenanceCtx([]);
    const pr = { number: 6, head: { sha: 'sha-none-approved' }, body: '' };
    expect(
      await isPullRequestApprovedForBranchMaintenance(ctx, maintenanceRepoInfo, pr as any, {}, mkMaintenanceCbs())
    ).toBe(false);
  });

  test('catches listPullRequestReviews errors and treats as empty reviews', async () => {
    const ctx = {
      octokit: {
        pulls: {
          listReviews: jest.fn((): Promise<never> => Promise.reject(new Error('api error'))),
        },
      },
    } as any;
    const pr = { number: 7, head: { sha: 'sha-err' }, body: '' };
    const result = await isPullRequestApprovedForBranchMaintenance(
      ctx,
      maintenanceRepoInfo,
      pr as any,
      {},
      mkMaintenanceCbs()
    );
    expect(result).toBe(false);
  });
});

// ---- auto-merge-trigger --------------------------------------------------------
import { runAutoMergeEvaluation, tryAutoMerge } from '../src/handlers/request/application/auto-merge-trigger.js';

const atCtx = {};
const atRepoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkAutoMergeTriggerCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    getStaticConfig: jest.fn(() => Promise.resolve({})),
    evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
      Promise.resolve({ green: true, reason: 'all-green', latestRuns: [], blockingRuns: [] })
    ),
    listOpenPullRequests: jest.fn(() => Promise.resolve([])),
    processPullRequestForAutoMerge: jest.fn(() => Promise.resolve()),
    releaseSequentialRegistryPrIfNotApprovedAfterGreen: jest.fn(() => Promise.resolve()),
    advanceSequentialRegistryPrQueueAfterTerminalState: jest.fn(() => Promise.resolve()),
    readFreshPullRequest: jest.fn(() => Promise.resolve(null)),
    isSequentialDirectRegistryPr: jest.fn(() => Promise.resolve(false)),
    getSequentialRegistryPrActive: jest.fn(() => null),
    clearSequentialRegistryPrActive: jest.fn(),
    markSequentialRegistryPrHeadSkipped: jest.fn(),
    runOneSequentialDirectRegistryPrMaintenance: jest.fn(() => Promise.resolve({})),
    log: jest.fn(),
    ...overrides,
  } as any;
}

describe('runAutoMergeEvaluation — deferred vs terminal', () => {
  test('returns false when checks are still running (check-runs-not-completed) — deferred', async () => {
    const cbs = mkAutoMergeTriggerCallbacks({
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
        Promise.resolve({
          green: false,
          reason: 'check-runs-not-completed',
          latestRuns: [{ status: 'in_progress' }],
          blockingRuns: [{ status: 'in_progress' }],
        })
      ),
    });
    const result = await runAutoMergeEvaluation(atCtx, atRepoInfo, 'sha-deferred', cbs);
    expect(result).toBe(false);
    expect(cbs.listOpenPullRequests).not.toHaveBeenCalled();
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'auto-merge:evaluation deferred: checks still running'
      )
    ).toBe(true);
  });

  test('returns true when not-green for a reason other than checks-not-completed — terminal', async () => {
    const cbs = mkAutoMergeTriggerCallbacks({
      evaluateHeadGreenForApprovalReevaluation: jest.fn(() =>
        Promise.resolve({ green: false, reason: 'status-not-green', latestRuns: [], blockingRuns: [] })
      ),
    });
    const result = await runAutoMergeEvaluation(atCtx, atRepoInfo, 'sha-not-green', cbs);
    expect(result).toBe(true);
  });

  test('returns true when green and candidates processed — terminal', async () => {
    const cbs = mkAutoMergeTriggerCallbacks();
    const result = await runAutoMergeEvaluation(atCtx, atRepoInfo, 'sha-green', cbs);
    expect(result).toBe(true);
  });
});

describe('tryAutoMerge — recently-completed dedup', () => {
  test('deferred evaluation does not block a subsequent call for the same SHA', async () => {
    const cbs = mkAutoMergeTriggerCallbacks();
    const deferredFn = jest.fn((): Promise<boolean> => Promise.resolve(false));
    const terminalFn = jest.fn((): Promise<boolean> => Promise.resolve(true));
    const sha = `sha-at-deferred-${Date.now()}`;

    await tryAutoMerge(atCtx, atRepoInfo, sha, deferredFn, cbs);
    await tryAutoMerge(atCtx, atRepoInfo, sha, terminalFn, cbs);

    expect(deferredFn).toHaveBeenCalledTimes(1);
    expect(terminalFn).toHaveBeenCalledTimes(1);
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'auto-merge:evaluation skipped: recently completed'
      )
    ).toBe(false);
  });

  test('terminal evaluation blocks a subsequent call for the same SHA', async () => {
    const cbs = mkAutoMergeTriggerCallbacks();
    const terminalFn = jest.fn((): Promise<boolean> => Promise.resolve(true));
    const sha = `sha-at-terminal-${Date.now()}`;

    await tryAutoMerge(atCtx, atRepoInfo, sha, terminalFn, cbs);
    await tryAutoMerge(atCtx, atRepoInfo, sha, terminalFn, cbs);

    expect(terminalFn).toHaveBeenCalledTimes(1);
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'auto-merge:evaluation skipped: recently completed'
      )
    ).toBe(true);
  });

  test('failed evaluation does not mark SHA as recently completed', async () => {
    const sha = `sha-error-${Date.now()}`;
    const cbs = mkAutoMergeTriggerCallbacks();
    const failingFn = jest.fn((): Promise<boolean> => Promise.reject(new Error('eval-boom')));
    const terminalFn = jest.fn((): Promise<boolean> => Promise.resolve(true));

    await expect(tryAutoMerge(atCtx, atRepoInfo, sha, failingFn, cbs)).rejects.toThrow('eval-boom');

    // SHA must NOT be recently-completed — the next call must actually run
    await tryAutoMerge(atCtx, atRepoInfo, sha, terminalFn, cbs);
    expect(terminalFn).toHaveBeenCalledTimes(1);
  });
});

// ---- check-completed-handler ---------------------------------------------------
import { handleCheckCompletedEvent } from '../src/handlers/request/application/check-completed-handler.js';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkCheckCompletedCallbacks(overrides: Partial<Record<string, jest.Mock | any>> = {}) {
  return {
    readCheckRunFromPayload: jest.fn(() => null),
    readCheckSuiteFromPayload: jest.fn(() => null),
    readRepoInfoFromPayload: jest.fn(() => ({ owner: 'org', repo: 'repo' })),
    readCheckRunPrNumbers: jest.fn((): number[] => []),
    resolveCheckSuitePrNumbers: jest.fn((): Promise<number[]> => Promise.resolve([])),
    readCheckSuiteId: jest.fn(() => null),
    listAllCheckRunsForSuite: jest.fn(() => Promise.resolve([])),
    readCheckRunId: jest.fn(() => null),
    readFirstRegistryValidationArtifactsForSuiteRuns: jest.fn(() => Promise.resolve(null)),
    readPullRequestHtmlUrl: jest.fn(() => Promise.resolve('')),
    collapseBotCommentsByPrefix: jest.fn(() => Promise.resolve()),
    postCheckSuiteRegistryValidationComments: jest.fn(() => Promise.resolve()),
    maybeHandleDefaultBranchCheckSuiteSuccess: jest.fn(() => Promise.resolve()),
    tryAutoMerge: jest.fn(() => Promise.resolve()),
    maybeApprovePendingWorkflowRunsForPrNumbers: jest.fn(() => Promise.resolve(false)),
    handleBlockingRegistryHeadConclusion: jest.fn(() => Promise.resolve(false)),
    isBlockingCheckConclusion: jest.fn((): boolean => false),
    readDefaultBranchFromPayload: jest.fn((): string => 'main'),
    getStaticConfig: jest.fn(() => Promise.resolve({})),
    log: jest.fn(),
    isDebugEnabled: false,
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkCheckRunPayload(sha = 'sha-abc', conclusion = 'success') {
  return {
    action: 'completed',
    check_run: { id: 1, status: 'completed', conclusion, head_sha: sha, pull_requests: [] },
    repository: { owner: { login: 'org' }, name: 'repo' },
  } as any;
}

describe('handleCheckCompletedEvent — check_run.completed with no PR mapping', () => {
  test('skips tryAutoMerge and logs deferred when prNumbers is empty — cross-repo PR scenario', async () => {
    const run = { id: 1, status: 'completed', conclusion: 'success', head_sha: 'sha-no-pr', pull_requests: [] };
    const cbs = mkCheckCompletedCallbacks({
      readCheckRunFromPayload: jest.fn(() => run),
      readCheckRunPrNumbers: jest.fn((): number[] => []),
    });
    await handleCheckCompletedEvent({}, mkCheckRunPayload('sha-no-pr'), 'check_run.completed', cbs);
    expect(cbs.tryAutoMerge).not.toHaveBeenCalled();
    expect(
      (cbs.log as jest.Mock).mock.calls.some(
        ([, , , msg]: unknown[]) => msg === 'checks:check-run-deferred-no-pr-mapping'
      )
    ).toBe(true);
  });

  test('calls tryAutoMerge when check_run has a PR mapping', async () => {
    const run = {
      id: 1,
      status: 'completed',
      conclusion: 'success',
      head_sha: 'sha-with-pr',
      pull_requests: [{ number: 42 }],
    };
    const cbs = mkCheckCompletedCallbacks({
      readCheckRunFromPayload: jest.fn(() => run),
      readCheckRunPrNumbers: jest.fn((): number[] => [42]),
    });
    await handleCheckCompletedEvent({}, mkCheckRunPayload('sha-with-pr'), 'check_run.completed', cbs);
    expect(cbs.tryAutoMerge).toHaveBeenCalledWith({}, { owner: 'org', repo: 'repo' }, 'sha-with-pr');
  });

  test('check_suite.completed with prNumbers still calls tryAutoMerge', async () => {
    const suite = { id: 9, status: 'completed', conclusion: 'success', head_sha: 'sha-suite', head_branch: 'main' };
    const cbs = mkCheckCompletedCallbacks({
      readCheckSuiteFromPayload: jest.fn(() => suite),
      resolveCheckSuitePrNumbers: jest.fn((): Promise<number[]> => Promise.resolve([599])),
    });
    const payload = {
      action: 'completed',
      check_suite: suite,
      repository: { owner: { login: 'org' }, name: 'repo' },
    };
    await handleCheckCompletedEvent({}, payload, 'check_suite.completed', cbs);
    expect(cbs.tryAutoMerge).toHaveBeenCalledWith({}, { owner: 'org', repo: 'repo' }, 'sha-suite');
  });

  test('L312 arm0 + L320 arm0: isDebugEnabled=true emits debug logs in postCheckSuiteRegistryValidationCommentsIfPresent', async () => {
    // non-success conclusion → skips success branch → postCheckSuiteRegistryValidationCommentsIfPresent
    // isBlockingCheckConclusion returns false → goes straight to postCheckSuiteRegistryValidationCommentsIfPresent
    // readCheckSuiteId='42' (non-null) + prNumbers=[1] → enters the function body
    // isDebugEnabled=true → L312 arm0 + L320 arm0 both log 'debug'
    const suite = { id: 42, status: 'completed', conclusion: 'neutral', head_sha: 'sha-debug', head_branch: 'main' };
    const cbs = mkCheckCompletedCallbacks({
      readCheckSuiteFromPayload: jest.fn(() => suite),
      resolveCheckSuitePrNumbers: jest.fn((): Promise<number[]> => Promise.resolve([1])),
      readCheckSuiteId: jest.fn((): number => 42),
      isBlockingCheckConclusion: jest.fn((): boolean => false),
      listAllCheckRunsForSuite: jest.fn(
        (): Promise<{ id: number; conclusion: string; html_url: string }[]> =>
          Promise.resolve([{ id: 7, conclusion: 'neutral', html_url: 'https://github.com/run/7' }])
      ),
      readCheckRunId: jest.fn((r: any) => r.id),
      isDebugEnabled: true,
    });
    const payload = {
      action: 'completed',
      check_suite: suite,
      repository: { owner: { login: 'org' }, name: 'repo' },
    };
    await handleCheckCompletedEvent({}, payload, 'check_suite.completed', cbs);
    const logCalls = (cbs.log as jest.Mock).mock.calls;
    const debugMsgs = logCalls.filter(([, level]: unknown[]) => level === 'debug').map(([, , , msg]: unknown[]) => msg);
    expect(debugMsgs).toContain('dbg:checks:failure suite');
    expect(debugMsgs).toContain('dbg:checks:runs listed for suite');
  });
});
