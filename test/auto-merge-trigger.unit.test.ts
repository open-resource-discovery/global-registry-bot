/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import { runAutoMergeEvaluation, tryAutoMerge } from '../src/handlers/request/application/auto-merge-trigger.js';

// GateGuard facts:
// (1) No files call this — Jest auto-discovers via testMatch;
// (2) No existing file serves the same purpose — Glob returned no matches;
// (3) No data files — all synthetic jest.fn() mocks;
// (4) "proceed, the goal is everything at least on 90%. coverageThreshold: {...}"

const repoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeGreenResult(overrides: Record<string, unknown> = {}) {
  return {
    green: true,
    reason: 'ok',
    latestRuns: [],
    blockingRuns: [],
    statusState: 'success',
    ...overrides,
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    getStaticConfig: jest.fn().mockResolvedValue({}),
    evaluateHeadGreenForApprovalReevaluation: jest.fn().mockResolvedValue(makeGreenResult()),
    listOpenPullRequests: jest.fn().mockResolvedValue([]),
    processPullRequestForAutoMerge: jest.fn().mockResolvedValue(undefined),
    releaseSequentialRegistryPrIfNotApprovedAfterGreen: jest.fn().mockResolvedValue(undefined),
    advanceSequentialRegistryPrQueueAfterTerminalState: jest.fn().mockResolvedValue(undefined),
    readFreshPullRequest: jest.fn().mockResolvedValue(null),
    isSequentialDirectRegistryPr: jest.fn().mockResolvedValue(false),
    getSequentialRegistryPrActive: jest.fn().mockReturnValue(null),
    clearSequentialRegistryPrActive: jest.fn(),
    markSequentialRegistryPrHeadSkipped: jest.fn(),
    runOneSequentialDirectRegistryPrMaintenance: jest.fn().mockResolvedValue(undefined),
    log: jest.fn(),
    ...overrides,
  } as any;
}

describe('tryAutoMerge', () => {
  test('L259 arm0: returns immediately when headSha is empty string', async () => {
    const cbs = makeCallbacks();
    const evalFn = jest.fn<() => Promise<boolean>>().mockResolvedValue(true);

    await tryAutoMerge({}, repoInfo, '', evalFn as any, cbs);

    expect(cbs.log).toHaveBeenCalledWith(
      {},
      'info',
      expect.objectContaining({ owner: 'org', repo: 'repo' }),
      'auto-merge:skip-missing-head-sha'
    );
    expect(evalFn).not.toHaveBeenCalled();
  });

  test('L259 arm0: returns immediately when headSha is whitespace-only', async () => {
    const cbs = makeCallbacks();
    const evalFn = jest.fn<() => Promise<boolean>>().mockResolvedValue(true);

    await tryAutoMerge({}, repoInfo, '   ', evalFn as any, cbs);

    expect(cbs.log).toHaveBeenCalledWith({}, 'info', expect.any(Object), 'auto-merge:skip-missing-head-sha');
    expect(evalFn).not.toHaveBeenCalled();
  });
});

describe('runAutoMergeEvaluation — catch block with sequential direct registry PR', () => {
  test('L203 arm0: isSequentialDirectRegistry=true → proceeds past continue to markSkipped', async () => {
    // greenResult is green, one candidate PR that throws during processing.
    // freshPr has base.ref='main' → baseBranch='main' → isSequentialDirectRegistryPr returns true.
    // → !isSequentialDirectRegistry = false → does NOT continue (L203 arm0 covered).
    const candidatePr = { number: 42, head: { sha: 'sha-trigger-42' }, base: { ref: 'main' } };
    const freshPr = { number: 42, head: { sha: 'sha-trigger-42' }, base: { ref: 'main' } };

    const cbs = makeCallbacks({
      evaluateHeadGreenForApprovalReevaluation: jest.fn().mockResolvedValue(makeGreenResult()),
      listOpenPullRequests: jest.fn().mockResolvedValue([candidatePr]),
      processPullRequestForAutoMerge: jest.fn().mockRejectedValue(new Error('process failed')),
      readFreshPullRequest: jest.fn().mockResolvedValue(freshPr),
      isSequentialDirectRegistryPr: jest.fn().mockResolvedValue(true),
      getSequentialRegistryPrActive: jest.fn().mockReturnValue(null),
    });

    const result = await runAutoMergeEvaluation({}, repoInfo, 'sha-trigger-42', cbs);

    expect(result).toBe(true);
    expect(cbs.markSequentialRegistryPrHeadSkipped).toHaveBeenCalledWith(
      {},
      repoInfo,
      freshPr,
      'auto-merge-candidate-processing-failed'
    );
    expect(cbs.clearSequentialRegistryPrActive).not.toHaveBeenCalled();
  });

  test('L217 arm0 + L218: wasActiveSequentialPr=true with baseBranch → clearActive + runMaintenance', async () => {
    const candidatePr = { number: 99, head: { sha: 'sha-active-99' }, base: { ref: 'release' } };
    const freshPr = { number: 99, head: { sha: 'sha-active-99' }, base: { ref: 'release' } };

    const cbs = makeCallbacks({
      evaluateHeadGreenForApprovalReevaluation: jest.fn().mockResolvedValue(makeGreenResult()),
      listOpenPullRequests: jest.fn().mockResolvedValue([candidatePr]),
      processPullRequestForAutoMerge: jest.fn().mockRejectedValue(new Error('fail')),
      readFreshPullRequest: jest.fn().mockResolvedValue(freshPr),
      isSequentialDirectRegistryPr: jest.fn().mockResolvedValue(true),
      getSequentialRegistryPrActive: jest.fn().mockReturnValue({ prNumber: 99 }),
    });

    const result = await runAutoMergeEvaluation({}, repoInfo, 'sha-active-99', cbs);

    expect(result).toBe(true);
    expect(cbs.clearSequentialRegistryPrActive).toHaveBeenCalledWith(repoInfo);
    expect(cbs.runOneSequentialDirectRegistryPrMaintenance).toHaveBeenCalledWith(
      {},
      repoInfo,
      'release',
      'sequential-direct-pr:advance-after-processing-failure'
    );
  });
});
