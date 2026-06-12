/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
import { describe, test, expect, jest } from '@jest/globals';
import { evaluateHeadGreenForApprovalReevaluation } from '../src/handlers/request/application/head-green-evaluation.js';

const repoInfo = { owner: 'org', repo: 'repo' };

function makeCallbacks() {
  return {
    isPlainObject: (value: unknown): value is Record<string, unknown> =>
      typeof value === 'object' && value !== null && !Array.isArray(value),
    getErrorMessage: (error: unknown) => (error instanceof Error ? error.message : String(error)),
    getHttpStatus: (_error: unknown) => undefined as number | undefined,
    logCheckRunsFetchFailed: jest.fn(),
  };
}

function makeContext(
  listForRefFn: (args: unknown) => Promise<unknown>,
  getCombinedStatusFn: (args: unknown) => Promise<unknown>
) {
  return {
    octokit: {
      checks: { listForRef: listForRefFn },
      repos: { getCombinedStatusForRef: getCombinedStatusFn },
    },
  };
}

describe('evaluateHeadGreenForApprovalReevaluation', () => {
  test('returns missing-head-sha when headSha is empty', async () => {
    const ctx = makeContext(jest.fn(), jest.fn());
    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, '', makeCallbacks());
    expect(result.green).toBe(false);
    expect(result.reason).toBe('missing-head-sha');
  });

  test('falls back to getCombinedStatusForRef when listForRef returns non-object data', async () => {
    // L88 cond-expr arm1: data is null → runs = [] → map empty → getCombinedStatus called
    // L112 if arm1: map is empty → fall through to getCombinedStatus
    // L170 cond-expr arm0: statusState === 'success' → reason = combined-status-success
    const listForRef = jest.fn().mockResolvedValue({ data: null });
    const getCombinedStatus = jest.fn().mockResolvedValue({ data: { state: 'success' } });
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.green).toBe(true);
    expect(result.reason).toBe('combined-status-success');
    expect(getCombinedStatus).toHaveBeenCalled();
  });

  test('falls back to combined-status-not-success when status is not success', async () => {
    const listForRef = jest.fn().mockResolvedValue({ data: null });
    const getCombinedStatus = jest.fn().mockResolvedValue({ data: { state: 'failure' } });
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.green).toBe(false);
    expect(result.reason).toBe('combined-status-not-success');
  });

  test('handles run with empty name by using __unnamed__ placeholder', async () => {
    // L102 binary-expr arm1: run.name is empty → uses '__unnamed__'
    const listForRef = jest
      .fn()
      .mockResolvedValueOnce({
        data: {
          check_runs: [{ id: 1, name: '', status: 'completed', conclusion: 'success' }],
        },
      })
      .mockResolvedValue({ data: { check_runs: [] } });
    const getCombinedStatus = jest.fn();
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.latestRuns.length).toBe(1);
    expect(result.green).toBe(true);
  });

  test('handles run with non-numeric id — uses -1 for comparison', async () => {
    // L103 cond-expr arm1: id is not a number → use -1
    const listForRef = jest
      .fn()
      .mockResolvedValueOnce({
        data: {
          check_runs: [{ id: 'not-a-number', name: 'check', status: 'completed', conclusion: 'success' }],
        },
      })
      .mockResolvedValue({ data: { check_runs: [] } });
    const getCombinedStatus = jest.fn();
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.green).toBe(true);
  });

  test('keeps the earlier run when a duplicate name appears with a lower id', async () => {
    // First run: id=100 (success), second run: id=50 (failure), same name
    // L105 cond-expr arm0: prev.id IS a number → use prev.id (100)
    // L107 binary-expr arm1: !prev is false (prev exists) → evaluates right side (50 > 100 = false)
    // L107 if arm1: condition FALSE (prev exists AND currentId <= prevId) → don't update map
    const listForRef = jest
      .fn()
      .mockResolvedValueOnce({
        data: {
          check_runs: [
            { id: 100, name: 'check', status: 'completed', conclusion: 'success' },
            { id: 50, name: 'check', status: 'completed', conclusion: 'failure' },
          ],
        },
      })
      .mockResolvedValue({ data: { check_runs: [] } });
    const getCombinedStatus = jest.fn();
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.latestRuns.length).toBe(1);
    expect(result.latestRuns[0]?.conclusion).toBe('success');
    expect(result.green).toBe(true);
  });

  test('returns combined-status-fetch-failed when getCombinedStatusForRef throws', async () => {
    const listForRef = jest.fn().mockResolvedValue({ data: null });
    const getCombinedStatus = jest.fn().mockRejectedValue(new Error('network error'));
    const ctx = makeContext(listForRef, getCombinedStatus);

    const result = await evaluateHeadGreenForApprovalReevaluation(ctx as any, repoInfo, 'abc123', makeCallbacks());
    expect(result.green).toBe(false);
    expect(result.reason).toBe('combined-status-fetch-failed');
  });
});
