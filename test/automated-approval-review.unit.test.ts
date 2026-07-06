/* eslint-disable @typescript-eslint/no-explicit-any */
import { jest, describe, test, expect } from '@jest/globals';
import { ensureAutomatedApprovalReviewForCurrentHead } from '../src/handlers/request/application/automated-approval-review.js';

// GateGuard facts:
// (1) No files import this — Jest auto-discovers via testMatch;
// (2) Tests ensureAutomatedApprovalReviewForCurrentHead (exported public function);
// (3) No data files — all synthetic jest.fn() mocks;
// (4) "proceed, the goal is everything at least on 90%. coverageThreshold: {...}"

const repoInfo = { owner: 'org', repo: 'repo' };
const decision: any = { status: 'approved', comment: 'LGTM' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, any> = {}) {
  return {
    toStringTrim: (v: unknown) => (typeof v === 'string' ? v.trim() : ''),
    isPlainObject: (v: unknown) => typeof v === 'object' && v !== null && !Array.isArray(v),
    getVisibleApprovalText: jest.fn().mockReturnValue('Approved by hook'),
    hasAutoApprovedPrHead: jest.fn().mockReturnValue(false),
    hasAutoApprovalReviewForHead: jest.fn().mockResolvedValue(false),
    markAutoApprovedPrHead: jest.fn(),
    addApprovedLabelToPr: jest.fn().mockResolvedValue(undefined),
    autoApprovedPrHeadKey: (_r: any, num: number, sha: string) => `${num}::${sha}`,
    logCreated: jest.fn(),
    logCreateFailed: jest.fn(),
    logDedupedInFlight: jest.fn(),
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCtx(createReviewImpl: () => Promise<unknown>) {
  return {
    octokit: {
      pulls: { createReview: jest.fn(createReviewImpl) },
      issues: {
        listComments: jest.fn().mockResolvedValue({ data: [] }),
        createComment: jest.fn().mockResolvedValue({ data: { id: 1 } }),
      },
    },
    log: { info: jest.fn(), debug: jest.fn(), warn: jest.fn() },
  } as any;
}

// L162 arm0: pr.head.sha is empty/null → toStringTrim returns '' → !headSha → return false
test('L162 arm0: PR with null head sha returns false immediately', async () => {
  const ctx = makeCtx(() => Promise.resolve({}));
  const cbs = makeCallbacks();
  const pr: any = { number: 100, head: null };

  const result = await ensureAutomatedApprovalReviewForCurrentHead(ctx, repoInfo, pr, decision, {}, cbs);
  expect(result).toBe(false);
  expect(cbs.hasAutoApprovedPrHead).not.toHaveBeenCalled();
});

// L158 default-arg arm0: call without 5th arg (options) → defaults to {}
test('L158 default-arg: call with undefined options (default {}) still works', async () => {
  const ctx = makeCtx(() => Promise.resolve({}));
  const cbs = makeCallbacks({
    hasAutoApprovedPrHead: jest.fn().mockReturnValue(true),
  });
  const pr: any = { number: 101, head: { sha: 'abc123' } };

  // Pass undefined explicitly to trigger the default parameter (arm0)
  const result = await ensureAutomatedApprovalReviewForCurrentHead(ctx, repoInfo, pr, decision, undefined as any, cbs);
  expect(result).toBe(true);
});

describe('createAutomatedApprovalReview catch block branches', () => {
  // L90 arm1: e is NOT a plain object (throws string) → errObj = {}
  // L94 arm1: e is NOT Error → String(e) path
  // L105 arm1: status is falsy → '' appended (no HTTP status)
  test('L90 arm1 + L94 arm1 + L105 arm1: createReview throws a string', async () => {
    const ctx = makeCtx((): Promise<never> => Promise.reject('plain-string-error'));
    const cbs = makeCallbacks();
    const pr: any = { number: 200, head: { sha: 'sha-str-1' } };

    const result = await ensureAutomatedApprovalReviewForCurrentHead(ctx, repoInfo, pr, decision, {}, cbs);
    expect(result).toBe(false);
    // logCreateFailed called with undefined status (no status on string), 'plain-string-error' message
    expect(cbs.logCreateFailed).toHaveBeenCalledWith(
      expect.anything(),
      200,
      undefined,
      'plain-string-error',
      undefined
    );
  });

  // L91 arm0: errObj['status'] is a number → returns status
  // L92 arm0: errObj['response'] is a plain object → returns it
  // L105 arm0: status is truthy → ' (HTTP 422)' appended
  test('L91 arm0 + L92 arm0 + L105 arm0: createReview throws object with numeric status+response', async () => {
    const ctx = makeCtx(
      (): Promise<never> =>
        Promise.reject({
          status: 422,
          response: { data: { message: 'already reviewed' } },
          message: 'already reviewed',
        })
    );
    const cbs = makeCallbacks();
    const pr: any = { number: 201, head: { sha: 'sha-obj-1' } };

    const result = await ensureAutomatedApprovalReviewForCurrentHead(ctx, repoInfo, pr, decision, {}, cbs);
    expect(result).toBe(false);
    // logCreateFailed called with status=422 (L91 arm0) and response data (L92 arm0)
    expect(cbs.logCreateFailed).toHaveBeenCalledWith(
      expect.anything(),
      201,
      422,
      expect.any(String),
      expect.objectContaining({ message: 'already reviewed' })
    );
    // postOnce was called with ' (HTTP 422)' in the message (L105 arm0)
    expect(ctx.octokit.issues.createComment).toHaveBeenCalledWith(
      expect.objectContaining({ body: expect.stringContaining('HTTP 422') })
    );
  });
});
