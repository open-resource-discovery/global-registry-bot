/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */

/**
 * Focused unit tests for createRequestPrWithRecovery and its single-flight guard.
 *
 * These tests exercise the public API in request-pr-creation-recovery.ts directly,
 * without going through the full approved-request-finalization pipeline.
 */

import { describe, it, expect, jest, beforeEach } from '@jest/globals';

// Dynamic import is required because createRequestPrWithRecovery uses a module-level
// inflight Map that must be reset between test suites (via jest.resetModules).
type CreateFn =
  typeof import('../src/handlers/request/application/request-pr-creation-recovery.js').createRequestPrWithRecovery;

async function loadSubject(): Promise<{ createRequestPrWithRecovery: CreateFn }> {
  jest.resetModules();
  const mod = await import('../src/handlers/request/application/request-pr-creation-recovery.js');
  return { createRequestPrWithRecovery: (mod as any).createRequestPrWithRecovery };
}

// Minimal fixtures
const REPO_PARAMS = { owner: 'org', repo: 'my-repo', issue_number: 42 };
const ISSUE = { number: 42 };
const FORM_DATA = { identifier: 'foo' };
const TEMPLATE = { _meta: { requestType: 'system', root: 'data', schema: 'x.json' } };
const RESOURCE_NAME = 'my-resource';
const BRANCH_NAME = 'feat/resource-my-resource-issue-42';

function makeCallbacks(createRequestPr: jest.Mock<any>): any {
  return {
    createRequestPr,
    getHttpStatus: (e: unknown) => {
      if (typeof e === 'object' && e !== null && 'status' in e) {
        return (e as { status: number }).status;
      }
      return undefined;
    },
    renderConfiguredRequestBranchName: () => BRANCH_NAME,
  };
}

describe('createRequestPrWithRecovery – single-flight guard', () => {
  // The inflight Map is module-level, so we reload the module between tests.
  beforeEach(() => {
    jest.resetModules();
  });

  // ── 1. Same key, concurrent success ─────────────────────────────────────────
  it('same key concurrent success: createRequestPr executes exactly once, both callers receive the PR', async () => {
    const { createRequestPrWithRecovery } = await loadSubject();

    let resolveInner!: (v: { number: number }) => void;
    const innerPromise = new Promise<{ number: number }>((res) => {
      resolveInner = res;
    });

    const createRequestPr = jest.fn<() => Promise<{ number: number }>>(() => innerPromise);
    const callbacks = makeCallbacks(createRequestPr as any);

    const logDebug = jest.fn();
    const ctx = { log: { debug: logDebug } } as any;

    // Start two concurrent calls before the underlying promise resolves.
    const p1 = createRequestPrWithRecovery(ctx, REPO_PARAMS, ISSUE, FORM_DATA, TEMPLATE, RESOURCE_NAME, callbacks);
    const p2 = createRequestPrWithRecovery(ctx, REPO_PARAMS, ISSUE, FORM_DATA, TEMPLATE, RESOURCE_NAME, callbacks);

    // Resolve the underlying operation.
    resolveInner({ number: 7 });

    const [r1, r2] = await Promise.all([p1, p2]);

    // Both must receive the same result.
    expect(r1.number).toBe(7);
    expect(r2.number).toBe(7);

    // The underlying createRequestPr must have been called exactly once.
    expect(createRequestPr).toHaveBeenCalledTimes(1);

    // The second call should have logged the single-flight-reused stage.
    const debugCalls = logDebug.mock.calls as any[][];
    const reused = debugCalls.some(
      (c) => typeof c[0] === 'object' && c[0] !== null && c[0].stage === 'request-pr:single-flight-reused'
    );
    expect(reused).toBe(true);
  });

  // ── 2. Same key, concurrent failure ─────────────────────────────────────────
  it('same key concurrent failure: createRequestPr executes exactly once, both callers receive same error, no double prefix', async () => {
    const { createRequestPrWithRecovery } = await loadSubject();

    let rejectInner!: (e: Error) => void;
    const innerPromise = new Promise<{ number: number }>((_res, rej) => {
      rejectInner = rej;
    });

    const createRequestPr = jest.fn<() => Promise<{ number: number }>>(() => innerPromise);
    const callbacks = makeCallbacks(createRequestPr as any);
    const ctx = {} as any;

    const p1 = createRequestPrWithRecovery(ctx, REPO_PARAMS, ISSUE, FORM_DATA, TEMPLATE, RESOURCE_NAME, callbacks);
    const p2 = createRequestPrWithRecovery(ctx, REPO_PARAMS, ISSUE, FORM_DATA, TEMPLATE, RESOURCE_NAME, callbacks);

    rejectInner(new Error('upstream failure'));

    const [e1, e2] = await Promise.all([p1.catch((e: unknown) => e), p2.catch((e: unknown) => e)]);

    expect(e1).toBeInstanceOf(Error);
    expect(e2).toBeInstanceOf(Error);

    // Both callers must receive the same formatted message.
    expect((e1 as Error).message).toBe((e2 as Error).message);

    // Message must contain exactly one "Failed to create Pull Request:" prefix.
    const msg = (e1 as Error).message;
    expect(msg).toContain('Failed to create Pull Request:');
    const prefixCount = (msg.match(/Failed to create Pull Request:/g) ?? []).length;
    expect(prefixCount).toBe(1);

    // Underlying function called exactly once.
    expect(createRequestPr).toHaveBeenCalledTimes(1);

    // Original error preserved as cause.
    expect((e1 as Error).cause).toBeInstanceOf(Error);
    expect(((e1 as Error).cause as Error).message).toBe('upstream failure');
  });

  // ── 3. Cleanup after rejection: immediate retry with same key can succeed ────
  it('cleanup after rejection: immediate same-key retry executes fresh underlying call, no stale entry reused', async () => {
    const { createRequestPrWithRecovery } = await loadSubject();

    let callCount = 0;
    // The underlying function rejects on the first call, resolves on the second.
    const underlyingFn = jest.fn<() => Promise<{ number: number }>>(() => {
      callCount++;
      if (callCount === 1) return Promise.reject(new Error('first fail'));
      return Promise.resolve({ number: 99 });
    });
    const callbacks = makeCallbacks(underlyingFn as any);
    const ctx = {} as any;

    // First call — will reject after wrapping.
    const p1 = createRequestPrWithRecovery(
      ctx,
      REPO_PARAMS,
      ISSUE,
      FORM_DATA,
      TEMPLATE,
      RESOURCE_NAME,
      callbacks
    ).catch(() => null);

    // Immediately start the second call — it must NOT join p1 because the key
    // will be cleaned up once p1 settles.
    // We await p1 first to let it settle (and thus trigger cleanup).
    await p1;

    // Now invoke immediately with the same key — cleanup must have run.
    const result = await createRequestPrWithRecovery(
      ctx,
      REPO_PARAMS,
      ISSUE,
      FORM_DATA,
      TEMPLATE,
      RESOURCE_NAME,
      callbacks
    );

    // The second underlying call succeeds.
    expect(result.number).toBe(99);
    // The underlying function was called exactly twice — once for each public call.
    expect(underlyingFn).toHaveBeenCalledTimes(2);
    // No stale rejected promise was reused — callCount proves fresh execution.
    expect(callCount).toBe(2);
  });

  // ── 4. Different keys run independently ─────────────────────────────────────
  it('different keys: two calls with different issue numbers start independently without serialization', async () => {
    const { createRequestPrWithRecovery } = await loadSubject();

    const callOrder: number[] = [];

    let resolveFirst!: (v: { number: number }) => void;
    let resolveSecond!: (v: { number: number }) => void;

    const firstPr = jest.fn<() => Promise<{ number: number }>>(
      () =>
        new Promise((res) => {
          resolveFirst = res;
          callOrder.push(1);
        })
    );
    const secondPr = jest.fn<() => Promise<{ number: number }>>(
      () =>
        new Promise((res) => {
          resolveSecond = res;
          callOrder.push(2);
        })
    );

    const paramsA = { owner: 'org', repo: 'my-repo', issue_number: 1 };
    const paramsB = { owner: 'org', repo: 'my-repo', issue_number: 2 };
    const issueA = { number: 1 };
    const issueB = { number: 2 };

    const ctx = {} as any;

    const p1 = createRequestPrWithRecovery(
      ctx,
      paramsA,
      issueA,
      FORM_DATA,
      TEMPLATE,
      RESOURCE_NAME,
      makeCallbacks(firstPr as any)
    );
    const p2 = createRequestPrWithRecovery(
      ctx,
      paramsB,
      issueB,
      FORM_DATA,
      TEMPLATE,
      RESOURCE_NAME,
      makeCallbacks(secondPr as any)
    );

    // Both underlying functions should have been called (not serialized behind each other).
    expect(callOrder).toEqual(expect.arrayContaining([1, 2]));
    expect(callOrder).toHaveLength(2);

    resolveFirst({ number: 10 });
    resolveSecond({ number: 20 });

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1.number).toBe(10);
    expect(r2.number).toBe(20);

    expect(firstPr).toHaveBeenCalledTimes(1);
    expect(secondPr).toHaveBeenCalledTimes(1);
  });
});

// ── findOpenIssuePrs failure test (via approved-request-finalization.ts) ─────

describe('approved-request-finalization – findOpenIssuePrs failure handling', () => {
  it('findOpenIssuePrs rejects: posts one error comment, createRequestPrWithRecovery not called, applyApprovedRequestState not called', async () => {
    jest.resetModules();
    const mod = await import('../src/handlers/request/application/approved-request-finalization.js');
    const { finalizeApprovedRequest } = mod;

    const postOnce = jest.fn(async () => {});
    const createRequestPrWithRecovery = jest.fn(async () => ({ number: 1 }));
    const applyApprovedRequestState = jest.fn(async () => {});
    const findOpenIssuePrs = jest.fn<() => Promise<never>>().mockRejectedValueOnce(new Error('PR lookup failed'));

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: async () => [],
      findOpenIssuePrs,
      applyApprovedRequestState,
      addApprovedLabelToPr: jest.fn(async () => {}),
      ensureAssigneesPresent: jest.fn(async () => {}),
      createRequestPrWithRecovery,
      postOnce,
    };

    await finalizeApprovedRequest({}, { owner: 'o', repo: 'r', issue_number: 1 }, { number: 1 }, {}, {}, {}, callbacks);

    // Exactly one failure comment posted.
    expect(postOnce).toHaveBeenCalledTimes(1);
    const postedBody = String((postOnce.mock.calls as any[][])[0][2]);
    expect(postedBody).toContain('Failed to create Pull Request:');
    expect(postedBody).toContain('PR lookup failed');

    // PR creation not attempted.
    expect(createRequestPrWithRecovery).not.toHaveBeenCalled();

    // State not applied.
    expect(applyApprovedRequestState).not.toHaveBeenCalled();
  });
});

// ── Exact prefix count assertions ────────────────────────────────────────────

describe('approved-request-finalization – no double prefix when createRequestPrWithRecovery already formatted error', () => {
  it('error already starting with "Failed to create Pull Request:" is not double-prefixed', async () => {
    jest.resetModules();
    const mod = await import('../src/handlers/request/application/approved-request-finalization.js');
    const { finalizeApprovedRequest } = mod;

    const postOnce = jest.fn(async () => {});

    // recovery.ts already formats the message
    const alreadyFormattedError = new Error('Failed to create Pull Request: some stage detail');
    const createRequestPrWithRecovery = jest.fn<() => Promise<never>>().mockRejectedValueOnce(alreadyFormattedError);

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: async () => [],
      findOpenIssuePrs: jest.fn(async () => []),
      applyApprovedRequestState: jest.fn(async () => {}),
      addApprovedLabelToPr: jest.fn(async () => {}),
      ensureAssigneesPresent: jest.fn(async () => {}),
      createRequestPrWithRecovery,
      postOnce,
    };

    await finalizeApprovedRequest({}, { owner: 'o', repo: 'r', issue_number: 1 }, { number: 1 }, {}, {}, {}, callbacks);

    expect(postOnce).toHaveBeenCalledTimes(1);
    const postedBody = String((postOnce.mock.calls as any[][])[0][2]);

    // Exactly one prefix, not two.
    const prefixCount = (postedBody.match(/Failed to create Pull Request:/g) ?? []).length;
    expect(prefixCount).toBe(1);
    expect(postedBody).toBe('Failed to create Pull Request: some stage detail');
  });

  it('raw error without prefix gets exactly one prefix added', async () => {
    jest.resetModules();
    const mod = await import('../src/handlers/request/application/approved-request-finalization.js');
    const { finalizeApprovedRequest } = mod;

    const postOnce = jest.fn(async () => {});
    const createRequestPrWithRecovery = jest.fn<() => Promise<never>>().mockRejectedValueOnce(new Error('raw error'));

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: async () => [],
      findOpenIssuePrs: jest.fn(async () => []),
      applyApprovedRequestState: jest.fn(async () => {}),
      addApprovedLabelToPr: jest.fn(async () => {}),
      ensureAssigneesPresent: jest.fn(async () => {}),
      createRequestPrWithRecovery,
      postOnce,
    };

    await finalizeApprovedRequest({}, { owner: 'o', repo: 'r', issue_number: 1 }, { number: 1 }, {}, {}, {}, callbacks);

    expect(postOnce).toHaveBeenCalledTimes(1);
    const postedBody = String((postOnce.mock.calls as any[][])[0][2]);
    const prefixCount = (postedBody.match(/Failed to create Pull Request:/g) ?? []).length;
    expect(prefixCount).toBe(1);
    expect(postedBody).toBe('Failed to create Pull Request: raw error');
  });
});
