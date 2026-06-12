/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import { createStatusEventHandler } from '../src/handlers/request/events/status.js';
import { createPullRequestEventHandler } from '../src/handlers/request/events/pull-requests.js';
import { createPushEventHandler } from '../src/handlers/request/events/push.js';
import { log } from '../src/handlers/request/infrastructure/logger.js';

const strTrim = (v: unknown): string => {
  if (v === undefined || v === null) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v).trim();
  return '';
};

// ── createStatusEventHandler ──────────────────────────────────────────────────

describe('createStatusEventHandler', () => {
  test('L17 false arm: non-plain-object payload → state="" → early return before tryAutoMerge', async () => {
    const tryAutoMerge = jest.fn();
    const handler = createStatusEventHandler({
      isPlainObject: () => false,
      toStringTrim: strTrim,
      toRepoInfo: () => ({}),
      tryAutoMerge,
    } as any);
    await handler({ payload: 'not-a-plain-object' });
    expect(tryAutoMerge).not.toHaveBeenCalled();
  });

  test('L20,21,22,23,25 false arms: first isPlainObject true (state=success), rest false → early return', async () => {
    const tryAutoMerge = jest.fn();
    const isPlainObject = jest
      .fn()
      .mockReturnValueOnce(true) // L17: payload → true → toStringTrim('success') = 'success'
      .mockReturnValue(false); // L20,21,22,23,25: false → repo/owner/sha all ''
    const handler = createStatusEventHandler({
      isPlainObject: isPlainObject as any,
      toStringTrim: strTrim,
      toRepoInfo: () => ({}),
      tryAutoMerge,
    } as any);
    await handler({ payload: { state: 'success' } });
    expect(tryAutoMerge).not.toHaveBeenCalled();
    expect(isPlainObject).toHaveBeenCalledTimes(6);
  });
});

// ── createPullRequestEventHandler ─────────────────────────────────────────────

describe('createPullRequestEventHandler', () => {
  test('L27 if-body: readRepoInfoFromPayload returns null → early return', async () => {
    const approve = jest.fn();
    const handler = createPullRequestEventHandler({
      getStaticConfig: () => Promise.resolve({}),
      readRepoInfoFromPayload: () => null,
      isPlainObject: () => false,
      isPullRequestOpen: () => false,
      maybeApprovePendingWorkflowRunsForRegistryPrWithRetry: approve,
      toStringTrim: strTrim,
    } as any);
    await handler({ payload: {} });
    expect(approve).not.toHaveBeenCalled();
  });

  test('L29 false arm + L30 if-body: isPlainObject(payload) false → prRaw=null → early return', async () => {
    const approve = jest.fn();
    const handler = createPullRequestEventHandler({
      getStaticConfig: () => Promise.resolve({}),
      readRepoInfoFromPayload: () => ({ owner: 'org', repo: 'repo' }),
      isPlainObject: () => false,
      isPullRequestOpen: () => false,
      maybeApprovePendingWorkflowRunsForRegistryPrWithRetry: approve,
      toStringTrim: strTrim,
    } as any);
    await handler({ payload: {} });
    expect(approve).not.toHaveBeenCalled();
  });

  test('L33 if-body: isPullRequestOpen returns false → early return', async () => {
    const approve = jest.fn();
    const handler = createPullRequestEventHandler({
      getStaticConfig: () => Promise.resolve({}),
      readRepoInfoFromPayload: () => ({ owner: 'org', repo: 'repo' }),
      isPlainObject: () => true,
      isPullRequestOpen: () => false,
      maybeApprovePendingWorkflowRunsForRegistryPrWithRetry: approve,
      toStringTrim: strTrim,
    } as any);
    await handler({ payload: { pull_request: { state: 'closed' } } });
    expect(approve).not.toHaveBeenCalled();
  });
});

// ── createPushEventHandler ─────────────────────────────────────────────────────

describe('createPushEventHandler', () => {
  test('L57 false arm + L79 if-body: isPlainObject false → ref="" and isDefaultBranchPush false → early return', async () => {
    const reevaluate = jest.fn();
    const handler = createPushEventHandler({
      readRepoInfoFromPayload: () => null,
      isPlainObject: () => false,
      toStringTrim: strTrim,
      readDefaultBranchFromPush: () => 'main',
      readPushChangedFiles: () => [],
      isApprovalConfigChangePath: () => false,
      isDefaultBranchPush: () => false,
      getStaticConfig: () => Promise.resolve({}),
      reevaluateOpenDirectPullRequestsAfterDefaultBranchPush: reevaluate,
      updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry: jest.fn(),
      log: () => {},
    } as any);
    await handler({ payload: 'not-an-object' });
    expect(reevaluate).not.toHaveBeenCalled();
  });
});

// ── log (infrastructure/logger.ts) ────────────────────────────────────────────

describe('log', () => {
  test('L11 false arm: fn is not a function → if-body skipped, no throw', () => {
    expect(() => {
      log({ log: { info: {} as any } }, 'info', { data: 1 }, 'test message');
    }).not.toThrow();
  });
});
