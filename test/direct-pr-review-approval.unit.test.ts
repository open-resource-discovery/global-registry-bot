/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import {
  hasAllowedStandaloneDirectPrApprovalForCurrentHead,
  hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr,
} from '../src/handlers/request/application/direct-pr-review-approval.js';

const repoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCtx() {
  return {
    octokit: {
      pulls: {
        listReviews: jest.fn().mockResolvedValue({ data: [] }),
      },
      users: {
        getByUsername: jest.fn().mockResolvedValue({ data: { login: 'alice' } }),
      },
    },
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks() {
  return {
    directPrRequestTypeResolutionCallbacks: {
      resolveRequestTypes: jest.fn().mockReturnValue([]),
      getRegistryBotConfig: jest.fn().mockReturnValue(null),
    },
    directPrApproverResolutionCallbacks: {
      getRegistryBotConfig: jest.fn().mockReturnValue(null),
      resolveEffectiveConstants: jest.fn().mockReturnValue({ directPrApprovers: [] }),
    },
    pullRequestAuthorResolutionCallbacks: {
      resolveRequestAuthorIdFromCommit: jest.fn().mockResolvedValue(null),
    },
    log: jest.fn(),
  } as any;
}

// ---------------------------------------------------------------------------
// hasAllowedStandaloneDirectPrApprovalForCurrentHead
// L70 default-arg arm0: called without options → options = {} default used
// L74 if arm0: headSha = '' → early return false
// ---------------------------------------------------------------------------

describe('hasAllowedStandaloneDirectPrApprovalForCurrentHead', () => {
  test('L70 default-arg arm0 + L74 if arm0: null sha returns false; options defaults to {}', async () => {
    const pr = { number: 1, head: { sha: '' }, user: { login: 'alice' } };
    // Pass undefined for options to trigger the default parameter (arm0)
    const result = await hasAllowedStandaloneDirectPrApprovalForCurrentHead(
      makeCtx(),
      repoInfo,
      pr as any,
      { status: 'unknown' } as any,
      undefined,
      makeCallbacks()
    );
    expect(result).toBe(false);
  });

  test('L74 if arm0: null head returns false immediately', async () => {
    const pr = { number: 1, head: null, user: { login: 'alice' } };
    const result = await hasAllowedStandaloneDirectPrApprovalForCurrentHead(
      makeCtx(),
      repoInfo,
      pr as any,
      { status: 'unknown' } as any,
      {},
      makeCallbacks()
    );
    expect(result).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr
// L131 default-arg arm0: called without options → options = {} default used
// L135 if arm0: headSha = '' → early return false
// ---------------------------------------------------------------------------

describe('hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr', () => {
  test('L131 default-arg arm0 + L135 if arm0: null sha returns false; options defaults to {}', async () => {
    const pr = { number: 1, head: { sha: '' }, user: { login: 'alice' } };
    const result = await hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr(
      makeCtx(),
      repoInfo,
      pr as any,
      { status: 'unknown' } as any,
      undefined,
      makeCallbacks()
    );
    expect(result).toBe(false);
  });

  test('L135 if arm0: pr.head is null → headSha is empty → returns false', async () => {
    const pr = { number: 1, head: null, user: { login: 'alice' } };
    const result = await hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr(
      makeCtx(),
      repoInfo,
      pr as any,
      { status: 'unknown' } as any,
      {},
      makeCallbacks()
    );
    expect(result).toBe(false);
  });
});
