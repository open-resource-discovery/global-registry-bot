/* eslint-disable @typescript-eslint/no-explicit-any */
import { jest, test, expect } from '@jest/globals';
import { processPullRequestForAutoMerge } from '../src/handlers/request/application/pull-request-auto-merge-entry.js';

// GateGuard: (1) Jest auto-discovers — no caller files;
// (2) auto-merge.test.ts covers lib/auto-merge.ts (different module), orchestrator test covers index.ts adapter;
// (3) all synthetic jest.fn() mocks, no data files;
// (4) "proceed, the goal is everything at least on 90%"

const ctx = {};
const repoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makePr(overrides: Record<string, any> = {}) {
  return { number: 1, body: 'pr body text', base: { ref: 'main' }, ...overrides } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, any> = {}) {
  return {
    isSequentialDirectRegistryPr: jest.fn().mockResolvedValue(false),
    shouldDeferSequentialDirectRegistryPrProcessing: jest.fn().mockResolvedValue(false),
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(null),
    readFreshPullRequest: jest.fn().mockResolvedValue(null),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('continue'),
    tryMergeApprovedPrOrUpdateBranch: jest.fn().mockResolvedValue(undefined),
    buildIssueParams: jest.fn().mockReturnValue({ owner: 'org', repo: 'repo', issue_number: 42 }),
    readLinkedIssue: jest.fn().mockResolvedValue({ number: 42, body: 'issue body' }),
    log: jest.fn(),
    getErrorMessage: jest.fn().mockReturnValue('err'),
    getHttpStatus: jest.fn().mockReturnValue(undefined),
    isCrossRepositoryPullRequest: jest.fn().mockReturnValue(false),
    hasIssueFormInputs: jest.fn().mockReturnValue(true),
    loadTemplateWithLabelRefresh: jest.fn().mockResolvedValue({ _meta: { schema: '' } }),
    parseForm: jest.fn().mockReturnValue({ namespace: 'ns', requestType: 'product' }),
    readIssueBodyForProcessing: jest.fn().mockReturnValue('issue body'),
    isRequestIssue: jest.fn().mockReturnValue(true),
    buildCompatibleRequestSnapshotHashes: jest.fn().mockReturnValue(['hash1']),
    calcSnapshotHash: jest.fn().mockReturnValue('fallback-hash'),
    extractHashFromPrBody: jest.fn().mockReturnValue(''),
    closeOutdatedRequestPrs: jest.fn().mockResolvedValue(undefined),
    maybeHandleDirectPrApprovalForMerge: jest.fn().mockResolvedValue('continue'),
    ...overrides,
  } as any;
}

// L130 arm0: isSequentialDirectRegistryPr=true + shouldDefer=true → early return
test('L130 arm0: sequential direct PR with defer=true → returns without processing', async () => {
  const cbs = makeCallbacks({
    isSequentialDirectRegistryPr: jest.fn().mockResolvedValue(true),
    shouldDeferSequentialDirectRegistryPrProcessing: jest.fn().mockResolvedValue(true),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, makePr(), cbs);
  expect(cbs.parseLinkedIssueNumberFromPr).not.toHaveBeenCalled();
});

// L145 arm1: issueNumber=null, standaloneOutcome='approved', readFreshPullRequest returns null → fallback to freshPr
test('L145 arm1: readFreshPullRequest null after approval → approvedPr falls back to freshPr', async () => {
  const pr = makePr({ number: 1 });
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(null),
    readFreshPullRequest: jest.fn().mockResolvedValue(null),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('approved'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, pr, cbs);
  expect(cbs.tryMergeApprovedPrOrUpdateBranch).toHaveBeenCalledWith(ctx, repoInfo, pr, 'auto-merge');
});

// L174 arm0: readLinkedIssue throws → standaloneOutcome !== 'approved' → returns early
test('L174 arm0: linked-issue read failed + not approved → returns without merge', async () => {
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(42),
    readLinkedIssue: jest.fn().mockRejectedValue(new Error('not found')),
    readFreshPullRequest: jest.fn().mockResolvedValue(makePr({ number: 2 })),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('continue'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, makePr(), cbs);
  expect(cbs.tryMergeApprovedPrOrUpdateBranch).not.toHaveBeenCalled();
});

// L169 + L176 arm1: readLinkedIssue throws → readFreshPullRequest null → fallback to original pr
test('L169/L176 arm1: linked-issue read failed + readFreshPullRequest null → both fallback to pr', async () => {
  const pr = makePr({ number: 1 });
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(42),
    readLinkedIssue: jest.fn().mockRejectedValue(new Error('not found')),
    readFreshPullRequest: jest.fn().mockResolvedValue(null),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('approved'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, pr, cbs);
  expect(cbs.tryMergeApprovedPrOrUpdateBranch).toHaveBeenCalledWith(ctx, repoInfo, pr, 'auto-merge');
});

// L216 arm0: loadTemplateWithLabelRefresh throws → standaloneOutcome not approved → returns early
test('L216 arm0: template load fails + not approved → returns without merge', async () => {
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(42),
    loadTemplateWithLabelRefresh: jest.fn().mockRejectedValue(new Error('template error')),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('continue'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, makePr(), cbs);
  expect(cbs.tryMergeApprovedPrOrUpdateBranch).not.toHaveBeenCalled();
});

// L238 arm0: isRequestIssue=false → standaloneOutcome not approved → returns early
test('L238 arm0: not a request issue + not approved → returns without merge', async () => {
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(42),
    loadTemplateWithLabelRefresh: jest.fn().mockResolvedValue({ _meta: {} }),
    isRequestIssue: jest.fn().mockReturnValue(false),
    maybeHandleStandaloneDirectPrApproval: jest.fn().mockResolvedValue('continue'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, makePr(), cbs);
  expect(cbs.tryMergeApprovedPrOrUpdateBranch).not.toHaveBeenCalled();
});

// L251 arm1: buildCompatibleRequestSnapshotHashes returns [] → calcSnapshotHash used as fallback
test('L251 arm1: empty snapshotHashes → calcSnapshotHash called', async () => {
  const cbs = makeCallbacks({
    parseLinkedIssueNumberFromPr: jest.fn().mockReturnValue(42),
    loadTemplateWithLabelRefresh: jest.fn().mockResolvedValue({ _meta: {} }),
    isRequestIssue: jest.fn().mockReturnValue(true),
    buildCompatibleRequestSnapshotHashes: jest.fn().mockReturnValue([]),
    extractHashFromPrBody: jest.fn().mockReturnValue(''),
    maybeHandleDirectPrApprovalForMerge: jest.fn().mockResolvedValue('continue'),
  });
  await processPullRequestForAutoMerge(ctx, repoInfo, makePr(), cbs);
  expect(cbs.calcSnapshotHash).toHaveBeenCalled();
});
