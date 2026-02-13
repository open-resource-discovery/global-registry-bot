import { jest } from '@jest/globals';
import { tryEnableAutoMerge, tryMergeIfGreen } from '../src/lib/auto-merge.js';

interface OpenPr {
  number: number;
  node_id: string;
  state: 'open' | 'closed' | string;
  draft: boolean;
  head: { sha: string };
}

function mkOpenPr(overrides: Partial<OpenPr> = {}): OpenPr {
  return {
    number: 5,
    node_id: 'PR_NODE',
    state: 'open',
    draft: false,
    head: { sha: 'SHA123' },
    ...overrides,
  };
}

type GraphqlFn = (query: string, variables: Record<string, unknown>) => Promise<unknown>;

type PullsGetFn = (args: { owner: string; repo: string; pull_number: number }) => Promise<{ data: OpenPr }>;

type PullsMergeFn = (args: {
  owner: string;
  repo: string;
  pull_number: number;
  merge_method: string;
}) => Promise<unknown>;

type PullsListReviewsFn = (args: {
  owner: string;
  repo: string;
  pull_number: number;
}) => Promise<{ data: { state: string }[] }>;

type CombinedStatusFn = (args: {
  owner: string;
  repo: string;
  ref: string;
}) => Promise<{ data: { state: string; total_count: number } }>;

type ChecksListForRefFn = (args: {
  owner: string;
  repo: string;
  ref: string;
}) => Promise<{ data: { total_count: number; check_runs: { conclusion: string | null }[] } }>;

type LogInfoFn = (msg: string) => void;
type LogWarnFn = (obj: unknown, msg: string) => void;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkContext() {
  return {
    octokit: {
      graphql: jest.fn<GraphqlFn>(() => Promise.resolve({})),

      pulls: {
        get: jest.fn<PullsGetFn>(() => Promise.resolve({ data: mkOpenPr() })),
        merge: jest.fn<PullsMergeFn>(() => Promise.resolve({})),
        listReviews: jest.fn<PullsListReviewsFn>(() => Promise.resolve({ data: [] })),
      },

      repos: {
        getCombinedStatusForRef: jest.fn<CombinedStatusFn>(() =>
          Promise.resolve({ data: { state: 'success', total_count: 0 } })
        ),
      },

      checks: {
        listForRef: jest.fn<ChecksListForRefFn>(() => Promise.resolve({ data: { total_count: 0, check_runs: [] } })),
      },
    },

    log: {
      info: jest.fn<LogInfoFn>(() => undefined),
      warn: jest.fn<LogWarnFn>(() => undefined),
    },
  };
}

describe('src/lib/auto-merge.ts', () => {
  describe('tryEnableAutoMerge', () => {
    it('enables auto-merge (default method SQUASH) and logs success', async () => {
      const ctx = mkContext();
      ctx.octokit.graphql.mockResolvedValueOnce({});

      const ok = await tryEnableAutoMerge(ctx, { number: 5, node_id: 'PR_NODE' });

      expect(ok).toBe(true);

      expect(ctx.octokit.graphql).toHaveBeenCalledWith(
        expect.stringContaining('enablePullRequestAutoMerge'),
        expect.objectContaining({ prId: 'PR_NODE', mergeMethodVar: 'SQUASH' })
      );

      expect(ctx.log.info).toHaveBeenCalledWith('Auto-merge enabled for PR #5 (SQUASH)');
      expect(ctx.log.warn).not.toHaveBeenCalled();
    });

    it('enables auto-merge with explicit merge method (MERGE)', async () => {
      const ctx = mkContext();
      ctx.octokit.graphql.mockResolvedValueOnce({});

      const ok = await tryEnableAutoMerge(ctx, { number: 5, node_id: 'PR_NODE' }, { mergeMethod: 'MERGE' });

      expect(ok).toBe(true);

      expect(ctx.octokit.graphql).toHaveBeenCalledWith(
        expect.stringContaining('enablePullRequestAutoMerge'),
        expect.objectContaining({ prId: 'PR_NODE', mergeMethodVar: 'MERGE' })
      );
      expect(ctx.log.info).toHaveBeenCalledWith('Auto-merge enabled for PR #5 (MERGE)');
    });

    it("returns false on 'clean status' response and logs info (no warn)", async () => {
      const ctx = mkContext();
      ctx.octokit.graphql.mockRejectedValueOnce(new Error('clean status'));

      const ok = await tryEnableAutoMerge(ctx, { number: 5, node_id: 'PR_NODE' });

      expect(ok).toBe(false);

      expect(ctx.log.info).toHaveBeenCalledWith(
        "PR #5: auto-merge enable responded 'clean status' → will try immediate merge fallback."
      );
      expect(ctx.log.warn).not.toHaveBeenCalled();
    });

    it('returns false and warns on other graphql errors', async () => {
      const ctx = mkContext();
      const err = new Error('no permission');
      ctx.octokit.graphql.mockRejectedValueOnce(err);

      const ok = await tryEnableAutoMerge(ctx, { number: 5, node_id: 'PR_NODE' });

      expect(ok).toBe(false);

      expect(ctx.log.warn).toHaveBeenCalledWith(expect.objectContaining({ err }), 'Auto-merge enable failed for PR #5');
    });

    it('treats non-Error throwables correctly (string)', async () => {
      const ctx = mkContext();
      ctx.octokit.graphql.mockRejectedValueOnce('clean status'); // string, not Error

      const ok = await tryEnableAutoMerge(ctx, { number: 5, node_id: 'PR_NODE' });

      expect(ok).toBe(false);
      expect(ctx.log.info).toHaveBeenCalledWith(
        "PR #5: auto-merge enable responded 'clean status' → will try immediate merge fallback."
      );
    });
  });

  describe('tryMergeIfGreen', () => {
    it('merges when statuses+checks green (default mergeMethod=squash) using provided prData', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'success', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: {
          total_count: 2,
          check_runs: [{ conclusion: 'success' }, { conclusion: 'neutral' }],
        },
      });

      ctx.octokit.pulls.merge.mockResolvedValueOnce({});

      const ok = await tryMergeIfGreen(ctx, {
        owner: 'o',
        repo: 'r',
        prNumber: 5,
        prData: pr,
      });

      expect(ok).toBe(true);

      expect(ctx.octokit.pulls.merge).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        pull_number: 5,
        merge_method: 'squash',
      });

      expect(ctx.log.info).toHaveBeenCalledWith('PR #5 merged (all checks green, method=squash)');
    });

    it('does not merge when PR is not open', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr({ state: 'closed' });

      const ok = await tryMergeIfGreen(ctx, { owner: 'o', repo: 'r', prNumber: 5, prData: pr });

      expect(ok).toBe(false);
      expect(ctx.octokit.repos.getCombinedStatusForRef).not.toHaveBeenCalled();
      expect(ctx.octokit.checks.listForRef).not.toHaveBeenCalled();
      expect(ctx.octokit.pulls.merge).not.toHaveBeenCalled();
    });

    it('does not merge when PR is draft', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr({ draft: true });

      const ok = await tryMergeIfGreen(ctx, { owner: 'o', repo: 'r', prNumber: 5, prData: pr });

      expect(ok).toBe(false);
      expect(ctx.octokit.repos.getCombinedStatusForRef).not.toHaveBeenCalled();
      expect(ctx.octokit.checks.listForRef).not.toHaveBeenCalled();
      expect(ctx.octokit.pulls.merge).not.toHaveBeenCalled();
    });

    it('does not merge when combined status is pending and total_count > 0', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'pending', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: { total_count: 1, check_runs: [{ conclusion: 'success' }] },
      });

      const ok = await tryMergeIfGreen(ctx, { owner: 'o', repo: 'r', prNumber: 5, prData: pr });

      expect(ok).toBe(false);
      expect(ctx.octokit.pulls.merge).not.toHaveBeenCalled();

      expect(ctx.log.info).toHaveBeenCalledWith(
        'PR #5 not merged yet (statusesOk=false, checksOk=true, approved=true)'
      );
    });

    it('treats combined status total_count=0 as OK even if state is pending, and merges', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'pending', total_count: 0 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: { total_count: 1, check_runs: [{ conclusion: 'skipped' }] },
      });

      ctx.octokit.pulls.merge.mockResolvedValueOnce({});

      const ok = await tryMergeIfGreen(ctx, { owner: 'o', repo: 'r', prNumber: 5, prData: pr });

      expect(ok).toBe(true);
      expect(ctx.octokit.pulls.merge).toHaveBeenCalledWith(expect.objectContaining({ merge_method: 'squash' }));
    });

    it('does not merge when checks are not OK', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'success', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: {
          total_count: 2,
          check_runs: [{ conclusion: 'success' }, { conclusion: 'failure' }],
        },
      });

      const ok = await tryMergeIfGreen(ctx, { owner: 'o', repo: 'r', prNumber: 5, prData: pr });

      expect(ok).toBe(false);
      expect(ctx.octokit.pulls.merge).not.toHaveBeenCalled();

      expect(ctx.log.info).toHaveBeenCalledWith(
        'PR #5 not merged yet (statusesOk=true, checksOk=false, approved=true)'
      );
    });

    it('requires approval when requireApproval=true (no APPROVED => no merge)', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'success', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: { total_count: 1, check_runs: [{ conclusion: 'success' }] },
      });

      ctx.octokit.pulls.listReviews.mockResolvedValueOnce({
        data: [{ state: 'COMMENTED' }],
      });

      const ok = await tryMergeIfGreen(ctx, {
        owner: 'o',
        repo: 'r',
        prNumber: 5,
        prData: pr,
        requireApproval: true,
      });

      expect(ok).toBe(false);
      expect(ctx.octokit.pulls.merge).not.toHaveBeenCalled();
      expect(ctx.octokit.pulls.listReviews).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        pull_number: 5,
      });

      expect(ctx.log.info).toHaveBeenCalledWith(
        'PR #5 not merged yet (statusesOk=true, checksOk=true, approved=false)'
      );
    });

    it('fetches PR when prData not provided and merges when approved + custom mergeMethod=rebase', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr({ number: 10, node_id: 'PR_NODE_10' });

      ctx.octokit.pulls.get.mockResolvedValueOnce({ data: pr });

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'success', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: { total_count: 1, check_runs: [{ conclusion: 'success' }] },
      });

      ctx.octokit.pulls.listReviews.mockResolvedValueOnce({
        data: [{ state: 'APPROVED' }],
      });

      ctx.octokit.pulls.merge.mockResolvedValueOnce({});

      const ok = await tryMergeIfGreen(ctx, {
        owner: 'o',
        repo: 'r',
        prNumber: 10,
        mergeMethod: 'rebase',
        requireApproval: true,
      });

      expect(ok).toBe(true);

      expect(ctx.octokit.pulls.get).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        pull_number: 10,
      });

      expect(ctx.octokit.pulls.merge).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        pull_number: 10,
        merge_method: 'rebase',
      });
    });

    it('logs warn and returns false if merge fails even though checks are green', async () => {
      const ctx = mkContext();
      const pr = mkOpenPr();

      ctx.octokit.repos.getCombinedStatusForRef.mockResolvedValueOnce({
        data: { state: 'success', total_count: 1 },
      });

      ctx.octokit.checks.listForRef.mockResolvedValueOnce({
        data: { total_count: 1, check_runs: [{ conclusion: 'success' }] },
      });

      const err = new Error('merge failed');
      ctx.octokit.pulls.merge.mockRejectedValueOnce(err);

      const ok = await tryMergeIfGreen(ctx, {
        owner: 'o',
        repo: 'r',
        prNumber: 5,
        prData: pr,
      });

      expect(ok).toBe(false);

      expect(ctx.log.warn).toHaveBeenCalledWith(expect.objectContaining({ err }), 'Merge failed for PR #5');
    });
  });
});
