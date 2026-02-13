export type MergeMethodRest = 'merge' | 'squash' | 'rebase';
export type MergeMethodGraphql = 'MERGE' | 'SQUASH' | 'REBASE';

type LoggerLike = {
  info: (msg: string) => void;
  warn: (obj: unknown, msg: string) => void;
};

type PullRequestMinimal = {
  number: number;
  node_id: string;
  state: 'open' | 'closed' | string;
  draft?: boolean;
  head: { sha: string };
};

type OctokitLike = {
  graphql: (query: string, variables: Record<string, unknown>) => Promise<unknown>;
  pulls: {
    get: (args: { owner: string; repo: string; pull_number: number }) => Promise<{ data: PullRequestMinimal }>;
    merge: (args: {
      owner: string;
      repo: string;
      pull_number: number;
      merge_method: MergeMethodRest;
    }) => Promise<unknown>;
    listReviews: (args: { owner: string; repo: string; pull_number: number }) => Promise<{ data: { state: string }[] }>;
  };
  repos: {
    getCombinedStatusForRef: (args: { owner: string; repo: string; ref: string }) => Promise<{
      data: { state: 'success' | 'pending' | 'failure' | 'error' | string; total_count: number };
    }>;
  };
  checks: {
    listForRef: (args: { owner: string; repo: string; ref: string }) => Promise<{
      data: { total_count: number; check_runs: { conclusion: string | null }[] };
    }>;
  };
};

export type ContextLike = {
  octokit: OctokitLike;
  log: LoggerLike;
};

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e ?? '');
}

const OK_CHECK_CONCLUSIONS = new Set(['success', 'skipped', 'neutral']); // per GitHub docs, other values mean not-ok

export async function tryEnableAutoMerge(
  context: ContextLike,
  pr: Pick<PullRequestMinimal, 'node_id' | 'number'>,
  opts: { mergeMethod?: MergeMethodGraphql } = {}
): Promise<boolean> {
  const mergeMethod = opts.mergeMethod ?? 'SQUASH';

  try {
    await context.octokit.graphql(
      `mutation EnableAutoMerge($prId:ID!, $mergeMethodVar: PullRequestMergeMethod!) {
        enablePullRequestAutoMerge(input:{ pullRequestId: $prId, mergeMethod: $mergeMethodVar }) {
          clientMutationId
        }
      }`,
      { prId: pr.node_id, mergeMethodVar: mergeMethod }
    );

    context.log.info(`Auto-merge enabled for PR #${pr.number} (${mergeMethod})`);
    return true;
  } catch (e: unknown) {
    const msg = errorMessage(e);

    if (/clean status/i.test(msg)) {
      context.log.info(
        `PR #${pr.number}: auto-merge enable responded 'clean status' → will try immediate merge fallback.`
      );
      return false;
    }

    context.log.warn({ err: e }, `Auto-merge enable failed for PR #${pr.number}`);
    return false;
  }
}

export async function tryMergeIfGreen(
  context: ContextLike,
  args: {
    owner: string;
    repo: string;
    prNumber: number;
    mergeMethod?: MergeMethodRest;
    requireApproval?: boolean;
    prData?: PullRequestMinimal;
  }
): Promise<boolean> {
  const { owner, repo, prNumber } = args;
  const mergeMethod = args.mergeMethod ?? 'squash';
  const requireApproval = args.requireApproval ?? false;

  let pr: PullRequestMinimal | undefined = args.prData;

  if (!pr) {
    const { data } = await context.octokit.pulls.get({ owner, repo, pull_number: prNumber });
    pr = data;
  }

  if (!pr || pr.state !== 'open') return false;
  if (pr.draft) return false;

  // Combined status (commit statuses)
  const { data: combined } = await context.octokit.repos.getCombinedStatusForRef({
    owner,
    repo,
    ref: pr.head.sha,
  });

  // IMPORTANT: pending if there are no statuses -> treat total_count=0 as OK
  const allStatusesOk = combined.total_count === 0 || combined.state === 'success';

  // Checks API
  const checks = await context.octokit.checks.listForRef({
    owner,
    repo,
    ref: pr.head.sha,
  });

  const allChecksOk =
    checks.data.total_count === 0 ||
    checks.data.check_runs.every((c) => OK_CHECK_CONCLUSIONS.has(String(c.conclusion || '')));

  // require at least one APPROVED review
  let approved = true;
  if (requireApproval) {
    const { data: reviews } = await context.octokit.pulls.listReviews({
      owner,
      repo,
      pull_number: prNumber,
    });
    approved = reviews.some((r) => r.state === 'APPROVED');
  }

  if (allStatusesOk && allChecksOk && approved) {
    try {
      await context.octokit.pulls.merge({
        owner,
        repo,
        pull_number: prNumber,
        merge_method: mergeMethod, // merge|squash|rebase
      });
      context.log.info(`PR #${prNumber} merged (all checks green, method=${mergeMethod})`);
      return true;
    } catch (e: unknown) {
      context.log.warn({ err: e }, `Merge failed for PR #${prNumber}`);
    }
  } else {
    context.log.info(
      `PR #${prNumber} not merged yet (statusesOk=${allStatusesOk}, checksOk=${allChecksOk}, approved=${approved})`
    );
  }

  return false;
}
