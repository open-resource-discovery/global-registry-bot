import { toStringTrim } from '../domain/login-utils.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  head?: { sha?: string | null } | null;
};

const MERGE_INFLIGHT = new Map<string, Promise<void>>();

export function mergeInflightKey(repoInfo: RepoInfo, pr: PullRequestLike): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${pr.number}:${toStringTrim(pr.head?.sha)}`;
}

export async function tryMergeApprovedPrOrUpdateBranch<
  ContextType,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  runMergeApprovedPrOrUpdateBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>
): Promise<void> {
  const key = mergeInflightKey(repoInfo, pr);
  const existing = MERGE_INFLIGHT.get(key);

  if (existing) {
    await existing;
    return;
  }

  const pending = runMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, reason).finally(() => {
    MERGE_INFLIGHT.delete(key);
  });

  MERGE_INFLIGHT.set(key, pending);
  await pending;
}
