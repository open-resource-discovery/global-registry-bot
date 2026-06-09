import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  base?: {
    ref?: string | null;
  } | null;
};

type SequentialRegistryPrActiveLike = {
  prNumber: number;
};

type SequentialRegistryPrResultLike = {
  updated: boolean;
};

export type BranchUpdateSequentialHandoffCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
> = {
  isSequentialDirectRegistryPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<boolean>;
  requestPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  getSequentialRegistryPrActive: (repoInfo: RepoInfoType) => ActiveStateType;
  markSequentialRegistryPrActive: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<SequentialRegistryPrResultLike>;
};

export async function requestPullRequestBranchUpdateRespectingSequentialRegistryQueue<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  baseBranch: string,
  reason: string,
  callbacks: BranchUpdateSequentialHandoffCallbacks<ContextType, RepoInfoType, PullRequestType, ActiveStateType>
): Promise<boolean> {
  const targetBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);

  if (!(await callbacks.isSequentialDirectRegistryPr(context, repoInfo, pr, targetBaseBranch))) {
    return await callbacks.requestPullRequestBranchUpdate(context, repoInfo, pr, reason);
  }

  const active = callbacks.getSequentialRegistryPrActive(repoInfo);

  if (active && active.prNumber === pr.number) {
    const requested = await callbacks.requestPullRequestBranchUpdate(context, repoInfo, pr, reason);

    if (requested) {
      callbacks.markSequentialRegistryPrActive(context, repoInfo, pr, reason);
    }

    return requested;
  }

  const result = await callbacks.runOneSequentialDirectRegistryPrMaintenance(
    context,
    repoInfo,
    targetBaseBranch,
    reason
  );
  return result.updated;
}
