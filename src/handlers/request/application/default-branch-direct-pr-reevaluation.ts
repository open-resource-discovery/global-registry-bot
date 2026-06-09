type RepoInfoBase = { owner: string; repo: string };

type ResourceBotContextBase = {
  resourceBotHooksSource?: string | null;
};

type SequentialRegistryPrResultLike = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export type DefaultBranchDirectPrReevaluationCallbacks<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  ResultType extends SequentialRegistryPrResultLike,
> = {
  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<ResultType>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

export async function reevaluateOpenDirectPullRequestsAfterDefaultBranchPush<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  ResultType extends SequentialRegistryPrResultLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  baseBranch: string,
  callbacks: DefaultBranchDirectPrReevaluationCallbacks<ContextType, RepoInfoType, ResultType>,
  reason = 'default-branch-push:direct-pr-reevaluation'
): Promise<ResultType> {
  callbacks.log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      baseBranch,
      reason,
      hooksSource: context.resourceBotHooksSource,
    },
    'direct-pr-reeval:start'
  );

  const result = await callbacks.runOneSequentialDirectRegistryPrMaintenance(context, repoInfo, baseBranch, reason);

  callbacks.log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      baseBranch,
      reason,
      ...result,
    },
    'direct-pr-reeval:done'
  );

  return result;
}
