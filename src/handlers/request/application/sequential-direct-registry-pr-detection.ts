import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  body?: string | null;
  base?: {
    ref?: string | null;
  } | null;
};

type LogLevel = 'warn';

export type SequentialDirectRegistryPrDetectionCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  pullRequestTargetsBranch: (pr: PullRequestType, branchName: string) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
};

export async function isSequentialDirectRegistryPr<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  baseBranch: string | undefined,
  callbacks: SequentialDirectRegistryPrDetectionCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const targetBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!targetBaseBranch) return false;
  if (callbacks.isSnapshotManagedRequestPr(pr)) return false;
  if (!callbacks.pullRequestTargetsBranch(pr, targetBaseBranch)) return false;

  try {
    const changedRegistryFiles = await callbacks.listChangedYamlFilesForPrWithFallback(
      context,
      repoInfo,
      pr,
      targetBaseBranch
    );
    return changedRegistryFiles.length > 0;
  } catch (error) {
    callbacks.log(
      context,
      'warn',
      {
        prNumber: pr.number,
        baseBranch: targetBaseBranch,
        error: callbacks.getErrorMessage(error),
      },
      'sequential-registry-pr:changed-files-lookup-failed'
    );

    return false;
  }
}
