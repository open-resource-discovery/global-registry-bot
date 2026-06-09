import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestBranchLikeBase = {
  ref?: string | null;
  sha?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  head: PullRequestBranchLikeBase;
  base?: PullRequestBranchLikeBase;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

function pullRequestTargetsBranch<PullRequestType extends PullRequestLikeBase>(
  pr: PullRequestType,
  branchName: string
): boolean {
  const target = toStringTrim(branchName);
  if (!target) return true;

  const prBase = toStringTrim(pr.base?.ref);
  return !prBase || prBase === target;
}

export type DefaultBranchApprovedPrBranchUpdateCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  isSequentialRegistryPrActiveBlocking: (context: ContextType, repoInfo: RepoInfoType) => Promise<boolean>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  isSequentialRegistryPrHeadSkipped: (repoInfo: RepoInfoType, pr: PullRequestType) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  isPullRequestApprovedForBranchMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<boolean>;
  waitForPullRequestMergeability: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<PullRequestType>;
  isPullRequestOpen: (pr: PullRequestType | null | undefined) => boolean;
  isPullRequestDirty: (pr: PullRequestType | null | undefined) => boolean;
  readMergeableState: (pr: PullRequestType | null | undefined) => string;
  shouldUpdatePullRequestBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<boolean>;
  requestPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  getErrorMessage: (error: unknown) => string;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

export async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  baseBranch: string,
  callbacks: DefaultBranchApprovedPrBranchUpdateCallbacks<ContextType, RepoInfoType, PullRequestType>,
  reason = 'default-branch-push'
): Promise<boolean> {
  if (await callbacks.isSequentialRegistryPrActiveBlocking(context, repoInfo)) {
    return false;
  }

  const openPrs = await callbacks.listOpenPullRequests(context, repoInfo);

  for (const pr of openPrs.sort((a, b) => b.number - a.number)) {
    const headSha = toStringTrim(pr.head?.sha);

    if (!headSha) continue;
    if (!pullRequestTargetsBranch(pr, baseBranch)) continue;
    if (callbacks.isSequentialRegistryPrHeadSkipped(repoInfo, pr)) continue;

    try {
      const changedRegistryFiles = await callbacks.listChangedYamlFilesForPrWithFallback(
        context,
        repoInfo,
        pr,
        baseBranch
      );

      if (!changedRegistryFiles.length) {
        callbacks.log(
          context,
          'info',
          { prNumber: pr.number, reason },
          'skip branch update: no registry yaml files changed'
        );
        continue;
      }

      if (!callbacks.isSnapshotManagedRequestPr(pr)) {
        callbacks.log(
          context,
          'info',
          {
            prNumber: pr.number,
            reason,
          },
          'skip branch update: direct registry PR handled by sequential queue'
        );
        continue;
      }

      const approved = await callbacks.isPullRequestApprovedForBranchMaintenance(context, repoInfo, pr);
      if (!approved) {
        callbacks.log(context, 'info', { prNumber: pr.number, reason }, 'skip branch update: PR is not approved');
        continue;
      }

      const freshPr = await callbacks.waitForPullRequestMergeability(
        context,
        repoInfo,
        pr,
        `${reason}:before-update-branch`
      );

      if (!callbacks.isPullRequestOpen(freshPr)) continue;

      if (callbacks.isPullRequestDirty(freshPr)) {
        callbacks.log(
          context,
          'warn',
          {
            prNumber: freshPr.number,
            mergeableState: callbacks.readMergeableState(freshPr),
            reason,
          },
          'skip branch update: PR has merge conflicts'
        );
        continue;
      }

      const mustUpdate = await callbacks.shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch);

      if (!mustUpdate) {
        callbacks.log(
          context,
          'info',
          {
            prNumber: freshPr.number,
            mergeable: freshPr.mergeable,
            mergeableState: callbacks.readMergeableState(freshPr),
            reason,
          },
          'skip branch update: PR is not behind current base'
        );
        continue;
      }

      const requested = await callbacks.requestPullRequestBranchUpdate(context, repoInfo, freshPr, reason);

      if (requested) {
        return true;
      }

      callbacks.markSequentialRegistryPrHeadSkipped(
        context,
        repoInfo,
        freshPr,
        'approved-branch-update-request-failed'
      );
    } catch (error: unknown) {
      callbacks.log(
        context,
        'warn',
        {
          err: callbacks.getErrorMessage(error),
          prNumber: pr.number,
          reason,
        },
        'failed to update approved pull request branch after default branch push'
      );

      callbacks.markSequentialRegistryPrHeadSkipped(context, repoInfo, pr, 'approved-branch-update-exception');
    }
  }

  return false;
}

export async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  baseBranch: string,
  retryDelayMs: number,
  callbacks: DefaultBranchApprovedPrBranchUpdateCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const requested = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
    context,
    repoInfo,
    baseBranch,
    callbacks,
    'default-branch-push'
  );

  if (requested) return true;

  const retryTimer = setTimeout(() => {
    void updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      context,
      repoInfo,
      baseBranch,
      callbacks,
      'default-branch-push:delayed-retry'
    ).catch((error: unknown) => {
      callbacks.log(
        context,
        'warn',
        {
          err: callbacks.getErrorMessage(error),
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          baseBranch,
        },
        'failed to run delayed approved pull request branch update retry'
      );
    });
  }, retryDelayMs);

  if (retryTimer && typeof (retryTimer as { unref?: () => void }).unref === 'function') {
    retryTimer.unref();
  }

  return false;
}
