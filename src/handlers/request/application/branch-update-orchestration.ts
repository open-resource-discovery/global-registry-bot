import type { BranchUpdateBenignRetryOutcome } from './branch-update-benign-retry.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  head?: { sha?: string | null } | null;
};

type BranchUpdateLogLevel = 'info' | 'warn';

export type BranchUpdateOrchestrationCallbacks<ContextType, RepoInfoType, PullRequestType> = {
  updateBranchInflightKey: (repoInfo: RepoInfoType, pr: PullRequestType) => string;
  getUpdateBranchInflight: (key: string) => Promise<boolean> | undefined;
  setUpdateBranchInflight: (key: string, pending: Promise<boolean>) => void;
  clearUpdateBranchInflight: (key: string) => void;
  isUpdateBranchCooldownActive: (key: string) => boolean;
  markUpdateBranchCooldown: (key: string) => void;
  callPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    expectedHeadSha?: string
  ) => Promise<void>;
  isBenignUpdateBranchFailure: (error: unknown) => boolean;
  isManualUpdateBranchFailure: (error: unknown) => boolean;
  runBranchUpdateBenignFailureRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    headSha: string
  ) => Promise<BranchUpdateBenignRetryOutcome>;
  postManualBranchUpdateNotice: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    message: string
  ) => Promise<void>;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  log: (context: ContextType, level: BranchUpdateLogLevel, obj: unknown, msg: string) => void;
  toStringTrim: (value: unknown) => string;
};

export async function requestPullRequestBranchUpdate<
  ContextType,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: BranchUpdateOrchestrationCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const headSha = callbacks.toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = callbacks.updateBranchInflightKey(repoInfo, pr);

  if (callbacks.isUpdateBranchCooldownActive(key)) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
        reason,
      },
      'pull-request branch update skipped: cooldown active'
    );

    return false;
  }

  const existing = callbacks.getUpdateBranchInflight(key);
  if (existing) return await existing;

  const pending = (async (): Promise<boolean> => {
    try {
      await callbacks.callPullRequestBranchUpdate(context, repoInfo, pr.number, headSha);

      callbacks.markUpdateBranchCooldown(key);

      callbacks.log(
        context,
        'info',
        {
          prNumber: pr.number,
          headSha,
          reason,
        },
        'pull-request branch update requested'
      );

      return true;
    } catch (error: unknown) {
      const msg = callbacks.getErrorMessage(error);
      const status = callbacks.getHttpStatus(error);

      if (callbacks.isBenignUpdateBranchFailure(error)) {
        const retryOutcome = await callbacks.runBranchUpdateBenignFailureRetry(context, repoInfo, pr.number, headSha);
        const { freshHeadSha, freshMergeableState } = retryOutcome;

        if (retryOutcome.outcome === 'head-changed') {
          callbacks.log(
            context,
            'info',
            {
              prNumber: pr.number,
              oldHeadSha: headSha,
              freshHeadSha,
              status,
              err: msg,
              reason,
              freshMergeableState,
            },
            'pull-request branch update skipped: head already changed'
          );

          return false;
        }

        if (retryOutcome.outcome === 'retry-success') {
          callbacks.markUpdateBranchCooldown(key);

          callbacks.log(
            context,
            'info',
            {
              prNumber: pr.number,
              headSha,
              freshHeadSha,
              reason,
            },
            'pull-request branch update requested after expected-head retry'
          );

          return true;
        }

        if (retryOutcome.outcome === 'retry-failed') {
          callbacks.markUpdateBranchCooldown(key);

          callbacks.log(
            context,
            'warn',
            {
              prNumber: pr.number,
              headSha,
              freshHeadSha,
              status: retryOutcome.retryErrorStatus,
              err: retryOutcome.retryErrorMessage,
              originalStatus: status,
              originalErr: msg,
              reason,
              freshMergeableState,
            },
            'pull-request branch update retry failed'
          );

          return false;
        }

        callbacks.log(
          context,
          'info',
          {
            prNumber: pr.number,
            oldHeadSha: headSha,
            freshHeadSha,
            status,
            err: msg,
            reason,
            freshMergeableState,
          },
          'pull-request branch update skipped after benign failure'
        );

        return false;
      }

      callbacks.log(
        context,
        'warn',
        {
          prNumber: pr.number,
          headSha,
          status,
          err: msg,
          reason,
        },
        'pull-request branch update failed'
      );

      if (callbacks.isManualUpdateBranchFailure(error)) {
        await callbacks.postManualBranchUpdateNotice(context, repoInfo, pr.number, msg);
      }

      return false;
    }
  })().finally(() => {
    callbacks.clearUpdateBranchInflight(key);
  });

  callbacks.setUpdateBranchInflight(key, pending);
  return await pending;
}
