import {
  isBenignUpdateBranchFailure,
  isManualUpdateBranchFailure,
  type BranchUpdateErrorClassificationCallbacks,
} from '../domain/branch-update-errors.js';
import { toStringTrim } from '../domain/login-utils.js';
import {
  clearUpdateBranchInflight,
  getUpdateBranchInflight,
  setUpdateBranchInflight,
} from './branch-update-inflight.js';
import {
  callPullRequestBranchUpdate,
  type PullRequestBranchUpdateCallContext,
} from './pull-request-branch-update-call.js';
import { postManualBranchUpdateNotice, type PostOnceContext } from './branch-update-manual-notice.js';
import type { BranchUpdateBenignRetryOutcome } from './branch-update-benign-retry.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  head?: { sha?: string | null } | null;
};

type BranchUpdateLogLevel = 'info' | 'warn';

export type BranchUpdateOrchestrationCallbacks<ContextType, RepoInfoType, PullRequestType> = {
  updateBranchInflightKey: (repoInfo: RepoInfoType, pr: PullRequestType) => string;
  isUpdateBranchCooldownActive: (key: string) => boolean;
  markUpdateBranchCooldown: (key: string) => void;
  runBranchUpdateBenignFailureRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    headSha: string
  ) => Promise<BranchUpdateBenignRetryOutcome>;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  log: (context: ContextType, level: BranchUpdateLogLevel, obj: unknown, msg: string) => void;
};

export async function requestPullRequestBranchUpdate<
  ContextType extends PullRequestBranchUpdateCallContext & PostOnceContext,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: BranchUpdateOrchestrationCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
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

  const existing = getUpdateBranchInflight(key);
  if (existing) return await existing;

  const pending = (async (): Promise<boolean> => {
    try {
      await callPullRequestBranchUpdate(context, repoInfo, pr.number, headSha);

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

      if (isBenignUpdateBranchFailure(error, callbacks satisfies BranchUpdateErrorClassificationCallbacks)) {
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

      if (isManualUpdateBranchFailure(error, callbacks satisfies BranchUpdateErrorClassificationCallbacks)) {
        await postManualBranchUpdateNotice(context, repoInfo, pr.number, msg);
      }

      return false;
    }
  })().finally(() => {
    clearUpdateBranchInflight(key);
  });

  setUpdateBranchInflight(key, pending);
  return await pending;
}
