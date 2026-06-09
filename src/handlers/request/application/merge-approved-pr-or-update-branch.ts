import {
  isMergeBlockedByBranchProtection,
  shouldTryBranchUpdateAfterMergeFailure,
} from '../domain/merge-failure-errors.js';
import {
  isMergeabilityPending,
  isPullRequestDirty,
  isPullRequestOpen,
  readMergeableState,
} from '../domain/pull-request-merge-state.js';
import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  state?: string | null;
  head?: {
    sha?: string | null;
  } | null;
  base?: {
    ref?: string | null;
  } | null;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
};

type HeadGreenRunSummaryLike = {
  status?: string | null;
};

type HeadGreenEvaluationLike = {
  green: boolean;
  reason: string;
  latestRuns: HeadGreenRunSummaryLike[];
  blockingRuns: HeadGreenRunSummaryLike[];
};

export type MergeApprovedPrOrUpdateBranchCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  waitForPullRequestMergeability: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<PullRequestType>;
  shouldUpdatePullRequestBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<boolean>;
  requestPullRequestBranchUpdateRespectingSequentialRegistryQueue: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string,
    reason: string
  ) => Promise<boolean>;
  hasAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => boolean;
  isPullRequestApprovedForBranchMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options?: { allowLabelFallback?: boolean }
  ) => Promise<boolean>;
  isCrossRepositoryPullRequest: (pr: PullRequestType, repoInfo: RepoInfoType) => boolean;
  evaluateHeadGreenForApprovalReevaluation: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string
  ) => Promise<HeadGreenEvaluationLike>;
  tryMergeIfGreen: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      prNumber: number;
      mergeMethod: 'merge' | 'squash' | 'rebase';
      prData: PullRequestType;
    }
  ) => Promise<boolean | void>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
};

export async function runMergeApprovedPrOrUpdateBranch<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: MergeApprovedPrOrUpdateBranchCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<void> {
  const originalHeadSha = toStringTrim(pr.head?.sha);
  const baseBranch = toStringTrim(pr.base?.ref);

  let currentPr = await callbacks.waitForPullRequestMergeability(context, repoInfo, pr, `${reason}:before-merge`);

  if (!isPullRequestOpen(currentPr)) return;

  const currentHeadSha = toStringTrim(currentPr.head?.sha);

  if (originalHeadSha && currentHeadSha && originalHeadSha !== currentHeadSha) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: currentPr.number,
        originalHeadSha,
        currentHeadSha,
        reason,
      },
      'pull-request head changed before merge, waiting for new CI'
    );

    return;
  }

  if (isPullRequestDirty(currentPr)) {
    callbacks.log(
      context,
      'warn',
      {
        prNumber: currentPr.number,
        mergeableState: readMergeableState(currentPr),
        reason,
      },
      'pull-request has merge conflicts, auto-merge skipped'
    );

    return;
  }

  if (await callbacks.shouldUpdatePullRequestBranch(context, repoInfo, currentPr, baseBranch)) {
    await callbacks.requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      context,
      repoInfo,
      currentPr,
      baseBranch,
      `${reason}:behind-before-merge`
    );
    return;
  }

  const hasCurrentHeadAutoApproval = currentHeadSha
    ? callbacks.hasAutoApprovedPrHead(repoInfo, currentPr.number, currentHeadSha)
    : false;

  const hasMergeApproval =
    hasCurrentHeadAutoApproval ||
    (await callbacks.isPullRequestApprovedForBranchMaintenance(context, repoInfo, currentPr, {
      allowLabelFallback: !callbacks.isCrossRepositoryPullRequest(currentPr, repoInfo),
    }));

  if (!hasMergeApproval) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: currentPr.number,
        headSha: currentHeadSha,
        reason,
      },
      'pull-request merge skipped: no qualifying approval'
    );

    return;
  }

  if (currentHeadSha) {
    const greenResult = await callbacks.evaluateHeadGreenForApprovalReevaluation(context, repoInfo, currentHeadSha);

    if (!greenResult.green) {
      callbacks.log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          headSha: currentHeadSha,
          greenReason: greenResult.reason,
          blockingRuns: greenResult.blockingRuns,
          latestRuns: greenResult.latestRuns.slice(0, 30),
          reason,
        },
        'pull-request merge skipped: current head checks are not green'
      );

      return;
    }

    const pendingRuns = greenResult.latestRuns.filter((run) => toStringTrim(run.status).toLowerCase() !== 'completed');

    if (pendingRuns.length) {
      callbacks.log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          headSha: currentHeadSha,
          pendingRuns,
          reason,
        },
        'pull-request merge skipped: current head checks are still pending'
      );

      return;
    }
  }

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const beforeHeadSha = toStringTrim(currentPr.head?.sha);

    try {
      const merged = await callbacks.tryMergeIfGreen(context, {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        prNumber: currentPr.number,
        mergeMethod: 'squash',
        prData: currentPr,
      });

      const afterMergeAttempt = await callbacks.readFreshPullRequest(context, repoInfo, currentPr.number);
      if (!afterMergeAttempt) return;

      if (!isPullRequestOpen(afterMergeAttempt)) return;

      const afterHeadSha = toStringTrim(afterMergeAttempt.head?.sha);

      if (beforeHeadSha && afterHeadSha && beforeHeadSha !== afterHeadSha) {
        callbacks.log(
          context,
          'info',
          {
            prNumber: currentPr.number,
            beforeHeadSha,
            afterHeadSha,
            reason,
          },
          'pull-request head changed after merge attempt'
        );

        return;
      }

      if (merged === true) return;

      if (merged === false) {
        callbacks.log(
          context,
          'info',
          {
            prNumber: afterMergeAttempt.number,
            headSha: toStringTrim(afterMergeAttempt.head?.sha),
            mergeable: afterMergeAttempt.mergeable,
            mergeableState: readMergeableState(afterMergeAttempt),
            reason,
          },
          'pull-request merge returned false, branch update not requested'
        );

        return;
      }

      currentPr = await callbacks.waitForPullRequestMergeability(
        context,
        repoInfo,
        afterMergeAttempt,
        `${reason}:after-merge-attempt-${attempt}`
      );

      if (!isPullRequestOpen(currentPr)) return;

      if (isPullRequestDirty(currentPr)) {
        callbacks.log(
          context,
          'warn',
          {
            prNumber: currentPr.number,
            mergeableState: readMergeableState(currentPr),
            reason,
          },
          'pull-request has merge conflicts after mergeability refresh'
        );

        return;
      }

      if (await callbacks.shouldUpdatePullRequestBranch(context, repoInfo, currentPr, baseBranch)) {
        await callbacks.requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
          context,
          repoInfo,
          currentPr,
          baseBranch,
          `${reason}:behind-after-merge-attempt`
        );
        return;
      }

      if (attempt < 2 && isMergeabilityPending(currentPr)) {
        continue;
      }

      callbacks.log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          mergeable: currentPr.mergeable,
          mergeableState: readMergeableState(currentPr),
          reason,
        },
        'pull-request not merged after green check'
      );

      return;
    } catch (error: unknown) {
      if (isMergeBlockedByBranchProtection(error, { getErrorMessage: callbacks.getErrorMessage })) {
        callbacks.log(
          context,
          'info',
          {
            prNumber: currentPr.number,
            headSha: toStringTrim(currentPr.head?.sha),
            err: callbacks.getErrorMessage(error),
            status: callbacks.getHttpStatus(error),
            reason,
          },
          'pull-request merge blocked by branch protection'
        );

        return;
      }

      if (shouldTryBranchUpdateAfterMergeFailure(error, { getErrorMessage: callbacks.getErrorMessage })) {
        const freshPr = (await callbacks.readFreshPullRequest(context, repoInfo, currentPr.number)) || currentPr;

        if (await callbacks.shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch)) {
          await callbacks.requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
            context,
            repoInfo,
            freshPr,
            baseBranch,
            `${reason}:merge-failed-outdated`
          );
        } else {
          callbacks.log(
            context,
            'info',
            {
              prNumber: freshPr.number,
              headSha: toStringTrim(freshPr.head?.sha),
              mergeable: freshPr.mergeable,
              mergeableState: readMergeableState(freshPr),
              err: callbacks.getErrorMessage(error),
              status: callbacks.getHttpStatus(error),
              reason,
            },
            'pull-request merge failed, branch update not requested'
          );
        }

        return;
      }

      throw error;
    }
  }
}
