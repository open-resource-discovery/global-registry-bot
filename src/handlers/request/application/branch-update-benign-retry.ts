import { evaluateBranchUpdateRefreshOutcome } from '../domain/branch-update-refresh-outcome.js';
import { planBranchUpdateRetryAfterRefresh } from '../domain/branch-update-retry-plan.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  mergeable_state?: string | null;
  head?: { sha?: string | null } | null;
};

export type BranchUpdateBenignRetryCallbacks<ContextType, PullRequestType> = {
  readFreshPullRequest: (context: ContextType, repoInfo: RepoInfo, prNumber: number) => Promise<PullRequestType | null>;
  readMergeableState: (pr: PullRequestType | null | undefined) => string;
  isPullRequestBehindBase: (pr: PullRequestType | null | undefined) => boolean;
  delayMs: (ms: number) => Promise<void>;
  callPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfo,
    prNumber: number,
    expectedHeadSha?: string
  ) => Promise<void>;
  getHttpStatus: (error: unknown) => number | undefined;
  getErrorMessage: (error: unknown) => string;
};

export type BranchUpdateBenignRetryOutcome =
  | {
      outcome: 'head-changed';
      freshHeadSha: string;
      freshMergeableState: string;
    }
  | {
      outcome: 'retry-success';
      freshHeadSha: string;
      freshMergeableState: string;
    }
  | {
      outcome: 'retry-failed';
      freshHeadSha: string;
      freshMergeableState: string;
      retryErrorStatus: number | undefined;
      retryErrorMessage: string;
    }
  | {
      outcome: 'skip-not-behind';
      freshHeadSha: string;
      freshMergeableState: string;
    };

export async function runBranchUpdateBenignFailureRetry<ContextType, PullRequestType extends PullRequestLike>(
  context: ContextType,
  repoInfo: RepoInfo,
  prNumber: number,
  headSha: string,
  retryDelayMs: number,
  callbacks: BranchUpdateBenignRetryCallbacks<ContextType, PullRequestType>
): Promise<BranchUpdateBenignRetryOutcome> {
  const fresh = await callbacks.readFreshPullRequest(context, repoInfo, prNumber);
  const refreshOutcome = evaluateBranchUpdateRefreshOutcome(headSha, fresh, {
    readMergeableState: callbacks.readMergeableState,
    isPullRequestBehindBase: callbacks.isPullRequestBehindBase,
  });
  const { freshHeadSha, freshMergeableState } = refreshOutcome;
  const retryPlan = planBranchUpdateRetryAfterRefresh(refreshOutcome);

  if (retryPlan.action === 'skip-head-changed') {
    return {
      outcome: 'head-changed',
      freshHeadSha,
      freshMergeableState,
    };
  }

  if (retryPlan.action === 'retry') {
    await callbacks.delayMs(retryDelayMs);

    try {
      await callbacks.callPullRequestBranchUpdate(context, repoInfo, prNumber);

      return {
        outcome: 'retry-success',
        freshHeadSha,
        freshMergeableState,
      };
    } catch (retryError: unknown) {
      return {
        outcome: 'retry-failed',
        freshHeadSha,
        freshMergeableState,
        retryErrorStatus: callbacks.getHttpStatus(retryError),
        retryErrorMessage: callbacks.getErrorMessage(retryError),
      };
    }
  }

  return {
    outcome: 'skip-not-behind',
    freshHeadSha,
    freshMergeableState,
  };
}
