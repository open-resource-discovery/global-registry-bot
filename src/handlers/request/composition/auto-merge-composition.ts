import type { AutoMergeTriggerCallbacks } from '../application/auto-merge-trigger.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type PullRequestLikeBase = {
  number: number;
  head?: {
    sha?: string | null;
  } | null;
  base?: {
    ref?: string | null;
  } | null;
};

type SequentialRegistryPrActiveLike = {
  prNumber: number;
};

type HeadGreenEvaluationLike = {
  green: boolean;
  reason: string;
  latestRuns: unknown[];
  blockingRuns: unknown[];
  statusState?: string;
};

export type AutoMergeTriggerCompositionDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
> = AutoMergeTriggerCallbacks<ContextType, RepoInfoType, PullRequestType, ActiveStateType, HeadGreenEvaluationType>;

export function composeAutoMergeTriggerCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  dependencies: AutoMergeTriggerCompositionDependencies<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): AutoMergeTriggerCallbacks<ContextType, RepoInfoType, PullRequestType, ActiveStateType, HeadGreenEvaluationType> {
  return {
    getStaticConfig: dependencies.getStaticConfig,
    evaluateHeadGreenForApprovalReevaluation: dependencies.evaluateHeadGreenForApprovalReevaluation,
    listOpenPullRequests: dependencies.listOpenPullRequests,
    processPullRequestForAutoMerge: dependencies.processPullRequestForAutoMerge,
    releaseSequentialRegistryPrIfNotApprovedAfterGreen: dependencies.releaseSequentialRegistryPrIfNotApprovedAfterGreen,
    advanceSequentialRegistryPrQueueAfterTerminalState: dependencies.advanceSequentialRegistryPrQueueAfterTerminalState,
    readFreshPullRequest: dependencies.readFreshPullRequest,
    isSequentialDirectRegistryPr: dependencies.isSequentialDirectRegistryPr,
    getSequentialRegistryPrActive: dependencies.getSequentialRegistryPrActive,
    clearSequentialRegistryPrActive: dependencies.clearSequentialRegistryPrActive,
    markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
    runOneSequentialDirectRegistryPrMaintenance: dependencies.runOneSequentialDirectRegistryPrMaintenance,
    log: dependencies.log,
  };
}
