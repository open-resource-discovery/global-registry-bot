import type { DefaultBranchApprovedPrBranchUpdateCallbacks } from '../application/default-branch-approved-pr-branch-update.js';
import type { DefaultBranchDirectPrReevaluationCallbacks } from '../application/default-branch-direct-pr-reevaluation.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type PullRequestBranchLikeBase = {
  ref?: string | null;
  sha?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  state?: string | null;
  draft?: boolean | null;
  head: PullRequestBranchLikeBase;
  base?: PullRequestBranchLikeBase;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
};

type SequentialRegistryPrResultBase = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

export type DefaultBranchApprovedPrBranchUpdateCompositionDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = DefaultBranchApprovedPrBranchUpdateCallbacks<ContextType, RepoInfoType, PullRequestType>;

export function composeDefaultBranchApprovedPrBranchUpdateCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  dependencies: DefaultBranchApprovedPrBranchUpdateCompositionDependencies<ContextType, RepoInfoType, PullRequestType>
): DefaultBranchApprovedPrBranchUpdateCallbacks<ContextType, RepoInfoType, PullRequestType> {
  return {
    isSequentialRegistryPrActiveBlocking: dependencies.isSequentialRegistryPrActiveBlocking,
    listOpenPullRequests: dependencies.listOpenPullRequests,
    isSequentialRegistryPrHeadSkipped: dependencies.isSequentialRegistryPrHeadSkipped,
    listChangedYamlFilesForPrWithFallback: dependencies.listChangedYamlFilesForPrWithFallback,
    isSnapshotManagedRequestPr: dependencies.isSnapshotManagedRequestPr,
    isPullRequestApprovedForBranchMaintenance: dependencies.isPullRequestApprovedForBranchMaintenance,
    waitForPullRequestMergeability: dependencies.waitForPullRequestMergeability,
    isPullRequestOpen: dependencies.isPullRequestOpen,
    isPullRequestDirty: dependencies.isPullRequestDirty,
    readMergeableState: dependencies.readMergeableState,
    shouldUpdatePullRequestBranch: dependencies.shouldUpdatePullRequestBranch,
    requestPullRequestBranchUpdate: dependencies.requestPullRequestBranchUpdate,
    markSequentialRegistryPrHeadSkipped: dependencies.markSequentialRegistryPrHeadSkipped,
    getErrorMessage: dependencies.getErrorMessage,
    log: dependencies.log,
  };
}

type ResourceBotContextBase = {
  resourceBotHooksSource?: string | null;
};

export type DefaultBranchDirectPrReevaluationCompositionDependencies<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
> = DefaultBranchDirectPrReevaluationCallbacks<ContextType, RepoInfoType, SequentialRegistryPrResultType>;

export function composeDefaultBranchDirectPrReevaluationCallbacks<
  ContextType extends ResourceBotContextBase,
  RepoInfoType extends RepoInfoBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
>(
  dependencies: DefaultBranchDirectPrReevaluationCompositionDependencies<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  >
): DefaultBranchDirectPrReevaluationCallbacks<ContextType, RepoInfoType, SequentialRegistryPrResultType> {
  return {
    runOneSequentialDirectRegistryPrMaintenance: dependencies.runOneSequentialDirectRegistryPrMaintenance,
    log: dependencies.log,
  };
}
