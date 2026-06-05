import type { WorkflowApprovalCallbacks } from '../application/workflow-approval.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type PullRequestLikeBase = {
  number: number;
  state?: string | null;
  draft?: boolean | null;
  head: {
    sha?: string | null;
  };
  base?: {
    ref?: string | null;
  };
};

type PullRequestFileLikeBase = {
  filename?: string | null;
  status?: string | null;
};

export type WorkflowApprovalCompositionDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileType extends PullRequestFileLikeBase,
> = WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileType>;

export function composeWorkflowApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileType extends PullRequestFileLikeBase,
>(
  dependencies: WorkflowApprovalCompositionDependencies<ContextType, RepoInfoType, PullRequestType, PullRequestFileType>
): WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileType> {
  return {
    isPullRequestOpen: dependencies.isPullRequestOpen,
    isSafeRegistryWorkflowApprovalFile: dependencies.isSafeRegistryWorkflowApprovalFile,
    listChangedFilesForPr: dependencies.listChangedFilesForPr,
    parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
    isSnapshotManagedRequestPr: dependencies.isSnapshotManagedRequestPr,
    evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
    hasAllowedStandaloneDirectPrApprovalForCurrentHead: dependencies.hasAllowedStandaloneDirectPrApprovalForCurrentHead,
    readFreshPullRequest: dependencies.readFreshPullRequest,
    isPlainObject: dependencies.isPlainObject,
    log: dependencies.log,
    getErrorMessage: dependencies.getErrorMessage,
    getHttpStatus: dependencies.getHttpStatus,
    toStringTrim: dependencies.toStringTrim,
  };
}
