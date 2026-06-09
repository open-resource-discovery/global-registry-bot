import type { DirectPrApprovalCommentHandlingCallbacks } from '../application/direct-pr-approval-comment-handling.js';
import type { StandaloneDirectPrApprovalCallbacks } from '../application/direct-pr-standalone-approval.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type PullRequestLikeBase = {
  number: number;
};

type IssueLikeBase = {
  number: number;
};

type EffectiveConstantsBase = {
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

export type StandaloneDirectPrApprovalCompositionDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ReviewHandoverOptionsType,
> = StandaloneDirectPrApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, ReviewHandoverOptionsType>;

export function composeStandaloneDirectPrApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ReviewHandoverOptionsType,
>(
  dependencies: StandaloneDirectPrApprovalCompositionDependencies<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ReviewHandoverOptionsType
  >
): StandaloneDirectPrApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, ReviewHandoverOptionsType> {
  return {
    evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
    hasAllowedStandaloneDirectPrApprovalForCurrentHead: dependencies.hasAllowedStandaloneDirectPrApprovalForCurrentHead,
    ensureAutomatedApprovalReviewForCurrentHead: dependencies.ensureAutomatedApprovalReviewForCurrentHead,
    postApprovalRejectedOnce: dependencies.postApprovalRejectedOnce,
    hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr:
      dependencies.hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr,
    addApprovedLabelToPr: dependencies.addApprovedLabelToPr,
    handoverStandaloneDirectPrToReview: dependencies.handoverStandaloneDirectPrToReview,
    isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
    buildStandaloneDirectPrReviewHandoverOptions: dependencies.buildStandaloneDirectPrReviewHandoverOptions,
    log: dependencies.log,
  };
}

export type DirectPrApprovalCommentCompositionDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  PullRequestType extends PullRequestLikeBase,
  IssueType extends IssueLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = Omit<
  DirectPrApprovalCommentHandlingCallbacks<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    PullRequestType,
    IssueType,
    EffectiveConstantsType
  >,
  'buildIssueParams'
>;

export function composeDirectPrApprovalCommentHandlingCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  PullRequestType extends PullRequestLikeBase,
  IssueType extends IssueLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  dependencies: DirectPrApprovalCommentCompositionDependencies<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    PullRequestType,
    IssueType,
    EffectiveConstantsType
  >
): DirectPrApprovalCommentHandlingCallbacks<
  ContextType,
  RepoInfoType,
  IssueParamsType,
  PullRequestType,
  IssueType,
  EffectiveConstantsType
> {
  return {
    resolveEffectiveConstants: dependencies.resolveEffectiveConstants,

    buildIssueParams: (repoInfo: RepoInfoType, pr: PullRequestType): IssueParamsType => {
      const params: IssueParamsBase = {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        issue_number: pr.number,
      };
      return params as unknown as IssueParamsType;
    },

    prAsIssueLike: dependencies.prAsIssueLike,
    ensureReviewLabelsPresentOnIssue: dependencies.ensureReviewLabelsPresentOnIssue,
    resolveDirectPrRequestTypes: dependencies.resolveDirectPrRequestTypes,
    resolveAllowedApproversForRequestTypes: dependencies.resolveAllowedApproversForRequestTypes,
    evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
    postApprovalRejectedOnce: dependencies.postApprovalRejectedOnce,
    isAuthorizedApprover: dependencies.isAuthorizedApprover,
    ensureAutomatedApprovalReviewForCurrentHead: dependencies.ensureAutomatedApprovalReviewForCurrentHead,
    isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
    tryMergeApprovedPrOrUpdateBranch: dependencies.tryMergeApprovedPrOrUpdateBranch,
    postOnce: dependencies.postOnce,
    log: dependencies.log,
  };
}
