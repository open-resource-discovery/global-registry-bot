import { type LogLevel } from '../infrastructure/logger.js';
import { isPlainObject } from '../infrastructure/errors.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';
import { getUnknownManualApprovers, getVisibleApprovalText } from '../domain/approval-policy.js';
import { rejectRequestFromApprovalHook } from '../application/approval-rejection.js';
import { postApprovalRejectedOnce, postApprovalUnknownOnce } from '../application/approval-outcome-posting.js';
import { handoverStandaloneDirectPrToReview } from '../application/pr-review-handover.js';
import { setStateLabel, ensureAssigneesOnce } from '../state.js';
import {
  ensureAutomatedApprovalReviewForCurrentHead as ensureAutomatedApprovalReviewForCurrentHeadApplication,
  type AutomatedApprovalReviewCallbacks,
  type AutomatedApprovalReviewOptions,
} from '../application/automated-approval-review.js';
import {
  hasAutoApprovalReviewForHead as hasAutoApprovalReviewForHeadApplication,
  type AutoApprovalReviewDetectionCallbacks,
} from '../application/auto-approval-review-detection.js';
import {
  maybeHandleStandaloneDirectPrApproval as maybeHandleStandaloneDirectPrApprovalApplication,
  type StandaloneDirectPrApprovalCallbacks,
} from '../application/direct-pr-standalone-approval.js';
import {
  maybeHandleDirectPrApprovalForMerge as maybeHandleDirectPrApprovalForMergeApplication,
  type DirectPrLinkedIssueApprovalCallbacks,
} from '../application/direct-pr-linked-issue-approval.js';
import {
  handleDirectPrApprovalComment as handleDirectPrApprovalCommentApplication,
  type DirectPrApprovalCommentHandlingCallbacks,
} from '../application/direct-pr-approval-comment-handling.js';
import {
  composeStandaloneDirectPrApprovalCallbacks,
  composeDirectPrApprovalCommentHandlingCallbacks,
} from './issue-comment-direct-pr-composition.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: unknown;
  user?: UserLikeBase | null;
};

type PullRequestLikeBase = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: UserLikeBase | null;
  head: { ref: string; sha?: string | null };
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  globalLabels: string[];
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type PullRequestReviewLikeBase = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  submitted_at?: string | null;
  user?: UserLikeBase | null;
  commit_id?: string | null;
};

type DirectPrApprovalOptionsBase = {
  baseBranch?: string;
};

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type StandaloneDirectPrReviewHandoverOptions<
  ContextType,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  prAsIssueLike: (pr: PullRequestType) => IssueType;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  resolveDirectPrRequestTypes: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    pr: PullRequestType,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<string[]>;
  getUnknownManualApprovers: (decision: ApprovalDecision) => string[];
  resolveReviewAssigneesForRequestTypes: (context: ContextType, issue: IssueType, requestTypes: string[]) => string[];
  ensureAssigneesPresent: (context: ContextType, params: IssueParamsBase, assignees: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: IssueParamsBase, labels: string[]) => Promise<void>;
  calcStandaloneDirectPrSnapshotHash: (pr: PullRequestType, changedFiles: string[]) => string;
  buildReviewHandoverBody: (
    context: ContextType,
    snapshotHash?: string,
    options?: { target?: 'issue' | 'pull_request' }
  ) => string;
  toStringTrim: (value: unknown) => string;
  logHandover: (args: {
    context: ContextType;
    prNumber: number;
    requestTypes: string[];
    changedFiles: string[];
    assignees: string[];
    snapshotHash: string;
    decisionStatus: string;
  }) => void;
};

type ContextLikeBase = Parameters<typeof postApprovalRejectedOnce>[0];
type SetStateLabelContextBase = Parameters<typeof setStateLabel>[0];
type EnsureAssigneesOnceContextBase = Parameters<typeof ensureAssigneesOnce>[0];

type DirectPrRuntimeContextBase<PullRequestType extends PullRequestLikeBase> = ContextLikeBase &
  SetStateLabelContextBase &
  EnsureAssigneesOnceContextBase & {
    state?: Record<string, unknown>;
    octokit: {
      rest: {
        pulls: {
          createReview: (args: {
            owner: string;
            repo: string;
            pull_number: number;
            event: 'APPROVE';
            body: string;
          }) => Promise<unknown>;
          update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
          list: (args: {
            owner: string;
            repo: string;
            state: 'open' | 'closed' | 'all';
            per_page?: number;
            page?: number;
          }) => Promise<{ data: PullRequestType[] }>;
        };
        issues: {
          update: (args: IssueParamsBase & { state: 'closed' }) => Promise<unknown>;
          removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
        };
      };
    };
  };

type DirectPrRuntimeDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  toStringTrim: (value: unknown) => string;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  prAsIssueLike: (pr: PullRequestType) => IssueType;
  isCrossRepositoryPullRequest: (pr: PullRequestType, repoInfo: RepoInfoType) => boolean;
  isAuthorizedApprover: (
    commenter: string,
    issueAuthor: string | undefined | null,
    allowedApprovers: string[]
  ) => boolean;
  postOnce: (
    context: ContextType,
    params: IssueParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;

  hasAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => boolean;
  markAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => void;
  autoApprovedPrHeadKey: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => string;
  addApprovedLabelToPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    options?: { skipStateCleanup?: boolean }
  ) => Promise<void>;
  buildAutoApprovalReviewMarker: (headSha: string) => string;
  listPullRequestReviews: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    prNumber: number
  ) => Promise<PullRequestReviewLikeBase[]>;

  resolveDirectPrRequestTypes: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    pr: PullRequestType,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<string[]>;
  resolveAllowedApproversForRequestTypes: (context: ContextType, requestTypes: string[]) => string[];
  evaluateDirectPrOnApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    requestAuthorIdOverride?: string,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<ApprovalDecision>;
  hasAllowedStandaloneDirectPrApprovalForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<boolean>;
  hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<boolean>;

  resolvePullRequestRequestAuthorId: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<string>;
  applyApprovedRequestState: (
    context: ContextType,
    params: IssueParamsType,
    eff: EffectiveConstantsType
  ) => Promise<void>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoBase) => Promise<PullRequestType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoBase) => number | null;
  ensureReviewLabelsPresentOnIssue: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    eff: EffectiveConstantsType
  ) => Promise<boolean>;
  tryMergeApprovedPrOrUpdateBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;

  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  resolveReviewAssigneesForRequestTypes: (context: ContextType, issue: IssueType, requestTypes: string[]) => string[];
  ensureAssigneesPresent: (context: ContextType, params: IssueParamsBase, assignees: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: IssueParamsBase, labels: string[]) => Promise<void>;
  calcStandaloneDirectPrSnapshotHash: (pr: PullRequestType, changedFiles: string[]) => string;
  buildReviewHandoverBody: (
    context: ContextType,
    snapshotHash?: string,
    options?: { target?: 'issue' | 'pull_request' }
  ) => string;
};

type DirectPrRuntime<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
> = {
  buildStandaloneDirectPrReviewHandoverOptions: () => StandaloneDirectPrReviewHandoverOptions<
    ContextType,
    IssueType,
    PullRequestType,
    EffectiveConstantsType
  >;
  maybeHandleStandaloneDirectPrApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<ApprovalHandlingResult>;
  handleDirectPrApprovalComment: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    commenter: string
  ) => Promise<void>;
  maybeHandleDirectPrApprovalForMerge: (
    context: ContextType,
    repoInfo: RepoInfoType,
    issueParams: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    pr: PullRequestType
  ) => Promise<ApprovalHandlingResult>;
  ensureAutomatedApprovalReviewForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: AutomatedApprovalReviewOptions
  ) => Promise<boolean>;
};

export function createDirectPrRuntime<
  ContextType extends DirectPrRuntimeContextBase<PullRequestType>,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  dependencies: DirectPrRuntimeDependencies<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    IssueType,
    PullRequestType,
    EffectiveConstantsType
  >
): DirectPrRuntime<
  ContextType,
  RepoInfoType,
  IssueParamsType,
  IssueType,
  PullRequestType,
  EffectiveConstantsType,
  TemplateType,
  FormDataType
> {
  function buildAutomatedApprovalReviewCallbacks(): AutomatedApprovalReviewCallbacks<ContextType, RepoInfoType> {
    return {
      toStringTrim: dependencies.toStringTrim,
      isPlainObject,
      getVisibleApprovalText,
      hasAutoApprovedPrHead: dependencies.hasAutoApprovedPrHead,
      hasAutoApprovalReviewForHead,
      markAutoApprovedPrHead: dependencies.markAutoApprovedPrHead,
      addApprovedLabelToPr: dependencies.addApprovedLabelToPr,
      autoApprovedPrHeadKey: dependencies.autoApprovedPrHeadKey,
      logCreated: (context: ContextType, prNumber: number, headSha: string): void => {
        dependencies.log(
          context,
          'info',
          {
            prNumber,
            headSha,
          },
          'automated PR approval review created'
        );
      },
      logCreateFailed: (
        context: ContextType,
        prNumber: number,
        status: number | undefined,
        message: string,
        responseData: unknown
      ): void => {
        dependencies.log(
          context,
          'warn',
          {
            prNumber,
            status,
            message,
            responseData,
          },
          'failed to create automated PR approval review'
        );
      },
      logDedupedInFlight: (context: ContextType, prNumber: number, headSha: string): void => {
        dependencies.log(
          context,
          'info',
          {
            prNumber,
            headSha,
          },
          'automated PR approval review deduped: already in flight'
        );
      },
    };
  }

  async function ensureAutomatedApprovalReviewForCurrentHead(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options: AutomatedApprovalReviewOptions = {}
  ): Promise<boolean> {
    return await ensureAutomatedApprovalReviewForCurrentHeadApplication(
      context,
      repoInfo,
      pr,
      decision,
      options,
      buildAutomatedApprovalReviewCallbacks()
    );
  }

  function buildAutoApprovalReviewDetectionCallbacks(): AutoApprovalReviewDetectionCallbacks<ContextType> {
    return {
      buildAutoApprovalReviewMarker: dependencies.buildAutoApprovalReviewMarker,
      listPullRequestReviews: dependencies.listPullRequestReviews,
      toStringTrim: dependencies.toStringTrim,
    };
  }

  async function hasAutoApprovalReviewForHead(
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    headSha: string
  ): Promise<boolean> {
    return await hasAutoApprovalReviewForHeadApplication(
      context,
      repoInfo,
      prNumber,
      headSha,
      buildAutoApprovalReviewDetectionCallbacks()
    );
  }

  function buildStandaloneDirectPrReviewHandoverOptions(): StandaloneDirectPrReviewHandoverOptions<
    ContextType,
    IssueType,
    PullRequestType,
    EffectiveConstantsType
  > {
    return {
      resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
      prAsIssueLike: dependencies.prAsIssueLike,
      listChangedYamlFilesForPrWithFallback: dependencies.listChangedYamlFilesForPrWithFallback,
      resolveDirectPrRequestTypes: dependencies.resolveDirectPrRequestTypes,
      getUnknownManualApprovers,
      resolveReviewAssigneesForRequestTypes: dependencies.resolveReviewAssigneesForRequestTypes,
      ensureAssigneesPresent: dependencies.ensureAssigneesPresent,
      ensureLabelsPresentOnce: dependencies.ensureLabelsPresentOnce,
      calcStandaloneDirectPrSnapshotHash: dependencies.calcStandaloneDirectPrSnapshotHash,
      buildReviewHandoverBody: dependencies.buildReviewHandoverBody,
      toStringTrim: dependencies.toStringTrim,
      logHandover: ({
        context,
        prNumber,
        requestTypes,
        changedFiles,
        assignees,
        snapshotHash,
        decisionStatus,
      }): void => {
        dependencies.log(
          context,
          'info',
          {
            prNumber,
            requestTypes,
            changedFiles,
            assignees,
            snapshotHash,
            decisionStatus,
          },
          'direct-pr:handover-to-review'
        );
      },
    };
  }

  function buildStandaloneDirectPrApprovalCallbacks(): StandaloneDirectPrApprovalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ReturnType<typeof buildStandaloneDirectPrReviewHandoverOptions>
  > {
    return composeStandaloneDirectPrApprovalCallbacks<
      ContextType,
      RepoInfoType,
      PullRequestType,
      ReturnType<typeof buildStandaloneDirectPrReviewHandoverOptions>
    >({
      evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
      hasAllowedStandaloneDirectPrApprovalForCurrentHead:
        dependencies.hasAllowedStandaloneDirectPrApprovalForCurrentHead,
      ensureAutomatedApprovalReviewForCurrentHead,
      postApprovalRejectedOnce,
      hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr:
        dependencies.hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr,
      addApprovedLabelToPr: dependencies.addApprovedLabelToPr,
      handoverStandaloneDirectPrToReview,
      isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
      buildStandaloneDirectPrReviewHandoverOptions,
      log: dependencies.log,
    });
  }

  async function maybeHandleStandaloneDirectPrApproval(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    options: DirectPrApprovalOptionsBase = {}
  ): Promise<ApprovalHandlingResult> {
    return await maybeHandleStandaloneDirectPrApprovalApplication(
      context,
      repoInfo,
      pr,
      options,
      buildStandaloneDirectPrApprovalCallbacks()
    );
  }

  function buildDirectPrLinkedIssueApprovalCallbacks(): DirectPrLinkedIssueApprovalCallbacks<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    IssueType,
    PullRequestType,
    EffectiveConstantsType
  > {
    return {
      resolvePullRequestRequestAuthorId: dependencies.resolvePullRequestRequestAuthorId,
      evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
      ensureAutomatedApprovalReviewForCurrentHead,
      applyApprovedRequestState: dependencies.applyApprovedRequestState,
      resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
      postApprovalRejectedOnce,
      rejectRequestFromApprovalHook: async (
        context: ContextType,
        params: IssueParamsType,
        issue: IssueType,
        decision: ApprovalDecision
      ): Promise<void> =>
        await rejectRequestFromApprovalHook(context, params, issue, decision, {
          closeLinkedPrs: true,
          minimizeTag: 'nsreq:on-approval:issue-rejected',
          listOpenPullRequests: dependencies.listOpenPullRequests,
          parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
        }),
      postApprovalUnknownOnce,
      log: dependencies.log,
    };
  }

  async function maybeHandleDirectPrApprovalForMerge(
    context: ContextType,
    repoInfo: RepoInfoType,
    issueParams: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    pr: PullRequestType
  ): Promise<ApprovalHandlingResult> {
    return await maybeHandleDirectPrApprovalForMergeApplication(
      context,
      repoInfo,
      issueParams,
      issue,
      template,
      parsedFormData,
      pr,
      buildDirectPrLinkedIssueApprovalCallbacks()
    );
  }

  function buildDirectPrApprovalCommentHandlingCallbacks(): DirectPrApprovalCommentHandlingCallbacks<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    PullRequestType,
    IssueType,
    EffectiveConstantsType
  > {
    return composeDirectPrApprovalCommentHandlingCallbacks<
      ContextType,
      RepoInfoType,
      IssueParamsType,
      PullRequestType,
      IssueType,
      EffectiveConstantsType
    >({
      resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
      prAsIssueLike: dependencies.prAsIssueLike,
      ensureReviewLabelsPresentOnIssue: dependencies.ensureReviewLabelsPresentOnIssue,
      resolveDirectPrRequestTypes: dependencies.resolveDirectPrRequestTypes,
      resolveAllowedApproversForRequestTypes: dependencies.resolveAllowedApproversForRequestTypes,
      evaluateDirectPrOnApproval: dependencies.evaluateDirectPrOnApproval,
      postApprovalRejectedOnce,
      isAuthorizedApprover: dependencies.isAuthorizedApprover,
      ensureAutomatedApprovalReviewForCurrentHead,
      isCrossRepositoryPullRequest: dependencies.isCrossRepositoryPullRequest,
      tryMergeApprovedPrOrUpdateBranch: dependencies.tryMergeApprovedPrOrUpdateBranch,
      postOnce: dependencies.postOnce,
      log: dependencies.log,
    });
  }

  async function handleDirectPrApprovalComment(
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    commenter: string
  ): Promise<void> {
    await handleDirectPrApprovalCommentApplication(
      context,
      repoInfo,
      pr,
      commenter,
      buildDirectPrApprovalCommentHandlingCallbacks()
    );
  }

  return {
    buildStandaloneDirectPrReviewHandoverOptions,
    maybeHandleStandaloneDirectPrApproval,
    handleDirectPrApprovalComment,
    maybeHandleDirectPrApprovalForMerge,
    ensureAutomatedApprovalReviewForCurrentHead,
  };
}
