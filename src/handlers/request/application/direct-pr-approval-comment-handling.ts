import { type ApprovalDecision } from '../domain/approval-decision.js';
import { toStringTrim, uniqLogins } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type UserLikeBase = { login?: string | null };

type PullRequestBaseLike = {
  ref?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  user?: UserLikeBase | null;
  base?: PullRequestBaseLike;
};

type IssueLikeBase = {
  number: number;
  user?: UserLikeBase | null;
};

type EffectiveConstantsBase = {
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type DirectPrApprovalOptionsBase = {
  baseBranch?: string;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export type DirectPrApprovalCommentHandlingCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  PullRequestType extends PullRequestLikeBase,
  IssueType extends IssueLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  buildIssueParams: (repoInfo: RepoInfoType, pr: PullRequestType) => ParamsType;
  prAsIssueLike: (pr: PullRequestType) => IssueType;
  ensureReviewLabelsPresentOnIssue: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    effectiveConstants: EffectiveConstantsType
  ) => Promise<boolean>;
  resolveDirectPrRequestTypes: (
    context: ContextType,
    repoInfo: RepoInfoType,
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
  postApprovalRejectedOnce: (context: ContextType, params: ParamsType, decision: ApprovalDecision) => Promise<void>;
  isAuthorizedApprover: (
    commenter: string,
    issueAuthor: string | undefined | null,
    allowedApprovers: string[]
  ) => boolean;
  ensureAutomatedApprovalReviewForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: { skipApprovedLabelStateCleanup?: boolean }
  ) => Promise<boolean>;
  isCrossRepositoryPullRequest: (pr: PullRequestType, repoInfo: RepoInfoType) => boolean;
  tryMergeApprovedPrOrUpdateBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

export async function handleDirectPrApprovalComment<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  PullRequestType extends PullRequestLikeBase,
  IssueType extends IssueLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  commenter: string,
  callbacks: DirectPrApprovalCommentHandlingCallbacks<
    ContextType,
    RepoInfoType,
    ParamsType,
    PullRequestType,
    IssueType,
    EffectiveConstantsType
  >
): Promise<void> {
  const eff = callbacks.resolveEffectiveConstants(context);
  const params = callbacks.buildIssueParams(repoInfo, pr);
  const prIssue = callbacks.prAsIssueLike(pr);

  const reviewOk = await callbacks.ensureReviewLabelsPresentOnIssue(context, params, prIssue, eff);
  if (!reviewOk) {
    await callbacks.postOnce(
      context,
      params,
      'Approval ignored: direct PR is not in review state. Please wait until validation has routed it to review.',
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  const requestTypes = await callbacks.resolveDirectPrRequestTypes(context, repoInfo, pr, {
    baseBranch: toStringTrim(pr.base?.ref),
  });

  const configuredApprovers = callbacks.resolveAllowedApproversForRequestTypes(context, requestTypes);

  const approvalDecision = await callbacks.evaluateDirectPrOnApproval(context, repoInfo, pr, undefined, {
    baseBranch: toStringTrim(pr.base?.ref),
  });

  if (approvalDecision.status === 'rejected') {
    await callbacks.postApprovalRejectedOnce(context, params, approvalDecision);
    return;
  }

  const allowedApprovers = uniqLogins([...(configuredApprovers || []), ...(approvalDecision.approvers || [])]);

  const okApprover = callbacks.isAuthorizedApprover(commenter, pr.user?.login, allowedApprovers);

  if (!okApprover) {
    await callbacks.postOnce(
      context,
      params,
      `Approval ignored: commenter ${commenter} is not an allowed approver for this direct PR.`,
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  const approved = await callbacks.ensureAutomatedApprovalReviewForCurrentHead(
    context,
    repoInfo,
    pr,
    {
      status: 'approved',
      comment: `Approved by @${commenter}`,
    },
    {
      skipApprovedLabelStateCleanup: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
    }
  );

  if (!approved) return;

  await callbacks.tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'direct-pr-manual-approval');
}
