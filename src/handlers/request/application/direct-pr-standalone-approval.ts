import { type ApprovalDecision } from '../domain/approval-decision.js';
import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type PullRequestHeadLike = {
  sha?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  head?: PullRequestHeadLike;
};

type DirectPrApprovalOptionsBase = {
  baseBranch?: string;
};

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

type PullsUpdateContext = {
  octokit: {
    rest: {
      pulls: {
        update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
      };
    };
  };
};

export type StandaloneDirectPrApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ReviewHandoverOptionsType,
> = {
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
  ensureAutomatedApprovalReviewForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: { skipApprovedLabelStateCleanup?: boolean }
  ) => Promise<boolean>;
  postApprovalRejectedOnce: (
    context: ContextType,
    params: IssueParamsBase,
    decision: ApprovalDecision
  ) => Promise<void>;
  hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: DirectPrApprovalOptionsBase
  ) => Promise<boolean>;
  addApprovedLabelToPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    options?: { skipStateCleanup?: boolean }
  ) => Promise<void>;
  handoverStandaloneDirectPrToReview: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options: DirectPrApprovalOptionsBase,
    reviewHandoverOptions: ReviewHandoverOptionsType
  ) => Promise<void>;
  isCrossRepositoryPullRequest: (pr: PullRequestType, repoInfo: RepoInfoType) => boolean;
  buildStandaloneDirectPrReviewHandoverOptions: () => ReviewHandoverOptionsType;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

export async function maybeHandleStandaloneDirectPrApproval<
  ContextType extends PullsUpdateContext,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ReviewHandoverOptionsType,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  options: DirectPrApprovalOptionsBase = {},
  callbacks: StandaloneDirectPrApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, ReviewHandoverOptionsType>
): Promise<ApprovalHandlingResult> {
  const decision = await callbacks.evaluateDirectPrOnApproval(context, repoInfo, pr, undefined, options);

  if (
    decision.status !== 'approved' &&
    decision.status !== 'rejected' &&
    (await callbacks.hasAllowedStandaloneDirectPrApprovalForCurrentHead(context, repoInfo, pr, decision, options))
  ) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        decisionStatus: toStringTrim(decision.status) || 'none',
      },
      'direct-pr:standalone-current-head-approval-present'
    );

    return 'approved';
  }

  if (decision.status === 'approved') {
    const approved = await callbacks.ensureAutomatedApprovalReviewForCurrentHead(context, repoInfo, pr, decision, {
      skipApprovedLabelStateCleanup: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
    });

    if (!approved) return 'continue';

    return 'approved';
  }

  if (decision.status === 'rejected') {
    await callbacks.postApprovalRejectedOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      decision
    );

    try {
      await context.octokit.rest.pulls.update({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        state: 'closed',
      });
    } catch {
      // ignore
    }

    return 'rejected';
  }

  if (decision.status === 'unknown') {
    const hasCurrentHeadManualApproval = await callbacks.hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr(
      context,
      repoInfo,
      pr,
      decision,
      options
    );

    if (hasCurrentHeadManualApproval) {
      await callbacks.addApprovedLabelToPr(context, repoInfo, pr.number, {
        skipStateCleanup: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
      });

      return 'approved';
    }

    await callbacks.handoverStandaloneDirectPrToReview(
      context,
      repoInfo,
      pr,
      decision,
      options,
      callbacks.buildStandaloneDirectPrReviewHandoverOptions()
    );

    return 'continue';
  }

  return 'continue';
}
