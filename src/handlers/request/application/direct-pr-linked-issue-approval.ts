import { normalizeApprovalDecision, type ApprovalDecision } from '../domain/approval-decision.js';
import { normalizeLogin } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  user?: UserLikeBase | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type PullRequestLikeBase = {
  number: number;
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

export type DirectPrLinkedIssueApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType,
> = {
  resolvePullRequestRequestAuthorId: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<string>;
  evaluateDirectPrOnApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    requestAuthorIdOverride?: string
  ) => Promise<ApprovalDecision>;
  ensureAutomatedApprovalReviewForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options?: Record<string, unknown>
  ) => Promise<boolean>;
  applyApprovedRequestState: (
    context: ContextType,
    params: ParamsType,
    effectiveConstants: EffectiveConstantsType
  ) => Promise<void>;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  postApprovalRejectedOnce: (
    context: ContextType,
    params: IssueParamsBase,
    decision: ApprovalDecision
  ) => Promise<void>;
  rejectRequestFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    decision: ApprovalDecision
  ) => Promise<void>;
  postApprovalUnknownOnce: (context: ContextType, params: IssueParamsBase, decision: ApprovalDecision) => Promise<void>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

export async function maybeHandleDirectPrApprovalForMerge<
  ContextType extends PullsUpdateContext,
  RepoInfoType extends RepoInfoBase,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  issueParams: ParamsType,
  issue: IssueType,
  _template: TemplateType,
  _parsedFormData: FormDataType,
  pr: PullRequestType,
  callbacks: DirectPrLinkedIssueApprovalCallbacks<
    ContextType,
    RepoInfoType,
    ParamsType,
    IssueType,
    PullRequestType,
    EffectiveConstantsType
  >
): Promise<ApprovalHandlingResult> {
  const issueAuthorId = normalizeLogin(issue.user?.login);
  const prRequesterId = await callbacks.resolvePullRequestRequestAuthorId(context, repoInfo, pr);
  const requestAuthorId = issueAuthorId || prRequesterId;

  callbacks.log(
    context,
    'info',
    {
      prNumber: pr.number,
      linkedIssueNumber: issue.number,
      issueAuthorId,
      prRequesterId,
      requestAuthorId,
    },
    'direct-pr:linked-issue-requester-resolved'
  );

  const decision = normalizeApprovalDecision(
    await callbacks.evaluateDirectPrOnApproval(context, repoInfo, pr, requestAuthorId || undefined)
  );

  if (decision.status === 'approved') {
    const approved = await callbacks.ensureAutomatedApprovalReviewForCurrentHead(context, repoInfo, pr, decision);
    if (!approved) return 'continue';

    await callbacks.applyApprovedRequestState(context, issueParams, callbacks.resolveEffectiveConstants(context));
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

    await callbacks.rejectRequestFromApprovalHook(context, issueParams, issue, decision);

    return 'rejected';
  }

  if (decision.status === 'unknown') {
    await callbacks.postApprovalUnknownOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      decision
    );
  }

  return 'continue';
}
