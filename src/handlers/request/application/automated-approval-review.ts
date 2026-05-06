import { postOnce } from '../comments.js';
import { buildAutoApprovalReviewBody } from '../domain/approval-comment-rendering.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';

type RepoInfo = { owner: string; repo: string };
type PullRequestLike = {
  number: number;
  head?: {
    sha?: string | null;
  } | null;
};

type AutomatedApprovalReviewContext = Parameters<typeof postOnce>[0] & {
  octokit: {
    pulls: {
      createReview: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        event: 'APPROVE';
        body: string;
      }) => Promise<unknown>;
    };
  };
};

export type AutomatedApprovalReviewOptions = {
  skipApprovedLabelStateCleanup?: boolean;
};

export type AutomatedApprovalReviewCallbacks<ContextType, RepoInfoType extends RepoInfo> = {
  toStringTrim: (value: unknown) => string;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  getVisibleApprovalText: (decision: ApprovalDecision) => string;
  hasAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => boolean;
  hasAutoApprovalReviewForHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    headSha: string
  ) => Promise<boolean>;
  markAutoApprovedPrHead: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => void;
  addApprovedLabelToPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    options: { skipStateCleanup?: boolean }
  ) => Promise<void>;
  autoApprovedPrHeadKey: (repoInfo: RepoInfoType, prNumber: number, headSha: string) => string;
  logCreated: (context: ContextType, prNumber: number, headSha: string) => void;
  logCreateFailed: (
    context: ContextType,
    prNumber: number,
    status: number | undefined,
    message: string,
    responseData: unknown
  ) => void;
  logDedupedInFlight: (context: ContextType, prNumber: number, headSha: string) => void;
};

const AUTO_APPROVAL_REVIEW_INFLIGHT = new Map<string, Promise<boolean>>();

async function createAutomatedApprovalReview<
  ContextType extends AutomatedApprovalReviewContext,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  decision: ApprovalDecision,
  callbacks: AutomatedApprovalReviewCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const headSha = callbacks.toStringTrim(pr.head?.sha);
  const reviewBody = buildAutoApprovalReviewBody(decision, headSha);
  const failureText = callbacks.getVisibleApprovalText(decision) || 'The onApproval hook matched this PR.';

  try {
    await context.octokit.pulls.createReview({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: pr.number,
      event: 'APPROVE',
      body: reviewBody,
    });

    callbacks.logCreated(context, pr.number, headSha);
    return true;
  } catch (e: unknown) {
    const errObj = callbacks.isPlainObject(e) ? e : {};
    const status = typeof errObj['status'] === 'number' ? errObj['status'] : undefined;
    const response = callbacks.isPlainObject(errObj['response']) ? errObj['response'] : {};
    const responseData = response['data'];
    const message = e instanceof Error ? e.message : String(e);

    callbacks.logCreateFailed(context, pr.number, status, message, responseData);

    await postOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      `## onApproval matched, but automatic PR approval failed

${failureText}

Approval API error: ${message}${status ? ` (HTTP ${status})` : ''}

The PR could not be approved automatically, so merge remains blocked until a review is added manually.`,
      { minimizeTag: 'nsreq:on-approval:approve-failed' }
    );

    return false;
  }
}

async function runEnsureAutomatedApprovalReviewForCurrentHead<
  ContextType extends AutomatedApprovalReviewContext,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  decision: ApprovalDecision,
  headSha: string,
  options: AutomatedApprovalReviewOptions,
  callbacks: AutomatedApprovalReviewCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  if (callbacks.hasAutoApprovedPrHead(repoInfo, pr.number, headSha)) {
    return true;
  }

  if (await callbacks.hasAutoApprovalReviewForHead(context, repoInfo, pr.number, headSha)) {
    callbacks.markAutoApprovedPrHead(repoInfo, pr.number, headSha);
    return true;
  }

  const approved = await createAutomatedApprovalReview(context, repoInfo, pr, decision, callbacks);
  if (!approved) return false;

  callbacks.markAutoApprovedPrHead(repoInfo, pr.number, headSha);

  await callbacks.addApprovedLabelToPr(context, repoInfo, pr.number, {
    skipStateCleanup: options.skipApprovedLabelStateCleanup === true,
  });

  return true;
}

export async function ensureAutomatedApprovalReviewForCurrentHead<
  ContextType extends AutomatedApprovalReviewContext,
  RepoInfoType extends RepoInfo,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  decision: ApprovalDecision,
  options: AutomatedApprovalReviewOptions = {},
  callbacks: AutomatedApprovalReviewCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<boolean> {
  const headSha = callbacks.toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = callbacks.autoApprovedPrHeadKey(repoInfo, pr.number, headSha);

  const existing = AUTO_APPROVAL_REVIEW_INFLIGHT.get(key);
  if (existing) {
    callbacks.logDedupedInFlight(context, pr.number, headSha);
    return await existing;
  }

  const pending = runEnsureAutomatedApprovalReviewForCurrentHead(
    context,
    repoInfo,
    pr,
    decision,
    headSha,
    options,
    callbacks
  ).finally(() => {
    AUTO_APPROVAL_REVIEW_INFLIGHT.delete(key);
  });

  AUTO_APPROVAL_REVIEW_INFLIGHT.set(key, pending);
  return await pending;
}
