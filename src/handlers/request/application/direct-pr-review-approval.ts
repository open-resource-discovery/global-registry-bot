import {
  extractApprovedByLoginFromReviewBody,
  isApprovalReviewForCurrentHead,
  resolveEffectiveReviewApproverLogin,
  reviewTargetsCurrentHead,
} from '../domain/current-head-approval.js';
import {
  getLatestActionableReviewStates,
  isActionableReviewState,
  sortPullRequestReviewsChronologically,
} from '../domain/pull-request-review-state.js';
import type { ApprovalDecision } from '../domain/approval-decision.js';
import { isApprovalDecisionAuthorizedByHookApprovers } from '../domain/approval-policy.js';
import {
  resolveAllowedApproversForRequestTypes,
  type DirectPrApproverResolutionCallbacks,
} from './direct-pr-approver-resolution.js';
import {
  resolvePullRequestRequestAuthorId,
  type PullRequestAuthorResolutionContext,
  type PullRequestAuthorResolutionCallbacks,
} from './pull-request-author-resolution.js';
import { listPullRequestReviews, type PullRequestReviewReadingContext } from './pull-request-review-reading.js';
import {
  resolveDirectPrRequestTypes,
  type DirectPrRequestTypeResolutionCallbacks,
} from './direct-pr-request-type-resolution.js';
import { isAuthorizedApprover } from '../domain/approval-authorization.js';

type RepoInfo = { owner: string; repo: string };

type DirectPrApprovalOptions = {
  baseBranch?: string;
};

type PullRequestLike = {
  number: number;
  head?: { sha?: string | null } | null;
  user?: { login?: string | null } | null;
};

type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  commit_id?: string | null;
  submitted_at?: string | null;
  user?: { login?: string | null } | null;
};

export type DirectPrReviewApprovalCallbacks<ContextType, DecisionType, PullRequestType extends PullRequestLike> = {
  directPrRequestTypeResolutionCallbacks: DirectPrRequestTypeResolutionCallbacks<ContextType, PullRequestType>;
  directPrApproverResolutionCallbacks: DirectPrApproverResolutionCallbacks<ContextType>;
  buildAutoApprovalReviewMarker: (headSha: string) => string;
  toStringTrim: (value: unknown) => string;
  normalizeLogin: (value: unknown) => string;
  uniqLogins: (values: string[]) => string[];
  pullRequestAuthorResolutionCallbacks: PullRequestAuthorResolutionCallbacks;
  resolveHookManualApprovers: (decision: DecisionType) => string[];
  log: (context: ContextType, level: 'info', metadata: Record<string, unknown>, message: string) => void;
};

export async function hasAllowedStandaloneDirectPrApprovalForCurrentHead<
  ContextType extends PullRequestAuthorResolutionContext & PullRequestReviewReadingContext,
  ReviewType extends PullRequestReviewLike,
  DecisionType extends ApprovalDecision,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  decision: DecisionType,
  options: DirectPrApprovalOptions = {},
  callbacks: DirectPrReviewApprovalCallbacks<ContextType, DecisionType, PullRequestType>
): Promise<boolean> {
  const headSha = callbacks.toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  let reviews: ReviewType[] = [];

  try {
    reviews = await listPullRequestReviews(context, repoInfo, pr.number);
  } catch {
    return false;
  }

  const latestStates = getLatestActionableReviewStates(reviews);
  if (new Set(latestStates.values()).has('CHANGES_REQUESTED')) {
    return false;
  }

  if (
    reviews.some(
      (review) =>
        isApprovalReviewForCurrentHead(review, headSha) &&
        callbacks.toStringTrim(review.body).includes(callbacks.buildAutoApprovalReviewMarker(headSha))
    )
  ) {
    return true;
  }

  const requestTypes = await resolveDirectPrRequestTypes(
    context,
    repoInfo,
    pr,
    options,
    callbacks.directPrRequestTypeResolutionCallbacks
  );
  const configuredApprovers = resolveAllowedApproversForRequestTypes(
    context,
    requestTypes,
    callbacks.directPrApproverResolutionCallbacks
  );

  return isApprovalDecisionAuthorizedByHookApprovers(
    decision,
    configuredApprovers,
    reviews
      .filter((review) => isApprovalReviewForCurrentHead(review, headSha))
      .map((review) => callbacks.normalizeLogin(review?.user?.login))
  );
}

export async function hasAllowedCurrentHeadManualApprovalForStandaloneDirectPr<
  ContextType extends PullRequestAuthorResolutionContext & PullRequestReviewReadingContext,
  ReviewType extends PullRequestReviewLike,
  DecisionType extends ApprovalDecision,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  decision: DecisionType,
  options: DirectPrApprovalOptions = {},
  callbacks: DirectPrReviewApprovalCallbacks<ContextType, DecisionType, PullRequestType>
): Promise<boolean> {
  const headSha = callbacks.toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const requestTypes = await resolveDirectPrRequestTypes(
    context,
    repoInfo,
    pr,
    options,
    callbacks.directPrRequestTypeResolutionCallbacks
  );

  const configuredApprovers = resolveAllowedApproversForRequestTypes(
    context,
    requestTypes,
    callbacks.directPrApproverResolutionCallbacks
  );
  const hookManualApprovers = callbacks.resolveHookManualApprovers(decision);
  const allowedApprovers = callbacks.uniqLogins([...(configuredApprovers || []), ...hookManualApprovers]);

  let requesterLogin = callbacks.normalizeLogin(pr.user?.login);

  try {
    requesterLogin =
      (await resolvePullRequestRequestAuthorId(
        context,
        repoInfo,
        pr,
        callbacks.pullRequestAuthorResolutionCallbacks
      )) || requesterLogin;
  } catch {
    // keep PR author fallback
  }

  let reviews: ReviewType[] = [];
  try {
    reviews = await listPullRequestReviews(context, repoInfo, pr.number);
  } catch {
    reviews = [];
  }

  const currentHeadReviews = reviews
    .filter((review) => reviewTargetsCurrentHead(review, headSha))
    .filter((review) => isActionableReviewState(callbacks.toStringTrim(review?.state).toUpperCase()));

  if (!currentHeadReviews.length) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
        requestTypes,
        allowedApprovers,
      },
      'direct-pr:current-head-manual-approval:not-found'
    );

    return false;
  }

  const latestByEffectiveApprover = new Map<string, ReviewType>();

  for (const review of sortPullRequestReviewsChronologically(currentHeadReviews)) {
    const approver = resolveEffectiveReviewApproverLogin(review).toLowerCase();
    if (!approver) continue;

    latestByEffectiveApprover.set(approver, review);
  }

  const latestCurrentHeadReviews = Array.from(latestByEffectiveApprover.values());

  const hasBlockingChangesRequested = latestCurrentHeadReviews.some(
    (review) => callbacks.toStringTrim(review?.state).toUpperCase() === 'CHANGES_REQUESTED'
  );

  if (hasBlockingChangesRequested) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
        requestTypes,
      },
      'direct-pr:current-head-manual-approval:blocking-changes-requested'
    );

    return false;
  }

  const approvingReview = latestCurrentHeadReviews.find((review) => {
    const state = callbacks.toStringTrim(review?.state).toUpperCase();
    if (state !== 'APPROVED') return false;

    const approver = resolveEffectiveReviewApproverLogin(review);
    return isAuthorizedApprover(approver, requesterLogin || pr.user?.login, allowedApprovers);
  });

  if (!approvingReview) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
        requestTypes,
        requesterLogin,
        allowedApprovers,
        currentHeadReviewApprovers: latestCurrentHeadReviews.map((review) => ({
          state: callbacks.toStringTrim(review?.state).toUpperCase(),
          user: callbacks.normalizeLogin(review?.user?.login),
          approvedBy: extractApprovedByLoginFromReviewBody(review?.body),
          commitId: callbacks.toStringTrim(review?.commit_id),
        })),
      },
      'direct-pr:current-head-manual-approval:no-authorized-approval'
    );

    return false;
  }

  const approver = resolveEffectiveReviewApproverLogin(approvingReview);

  callbacks.log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha,
      requestTypes,
      requesterLogin,
      approver,
      allowedApprovers,
    },
    'direct-pr:current-head-manual-approval:accepted'
  );

  return true;
}
