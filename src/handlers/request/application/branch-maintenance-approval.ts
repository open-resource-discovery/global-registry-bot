import { hasAutoApprovedPrHead } from './auto-approved-head-tracking.js';
import { listPullRequestReviews, type PullRequestReviewReadingContext } from './pull-request-review-reading.js';
import { buildAutoApprovalReviewMarker } from '../domain/auto-approval-review-marker.js';
import { toStringTrim } from '../domain/login-utils.js';
import { getLatestActionableReviewStates } from '../domain/pull-request-review-state.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  head?: { sha?: string | null } | null;
  body?: string | null;
};

type PullRequestReviewLike = {
  state?: string | null;
  body?: string | null;
};

export type BranchMaintenanceApprovalCallbacks<ContextType, PullRequestType> = {
  hasApprovedLabelOnPr: (context: ContextType, repoInfo: RepoInfo, prNumber: number) => Promise<boolean>;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
};

export async function isPullRequestApprovedForBranchMaintenance<
  ContextType extends PullRequestReviewReadingContext,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  options: { allowLabelFallback?: boolean } = {},
  callbacks: BranchMaintenanceApprovalCallbacks<ContextType, PullRequestType>
): Promise<boolean> {
  let reviews: PullRequestReviewLike[];
  try {
    reviews = await listPullRequestReviews(context, repoInfo, pr.number);
  } catch {
    reviews = [];
  }

  const latestStates = getLatestActionableReviewStates(reviews);
  const latestStateValues = new Set(latestStates.values());

  if (latestStateValues.has('CHANGES_REQUESTED')) {
    return false;
  }

  if (callbacks.isSnapshotManagedRequestPr(pr)) return true;

  const headSha = toStringTrim(pr.head?.sha);
  const marker = headSha ? buildAutoApprovalReviewMarker(headSha) : null;

  if (
    marker &&
    reviews.some(
      (review) =>
        toStringTrim(review?.state).toUpperCase() === 'APPROVED' && toStringTrim(review?.body).includes(marker)
    )
  ) {
    return true;
  }

  if (latestStateValues.has('APPROVED')) {
    return true;
  }

  if (headSha && hasAutoApprovedPrHead(repoInfo, pr.number, headSha)) {
    return true;
  }

  if (options.allowLabelFallback !== false && (await callbacks.hasApprovedLabelOnPr(context, repoInfo, pr.number))) {
    return true;
  }

  return false;
}
