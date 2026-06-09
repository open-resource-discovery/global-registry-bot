type RepoInfo = { owner: string; repo: string };

type PullRequestReviewLike = {
  state?: string | null;
  body?: string | null;
};

export type AutoApprovalReviewDetectionCallbacks<ContextType> = {
  buildAutoApprovalReviewMarker: (headSha: string) => string;
  listPullRequestReviews: (
    context: ContextType,
    repoInfo: RepoInfo,
    prNumber: number
  ) => Promise<PullRequestReviewLike[]>;
  toStringTrim: (value: unknown) => string;
};

export async function hasAutoApprovalReviewForHead<ContextType>(
  context: ContextType,
  repoInfo: RepoInfo,
  prNumber: number,
  headSha: string,
  callbacks: AutoApprovalReviewDetectionCallbacks<ContextType>
): Promise<boolean> {
  const marker = callbacks.buildAutoApprovalReviewMarker(headSha);

  try {
    const reviews = await callbacks.listPullRequestReviews(context, repoInfo, prNumber);

    return reviews.some(
      (review) =>
        callbacks.toStringTrim(review?.state).toUpperCase() === 'APPROVED' &&
        callbacks.toStringTrim(review?.body).includes(marker)
    );
  } catch {
    return false;
  }
}
