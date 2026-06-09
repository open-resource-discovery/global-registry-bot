type RepoInfo = { owner: string; repo: string };

type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  commit_id?: string | null;
  submitted_at?: string | null;
  user?: { login?: string | null } | null;
};

export type PullRequestReviewReadingContext = {
  octokit: {
    pulls: {
      listReviews: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: PullRequestReviewLike[] }>;
    };
  };
};

export async function listPullRequestReviews<
  ContextType extends PullRequestReviewReadingContext,
  ReviewType extends PullRequestReviewLike,
>(context: ContextType, repoInfo: RepoInfo, prNumber: number): Promise<ReviewType[]> {
  const out: ReviewType[] = [];
  let page = 1;

  while (true) {
    const res = await context.octokit.pulls.listReviews({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: prNumber,
      per_page: 100,
      page,
    });

    const reviews = Array.isArray(res?.data) ? (res.data as ReviewType[]) : [];
    if (!reviews.length) break;

    out.push(...reviews);

    if (reviews.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return out;
}
