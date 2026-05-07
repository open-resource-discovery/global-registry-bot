type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  submitted_at?: string | null;
  user?: { login?: string | null } | null;
};

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function normalizeLogin(value: unknown): string {
  return toStringTrim(value).replace(/^@+/, '').trim();
}

export function normalizeReviewState(value: unknown): string {
  return toStringTrim(value).toUpperCase();
}

export function isActionableReviewState(state: string): boolean {
  return new Set<string>(['APPROVED', 'CHANGES_REQUESTED', 'DISMISSED']).has(state);
}

export function sortPullRequestReviewsChronologically(reviews: PullRequestReviewLike[]): PullRequestReviewLike[] {
  return reviews.slice().sort((a, b) => {
    const at = Date.parse(toStringTrim(a.submitted_at));
    const bt = Date.parse(toStringTrim(b.submitted_at));

    if (Number.isFinite(at) && Number.isFinite(bt) && at !== bt) return at - bt;

    const aid = typeof a.id === 'number' ? a.id : 0;
    const bid = typeof b.id === 'number' ? b.id : 0;
    return aid - bid;
  });
}

export function getLatestActionableReviewStates(reviews: PullRequestReviewLike[]): Map<string, string> {
  const latestByReviewer = new Map<string, string>();

  for (const review of sortPullRequestReviewsChronologically(reviews)) {
    const reviewer = normalizeLogin(review?.user?.login).toLowerCase();
    const state = normalizeReviewState(review?.state);

    if (!reviewer || !isActionableReviewState(state)) continue;

    latestByReviewer.set(reviewer, state);
  }

  return latestByReviewer;
}
