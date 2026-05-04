type UserLike = { login?: string | null };

type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  submitted_at?: string | null;
  user?: UserLike | null;
  commit_id?: string | null;
};

const AUTO_APPROVAL_REVIEW_MARKER_PREFIX = 'nsreq:auto-approval:';

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function normalizeLogin(value: unknown): string {
  return toStringTrim(value).replace(/^@+/, '').trim();
}

function buildAutoApprovalReviewMarker(headSha: string): string {
  return `<!-- ${AUTO_APPROVAL_REVIEW_MARKER_PREFIX}${toStringTrim(headSha)} -->`;
}

export function isApprovalReviewForCurrentHead(review: PullRequestReviewLike, headSha: string): boolean {
  const normalizedHeadSha = toStringTrim(headSha);
  if (!normalizedHeadSha) return false;

  const state = toStringTrim(review?.state).toUpperCase();
  if (state !== 'APPROVED') return false;

  const commitId = toStringTrim(review?.commit_id);
  if (commitId && commitId === normalizedHeadSha) return true;

  const marker = buildAutoApprovalReviewMarker(normalizedHeadSha);
  return Boolean(marker && toStringTrim(review?.body).includes(marker));
}

export function reviewTargetsCurrentHead(review: PullRequestReviewLike, headSha: string): boolean {
  const normalizedHeadSha = toStringTrim(headSha);
  if (!normalizedHeadSha) return false;

  const reviewCommitId = toStringTrim(review?.commit_id);
  if (reviewCommitId && reviewCommitId === normalizedHeadSha) return true;

  const body = toStringTrim(review?.body);
  if (!body) return false;

  return body.includes(buildAutoApprovalReviewMarker(normalizedHeadSha));
}

export function extractApprovedByLoginFromReviewBody(body: unknown): string {
  const raw = toStringTrim(body);
  if (!raw) return '';

  const match = /\bApproved by\s+@?([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)\b/i.exec(raw);
  return normalizeLogin(match?.[1]);
}

export function resolveEffectiveReviewApproverLogin(review: PullRequestReviewLike): string {
  const approvedByFromBody = extractApprovedByLoginFromReviewBody(review?.body);
  if (approvedByFromBody) return approvedByFromBody;

  return normalizeLogin(review?.user?.login);
}
