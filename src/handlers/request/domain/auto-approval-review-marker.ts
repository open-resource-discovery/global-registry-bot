import { toStringTrim } from './login-utils.js';

const AUTO_APPROVAL_REVIEW_MARKER_PREFIX = 'nsreq:auto-approval:';

export function buildAutoApprovalReviewMarker(headSha: string): string {
  return `<!-- ${AUTO_APPROVAL_REVIEW_MARKER_PREFIX}${toStringTrim(headSha)} -->`;
}
