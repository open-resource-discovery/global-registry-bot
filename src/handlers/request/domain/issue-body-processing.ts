import { stripRoutingLockFromBody } from './routing-lock-marker.js';
import { stripContactApprovalFromBody, stripParentApprovalFromBody } from './approval-markers.js';

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function readIssueBodyForProcessing(issueBody: unknown): string {
  return toStringTrim(stripContactApprovalFromBody(stripParentApprovalFromBody(stripRoutingLockFromBody(issueBody))));
}

/**
 * Reproduces the historical normalization used by create.ts before the snapshot-hash
 * stability fix: strips routing-lock and parent-approval but NOT contact-approval.
 * Used only to generate a backwards-compatibility hash for PRs created by the old code.
 */
export function readIssueBodyForSnapshotLegacy(issueBody: unknown): string {
  return toStringTrim(stripParentApprovalFromBody(stripRoutingLockFromBody(issueBody)));
}
