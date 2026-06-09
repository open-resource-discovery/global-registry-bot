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
