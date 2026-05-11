import { type ApprovalDecision } from './approval-decision.js';
import { buildAutoApprovalReviewMarker } from './auto-approval-review-marker.js';
import { getVisibleApprovalText } from './approval-policy.js';
import {
  buildDetectedIssuesBody,
  normalizeMachineReadableIssues,
  type MachineReadableIssue,
} from './machine-readable.js';

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function toSectionTitle(field: string): string {
  const raw = toStringTrim(field);
  if (!raw) return 'Details';

  const lowerCase = raw.toLowerCase();
  if (lowerCase === 'contact' || lowerCase === 'contacts') return 'Contacts';

  const spaced = raw
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim();

  if (!spaced) return 'Details';
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function buildApprovalDecisionJson(decision: ApprovalDecision): string {
  const payload: Record<string, unknown> = {};
  if (decision.status) payload.status = decision.status;
  if (decision.path) payload.path = decision.path;
  if (decision.reason) payload.reason = decision.reason;
  if (decision.comment) payload.comment = decision.comment;
  if (decision.message) payload.message = decision.message;
  if (Array.isArray(decision.approvers) && decision.approvers.length) payload.approvers = decision.approvers;
  if (Array.isArray(decision.errors) && decision.errors.length) payload.errors = decision.errors;
  return JSON.stringify(payload, null, 2);
}

export function normalizeApprovalHookErrorsForComment(decision: ApprovalDecision): MachineReadableIssue[] {
  const raw = Array.isArray(decision.errors) ? decision.errors : [];
  const mapped = raw.map((entry) => ({
    field: toStringTrim(entry?.field) || 'details',
    message: toStringTrim(entry?.message),
  }));

  const normalized = normalizeMachineReadableIssues(mapped);
  if (normalized.length) return normalized;

  const fallbackMessage =
    toStringTrim(decision.message) || toStringTrim(decision.reason) || toStringTrim(decision.comment);
  const fallbackField = toStringTrim(decision.path) || 'details';

  return fallbackMessage ? [{ field: fallbackField, message: fallbackMessage }] : [];
}

export function buildApprovalHookIssueList(issues: MachineReadableIssue[]): string {
  const normalized = normalizeMachineReadableIssues(issues);
  if (!normalized.length) return '';

  const grouped = new Map<string, string[]>();

  for (const issue of normalized) {
    const key = toStringTrim(issue.field) || 'details';
    const arr = grouped.get(key) ?? [];
    if (!arr.includes(issue.message)) arr.push(issue.message);
    grouped.set(key, arr);
  }

  const keys = Array.from(grouped.keys()).sort((a, b) => {
    if (a === 'details') return 1;
    if (b === 'details') return -1;
    return a.localeCompare(b);
  });

  const lines: string[] = [];
  for (const key of keys) {
    lines.push(`### ${toSectionTitle(key)}`);
    for (const msg of grouped.get(key) ?? []) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }

  return lines.join('\n').trim();
}

export function buildAutoApprovalReviewBody(decision: ApprovalDecision, headSha: string): string {
  const visible = getVisibleApprovalText(decision);
  const marker = buildAutoApprovalReviewMarker(headSha);

  return visible ? `${visible}\n\n${marker}` : marker;
}

export function buildApprovalUnknownBody(decision: ApprovalDecision): string {
  const lead = toStringTrim(decision.message) || toStringTrim(decision.comment) || toStringTrim(decision.reason);
  const leadBlock = lead ? `${lead}\n\n` : '';

  return `${leadBlock}<details>
<summary>Decision details</summary>

\`\`\`json
${buildApprovalDecisionJson({ status: 'unknown', ...decision })}
\`\`\`
</details>

Continuing with the standard review flow.`;
}

export function buildApprovalRejectedBody(decision: ApprovalDecision): string {
  const issues = normalizeApprovalHookErrorsForComment(decision);
  const groupedIssues = buildApprovalHookIssueList(issues);
  const detectedIssuesBlock = groupedIssues ? buildDetectedIssuesBody(groupedIssues, issues) : '';

  const lead = toStringTrim(decision.message) || toStringTrim(decision.comment) || toStringTrim(decision.reason);
  const leadBlock = lead && !detectedIssuesBlock ? `${lead}\n\n` : '';
  const issuesBlock = detectedIssuesBlock ? `${detectedIssuesBlock}\n\n` : '';

  return `## onApproval rejected this request

${leadBlock}${issuesBlock}Closing this request automatically.`;
}
