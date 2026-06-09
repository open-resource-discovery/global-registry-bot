import { normalizeApprovalDecision, type ApprovalDecision } from './approval-decision.js';

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function normalizeLogin(value: unknown): string {
  return toStringTrim(value).replace(/^@+/, '').trim();
}

function uniqLogins(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  for (const value of values) {
    const login = normalizeLogin(value);
    const key = login.toLowerCase();
    if (!login || seen.has(key)) continue;
    seen.add(key);
    out.push(login);
  }

  return out;
}

function isManualApprovalRequiredText(value: unknown): boolean {
  return /\bmanual approval required\b/i.test(toStringTrim(value));
}

export function getUnknownManualApprovers(decision: ApprovalDecision): string[] {
  const normalized = normalizeApprovalDecision(decision);

  if (normalized.status !== 'unknown') return [];

  return uniqLogins((normalized.approvers || []).map((value) => toStringTrim(value)).filter(Boolean));
}

export function getVisibleApprovalText(decision: ApprovalDecision): string {
  const comment = toStringTrim(decision.comment);
  if (comment && !isManualApprovalRequiredText(comment)) return comment;

  const message = toStringTrim(decision.message);
  if (message && !isManualApprovalRequiredText(message)) return message;

  const reason = toStringTrim(decision.reason);
  if (reason && !isManualApprovalRequiredText(reason)) return reason;

  return '';
}

export function isApprovalDecisionAuthorizedByHookApprovers(
  decision: ApprovalDecision,
  configuredApprovers: string[],
  reviewerLogins: string[]
): boolean {
  const normalized = normalizeApprovalDecision(decision);
  const allowedApprovers = new Set(
    uniqLogins([...(configuredApprovers || []), ...(normalized.approvers || [])]).map((login) =>
      normalizeLogin(login).toLowerCase()
    )
  );

  if (!allowedApprovers.size) return false;

  return uniqLogins((reviewerLogins || []).map((login) => toStringTrim(login)).filter(Boolean)).some((reviewer) =>
    allowedApprovers.has(normalizeLogin(reviewer).toLowerCase())
  );
}
