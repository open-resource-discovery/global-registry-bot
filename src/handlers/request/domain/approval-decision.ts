export type ApprovalDecision = {
  status?: 'approved' | 'rejected' | 'unknown';
  path?: string;
  reason?: string;
  comment?: string;
  message?: string;
  approvers?: string[];
  errors?: {
    field?: string;
    message?: string;
  }[];
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

function getVisibleApprovalText(decision: ApprovalDecision): string {
  const comment = toStringTrim(decision.comment);
  if (comment && !isManualApprovalRequiredText(comment)) return comment;

  const message = toStringTrim(decision.message);
  if (message && !isManualApprovalRequiredText(message)) return message;

  const reason = toStringTrim(decision.reason);
  if (reason && !isManualApprovalRequiredText(reason)) return reason;

  return '';
}

function isApprovalDecisionAuthorizedByHookApprovers(
  decision: ApprovalDecision,
  requesterId: string | undefined | null
): boolean {
  const requester = normalizeLogin(requesterId).toLowerCase();
  if (!requester) return false;

  const approvers = uniqLogins((decision.approvers || []).map((value) => toStringTrim(value)).filter(Boolean));
  return approvers.some((approver) => normalizeLogin(approver).toLowerCase() === requester);
}

export function normalizeApprovalDecision(decision: ApprovalDecision | boolean): ApprovalDecision {
  if (decision === true) return { status: 'approved' };
  if (decision === false) return {};
  if (!decision) return {};

  const normalized = decision || {};
  const approvers = uniqLogins(
    Array.isArray(normalized.approvers) ? normalized.approvers.map((x) => toStringTrim(x)).filter(Boolean) : []
  );
  const { approvers: _approvers, ...normalizedWithoutApprovers } = normalized;

  return {
    ...normalizedWithoutApprovers,
    ...(approvers.length ? { approvers } : {}),
  };
}

export function promoteUnknownApprovalDecisionForDirectPrRequester(
  decision: ApprovalDecision,
  requesterId: string | undefined | null
): ApprovalDecision {
  const normalized = normalizeApprovalDecision(decision);

  if (normalized.status !== 'unknown') return normalized;
  if (!isApprovalDecisionAuthorizedByHookApprovers(normalized, requesterId)) return normalized;

  return {
    ...normalized,
    status: 'approved',
    comment: getVisibleApprovalText(normalized),
    message: '',
    reason: '',
  };
}
