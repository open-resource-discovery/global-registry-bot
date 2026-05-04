const PARENT_APPROVAL_READ_RE = /<!--\s*nsreq:parent-approval\s*=\s*({[\s\S]*?})\s*-->/i;
const PARENT_APPROVAL_STRIP_RE = /<!--\s*nsreq:parent-approval\s*=\s*{[\s\S]*?}\s*-->\s*/gi;
const CONTACT_APPROVAL_READ_RE = /<!--\s*nsreq:contact-approval\s*=\s*({[\s\S]*?})\s*-->/i;
const CONTACT_APPROVAL_STRIP_RE = /<!--\s*nsreq:contact-approval\s*=\s*{[\s\S]*?}\s*-->\s*/gi;

export type ParentApprovalMeta = {
  v: 1;
  parent: string;
  target: string;
  owners: string[];
  approvedBy?: string;
  approvedAt?: string;
};

export type ContactApprovalMeta = {
  v: 1;
  target: string;
  owners: string[];
  approvedBy?: string;
  approvedAt?: string;
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

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function stripContactApprovalFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(CONTACT_APPROVAL_STRIP_RE, '').trimEnd();
}

export function readContactApprovalMeta(issueBody: unknown): ContactApprovalMeta | null {
  const body = String(issueBody || '');
  const match = body.match(CONTACT_APPROVAL_READ_RE);
  if (!match) return null;

  try {
    const raw = JSON.parse(String(match[1] || ''));
    if (!isPlainObject(raw)) return null;
    if (raw['v'] !== 1) return null;

    const target = toStringTrim(raw['target']);
    const ownersRaw = raw['owners'];
    const owners = Array.isArray(ownersRaw) ? uniqLogins(ownersRaw.map(toStringTrim).filter(Boolean)) : [];
    const approvedBy = normalizeLogin(raw['approvedBy']);
    const approvedAt = toStringTrim(raw['approvedAt']);

    if (!target || !owners.length) return null;

    const out: ContactApprovalMeta = { v: 1, target, owners };
    if (approvedBy) out.approvedBy = approvedBy;
    if (approvedAt) out.approvedAt = approvedAt;
    return out;
  } catch {
    return null;
  }
}

export function buildContactApprovalBody(issueBody: unknown, meta: ContactApprovalMeta | null): string {
  const cleaned = stripContactApprovalFromBody(issueBody);
  if (!meta) return `${cleaned}\n`;

  const next: ContactApprovalMeta = {
    v: 1,
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const approvedBy = normalizeLogin(meta.approvedBy);
  const approvedAt = toStringTrim(meta.approvedAt);

  if (approvedBy) next.approvedBy = approvedBy;
  if (approvedAt) next.approvedAt = approvedAt;

  const metaStr = JSON.stringify(next);
  return `${cleaned}\n\n<!-- nsreq:contact-approval = ${metaStr} -->\n`;
}

export function stripParentApprovalFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(PARENT_APPROVAL_STRIP_RE, '').trimEnd();
}

export function readParentApprovalMeta(issueBody: unknown): ParentApprovalMeta | null {
  const body = String(issueBody || '');
  const match = body.match(PARENT_APPROVAL_READ_RE);
  if (!match) return null;

  try {
    const raw = JSON.parse(String(match[1] || ''));
    if (!isPlainObject(raw)) return null;
    if (raw['v'] !== 1) return null;

    const parent = toStringTrim(raw['parent']);
    const target = toStringTrim(raw['target']);
    const ownersRaw = raw['owners'];
    const owners = Array.isArray(ownersRaw) ? uniqLogins(ownersRaw.map(toStringTrim).filter(Boolean)) : [];
    const approvedBy = normalizeLogin(raw['approvedBy']);
    const approvedAt = toStringTrim(raw['approvedAt']);

    if (!parent || !target) return null;

    const out: ParentApprovalMeta = { v: 1, parent, target, owners };
    if (approvedBy) out.approvedBy = approvedBy;
    if (approvedAt) out.approvedAt = approvedAt;
    return out;
  } catch {
    return null;
  }
}

export function buildParentApprovalBody(issueBody: unknown, meta: ParentApprovalMeta | null): string {
  const cleaned = stripParentApprovalFromBody(issueBody);
  if (!meta) return `${cleaned}\n`;

  const next: ParentApprovalMeta = {
    v: 1,
    parent: toStringTrim(meta.parent),
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const approvedBy = normalizeLogin(meta.approvedBy);
  const approvedAt = toStringTrim(meta.approvedAt);
  if (approvedBy) next.approvedBy = approvedBy;
  if (approvedAt) next.approvedAt = approvedAt;

  const metaStr = JSON.stringify(next);
  return `${cleaned}\n\n<!-- nsreq:parent-approval = ${metaStr} -->\n`;
}
