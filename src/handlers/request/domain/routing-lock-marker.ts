const ROUTING_LOCK_READ_RE = /<!--\s*nsreq:routing-lock\s*=\s*({[\s\S]*?})\s*-->/i;
const ROUTING_LOCK_STRIP_RE = /<!--\s*nsreq:routing-lock\s*=\s*{[\s\S]*?}\s*-->\s*/gi;

export type RoutingLockMeta = { v: 1; expected: string };

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function readRoutingLockExpected(issueBody: unknown): string {
  const body = String(issueBody || '');
  const match = body.match(ROUTING_LOCK_READ_RE);
  if (!match) return '';
  try {
    const meta = JSON.parse(String(match[1] || ''));
    return toStringTrim((meta as Record<string, unknown>)?.['expected']);
  } catch {
    return '';
  }
}

export function stripRoutingLockFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(ROUTING_LOCK_STRIP_RE, '').trimEnd();
}

export function buildRoutingLockBody(issueBody: unknown, expectedLabel: string): string {
  const expected = toStringTrim(expectedLabel);
  const cleaned = stripRoutingLockFromBody(issueBody);
  const meta: RoutingLockMeta = { v: 1, expected };
  const metaStr = JSON.stringify(meta);
  return `${cleaned}\n\n<!-- nsreq:routing-lock = ${metaStr} -->\n`;
}
