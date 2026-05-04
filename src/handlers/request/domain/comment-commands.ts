function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function stripQuoteAndCode(text: unknown): string {
  return toStringTrim(text)
    .replaceAll(/```[\s\S]*?```/g, '')
    .replaceAll(/^>.*$/gm, '')
    .trim();
}

function normalizeApprovalCommandToken(value: unknown): string {
  let text = toStringTrim(value).replace(/^\/+/, '').trim().toLowerCase();

  const leadingTrimChars = new Set(['"', "'", '`', '(', '[', '{', '<']);
  const trailingTrimChars = new Set(['"', "'", '`', ')', ']', '}', '>', '.', ',', '!', '?', ';', ':']);

  while (text && leadingTrimChars.has(text[0])) text = text.slice(1).trim();
  while (text && trailingTrimChars.has(text.at(-1) || '')) text = text.slice(0, -1).trim();

  return text;
}

export function isApprovalComment(text: unknown, configuredKeyword?: string): boolean {
  const lines = toStringTrim(text)
    .split(/\r?\n/)
    .map((line) => toStringTrim(line))
    .filter(Boolean);

  if (!lines.length) return false;

  const allowed = new Set<string>(['approved', 'approve', 'lgtm']);
  const configured = normalizeApprovalCommandToken(configuredKeyword);
  if (configured) allowed.add(configured);

  return lines.some((line) => {
    const normalized = normalizeApprovalCommandToken(line);
    return Boolean(normalized) && allowed.has(normalized);
  });
}

export function isAuthorUpdateComment(text: unknown): boolean {
  return /\b(updated|update|fixed|fix(ed)?|addressed|done)\b/i.test(toStringTrim(text));
}
