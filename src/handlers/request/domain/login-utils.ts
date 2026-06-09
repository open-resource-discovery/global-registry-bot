export function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function normalizeLogin(value: unknown): string {
  return toStringTrim(value).replace(/^@+/, '').trim();
}

export function uniqLogins(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const v of values || []) {
    const s = normalizeLogin(v);
    if (!s) continue;
    const k = s.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out;
}
