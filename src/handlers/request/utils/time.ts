export function parseYmdUtc(s: string): Date | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(s || '').trim());
  if (!m) return null;

  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const d = Number(m[3]);

  const t = Date.UTC(y, mo, d);
  const dt = new Date(t);

  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== mo || dt.getUTCDate() !== d) {
    return null;
  }

  return dt;
}

export function addMonthsUtc(dateUtc: Date, months: number): Date {
  return new Date(Date.UTC(dateUtc.getUTCFullYear(), dateUtc.getUTCMonth() + months, dateUtc.getUTCDate()));
}
