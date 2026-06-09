type RefCheckRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
};

export type HeadGreenRunSummary = {
  id?: number;
  name: string;
  status: string;
  conclusion: string;
};

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function isGreenCheckConclusion(conclusion: string): boolean {
  const value = toStringTrim(conclusion).toLowerCase();
  return value === 'success' || value === 'neutral' || value === 'skipped';
}

export function isBlockingCheckConclusion(conclusion: string): boolean {
  const value = toStringTrim(conclusion).toLowerCase();
  return (
    value === 'failure' ||
    value === 'cancelled' ||
    value === 'timed_out' ||
    value === 'action_required' ||
    value === 'startup_failure' ||
    value === 'stale'
  );
}

export function summarizeHeadGreenRun(run: RefCheckRunLike): HeadGreenRunSummary {
  const id = typeof run?.id === 'number' && Number.isFinite(run.id) ? run.id : undefined;

  return {
    ...(id !== undefined ? { id } : {}),
    name: toStringTrim(run?.name) || '__unnamed__',
    status: toStringTrim(run?.status).toLowerCase(),
    conclusion: toStringTrim(run?.conclusion).toLowerCase(),
  };
}
