import crypto from 'node:crypto';
import { categoryFromTemplate as categoryFromTemplateRaw } from '../template.js';

type TemplateField = {
  id?: string;
  type?: string;
  [k: string]: unknown;
};

type TemplateLike = {
  name?: string;
  title?: string;
  body?: TemplateField[];
  [k: string]: unknown;
};

type Snapshot = Record<string, string>;

type PullRequestLike = {
  number: number;
  body?: string | null;
  [k: string]: unknown;
};

type OctokitLike = {
  pulls: {
    list: (args: {
      owner: string;
      repo: string;
      state: 'open' | 'closed' | 'all';
      per_page?: number;
      page?: number;
    }) => Promise<{ data: PullRequestLike[] }>;
  };
};

type ProbotState = Record<string, unknown> & {
  _nsreqOpenPrCache?: Map<string, PullRequestLike[]>;
};

type ContextLike = {
  octokit: OctokitLike;
  state?: ProbotState;
};

const categoryFromTemplate = categoryFromTemplateRaw as unknown as (template: TemplateLike) => string;

function sha1(str: unknown): string {
  return crypto.createHash('sha1').update(String(str)).digest('hex');
}

// Marker key for snapshot hashes inside PR bodies
export const SNAPSHOT_HASH_MARKER_KEY = 'nsreq:snapshot-hash';

// Stable stringify with sorted keys and deterministic recursion
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    const items = value.map((v) => stableStringify(v));
    return `[${items.join(',')}]`;
  }

  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  const parts = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`);
  return `{${parts.join(',')}}`;
}

/**
 * Generic scalar normalization:
 * - trim strings
 * - arrays -> comma-separated list
 * - objects -> ignore (empty string)
 * - null/undefined -> empty
 */
function normalizeScalar(value: unknown): string {
  if (value === null || typeof value === 'undefined') return '';

  if (Array.isArray(value)) {
    return value
      .map((x) => String(x).trim())
      .filter(Boolean)
      .join(', ');
  }

  if (typeof value === 'object') {
    return '';
  }

  return String(value).trim();
}

/**
 * Template-driven snapshot:
 * - uses only fields from template.body
 * - ignores Markdown blocks
 * - includes only non-empty values in the snapshot
 * - adds some meta (_template, _category)
 */
export function pickSnapshot(
  formData: Record<string, unknown> | null | undefined,
  template: TemplateLike | null = null
): Snapshot {
  const data: Record<string, unknown> = formData || {};

  if (template && Array.isArray(template.body)) {
    const snapshot: Snapshot = {};

    const templateName = String(template.name || template.title || '').trim();
    if (templateName) snapshot._template = templateName;

    const category = categoryFromTemplate?.(template);
    if (category) snapshot._category = String(category).trim();

    for (const f of template.body) {
      const id = f?.id;
      if (!id) continue;

      const t = String(f?.type || '').toLowerCase();
      if (t === 'markdown') continue;

      if (!Object.prototype.hasOwnProperty.call(data, id)) continue;

      const val = normalizeScalar(data[id]);
      if (val !== '') {
        snapshot[id] = val;
      }
    }

    return snapshot;
  }

  const snapshot: Snapshot = {};
  const keys = Object.keys(data).sort();

  for (const k of keys) {
    const val = normalizeScalar(data[k]);
    if (val !== '') {
      snapshot[k] = val;
    }
  }

  return snapshot;
}

export function calcSnapshotHash(
  formData: Record<string, unknown>,
  template: TemplateLike,
  rawBody: string = ''
): string {
  const base = pickSnapshot(formData, template);

  const category = categoryFromTemplate?.(template) || '';
  const templateName = String(template?.name || template?.title || '').trim();
  const bodyNorm = String(rawBody || '')
    .replace(/\r\n/g, '\n')
    .trim();

  const envelope: Record<string, unknown> = { ...base };

  if (templateName) envelope._template = templateName;
  if (category) envelope._category = category;
  if (bodyNorm) envelope._body = bodyNorm;

  const json = stableStringify(envelope);
  return sha1(json);
}

export function extractHashFromPrBody(body: string | null | undefined): string | null {
  if (!body) return null;

  const re = new RegExp(`<!--\\s*${SNAPSHOT_HASH_MARKER_KEY}\\s*:\\s*([0-9a-f]{40})\\s*-->`, 'i');
  const m = String(body).match(re);
  return m ? m[1] : null;
}

export async function findOpenIssuePrs(
  context: ContextLike,
  repoRef: { owner: string; repo: string },
  issue_number: number
): Promise<PullRequestLike[]> {
  const { owner, repo } = repoRef;

  const cacheKey = `${owner}/${repo}:open-prs`;
  const state: ProbotState = context.state ?? (context.state = {});
  const cache = (state._nsreqOpenPrCache ??= new Map<string, PullRequestLike[]>());

  if (!cache.has(cacheKey)) {
    const all: PullRequestLike[] = [];
    let page = 1;

    for (;;) {
      // GitHub REST endpoints are paginated
      const { data } = await context.octokit.pulls.list({
        owner,
        repo,
        state: 'open',
        per_page: 100,
        page,
      });

      all.push(...data);
      if (data.length < 100) break;

      page += 1;
    }

    cache.set(cacheKey, all);
  }

  const prs = cache.get(cacheKey) || [];
  return prs.filter((pr) => {
    const body = pr.body || '';

    // Keep the matching behavior identical to the JS version
    const m =
      body.match(/\bsource\s*:\s*#(\d+)\b/i) ||
      body.match(/\bissue\s*#(\d+)\b/i) ||
      body.match(/\bfix(?:es)?\s*:\s*#(\d+)\b/i) ||
      body.match(/\bfix(?:es)?\s+#(\d+)\b/i) ||
      body.match(/\bclose(?:s)?\s*:\s*#(\d+)\b/i) ||
      body.match(/\bresolve(?:s)?\s*:\s*#(\d+)\b/i);

    const noRaw = m ? m.slice(1).find(Boolean) : null;
    if (!noRaw) return false;

    const no = Number.parseInt(noRaw, 10);
    return no === issue_number;
  });
}
