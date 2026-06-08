import { normalizeLogin, toStringTrim, uniqLogins } from '../domain/login-utils.js';

const LOGIN_RE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function extractParentContactCandidates(value: unknown): { logins: string[]; emails: string[] } {
  const logins: string[] = [];
  const emails: string[] = [];

  const pushLogin = (v: unknown): void => {
    const s = normalizeLogin(v);
    if (!s) return;
    if (!LOGIN_RE.test(s)) return;
    logins.push(s);
  };

  const pushEmail = (v: unknown): void => {
    const s = toStringTrim(v);
    if (!s) return;
    const t = s.replace(/^<|>$/g, '').trim();
    if (!EMAIL_RE.test(t)) return;
    emails.push(t);
  };

  const fromString = (raw: string, strongLoginHint: boolean): void => {
    const s = toStringTrim(raw);
    if (!s) return;

    const urlM =
      /(?:https?:\/\/)?(?:www\.)?(?:github\.com|github\.tools\.sap)\/([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)/gi;
    for (const m of s.matchAll(urlM)) {
      if (m?.[1]) pushLogin(m[1]);
    }

    const tokens = s
      .split(/[,\s;]+/)
      .map((t) => t.trim())
      .filter(Boolean);

    for (let t of tokens) {
      t = t.replace(/^[<([{"']+|[>)\]},"']+$/g, '').trim();
      if (!t) continue;

      if (t.includes('@') && EMAIL_RE.test(t)) {
        pushEmail(t);
        continue;
      }

      if (t.startsWith('@')) {
        const u = t.slice(1);
        if (u && !u.includes('.') && LOGIN_RE.test(u)) pushLogin(u);
        continue;
      }

      if ((strongLoginHint || tokens.length === 1) && !t.includes('.') && LOGIN_RE.test(t)) {
        pushLogin(t);
      }
    }
  };

  const walk = (v: unknown, keyHint?: string): void => {
    if (v === null || v === undefined) return;

    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      const k = String(keyHint || '').toLowerCase();
      const strong = ['github', 'login', 'username', 'user', 'owner', 'id', 'uid', 'account', 'gh'].some((x) =>
        k.includes(x)
      );
      fromString(String(v), strong);
      return;
    }

    if (Array.isArray(v)) {
      for (const el of v) walk(el, keyHint);
      return;
    }

    if (isPlainObject(v)) {
      for (const [k, vv] of Object.entries(v)) walk(vv, k);
      return;
    }
  };

  walk(value);

  return { logins: uniqLogins(logins), emails: Array.from(new Set(emails.map((e) => e.toLowerCase()))) };
}

const EMAIL_TO_LOGINS_CACHE = new Map<string, Promise<string[]>>();

export async function lookupGithubLoginsByEmail<ContextType>(context: ContextType, email: string): Promise<string[]> {
  const e = toStringTrim(email).toLowerCase();
  if (!e || !e.includes('@')) return [];

  const cached = EMAIL_TO_LOGINS_CACHE.get(e);
  if (cached) return await cached;

  const p = (async (): Promise<string[]> => {
    const found: string[] = [];
    const q = `${e} in:email`;

    try {
      const res = await (
        context as unknown as {
          octokit: { search: { users: (args: { q: string; per_page: number }) => Promise<unknown> } };
        }
      ).octokit.search.users({ q, per_page: 5 });
      const items = (res as { data?: { items?: { login?: string }[] } })?.data?.items ?? [];
      for (const it of items) {
        const login = normalizeLogin(it?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    if (found.length) return uniqLogins(found);

    try {
      const gql = `
        query($q: String!) {
          search(type: USER, query: $q, first: 5) {
            nodes { ... on User { login } }
          }
        }
      `;
      const r = await (
        context as unknown as {
          octokit: {
            graphql: (q: string, v: unknown) => Promise<{ search?: { nodes?: { login?: string }[] } }>;
          };
        }
      ).octokit.graphql(gql, { q });

      const nodes = r?.search?.nodes ?? [];
      for (const n of nodes) {
        const login = normalizeLogin(n?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    return uniqLogins(found);
  })();

  EMAIL_TO_LOGINS_CACHE.set(e, p);
  return await p;
}
