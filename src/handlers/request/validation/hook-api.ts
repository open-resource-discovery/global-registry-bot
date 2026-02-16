/**
 * Safe API surface for repo hooks:
 * - HTTPS only
 * - Host allowlist
 * - Timeout + max response size
 * - Redirects blocked
 * - Secrets never exposed to the hook (auth via secret key only)
 *
 * Uses Node.js built-in fetch (Node 18+ / undici).
 */

export type HookSecrets = Readonly<Record<string, string | undefined>>;

export interface BearerAuth {
  type: 'bearer';
  secret: string;
}

export interface HeaderAuth {
  type: 'header';
  header: string;
  secret: string;
  prefix?: string;
}

export type HttpAuth = BearerAuth | HeaderAuth;

export interface HttpGetJsonOptions {
  timeoutMs?: number;
  maxBytes?: number;
  auth?: HttpAuth;
  headers?: Readonly<Record<string, string>>;
}

export interface CreateHookApiOptions {
  secrets?: HookSecrets;
  allowedHosts?: readonly string[];
}

export interface HookApi {
  httpGetJson<T = unknown>(url: string, opts?: HttpGetJsonOptions): Promise<T>;
  assertAllowedUrl(url: string): URL;
}

// NOTE: '*' means allow any public HTTPS host
const DEFAULT_ALLOWED_HOSTS: readonly string[] = ['*'];

const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_MAX_BYTES = 1024 * 1024;

const MAX_TIMEOUT_MS = 30_000;
const MAX_MAX_BYTES = 5 * 1024 * 1024;

function clampNumber(v: unknown, fallback: number, min: number, max: number): number {
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function getFetch(): typeof fetch {
  const f = (globalThis as { fetch?: unknown }).fetch;
  if (typeof f !== 'function') {
    throw new Error('fetch is not available in this runtime');
  }
  return f as typeof fetch;
}

function canonicalizeHost(hostname: string): string {
  const h = String(hostname ?? '')
    .trim()
    .toLowerCase();
  return h.replace(/\.+$/g, '');
}

function normalizeHostInput(input: string): string {
  const raw = String(input ?? '').trim();
  if (!raw) return '';

  if (raw.includes('://')) {
    try {
      return canonicalizeHost(new URL(raw).hostname);
    } catch {
      return '';
    }
  }

  const hostOnly = raw.split('/')[0] ?? '';
  const noPort = hostOnly.split(':')[0] ?? '';
  return canonicalizeHost(noPort);
}

function isLoopbackLikeHost(host: string): boolean {
  const h = canonicalizeHost(host);
  return h === 'localhost' || h === '127.0.0.1' || h === '::1' || h === '0.0.0.0';
}

function isIpv4Literal(host: string): boolean {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
}

function isPrivateOrLinkLocalIpv4(host: string): boolean {
  if (!isIpv4Literal(host)) return false;

  const parts = host.split('.').map((x) => Number(x));
  if (parts.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) return true;

  const [a, b] = parts;
  if (a === 10) return true;
  if (a === 127) return true;
  if (a === 0) return true;
  if (a === 169 && b === 254) return true;
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 192 && b === 168) return true;

  return false;
}

function isIpv6Literal(host: string): boolean {
  return host.includes(':');
}

function isPrivateOrLinkLocalIpv6(host: string): boolean {
  const h = canonicalizeHost(host);
  if (!isIpv6Literal(h)) return false;

  if (h === '::1') return true;
  if (h.startsWith('fe80:')) return true;
  if (h.startsWith('fc') || h.startsWith('fd')) return true;

  return false;
}

function isAbortError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const name = (err as { name?: unknown }).name;
  return typeof name === 'string' && name === 'AbortError';
}

function isSafeHeaderName(name: string): boolean {
  // RFC 7230 token-ish
  return /^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(name);
}

function isSafeHeaderValue(value: string): boolean {
  // Prevent CRLF injection
  return !/[\r\n]/.test(value);
}

function normalizeHeaderRecord(headers?: Readonly<Record<string, string>>): Record<string, string> {
  const out: Record<string, string> = {};
  if (!headers) return out;

  for (const [k, v] of Object.entries(headers)) {
    const key = String(k ?? '').trim();
    const val = String(v ?? '').trim();
    if (!key || !val) continue;
    if (!isSafeHeaderName(key)) continue;
    if (!isSafeHeaderValue(val)) continue;

    const keyLc = key.toLowerCase();
    if (keyLc === 'authorization' || keyLc === 'cookie') continue;

    out[keyLc] = val;
  }

  return out;
}

function buildRequestHeaders(opts: HttpGetJsonOptions, secrets: HookSecrets): Record<string, string> {
  const headers: Record<string, string> = {
    accept: 'application/json',
    ...normalizeHeaderRecord(opts.headers),
  };

  const auth = opts.auth;
  if (!auth) return headers;

  if (auth.type === 'bearer') {
    const name = String(auth.secret ?? '').trim();
    if (!name) throw new Error('Missing auth secret name');
    const token = secrets[name];
    if (!token) throw new Error(`Missing secret: ${name}`);
    headers.authorization = `Bearer ${token}`;
    return headers;
  }

  if (auth.type === 'header') {
    const headerName = String(auth.header ?? '')
      .trim()
      .toLowerCase();
    if (!headerName || !isSafeHeaderName(headerName)) throw new Error('Invalid auth header name');

    const name = String(auth.secret ?? '').trim();
    if (!name) throw new Error('Missing auth secret name');
    const token = secrets[name];
    if (!token) throw new Error(`Missing secret: ${name}`);

    const prefix = String(auth.prefix ?? '');
    const value = `${prefix}${token}`;
    if (!isSafeHeaderValue(value)) throw new Error('Invalid auth header value');

    headers[headerName] = value;
    return headers;
  }

  return headers;
}

async function readBodyLimited(
  res: Response,
  { maxBytes, controller }: { maxBytes: number; controller: AbortController }
): Promise<string> {
  const lenHeader = res.headers.get('content-length');
  const len = lenHeader ? Number(lenHeader) : 0;

  if (Number.isFinite(len) && len > maxBytes) {
    controller.abort();
    throw new Error(`Response too large (content-length ${len} > ${maxBytes})`);
  }

  const body = res.body;
  if (!body) return '';

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (!value) continue;

    total += value.byteLength;
    if (total > maxBytes) {
      controller.abort();
      throw new Error(`Response too large (${total} > ${maxBytes})`);
    }

    chunks.push(value);
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.byteLength;
  }

  return new TextDecoder('utf-8').decode(merged);
}

function compactSnippet(text: string, maxChars: number): string {
  const s = String(text ?? '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return '';
  return s.length > maxChars ? `${s.slice(0, maxChars)}…` : s;
}

export function createHookApi(
  _context: unknown,
  { secrets = {}, allowedHosts = [] }: CreateHookApiOptions = {}
): HookApi {
  const allow = new Set<string>();
  let allowAllHosts = false;

  for (const h of DEFAULT_ALLOWED_HOSTS) {
    if (String(h).trim() === '*') allowAllHosts = true;
    else allow.add(canonicalizeHost(h));
  }

  for (const h of allowedHosts) {
    const raw = String(h ?? '').trim();
    if (raw === '*') {
      allowAllHosts = true;
      continue;
    }
    const host = normalizeHostInput(raw);
    if (host) allow.add(host);
  }

  function assertAllowedUrl(rawUrl: string): URL {
    let u: URL;
    try {
      u = new URL(String(rawUrl));
    } catch {
      throw new Error('Invalid URL');
    }

    if (u.protocol !== 'https:') throw new Error('Only HTTPS is allowed');

    if (u.username || u.password) {
      throw new Error('Credentials in URL are not allowed');
    }

    const host = canonicalizeHost(u.hostname);
    if (isPrivateOrLinkLocalIpv4(host)) throw new Error(`Host not allowed: ${host}`);
    if (isPrivateOrLinkLocalIpv6(host)) throw new Error(`Host not allowed: ${host}`);

    if (!host) throw new Error('Invalid host');

    if (isLoopbackLikeHost(host)) throw new Error(`Host not allowed: ${host}`);
    if (!allowAllHosts && !allow.has(host)) throw new Error(`Host not allowed: ${host}`);

    return u;
  }

  async function httpGetJson<T = unknown>(url: string, opts: HttpGetJsonOptions = {}): Promise<T> {
    const timeoutMs = clampNumber(opts.timeoutMs, DEFAULT_TIMEOUT_MS, 1, MAX_TIMEOUT_MS);
    const maxBytes = clampNumber(opts.maxBytes, DEFAULT_MAX_BYTES, 1_024, MAX_MAX_BYTES);

    const u = assertAllowedUrl(url);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const reqHeaders = buildRequestHeaders(opts, secrets);
      const fetchFn = getFetch();

      const res = await fetchFn(u.toString(), {
        method: 'GET',
        headers: reqHeaders,
        signal: controller.signal,
        redirect: 'manual',
      });

      // Fetch redirect modes: follow|error|manual. Manual may produce an opaque-redirect response.
      const resAny = res as unknown as { type?: unknown };
      const resType = typeof resAny.type === 'string' ? resAny.type : '';
      const isOpaqueRedirect = resType === 'opaqueredirect';
      const is3xx = res.status >= 300 && res.status < 400;

      if (isOpaqueRedirect || is3xx || res.redirected) {
        const code = res.status || 0;
        throw new Error(`Redirect blocked (HTTP ${code})`);
      }

      if (!res.ok) {
        const errMax = Math.min(maxBytes, 16 * 1024);
        let body = '';
        try {
          body = await readBodyLimited(res, { maxBytes: errMax, controller });
        } catch {
          // ignore
        }
        const snippet = compactSnippet(body, 400);
        throw new Error(snippet ? `HTTP ${res.status}: ${snippet}` : `HTTP ${res.status}`);
      }

      const text = await readBodyLimited(res, { maxBytes, controller });
      if (!text.trim()) return null as T;

      try {
        return JSON.parse(text) as T;
      } catch {
        const ct = String(res.headers.get('content-type') || '').trim();
        const snippet = compactSnippet(text, 400);
        throw new Error(`Invalid JSON response${ct ? ` (content-type: ${ct})` : ''}${snippet ? `: ${snippet}` : ''}`);
      }
    } catch (err: unknown) {
      const msg = isAbortError(err) ? 'timeout' : err instanceof Error ? err.message : String(err);
      throw new Error(msg);
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    httpGetJson,
    assertAllowedUrl,
  };
}
