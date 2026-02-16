import { jest } from '@jest/globals';
import { createHookApi } from '../src/handlers/request/validation/hook-api.js';

type FetchFn = (input: string, init?: RequestInit) => Promise<Response>;

type MockHeaders = { get: (k: string) => string | null };
type ReaderReadResult = { done: boolean; value?: Uint8Array };
type MockReader = { read: () => Promise<ReaderReadResult> };
type MockBody = { getReader: () => MockReader };

type MockResponse = {
  status: number;
  ok: boolean;
  redirected: boolean;
  headers: MockHeaders;
  body: MockBody | null;
  type?: string;
};

const globalFetchRef = globalThis as unknown as { fetch?: unknown };
const realFetch = globalFetchRef.fetch;

import type { MockedFunction } from 'jest-mock';
let fetchMock: MockedFunction<FetchFn>;

function headersFrom(obj: Record<string, string>): MockHeaders {
  const map: Record<string, string> = {};
  for (const [k, v] of Object.entries(obj)) map[String(k).toLowerCase()] = String(v);
  return {
    get: (k: string) => map[String(k).toLowerCase()] ?? null,
  };
}

function bodyFromText(text: string, chunkSize: number = text.length): MockBody {
  const bytes = Buffer.from(text, 'utf8');
  let offset = 0;

  return {
    getReader: () => ({
      read: async (): Promise<ReaderReadResult> => {
        await Promise.resolve(); // Ensure async method contains an await
        if (offset >= bytes.length) return { done: true };

        const end = Math.min(bytes.length, offset + chunkSize);
        const chunk = bytes.subarray(offset, end);
        offset = end;

        return { done: false, value: chunk };
      },
    }),
  };
}

function mockRes(opts: {
  status: number;
  text?: string;
  headers?: Record<string, string>;
  redirected?: boolean;
  chunkSize?: number;
  type?: string;
}): MockResponse {
  const status = opts.status;
  const hdrs = opts.headers ?? {};
  const redirected = opts.redirected ?? false;

  return {
    status,
    ok: status >= 200 && status < 300,
    redirected,
    type: opts.type,
    headers: headersFrom(hdrs),
    body: opts.text === undefined ? null : bodyFromText(opts.text, opts.chunkSize),
  };
}

afterAll(() => {
  globalFetchRef.fetch = realFetch;
});

beforeEach(() => {
  fetchMock = jest.fn() as MockedFunction<FetchFn>;
  globalFetchRef.fetch = fetchMock as unknown as typeof fetch;
});

afterEach(() => {
  jest.restoreAllMocks();
});

test('assertAllowedUrl allows default host api.sap.com', () => {
  const api = createHookApi({}, { allowedHosts: ['api.sap.com'] });
  const u = api.assertAllowedUrl('https://api.sap.com/path?a=1');
  expect(u.hostname).toBe('api.sap.com');
});

test('assertAllowedUrl rejects invalid url / non-https / non-allowed host', () => {
  const api = createHookApi({}, {});
  expect(() => api.assertAllowedUrl('not a url')).toThrow('Invalid URL');
  expect(() => api.assertAllowedUrl('http://api.sap.com/x')).toThrow('Only HTTPS is allowed');
  expect(() => api.assertAllowedUrl('https://example.com/x')).toThrow('Host not allowed: example.com');
});

test('assertAllowedUrl rejects credentials in URL', () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });
  expect(() => api.assertAllowedUrl('https://user:pass@example.com/x')).toThrow('Credentials in URL are not allowed');
});

test('assertAllowedUrl rejects loopback-like hosts even if allowlisted', () => {
  const api = createHookApi({}, { allowedHosts: ['localhost', '127.0.0.1', '::1', '0.0.0.0'] });

  expect(() => api.assertAllowedUrl('https://localhost/x')).toThrow('Host not allowed: localhost');
  expect(() => api.assertAllowedUrl('https://127.0.0.1/x')).toThrow('Host not allowed: 127.0.0.1');
  expect(() => api.assertAllowedUrl('https://0.0.0.0/x')).toThrow('Host not allowed: 0.0.0.0');
});

test('assertAllowedUrl rejects empty/invalid host', () => {
  const api = createHookApi({}, {});
  expect(() => api.assertAllowedUrl('https://./')).toThrow('Invalid host');
});

test('assertAllowedUrl allows trailing-dot host via canonicalization', () => {
  const api = createHookApi({}, { allowedHosts: ['api.sap.com'] });
  const u = api.assertAllowedUrl('https://api.sap.com./x');
  expect(u.hostname).toBe('api.sap.com.');
});

test('assertAllowedUrl denies all hosts by default (deny-by-default)', () => {
  const api = createHookApi({}, {});
  expect(() => api.assertAllowedUrl('https://api.sap.com/x')).toThrow('Host not allowed: api.sap.com');
});

test('allowedHosts supports host:port, host/path and full urls; ignores invalid entries', () => {
  const api = createHookApi({}, { allowedHosts: ['Example.com:443', 'foo.bar/baz', 'https://a.b/x', 'https://'] });

  expect(api.assertAllowedUrl('https://example.com/x').hostname).toBe('example.com');
  expect(api.assertAllowedUrl('https://foo.bar/x').hostname).toBe('foo.bar');
  expect(api.assertAllowedUrl('https://a.b/y').hostname).toBe('a.b');

  expect(() => api.assertAllowedUrl('https://ignored.invalid/x')).toThrow('Host not allowed: ignored.invalid');
});

test('httpGetJson sends accept header, normalizes custom headers, strips Authorization/Cookie and applies bearer auth', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: JSON.stringify({ ok: true }) }) as unknown as Response);

  const out = await api.httpGetJson<{ ok: boolean }>('https://example.com/test', {
    headers: {
      ' X-Test ': ' 1 ',
      'Authorization': 'Bearer evil',
      'Cookie': 'x=y',
      'Bad Header': 'x',
      'X-Evil': 'a\nb',
      'Empty': ' ',
    },
    auth: { type: 'bearer', secret: 'TOKEN' },
  });

  expect(out).toEqual({ ok: true });

  const call = fetchMock.mock.calls[0];
  const init = call?.[1];
  expect(call?.[0]).toBe('https://example.com/test');
  expect(init?.method).toBe('GET');
  expect(init?.redirect).toBe('manual');

  const hdrs = init?.headers as Record<string, string> | undefined;
  expect(hdrs).toMatchObject({
    'accept': 'application/json',
    'x-test': '1',
    'authorization': 'Bearer t123',
  });

  expect(hdrs?.cookie).toBeUndefined();
});

test('httpGetJson supports header auth with optional prefix', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: JSON.stringify({ ok: true }) }) as unknown as Response);

  await api.httpGetJson('https://example.com/test', {
    auth: { type: 'header', header: ' X-Api-Key ', secret: 'TOKEN', prefix: 'Token ' },
  });

  const init = fetchMock.mock.calls[0]?.[1];
  const hdrs = init?.headers as Record<string, string> | undefined;

  expect(hdrs).toMatchObject({
    'accept': 'application/json',
    'x-api-key': 'Token t123',
  });
  expect(hdrs?.authorization).toBeUndefined();
});

test('httpGetJson fails if auth secret name is missing (bearer + header)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  await expect(
    api.httpGetJson('https://example.com/test', { auth: { type: 'bearer', secret: '   ' } })
  ).rejects.toThrow('Missing auth secret name');

  await expect(
    api.httpGetJson('https://example.com/test', {
      auth: { type: 'header', header: 'x', secret: ' ' },
    })
  ).rejects.toThrow('Missing auth secret name');
});

test('httpGetJson fails for invalid auth header name/value', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  await expect(
    api.httpGetJson('https://example.com/test', {
      auth: { type: 'header', header: 'Bad Header', secret: 'TOKEN' },
    })
  ).rejects.toThrow('Invalid auth header name');

  await expect(
    api.httpGetJson('https://example.com/test', {
      auth: { type: 'header', header: 'x-api-key', secret: 'TOKEN', prefix: 'Token\n' },
    })
  ).rejects.toThrow('Invalid auth header value');
});

test('httpGetJson fails if bearer secret is missing', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: {} });

  await expect(
    api.httpGetJson('https://example.com/test', { auth: { type: 'bearer', secret: 'MISSING' } })
  ).rejects.toThrow('Missing secret: MISSING');
});

test('httpGetJson blocks redirects (3xx)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 302, text: '' }) as unknown as Response);

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 302)');
});

test('httpGetJson blocks redirects (redirected=true)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: JSON.stringify({ ok: true }),
      redirected: true,
    }) as unknown as Response
  );

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 200)');
});

test('httpGetJson blocks redirects (opaqueredirect response type)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 0, type: 'opaqueredirect', text: undefined }) as unknown as Response);

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 0)');
});

test('httpGetJson returns HTTP <status>: <snippet> for non-ok responses with body', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 400, text: 'bad\n\nrequest' }) as unknown as Response);

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('HTTP 400: bad request');
});

test('httpGetJson returns null for empty response body', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: '   \n\t  ' }) as unknown as Response);

  await expect(api.httpGetJson('https://example.com/test')).resolves.toBeNull();
});

test('httpGetJson rejects invalid JSON response and includes content-type + snippet', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: 'not-json',
      headers: { 'content-type': 'application/json' },
    }) as unknown as Response
  );

  const p = api.httpGetJson('https://example.com/test');

  await expect(p).rejects.toThrow('Invalid JSON response');

  await p.catch((e: unknown) => {
    const msg = e instanceof Error ? e.message : String(e);
    expect(msg).toContain('content-type: application/json');
    expect(msg).toContain('not-json');
  });
});

test('httpGetJson rejects if content-length exceeds maxBytes (min clamp to 1024)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: undefined,
      headers: { 'content-length': '2048' },
    }) as unknown as Response
  );

  await expect(api.httpGetJson('https://example.com/test', { maxBytes: 1 })).rejects.toThrow(
    'Response too large (content-length 2048 > 1024)'
  );
});

test('httpGetJson rejects if content-length exceeds maxBytes (max clamp to 5MB)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: undefined,
      headers: { 'content-length': String(6 * 1024 * 1024) },
    }) as unknown as Response
  );

  await expect(api.httpGetJson('https://example.com/test', { maxBytes: 999_999_999 })).rejects.toThrow(
    `Response too large (content-length ${6 * 1024 * 1024} > ${5 * 1024 * 1024})`
  );
});

test('httpGetJson rejects if streamed body exceeds maxBytes', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: 'x'.repeat(1200),
      chunkSize: 600,
    }) as unknown as Response
  );

  await expect(api.httpGetJson('https://example.com/test', { maxBytes: 1024 })).rejects.toThrow(
    'Response too large (1200 > 1024)'
  );
});

test('httpGetJson converts AbortError to "timeout"', async () => {
  jest.useFakeTimers();

  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockImplementation((_url: string, init?: RequestInit) => {
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => {
        const e = new Error('aborted');
        (e as { name?: string }).name = 'AbortError';
        reject(e);
      });
    });
  });

  const p = api.httpGetJson('https://example.com/test', { timeoutMs: 5 });

  jest.advanceTimersByTime(10);
  await expect(p).rejects.toThrow('timeout');

  jest.useRealTimers();
});

test('httpGetJson fails when fetch is not available', async () => {
  const prev = globalFetchRef.fetch;
  globalFetchRef.fetch = undefined;

  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('fetch is not available in this runtime');

  globalFetchRef.fetch = prev;
});

test('httpGetJson propagates fetch/network errors', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockRejectedValue(new Error('network down'));

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('network down');
});

test('httpGetJson clamps timeoutMs to [1..30000] (via setTimeout argument)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: JSON.stringify({ ok: true }) }) as unknown as Response);

  const spy = jest.spyOn(globalThis, 'setTimeout');

  await api.httpGetJson('https://example.com/test', { timeoutMs: 0 });
  const first = spy.mock.calls.at(-1);
  expect(first?.[1]).toBe(1);

  await api.httpGetJson('https://example.com/test', { timeoutMs: 999_999 });
  const second = spy.mock.calls.at(-1);
  expect(second?.[1]).toBe(30_000);

  spy.mockRestore();
});
