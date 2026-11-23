import { type MockedFunction, vi } from 'vitest';
import { createHookApi } from '../src/handlers/request/validation/hook-api.js';

type FetchFn = (input: string, init?: RequestInit) => Promise<Response>;

const globalFetchRef = globalThis as unknown as { fetch?: unknown };
const realFetch = globalFetchRef.fetch;
let fetchMock: MockedFunction<FetchFn>;

function mockRes(opts: {
  status: number;
  text?: string;
  headers?: Record<string, string>;
  redirected?: boolean;
  chunkSize?: number;
  type?: string;
}): Response {
  const body =
    opts.text === undefined
      ? null
      : new ReadableStream<Uint8Array>({
          start(controller): void {
            const bytes = new TextEncoder().encode(opts.text);
            const chunkSize = opts.chunkSize ?? Math.max(1, bytes.length);
            for (let offset = 0; offset < bytes.length; offset += chunkSize) {
              controller.enqueue(bytes.subarray(offset, offset + chunkSize));
            }
            controller.close();
          },
        });
  const response =
    opts.status === 0 ? Response.error() : new Response(body, { status: opts.status, headers: opts.headers });
  Object.defineProperties(response, {
    redirected: { value: opts.redirected ?? false },
    type: { value: opts.type ?? response.type },
  });
  return response;
}

afterAll(() => {
  globalFetchRef.fetch = realFetch;
});

beforeEach(() => {
  fetchMock = vi.fn();
  globalFetchRef.fetch = fetchMock;
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('assertAllowedUrl allows an explicitly configured host', () => {
  const api = createHookApi({}, { allowedHosts: ['api.sap.com'] });
  const u = api.assertAllowedUrl('https://api.sap.com/path?a=1');
  expect(u.hostname).toBe('api.sap.com');
});

test('assertAllowedUrl rejects invalid URLs and non-HTTPS schemes', () => {
  const api = createHookApi({}, {});
  expect(() => api.assertAllowedUrl('not a url')).toThrow('Invalid URL');
  expect(() => api.assertAllowedUrl('http://api.sap.com/x')).toThrow('Only HTTPS is allowed');
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

test.each([undefined, {}, { allowedHosts: [] }])('denies outbound HTTP by default with options %j', async (options) => {
  const api = createHookApi({}, options);
  await expect(api.httpGetJson('https://example.com/x')).rejects.toThrow('hooks.allowedHosts');
  expect(fetchMock).not.toHaveBeenCalled();
});

test('allows public hosts only with an explicit wildcard', () => {
  const api = createHookApi({}, { allowedHosts: ['*'] });
  expect(api.assertAllowedUrl('https://example.com/x').hostname).toBe('example.com');
});

test.each([
  '[::1]',
  '[::]',
  '[fe80::1]',
  '[febf::1]',
  '[fc00::1]',
  '[fd00::1]',
  '[::ffff:127.0.0.1]',
  '[::ffff:192.168.1.1]',
])('rejects local IPv6 literal %s even with a wildcard', (host) => {
  const api = createHookApi({}, { allowedHosts: ['*'] });
  expect(() => api.assertAllowedUrl(`https://${host}/x`)).toThrow('Host not allowed');
});

test.each(['[2001:4860:4860::8888]', '[::ffff:8.8.8.8]'])('accepts public IPv6 literal %s with a wildcard', (host) => {
  const api = createHookApi({}, { allowedHosts: ['*'] });
  expect(() => api.assertAllowedUrl(`https://${host}/x`)).not.toThrow();
});

test('normalizes configured hosts and rejects unlisted hosts', () => {
  const api = createHookApi({}, { allowedHosts: ['Example.com:443', 'foo.bar/baz', 'https://a.b/x', 'https://'] });

  expect(api.assertAllowedUrl('https://example.com/x').hostname).toBe('example.com');
  expect(api.assertAllowedUrl('https://foo.bar/x').hostname).toBe('foo.bar');
  expect(api.assertAllowedUrl('https://a.b/y').hostname).toBe('a.b');

  expect(() => api.assertAllowedUrl('https://ignored.invalid/x')).toThrow('Host not allowed');
});

test('httpGetJson sends accept header, normalizes custom headers, strips Authorization/Cookie and applies bearer auth', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: JSON.stringify({ ok: true }) }));

  const out = await api.httpGetJson<{ ok: boolean }>('https://example.com/test', {
    headers: {
      ' X-Test ': ' 1 ',
      Authorization: 'Bearer evil',
      Cookie: 'x=y',
      'Bad Header': 'x',
      'X-Evil': 'a\nb',
      Empty: ' ',
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
    accept: 'application/json',
    'x-test': '1',
    authorization: 'Bearer t123',
  });

  expect(hdrs?.cookie).toBeUndefined();
});

test('httpGetJson supports header auth with optional prefix', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'], secrets: { TOKEN: 't123' } });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: JSON.stringify({ ok: true }) }));

  await api.httpGetJson('https://example.com/test', {
    auth: { type: 'header', header: ' X-Api-Key ', secret: 'TOKEN', prefix: 'Token ' },
  });

  const init = fetchMock.mock.calls[0]?.[1];
  const hdrs = init?.headers as Record<string, string> | undefined;

  expect(hdrs).toMatchObject({
    accept: 'application/json',
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

  fetchMock.mockResolvedValue(mockRes({ status: 302, text: '' }));

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 302)');
});

test('httpGetJson blocks redirects (redirected=true)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: JSON.stringify({ ok: true }),
      redirected: true,
    })
  );

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 200)');
});

test('httpGetJson blocks redirects (opaqueredirect response type)', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 0, type: 'opaqueredirect', text: undefined }));

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('Redirect blocked (HTTP 0)');
});

test('httpGetJson returns HTTP <status>: <snippet> for non-ok responses with body', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 400, text: 'bad\n\nrequest' }));

  await expect(api.httpGetJson('https://example.com/test')).rejects.toThrow('HTTP 400: bad request');
});

test('httpGetJson returns null for empty response body', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(mockRes({ status: 200, text: '   \n\t  ' }));

  await expect(api.httpGetJson('https://example.com/test')).resolves.toBeNull();
});

test('httpGetJson rejects invalid JSON response and includes content-type + snippet', async () => {
  const api = createHookApi({}, { allowedHosts: ['example.com'] });

  fetchMock.mockResolvedValue(
    mockRes({
      status: 200,
      text: 'not-json',
      headers: { 'content-type': 'application/json' },
    })
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
    })
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
    })
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
    })
  );

  await expect(api.httpGetJson('https://example.com/test', { maxBytes: 1024 })).rejects.toThrow(
    'Response too large (1200 > 1024)'
  );
});

test('httpGetJson converts AbortError to "timeout"', async () => {
  vi.useFakeTimers();

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

  vi.advanceTimersByTime(10);
  await expect(p).rejects.toThrow('timeout');

  vi.useRealTimers();
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

  fetchMock.mockImplementation(() => Promise.resolve(mockRes({ status: 200, text: JSON.stringify({ ok: true }) })));

  const spy = vi.spyOn(globalThis, 'setTimeout');

  await api.httpGetJson('https://example.com/test', { timeoutMs: 0 });
  const first = spy.mock.calls.at(-1);
  expect(first?.[1]).toBe(1);

  await api.httpGetJson('https://example.com/test', { timeoutMs: 999_999 });
  const second = spy.mock.calls.at(-1);
  expect(second?.[1]).toBe(30_000);

  spy.mockRestore();
});
