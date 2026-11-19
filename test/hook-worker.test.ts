import { jest } from '@jest/globals';

type Task = {
  owner: string;
  repo: string;
  path: string;
  hash: string;
  code: string;
  fn: string;
  args: unknown;
  allowedHosts?: string[];
  secrets?: Record<string, string>;
};

type HookWorkerResult = {
  found: boolean;
  value: unknown;
  logs: { level: 'debug' | 'info' | 'warn' | 'error'; obj: unknown; msg?: string }[];
};

let hookWorker: (task: Task) => Promise<HookWorkerResult>;

let originalFetch: unknown;
let originalExit: NodeJS.Process['exit'];
let originalKill: NodeJS.Process['kill'];
let originalEnableFlag: string | undefined;

let seq = 0;

function nextHash(): string {
  seq += 1;
  return `${seq}`.padStart(16, '0');
}

function setGlobalFetch(fn: unknown): void {
  (globalThis as unknown as { fetch?: unknown }).fetch = fn;
}

function getGlobalFetch(): unknown {
  return (globalThis as unknown as { fetch?: unknown }).fetch;
}

function mkTask(args: { code: string; fn: string; args?: unknown; allowedHosts?: string[] }): Task {
  return {
    owner: 'o',
    repo: 'r',
    path: '.github/registry-bot/config.js',
    hash: nextHash(),
    code: args.code,
    fn: args.fn,
    args: args.args ?? {},
    allowedHosts: args.allowedHosts,
    secrets: { DUMMY: '1' },
  };
}

beforeAll(async (): Promise<void> => {
  const mod = await import('../src/handlers/request/validation/hook-worker.js');
  hookWorker = mod.default;
});

beforeEach((): void => {
  originalFetch = getGlobalFetch();
  originalExit = process.exit;
  originalKill = process.kill;
  originalEnableFlag = process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
  // Enable JS hooks for all existing happy-path tests by default.
  // Security regression tests below explicitly unset the flag.
  process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = 'true';
});

afterEach((): void => {
  setGlobalFetch(originalFetch);
  process.exit = originalExit;
  process.kill = originalKill;
  if (originalEnableFlag === undefined) {
    delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
  } else {
    process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = originalEnableFlag;
  }
});

test('returns found=false when function does not exist', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `export default {}`,
    fn: 'onValidate',
    args: { requestType: 'product' },
  });

  const res = await hookWorker(task);

  expect(res).toEqual({ found: false, value: undefined, logs: [] });
});

test('calls default-exported function and captures logs + return value', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export default {
        onValidate({ log }) {
          log.info({ x: 1 }, 'hello');
          return ['err1'];
        }
      };
    `,
    fn: 'onValidate',
    args: { requestType: 'product' },
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual(['err1']);
  expect(res.logs).toEqual([{ level: 'info', obj: { x: 1 }, msg: 'hello' }]);
});

test('resolves named export when not present on default export', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export async function onValidate({ log }) {
        log.warn({ ok: true }, 'named');
        return [];
      }
      export default {};
    `,
    fn: 'onValidate',
    args: { requestType: 'product' },
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual([]);
  expect(res.logs).toEqual([{ level: 'warn', obj: { ok: true }, msg: 'named' }]);
});

test('beforeValidate returns mutated form for main-thread re-apply', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export default {
        beforeValidate({ form, log }) {
          form.identifier = 'X';
          log.debug({ id: form.identifier }, 'mutated');
        }
      };
    `,
    fn: 'beforeValidate',
    args: { requestType: 'product', form: { identifier: 'A' } },
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual({ form: { identifier: 'X' } });
  expect(res.logs).toEqual([{ level: 'debug', obj: { id: 'X' }, msg: 'mutated' }]);
});

test('overrides fetch in worker and forces redirect=manual', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, init?: RequestInit) => {
    // we assert redirect is injected by worker override
    expect(init?.redirect).toBe('manual');
    return Promise.resolve(new Response('ok'));
  });
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export async function onValidate() {
        await fetch('https://api.sap.com/odata/1.0/catalog.svc/Products?$format=json');
        return [];
      }
      export default { onValidate };
    `,
    fn: 'onValidate',
    args: { requestType: 'product' },
    allowedHosts: ['api.sap.com'],
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual([]);
  expect(baseFetch).toHaveBeenCalledTimes(1);
});

test('blocks disallowed fetch host and returns __hookError', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export async function onValidate() {
        await fetch('https://127.0.0.1/');
        return [];
      }
      export default { onValidate };
    `,
    fn: 'onValidate',
    args: { requestType: 'product' },
    allowedHosts: ['api.sap.com'],
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual(
    expect.objectContaining({
      __hookError: expect.stringContaining('Host not allowed'),
    })
  );
});

test('process.exit is disabled; calling it becomes __hookError', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export default {
        onValidate() {
          process.exit(0);
          return [];
        }
      };
    `,
    fn: 'onValidate',
    args: { requestType: 'product' },
  });

  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual(
    expect.objectContaining({
      __hookError: expect.stringContaining('process.exit is disabled'),
    })
  );
});

// --- Security: REGISTRY_BOT_ENABLE_JS_HOOKS fail-closed ---

test.each([
  ['omitted', undefined],
  ['empty', []],
] as const)('worker blocks outbound fetch with an %s host allowlist', async (_name, allowedHosts): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = mkTask({
    code: `
      export async function onValidate() {
        await fetch('https://unlisted.example/path');
        return [];
      }
    `,
    fn: 'onValidate',
    ...(allowedHosts === undefined ? {} : { allowedHosts: [...allowedHosts] }),
  });
  const res = await hookWorker(task);

  expect(res.found).toBe(true);
  expect(res.value).toEqual({ __hookError: expect.stringContaining('Host not allowed: unlisted.example') });
  expect(baseFetch).not.toHaveBeenCalled();
});

test.each([undefined, '', 'false', '1', 'TRUE'])(
  'does not evaluate hook modules when the deployment flag is %s',
  async (flag): Promise<void> => {
    if (flag === undefined) delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
    else process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = flag;
    const task = mkTask({
      code: `
      throw new Error('Hook module must not be evaluated');
      export default {
        onValidate() {
          return ['EXECUTED'];
        }
      };
    `,
      fn: 'onValidate',
      args: { requestType: 'product' },
    });

    const res = await hookWorker(task);

    expect(res.found).toBe(false);
    expect(res.value).toBeUndefined();
    expect(res.logs).toEqual([]);
  }
);

test('does not retain a previous task allowlist when a worker is reused', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const first = await hookWorker(mkTask({ code: 'export function onValidate() { return []; }', fn: 'onValidate' }));
  expect(first.value).toEqual([]);

  const second = await hookWorker(
    mkTask({
      code: `export async function onValidate() { await fetch('https://allowed.example/path'); return []; }`,
      fn: 'onValidate',
      allowedHosts: ['allowed.example'],
    })
  );
  expect(second.value).toEqual([]);
  expect(baseFetch).toHaveBeenCalledTimes(1);
  expect(getGlobalFetch()).toBe(baseFetch);
});

test('restores fetch after a failed task and applies the next task allowlist', async (): Promise<void> => {
  const baseFetch = jest.fn((_input: RequestInfo | URL, _init?: RequestInit) => Promise.resolve(new Response('ok')));
  setGlobalFetch(baseFetch);

  const task = (host: string, allowedHosts: string[]): Task =>
    mkTask({
      code: `export async function onValidate() { await fetch('https://${host}/path'); return []; }`,
      fn: 'onValidate',
      allowedHosts,
    });
  const first = await hookWorker(task('blocked.example', []));
  expect(first.value).toEqual({ __hookError: expect.stringContaining('Host not allowed') });
  expect(getGlobalFetch()).toBe(baseFetch);

  const second = await hookWorker(task('allowed.example', ['allowed.example']));
  expect(second.value).toEqual([]);
  expect(baseFetch).toHaveBeenCalledTimes(1);

  const third = await hookWorker(task('allowed.example', []));
  expect(third.value).toEqual({ __hookError: expect.stringContaining('Host not allowed') });
  expect(baseFetch).toHaveBeenCalledTimes(1);
  expect(getGlobalFetch()).toBe(baseFetch);
});

test('JS hooks disabled: malicious dynamic import(node:child_process) never executes', async (): Promise<void> => {
  delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];

  const task = mkTask({
    code: `
      export default {
        async onValidate() {
          const cp = await import('node:child_process');
          cp.execSync('echo pwned');
          return [];
        }
      };
    `,
    fn: 'onValidate',
    args: {},
  });

  const res = await hookWorker(task);

  // Worker must return early without executing — never reach the import()
  expect(res.found).toBe(false);
  expect(res.value).toBeUndefined();
});

test('JS hooks disabled: code accessing process.env.PRIVATE_KEY never executes', async (): Promise<void> => {
  const prev = process.env['PRIVATE_KEY'];
  process.env['PRIVATE_KEY'] = 'super-secret-value';
  delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];

  try {
    const task = mkTask({
      code: `
        export default {
          onValidate() {
            return [process.env.PRIVATE_KEY ?? 'not-leaked'];
          }
        };
      `,
      fn: 'onValidate',
      args: {},
    });

    const res = await hookWorker(task);

    // Worker returns early; code never ran so PRIVATE_KEY was never read
    expect(res.found).toBe(false);
    expect(res.value).toBeUndefined();
  } finally {
    if (prev === undefined) delete process.env['PRIVATE_KEY'];
    else process.env['PRIVATE_KEY'] = prev;
  }
});
