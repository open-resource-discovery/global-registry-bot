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
  hookWorker = mod.default as unknown as (task: Task) => Promise<HookWorkerResult>;
});

beforeEach((): void => {
  originalFetch = getGlobalFetch();
  originalExit = process.exit;
  originalKill = process.kill;
});

afterEach((): void => {
  setGlobalFetch(originalFetch);
  process.exit = originalExit;
  process.kill = originalKill;
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
        await fetch('https://evil.com/');
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
