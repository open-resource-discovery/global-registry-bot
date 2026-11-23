import { type MockInstance, vi } from 'vitest';
import type { HookWorkerTask } from '../src/handlers/request/validation/hook-pool.js';
import hookWorker from '../src/handlers/request/validation/hook-worker.js';

let fetchMock: MockInstance<typeof fetch>;
let sequence = 0;

function mkTask(overrides: Partial<HookWorkerTask> & Pick<HookWorkerTask, 'code'>): HookWorkerTask {
  return {
    owner: 'o',
    repo: 'r',
    path: '.github/registry-bot/config.js',
    hash: String(++sequence).padStart(16, '0'),
    fn: 'onValidate',
    args: {},
    ...overrides,
  };
}

beforeEach(() => {
  vi.stubEnv('REGISTRY_BOT_ENABLE_JS_HOOKS', 'true');
  fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('ok'));
  vi.spyOn(process, 'exit').mockImplementation(() => {
    throw new Error('Unexpected process.exit');
  });
  vi.spyOn(process, 'kill').mockImplementation(() => {
    throw new Error('Unexpected process.kill');
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('returns found=false and restores fetch when the hook is missing', async () => {
  const result = await hookWorker(mkTask({ code: 'export default {};' }));
  expect(result).toEqual({ found: false, value: undefined, logs: [] });
  expect(globalThis.fetch).toBe(fetchMock);
});

test('calls a default-exported hook and captures its logs and result', async () => {
  const result = await hookWorker(
    mkTask({
      code: `export default { onValidate({ log }) { log.info({ x: 1 }, 'hello'); return ['err1']; } };`,
    })
  );
  expect(result).toEqual({
    found: true,
    value: ['err1'],
    logs: [{ level: 'info', obj: { x: 1 }, msg: 'hello' }],
  });
});

test('resolves a named hook when the default export does not contain it', async () => {
  const result = await hookWorker(
    mkTask({
      code: `export function onValidate({ log }) { log.warn({ ok: true }, 'named'); return []; } export default {};`,
    })
  );
  expect(result).toEqual({
    found: true,
    value: [],
    logs: [{ level: 'warn', obj: { ok: true }, msg: 'named' }],
  });
});

test('returns form changes made by beforeValidate', async () => {
  const result = await hookWorker(
    mkTask({
      code: `export function beforeValidate({ form, log }) { form.identifier = 'X'; log.debug({ id: form.identifier }, 'mutated'); }`,
      fn: 'beforeValidate',
      args: { form: { identifier: 'A' } },
    })
  );
  expect(result).toEqual({
    found: true,
    value: { form: { identifier: 'X' } },
    logs: [{ level: 'debug', obj: { id: 'X' }, msg: 'mutated' }],
  });
});

test('fetches an allowlisted host with redirects disabled', async () => {
  const url = 'https://api.sap.com/catalog';
  const result = await hookWorker(
    mkTask({
      code: `export async function onValidate() { await fetch('${url}'); return []; }`,
      allowedHosts: ['api.sap.com'],
    })
  );
  expect(result.value).toEqual([]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock).toHaveBeenCalledWith(url, { redirect: 'manual' });
  expect(globalThis.fetch).toBe(fetchMock);
});

test.each([
  ['omitted', undefined, 'https://blocked.example/x'],
  ['empty', [], 'https://blocked.example/x'],
  ['different host', ['allowed.example'], 'https://blocked.example/x'],
  ['local IP', ['127.0.0.1'], 'https://127.0.0.1/x'],
] as const)('blocks fetch with an %s allowlist', async (_name, allowedHosts, url) => {
  const result = await hookWorker(
    mkTask({
      code: `export async function onValidate() { await fetch('${url}'); return []; }`,
      ...(allowedHosts === undefined ? {} : { allowedHosts: [...allowedHosts] }),
    })
  );
  expect(result.value).toEqual({ __hookError: expect.stringContaining('Host not allowed') });
  expect(fetchMock).not.toHaveBeenCalled();
  expect(globalThis.fetch).toBe(fetchMock);
});

test('converts process.exit into a hook error', async () => {
  const result = await hookWorker(
    mkTask({
      code: 'export function onValidate() { process.exit(0); }',
    })
  );
  expect(result.value).toEqual({ __hookError: expect.stringContaining('process.exit is disabled') });
});

test.each([undefined, '', 'false', '1', 'TRUE'])(
  'does not evaluate hook code when deployment opt-in is %s',
  async (flag) => {
    if (flag === undefined) delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
    else process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = flag;
    const result = await hookWorker(
      mkTask({
        code: 'throw new Error("module must not be evaluated"); export function onValidate() { return []; }',
      })
    );
    expect(result).toEqual({ found: false, value: undefined, logs: [] });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(globalThis.fetch).toBe(fetchMock);
  }
);

test.each([false, true])('isolates consecutive task allowlists after failure=%s', async (fails) => {
  const task = (allowedHosts: string[] | undefined): HookWorkerTask =>
    mkTask({
      code: `export async function onValidate() { await fetch('https://allowed.example/x'); return []; }`,
      allowedHosts,
    });
  const first = await hookWorker(
    fails ? task(undefined) : mkTask({ code: 'export function onValidate() { return []; }' })
  );
  expect(first.value).toEqual(fails ? { __hookError: expect.stringContaining('Host not allowed') } : []);
  expect(globalThis.fetch).toBe(fetchMock);

  const second = await hookWorker(task(['allowed.example']));
  expect(second.value).toEqual([]);
  const third = await hookWorker(task([]));
  expect(third.value).toEqual({ __hookError: expect.stringContaining('Host not allowed') });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(globalThis.fetch).toBe(fetchMock);
});

test('resolves secret aliases with configuration values taking precedence', async () => {
  const result = await hookWorker(
    mkTask({
      code: `export function onValidate({ config }) {
      return [config.getSecret('TOKEN'), config.getSecret('HOOK_SECRET_TOKEN'), config.getSecret('OTHER'),
        config.getSecret('EMPTY'), Object.keys(config).includes('getSecret')];
    }`,
      secrets: { TOKEN: 'default', HOOK_SECRET_TOKEN: 'alias', HOOK_SECRET_OTHER: ' other ', EMPTY: ' ' },
      args: { config: { TOKEN: ' override ' } },
    })
  );
  expect(result.value).toEqual(['override', 'override', 'other', '', false]);
});
