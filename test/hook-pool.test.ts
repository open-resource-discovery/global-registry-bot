import { jest } from '@jest/globals';
import type { HookWorkerResult, HookWorkerTask } from '../src/handlers/request/validation/hook-pool.js';

type RunOpts = { abortSignal?: AbortSignal; signal?: AbortSignal };
type RunImpl = (task: unknown, opts: RunOpts) => Promise<unknown>;

function makeAbortError(message: string): Error {
  const e = new Error(message);
  (e as { name?: string }).name = 'AbortError';
  return e;
}

// hook-pool.ts caches the pool internally.
// keep ONE mocked Piscina instance and switch behavior via runImpl per test.
let runImpl: RunImpl = (_task: unknown, _opts: RunOpts): Promise<unknown> =>
  Promise.resolve({ found: true, value: 'ok', logs: [] });

const runMock = jest.fn((task: unknown, opts: unknown): Promise<unknown> => runImpl(task, opts as RunOpts));

const piscinaCtor = jest.fn((_opts: unknown): { run: typeof runMock } => ({
  run: runMock,
}));

jest.unstable_mockModule('piscina', (): { Piscina: typeof piscinaCtor } => ({
  Piscina: piscinaCtor,
}));

let runHookInWorker: (task: HookWorkerTask, opts: { timeoutMs: number }) => Promise<HookWorkerResult>;

function mkTask(): HookWorkerTask {
  return {
    owner: 'o',
    repo: 'r',
    path: '.github/registry-bot/config.js',
    hash: 'deadbeefdeadbeef',
    code: 'export default { onValidate() { return []; } }',
    fn: 'onValidate',
    args: { requestType: 'product' },
    allowedHosts: ['api.sap.com'],
    secrets: { DUMMY: '1' },
  };
}

beforeAll(async (): Promise<void> => {
  const mod = await import('../src/handlers/request/validation/hook-pool.js');
  runHookInWorker = mod.runHookInWorker;
});

beforeEach((): void => {
  runMock.mockClear();
  runImpl = (_task: unknown, _opts: RunOpts): Promise<unknown> =>
    Promise.resolve({ found: true, value: 123, logs: [] });
});

test('creates a capped singleton pool and forwards abort signals', async (): Promise<void> => {
  const t = mkTask();

  const r1 = await runHookInWorker(t, { timeoutMs: 10 });
  const r2 = await runHookInWorker(t, { timeoutMs: 10 });

  expect(r1).toEqual({ found: true, value: 123, logs: [] });
  expect(r2).toEqual({ found: true, value: 123, logs: [] });

  // pool is cached -> constructor only once
  expect(piscinaCtor).toHaveBeenCalledTimes(1);

  const piscinaOpts = piscinaCtor.mock.calls[0]?.[0] as Record<string, unknown>;
  expect(String(piscinaOpts.filename)).toContain('hook-worker.js');

  expect(piscinaOpts.minThreads).toBe(1);
  expect(piscinaOpts.maxThreads).toBe(1);
  expect(piscinaOpts.idleTimeout).toBe(30_000);
  expect(piscinaOpts.maxQueue).toBe(25);
  expect(piscinaOpts.concurrentTasksPerWorker).toBe(1);

  const rl = piscinaOpts.resourceLimits as Record<string, unknown>;
  expect(rl.maxOldGenerationSizeMb).toBe(64);

  // run was called with both abortSignal + signal and they must be identical
  expect(runMock).toHaveBeenCalledTimes(2);

  const runOpts = runMock.mock.calls[0]?.[1] as Record<string, unknown>;
  const abortSignal = runOpts.abortSignal as AbortSignal;
  const signal = runOpts.signal as AbortSignal;

  expect(abortSignal).toBe(signal);
  expect(typeof abortSignal.aborted).toBe('boolean');
});

test('returns default result when piscina.run resolves undefined', async (): Promise<void> => {
  runImpl = (_task: unknown, _opts: RunOpts): Promise<unknown> => Promise.resolve(undefined);

  const res = await runHookInWorker(mkTask(), { timeoutMs: 10 });
  expect(res).toEqual({ found: false, value: undefined, logs: [] });
});

test('converts AbortError to a __hookError payload (timeout)', async (): Promise<void> => {
  jest.useFakeTimers();
  try {
    runImpl = (_task: unknown, opts: RunOpts): Promise<unknown> => {
      const sig = opts.abortSignal ?? opts.signal;
      return new Promise((_resolve, reject) => {
        if (!sig) {
          reject(new Error('missing AbortSignal'));
          return;
        }
        sig.addEventListener('abort', () => reject(makeAbortError('aborted')), { once: true });
      });
    };

    const p = runHookInWorker(mkTask(), { timeoutMs: 0 });
    await jest.advanceTimersByTimeAsync(1);

    const res = await p;
    expect(res).toEqual({
      found: true,
      value: { __hookError: 'Hook timed out after 1ms' },
      logs: [],
    });
  } finally {
    jest.useRealTimers();
  }
});

test('converts generic errors to a __hookError payload', async (): Promise<void> => {
  runImpl = (_task: unknown, _opts: RunOpts): Promise<unknown> => Promise.reject(new Error('boom'));

  const res = await runHookInWorker(mkTask(), { timeoutMs: 10 });
  expect(res).toEqual({
    found: true,
    value: { __hookError: 'boom' },
    logs: [],
  });
});
