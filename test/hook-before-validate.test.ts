/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest, beforeAll, beforeEach } from '@jest/globals';
import type { HookWorkerResult } from '../src/handlers/request/validation/hook-pool.js';

type RunBeforeValidate =
  typeof import('../src/handlers/request/validation/hook-before-validate-runtime.js').runBeforeValidateHookRuntime;

let runBeforeValidateHookRuntime: RunBeforeValidate;

// Control what runHookInWorker returns via this mutable variable
let nextWorkerResult: HookWorkerResult = { found: true, value: null, logs: [] };
const workerCallArgs: unknown[] = [];

jest.unstable_mockModule('../src/handlers/request/validation/hook-pool.js', () => ({
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  runHookInWorker: (...args: unknown[]) => {
    workerCallArgs.splice(0, workerCallArgs.length, ...args);
    return Promise.resolve(nextWorkerResult);
  },
}));

beforeAll(async () => {
  const mod = await import('../src/handlers/request/validation/hook-before-validate-runtime.js');
  runBeforeValidateHookRuntime = mod.runBeforeValidateHookRuntime;
});

beforeEach(() => {
  nextWorkerResult = { found: true, value: null, logs: [] };
  workerCallArgs.length = 0;
});

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkLog() {
  return {
    debug: jest.fn() as jest.MockedFunction<(...args: any[]) => void>,
    info: jest.fn() as jest.MockedFunction<(...args: any[]) => void>,
    warn: jest.fn() as jest.MockedFunction<(...args: any[]) => void>,
    error: jest.fn() as jest.MockedFunction<(...args: any[]) => void>,
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkBaseArgs(overrides: Record<string, unknown> = {}) {
  return {
    hooks: {
      __type: 'hook-descriptor',
      __path: '.github/registry-bot/hooks.js',
      __hash: 'abc123',
      __code: 'export function beforeValidate() {}',
    },
    owner: 'org',
    repo: 'repo',
    requestType: 'product',
    formData: { title: 'Test' },
    allowedHosts: ['api.sap.com'],
    workerSecrets: { SECRET: 'value' },
    hookWorkerConfig: { apiUrl: 'https://api.example.com' },
    hookRuntimeConfig: {
      apiUrl: 'https://api.example.com',
      getSecret: (_key: string) => 'secret-value',
    },
    hookApi: null,
    log: mkLog(),
    isHookDescriptor: (_v: unknown): _v is { __type: string; __path: string; __hash: string; __code: string } =>
      typeof (_v as Record<string, unknown>)?.__type === 'string',
    getHookLogger: (log?: { debug?: (...a: unknown[]) => void }) => ({
      debug: log?.debug ?? ((): void => {}),
      info: ((): void => {}) as (...a: unknown[]) => void,
      warn: ((): void => {}) as (...a: unknown[]) => void,
      error: ((): void => {}) as (...a: unknown[]) => void,
    }),
    getStringProp: (obj: unknown, key: string): string | undefined => {
      const val = (obj as Record<string, unknown>)?.[key];
      return typeof val === 'string' ? val : undefined;
    },
    getObjectProp: (obj: unknown, key: string): Record<string, unknown> | null => {
      const val = (obj as Record<string, unknown>)?.[key];
      return val !== null && typeof val === 'object' && !Array.isArray(val) ? (val as Record<string, unknown>) : null;
    },
    ...overrides,
  } as any;
}

// ---- Descriptor (worker) path -----------------------------------------------

describe('runBeforeValidateHookRuntime — descriptor (worker) path', () => {
  test('calls runHookInWorker with correct owner, repo and fn', async () => {
    await runBeforeValidateHookRuntime(mkBaseArgs());
    const task = workerCallArgs[0] as Record<string, unknown>;
    expect(task.fn).toBe('beforeValidate');
    expect(task.owner).toBe('org');
    expect(task.repo).toBe('repo');
  });

  test('forwards error-level log entries — covers lines 90-95', async () => {
    const log = mkLog();
    nextWorkerResult = {
      found: true,
      value: null,
      logs: [{ level: 'error', msg: 'something exploded', obj: { detail: 'x' } }],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ log }));
    expect(log.error).toHaveBeenCalledWith({ detail: 'x' }, 'something exploded');
  });

  test('forwards warn-level log entries', async () => {
    const log = mkLog();
    nextWorkerResult = {
      found: true,
      value: null,
      logs: [{ level: 'warn', msg: 'watch out', obj: {} }],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ log }));
    expect(log.warn).toHaveBeenCalledWith({}, 'watch out');
  });

  test('forwards debug-level log entries', async () => {
    const log = mkLog();
    nextWorkerResult = {
      found: true,
      value: null,
      logs: [{ level: 'debug', msg: 'trace', obj: { k: 1 } }],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ log }));
    expect(log.debug).toHaveBeenCalledWith({ k: 1 }, 'trace');
  });

  test('uses default message hook:beforeValidate when log entry msg is empty', async () => {
    // Covers line 91: msg = l.msg || 'hook:beforeValidate'
    const log = mkLog();
    nextWorkerResult = {
      found: true,
      value: null,
      logs: [{ level: 'info', msg: '', obj: null }],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ log }));
    expect(log.info).toHaveBeenCalledWith(null, 'hook:beforeValidate');
  });

  test('logs warning when hookErr is present in result — covers lines 99-102', async () => {
    const log = mkLog();
    nextWorkerResult = {
      found: true,
      value: { __hookError: 'Validation script threw an error' },
      logs: [],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ log }));
    expect(log.warn).toHaveBeenCalledWith(
      { err: 'Validation script threw an error' },
      'resource-bot hooks.beforeValidate failed'
    );
  });

  test('mutates formData with string values from workerForm — covers lines 104-116', async () => {
    const formData: Record<string, string> = { title: 'old-title', extra: 'remove-me' };
    nextWorkerResult = {
      found: true,
      value: { form: { title: 'new-title', added: 'new-field' } },
      logs: [],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ formData }));
    expect(formData.title).toBe('new-title');
    expect(formData.added).toBe('new-field');
    expect(Object.keys(formData)).not.toContain('extra');
  });

  test('converts numeric formData values to strings — covers line 114', async () => {
    const formData: Record<string, string> = {};
    nextWorkerResult = {
      found: true,
      value: { form: { count: 42, flag: true } },
      logs: [],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ formData }));
    expect(formData['count']).toBe('42');
    expect(formData['flag']).toBe('true');
  });

  test('skips null/undefined fields in workerForm — covers line 111', async () => {
    const formData: Record<string, string> = {};
    nextWorkerResult = {
      found: true,
      value: { form: { present: 'yes', missing: null, absent: undefined } },
      logs: [],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ formData }));
    expect(formData['present']).toBe('yes');
    expect(Object.keys(formData)).not.toContain('missing');
    expect(Object.keys(formData)).not.toContain('absent');
  });

  test('converts object/array form values via String() — covers line 115', async () => {
    // Hits the else branch: not string, not number/boolean → String(v)
    const formData: Record<string, string> = {};
    nextWorkerResult = {
      found: true,
      value: { form: { data: ['a', 'b'] } },
      logs: [],
    };
    await runBeforeValidateHookRuntime(mkBaseArgs({ formData }));
    // String(['a', 'b']) === 'a,b'
    expect(formData['data']).toBe('a,b');
  });
});

// ---- Direct hooks object path -----------------------------------------------

describe('runBeforeValidateHookRuntime — direct hooks path', () => {
  test('returns immediately when hooks has no beforeValidate function — covers line 122', async () => {
    const args = mkBaseArgs({
      hooks: { someOtherHook: () => {} },
      isHookDescriptor: () => false,
    });
    await runBeforeValidateHookRuntime(args);
    expect(workerCallArgs.length).toBe(0);
  });

  test('calls beforeValidate with runtime args when hooks object provided — covers lines 124-131', async () => {
    const beforeValidate = jest.fn((): Promise<void> => Promise.resolve());
    const formData: Record<string, string> = { title: 'Test' };
    const args = mkBaseArgs({
      hooks: { beforeValidate },
      isHookDescriptor: () => false,
      formData,
    });
    await runBeforeValidateHookRuntime(args);
    expect(beforeValidate).toHaveBeenCalledTimes(1);
    const callArg = (beforeValidate as any).mock.calls[0][0] as Record<string, unknown>;
    expect(callArg.requestType).toBe('product');
    expect(callArg.form).toBe(formData);
    expect(workerCallArgs.length).toBe(0);
  });

  test('logs warning when beforeValidate throws an Error — covers lines 132-136', async () => {
    const log = mkLog();
    const beforeValidate = jest.fn((): Promise<never> => Promise.reject(new Error('hook exploded')));
    const args = mkBaseArgs({
      hooks: { beforeValidate },
      isHookDescriptor: () => false,
      log,
    });
    await runBeforeValidateHookRuntime(args);
    expect(log.warn).toHaveBeenCalledWith({ err: 'hook exploded' }, 'resource-bot hooks.beforeValidate failed');
  });

  test('logs warning when beforeValidate throws a non-Error — String(err) path', async () => {
    const log = mkLog();
    const beforeValidate = jest.fn((): Promise<never> => Promise.reject('string error'));
    const args = mkBaseArgs({
      hooks: { beforeValidate },
      isHookDescriptor: () => false,
      log,
    });
    await runBeforeValidateHookRuntime(args);
    expect(log.warn).toHaveBeenCalledWith({ err: 'string error' }, 'resource-bot hooks.beforeValidate failed');
  });
});
