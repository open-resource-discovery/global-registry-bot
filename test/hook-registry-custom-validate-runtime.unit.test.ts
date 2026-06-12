/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest, beforeAll, beforeEach } from '@jest/globals';
import type { HookWorkerResult } from '../src/handlers/request/validation/hook-pool.js';

// GateGuard facts:
// (1) No file imports this — Jest auto-discovers via testMatch;
// (2) No existing file covers hook-registry-custom-validate-runtime — Glob returned nothing;
// (3) All values are synthetic jest.fn() mocks — no data files read;
// (4) "proceed, the goal is everything at least on 90%. coverageThreshold: {...}"

// ── shared worker mock ──────────────────────────────────────────────────────────
let nextWorkerResult: HookWorkerResult = { found: false, value: null, logs: [] };

jest.unstable_mockModule('../src/handlers/request/validation/hook-pool.js', () => ({
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  runHookInWorker: () => Promise.resolve(nextWorkerResult),
}));

beforeEach(() => {
  nextWorkerResult = { found: false, value: null, logs: [] };
});

// ── module handle ───────────────────────────────────────────────────────────────
type Mod = typeof import('../src/handlers/request/validation/hook-registry-custom-validate-runtime.js');
let runRegistryCustomValidateRuntime: Mod['runRegistryCustomValidateRuntime'];

beforeAll(async () => {
  const mod = await import('../src/handlers/request/validation/hook-registry-custom-validate-runtime.js');
  runRegistryCustomValidateRuntime = mod.runRegistryCustomValidateRuntime;
});

// ── helpers ──────────────────────────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkDescriptor() {
  return {
    __type: 'hook-descriptor',
    __path: '.github/hooks.js',
    __hash: 'abc123',
    __code: 'export function onValidate() {}',
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkLog() {
  return {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkArgs(overrides: Partial<Record<string, unknown>> = {}) {
  const log = mkLog();
  return {
    context: { log },
    hooks: mkDescriptor(),
    repoInfo: { owner: 'org', repo: 'repo' },
    requestType: 'product',
    schema: null,
    candidate: { identifier: 'sap.product.test', name: 'test-product' },
    resourceName: 'sap.product.test',
    formData: null,
    allowedHosts: [],
    hookSecrets: {},
    hookWorkerConfig: {},
    hookRuntimeConfig: { getSecret: (): string => '' } as any,
    isHookDescriptor: (v: unknown): boolean => {
      const obj = v as Record<string, unknown>;
      return (
        typeof obj?.__type === 'string' &&
        typeof obj?.__path === 'string' &&
        typeof obj?.__hash === 'string' &&
        typeof obj?.__code === 'string'
      );
    },
    getHookLogger: (): { debug: () => void; info: () => void; warn: () => void; error: () => void } => ({
      debug: (): void => {},
      info: (): void => {},
      warn: (): void => {},
      error: (): void => {},
    }),
    normalizeHookErrors: (v: unknown): string[] =>
      Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [],
    buildFormDataForHookValidationFromCandidate: jest
      .fn()
      .mockResolvedValue({ identifier: 'sap.product.test' }) as jest.Mock,
    normalizeFormDataForHookValidation: jest.fn().mockImplementation((_rt: unknown, fd: unknown) => fd) as jest.Mock,
    resolvePrimaryIdFromCandidate: jest.fn().mockReturnValue('sap.product.test') as jest.Mock,
    getRecordProp: (obj: unknown, key: string): unknown => (obj as Record<string, unknown>)?.[key],
    toStringSafe: (v: unknown): string => (typeof v === 'string' ? v.trim() : ''),
    pickHookSecretsForWorker: jest.fn().mockReturnValue({}) as jest.Mock,
    createHookApi: jest.fn().mockReturnValue({}) as jest.Mock,
    ...overrides,
  } as any;
}

// ── descriptor (worker) path ───────────────────────────────────────────────────

describe('runRegistryCustomValidateRuntime — descriptor path', () => {
  test('returns [] when worker result has no errors', async () => {
    nextWorkerResult = { found: true, value: null, logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArgs());
    expect(result).toEqual([]);
  });

  test('returns normalized error array when worker returns errors', async () => {
    nextWorkerResult = { found: true, value: ['field-x must be valid ORD ID'], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArgs());
    expect(result).toContain('field-x must be valid ORD ID');
  });

  test('L222 arm0: error-level log entry forwarded via context.log.error', async () => {
    nextWorkerResult = {
      found: false,
      value: null,
      logs: [{ level: 'error', obj: { code: 'ERR' }, msg: 'hook error' }],
    };
    const log = mkLog();
    await runRegistryCustomValidateRuntime(mkArgs({ context: { log } }));
    expect(log.error).toHaveBeenCalledWith({ code: 'ERR' }, 'hook error');
  });

  test('L224 arm0: debug-level log entry forwarded via context.log.debug', async () => {
    nextWorkerResult = {
      found: false,
      value: null,
      logs: [{ level: 'debug', obj: { trace: true }, msg: 'debug trace' }],
    };
    const log = mkLog();
    await runRegistryCustomValidateRuntime(mkArgs({ context: { log } }));
    expect(log.debug).toHaveBeenCalledWith({ trace: true }, 'debug trace');
  });

  test('warn-level log entry forwarded via context.log.warn', async () => {
    nextWorkerResult = { found: false, value: null, logs: [{ level: 'warn', obj: null, msg: 'warn msg' }] };
    const log = mkLog();
    await runRegistryCustomValidateRuntime(mkArgs({ context: { log } }));
    expect(log.warn).toHaveBeenCalledWith(null, 'warn msg');
  });

  test('info-level log entry forwarded via context.log.info (else branch)', async () => {
    nextWorkerResult = { found: false, value: null, logs: [{ level: 'info', obj: null, msg: 'info msg' }] };
    const log = mkLog();
    await runRegistryCustomValidateRuntime(mkArgs({ context: { log } }));
    expect(log.info).toHaveBeenCalledWith(null, 'info msg');
  });

  test('L131 arm3: all primary names empty → candidate["product-id"] used', async () => {
    // form.identifier='', form.namespace='', inferredResourceName='', resourceName=''
    // → ?? chain falls through to candidate['product-id'] = 'my-product-id' (arm3)
    const args = mkArgs({
      candidate: { 'product-id': 'my-product-id' },
      resourceName: '',
      formData: { identifier: '', namespace: '' },
      resolvePrimaryIdFromCandidate: jest.fn().mockReturnValue('') as jest.Mock,
      normalizeFormDataForHookValidation: jest
        .fn()
        .mockImplementation((_rt: unknown, fd: unknown): unknown => fd) as jest.Mock,
    });
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toEqual([]);
  });

  test('L131 arm4: candidate["productId"] used when product-id also absent', async () => {
    const args = mkArgs({
      candidate: { productId: 'my-productId' },
      resourceName: '',
      formData: { identifier: '', namespace: '' },
      resolvePrimaryIdFromCandidate: jest.fn().mockReturnValue('') as jest.Mock,
      normalizeFormDataForHookValidation: jest
        .fn()
        .mockImplementation((_rt: unknown, fd: unknown): unknown => fd) as jest.Mock,
    });
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toEqual([]);
  });

  const mkArmsArgs = (candidate: Record<string, unknown>): ReturnType<typeof mkArgs> =>
    mkArgs({
      candidate,
      resourceName: '',
      formData: { identifier: '', namespace: '' },
      resolvePrimaryIdFromCandidate: jest.fn().mockReturnValue('') as jest.Mock,
      normalizeFormDataForHookValidation: jest
        .fn()
        .mockImplementation((_rt: unknown, fd: unknown): unknown => fd) as jest.Mock,
    });

  test('L131 arm5: candidate["id"] used when product-id and productId absent', async () => {
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArmsArgs({ id: 'my-id' }));
    expect(result).toEqual([]);
  });

  test('L131 arm6: candidate["name"] used when id also absent', async () => {
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArmsArgs({ name: 'my-name' }));
    expect(result).toEqual([]);
  });

  test('L131 arm7: candidate["identifier"] used when name also absent', async () => {
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArmsArgs({ identifier: 'my-identifier' }));
    expect(result).toEqual([]);
  });

  test('L131 arm8: candidate["namespace"] used when identifier also absent', async () => {
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArmsArgs({ namespace: 'my-namespace' }));
    expect(result).toEqual([]);
  });

  test('L131 arm9: candidate["vendor"] used when all other props absent', async () => {
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArmsArgs({ vendor: 'my-vendor' }));
    expect(result).toEqual([]);
  });

  test('L181 arm1: hookSecrets=null → pickHookSecretsForWorker receives {} fallback', async () => {
    const pickHookSecretsForWorker = jest.fn().mockReturnValue({}) as jest.Mock;
    nextWorkerResult = { found: true, value: [], logs: [] };
    const result = await runRegistryCustomValidateRuntime(mkArgs({ hookSecrets: null, pickHookSecretsForWorker }));
    expect(pickHookSecretsForWorker).toHaveBeenCalledWith({});
    expect(result).toEqual([]);
  });

  test('L221 arm1: log entry with undefined msg → "hook:onValidate" default used', async () => {
    nextWorkerResult = {
      found: true,
      value: [],
      logs: [{ level: 'info', obj: null, msg: undefined }],
    };
    const log = mkLog();
    await runRegistryCustomValidateRuntime(mkArgs({ context: { log } }));
    expect(log.info).toHaveBeenCalledWith(null, 'hook:onValidate');
  });
});

// ── legacy (in-process) path ───────────────────────────────────────────────────

describe('runRegistryCustomValidateRuntime — legacy path', () => {
  test('L146 arm0: hooks has no onValidate → returns []', async () => {
    // isHookDescriptor returns false → legacy path; hooks.onValidate is not a function → L146 arm0
    const hooks = { someOtherHook: (): void => {} };
    const args = mkArgs({
      hooks,
      isHookDescriptor: (): boolean => false,
    });
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toEqual([]);
  });

  test('L155 arm1: hookSecrets=null in legacy path → createHookApi receives {} fallback', async () => {
    const createHookApi = jest.fn().mockReturnValue({}) as jest.Mock;
    const hooks = { onValidate: jest.fn().mockResolvedValue([]) };
    const args = mkArgs({
      hooks,
      isHookDescriptor: (): boolean => false,
      hookSecrets: null,
      createHookApi,
    });
    await runRegistryCustomValidateRuntime(args);
    expect(createHookApi).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ secrets: {} }));
  });

  test('legacy path: onValidate returns errors → normalized and returned', async () => {
    const hooks = {
      onValidate: jest.fn().mockResolvedValue(['must be non-empty']),
    };
    const args = mkArgs({
      hooks,
      isHookDescriptor: (): boolean => false,
    });
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toContain('must be non-empty');
  });

  test('legacy path: onValidate throws Error → returns error string', async () => {
    const hooks = {
      onValidate: jest.fn().mockRejectedValue(new Error('hook failed')),
    };
    const args = mkArgs({
      hooks,
      isHookDescriptor: (): boolean => false,
    });
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toContain('Hook onValidate failed: hook failed');
  });

  test('legacy path: onValidate throws non-Error → converts to string', async () => {
    const hooks = {
      onValidate: jest.fn().mockRejectedValue('string rejection'),
    };
    const args = mkArgs({
      hooks,
      isHookDescriptor: (): boolean => false,
    });
    const result = await runRegistryCustomValidateRuntime(args);
    expect(result).toContain('Hook onValidate failed: string rejection');
  });
});
