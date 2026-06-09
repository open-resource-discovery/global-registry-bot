/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest, beforeAll, beforeEach } from '@jest/globals';
import type { HookWorkerResult } from '../src/handlers/request/validation/hook-pool.js';

// ── shared worker mock ────────────────────────────────────────────────────────

let nextWorkerResults: HookWorkerResult[] = [];
let workerCallIndex = 0;

jest.unstable_mockModule('../src/handlers/request/validation/hook-pool.js', () => ({
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  runHookInWorker: () => {
    const res = nextWorkerResults[workerCallIndex] ?? { found: false, value: null, logs: [] };
    workerCallIndex++;
    return Promise.resolve(res);
  },
}));

beforeEach(() => {
  nextWorkerResults = [{ found: false, value: null, logs: [] }];
  workerCallIndex = 0;
});

// ── module handles ────────────────────────────────────────────────────────────

type ApprovalMod = typeof import('../src/handlers/request/validation/hook-approval-runtime.js');
let runApprovalHookRuntime: ApprovalMod['runApprovalHookRuntime'];

type ValidationMod = typeof import('../src/handlers/request/validation/hook-validation-runtime.js');
let runValidationHookRuntime: ValidationMod['runValidationHookRuntime'];

beforeAll(async () => {
  const approvalMod = await import('../src/handlers/request/validation/hook-approval-runtime.js');
  runApprovalHookRuntime = approvalMod.runApprovalHookRuntime;

  const validationMod = await import('../src/handlers/request/validation/hook-validation-runtime.js');
  runValidationHookRuntime = validationMod.runValidationHookRuntime;
});

// ── helpers ───────────────────────────────────────────────────────────────────

const repoInfo = { owner: 'org', repo: 'repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkLog() {
  return {
    debug: jest.fn() as jest.MockedFunction<(obj: unknown, msg?: string) => void>,
    info: jest.fn() as jest.MockedFunction<(obj: unknown, msg?: string) => void>,
    warn: jest.fn() as jest.MockedFunction<(obj: unknown, msg?: string) => void>,
    error: jest.fn() as jest.MockedFunction<(obj: unknown, msg?: string) => void>,
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkDescriptorHooks() {
  return {
    __type: 'hook-descriptor',
    __path: '.github/hooks.js',
    __hash: 'abc123',
    __code: 'export function onApproval() {}',
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkApprovalCtx(overrides: Record<string, unknown> = {}) {
  return {
    octokit: {},
    log: mkLog(),
    repo: (): { owner: string; repo: string } => ({ owner: 'org', repo: 'repo' }),
    issue: (): { owner: string; repo: string; issue_number: number } => ({
      owner: 'org',
      repo: 'repo',
      issue_number: 1,
    }),
    resourceBotConfig: {},
    resourceBotHooks: mkDescriptorHooks(),
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkApprovalArgs(overrides: Record<string, unknown> = {}) {
  return {
    requestType: 'product',
    namespace: 'sap.com:product',
    resourceName: 'MyProduct',
    formData: { identifier: 'MyProduct', namespace: 'sap.com' },
    issue: {
      number: 1,
      title: 'Request',
      body: '',
      state: 'open',
      labels: [],
      user: { login: 'user1' },
    },
    ...overrides,
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkValidationArgs(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    hooks: mkDescriptorHooks(),
    owner: 'org',
    repo: 'repo',
    requestType: 'product',
    rawIdOrNs: 'my-resource',
    candidate: { name: 'my-resource' },
    normalizedFormData: {},
    customValidateContextArgs: {
      requestAuthor: { id: 'user1', email: '' },
      issue: { number: 1, title: 'T', body: '', state: 'open', author: 'user1', labels: [] },
    },
    hookApi: null,
    hookWorkerConfig: {},
    hookRuntimeConfig: { getSecret: (_k: string): string => '' },
    allowedHosts: [],
    workerSecrets: {},
    log: mkLog(),
    isHookDescriptor: (v: unknown): boolean => {
      const obj = v as Record<string, unknown>;
      return (
        typeof obj?.__type === 'string' &&
        typeof obj?.__path === 'string' &&
        typeof obj?.__hash === 'string' &&
        typeof obj?.__code === 'string'
      );
    },
    getStringProp: (obj: unknown, key: string): string | undefined => {
      const val = (obj as Record<string, unknown>)?.[key];
      return typeof val === 'string' ? val : undefined;
    },
    normalizeHookErrors: (value: unknown): string[] => {
      if (!Array.isArray(value)) return [];
      return value.filter((v): v is string => typeof v === 'string');
    },
    getHookLogger: (): {
      debug: () => void;
      info: () => void;
      warn: () => void;
      error: () => void;
    } => ({ debug: (): void => {}, info: (): void => {}, warn: (): void => {}, error: (): void => {} }),
    rulesBucket: [] as string[],
    errors: [] as string[],
    ...overrides,
  } as any;
}

// ── hook-approval-runtime: descriptor path ───────────────────────────────────

describe('hook-approval-runtime — descriptor path', () => {
  test('logs warn and returns {} when hookErr is present — covers lines 462-463', async () => {
    nextWorkerResults = [{ found: true, value: { __hookError: 'hook failed' }, logs: [] }];
    const ctx = mkApprovalCtx();
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.warn).toHaveBeenCalledWith({ err: 'hook failed' }, 'resource-bot hooks.onApproval failed');
    expect(result).toEqual({});
  });

  test('normalizes errors array and deduplicates — covers lines 190-210 and line 201', async () => {
    nextWorkerResults = [
      {
        found: true,
        value: {
          status: 'rejected',
          errors: [
            { field: 'name', message: 'required' },
            { field: 'name', message: 'required' }, // duplicate → deduped at line 201
            'not-an-object', // skipped at line 194
          ],
        },
        logs: [],
      },
    ];
    const ctx = mkApprovalCtx();
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('rejected');
    expect(result.errors).toHaveLength(1);
    expect((result.errors as any)[0]).toEqual({ field: 'name', message: 'required' });
  });

  test('returns {} when hook result has unrecognized status — covers line 321', async () => {
    nextWorkerResults = [{ found: true, value: { status: 'pending' }, logs: [] }];
    const ctx = mkApprovalCtx();
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('returns {} when worker result is not found', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [] }];
    const ctx = mkApprovalCtx();
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });
});

// ── hook-approval-runtime: in-process path ───────────────────────────────────

describe('hook-approval-runtime — in-process path', () => {
  test('passes object label names to hook — covers line 176', async () => {
    let capturedLabels: string[] | undefined;
    const hooks = {
      onApproval: (hookArgs: any): Promise<{ status: string }> => {
        capturedLabels = hookArgs.issue.labels as string[];
        return Promise.resolve({ status: 'approved' });
      },
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const args = mkApprovalArgs({
      issue: {
        number: 1,
        title: 'Test',
        body: '',
        state: 'open',
        labels: [{ name: 'org-label' }, 'string-label'],
        user: null,
      },
    });
    await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(capturedLabels).toContain('org-label');
    expect(capturedLabels).toContain('string-label');
  });

  test('returns {} when hooks has no onApproval function', async () => {
    const ctx = mkApprovalCtx({ resourceBotHooks: { someOtherHook: (): void => {} } });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('logs warn and returns {} when onApproval throws an Error', async () => {
    const hooks = {
      onApproval: (): Promise<never> => Promise.reject(new Error('in-process hook failed')),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.warn).toHaveBeenCalledWith(
      { err: 'in-process hook failed' },
      'resource-bot hooks.onApproval failed'
    );
    expect(result).toEqual({});
  });

  test('logs warn and returns {} when onApproval throws a non-Error', async () => {
    const hooks = {
      onApproval: (): Promise<never> => Promise.reject('string rejection'),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.warn).toHaveBeenCalledWith({ err: 'string rejection' }, 'resource-bot hooks.onApproval failed');
    expect(result).toEqual({});
  });

  test('returns {} when resourceBotHooks is null', async () => {
    const ctx = mkApprovalCtx({ resourceBotHooks: null });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });
});

// ── hook-validation-runtime: worker path log forwarding ───────────────────────

describe('hook-validation-runtime — worker path log forwarding', () => {
  test('forwards debug-level log entries — covers line 145', async () => {
    const log = mkLog();
    nextWorkerResults = [{ found: true, value: null, logs: [{ level: 'debug', msg: 'trace', obj: { k: 1 } }] }];
    await runValidationHookRuntime(mkValidationArgs({ log }));
    expect(log.debug).toHaveBeenCalledWith({ k: 1 }, 'trace');
  });

  test('forwards info-level log entries (else branch) — covers line 146', async () => {
    const log = mkLog();
    nextWorkerResults = [{ found: true, value: null, logs: [{ level: 'info', msg: 'information', obj: null }] }];
    await runValidationHookRuntime(mkValidationArgs({ log }));
    expect(log.info).toHaveBeenCalledWith(null, 'information');
  });

  test('continues to customValidate when onValidate has hookErr but not found — covers line 155', async () => {
    const log = mkLog();
    const rulesBucket: string[] = [];
    const errors: string[] = [];
    nextWorkerResults = [
      { found: false, value: { __hookError: 'not found error' }, logs: [] }, // onValidate: hookErr, not found
      { found: true, value: ['custom-error'], logs: [] }, // customValidate: found, returns errors
    ];
    await runValidationHookRuntime(mkValidationArgs({ log, rulesBucket, errors }));
    expect(log.warn).toHaveBeenCalledWith(
      { err: 'not found error', fn: 'onValidate' },
      'resource-bot hook validation failed'
    );
    expect(errors).toContain('custom-error');
  });

  test('breaks out of loop when onValidate has hookErr and is found', async () => {
    const log = mkLog();
    const errors: string[] = [];
    nextWorkerResults = [
      { found: true, value: { __hookError: 'found but errored' }, logs: [] }, // onValidate: hookErr, found → break
    ];
    await runValidationHookRuntime(mkValidationArgs({ log, errors }));
    expect(log.warn).toHaveBeenCalledWith(
      { err: 'found but errored', fn: 'onValidate' },
      'resource-bot hook validation failed'
    );
    // customValidate was NOT called because we broke out
    expect(workerCallIndex).toBe(1);
    expect(errors).toHaveLength(0);
  });
});

// ── hook-validation-runtime: legacy path ────────────────────────────────────

describe('hook-validation-runtime — legacy path', () => {
  test('logs warn when validateHook throws an Error — covers line 201', async () => {
    const log = mkLog();
    const hooks = {
      onValidate: (): Promise<never> => Promise.reject(new Error('legacy hook failed')),
    };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        log,
      })
    );
    expect(log.warn).toHaveBeenCalledWith({ err: 'legacy hook failed' }, 'resource-bot hooks custom validation failed');
  });

  test('logs warn when validateHook throws a non-Error', async () => {
    const log = mkLog();
    const hooks = {
      onValidate: (): Promise<never> => Promise.reject('string-error'),
    };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        log,
      })
    );
    expect(log.warn).toHaveBeenCalledWith({ err: 'string-error' }, 'resource-bot hooks custom validation failed');
  });

  test('uses customValidate fallback when onValidate is not a function', async () => {
    const log = mkLog();
    const errors: string[] = [];
    const rulesBucket: string[] = [];
    const hooks = {
      customValidate: (): Promise<string[]> => Promise.resolve(['custom-validation-error']),
    };
    const normalizeHookErrors = (value: unknown): string[] => {
      if (!Array.isArray(value)) return [];
      return value.filter((v): v is string => typeof v === 'string');
    };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        log,
        errors,
        rulesBucket,
        normalizeHookErrors,
      })
    );
    expect(errors).toContain('custom-validation-error');
  });

  test('returns immediately when neither onValidate nor customValidate is a function', async () => {
    const log = mkLog();
    const hooks = { someOtherHook: (): void => {} };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        log,
      })
    );
    expect(log.warn).not.toHaveBeenCalled();
  });
});
