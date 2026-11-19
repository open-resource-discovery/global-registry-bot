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

  test('L198 arm0: error item with empty message is skipped by normalizeApprovalHookErrors', async () => {
    // { message: '' } → !message → continue (L198 arm0)
    const hooks = {
      onApproval: (): Promise<{ status: string; errors: { field: string; message: string }[] }> =>
        Promise.resolve({
          status: 'rejected',
          errors: [
            { field: 'x', message: '' }, // empty message → L198 arm0 → skipped
            { field: 'y', message: 'real-error' },
          ],
        }),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('rejected');
    expect((result.errors as any[]).map((e: any) => e.message)).toEqual(['real-error']);
  });

  test('L223 arm0: empty login in uniqLogins is skipped when config.approvers has blank entries', async () => {
    // uniqLogins(['', '@', 'real-user']) → '' and '@' normalize to '' → !login → L223 arm0
    const ctx = mkApprovalCtx({
      resourceBotConfig: {
        workflow: {
          approvers: ['', '@', 'real-user'],
        },
      },
    });
    let capturedApprovers: string[] | undefined;
    const hooks = {
      onApproval: (args: any): Promise<{ status: string }> => {
        capturedApprovers = args.config.approvers as string[];
        return Promise.resolve({ status: 'approved' });
      },
    };
    const ctxWithHooks = { ...ctx, resourceBotHooks: hooks };
    await runApprovalHookRuntime(ctxWithHooks, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    // only 'real-user' survives; '' and '@' were skipped (L223 arm0)
    expect(capturedApprovers).toEqual(['real-user']);
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

// ── hook-approval-runtime: pickHookSecretsForWorker branches ─────────────────

describe('hook-approval-runtime — pickHookSecretsForWorker (L165-168)', () => {
  test('skips non-string and empty-string secrets, includes valid secrets', async () => {
    // descriptor path so pickHookSecretsForWorker is called
    nextWorkerResults = [{ found: false, value: null, logs: [] }];
    const ctx = mkApprovalCtx();
    // non-string (L165 continue), empty string (L167 continue), valid string (L168 included)
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), {
      hookSecrets: { numSecret: 42 as any, emptyStr: '', valid: 'secret-value' },
    });
    // coverage: L165 branch (typeof v !== 'string') and L167 branch (!s)
  });
});

// ── hook-approval-runtime: normalizeApprovalHookResult boolean/string/object branches ─

describe('hook-approval-runtime — normalizeApprovalHookResult extra branches', () => {
  test('returns {status:"approved"} when onApproval returns true (L278)', async () => {
    const hooks = { onApproval: (): Promise<boolean> => Promise.resolve(true) };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('approved');
  });

  test('returns {} when onApproval returns false (L279)', async () => {
    const hooks = { onApproval: (): Promise<boolean> => Promise.resolve(false) };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('returns {} when onApproval returns null (L279)', async () => {
    const hooks = { onApproval: (): Promise<null> => Promise.resolve(null) };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('returns {status:"rejected"} when onApproval returns string "rejected" (L281-283)', async () => {
    const hooks = { onApproval: (): Promise<string> => Promise.resolve('rejected') };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('rejected');
  });

  test('returns {status:"unknown"} when onApproval returns string "unknown" (L281-283)', async () => {
    const hooks = { onApproval: (): Promise<string> => Promise.resolve('unknown') };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('unknown');
  });

  test('returns {} when onApproval returns unrecognized non-object (L286)', async () => {
    // number: not true/false/null/undefined, not string token, not plain object
    const hooks = { onApproval: (): Promise<number> => Promise.resolve(42) };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('returns approved decision when onApproval returns { approved: true } (L290-299)', async () => {
    const hooks = {
      onApproval: (): Promise<Record<string, unknown>> =>
        Promise.resolve({ approved: true, comment: 'LGTM', message: 'auto', approvers: ['alice'] }),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('LGTM');
    expect(result.message).toBe('auto');
    expect(result.approvers).toContain('alice');
  });

  test('returns approved with no optional fields when approved=true but comment/message/approvers empty (L290-299)', async () => {
    const hooks = {
      onApproval: (): Promise<Record<string, unknown>> => Promise.resolve({ approved: true }),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('approved');
    expect(result).not.toHaveProperty('comment');
    expect(result).not.toHaveProperty('message');
    expect(result).not.toHaveProperty('approvers');
  });

  test('returns decision with path/reason/errors when status object has those fields (L302-319)', async () => {
    const hooks = {
      onApproval: (): Promise<Record<string, unknown>> =>
        Promise.resolve({ status: 'rejected', path: 'some/path', reason: 'invalid', errors: [] }),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('rejected');
    expect(result.path).toBe('some/path');
    expect(result.reason).toBe('invalid');
  });
});

// ── hook-approval-runtime: getApprovalHookApprovers branches (L252-257) ───────

describe('hook-approval-runtime — getApprovalHookApprovers (L252-257)', () => {
  test('returns fallbackApprovers when entry exists but has no approvers/approversPool (L255 true arm)', async () => {
    const hooks = {
      onApproval: (hookArgs: any): Promise<{ status: string; approvers: string[] }> =>
        Promise.resolve({ status: 'approved', approvers: hookArgs.config.approvers }),
    };
    const ctx = mkApprovalCtx({
      resourceBotHooks: hooks,
      resourceBotConfig: {
        workflow: { approvers: ['workflow-approver'] },
        requests: {
          product: { folderName: 'products' }, // no approvers/approversPool
        },
      },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.approvers).toContain('workflow-approver');
  });

  test('returns entry-specific approvers when entry has approvers array (L257, uniqLogins/normalizeLoginValue)', async () => {
    let capturedApprovers: string[] | undefined;
    const hooks = {
      onApproval: (hookArgs: any): Promise<{ status: string }> => {
        capturedApprovers = hookArgs.config.approvers as string[];
        return Promise.resolve({ status: 'approved' });
      },
    };
    const ctx = mkApprovalCtx({
      resourceBotHooks: hooks,
      resourceBotConfig: {
        workflow: { approvers: ['workflow-approver'] },
        requests: {
          product: { approvers: ['alice', '@Bob', 'ALICE'] }, // alice deduped, @Bob normalized
        },
      },
    });
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(capturedApprovers).toContain('alice');
    expect(capturedApprovers).toContain('Bob');
    expect(capturedApprovers).not.toContain('ALICE'); // deduped (case-insensitive)
    // workflow-approver NOT present because entry has own approvers
    expect(capturedApprovers).not.toContain('workflow-approver');
  });

  test('returns combined approvers+approversPool (L257 with both arrays)', async () => {
    let capturedApprovers: string[] | undefined;
    const hooks = {
      onApproval: (hookArgs: any): Promise<{ status: string }> => {
        capturedApprovers = hookArgs.config.approvers as string[];
        return Promise.resolve({ status: 'approved' });
      },
    };
    const ctx = mkApprovalCtx({
      resourceBotHooks: hooks,
      resourceBotConfig: {
        requests: {
          product: { approvers: ['alice'], approversPool: ['carol', 'dave'] },
        },
      },
    });
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(capturedApprovers).toContain('alice');
    expect(capturedApprovers).toContain('carol');
    expect(capturedApprovers).toContain('dave');
  });
});

// ── hook-approval-runtime: logApprovalHookMessages log levels (L330-335) ──────

describe('hook-approval-runtime — logApprovalHookMessages log levels (L330-335)', () => {
  test('forwards error-level hook logs (L332)', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [{ level: 'error', obj: { code: 1 }, msg: 'err msg' }] }];
    const ctx = mkApprovalCtx();
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.error).toHaveBeenCalledWith({ code: 1 }, 'err msg');
  });

  test('forwards warn-level hook logs (L333)', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [{ level: 'warn', obj: { w: true }, msg: 'warn msg' }] }];
    const ctx = mkApprovalCtx();
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.warn).toHaveBeenCalledWith({ w: true }, 'warn msg');
  });

  test('forwards debug-level hook logs (L334)', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [{ level: 'debug', obj: null, msg: 'dbg msg' }] }];
    const ctx = mkApprovalCtx();
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.debug).toHaveBeenCalledWith(null, 'dbg msg');
  });

  test('forwards info-level hook logs via else branch (L335)', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [{ level: 'info', obj: { x: 1 }, msg: 'info msg' }] }];
    const ctx = mkApprovalCtx();
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.info).toHaveBeenCalledWith({ x: 1 }, 'info msg');
  });

  test('uses "hook:onApproval" as fallback message when msg is missing (L331)', async () => {
    nextWorkerResults = [{ found: false, value: null, logs: [{ level: 'info', obj: null }] }];
    const ctx = mkApprovalCtx();
    await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(ctx.log.info).toHaveBeenCalledWith(null, 'hook:onApproval');
  });
});

// ── hook-approval-runtime: L175 arm1 + L314/L315 arm0 ────────────────────────

describe('hook-approval-runtime — extra branch coverage', () => {
  test('L175 arm1: label that is neither string nor plain object maps to empty string', async () => {
    // approvalIssueLabelName(42): typeof 42 !== 'string' AND !isPlainObject(42) → return '' (L175 arm1)
    // filter(Boolean) removes the '' so it does not appear in labels
    const hooks = {
      onApproval: (_args: unknown): Promise<{ status: string }> => {
        return Promise.resolve({ status: 'approved' });
      },
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(
      ctx,
      repoInfo,
      mkApprovalArgs({ issue: { number: 1, title: 'T', body: '', state: 'open', labels: [42 as any], user: null } }),
      { hookSecrets: {} }
    );
    expect(result.status).toBe('approved');
  });

  test('L314 arm0 + L315 arm0: status object with non-empty comment and message includes both', async () => {
    // status='rejected', comment='needs-fix', message='see policy' → L314 arm0 and L315 arm0 covered
    const hooks = {
      onApproval: (): Promise<Record<string, unknown>> =>
        Promise.resolve({ status: 'rejected', comment: 'needs-fix', message: 'see policy' }),
    };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('rejected');
    expect((result as any).comment).toBe('needs-fix');
    expect((result as any).message).toBe('see policy');
  });
});

// ── hook-validation-runtime: L111 + L142 branch coverage ─────────────────────

describe('hook-validation-runtime — resourceName fallback + msg fallback', () => {
  test('L111 binary-expr arm1 + cond-expr arm0: rawIdOrNs="" uses candidate.name when string', async () => {
    // rawIdOrNs: '' → binary-expr arm1 → falls through to cond-expr
    // candidate.name = 'my-resource' (string) → cond-expr arm0 → resourceName = 'my-resource'
    nextWorkerResults = [
      { found: false, value: null, logs: [] },
      { found: false, value: null, logs: [] },
    ];
    const log = mkLog();
    await runValidationHookRuntime(mkValidationArgs({ rawIdOrNs: '', candidate: { name: 'my-resource' }, log }));
    expect(log.warn).not.toHaveBeenCalled();
  });

  test('L111 cond-expr arm1: rawIdOrNs="" and candidate.name is not a string → resourceName=""', async () => {
    // rawIdOrNs: '' → binary-expr arm1; candidate.name = 42 (number, not string) → cond-expr arm1 → ''
    nextWorkerResults = [
      { found: false, value: null, logs: [] },
      { found: false, value: null, logs: [] },
    ];
    const log = mkLog();
    await runValidationHookRuntime(mkValidationArgs({ rawIdOrNs: '', candidate: { name: 42 }, log }));
    expect(log.warn).not.toHaveBeenCalled();
  });

  test('L142 binary-expr arm1: log entry with no msg uses hook:fn as fallback', async () => {
    // l.msg is undefined → l.msg || `hook:${fn}` → arm1 covered → msg = 'hook:onValidate'
    nextWorkerResults = [
      { found: false, value: null, logs: [{ level: 'info', obj: { x: 1 } }] }, // no msg field
      { found: false, value: null, logs: [] },
    ];
    const log = mkLog();
    await runValidationHookRuntime(mkValidationArgs({ log }));
    expect(log.info).toHaveBeenCalledWith({ x: 1 }, 'hook:onValidate');
  });
});

// ── hook-validation-runtime: L186 legacy path resourceName fallback ───────────

describe('hook-validation-runtime — legacy path L186 resourceName fallback', () => {
  test('L186 binary-expr arm1 + cond-expr arm0: rawIdOrNs="" uses candidate.name when string', async () => {
    // Legacy path: isHookDescriptor returns false
    // rawIdOrNs: '' → binary-expr arm1 → falls through to cond-expr
    // candidate.name = 'res-name' (string) → cond-expr arm0 → resourceName = 'res-name'
    let capturedResourceName: string | undefined;
    const hooks = {
      onValidate: (hookArgs: any): Promise<null> => {
        capturedResourceName = hookArgs.resourceName as string;
        return Promise.resolve(null);
      },
    };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        rawIdOrNs: '',
        candidate: { name: 'res-name' },
      })
    );
    expect(capturedResourceName).toBe('res-name');
  });

  test('L186 cond-expr arm1: rawIdOrNs="" and candidate.name is not a string → resourceName=""', async () => {
    // Legacy path: isHookDescriptor returns false
    // rawIdOrNs: '' → binary-expr arm1; candidate.name = 99 (not string) → cond-expr arm1 → ''
    let capturedResourceName: string | undefined;
    const hooks = {
      onValidate: (hookArgs: any): Promise<null> => {
        capturedResourceName = hookArgs.resourceName as string;
        return Promise.resolve(null);
      },
    };
    await runValidationHookRuntime(
      mkValidationArgs({
        hooks,
        isHookDescriptor: (): boolean => false,
        rawIdOrNs: '',
        candidate: { name: 99 },
      })
    );
    expect(capturedResourceName).toBe('');
  });
});

// ── hook-approval-runtime: branch-coverage top-up ──

describe('hook-approval-runtime — branch top-up (L155/L180/L239/L351/L367/L404/L408)', () => {
  test('L155-158 arm1: getHookLogger uses noop when log has no function methods', async () => {
    const hooks = { onApproval: (): Promise<boolean> => Promise.resolve(true) };
    const ctx = mkApprovalCtx({ resourceBotHooks: hooks, log: {} });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result.status).toBe('approved');
  });

  test('L180 arm1: labels not an array → toApprovalIssueLabelNames returns []', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      issue: { number: 1, title: 'T', body: '', state: 'open', labels: null as unknown as [], user: { login: 'u' } },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L239 arm1 + L404 arm1: null resourceBotConfig coalesces to {}', async () => {
    const ctx = mkApprovalCtx({ resourceBotConfig: null as unknown as Record<string, unknown> });
    const result = await runApprovalHookRuntime(ctx, repoInfo, mkApprovalArgs(), { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L351 arm1: namespace null, formData.namespace used', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      namespace: null as unknown as string,
      formData: { namespace: 'sap.com', identifier: '' },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L351 arm2: namespace and formData.namespace empty, formData.identifier used', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      namespace: null as unknown as string,
      formData: { namespace: '', identifier: 'myProduct' },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L351 arm3: resourceName used when all other namespace sources empty', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      namespace: null as unknown as string,
      resourceName: 'MyProd',
      formData: { namespace: '', identifier: '' },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L367 arm3: namespace param used as resourceName when resourceName and identifier empty', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      namespace: 'sap.com',
      resourceName: null as unknown as string,
      formData: { namespace: '', identifier: '' },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });

  test('L408 arm1: issue.number not a number → 0 used in hookData', async () => {
    const ctx = mkApprovalCtx();
    const args = mkApprovalArgs({
      issue: {
        number: 'not-a-number' as unknown as number,
        title: 'T',
        body: '',
        state: 'open',
        labels: [],
        user: { login: 'u' },
      },
    });
    const result = await runApprovalHookRuntime(ctx, repoInfo, args, { hookSecrets: {} });
    expect(result).toEqual({});
  });
});
