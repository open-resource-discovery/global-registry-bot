/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { describe, it, expect, jest } from '@jest/globals';

type MockedAsync<Args extends unknown[], Result> = jest.MockedFunction<(...args: Args) => Promise<Result>>;

const b64json = (obj: unknown): string => Buffer.from(JSON.stringify(obj), 'utf8').toString('base64');

function httpErr(status: number, msg = 'http'): Error & { status: number } {
  const e = new Error(msg) as Error & { status: number };
  e.status = status;
  return e;
}

function mkIssuesStub() {
  return {
    get: jest.fn(async () => ({ data: {} })),
    listForRepo: jest.fn(async () => ({ data: [] })),
    update: jest.fn(async () => ({})),
    create: jest.fn(async () => ({})),
    createComment: jest.fn(async () => ({})),
    addLabels: jest.fn(async () => ({})),
    removeLabel: jest.fn(async () => ({})),
  };
}

async function loadSubject(opts: { dbg?: '0' | '1'; mockYaml?: boolean } = {}) {
  const prevDbg = process.env.DEBUG_NS;
  process.env.DEBUG_NS = opts.dbg ?? '0';

  jest.resetModules();

  // ---- typed mocks ----
  const readFile = jest.fn() as unknown as MockedAsync<[string, string], string>;

  const loadStaticConfig = jest.fn() as unknown as MockedAsync<
    [unknown, unknown],
    { config: any; hooks: any; hooksSource?: string | null }
  >;

  const loadSecrets = jest.fn(() => ({ HOOK_SECRETS: { TEST: '1' } })) as unknown as jest.Mock;

  const parseForm = jest.fn() as unknown as jest.Mock;
  const loadTemplate = jest.fn() as unknown as MockedAsync<[unknown], unknown>;

  const createHookApi = jest.fn(() => ({ mocked: true })) as unknown as jest.Mock;

  type HookWorkerTask = {
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

  const runHookInWorker = jest.fn() as unknown as MockedAsync<
    [HookWorkerTask, { timeoutMs: number }],
    HookWorkerResult
  >;

  if (opts.mockYaml) {
    await jest.unstable_mockModule('yaml', () => ({
      stringify: (v: unknown) => JSON.stringify(v),
      default: {
        parse: (src: string) => {
          if (String(src).includes('kind:')) return [{ kind: 'x', value: 'y' }];
          return [];
        },
      },
    }));
  }

  await jest.unstable_mockModule('node:fs/promises', () => ({ readFile }));
  await jest.unstable_mockModule('../src/config.js', () => ({ loadStaticConfig }));
  await jest.unstable_mockModule('../src/utils/secrets.js', () => ({ loadSecrets }));
  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    parseForm,
    loadTemplate,
  }));
  await jest.unstable_mockModule('../src/handlers/request/validation/hook-api.js', () => ({
    createHookApi,
  }));

  await jest.unstable_mockModule('../src/handlers/request/validation/hook-pool.js', () => ({
    runHookInWorker,
  }));

  const mod = await import('../src/handlers/request/validation/run.js');

  const restore = () => {
    process.env.DEBUG_NS = prevDbg;
  };

  return {
    mod,
    mocks: {
      readFile,
      loadStaticConfig,
      loadSecrets,
      parseForm,
      loadTemplate,
      createHookApi,
      runHookInWorker,
    },
    restore,
  };
}

describe('validation/run.ts extra coverage', () => {
  it('projectForSchema: coerces booleans/numbers/objects/arrays + YAML path', async () => {
    const { mod, restore } = await loadSubject({ mockYaml: true });

    const schemaObj = {
      type: 'object',
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },

        // open-system -> visibility mapping
        visibility: { type: 'string' },

        // contactProp pick: "contacts" preferred
        contacts: { type: 'array', items: { type: 'string' } },

        correlationIds: { type: 'array', items: { type: 'string' } },

        // YAML parsing branch: itemsType === 'object'
        correlationIdTypes: { type: 'array', items: { type: 'object' } },

        enabled: { 'type': 'boolean', 'x-form-field': 'enabled' },
        count: { 'type': 'integer', 'x-form-field': 'count' },

        // array raw for string type should join with newline
        notes: { 'type': 'string', 'x-form-field': 'notes' },

        // schema-driven object parse (JSON)
        meta: { 'type': 'object', 'x-form-field': 'meta' },

        // schema-driven array from string list
        tags: { 'type': 'array', 'items': { type: 'string' }, 'x-form-field': 'tags' },
      },
    };

    const form = {
      'namespace': 'acme.system',
      'open-system': 'yes',
      'contact': 'a@b\nc@d',
      'correlationIds': 'id1, id2, id1',
      'correlationIdTypes': '- kind: a\n  value: b\n',
      'enabled': 'yes',
      'count': '42',
      'notes': ['line1', 'line2'] as any,
      'meta': '{"a": 1}',
      'tags': 'x\ny',
    };

    const candidate = await mod.projectForSchema('system', form, schemaObj);

    expect(candidate.type).toBe('system');
    expect(candidate.name).toBe('acme.system');
    expect(candidate.visibility).toBe('public');
    expect(candidate.contacts).toEqual(['a@b', 'c@d']);
    expect(candidate.correlationIds).toEqual(['id1', 'id2']);
    expect(Array.isArray(candidate.correlationIdTypes)).toBe(true);
    expect(candidate.enabled).toBe(true);
    expect(candidate.count).toBe(42);
    expect(candidate.notes).toBe('line1\nline2');
    expect(candidate.meta).toEqual({ a: 1 });
    expect(candidate.tags).toEqual(['x', 'y']);

    restore();
  });

  it('validateRequestIssue: descriptor hooks => forwards worker logs and logs __hookError (onValidate)', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          product: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
        hooks: { allowedHosts: ['api.sap.com'] },
      },
      source: 'repo:cfg',
      hooks: {
        __type: 'registry-bot-hooks:esm',
        __path: '.github/registry-bot/config.js',
        __hash: 'deadbeefdeadbeef',
        __code: 'export default {}',
      },
      hooksSource: 'repo:.github/registry-bot/config.js#deadbeefdeadbeef',
    } as any);

    // 1st call = beforeValidate
    mocks.runHookInWorker.mockResolvedValueOnce({ found: false, value: undefined, logs: [] });

    // 2nd call = onValidate
    mocks.runHookInWorker.mockResolvedValueOnce({
      found: true,
      value: { __hookError: 'boom' },
      logs: [
        { level: 'warn', obj: { w: 1 }, msg: 'worker-warn' },
        { level: 'error', obj: { e: 2 }, msg: 'worker-error' },
      ],
    });

    const schemaObj = {
      type: 'object',
      required: ['type', 'name', 'identifier'],
      properties: {
        type: { const: 'product' },
        name: { type: 'string' },
        identifier: { type: 'string' },
      },
    };

    const getContent = jest.fn(async (args: any) => {
      if (args.path === 'schema.json') {
        return {
          data: {
            content: Buffer.from(JSON.stringify(schemaObj), 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }
      const e: any = new Error('not found');
      e.status = 404;
      throw e;
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
    };

    const template: any = {
      body: [{ id: 'identifier', attributes: { label: 'Product ID' }, validations: { required: true } }],
      _meta: { requestType: 'product', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { identifier: 'SAPS4HANA123' } }
    );

    // Two worker calls: beforeValidate + onValidate
    expect(mocks.runHookInWorker).toHaveBeenCalledTimes(2);

    expect(mocks.runHookInWorker.mock.calls[0]?.[0]).toEqual(
      expect.objectContaining({ fn: 'beforeValidate', path: '.github/registry-bot/config.js' })
    );
    expect(mocks.runHookInWorker.mock.calls[1]?.[0]).toEqual(
      expect.objectContaining({ fn: 'onValidate', path: '.github/registry-bot/config.js' })
    );

    // Worker logs forwarded
    expect(ctx.log.warn).toHaveBeenCalledWith({ w: 1 }, 'worker-warn');
    expect(ctx.log.error).toHaveBeenCalledWith({ e: 2 }, 'worker-error');

    // __hookError is logged as warn
    expect(ctx.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'boom', fn: 'onValidate' }),
      'resource-bot hook validation failed'
    );

    expect(res.errors).toEqual([]);

    restore();
  });

  it('projectForSchema: fallback maps same-name props, skips empty arrays/objects, and tolerates non-object JSON/YAML', async () => {
    const { mod, restore } = await loadSubject();

    const schemaObj: any = {
      type: 'object',
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },

        extraFlag: { type: 'boolean' },

        meta: { type: 'object' },

        tags: { type: 'array', items: { type: 'string' } },

        emptyObj: { type: 'object' },
      },
    };

    const form: any = {
      identifier: 'ACME',
      extraFlag: 'true',
      meta: [],
      tags: '',
      emptyObj: {},
    };

    const candidate = await mod.projectForSchema('system', form, schemaObj);

    expect(candidate.extraFlag).toBe(true);
    expect(candidate).not.toHaveProperty('meta');
    expect(candidate).not.toHaveProperty('tags');
    expect(candidate).not.toHaveProperty('emptyObj');

    restore();
  });

  it('validateRequestIssue: descriptor hooks => falls back from onValidate to customValidate', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          product: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
        hooks: { allowedHosts: ['api.sap.com'] },
      },
      source: 'repo:cfg',
      hooks: {
        __type: 'registry-bot-hooks:esm',
        __path: '.github/registry-bot/config.js',
        __hash: 'h',
        __code: 'export default {}',
      },
      hooksSource: 'repo:.github/registry-bot/config.js#h',
    } as any);

    mocks.runHookInWorker.mockImplementation((task: any) => {
      if (task.fn === 'beforeValidate') {
        return Promise.resolve({ found: false, value: undefined, logs: [] });
      }
      if (task.fn === 'onValidate') {
        return Promise.resolve({ found: false, value: undefined, logs: [] });
      }
      // customValidate
      return Promise.resolve({ found: true, value: ['bad request'], logs: [] });
    });

    const schemaObj = {
      type: 'object',
      required: ['type', 'name', 'identifier'],
      properties: {
        type: { const: 'product' },
        name: { type: 'string' },
        identifier: { type: 'string' },
      },
    };

    const getContent = jest.fn(async (args: any) => {
      if (args.path === 'schema.json') {
        return {
          data: {
            content: Buffer.from(JSON.stringify(schemaObj), 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }
      const e: any = new Error('not found');
      e.status = 404;
      throw e;
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
    };

    const template: any = {
      body: [{ id: 'identifier', attributes: { label: 'Product ID' }, validations: { required: true } }],
      _meta: { requestType: 'product', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { identifier: 'SAPS4HANA123' } }
    );

    // 3 worker calls: beforeValidate + onValidate + customValidate
    expect(mocks.runHookInWorker).toHaveBeenCalledTimes(3);

    expect(mocks.runHookInWorker).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ fn: 'beforeValidate' }),
      expect.anything()
    );
    expect(mocks.runHookInWorker).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ fn: 'onValidate' }),
      expect.anything()
    );
    expect(mocks.runHookInWorker).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ fn: 'customValidate' }),
      expect.anything()
    );

    expect(res.errors.join('\n')).toContain('bad request');

    restore();
  });

  it('validateRequestIssue: registry existence check logs warning on non-404 errors (does not crash)', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          product: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      source: 'repo:cfg',
      hooks: null,
      hooksSource: null,
    } as any);

    const schemaObj = {
      type: 'object',
      required: ['type', 'name', 'identifier'],
      properties: {
        type: { const: 'product' },
        name: { type: 'string' },
        identifier: { type: 'string' },
      },
    };

    const getContent = jest.fn(async (args: any) => {
      if (args.path === 'schema.json') {
        return {
          data: {
            content: Buffer.from(JSON.stringify(schemaObj), 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }
      if (String(args.path).endsWith('.yaml')) {
        const e: any = new Error('boom');
        e.status = 500;
        throw e;
      }
      const e: any = new Error('not found');
      e.status = 404;
      throw e;
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
    };

    const template: any = {
      body: [{ id: 'identifier', attributes: { label: 'Product ID' }, validations: { required: true } }],
      _meta: { requestType: 'product', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { identifier: 'SAPS4HANA123' } }
    );

    expect(ctx.log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.stringContaining('boom') }),
      'registry existence check failed'
    );
    expect(res.errors.join('\n')).not.toMatch(/already exists in registry/i);

    restore();
  });

  it('validateRequestIssue: cannot resolve primary id => returns early with a clear error', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          product: { folderName: 'data', schema: '', issueTemplate: 'x' },
        },
      },
      source: 'repo:cfg',
      hooks: null,
      hooksSource: null,
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
    };

    const template: any = {
      body: [{ id: 'foo', attributes: { label: 'Foo' }, validations: { required: true } }],
      _meta: { requestType: 'product', schema: '', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: {} }
    );

    expect(res.errors).toEqual(['Cannot resolve primary identifier from template']);
    expect(res.namespace).toBe('');
    expect(res.nsType).toBe('');

    restore();
  });

  it('resolvePrimaryIdFromTemplate: uses schema x-form-field="identifier" property fallback', async () => {
    const { mod, restore } = await loadSubject();

    const template = {
      body: [{ id: 'foo', attributes: { label: 'Foo' }, validations: { required: true } }],
    };

    const formData = { foo: 'ID_FROM_OTHER_FIELD' };

    const schemaObj = {
      type: 'object',
      properties: {
        foo: { 'type': 'string', 'x-form-field': 'identifier' },
      },
    };

    const id = mod.resolvePrimaryIdFromTemplate(template, formData, schemaObj);
    expect(id).toBe('ID_FROM_OTHER_FIELD');

    restore();
  });

  it('validateRequestIssue: DBG logs + hooks throw-catches + repo schema cache + ajv caches + additionalProperties debug', async () => {
    const { mod, mocks, restore } = await loadSubject({ dbg: '1' });

    // hooks: ajvPlugins throws, beforeValidate throws, customValidate returns rule errors
    mocks.loadStaticConfig.mockResolvedValue({
      config: {
        requests: {
          system: {
            folderName: 'data',
            schema: '/schema.json',
            issueTemplate: 'system.yml',
          },
        },
        hooks: { allowedHosts: ['api.example.com'] },
      },
      hooks: {
        ajvPlugins: () => {
          throw new Error('ajvPlugins boom');
        },
        beforeValidate: () => {
          throw new Error('beforeValidate boom');
        },
        customValidate: async () => ['rule failed'],
      },
      hooksSource: 'mock-hooks.js',
    });

    // form: missing "identifier", but has namespace -> primary id resolution works
    mocks.parseForm.mockReturnValue({
      namespace: 'acme.sys',
      tags: 'BAD',
      title: 'x',
      meta: '{"extra":"x"}',
    });

    const schemaObj = {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name', 'identifier', 'title', 'tags', 'meta'],
      properties: {
        type: { const: 'system' },
        name: { type: 'string', minLength: 1 },
        identifier: { type: 'string', minLength: 1 },

        // ajv-errors errorMessage wrapper branch
        title: {
          type: 'string',
          minLength: 5,
          errorMessage: { minLength: 'Title too short' },
        },

        // noisy oneOf type error + minItems at same path -> filterNoisyOneOfTypeErrors()
        tags: {
          'type': 'array',
          'x-form-field': 'tags',
          'oneOf': [{ type: 'string' }, { type: 'array', minItems: 2, items: { type: 'string', pattern: '^[a-z]+$' } }],
        },

        // additionalProperties error with params.additionalProperty -> DBG "extras" logging
        meta: {
          'type': 'object',
          'x-form-field': 'meta',
          'properties': { allowed: { type: 'string' } },
          'additionalProperties': false,
        },
      },
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      // schemaPath starts with "/": loader strips leading "/" -> "schema.json"
      if (path === 'schema.json') {
        return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      }

      // registry check (template._meta.root wins): "/mydata/" -> "mydata"
      if (path === 'mydata/acme.sys.yaml') throw httpErr(404, 'not found');

      throw new Error(`unexpected path: ${path}`);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [
        { id: 'identifier', attributes: { label: 'Identifier' }, validations: { required: true } },
        { id: 'tags', attributes: { label: 'Tags' }, validations: { required: true } },
        { id: 'title', attributes: { label: 'Title' } },
        { id: 'meta', attributes: { label: 'Meta' } },
      ],
      _meta: { requestType: 'system', schema: '/schema.json', root: '/mydata/' },
    };

    // call twice -> hits ensureStaticConfigLoaded early return + AJV_CACHE/SCHEMA_CACHE branches
    const r1 = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'body' }, { template });

    const r2 = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'body' }, { template });

    // loadStaticConfig only first time
    expect(mocks.loadStaticConfig).toHaveBeenCalledTimes(1);

    // schema fetched once total
    const schemaCalls = getContent.mock.calls.filter((c) => c?.[0]?.path === 'schema.json');
    expect(schemaCalls).toHaveLength(1);

    // DBG logs + hook warnings
    expect(ctx.log.info).toHaveBeenCalled();
    expect(ctx.log.warn).toHaveBeenCalled();

    // hook api sees allowedHosts
    expect(mocks.createHookApi).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ allowedHosts: ['api.example.com'] })
    );

    // sanity: we produced errors
    expect(r1.errorsGrouped.rules).toContain('rule failed');
    expect(r1.errorsGrouped.form.join('\n')).toMatch(/Required field is missing in form: Identifier/);
    expect(r1.errorsFormatted).toMatch(/### Identifier/);

    // second call should behave same-ish
    expect(r2.errorsGrouped.rules).toContain('rule failed');

    restore();
  });

  it('validateRequestIssue: schema has x-form-field="identifier" but template lacks identifier field => config error message', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValue({
      config: {
        requests: {
          system: {
            folderName: 'data',
            schema: '/schema.json',
            issueTemplate: 'system.yml',
          },
        },
      },
      hooks: null,
      hooksSource: null,
    });

    mocks.parseForm.mockReturnValue({
      namespace: 'acme.sys',
      title: 'valid title',
    });

    const schemaObj = {
      type: 'object',
      properties: {
        // marks primary identifier via x-form-field="identifier"
        name: { 'type': 'string', 'x-form-field': 'identifier' },
        type: { const: 'system' },
      },
      required: ['type', 'name'],
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'namespace', attributes: { label: 'Namespace' }, validations: { required: true } }],
      _meta: { requestType: 'system', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'body' }, { template });

    expect(res.errors.join('\n')).toMatch(/schema marks a primary identifier/i);

    restore();
  });

  it('validateRequestIssue: registry existence check adds registry error when file exists', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValue({
      config: {
        requests: {
          system: {
            folderName: 'data',
            schema: '/schema.json',
            issueTemplate: 'system.yml',
          },
        },
      },
      hooks: null,
      hooksSource: null,
    });

    mocks.parseForm.mockReturnValue({
      identifier: 'acme.sys',
      title: 'valid title',
    });

    const schemaObj = {
      type: 'object',
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },
        identifier: { type: 'string' },
        title: { type: 'string' },
      },
      required: ['type', 'name', 'identifier', 'title'],
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') return { data: { content: b64json(schemaObj), encoding: 'base64' } };

      // file exists -> registry error bucket
      if (path === 'data/acme.sys.yaml') return { data: { content: '', encoding: 'base64' } };

      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'identifier', validations: { required: true } }],
      _meta: { requestType: 'system', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'body' }, { template });

    expect(res.errorsGrouped.registry.join('\n')).toMatch(/already exists/i);

    restore();
  });

  it('validateRequestIssue: repo schema 404 => local schema fallback exercises readFile error+success path', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValue({
      config: {
        requests: {
          system: {
            folderName: 'data',
            schema: 'local.schema.json',
            issueTemplate: 'system.yml',
          },
        },
      },
      hooks: null,
      hooksSource: null,
    });

    mocks.parseForm.mockReturnValue({
      identifier: 'acme.sys',
      title: 'valid title',
    });

    // repo always 404 -> fallback to loadSchemaLocal()
    const getContent = jest.fn(async () => {
      throw httpErr(404);
    });

    const localSchema = JSON.stringify({
      type: 'object',
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },
        identifier: { type: 'string' },
        title: { type: 'string' },
      },
      required: ['type', 'name', 'identifier', 'title'],
    });

    mocks.readFile
      .mockRejectedValueOnce(new Error('ENOENT'))
      .mockResolvedValueOnce(localSchema)
      .mockResolvedValue(localSchema);

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'identifier', validations: { required: true } }],
      _meta: { requestType: 'system', schema: 'local.schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'body' }, { template });

    expect(mocks.readFile.mock.calls.length).toBeGreaterThanOrEqual(2);

    expect(res.errorsGrouped.registry).toEqual([]);

    restore();
  });

  it('validateRequestIssue: loadStaticConfig throws => handled (static-config:load-failed)', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockRejectedValueOnce(new Error('boom'));

    const ctx: any = {
      octokit: {
        repos: { getContent: jest.fn(async () => ({ data: [] })) },
        issues: mkIssuesStub(),
      },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [],
      _meta: { requestType: 'system', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: {} }
    );

    expect(ctx.log.warn).toHaveBeenCalled();
    expect(res.errors.join('\n')).toMatch(/unknown requestType/i);

    restore();
  });

  it('validateRequestIssue: missing template => returns missing-template config error', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: { requests: {} },
      source: 'default',
      hooks: null,
      hooksSource: null,
    } as any);

    mocks.loadTemplate.mockRejectedValueOnce(new Error('template missing'));

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
    };

    const res = await mod.validateRequestIssue(ctx, { owner: 'o', repo: 'r' }, { body: 'issue body' }, {});

    expect(res.template).toBeNull();
    expect(res.errorsFormatted).toContain('Missing form template');
    expect(res.errorsFormatted).toContain('template missing');

    restore();
  });

  it('projectForSchema: false booleans + type fallback + misc text fields', async () => {
    const { mod, restore } = await loadSubject();

    const schemaObj: any = {
      type: 'object',
      properties: {
        type: { type: 'string' },
        name: { type: 'string' },

        enabled: { 'type': 'boolean', 'x-form-field': 'enabled' },

        shortDescription: { type: 'string' },
        summary: { type: 'string' },
        details: { type: 'string' },
        parentId: { type: 'string' },
      },
    };

    const form: any = {
      'identifier': 'acme.sys',
      'enabled': 'no',
      'short-description': '  short  ',
      'summary': ' sum ',
      'details': ' det ',
      'parentId': ' p1 ',
    };

    const cand = await mod.projectForSchema('system', form, schemaObj);

    expect(cand.type).toBe('system');
    expect(cand.name).toBe('acme.sys');
    expect(cand.enabled).toBe(false);

    expect(cand.shortDescription).toBe('short');
    expect(cand.summary).toBe('sum');
    expect(cand.details).toBe('det');
    expect(cand.parentId).toBe('p1');

    restore();
  });
  it('projectForSchema: array(items:object) parses JSON into array', async () => {
    const { mod, restore } = await loadSubject();

    const schemaObj: any = {
      type: 'object',
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },
        correlationIdTypes: { type: 'array', items: { type: 'object' } },
      },
    };

    const form: any = {
      identifier: 'acme.sys',
      correlationIdTypes: '[{"k":"v"},{"k":"v2"}]',
    };

    const cand = await mod.projectForSchema('system', form, schemaObj);
    expect(cand.correlationIdTypes).toEqual([{ k: 'v' }, { k: 'v2' }]);

    restore();
  });
  it('validateRequestIssue: PartnerNamespace => invalid requestType selection returns form error', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          subContextNamespace: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [],
      _meta: { requestType: 'partnerNamespace', schema: '/partner.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { requestType: 'nope' } }
    );

    expect(res.errors.join('\n')).toMatch(/Invalid Partner Namespace 'Request Type' selection/i);

    restore();
  });
  it('validateRequestIssue: PartnerNamespace => mapped requestType missing in cfg.requests returns schema error', async () => {
    const { mod, mocks, restore } = await loadSubject();

    // Note: systemNamespace is NOT present => mapping should fail here
    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          subContextNamespace: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [],
      _meta: { requestType: 'partnerNamespace', schema: '/partner.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { requestType: 'system' } } // => maps to systemNamespace
    );

    expect(res.errors.join('\n')).toMatch(/cfg\.requests has no such entry/i);

    restore();
  });
  it('validateRequestIssue: PartnerNamespace => mapped schema empty returns schema error', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          systemNamespace: { folderName: 'data', schema: null, issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [],
      _meta: { requestType: 'partnerNamespace', schema: '/partner.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { requestType: 'system' } }
    );

    expect(res.errors.join('\n')).toMatch(/schema is empty/i);

    restore();
  });
  it('validateRequestIssue: PartnerNamespace => switches schema and continues full pipeline', async () => {
    const { mod, mocks, restore } = await loadSubject({ dbg: '1' });

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          subContextNamespace: {
            folderName: '/data/namespaces/',
            schema: '/schema.json',
            issueTemplate: 'x',
          },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const schemaObj = {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name', 'identifier'],
      properties: {
        type: { const: 'subcontext' },
        name: { type: 'string' },
        identifier: { type: 'string' },
        description: { type: 'string' },
        correlationIds: { type: 'array', items: { type: 'string' } },
      },
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') {
        return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      }
      // registry existence check path:
      if (path === 'data/namespaces/sap.cds.ai.yaml') throw httpErr(404);
      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'identifier', validations: { required: true } }],
      _meta: { requestType: 'partnerNamespace', schema: '/partner.json', root: 'ignored' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      {
        template,
        formData: {
          'requestType': 'subContext',
          'identifier': 'sap.cds.ai',
          'sub-context-description': '  hello  ',
          'correlation-ids': 'c1\nc2',
        },
      }
    );

    expect(res.errors).toEqual([]);
    expect(res.formData.requestType).toBe('subContextNamespace');
    expect(res.formData.identifier).toBe('sap.cds.ai');
    expect(res.formData.description).toBe('hello');
    expect(res.formData.correlationIds).toBe('c1\nc2');
    expect(res.nsType).toBe('subcontext');

    // DBG path logs normalizedFormData
    expect(ctx.log.info.mock.calls.some((c: any[]) => c?.[1] === 'ns:normalizedFormData')).toBe(true);

    // template meta was cloned/overridden
    expect(res.template?._meta?.requestType).toBe('subContextNamespace');
    expect(res.template?._meta?.schema).toBe('/schema.json');

    restore();
  });
  it('validateRequestIssue: nsType falls back to requestType lowercase for unknown types', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          WeirdThing: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const schemaObj = {
      type: 'object',
      required: ['type', 'name'],
      properties: { type: { const: 'weird' }, name: { type: 'string' } },
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      if (path === 'data/acme.weird.yaml') throw httpErr(404);
      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { warn: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'identifier', validations: { required: true } }],
      _meta: { requestType: 'WeirdThing', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { identifier: 'acme.weird' } }
    );

    expect(res.nsType).toBe('weirdthing');

    restore();
  });
  it('validateRequestIssue: DBG logs additional-properties when nested object has extras', async () => {
    const { mod, mocks, restore } = await loadSubject({ dbg: '1' });

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          system: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const schemaObj = {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name', 'meta'],
      properties: {
        type: { const: 'system' },
        name: { type: 'string' },
        meta: {
          'type': 'object',
          'x-form-field': 'meta',
          'properties': { allowed: { type: 'string' } },
          'additionalProperties': false, // triggers additionalProperties error
        },
      },
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      if (path === 'data/acme.sys.yaml') throw httpErr(404);
      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'identifier', validations: { required: true } }, { id: 'meta' }],
      _meta: { requestType: 'system', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { identifier: 'acme.sys', meta: '{"extra":"x"}' } }
    );

    expect(res.errors.length).toBeGreaterThan(0);
    expect(ctx.log.info.mock.calls.some((c: any[]) => c?.[1] === 'ns:additional-properties')).toBe(true);

    restore();
  });
  it('runCustomValidateForRegistryCandidate: buildFormFromCandidate stringifies complex values', async () => {
    const { mod, restore } = await loadSubject();

    const onValidate = jest.fn(async ({ form }: any) => {
      expect(form).toHaveProperty('name', 'acme.sys');
      expect(form).toHaveProperty('identifier', 'acme.sys');
      expect(form).toHaveProperty('namespace', 'acme.sys');

      expect(typeof form.meta).toBe('string');
      return ['ok'];
    });

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: { onValidate },
      resourceBotConfig: { requests: {} },
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'system',
        schema: {},
        candidate: { name: 'acme.sys', meta: { a: 1 } }, // triggers yaml stringify branch
      }
    );

    expect(msgs).toEqual(['ok']);
    expect(onValidate).toHaveBeenCalledTimes(1);

    restore();
  });
  it('runCustomValidateForRegistryCandidate: descriptor hooks run via worker', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.runHookInWorker.mockResolvedValueOnce({
      found: true,
      value: [{ field: 'meta', message: 'bad' }],
      logs: [],
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: {
        __type: 'registry-bot-hooks:esm',
        __path: '.github/registry-bot/config.js',
        __hash: 'h',
        __code: 'export default {}',
      },
      resourceBotConfig: { requests: {} },
      resourceBotHooksSource: 'repo:.github/registry-bot/config.js#h',
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'system',
        schema: {},
        candidate: { name: 'acme.sys', meta: { a: 1 } },
      }
    );

    expect(mocks.runHookInWorker).toHaveBeenCalledWith(
      expect.objectContaining({
        fn: 'onValidate',
        args: expect.objectContaining({
          requestType: 'system',
          form: expect.objectContaining({
            identifier: 'acme.sys',
            namespace: 'acme.sys',
          }),
        }),
      }),
      expect.anything()
    );

    expect(msgs).toEqual(['meta: bad']);

    restore();
  });
  it('runCustomValidateForRegistryCandidate: normalizes explicit formData like issue validation', async () => {
    const { mod, restore } = await loadSubject();

    const onValidate = jest.fn(async ({ form, requestType, resourceName }: any) => {
      expect(requestType).toBe('product');
      expect(resourceName).toBe('EXPLICIT-ID');

      expect(form.requestType).toBe('product');
      expect(form.identifier).toBe('PROD-1');
      expect(form.namespace).toBe('PROD-1');
      expect(form.description).toBe('desc');
      expect(form.contact).toBe('a@x\nb@y');
      expect(form.correlationIds).toBe('c1\nc2');

      return [];
    });

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), debug: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: { onValidate },
      resourceBotConfig: { requests: {}, hooks: { allowedHosts: ['api.sap.com'] } },
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'product',
        resourceName: 'EXPLICIT-ID',
        schema: {
          type: 'object',
          properties: {
            identifier: { type: 'string' },
            title: { type: 'string' },
          },
        },
        candidate: { name: 'ignored' },
        formData: {
          'product-id': ' PROD-1 ',
          'description': '  desc  ',
          'contacts': 'a@x\nb@y',
          'correlation-ids': 'c1\nc2',
        },
      }
    );

    expect(msgs).toEqual([]);
    expect(onValidate).toHaveBeenCalledTimes(1);

    restore();
  });

  it('runCustomValidateForRegistryCandidate: builds normalized form from candidate using schema mapping', async () => {
    const { mod, restore } = await loadSubject({ mockYaml: true });

    const onValidate = jest.fn(async ({ form, requestType, resourceName }: any) => {
      expect(requestType).toBe('system');
      expect(resourceName).toBe('acme.sys');

      expect(form.identifier).toBe('acme.sys');
      expect(form.namespace).toBe('acme.sys');
      expect(form.requestType).toBe('system');

      expect(form.contact).toBe('a@x\nb@y');
      expect(form.contacts).toBe('a@x\nb@y');
      expect(typeof form.meta).toBe('string');
      expect(form.meta).toContain('"a":1');
      expect(form.enabled).toBe('true');
      expect(form.count).toBe('3');

      return ['ok'];
    });

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), debug: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: { onValidate },
      resourceBotConfig: { requests: {}, hooks: { allowedHosts: ['api.sap.com'] } },
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'system',
        schema: {
          type: 'object',
          properties: {
            name: { type: 'string' },
            contacts: { type: 'array', items: { type: 'string' } },
            meta: { type: 'object' },
            enabled: { type: 'boolean' },
            count: { type: 'integer' },
          },
        },
        candidate: {
          name: 'acme.sys',
          contacts: ['a@x', 'b@y'],
          meta: { a: 1 },
          enabled: true,
          count: 3,
        },
      }
    );

    expect(msgs).toEqual(['ok']);
    expect(onValidate).toHaveBeenCalledTimes(1);

    restore();
  });

  it('runCustomValidateForRegistryCandidate: descriptor hooks forward worker logs and use normalized form', async () => {
    const { mod, mocks, restore } = await loadSubject();

    mocks.runHookInWorker.mockResolvedValueOnce({
      found: true,
      value: [{ field: 'identifier', message: 'bad id' }],
      logs: [
        { level: 'info', obj: { i: 1 }, msg: 'worker-info' },
        { level: 'warn', obj: { w: 2 }, msg: 'worker-warn' },
      ],
    } as any);

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), debug: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: {
        __type: 'registry-bot-hooks:esm',
        __path: '.github/registry-bot/config.js',
        __hash: 'h',
        __code: 'export default {}',
      },
      resourceBotConfig: { requests: {}, hooks: { allowedHosts: ['api.sap.com'] } },
      resourceBotHooksSource: 'repo:.github/registry-bot/config.js#h',
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'product',
        schema: {
          type: 'object',
          properties: {
            identifier: { type: 'string' },
            title: { type: 'string' },
          },
        },
        candidate: { name: 'ignored' },
        formData: {
          'product-id': ' PROD-2 ',
          'contacts': ['a@x', 'b@y'] as any,
        },
      }
    );

    expect(mocks.runHookInWorker).toHaveBeenCalledWith(
      expect.objectContaining({
        fn: 'onValidate',
        args: expect.objectContaining({
          requestType: 'product',
          resourceName: 'PROD-2',
          form: expect.objectContaining({
            identifier: 'PROD-2',
            namespace: 'PROD-2',
            contact: 'a@x\nb@y',
          }),
        }),
      }),
      expect.anything()
    );

    expect(ctx.log.info).toHaveBeenCalledWith({ i: 1 }, 'worker-info');
    expect(ctx.log.warn).toHaveBeenCalledWith({ w: 2 }, 'worker-warn');
    expect(msgs).toEqual(['identifier: bad id']);

    restore();
  });

  it('runCustomValidateForRegistryCandidate: returns hook error when legacy onValidate throws', async () => {
    const { mod, restore } = await loadSubject();

    const onValidate = jest.fn(async () => {
      throw new Error('boom');
    });

    const ctx: any = {
      octokit: { repos: { getContent: jest.fn() }, issues: mkIssuesStub() },
      log: { warn: jest.fn(), info: jest.fn(), debug: jest.fn(), error: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      resourceBotHooks: { onValidate },
      resourceBotConfig: { requests: {}, hooks: { allowedHosts: ['api.sap.com'] } },
    };

    const msgs = await mod.runCustomValidateForRegistryCandidate(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        requestType: 'product',
        schema: {},
        candidate: { name: 'acme.sys' },
      }
    );

    expect(msgs).toEqual(['Hook onValidate failed: boom']);

    restore();
  });

  it('validateRequestIssue: DBG logs schema-path and schema-input for x-form-field identifier mismatch', async () => {
    const { mod, mocks, restore } = await loadSubject({ dbg: '1' });

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        requests: {
          system: { folderName: 'data', schema: '/schema.json', issueTemplate: 'x' },
        },
      },
      hooks: null,
      hooksSource: null,
    } as any);

    const schemaObj = {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      properties: {
        type: { const: 'system' },
        resourceName: { 'type': 'string', 'x-form-field': 'identifier' },
      },
      required: ['type', 'resourceName'],
    };

    const getContent = jest.fn(async ({ path }: { path: string }) => {
      if (path === 'schema.json') return { data: { content: b64json(schemaObj), encoding: 'base64' } };
      if (path === 'data/acme.sys.yaml') throw httpErr(404);
      throw httpErr(404);
    });

    const ctx: any = {
      octokit: { repos: { getContent }, issues: mkIssuesStub() },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
      repo: () => ({ owner: 'o', repo: 'r' }),
      issue: () => ({ owner: 'o', repo: 'r', issue_number: 1 }),
    };

    const template: any = {
      body: [{ id: 'namespace', attributes: { label: 'Namespace' }, validations: { required: true } }],
      _meta: { requestType: 'system', schema: '/schema.json', root: 'data' },
    };

    const res = await mod.validateRequestIssue(
      ctx,
      { owner: 'o', repo: 'r' },
      { body: 'body' },
      { template, formData: { resourceName: 'acme.sys' } }
    );

    expect(res.errors.join('\n')).toMatch(/schema marks a primary identifier/i);
    expect(ctx.log.info.mock.calls.some((c: any[]) => c?.[1] === 'ns:schema-path')).toBe(true);
    expect(ctx.log.info.mock.calls.some((c: any[]) => c?.[1] === 'schema-input')).toBe(true);

    restore();
  });
});
