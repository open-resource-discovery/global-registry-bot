/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { beforeAll, beforeEach, expect, jest, test } from '@jest/globals';

process.env.DEBUG_NS = '1';

type RepoRef = { owner: string; repo: string };
type IssueRef = { owner: string; repo: string; issue_number: number };

type FileEntry = { kind: 'file'; text: string; encoding?: string } | { kind: 'dir' } | { kind: 'err'; status: number };

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function httpErr(status: number): Error & { status: number } {
  const e = new Error(String(status)) as Error & { status: number };
  e.status = status;
  return e;
}

/* mocks wired into run.ts */
type LoadStaticConfigFn = (...args: any[]) => Promise<{
  config: any;
  source: string;
  hooks: any;
  hooksSource: string | null;
}>;

type LoadTemplateFn = (...args: any[]) => Promise<any>;

const loadStaticConfig = jest.fn<LoadStaticConfigFn>();
const loadTemplate = jest.fn<LoadTemplateFn>();

const loadSecrets = jest.fn();
const createHookApi = jest.fn();

const parseForm = jest.fn();

type ReadFileFn = (...args: any[]) => Promise<string>;
const readFile = jest.fn<ReadFileFn>();

/* module under test (lazy) */

let validateRequestIssue: any;
let projectForSchema: any;
let resolvePrimaryIdFromTemplate: any;

beforeAll(async () => {
  jest.resetModules();

  // ../../../config.js
  await jest.unstable_mockModule('../src/config.js', () => ({
    loadStaticConfig,
  }));

  // ../../../utils/secrets.js
  loadSecrets.mockReturnValue({
    APP_ID: '1',
    WEBHOOK_SECRET: '1',
    PRIVATE_KEY: '1',
    DEBUG_NS: '1',
    HOOK_SECRETS: {
      TEST_TOKEN: 't',
    },
  });

  await jest.unstable_mockModule('../src/utils/secrets.js', () => ({
    loadSecrets,
  }));

  // ../template.js
  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    loadTemplate,
    parseForm,
  }));

  // ./hook-api.js
  createHookApi.mockReturnValue({ ok: true });
  await jest.unstable_mockModule('../src/handlers/request/validation/hook-api.js', () => ({
    createHookApi,
  }));

  // node:fs/promises
  await jest.unstable_mockModule('node:fs/promises', () => ({
    readFile,
  }));

  const mod = await import('../src/handlers/request/validation/run.js');
  validateRequestIssue = mod.validateRequestIssue;
  projectForSchema = mod.projectForSchema;
  resolvePrimaryIdFromTemplate = mod.resolvePrimaryIdFromTemplate;
});

beforeEach(() => {
  jest.clearAllMocks();

  // default: config load ok
  loadStaticConfig.mockResolvedValue({
    config: {
      requests: {
        system: {
          folderName: 'registry',
          schema: 'schemas/default.json',
          issueTemplate: '.github/ISSUE_TEMPLATE/system.yml',
        },
      },
      hooks: { allowedHosts: ['api.sap.com'] },
    },
    source: 'repo:.github/registry-bot/config.yaml',
    hooks: null,
    hooksSource: null,
  });

  // default: template parse (only used if you don't pass options.formData)
  parseForm.mockReturnValue({ identifier: 'sys.one' });

  // default: no local schema reads
  readFile.mockRejectedValue(new Error('ENOENT'));
});

function mkContext(args: {
  owner: string;
  repo: string;
  issue_number?: number;
  files?: Record<string, FileEntry>;
  presetConfig?: any;
  presetHooks?: any;
  presetHooksSource?: string | null;
}) {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const issue_number = args.issue_number ?? 1;

  const getContent = jest.fn(async ({ owner, repo, path }: any) => {
    const key = `${owner}/${repo}:${path}`;
    const entry = args.files?.[key];
    if (!entry) throw httpErr(404);

    if (entry.kind === 'dir') return { data: [] };
    if (entry.kind === 'err') throw httpErr(entry.status);

    return {
      data: {
        content: b64(entry.text),
        encoding: entry.encoding ?? 'base64',
      },
    };
  });

  const context: any = {
    octokit: {
      repos: { getContent },
      issues: {
        get: jest.fn(),
        listForRepo: jest.fn(),
        update: jest.fn(),
        create: jest.fn(),
        createComment: jest.fn(),
        addLabels: jest.fn(),
        removeLabel: jest.fn(),
      },
    },
    log: {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
    repo: (): RepoRef => ({ owner: args.owner, repo: args.repo }),
    issue: (): IssueRef => ({ owner: args.owner, repo: args.repo, issue_number }),
  };

  if (args.presetConfig !== undefined) context.resourceBotConfig = args.presetConfig;
  if (args.presetHooks !== undefined) context.resourceBotHooks = args.presetHooks;
  if (args.presetHooksSource !== undefined) context.resourceBotHooksSource = args.presetHooksSource;

  return { context, getContent };
}

test('returns missing-template result when loadTemplate throws', async () => {
  loadTemplate.mockRejectedValueOnce(new Error('no template'));

  const { context } = mkContext({ owner: 'o1', repo: 'r1' });

  const res = await validateRequestIssue(
    context,
    { owner: 'o1', repo: 'r1' },
    { body: 'x', title: 't', labels: [] },
    {}
  );

  expect(res.template).toBeNull();
  expect(res.errorsFormattedSingle).toContain('### Form');
  expect(res.errors.join('\n')).toMatch(/Missing form template/i);
});

test('returns schema error when template is missing _meta.requestType', async () => {
  const { context } = mkContext({
    owner: 'o2',
    repo: 'r2',
    presetConfig: { requests: { system: { folderName: 'registry', schema: 'schemas/x.json' } } },
  });

  const tpl = {
    body: [],
    _meta: { schema: 'schemas/x.json', root: 'registry', path: 't.yml' },
  };

  const res = await validateRequestIssue(
    context,
    { owner: 'o2', repo: 'r2' },
    { body: 'x', title: 't' },
    { template: tpl as any, formData: {} as any }
  );

  expect(res.errors.join('\n')).toMatch(/template missing _meta\.requestType/i);
  expect(res.errorsFormatted).toContain('### General');
});

test('returns schema error when requestType is unknown (missing cfg.requests entry)', async () => {
  const { context } = mkContext({
    owner: 'o3',
    repo: 'r3',
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: { requests: { other: { folderName: 'x', schema: 'schemas/y.json' } } },
    source: 'repo:.github/registry-bot/config.yaml',
    hooks: null,
    hooksSource: null,
  });

  const tpl = {
    body: [],
    _meta: { requestType: 'system', schema: 'schemas/y.json', root: 'registry', path: 't.yml' },
  };

  const res = await validateRequestIssue(
    context,
    { owner: 'o3', repo: 'r3' },
    { body: 'x', title: 't' },
    { template: tpl as any, formData: { identifier: 'sys.one' } as any }
  );

  expect(res.errors.join('\n')).toMatch(/unknown requestType/i);
});

test('adds required-field messages and returns "Cannot resolve primary identifier" if no id can be resolved', async () => {
  const schema = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    type: 'object',
    properties: { type: { const: 'system' } },
  };

  const owner = 'o4';
  const repo = 'r4';

  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:.github/registry-bot/schemas/t4.json`]: {
        kind: 'file',
        text: JSON.stringify(schema),
      },
    },
  });

  const tpl = {
    body: [{ id: 'identifier', attributes: { label: 'Identifier' }, validations: { required: true } }],
    _meta: { requestType: 'system', schema: 'schemas/t4.json', root: 'registry', path: 't.yml' },
  };

  const res = await validateRequestIssue(
    context,
    { owner, repo },
    { body: 'x', title: 't' },
    { template: tpl as any, formData: {} as any }
  );

  expect(res.errors).toEqual([
    'Required field is missing in form: Identifier',
    'Cannot resolve primary identifier from template',
  ]);
  expect(res.errorsFormatted).toMatch(/Required field is missing/i);
});

test('resolvePrimaryIdFromTemplate can use schema x-form-field="identifier" mapping', async () => {
  const template = { body: [{ id: 'something' }] };
  const form = { resourceName: 'sys.from.schema' };
  const schemaObj = {
    properties: {
      resourceName: { 'x-form-field': 'identifier' },
    },
  };

  const id = resolvePrimaryIdFromTemplate(template as any, form as any, schemaObj as any);
  expect(id).toBe('sys.from.schema');
});

test('projectForSchema builds candidate with coercions (arrays/objects/visibility)', async () => {
  const schemaObj = {
    type: 'object',
    properties: {
      type: { const: 'system' },
      name: { type: 'string' },
      description: { type: 'string' },
      visibility: { type: 'string' },
      contacts: { type: 'array', items: { type: 'string' } },
      correlationIds: { type: 'array', items: { type: 'string' } },
      correlationIdTypes: { type: 'array', items: { type: 'object' } },
      tags: { 'type': 'array', 'items': { type: 'string' }, 'x-form-field': 'tags' },
    },
  };

  const form: any = {
    'identifier': 'sys.one',
    'description': '  hello  ',
    'open-system': 'yes',
    'contact': ' a@x \n b@y \n',
    'correlationIds': ' c1\nc1\nc2, c3 ',
    'correlationIdTypes': '[{"k":"v"},{"k":"v2"}]',
    'tags': 'tag1',
  };

  const cand = await projectForSchema('system', form, schemaObj);
  expect(cand.type).toBe('system');
  expect(cand.name).toBe('sys.one');
  expect(cand.description).toBe('hello');
  expect(cand.visibility).toBe('public');
  expect(cand.contacts).toEqual(['a@x', 'b@y']);
  expect(cand.correlationIds).toEqual(['c1', 'c2', 'c3']);
  expect(cand.correlationIdTypes).toEqual([{ k: 'v' }, { k: 'v2' }]);
  expect(cand.tags).toEqual(['tag1']);
});

test('full flow: valid schema + hooks.customValidate + registry-exists adds buckets', async () => {
  const owner = 'o7';
  const repo = 'r7';

  const schemaOk = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    type: 'object',
    additionalProperties: true,
    properties: {
      type: { const: 'system' },
      name: { type: 'string' },
      description: { type: 'string' },
      visibility: { type: 'string' },
    },
    required: ['type', 'name'],
  };

  const hooks = {
    beforeValidate: jest.fn(),
    customValidate: jest.fn(async () => ['identifier contains spaces']),
  };

  loadStaticConfig.mockResolvedValueOnce({
    config: {
      requests: {
        system: { folderName: 'registry', schema: 'schemas/t7.json', issueTemplate: 't.yml' },
      },
      hooks: { allowedHosts: ['api.sap.com', 'example.com'] },
    },
    source: 'repo:.github/registry-bot/config.yaml',
    hooks,
    hooksSource: 'repo:.github/registry-bot/config.js',
  });

  const { context, getContent } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:.github/registry-bot/schemas/t7.json`]: {
        kind: 'file',
        text: JSON.stringify(schemaOk),
      },
      [`${owner}/${repo}:registry/sys.one.yaml`]: {
        kind: 'file',
        text: 'exists: true',
      },
    },
  });

  const tpl = {
    body: [
      { id: 'identifier', attributes: { label: 'Identifier' }, validations: { required: true } },
      { id: 'description', attributes: { label: 'Description' } },
    ],
    _meta: { requestType: 'system', schema: 'schemas/t7.json', root: 'registry', path: 't.yml' },
    title: 'System: Request',
  };

  const formData: any = {
    'identifier': 'sys.one',
    'description': '  desc  ',
    'open-system': 'yes',
    'contact': 'a@x',
    'correlationIds': 'c1',
  };

  const res = await validateRequestIssue(
    context,
    { owner, repo },
    { body: 'x', title: 't' },
    { template: tpl as any, formData }
  );

  expect(createHookApi).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({
      allowedHosts: ['api.sap.com', 'example.com'],
    })
  );

  expect(hooks.beforeValidate).toHaveBeenCalledTimes(1);
  expect(hooks.customValidate).toHaveBeenCalledTimes(1);

  expect(res.formData.identifier).toBe('sys.one');
  expect(res.formData.namespace).toBe('sys.one');
  expect(res.formData.description).toBe('desc');

  expect(res.errors.join('\n')).toMatch(/identifier contains spaces/i);
  expect(res.errors.join('\n')).toMatch(/already exists in registry/i);

  // schema file + registry check
  expect(getContent).toHaveBeenCalled();
});

test('schema invalid: filters noisy type errors for arrays and still runs customValidate', async () => {
  const owner = 'o8';
  const repo = 'r8';

  const schemaBad = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    type: 'object',
    additionalProperties: false,
    properties: {
      type: { const: 'system' },
      name: { type: 'string', minLength: 3 },
      tags: {
        'type': 'array',
        'items': { type: 'string' },
        'minItems': 1,
        'oneOf': [{ type: 'string' }, { type: 'array', minItems: 2 }],
        'x-form-field': 'tags',
      },
    },
    required: ['type', 'name', 'tags'],
    errorMessage: {
      properties: { name: 'Name too short.' },
      required: { name: 'Name is required.' },
    },
  };

  const hooks = {
    customValidate: jest.fn(async () => ['identifier must be lowercase']),
  };

  loadStaticConfig.mockResolvedValueOnce({
    config: {
      requests: {
        system: { folderName: 'registry', schema: 'schemas/t8.json', issueTemplate: 't.yml' },
      },
      hooks: { allowedHosts: ['api.sap.com'] },
    },
    source: 'repo:.github/registry-bot/config.yaml',
    hooks,
    hooksSource: 'repo:.github/registry-bot/config.js',
  });

  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:.github/registry-bot/schemas/t8.json`]: {
        kind: 'file',
        text: JSON.stringify(schemaBad),
      },
      // registry does NOT exist
      [`${owner}/${repo}:registry/sys.one.yaml`]: { kind: 'err', status: 404 },
    },
  });

  const tpl = {
    body: [
      { id: 'identifier', attributes: { label: 'Identifier' }, validations: { required: true } },
      { id: 'tags', attributes: { label: 'Tags' }, validations: { required: true } },
    ],
    _meta: { requestType: 'system', schema: 'schemas/t8.json', root: 'registry', path: 't.yml' },
    title: 'System: Request',
  };

  const formData: any = {
    identifier: 'a', // name will become "a" -> minLength error
    tags: 'one', // => ['one'] -> violates oneOf branch minItems:2 and triggers type(string) noise internally
  };

  const res = await validateRequestIssue(
    context,
    { owner, repo },
    { body: 'x', title: 't' },
    { template: tpl as any, formData }
  );

  expect(hooks.customValidate).toHaveBeenCalledTimes(1);
  expect(res.errors.length).toBeGreaterThan(0);

  // the filter is meant to drop "must be string" noise when value is actually an array and minItems/oneOf exists
  expect(res.errors.join('\n').toLowerCase()).not.toMatch(/be string/);
});

test('falls back to local schema loader when repo does not contain schema', async () => {
  const owner = 'o9';
  const repo = 'r9';

  // repo always 404 for schema
  const { context } = mkContext({
    owner,
    repo,
    files: {},
  });

  // succeed after a couple of attempts in loadSchemaLocal loop
  let n = 0;
  readFile.mockImplementation(async () => {
    n += 1;
    if (n < 3) throw new Error('ENOENT');
    return JSON.stringify({
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      properties: { type: { const: 'system' }, name: { type: 'string' } },
      required: ['type', 'name'],
    });
  });

  const tpl = {
    body: [{ id: 'identifier', attributes: { label: 'Identifier' }, validations: { required: true } }],
    _meta: { requestType: 'system', schema: 'schemas/t9.json', root: 'registry', path: 't.yml' },
  };

  const res = await validateRequestIssue(
    context,
    { owner, repo },
    { body: 'x', title: 't' },
    { template: tpl as any, formData: { identifier: 'sys.one' } as any }
  );

  // may still end up with registry check only (depends on schema + candidate)
  expect(readFile).toHaveBeenCalled();
  expect(res.nsType).toBe('system');
});
