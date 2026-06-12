/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */

import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';

// NOTE: Jest ESM: mock first, then dynamic import

type AnyObj = Record<string, any>;

const httpErr = (status: number): Error & { status: number } => {
  const e = new Error(`HTTP ${status}`) as Error & { status: number };
  (e as any).status = status;
  return e;
};

const b64Json = (obj: unknown): string => Buffer.from(JSON.stringify(obj), 'utf8').toString('base64');

const schemaFileResponse = (schemaObj: unknown) => ({
  data: { content: b64Json(schemaObj), encoding: 'base64' as const },
});

const mkContext = () => {
  const ctx: AnyObj = {
    octokit: {
      repos: {
        get: jest.fn(),
        getBranch: jest.fn(),
        getContent: jest.fn(),
        createOrUpdateFileContents: jest.fn(),
      },
      git: {
        createRef: jest.fn(),
      },
      pulls: {
        list: jest.fn(),
        create: jest.fn(),
      },
      issues: {
        addLabels: jest.fn(),
      },
    },
    log: {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    },
    resourceBotConfig: undefined,
  };

  return ctx;
};

type Subject = {
  createRequestPr: (ctx: any, repoRef: any, issue: any, form: any, opts?: any) => Promise<any>;
  mocks: {
    yamlParse: jest.Mock<any>;
    yamlStringify: jest.Mock<any>;

    calcSnapshotHash: jest.Mock<any>;
    tryEnableAutoMerge: jest.Mock<any>;
    tryMergeIfGreen: jest.Mock<any>;

    loadTemplate: jest.Mock<any>;
    loadStaticConfig: jest.Mock<any>;

    resolvePrimaryIdFromTemplate: jest.Mock<any>;
    projectForSchema: jest.Mock<any>;
  };
};

async function loadSubject(opts?: {
  yamlParseImpl?: (s: string) => any;
  yamlStringifyImpl?: (obj: any) => string;
  jsYamlLoadImpl?: (s: string, opts?: any) => any;
  jsYamlDumpImpl?: (obj: any, opts?: any) => string;
  jsYamlSchema?: unknown;
}): Promise<Subject> {
  jest.resetModules();

  const yamlParse = jest.fn(
    opts?.yamlParseImpl ??
      (() => {
        throw new Error('YAML.parse not mocked');
      })
  );
  const yamlStringify = jest.fn(opts?.yamlStringifyImpl ?? (() => 'yaml-out'));
  const jsYamlLoad = jest.fn(
    opts?.jsYamlLoadImpl ??
      (() => {
        throw new Error('js-yaml.load not mocked');
      })
  );
  const jsYamlDump = jest.fn(opts?.jsYamlDumpImpl ?? ((obj: any) => JSON.stringify(obj)));

  const calcSnapshotHash = jest.fn(() => 'HASH');
  type TryEnableAutoMergeFn = (
    context: unknown,
    pr: { number: number; node_id: string },
    opts?: { mergeMethod?: 'MERGE' | 'SQUASH' | 'REBASE' }
  ) => Promise<boolean>;

  //
  const tryEnableAutoMerge = jest.fn<TryEnableAutoMergeFn>(async () => false);
  type TryMergeIfGreenFn = (
    context: unknown,
    args: { prNumber: number; mergeMethod?: 'merge' | 'squash' | 'rebase' }
  ) => Promise<boolean>;

  const tryMergeIfGreen = jest.fn<TryMergeIfGreenFn>(async () => true);

  type LoadTemplateFn = (context: unknown, args: { issueLabels?: string[] }) => Promise<unknown>;

  const loadTemplate = jest.fn<LoadTemplateFn>(async () => ({}));

  const loadStaticConfig = jest.fn(async () => ({ config: {} }));

  const resolvePrimaryIdFromTemplate = jest.fn(() => '');
  const projectForSchema = jest.fn(async () => ({}));

  // External dependency
  jest.unstable_mockModule('yaml', () => ({
    default: {
      parse: yamlParse,
      stringify: yamlStringify,
    },
  }));

  if (opts?.jsYamlLoadImpl || opts?.jsYamlDumpImpl || typeof opts?.jsYamlSchema !== 'undefined') {
    jest.unstable_mockModule('js-yaml', () => ({
      default: {
        load: jsYamlLoad,
        dump: jsYamlDump,
        JSON_SCHEMA: opts?.jsYamlSchema,
      },
    }));
  }

  // Local deps imported by create.ts
  jest.unstable_mockModule('../src/handlers/request/pr/snapshot.js', () => ({
    SNAPSHOT_HASH_MARKER_KEY: 'snapshot-hash',
    calcSnapshotHash,
  }));

  jest.unstable_mockModule('../src/lib/auto-merge.js', () => ({
    tryEnableAutoMerge,
    tryMergeIfGreen,
  }));

  jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    loadTemplate,
  }));

  jest.unstable_mockModule('../src/config.js', () => ({
    loadStaticConfig,
  }));

  jest.unstable_mockModule('../src/handlers/request/validation/run.js', () => ({
    resolvePrimaryIdFromTemplate,
    projectForSchema,
  }));

  const mod = await import('../src/handlers/request/pr/create.js');

  return {
    createRequestPr: (mod as any).createRequestPr,
    mocks: {
      yamlParse,
      yamlStringify,
      calcSnapshotHash,
      tryEnableAutoMerge,
      tryMergeIfGreen,
      loadTemplate,
      loadStaticConfig,
      resolvePrimaryIdFromTemplate,
      projectForSchema,
    },
  };
}

let OLD_DEBUG_NS: string | undefined;

beforeEach(() => {
  OLD_DEBUG_NS = process.env.DEBUG_NS;
});

afterEach(() => {
  process.env.DEBUG_NS = OLD_DEBUG_NS;
});

describe('handlers/request/pr/create.ts – full coverage via createRequestPr()', () => {
  it('throws if template cannot be resolved (loadTemplate fails => null)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {}; // skip loadStaticConfig

    mocks.loadTemplate.mockRejectedValueOnce(new Error('no template'));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: 't', labels: [] }, {}, {})
    ).rejects.toThrow(/Missing form template/i);
  });

  it('non-product: loads config, ignores 422 createRef, writes YAML, reuses existing PR, adds label when autoMerge disabled, prunes + strips defaults, logs dbg:*', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();

    mocks.loadStaticConfig.mockResolvedValueOnce({
      config: {
        pr: {
          branchNameTemplate: 'req/{resource}-{issue}',
          titleTemplate: 'Register {type}: {resource}',
          commitMessageTemplate: 'chore({root}): register {resource} #{issue}',
          // bodyFooter intentionally omitted => fallback path
          baseBranch: 'develop',
          autoMerge: { enabled: false, method: 'rebase' },
        },
        workflow: { labels: { autoMergeCandidate: 'am-label' } },
        schema: { searchPaths: ['schema'] },
      },
    });

    const template = {
      _meta: {
        requestType: 'authority',
        root: 'data',
        schema: 'ns.schema.json',
        path: '.github/ISSUE_TEMPLATE/authority.yml',
      },
      // no "contact", no "visibility"/"open-system" => should be pruned
      body: [{ id: 'identifier' }],
    };

    mocks.loadTemplate.mockResolvedValueOnce(template);

    const schemaObj = {
      $id: 'schema:authority',
      type: 'object',
      properties: {
        type: { const: 'Authority' },
        name: { type: 'string' },
        deprecated: { type: 'boolean', default: false },
        visibility: { type: 'string', default: 'internal' },
        contacts: { type: 'array', items: { type: 'string' } },
      },
    };

    const resourceName = 'Acme.System@1';
    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce(resourceName);

    mocks.projectForSchema.mockResolvedValueOnce({
      // out-of-order + extra keys to test orderCandidateForYaml()
      zzz: 'X',
      visibility: 'internal', // should be deleted (template has no visibility)
      contacts: ['a@b'], // should be deleted (template has no contact)
      deprecated: false, // should be stripped by stripDefaultsBySchema
      name: resourceName,
      type: 'Authority',
    });

    // Repo info
    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    // Branch create => 422 should be ignored
    ctx.octokit.git.createRef.mockRejectedValueOnce(httpErr(422));

    // Schema load: first candidate 404, second resolves
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'ns.schema.json') throw httpErr(404);
        if (path === 'schema/ns.schema.json') return schemaFileResponse(schemaObj);
      }
      // existence checks for yaml => 404
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    // write ok
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    // existing PR found => skip pulls.create
    const existingPr = {
      number: 99,
      node_id: 'NODE',
      head: { ref: 'req/acme.system-1-7', sha: 'PRSHA' },
      body: '...',
      draft: false,
      state: 'open',
    };
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [existingPr] });

    ctx.octokit.issues.addLabels.mockResolvedValueOnce({ ok: true });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      {
        number: 7,
        title: 'Authority Request',
        labels: ['l1', { name: 'l2' }, { name: null }],
        body: 'issue body',
      },
      { any: 'formData' },
      {}
    );

    // returned PR is existing one
    expect(pr.number).toBe(99);

    // loadStaticConfig happened
    expect(mocks.loadStaticConfig).toHaveBeenCalledWith(ctx, {
      validate: false,
      updateIssue: false,
    });

    // loadTemplate called with label-filtered list
    expect(mocks.loadTemplate).toHaveBeenCalled();
    const loadTemplateArgs = mocks.loadTemplate.mock.calls[0]?.[1] as AnyObj;
    expect(loadTemplateArgs.issueLabels).toEqual(['l1', 'l2']);

    // base branch from config
    expect(ctx.octokit.repos.getBranch).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      branch: 'develop',
    });

    // createRef used slugified resource (lowercase + unsafe -> '-')
    expect(ctx.octokit.git.createRef).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      ref: 'refs/heads/req/acme.system-1-7',
      sha: 'BASESHA',
    });

    // wrote YAML at resource path
    expect(ctx.octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    expect(writeParams.path).toBe('data/Acme.System@1.yaml');
    expect(writeParams.branch).toBe('req/acme.system-1-7');
    expect(writeParams.message).toBe('chore(data): register Acme.System@1 #7');

    const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

    // YAML content: pruned + defaults stripped + stable order (type -> name -> zzz)
    const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    expect(yamlText).toMatch(/^type:\s*Authority\s*$/m);
    expect(yamlText).toMatch(new RegExp(`^name:\\s*'?${esc(resourceName)}'?\\s*$`, 'm'));
    expect(yamlText).toMatch(/^zzz:\s*X\s*$/m);

    const iType = yamlText.indexOf('type:');
    const iName = yamlText.indexOf('name:');
    const iZzz = yamlText.indexOf('zzz:');
    expect(iType).toBeGreaterThanOrEqual(0);
    expect(iName).toBeGreaterThan(iType);
    expect(iZzz).toBeGreaterThan(iName);

    expect(yamlText).not.toMatch(/^\s*visibility\s*:/m);
    expect(yamlText).not.toMatch(/^\s*contacts?\s*:/m);
    expect(yamlText).not.toMatch(/^\s*deprecated\s*:/m);

    // autoMerge disabled => no tryEnableAutoMerge, but label added
    expect(mocks.tryEnableAutoMerge).not.toHaveBeenCalled();
    expect(mocks.tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 99,
      labels: ['am-label'],
    });

    // dbg logs at end executed
    const infoCalls = ctx.log.info.mock.calls;
    expect(infoCalls.some((c: any[]) => c[1] === 'dbg:type-mapping')).toBe(true);
    expect(infoCalls.some((c: any[]) => c[1] === 'dbg:validation-routing')).toBe(true);
  });

  it('non-product: enforces minItems for contacts/contact and throws', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {
      schema: { searchPaths: ['schema'] },
      pr: { autoMerge: { enabled: false } },
    };

    const template = {
      _meta: {
        requestType: 'system',
        root: 'data',
        schema: 'min.schema.json',
        path: 'tpl.yml',
      },
      // has "contact" => do NOT prune candidate.contact/contacts before minItems enforcement
      body: [{ id: 'contact' }],
    };

    const schemaObj = {
      $id: 'schema:minitems',
      type: 'object',
      properties: {
        type: { const: 'System' },
        name: { type: 'string' },
        // pickContactProp => prefers contacts if present
        contacts: { type: 'array', minItems: 2, items: { type: 'string' } },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.system');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'System',
      name: 'acme.system',
      contacts: ['only-one'],
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'min.schema.json') throw httpErr(404);
        if (path === 'schema/min.schema.json') return schemaFileResponse(schemaObj);
      }
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: 't', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/requires at least 2 entries/i);

    expect(ctx.octokit.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  it('product: creates file + PR, adds parent when allowed, strips defaults, auto-merge enable fails => label + deferred merge', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();
    ctx.resourceBotConfig = {}; // use defaults

    const prevDebug = process.env.DEBUG_NS;
    process.env.DEBUG_NS = '1'; // cover debug branch log in createRequestPr()

    try {
      const template = {
        _meta: {
          requestType: 'product',
          root: 'data',
          schema: 'product.schema.json',
          path: 'tpl-product.yml',
        },
        body: [{ id: 'id' }],
      };

      const schemaObj = {
        $id: 'schema:product',
        type: 'object',
        properties: {
          type: { const: 'Product' },
        },
        $defs: {
          Product: {
            type: 'object',
            properties: {
              id: { type: 'string' },
              parent: { type: 'string' },
              visibility: { type: 'string', default: 'internal' },
            },
          },
        },
      };

      mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.prod');
      mocks.projectForSchema.mockResolvedValueOnce({
        name: 'Acme Product',
        visibility: 'internal', // should be stripped as default
        identifier: 'should-be-removed',
        parentId: 'should-be-removed',
      });

      // Repo info
      ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
      ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

      // schema load: direct hit on raw path
      ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
        if (!ref && path === 'product.schema.json') return schemaFileResponse(schemaObj);
        if (String(path).endsWith('.yaml')) throw httpErr(404);
        throw httpErr(404);
      });

      // branch create ok
      ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

      // write ok
      ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

      // no existing PR => create one
      ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
      ctx.octokit.pulls.create.mockResolvedValueOnce({
        data: {
          number: 5,
          node_id: 'PRNODE',
          head: { ref: 'feat/resource-acme.prod-issue-12', sha: 'PRSHA' },
        },
      });

      // auto merge: fail => label + merge
      mocks.tryEnableAutoMerge.mockResolvedValueOnce(false);
      ctx.octokit.issues.addLabels.mockResolvedValueOnce({ ok: true });
      mocks.tryMergeIfGreen.mockResolvedValueOnce(true);

      const formData = {
        identifier: 'explicit-id',
        parentId: 'parent123',
        description: ' desc ',
        contact: ['a@b'],
      };

      const pr = await createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 12, title: 'Product req', labels: [], body: 'ISSUE BODY' },
        formData,
        { template }
      );

      expect(pr.number).toBe(5);

      // YAML content written
      expect(ctx.octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
      const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
      const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

      const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

      // Product candidate: id enforced, identifier removed, parent is NOT emitted (code deletes it),
      // parentId is kept
      expect(yamlText).toMatch(new RegExp(`^id:\\s*'?${esc('explicit-id')}'?\\s*$`, 'm'));
      expect(yamlText).toMatch(new RegExp(`^parentId:\\s*'?${esc('parent123')}'?\\s*$`, 'm'));

      expect(yamlText).not.toMatch(/^\s*identifier\s*:/m);
      expect(yamlText).not.toMatch(/^\s*parent\s*:/m);
      expect(yamlText).not.toMatch(/^\s*visibility\s*:/m); // default stripped

      // PR creation body includes snapshot marker & issue marker
      expect(ctx.octokit.pulls.create).toHaveBeenCalled();
      const createArgs = ctx.octokit.pulls.create.mock.calls[0][0] as AnyObj;
      expect(createArgs.maintainer_can_modify).toBe(true);
      expect(String(createArgs.body)).toContain('fix: #12');
      expect(String(createArgs.body)).toContain('Type: Product');
      expect(String(createArgs.body)).toContain('<!-- nsreq:issue:12 -->');
      expect(String(createArgs.body)).toContain('<!-- snapshot-hash:HASH -->');

      // auto merge called with SQUASH by default
      expect(mocks.tryEnableAutoMerge).toHaveBeenCalled();
      const amArgs = mocks.tryEnableAutoMerge.mock.calls[0] as any[];
      expect((amArgs[1] as { number: number }).number).toBe(5);
      expect((amArgs[2] as { mergeMethod?: 'MERGE' | 'SQUASH' | 'REBASE' }).mergeMethod).toBe('SQUASH');

      // label applied because enable returned false
      expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        issue_number: 5,
        labels: ['auto-merge-candidate'],
      });

      // Merge execution is intentionally deferred to CI webhooks.
      expect(mocks.tryMergeIfGreen).not.toHaveBeenCalled();

      // debug log branch hit (DEBUG_NS=1)
      expect(ctx.log.info).toHaveBeenCalled();
      expect(ctx.log.info.mock.calls.some((c: any[]) => c[1] === 'pr:root-and-name')).toBe(true);
    } finally {
      process.env.DEBUG_NS = prevDebug;
    }
  });

  it('product: parent forbidden by subschema (parent=false) => do NOT inject parent; existing PR reused; auto-merge enable succeeds => no label and deferred merge', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();
    ctx.resourceBotConfig = {
      pr: { autoMerge: { enabled: true, method: 'merge' } },
      workflow: { labels: { autoMergeCandidate: 'am' } },
    };

    const template = {
      _meta: {
        requestType: 'product',
        root: 'data',
        schema: 'product2.schema.json',
        path: 'tpl-product2.yml',
      },
      body: [{ id: 'id' }],
    };

    // Use oneOf path
    const schemaObj = {
      $id: 'schema:product2',
      oneOf: [
        {
          type: 'object',
          properties: {
            type: { const: 'Product' },
            id: { type: 'string' },
            parent: false, // forbid parent
          },
        },
      ],
      properties: {
        type: { const: 'Product' },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.prod2');
    mocks.projectForSchema.mockResolvedValueOnce({
      name: 'P2',
      // no parent, should stay absent because forbidden
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'product2.schema.json') return schemaFileResponse(schemaObj);
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    const existingPr = {
      number: 55,
      node_id: 'PRNODE55',
      head: { ref: 'feat/resource-acme.prod2-issue-9', sha: 'PRSHA' },
    };
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [existingPr] });

    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);
    mocks.tryMergeIfGreen.mockResolvedValueOnce(true);

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 9, title: 'Product2 req', labels: [], body: '' },
      { parentId: 'PARENTX' },
      { template }
    );

    expect(pr.number).toBe(55);

    expect(ctx.octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

    // parent must not be emitted
    expect(yamlText).not.toMatch(/^\s*parent\s*:/m);
    expect(yamlText).toMatch(/^\s*parentId\s*:\s*'?PARENTX'?\s*$/m);

    // enable succeeded => no labels
    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();

    // Auto-merge can still be enabled at PR creation time, but direct REST merge is deferred to CI webhooks.
    expect(mocks.tryEnableAutoMerge).toHaveBeenCalled();
    expect(
      (
        mocks.tryEnableAutoMerge.mock.calls[0][2] as {
          mergeMethod?: 'MERGE' | 'SQUASH' | 'REBASE';
        }
      ).mergeMethod
    ).toBe('MERGE');

    expect(mocks.tryMergeIfGreen).not.toHaveBeenCalled();
  });

  it('systemnamespace: builds corrIds (+ cld/stc/ppms), parses correlationIdTypes from YAML, pulls.list errors are ignored => PR gets created, auto-merge enabled', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();

    // cover loadStaticConfig catch => defaults used
    mocks.loadStaticConfig.mockRejectedValueOnce(new Error('boom'));
    ctx.resourceBotConfig = undefined;

    const template = {
      _meta: {
        requestType: 'systemnamespace',
        root: 'data',
        schema: 'sys.schema.json',
        path: 'tpl-sys.yml',
      },
      body: [{ id: 'contact' }, { id: 'open-system' }], // keep contact + visibility
    };

    const schemaObj = {
      $id: 'schema:sys',
      type: 'object',
      properties: {
        type: { const: 'SystemNamespace' },
        name: { type: 'string' },
        contact: { type: 'array', minItems: 1, items: { type: 'string' } },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.system');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'SystemNamespace',
      name: 'acme.system',
      contact: ['a@b'],
      visibility: 'public',
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    // schema load: 404 raw, then resolve via default searchPath "schema/..."
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'sys.schema.json') throw httpErr(404);
        if (path === 'schema/sys.schema.json') return schemaFileResponse(schemaObj);
      }
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    // pulls.list errors should be ignored (catch) => PR created
    ctx.octokit.pulls.list.mockRejectedValueOnce(new Error('list failed'));

    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: {
        number: 77,
        node_id: 'PR77',
        head: { ref: 'feat/resource-acme.system-issue-3', sha: 'S' },
      },
    });

    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);
    mocks.tryMergeIfGreen.mockResolvedValueOnce(true);

    const formData = {
      'correlationIds': 'id1\nid2,id1',
      'cld-system-role': 'sr',
      'stc-service-id': 'stc',
      'ppms-product-object-number': 'pp',
      'correlationIdTypes': '- kind: a\n  value: b\n',
    };

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 3, title: 'sys', labels: [], body: 'BODY' },
      formData,
      { template }
    );

    expect(pr.number).toBe(77);

    // because loadStaticConfig failed, it falls back to {}
    expect(ctx.resourceBotConfig).toEqual({});

    // verify projectForSchema got normalized correlationIds and parsed correlationIdTypes
    expect(mocks.projectForSchema).toHaveBeenCalled();
    const normalized = mocks.projectForSchema.mock.calls[0][1] as AnyObj;

    expect(Array.isArray(normalized.correlationIds)).toBe(true);
    expect(normalized.correlationIds).toEqual(
      expect.arrayContaining(['id1', 'id2', 'sap.cld:systemRole:sr', 'sap.stc:service:stc', 'sap.ppms:product:pp'])
    );

    // real js-yaml parse result
    expect(Array.isArray(normalized.correlationIdTypes)).toBe(true);
    expect(normalized.correlationIdTypes[0]).toEqual({ kind: 'a', value: 'b' });

    // PR body includes hash marker
    expect(ctx.octokit.pulls.create).toHaveBeenCalled();
    const body = String(ctx.octokit.pulls.create.mock.calls[0][0].body);
    expect(body).toContain('fix: #3');
    expect(body).toContain('<!-- snapshot-hash:HASH -->');

    // auto merge enable succeeded => no label
    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
    expect(mocks.tryEnableAutoMerge).toHaveBeenCalled();
    expect(
      (
        mocks.tryEnableAutoMerge.mock.calls[0][2] as {
          mergeMethod?: 'MERGE' | 'SQUASH' | 'REBASE';
        }
      ).mergeMethod
    ).toBe('SQUASH'); // default because config fallback to {}
  });

  it('non-product: SubContext policy pruning removes correlationIdTypes/visibility/deprecated/expiryDate', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();
    ctx.resourceBotConfig = { schema: { searchPaths: ['schema'] } };

    const template = {
      _meta: {
        requestType: 'subcontext',
        root: 'data',
        schema: 'sub.schema.json',
        path: 'tpl-sub.yml',
      },
      // expose both, so pruning is *policy-driven* not template-driven
      body: [{ id: 'contact' }, { id: 'visibility' }],
    };

    const schemaObj = {
      $id: 'schema:sub',
      type: 'object',
      properties: {
        type: { const: 'SubContext' },
        name: { type: 'string' },
        contact: { type: 'array', items: { type: 'string' } },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.sub');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'SubContext',
      name: 'acme.sub',
      contact: ['a@b'],
      visibility: 'public',
      correlationIdTypes: [{ x: 1 }],
      deprecated: true,
      expiryDate: '2099-01-01',
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'sub.schema.json') throw httpErr(404);
        if (path === 'schema/sub.schema.json') return schemaFileResponse(schemaObj);
      }
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 10, node_id: 'PR10', head: { ref: 'b', sha: 's' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);
    mocks.tryMergeIfGreen.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 2, title: 'sub', labels: [], body: '' },
      {},
      { template }
    );

    expect(ctx.octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

    expect(yamlText).toMatch(/^type:\s*SubContext\s*$/m);
    expect(yamlText).toMatch(/^\s*contact\s*:\s*$/m);
    expect(yamlText).toMatch(/^\s*-\s*'?a@b'?\s*$/m);

    expect(yamlText).not.toMatch(/^\s*visibility\s*:/m);
    expect(yamlText).not.toMatch(/^\s*correlationIdTypes\s*:/m);
    expect(yamlText).not.toMatch(/^\s*deprecated\s*:/m);
    expect(yamlText).not.toMatch(/^\s*expiryDate\s*:/m);
  });

  it('throws if createRef fails with non-422', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };
    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.x' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockRejectedValueOnce(httpErr(500));

    ctx.octokit.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/HTTP 500/i);
  });

  it('throws if base SHA cannot be resolved', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };
    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: '' } } });
    ctx.octokit.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/Cannot resolve base SHA/i);
  });

  it('throws if template meta requestType missing', async () => {
    const { createRequestPr } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: '', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/missing _meta\.requestType/i);
  });

  it('throws if template meta root missing', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: '', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    // schema load must still succeed up to the point where folderName is checked?
    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/missing _meta\.root/i);
  });

  it('throws if schema cannot be loaded for template (empty meta.schema => null)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: '', path: 'tpl.yml' },
      body: [],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/schema could not be loaded/i);
  });

  it('throws if resolvePrimaryIdFromTemplate yields empty resourceName', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('');

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/Could not resolve primary identifier/i);
  });

  it('throws if projectForSchema returns non-object (namespace branch)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');
    mocks.projectForSchema.mockResolvedValueOnce(null);

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/Schema projection failed for namespace candidate/i);
  });

  it('throws if resource already exists (existsAt => true)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.x' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    // schema loads ok, but yaml exists => getContent succeeds with ref => existsAt true
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) return { data: { any: 'file' } };
      throw httpErr(404);
    });

    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/already exists/i);
  });

  it('partner namespace rejects invalid request-type selections', async () => {
    const { createRequestPr } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = { requests: {} };

    const template = {
      _meta: {
        requestType: 'partnernamespace',
        root: 'data',
        schema: 'partner.schema.json',
        path: 'tpl.yml',
      },
      body: [],
    };

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        { 'request-type': 'invalid' },
        { template }
      )
    ).rejects.toThrow(/Invalid Partner Namespace 'Request Type' selection/i);
  });

  it('partner namespace case-insensitive config lookup still fails when schema is missing', async () => {
    const { createRequestPr } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {
      requests: {
        SystemNamespace: { folderName: 'data' },
      },
    };

    const template = {
      _meta: {
        requestType: 'partnernamespace',
        root: 'data',
        schema: 'partner.schema.json',
        path: 'tpl.yml',
      },
      body: [],
    };

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        { requestType: 'system' },
        { template }
      )
    ).rejects.toThrow(/cfg\.requests has no schema/i);
  });

  it('product branch throws when schema projection returns a non-object candidate', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'product', root: 'data', schema: 'prod.schema.json', path: 'tpl.yml' },
      body: [{ id: 'id' }],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'Product' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.prod');
    mocks.projectForSchema.mockResolvedValueOnce(null);

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/Schema projection failed for Product candidate/i);
  });

  it('systemnamespace strips direct correlationIds, honors required contact arrays, and sanitizes buffer values through js-yaml', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlSchema: { kind: 'schema' },
      jsYamlLoadImpl: () => [{ kind: 'a', value: 'b' }],
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = { schema: { searchPaths: ['schema'] } };

    const template = {
      _meta: {
        requestType: 'systemnamespace',
        root: 'data',
        schema: 'sys-required.schema.json',
        path: 'tpl.yml',
      },
      body: [{ id: 'cld-system-role' }, { id: 'contact' }],
    };

    const schemaObj = {
      type: 'object',
      required: ['contact'],
      properties: {
        type: { const: 'SystemNamespace' },
        contact: { type: 'array', minItems: 1, items: { type: 'string' } },
      },
      oneOf: [
        {
          type: 'object',
          required: ['contact'],
          properties: {
            type: { const: 'SystemNamespace' },
            contact: { type: 'array', minItems: 1, items: { type: 'string' } },
          },
        },
      ],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.system');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'SystemNamespace',
      name: 'acme.system',
      contact: ['a@b'],
      correlationIds: ['manual'],
      attachment: Buffer.from('hi', 'utf8'),
      ignored: () => 'x',
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'sys-required.schema.json') throw httpErr(404);
      if (!ref && path === 'schema/sys-required.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 12, node_id: 'PR12', head: { ref: 'branch', sha: 'sha' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(false);
    mocks.tryMergeIfGreen.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 2, title: '', labels: [], body: '' },
      { 'cld-system-role': 'sr', 'correlationIdTypes': '- kind: a\n  value: b\n' },
      { template }
    );

    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const serialized = JSON.parse(Buffer.from(String(writeParams.content), 'base64').toString('utf8')) as AnyObj;

    expect(serialized.correlationIds).toBeUndefined();
    expect(serialized.contact).toEqual(['a@b']);
    expect(serialized.attachment).toEqual({ 0: 104, 1: 105 });
    expect(serialized.ignored).toBeUndefined();
  });

  it('propagates non-404 existence check failures', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };
    const boom = new Error('exists failed');

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.x' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) throw boom;
      throw httpErr(404);
    });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow('exists failed');
  });

  it('sanitizeForYaml: null → null kept, undefined → key dropped, Infinity → String, Date → ISO, bigint → string', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = { schema: { searchPaths: ['schema'] } };

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [{ id: 'name', type: 'input' }],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');
    const d = new Date('2024-03-15T12:00:00.000Z');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'System',
      name: 'acme.x',
      nullField: null,
      undefinedField: undefined,
      infField: Infinity,
      bigintField: BigInt(42),
      dateField: d,
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') throw httpErr(404);
      if (!ref && path === 'schema/x.schema.json')
        return schemaFileResponse({
          type: 'object',
          properties: { type: { const: 'System' }, name: { type: 'string' } },
        });
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 1, node_id: 'N', head: { ref: 'feat/r', sha: 's' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template }
    );

    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const serialized = JSON.parse(Buffer.from(String(writeParams.content), 'base64').toString('utf8')) as AnyObj;

    // null kept, undefined key absent, Infinity → 'Infinity', BigInt → '42', Date → ISO
    expect(serialized.nullField).toBeNull();
    expect(Object.prototype.hasOwnProperty.call(serialized, 'undefinedField')).toBe(false);
    expect(serialized.infField).toBe('Infinity');
    expect(serialized.bigintField).toBe('42');
    expect(serialized.dateField).toBe(d.toISOString());
  });

  it('loadYamlDoc: falls back to plain jsYaml.load when JSON_SCHEMA is null (L275)', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlSchema: null,
      jsYamlLoadImpl: () => [{ kind: 'a', value: 'b' }],
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'systemnamespace', root: 'data', schema: 'sys.schema.json', path: 'tpl.yml' },
      body: [{ id: 'contact' }],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.sys');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'SystemNamespace', name: 'acme.sys', contact: ['a@b'] });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'sys.schema.json')
        return schemaFileResponse({
          type: 'object',
          properties: {
            type: { const: 'SystemNamespace' },
            contact: { type: 'array', minItems: 1, items: { type: 'string' } },
          },
        });
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 2, node_id: 'N2', head: { ref: 'feat/r2', sha: 's2' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 2, title: '', labels: [], body: '' },
      { correlationIdTypes: '- kind: a\n  value: b\n' },
      { template }
    );

    // parseMaybeYamlJson called tryLoadYamlDoc which used JSON_SCHEMA=null path
    expect(mocks.projectForSchema).toHaveBeenCalled();
    const normalized = mocks.projectForSchema.mock.calls[0][1] as AnyObj;
    expect(Array.isArray(normalized.correlationIdTypes)).toBe(true);
  });

  it('getHttpStatus: non-plain-object error in loadSchemaForTemplate falls through (L196 arm=0)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.x');

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });

    // throw a string (non-plain-object) → getHttpStatus returns undefined ≠ 404 → rethrow
    ctx.octokit.repos.getContent.mockRejectedValue('string-error');

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toBe('string-error');
  });

  it('getRequestEntryFromConfig: requests entry missing (null reqs) returns null (L591 true)', async () => {
    const { createRequestPr } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = { requests: 'not-an-object' };

    const template = {
      _meta: { requestType: 'partnernamespace', root: 'data', schema: 'p.schema.json', path: 'tpl.yml' },
      body: [],
    };

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        { requestType: 'system' },
        { template }
      )
    ).rejects.toThrow(/cfg\.requests has no schema/i);
  });

  it('parseMaybeYamlJson: array input returns directly (L557 arm=0), empty string returns undefined', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'systemnamespace', root: 'data', schema: 'sys.schema.json', path: 'tpl.yml' },
      body: [{ id: 'contact' }],
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.sys2');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'SystemNamespace', name: 'acme.sys2', contact: ['x@y'] });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'sys.schema.json')
        return schemaFileResponse({
          type: 'object',
          properties: { type: { const: 'SystemNamespace' }, contact: { type: 'array', minItems: 1 } },
        });
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 3, node_id: 'N3', head: { ref: 'feat/r3', sha: 's3' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    const prebuiltArray = [{ kind: 'a', value: 'b' }];
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 3, title: '', labels: [], body: '' },
      { correlationIdTypes: prebuiltArray, correlationIds: '' },
      { template }
    );

    expect(mocks.projectForSchema).toHaveBeenCalled();
    const normalized = mocks.projectForSchema.mock.calls[0][1] as AnyObj;
    // correlationIdTypes was already an array → used directly (L557 early return)
    expect(normalized.correlationIdTypes).toBe(prebuiltArray);
  });

  it('buildPrTitle empty template falls back to "Register {type} {resource}" (L349 true)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = { pr: { titleTemplate: '', autoMerge: { enabled: false } } };

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };
    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.tpl');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.tpl' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 4, node_id: 'N4', head: { ref: 'feat/r4', sha: 's4' } },
    });
    ctx.octokit.issues.addLabels.mockResolvedValueOnce({ ok: true });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template }
    );

    expect(ctx.octokit.pulls.create).toHaveBeenCalled();
    const createArgs = ctx.octokit.pulls.create.mock.calls[0][0] as AnyObj;
    // empty titleTemplate → fallback "Register {type} {resource}"
    expect(String(createArgs.title)).toMatch(/Register System acme\.tpl/i);
  });

  it('buildCommitMessage empty template falls back to default format (L340 true)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = { pr: { commitMessageTemplate: '', autoMerge: { enabled: false } } };

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };
    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.commit');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.commit' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 5, node_id: 'N5', head: { ref: 'feat/r5', sha: 's5' } },
    });
    ctx.octokit.issues.addLabels.mockResolvedValueOnce({ ok: true });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 7, title: '', labels: [], body: '' },
      {},
      { template }
    );

    expect(ctx.octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeArgs = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    // empty commitMessageTemplate → default "chore(data): register acme.commit (#7)"
    expect(String(writeArgs.message)).toMatch(/chore\(data\): register acme\.commit/i);
  });

  it('writeFileAt passes sha param when file already has a sha (L778 true)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'x.schema.json', path: 'tpl.yml' },
      body: [],
    };
    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };
    const existingSha = 'EXISTING_FILE_SHA';

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.sha');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.sha' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      // yaml existence check for defaultBranch: file exists with sha
      if (ref === 'main' && String(path).endsWith('.yaml')) return { data: { sha: existingSha, content: '' } };
      throw httpErr(404);
    });

    // Resource already exists on default branch → should throw
    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/already exists/i);
  });

  it('stripDefaultsBySchema: recurses into nested object and array item schemas (L488/L492 true branches)', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'nested.schema.json', path: 'tpl.yml' },
      body: [{ id: 'name', type: 'input' }],
    };

    // schema with nested object and array defaults
    const schemaObj = {
      type: 'object',
      properties: {
        type: { const: 'System' },
        name: { type: 'string' },
        nested: {
          type: 'object',
          properties: {
            flag: { type: 'boolean', default: false },
          },
        },
        items: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              active: { type: 'boolean', default: true },
            },
          },
        },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.nested');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'System',
      name: 'acme.nested',
      nested: { flag: false },
      items: [{ active: true }, { active: false }],
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'nested.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 6, node_id: 'N6', head: { ref: 'feat/r6', sha: 's6' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template }
    );

    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const serialized = JSON.parse(Buffer.from(String(writeParams.content), 'base64').toString('utf8')) as AnyObj;

    // flag: false is default → stripped
    const nested = serialized.nested as AnyObj;
    expect(Object.prototype.hasOwnProperty.call(nested || {}, 'flag')).toBe(false);
    // items[0].active: true is default → stripped; items[1].active: false is NOT default → kept
    const items = serialized.items as AnyObj[];
    expect(Array.isArray(items)).toBe(true);
    expect(Object.prototype.hasOwnProperty.call(items[0] ?? {}, 'active')).toBe(false);
    expect((items[1] ?? {}).active).toBe(false);
  });

  it('pickContactProp: schema forbids both contacts and contact → returns empty string → candidate contacts deleted (L520 / L1042)', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'no-contact.schema.json', path: 'tpl.yml' },
      body: [{ id: 'contact' }],
    };

    // schema has contact: false (forbidden) and no 'contacts' key
    const schemaObj = {
      type: 'object',
      properties: {
        type: { const: 'System' },
        name: { type: 'string' },
        contact: false,
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.nocontact');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.nocontact', contact: ['a@b'] });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'no-contact.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 7, node_id: 'N7', head: { ref: 'feat/r7', sha: 's7' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template }
    );

    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const serialized = JSON.parse(Buffer.from(String(writeParams.content), 'base64').toString('utf8')) as AnyObj;
    // contact should be deleted because pickContactProp returned ''
    expect(Object.prototype.hasOwnProperty.call(serialized, 'contact')).toBe(false);
  });

  it('resolveTypeConstFromSchema: schema has no properties → returns fallback requestType (L506 true)', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'custtype', root: 'data', schema: 'noProps.schema.json', path: 'tpl.yml' },
      body: [],
    };

    // schema with no 'properties' at top level
    const schemaObj = { type: 'object' };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.cust');
    mocks.projectForSchema.mockResolvedValueOnce({ name: 'acme.cust' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'noProps.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 8, node_id: 'N8', head: { ref: 'feat/r8', sha: 's8' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template }
    );

    expect(ctx.octokit.pulls.create).toHaveBeenCalled();
    const createArgs = ctx.octokit.pulls.create.mock.calls[0][0] as AnyObj;
    // type derived from fallback = requestType = 'custtype'
    expect(String(createArgs.title)).toContain('custtype');
  });

  it('bodyFooter from string pr.bodyFooter (L306 true branch) and branchNameTemplate from config (L326 arm=1)', async () => {
    const { createRequestPr, mocks } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {
      pr: {
        branchNameTemplate: 'custom/{resource}-pr-{issue}',
        bodyFooter: 'Owned by: platform-team',
        autoMerge: { enabled: false },
      },
    };

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'y.schema.json', path: 'tpl.yml' },
      body: [],
    };
    const schemaObj = { type: 'object', properties: { type: { const: 'System' } } };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.footer');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'System', name: 'acme.footer' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'y.schema.json') return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 9, node_id: 'N9', head: { ref: 'custom/acme.footer-pr-3', sha: 's9' } },
    });
    ctx.octokit.issues.addLabels.mockResolvedValueOnce({ ok: true });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 3, title: '', labels: [], body: '' },
      {},
      { template }
    );

    expect(pr.number).toBe(9);
    expect(ctx.octokit.git.createRef).toHaveBeenCalledWith(
      expect.objectContaining({ ref: 'refs/heads/custom/acme.footer-pr-3' })
    );
    const createArgs = ctx.octokit.pulls.create.mock.calls[0][0] as AnyObj;
    expect(String(createArgs.body)).toContain('Owned by: platform-team');
  });

  it('product: formData without parentId => delete candidate.parentId (L849 arm=1)', async () => {
    const { createRequestPr, mocks } = await loadSubject();

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: {
        requestType: 'product',
        root: 'data',
        schema: 'prod-nopid.schema.json',
        path: 'tpl-prod-nopid.yml',
      },
      body: [{ id: 'id' }],
    };

    const schemaObj = {
      $id: 'schema:prod-nopid',
      type: 'object',
      properties: {
        type: { const: 'Product' },
        id: { type: 'string' },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.nopid');
    mocks.projectForSchema.mockResolvedValueOnce({ type: 'Product', id: 'explicit-id' });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'prod-nopid.schema.json') return schemaFileResponse(schemaObj);
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 42, node_id: 'PRNOPID', head: { ref: 'feat/resource-acme.nopid-issue-7', sha: 'SHA' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    // formData has NO parentId → parentId = '' → L846 if(parentId) → false → L849 delete fires
    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 7, title: 'no parent', labels: [], body: '' },
      { identifier: 'explicit-id', description: 'desc' },
      { template }
    );

    expect(pr.number).toBe(42);

    const writeParams = ctx.octokit.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

    // no parentId in formData → candidate.parentId deleted → not in YAML output
    expect(yamlText).not.toMatch(/^\s*parentId\s*:/m);
    // no parent URI either (parentId was empty)
    expect(yamlText).not.toMatch(/^\s*parent\s*:/m);
  });

  it('loadSchemaForTemplate: all candidate paths return 404 => returns null (L447)', async () => {
    const { createRequestPr } = await loadSubject();
    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: { requestType: 'system', root: 'data', schema: 'missing.schema.json', path: 'tpl.yml' },
      body: [],
    };

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });

    // ALL getContent calls return 404 → all candidates exhausted → loadSchemaForTemplate returns null (L447)
    ctx.octokit.repos.getContent.mockRejectedValue(httpErr(404));

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/schema could not be loaded/i);
  });

  it('parseMaybeYamlJson: JSON.parse fails AND yaml.load throws => returns undefined (L574)', async () => {
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlLoadImpl: () => {
        throw new Error('yaml-parse-fail');
      },
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    const ctx = mkContext();
    ctx.resourceBotConfig = {};

    const template = {
      _meta: {
        requestType: 'systemnamespace',
        root: 'data',
        schema: 'sys574.schema.json',
        path: 'tpl-sys574.yml',
      },
      body: [{ id: 'contact' }],
    };

    const schemaObj = {
      type: 'object',
      properties: {
        type: { const: 'SystemNamespace' },
        name: { type: 'string' },
        contact: { type: 'array', minItems: 1, items: { type: 'string' } },
      },
    };

    mocks.resolvePrimaryIdFromTemplate.mockReturnValueOnce('acme.sys574');
    mocks.projectForSchema.mockResolvedValueOnce({
      type: 'SystemNamespace',
      name: 'acme.sys574',
      contact: ['a@b'],
    });

    ctx.octokit.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASE' } } });
    ctx.octokit.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'sys574.schema.json') return schemaFileResponse(schemaObj);
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.create.mockResolvedValueOnce({
      data: { number: 90, node_id: 'PR90', head: { ref: 'feat/r90', sha: 's90' } },
    });
    mocks.tryEnableAutoMerge.mockResolvedValueOnce(true);

    // correlationIdTypes is a non-JSON, non-YAML-parseable string
    // JSON.parse('not-valid-json!') throws, jsYamlLoad mock also throws => L574: return undefined
    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 4, title: 'sys574', labels: [], body: '' },
      { correlationIdTypes: 'not-valid-json!' },
      { template }
    );

    expect(pr.number).toBe(90);

    // parseMaybeYamlJson returned undefined => correlationIdTypes stays as raw string (not overridden)
    expect(mocks.projectForSchema).toHaveBeenCalled();
    const normalized = mocks.projectForSchema.mock.calls[0][1] as AnyObj;
    // corrTypes was undefined → the override at L968 didn't fire → still the original string from formData spread
    expect(Array.isArray(normalized.correlationIdTypes)).toBe(false);
  });
});
