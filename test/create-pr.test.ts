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
}): Promise<Subject> {
  jest.resetModules();

  const yamlParse = jest.fn(
    opts?.yamlParseImpl ??
      (() => {
        throw new Error('YAML.parse not mocked');
      })
  );
  const yamlStringify = jest.fn(opts?.yamlStringifyImpl ?? (() => 'yaml-out'));

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

  it('product: creates file + PR, adds parent when allowed, strips defaults, auto-merge enable fails => label + merge', async () => {
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

      // tryMergeIfGreen invoked
      expect(mocks.tryMergeIfGreen).toHaveBeenCalled();
      const mig = mocks.tryMergeIfGreen.mock.calls[0][1] as {
        prNumber: number;
        mergeMethod?: 'merge' | 'squash' | 'rebase';
      };
      expect(mig.mergeMethod).toBe('squash');
      expect(mig.prNumber).toBe(5);

      // debug log branch hit (DEBUG_NS=1)
      expect(ctx.log.info).toHaveBeenCalled();
      expect(ctx.log.info.mock.calls.some((c: any[]) => c[1] === 'pr:root-and-name')).toBe(true);
    } finally {
      process.env.DEBUG_NS = prevDebug;
    }
  });

  it('product: parent forbidden by subschema (parent=false) => do NOT inject parent; existing PR reused; auto-merge enable succeeds => no label', async () => {
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

    // merge attempted
    expect(mocks.tryMergeIfGreen).toHaveBeenCalled();
    expect(mocks.tryEnableAutoMerge).toHaveBeenCalled();
    expect(
      (
        mocks.tryEnableAutoMerge.mock.calls[0][2] as {
          mergeMethod?: 'MERGE' | 'SQUASH' | 'REBASE';
        }
      ).mergeMethod
    ).toBe('MERGE');
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
});
