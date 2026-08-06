/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */

import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';

// Pre-import the real reconciliation module BEFORE any jest.resetModules() calls so
// we can forward its exports through jest.unstable_mockModule inside loadSubject.
// create.ts now depends on reconciliation.js, so it must be mocked in the ESM mock
// registry before create.js is dynamically imported.
import * as reconciliationModule from '../src/handlers/request/pr/reconciliation.js';

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
      rest: {
        repos: {
          get: jest.fn(),
          getBranch: jest.fn(),
          getContent: jest.fn(),
          createOrUpdateFileContents: jest.fn(),
          compareCommitsWithBasehead: jest.fn(),
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

  // create.ts imports reconciliation.js — forward the real module so both the
  // JSON-yaml first-describe tests and the real-yaml integration tests use the
  // actual reconciliation functions without Jest trying to re-transform the file.
  jest.unstable_mockModule('../src/handlers/request/pr/reconciliation.js', () => ({
    ...reconciliationModule,
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
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch
      // First call: base branch SHA lookup
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } })
      // Second call: inspectExistingBranch after createRef 422 — head == baseSha => safe empty branch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    // Branch create => 422 triggers branch inspection (resolved as safe empty branch above)
    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));

    // Schema load: first candidate 404, second resolves
    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'ns.schema.json') throw httpErr(404);
        if (path === 'schema/ns.schema.json') return schemaFileResponse(schemaObj);
      }
      // existence checks for yaml => 404
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    // write ok
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    // existing PR found => skip pulls.create
    const existingPr = {
      number: 99,
      node_id: 'NODE',
      head: { ref: 'req/acme.system-1-7', sha: 'PRSHA' },
      body: '...',
      draft: false,
      state: 'open',
    };
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [existingPr] });

    ctx.octokit.rest.issues.addLabels.mockResolvedValueOnce({ ok: true });

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
    expect(ctx.octokit.rest.repos.getBranch).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      branch: 'develop',
    });

    // createRef used slugified resource (lowercase + unsafe -> '-')
    expect(ctx.octokit.rest.git.createRef).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      ref: 'refs/heads/req/acme.system-1-7',
      sha: 'BASESHA',
    });

    // wrote YAML at resource path
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.rest.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
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
    expect(ctx.octokit.rest.issues.addLabels).toHaveBeenCalledWith({
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
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

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
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
      ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
      ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

      // schema load: direct hit on raw path
      ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
        if (!ref && path === 'product.schema.json') return schemaFileResponse(schemaObj);
        if (String(path).endsWith('.yaml')) throw httpErr(404);
        throw httpErr(404);
      });

      // branch create ok
      ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

      // write ok
      ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

      // no existing PR => create one
      ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] });
      ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
        data: {
          number: 5,
          node_id: 'PRNODE',
          head: { ref: 'feat/resource-acme.prod-issue-12', sha: 'PRSHA' },
        },
      });

      // auto merge: fail => label + merge
      mocks.tryEnableAutoMerge.mockResolvedValueOnce(false);
      ctx.octokit.rest.issues.addLabels.mockResolvedValueOnce({ ok: true });
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
      expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalled();
      const writeParams = ctx.octokit.rest.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
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
      expect(ctx.octokit.rest.pulls.create).toHaveBeenCalled();
      const createArgs = ctx.octokit.rest.pulls.create.mock.calls[0][0] as AnyObj;
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
      expect(ctx.octokit.rest.issues.addLabels).toHaveBeenCalledWith({
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'product2.schema.json') return schemaFileResponse(schemaObj);
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    const existingPr = {
      number: 55,
      node_id: 'PRNODE55',
      head: { ref: 'feat/resource-acme.prod2-issue-9', sha: 'PRSHA' },
    };
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [existingPr] });

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

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.rest.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
    const yamlText = Buffer.from(String(writeParams.content), 'base64').toString('utf8');

    // parent must not be emitted
    expect(yamlText).not.toMatch(/^\s*parent\s*:/m);
    expect(yamlText).toMatch(/^\s*parentId\s*:\s*'?PARENTX'?\s*$/m);

    // enable succeeded => no labels
    expect(ctx.octokit.rest.issues.addLabels).not.toHaveBeenCalled();

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    // schema load: 404 raw, then resolve via default searchPath "schema/..."
    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'sys.schema.json') throw httpErr(404);
        if (path === 'schema/sys.schema.json') return schemaFileResponse(schemaObj);
      }
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    // pulls.list returns no existing PR => proceeds to create
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] });

    ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
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
    expect(ctx.octokit.rest.pulls.create).toHaveBeenCalled();
    const body = String(ctx.octokit.rest.pulls.create.mock.calls[0][0].body);
    expect(body).toContain('fix: #3');
    expect(body).toContain('<!-- snapshot-hash:HASH -->');

    // auto merge enable succeeded => no label
    expect(ctx.octokit.rest.issues.addLabels).not.toHaveBeenCalled();
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref) {
        if (path === 'sub.schema.json') throw httpErr(404);
        if (path === 'schema/sub.schema.json') return schemaFileResponse(schemaObj);
      }
      if (String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
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

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalled();
    const writeParams = ctx.octokit.rest.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(500));

    ctx.octokit.rest.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: '' } } });
    ctx.octokit.rest.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

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
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.rest.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    ctx.octokit.rest.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

    // schema loads ok, but yaml exists => getContent succeeds with ref => existsAt true
    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) return { data: { any: 'file' } };
      throw httpErr(404);
    });

    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow(/already (exists|registered)/i);
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.repos.getContent.mockResolvedValueOnce(schemaFileResponse(schemaObj));

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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'sys-required.schema.json') throw httpErr(404);
      if (!ref && path === 'schema/sys-required.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) throw httpErr(404);
      throw httpErr(404);
    });

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] });
    ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
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

    const writeParams = ctx.octokit.rest.repos.createOrUpdateFileContents.mock.calls[0][0] as AnyObj;
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

    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValueOnce({ data: { commit: { sha: 'BASESHA' } } });
    ctx.octokit.rest.git.createRef.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.repos.getContent.mockImplementation(async ({ path, ref }: AnyObj) => {
      if (!ref && path === 'x.schema.json') return schemaFileResponse(schemaObj);
      if (ref && String(path).endsWith('.yaml')) throw boom;
      throw httpErr(404);
    });

    await expect(
      createRequestPr(ctx, { owner: 'o', repo: 'r' }, { number: 1, title: '', labels: [], body: '' }, {}, { template })
    ).rejects.toThrow('exists failed');
  });
});

// ─── New idempotency & reconciliation tests ────────────────────────────────────

describe('createRequestPr – idempotency, reconciliation, and branch safety', () => {
  // ---- shared helpers --------------------------------------------------------

  type AnyObj = Record<string, any>;

  const httpErr = (status: number, message = `HTTP ${status}`): Error & { status: number } => {
    const e = new Error(message) as Error & { status: number };
    e.status = status;
    return e;
  };

  const b64 = (s: string): string => Buffer.from(s, 'utf8').toString('base64');
  const b64Json = (obj: unknown): string => b64(JSON.stringify(obj));
  const schemaFileResponse = (obj: unknown) => ({
    data: { content: b64Json(obj), encoding: 'base64' },
  });
  const yamlFileResponse = (yamlText: string) => ({
    data: { content: b64(yamlText), encoding: 'base64' },
  });

  const noDelay = async (_ms: number) => {
    /* instant in tests */
  };

  /** Standard loadSubject setup with fixed mocks for resolvePrimaryId and projectForSchema. */
  async function makeSubject(opts: {
    resourceName: string;
    requestType: string;
    root: string;
    candidateObj: Record<string, unknown>;
    schemaObj?: unknown;
  }) {
    // We can't use jest.mock hoisting inside a nested describe, so we use a dynamic import
    // approach compatible with the existing test harness by calling loadSubject() directly.
    // The existing `loadSubject` is defined in the outer describe and already set up all mocks.
    // Here we re-use a fresh invocation:
    const { createRequestPr, mocks } = await loadSubject({
      jsYamlLoadImpl: (src: string) => {
        // Parse YAML as JSON for test predictability.
        try {
          return JSON.parse(src);
        } catch {
          return {};
        }
      },
      jsYamlDumpImpl: (obj: any) => JSON.stringify(obj),
    });

    mocks.resolvePrimaryIdFromTemplate.mockReturnValue(opts.resourceName);
    mocks.projectForSchema.mockResolvedValue({ ...opts.candidateObj });

    return { createRequestPr, mocks };
  }

  function mkMock(): jest.Mock<any> {
    return jest.fn() as jest.Mock<any>;
  }

  function makeCtx(overrides: Partial<AnyObj> = {}): AnyObj {
    const get = mkMock();
    get.mockResolvedValue({ data: { default_branch: 'main' } });
    const getBranch = mkMock();
    getBranch.mockResolvedValue({ data: { commit: { sha: 'BASE_SHA' } } });
    const getContent = mkMock();
    getContent.mockRejectedValue(httpErr(404));
    const createOrUpdateFileContents = mkMock();
    createOrUpdateFileContents.mockResolvedValue({ ok: true });
    const compareCommitsWithBasehead = mkMock();
    compareCommitsWithBasehead.mockResolvedValue({ data: { files: [] } });
    const createRef = mkMock();
    createRef.mockResolvedValue({});
    const list = mkMock();
    list.mockResolvedValue({ data: [] });
    const create = mkMock();
    create.mockResolvedValue({
      data: { number: 42, node_id: 'PR42', head: { ref: 'feat/resource-test-issue-1', sha: 'S' } },
    });
    const addLabels = mkMock();
    addLabels.mockResolvedValue({});

    const ctx: AnyObj = {
      octokit: {
        rest: {
          repos: { get, getBranch, getContent, createOrUpdateFileContents, compareCommitsWithBasehead },
          git: { createRef },
          pulls: { list, create },
          issues: { addLabels },
        },
      },
      log: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
      resourceBotConfig: {
        pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}' },
      },
    };
    return { ...ctx, ...overrides };
  }

  const NS_SCHEMA = {
    type: 'object',
    properties: { type: { const: 'system' }, name: { type: 'string' } },
  };
  const CANDIDATE = { type: 'system', name: 'sap.test' };
  const CANDIDATE_YAML = JSON.stringify(CANDIDATE);
  const TEMPLATE = {
    _meta: { requestType: 'systemNamespace', root: 'data/namespaces', schema: 'ns.schema.json', path: 'tpl.yml' },
    body: [],
  };
  const RESOURCE_FILE = 'data/namespaces/sap.test.yaml';

  function setSchema(ctx: AnyObj, path: string, schemaObj: unknown) {
    const original = ctx.octokit.rest.repos.getContent as jest.Mock;
    original.mockImplementation(async (args: unknown) => {
      const { path: p, ref } = args as AnyObj;
      if (!ref && p === path) return schemaFileResponse(schemaObj);
      throw httpErr(404);
    });
  }

  // ── 1. Clean namespace creation ─────────────────────────────────────────────
  it('clean system namespace creation: branch absent, file absent, PR absent → file and PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.rest.pulls.create).toHaveBeenCalledTimes(1);
  });

  // ── 2. Clean Product creation ────────────────────────────────────────────────
  it('clean Product creation: branch absent, file absent, PR absent → file and PR created', async () => {
    const productSchema = { type: 'object', properties: { id: { type: 'string' } } };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'MyProduct',
      requestType: 'product',
      root: 'data/products',
      candidateObj: { id: 'MyProduct' },
    });
    const ctx = makeCtx();
    const productTemplate = {
      _meta: { requestType: 'product', root: 'data/products', schema: 'prod.schema.json', path: 'tpl.yml' },
      body: [],
    };
    mocks.loadTemplate.mockResolvedValueOnce(productTemplate);
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'prod.schema.json') return schemaFileResponse(productSchema);
      throw httpErr(404);
    });
    ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
      data: { number: 55, node_id: 'PR55', head: { ref: 'feat/resource-myproduct-issue-1', sha: 'S' } },
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      { identifier: 'MyProduct', description: 'desc', contact: 'a@b' },
      { template: productTemplate, _delay: noDelay }
    );

    expect(pr.number).toBe(55);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(1);
  });

  // ── 3. Existing empty branch (createRef 422, branch head == baseSha) ─────────
  it('existing empty branch: createRef 422, branch head = baseSha → safe resume, file written, PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    // createRef 422
    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    // inspectExistingBranch: getBranch returns head == baseSha → safe, no compare needed
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } }) // base branch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } }); // inspectExistingBranch

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.rest.pulls.create).toHaveBeenCalledTimes(1);
  });

  // ── 4. Unrelated createRef 422 (branch cannot be confirmed) ──────────────────
  it('unrelated createRef 422: branch not confirmable → original error propagated, no write, no PR', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    // getBranch returns 404 → branch not confirmed
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } }) // base branch
      .mockRejectedValueOnce(httpErr(404)); // inspectExistingBranch

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/branch-existing|not confirmed/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 5. Partial-success matching Issue #933 ────────────────────────────────────
  it('#933-equivalent: branch exists with only target file changed, equivalent YAML → no write, PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    // createRef 422 → branch exists
    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    // getBranch for base, then for inspect — head differs from base
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    // compareCommitsWithBasehead: only target file changed
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });

    // getContent: schema + default-branch 404 + branch file equivalent
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404); // not on default
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML); // equivalent file on branch
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    // File was NOT rewritten — equivalent content found
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    // PR was created
    expect(ctx.octokit.rest.pulls.create).toHaveBeenCalledTimes(1);
  });

  // ── 6. Existing matching open PR ─────────────────────────────────────────────
  it('equivalent file + matching open PR → existing PR returned, no write, no second PR', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });
    // Existing open PR
    const existingPr = { number: 77, node_id: 'PR77', head: { ref: 'feat/resource-sap.test-issue-1', sha: 'S' } };
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [existingPr] });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(77);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 7. Default-branch duplicate ───────────────────────────────────────────────
  it('file exists on default branch → duplicate error, no write, no PR, no branch deletion', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    // getContent returns the file on the default branch
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/already registered|already exists/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 8. Conflicting branch file ────────────────────────────────────────────────
  it('branch file differs semantically → no overwrite, no PR, precise conflict error', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE)
        // Different content
        return yamlFileResponse(JSON.stringify({ type: 'system', name: 'DIFFERENT_VALUE' }));
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 9. Invalid YAML on branch ──────────────────────────────────────────────
  it('invalid YAML on branch → treated as conflict, no overwrite, no PR', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE)
        return yamlFileResponse('invalid: yaml: [{: broken');
      throw httpErr(404);
    });

    // The jsYamlDump mock returns stringified JSON; jsYamlLoad mock returns JSON.parse result.
    // For "invalid: yaml: [{: broken", JSON.parse will throw, so parseYamlSafe returns conflict.
    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 10. Branch with unrelated changed file, target absent ────────────────────
  it('branch has unrelated changed file, target absent → unsafe: fail closed, no write, no deletion', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    // Unrelated file in compare result
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: 'README.md', status: 'modified' }] },
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/unrelated|unsafe/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 11. Equivalent target file + unrelated file ───────────────────────────────
  it('branch has equivalent target file + unrelated changed file → fail closed, no PR, no deletion', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    // Both target file AND unrelated file changed
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: {
        files: [
          { filename: RESOURCE_FILE, status: 'added' },
          { filename: 'data/other.yaml', status: 'modified' },
        ],
      },
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/unrelated|unsafe/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── Semantic comparison tests ────────────────────────────────────────────────

  // These test reconciliation.ts helpers directly via the compareFileOnBranch path
  // by driving them through createRequestPr with a branch that has the given file content.

  function makePartialSuccessCtx(branchFileYaml: string) {
    const ctx = makeCtx();
    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(branchFileYaml);
      throw httpErr(404);
    });
    return ctx;
  }

  // ── 12. Mapping key order differs but content exact → equivalent ──────────────
  it('semantic comparison: different key order in YAML → equivalent (no write)', async () => {
    const candidate = { type: 'system', name: 'sap.test' };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    // Branch file has keys in different order: name first, then type
    const ctx = makePartialSuccessCtx(JSON.stringify({ name: 'sap.test', type: 'system' }));

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 13. Extra key in branch file → conflicting ───────────────────────────────
  it('semantic comparison: branch file has extra key → conflict', async () => {
    const candidate = { type: 'system', name: 'sap.test' };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makePartialSuccessCtx(JSON.stringify({ type: 'system', name: 'sap.test', extra: 'key' }));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 14. Missing key in branch file → conflicting ──────────────────────────────
  it('semantic comparison: branch file missing a key → conflict', async () => {
    const candidate = { type: 'system', name: 'sap.test' };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makePartialSuccessCtx(JSON.stringify({ type: 'system' })); // missing name

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);
  });

  // ── 15. Scalar type differs (string vs number) → conflicting ─────────────────
  it('semantic comparison: scalar type differs → conflict', async () => {
    const candidate = { type: 'system', name: 'sap.test', count: 1 };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makePartialSuccessCtx(JSON.stringify({ type: 'system', name: 'sap.test', count: '1' }));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);
  });

  // ── 16. Array order differs → conflicting ─────────────────────────────────────
  it('semantic comparison: array order differs → conflict', async () => {
    const candidate = { type: 'system', name: 'sap.test', contact: ['a@b', 'c@d'] };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makePartialSuccessCtx(JSON.stringify({ type: 'system', name: 'sap.test', contact: ['c@d', 'a@b'] }));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);
  });

  // ── 17. Nested object key order differs but content exact → equivalent ────────
  it('semantic comparison: nested key order differs → equivalent', async () => {
    const candidate = { type: 'system', name: 'sap.test', meta: { a: 1, b: 2 } };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidate,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makePartialSuccessCtx(JSON.stringify({ type: 'system', name: 'sap.test', meta: { b: 2, a: 1 } }));

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── Ambiguous write recovery ─────────────────────────────────────────────────

  function makeCleanCtx(schemaPath = 'ns.schema.json') {
    const ctx = makeCtx();
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === schemaPath) return schemaFileResponse(NS_SCHEMA);
      throw httpErr(404);
    });
    return ctx;
  }

  // ── 18. 422 "sha wasn't supplied" → reconcile finds equivalent ───────────────
  it('write throws 422 "sha wasn\'t supplied" → reconcile reads equivalent → PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(
      Object.assign(new Error('"sha" wasn\'t supplied'), { status: 422 })
    );
    // reconcile read: getContent returns equivalent file
    // After the 422, reconciliation reads the file from the branch
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    let getContentCallCount = 0;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      // Branch file read during reconciliation
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) {
        getContentCallCount++;
        return yamlFileResponse(CANDIDATE_YAML);
      }
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(getContentCallCount).toBeGreaterThanOrEqual(1); // reconciliation read happened
    expect(ctx.octokit.rest.pulls.create).toHaveBeenCalledTimes(1);
  });

  // ── 19. HTTP 500 → reconcile finds equivalent ────────────────────────────────
  it('write throws HTTP 500 → reconcile reads equivalent → PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(httpErr(500));

    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
  });

  // ── 20. HTTP 502/503/504 recovery (table-driven) ──────────────────────────────
  it.each([502, 503, 504])('write throws HTTP %i → reconcile reads equivalent → PR created', async (status) => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(httpErr(status));
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );
    expect(pr.number).toBe(42);
  });

  // ── 21. Timeout/ECONNRESET → reconcile finds equivalent ──────────────────────
  it('write throws ECONNRESET → reconcile reads equivalent → PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    const resetErr = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(resetErr);
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );
    expect(pr.number).toBe(42);
  });

  // ── 22. Ambiguous write, first read absent, retry succeeds ───────────────────
  it('ambiguous write → first reconcile absent → controlled retry succeeds → PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    // First write: ambiguous 500
    // Second write (retry): success
    ctx.octokit.rest.repos.createOrUpdateFileContents
      .mockRejectedValueOnce(httpErr(500))
      .mockResolvedValueOnce({ ok: true });

    let branchReadCount = 0;
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) {
        branchReadCount++;
        // First reconcile read: still absent; after retry: equivalent
        if (branchReadCount === 1) throw httpErr(404);
        return yamlFileResponse(CANDIDATE_YAML);
      }
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(2);
  });

  // ── 23. Controlled retry throws but final read equivalent ────────────────────
  it('ambiguous write → retry throws → final read finds equivalent → PR created', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents
      .mockRejectedValueOnce(httpErr(500)) // first ambiguous
      .mockRejectedValueOnce(httpErr(503)); // retry also fails

    let branchReadCount = 0;
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) {
        branchReadCount++;
        // First read: absent; second read (after retry): equivalent
        if (branchReadCount === 1) throw httpErr(404);
        return yamlFileResponse(CANDIDATE_YAML);
      }
      throw httpErr(404);
    });

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(42);
  });

  // ── 24. File absent after retry → original failure preserved ─────────────────
  it('ambiguous write + file remains absent after retry → original error preserved, no PR', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents
      .mockRejectedValueOnce(httpErr(500, 'upstream error'))
      .mockRejectedValueOnce(httpErr(503, 'still down'));

    // All branch reads return 404 (file never appears)
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/upstream error|could not be written|file-write/i);

    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 25. Ambiguous write + conflicting reconcile file ─────────────────────────
  it('ambiguous write, reconcile finds conflicting file → conflict error, original error as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(httpErr(500, 'original write error'));

    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE)
        return yamlFileResponse(JSON.stringify({ type: 'system', name: 'DIFFERENT' }));
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/conflict/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(1); // no retry after conflict
  });

  // ── 26. Non-ambiguous 422 validation error → no controlled retry ─────────────
  it('non-ambiguous 422 validation error → no retry, error propagated immediately', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    // 422 without sha-related message → definitive failure
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(
      httpErr(422, 'Validation Failed: bad request')
    );

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/file-write|bad request/i);

    // Only 1 attempt, no retry
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).toHaveBeenCalledTimes(1);
  });

  // ── PR idempotency tests ─────────────────────────────────────────────────────

  // ── 27. pulls.list fails → pulls.create NOT called ───────────────────────────
  it('pulls.list fails → error propagated, pulls.create not called', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.pulls.list.mockRejectedValueOnce(new Error('list forbidden'));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/list forbidden/i);

    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 28. pulls.create existing-PR 422 → re-list finds PR ──────────────────────
  it('pulls.create returns existing-PR 422 → re-list finds PR → existing PR returned', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    const existingPr = { number: 88, node_id: 'PR88', head: { ref: 'feat/resource-sap.test-issue-1', sha: 'S' } };
    // First list (before create): empty
    // create: 422 existing PR
    // Second list (reconciliation): returns PR
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [existingPr] });
    ctx.octokit.rest.pulls.create.mockRejectedValueOnce(
      Object.assign(new Error('A pull request already exists'), { status: 422 })
    );

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(88);
  });

  // ── 29. pulls.create 500/timeout → re-list finds PR ─────────────────────────
  it('pulls.create returns 500 → re-list finds PR → existing PR returned', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    const existingPr = { number: 99, node_id: 'PR99', head: { ref: 'feat/resource-sap.test-issue-1', sha: 'S' } };
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [existingPr] });
    ctx.octokit.rest.pulls.create.mockRejectedValueOnce(httpErr(500));

    const pr = await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    expect(pr.number).toBe(99);
  });

  // ── 30. pulls.create fails, re-list finds nothing → original error ───────────
  it('pulls.create fails + re-list empty → original create error preserved', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [] }); // re-list also empty
    ctx.octokit.rest.pulls.create.mockRejectedValueOnce(httpErr(500, 'create failed'));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/create failed/i);
  });

  // ── 31. pulls.create fails + reconciliation list also fails ──────────────────
  it('pulls.create fails + reconciliation re-list fails → original create error is primary', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    ctx.octokit.rest.pulls.list
      .mockResolvedValueOnce({ data: [] }) // pre-create lookup
      .mockRejectedValueOnce(new Error('list also failed')); // reconciliation re-list
    ctx.octokit.rest.pulls.create.mockRejectedValueOnce(httpErr(500, 'primary create failure'));

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/primary create failure/i);
  });

  // ── Stage-aware error tests ──────────────────────────────────────────────────

  // ── 32. pulls.list failure carries request-pr:pr-lookup stage ────────────────
  it('pulls.list failure: error carries request-pr:pr-lookup stage, original as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    const listErr = httpErr(403, 'list forbidden');
    ctx.octokit.rest.pulls.list.mockRejectedValueOnce(listErr);

    let thrown: Error | null = null;
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    ).catch((e: unknown) => {
      thrown = e instanceof Error ? e : new Error(String(e));
    });

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('request-pr:pr-lookup');
    expect(thrown!.cause).toBe(listErr);
  });

  // ── 33. Non-ambiguous pulls.create failure carries request-pr:pr-create ───────
  it('non-ambiguous pulls.create failure: error carries request-pr:pr-create stage, original as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });
    ctx.octokit.rest.pulls.list.mockResolvedValueOnce({ data: [] }); // no existing PR
    const createErr = httpErr(400, 'bad request');
    ctx.octokit.rest.pulls.create.mockRejectedValueOnce(createErr);

    let thrown: Error | null = null;
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    ).catch((e: unknown) => {
      thrown = e instanceof Error ? e : new Error(String(e));
    });

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('request-pr:pr-create');
    expect(thrown!.cause).toBe(createErr);
  });

  // ── 34. Default-branch duplicate lookup non-404 failure carries stage tag ─────
  it('default-branch lookup non-404 failure: error carries request-pr:default-file-read stage, original as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    const lookupErr = httpErr(500, 'default branch lookup failed');
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw lookupErr;
      throw httpErr(404);
    });

    let thrown: Error | null = null;
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    ).catch((e: unknown) => {
      thrown = e instanceof Error ? e : new Error(String(e));
    });

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('request-pr:default-file-read');
    expect(thrown!.cause).toBe(lookupErr);
  });

  // ── 35. Base-branch duplicate lookup non-404 failure carries stage tag ────────
  it('effective-base lookup non-404 failure: error carries request-pr:base-file-read stage, original as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.resourceBotConfig = {
      pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}', baseBranch: 'develop' },
    };
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValue({ data: { commit: { sha: 'BASE_SHA' } } });

    const baseErr = httpErr(503, 'base branch lookup failed');
    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'develop' && path === RESOURCE_FILE) throw baseErr;
      throw httpErr(404);
    });

    let thrown: Error | null = null;
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    ).catch((e: unknown) => {
      thrown = e instanceof Error ? e : new Error(String(e));
    });

    expect(thrown).not.toBeNull();
    expect(thrown!.message).toContain('request-pr:base-file-read');
    expect(thrown!.cause).toBe(baseErr);
  });

  // ── Workflow compatibility ───────────────────────────────────────────────────

  // ── 37. Default commit template produces (#42), not (#$42) ───────────────────
  it('default commit template produces (#42) not (#$42)', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    // No custom commitMessageTemplate → use default
    ctx.resourceBotConfig = { pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}' } };
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 42, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    const writeCall = (ctx.octokit.rest.repos.createOrUpdateFileContents as jest.Mock).mock.calls[0][0] as AnyObj;
    expect(writeCall.message).toMatch(/\(#42\)/);
    expect(writeCall.message).not.toMatch(/\(#\$42\)/);
  });

  // ── 38. Custom commitMessageTemplate respected exactly ───────────────────────
  it('custom commitMessageTemplate is passed through byte-for-byte', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();
    ctx.resourceBotConfig = {
      pr: {
        branchNameTemplate: 'feat/resource-{resource}-issue-{issue}',
        commitMessageTemplate: 'custom: add {resource} for issue {issue}',
      },
    };
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 7, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    const writeCall = (ctx.octokit.rest.repos.createOrUpdateFileContents as jest.Mock).mock.calls[0][0] as AnyObj;
    expect(writeCall.message).toBe('custom: add sap.test for issue 7');
  });

  // ── 35. PR creation failure → Issue stays open, no applyApprovedRequestState ─
  // (Tested via the recovery wrapper — the finalization-level test is in request-orchestrator.more)

  // ── 39. Product PR body retains issue marker, snapshot hash, maintainer_can_modify ─
  it('Product PR body retains issue marker, snapshot hash, and maintainer_can_modify', async () => {
    const productSchema = { type: 'object', properties: { id: { type: 'string' } } };
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'MyProduct',
      requestType: 'product',
      root: 'data/products',
      candidateObj: { id: 'MyProduct' },
    });
    const ctx = makeCtx();
    const productTemplate = {
      _meta: { requestType: 'product', root: 'data/products', schema: 'prod.schema.json', path: 'tpl.yml' },
      body: [],
    };
    mocks.loadTemplate.mockResolvedValueOnce(productTemplate);
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'prod.schema.json') return schemaFileResponse(productSchema);
      throw httpErr(404);
    });
    ctx.octokit.rest.pulls.create.mockResolvedValueOnce({
      data: { number: 55, node_id: 'PR55', head: { ref: 'feat/resource-myproduct-issue-5', sha: 'S' } },
    });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 5, title: '', labels: [], body: '' },
      { identifier: 'MyProduct', description: 'desc', contact: 'a@b' },
      { template: productTemplate, _delay: noDelay }
    );

    const createCall = (ctx.octokit.rest.pulls.create as jest.Mock).mock.calls[0][0] as AnyObj;
    expect(String(createCall.body)).toContain('<!-- nsreq:issue:5 -->');
    expect(String(createCall.body)).toContain('<!-- snapshot-hash:');
    expect(createCall.maintainer_can_modify).toBe(true);
  });

  // ── Effective-base duplicate detection (correction D) ────────────────────────

  // ── 40. baseBranch=develop, file exists on develop (not on main) ─────────────
  it('effective-base duplicate: file exists on baseBranch (develop) but not defaultBranch (main) → duplicate error', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    // Use develop as baseBranch
    ctx.resourceBotConfig = {
      pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}', baseBranch: 'develop' },
    };
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    // getBranch: first call for develop (baseBranch SHA), subsequent for inspectExistingBranch if needed
    ctx.octokit.rest.repos.getBranch.mockResolvedValue({ data: { commit: { sha: 'BASE_SHA' } } });
    // develop is the configured baseBranch (repos.get must return main as default_branch)
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });

    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      // File absent on main (default branch)
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      // File present on develop (base branch)
      if (ref === 'develop' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/already registered|base-file-read/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();
  });

  // ── 41. baseBranch=develop, file exists on main (not on develop) ─────────────
  it('effective-base duplicate: file exists on defaultBranch (main) but not baseBranch (develop) → duplicate error', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    ctx.resourceBotConfig = {
      pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}', baseBranch: 'develop' },
    };
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValue({ data: { commit: { sha: 'BASE_SHA' } } });

    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      // File present on main — triggers duplicate on default-branch check
      if (ref === 'main' && path === RESOURCE_FILE) return yamlFileResponse(CANDIDATE_YAML);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/already registered|default-file-read/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 42. baseBranch=main (same as default): only one getContent call for default ─
  it('baseBranch equals defaultBranch: no duplicate getContent call for base branch', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    // No baseBranch override → uses defaultBranch (main)
    ctx.resourceBotConfig = {
      pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}' },
    };
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    const getContentCalls: string[] = [];
    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref) getContentCalls.push(`${ref}:${path as string}`);
      throw httpErr(404);
    });
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockResolvedValueOnce({ ok: true });

    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    // The duplicate check for default branch (main) should happen exactly once
    const mainChecks = getContentCalls.filter((c) => c.startsWith('main:') && c.endsWith(RESOURCE_FILE));
    expect(mainChecks).toHaveLength(1);
  });

  // ── 43. Lookup failure on effective base ref is propagated ───────────────────
  it('lookup failure on baseBranch (non-404) is propagated as error', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    ctx.resourceBotConfig = {
      pr: { branchNameTemplate: 'feat/resource-{resource}-issue-{issue}', baseBranch: 'develop' },
    };
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    ctx.octokit.rest.repos.get.mockResolvedValueOnce({ data: { default_branch: 'main' } });
    ctx.octokit.rest.repos.getBranch.mockResolvedValue({ data: { commit: { sha: 'BASE_SHA' } } });

    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      // Non-404 on develop → propagate
      if (ref === 'develop' && path === RESOURCE_FILE) throw httpErr(503);
      throw httpErr(404);
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/503/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── Branch safety structured log (correction E) ──────────────────────────────

  // ── 44. Unsafe branch: request-pr:branch-unsafe structured log emitted ───────
  it('unsafe branch: request-pr:branch-unsafe structured log is emitted', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'DIVERGED_SHA' } } });
    // Unrelated file change → unsafe
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: 'README.md', status: 'modified' }] },
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/unrelated|unsafe/i);

    // Structured log emitted
    const warnCalls = (ctx.log.warn as jest.Mock).mock.calls as any[][];
    const unsafeLog = warnCalls.find(
      (c) => typeof c[0] === 'object' && c[0] !== null && c[0].stage === 'request-pr:branch-unsafe'
    );
    expect(unsafeLog).toBeDefined();
    expect(unsafeLog![0].owner).toBe('o');
    expect(unsafeLog![0].repo).toBe('r');
    expect(unsafeLog![0].branch).toContain('sap.test');
  });

  // ── 45. Fail-closed: compare returns undefined files ─────────────────────────
  it('inspectExistingBranch: compare returns undefined files → fail closed, no write', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'DIVERGED_SHA' } } });
    // No files property in response
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({ data: {} });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/branch-existing|cannot verify|files array/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 46. Fail-closed: compare returns empty files with diverged SHA ────────────
  it('inspectExistingBranch: empty files array with diverged SHA → fail closed', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'DIVERGED_SHA' } } });
    // Empty files with diverged SHA — suspicious
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({ data: { files: [] } });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/branch-existing|diverged|zero changed/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 47. Fail-closed: target file has status 'removed' ────────────────────────
  it('inspectExistingBranch: target file status "removed" → fail closed', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'DIVERGED_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'removed' }] },
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/branch-existing|removed|unsafe/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 48. YAML comparison uses sanitized representation (undefined omitted) ─────
  it('YAML semantic comparison: candidate property undefined is omitted; branch file without that key → equivalent', async () => {
    // The candidate has an undefined field. dumpYamlDoc omits it via sanitizeForYaml.
    // The branch file also lacks that field. They should be equivalent.
    const candidateWithUndefined = { type: 'system', name: 'sap.test', optionalField: undefined };
    const { createRequestPr: _unused, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: candidateWithUndefined,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    // Use real YAML serialization for this test
    const { createRequestPr: realCreateRequestPr, mocks: realMocks } = await loadSubject();
    realMocks.resolvePrimaryIdFromTemplate.mockReturnValue('sap.test');
    realMocks.projectForSchema.mockResolvedValue({ ...candidateWithUndefined });
    realMocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: { files: [{ filename: RESOURCE_FILE, status: 'added' }] },
    });

    ctx.octokit.rest.repos.getContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      // Branch file has only the defined keys (no optionalField)
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE) {
        return yamlFileResponse('type: system\nname: sap.test\n');
      }
      throw httpErr(404);
    });

    const pr = await realCreateRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    );

    // Equivalent → no write
    expect(pr.number).toBe(42);
    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
  });

  // ── 49. Write reconciliation conflict preserves original error as cause ───────
  it('ambiguous write, reconcile conflict: original write error is preserved as cause', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);
    const ctx = makeCleanCtx();

    const originalWriteError = Object.assign(new Error('original write 500'), { status: 500 });
    ctx.octokit.rest.repos.createOrUpdateFileContents.mockRejectedValueOnce(originalWriteError);

    const originalGetContent = ctx.octokit.rest.repos.getContent as jest.Mock;
    originalGetContent.mockImplementation(async (args: unknown) => {
      const { path, ref } = args as AnyObj;
      if (!ref && path === 'ns.schema.json') return schemaFileResponse(NS_SCHEMA);
      if (ref === 'main' && path === RESOURCE_FILE) throw httpErr(404);
      if (ref === 'feat/resource-sap.test-issue-1' && path === RESOURCE_FILE)
        return yamlFileResponse(JSON.stringify({ type: 'system', name: 'DIFFERENT' }));
      throw httpErr(404);
    });

    let thrownError: Error | null = null;
    await createRequestPr(
      ctx,
      { owner: 'o', repo: 'r' },
      { number: 1, title: '', labels: [], body: '' },
      {},
      { template: TEMPLATE, _delay: noDelay }
    ).catch((e: unknown) => {
      thrownError = e instanceof Error ? e : new Error(String(e));
    });

    expect(thrownError).not.toBeNull();
    // The thrown error must carry request-pr:file-conflict stage tag.
    expect(thrownError!.message).toMatch(/request-pr:file-conflict/);
    // The cause must be the original ambiguous write error (status 500).
    expect(thrownError!.cause).toBeInstanceOf(Error);
    // The WriteFileResult.conflict.cause is the original firstError from writeFileWithReconciliation.
    // create.ts throws: new Error('[request-pr:file-conflict] ...', { cause: writeResult.cause })
    // writeResult.cause === originalWriteError
    expect((thrownError!.cause as Error).message).toBe('original write 500');
    expect((thrownError!.cause as any).status).toBe(500);
  });

  // ── 50. Renamed target file via previous_filename: fail closed ───────────────
  it('branch has target file with status=renamed (previous_filename present): fail closed, no write, no PR', async () => {
    const { createRequestPr, mocks } = await makeSubject({
      resourceName: 'sap.test',
      requestType: 'systemNamespace',
      root: 'data/namespaces',
      candidateObj: CANDIDATE,
    });
    const ctx = makeCtx();
    setSchema(ctx, 'ns.schema.json', NS_SCHEMA);
    mocks.loadTemplate.mockResolvedValueOnce(TEMPLATE);

    ctx.octokit.rest.git.createRef.mockRejectedValueOnce(httpErr(422));
    ctx.octokit.rest.repos.getBranch
      .mockResolvedValueOnce({ data: { commit: { sha: 'BASE_SHA' } } })
      .mockResolvedValueOnce({ data: { commit: { sha: 'BRANCH_HEAD_SHA' } } });
    // compare shows target arrived via rename from an unrelated path
    ctx.octokit.rest.repos.compareCommitsWithBasehead.mockResolvedValueOnce({
      data: {
        files: [
          {
            filename: RESOURCE_FILE,
            status: 'renamed',
            previous_filename: 'data/old/other.yaml',
          },
        ],
      },
    });

    await expect(
      createRequestPr(
        ctx,
        { owner: 'o', repo: 'r' },
        { number: 1, title: '', labels: [], body: '' },
        {},
        { template: TEMPLATE, _delay: noDelay }
      )
    ).rejects.toThrow(/rename|renamed|branch-existing/i);

    expect(ctx.octokit.rest.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.pulls.create).not.toHaveBeenCalled();

    // branch-unsafe structured log must be emitted
    const warnCalls = (ctx.log.warn as jest.Mock).mock.calls as any[][];
    const unsafeLog = warnCalls.find(
      (c) => typeof c[0] === 'object' && c[0] !== null && c[0].stage === 'request-pr:branch-unsafe'
    );
    expect(unsafeLog).toBeDefined();
    expect(unsafeLog![0].outcome).toBe('target-arrived-via-rename');
  });
});
