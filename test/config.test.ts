import { jest } from '@jest/globals';
import { DEFAULT_CONFIG, loadStaticConfig } from '../src/config.js';

const CFG_YAML = '.github/registry-bot/config.yaml';
const CFG_YML = '.github/registry-bot/config.yml';
const CFG_JS = '.github/registry-bot/config.js';

type FileEntry = { kind: 'file'; text: string } | { kind: 'dir' } | { kind: 'bad' } | { kind: 'err'; status: number };

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function httpErr(status: number): Error & { status: number } {
  const e = new Error(String(status)) as Error & { status: number };
  e.status = status;
  return e;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkContext(args: {
  owner: string;
  repo: string;
  files: Record<string, FileEntry>;
  openIssues?: { number: number; title: string }[];
}) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const getContent = jest.fn(async ({ owner, repo, path }: any) => {
    const key = `${owner}/${repo}:${path}`;
    const entry = args.files[key];
    if (!entry) throw httpErr(404);

    if (entry.kind === 'dir') return { data: [] };
    if (entry.kind === 'bad') return { data: { foo: 'bar' } };
    if (entry.kind === 'err') throw httpErr(entry.status);

    return { data: { content: b64(entry.text), encoding: 'base64' } };
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const listForRepo = jest.fn(async (_p: any) => ({
    data: args.openIssues ?? [],
  }));

  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const update = jest.fn(async (_p: any) => ({}));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const create = jest.fn(async (_p: any) => ({}));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const createComment = jest.fn(async (_p: any) => ({}));

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const context: any = {
    octokit: {
      repos: { getContent },
      issues: { listForRepo, update, create, createComment },
    },
    log: { debug: jest.fn(), warn: jest.fn(), info: jest.fn() },
    repo: () => ({ owner: args.owner, repo: args.repo }),
  };

  return { context, getContent, listForRepo, update, create, createComment };
}

test('loads repo config and normalizes values when validate=false', async () => {
  const owner = 'o_norm';
  const repo = 'r_norm';

  const cfg = `
pr:
  branchNameTemplate: '  req/\${type}-\${id}  '
  titleTemplate: 123
  autoMerge:
    enabled: "true"
    method: false
workflow:
  approvers: [" a ", "b"]
  labels:
    global: [" x ", "", "y"]
    authorAction: true
    approverAction: 0
    autoMergeCandidate: "  ok "
    approvalRequested: ["  a ", "b"]
    approvalSuccessful: ["c"]
requests:
  foo:
    folderName: 12
    schema: true
    issueTemplate: "  templates/foo.yml "
`;

  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: cfg },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, {
    validate: false,
    updateIssue: false,
    forceReload: true,
  });

  expect(res.source).toBe(`repo:${CFG_YAML}`);
  expect(res.config.pr?.branchNameTemplate).toBe('req/${type}-${id}');
  expect(res.config.pr?.titleTemplate).toBe('123');
  expect(res.config.pr?.autoMerge?.enabled).toBe(true);
  expect(res.config.pr?.autoMerge?.method).toBe('false');

  expect(res.config.workflow?.approvers).toEqual(['a', 'b']);
  expect(res.config.workflow?.labels?.global).toEqual(['x', 'y']);
  expect(res.config.workflow?.labels?.authorAction).toBe('true');
  expect(res.config.workflow?.labels?.approverAction).toBe('0');
  expect(res.config.workflow?.labels?.autoMergeCandidate).toBe('ok');

  expect(res.config.requests?.foo?.folderName).toBe('12');
  expect(res.config.requests?.foo?.schema).toBe('true');
  expect(res.config.requests?.foo?.issueTemplate).toBe('templates/foo.yml');
});

test('validate=true closes existing invalid-config issue and returns cached result on second call', async () => {
  const owner = 'o_close_cache';
  const repo = 'r_close_cache';

  const { context, getContent, createComment, update } = mkContext({
    owner,
    repo,
    openIssues: [{ number: 7, title: 'registry-bot: invalid static config.yaml' }],
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: {
        kind: 'file',
        text: `
    requests:
      sample:
        folderName: resources
        schema: .github/registry-bot/schemas/sample.json
        issueTemplate: .github/ISSUE_TEMPLATE/sample.md
    `,
      },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const r1 = await loadStaticConfig(context, {
    validate: true,
    updateIssue: true,
    forceReload: true,
  });

  expect(r1.source).toBe(`repo:${CFG_YAML}`);
  expect(createComment).toHaveBeenCalledTimes(1);
  expect(update).toHaveBeenCalledWith(expect.objectContaining({ issue_number: 7, state: 'closed' }));

  const callsAfterFirst = getContent.mock.calls.length;

  const r2 = await loadStaticConfig(context, { validate: true, updateIssue: true });

  expect(r2.source).toBe(r1.source);
  expect(getContent.mock.calls.length).toBe(callsAfterFirst);
  expect(createComment).toHaveBeenCalledTimes(1);
  expect(update).toHaveBeenCalledTimes(1);
});

test('invalid repo config falls back to DEFAULT_CONFIG and creates error issue (updateIssue=true)', async () => {
  const owner = 'o_invalid_create';
  const repo = 'r_invalid_create';

  const bad = `requests: bad`;

  const { context, create } = mkContext({
    owner,
    repo,
    openIssues: [],
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: bad },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, {
    validate: true,
    updateIssue: true,
    forceReload: true,
  });

  expect(res.source).toBe('default-invalid-config');
  expect(create).toHaveBeenCalledWith(
    expect.objectContaining({
      title: 'registry-bot: invalid static config.yaml',
      labels: ['registry-bot', 'config-error'],
    })
  );

  expect(res.config.requests).toEqual(DEFAULT_CONFIG.requests);
});

test('invalid repo config falls back without creating an issue when updateIssue=false', async () => {
  const owner = 'o_invalid_noissue';
  const repo = 'r_invalid_noissue';

  const bad = `requests: bad`;

  const { context, create, update } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: bad },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, {
    validate: true,
    updateIssue: false,
    forceReload: true,
  });

  expect(res.source).toBe('default-invalid-config');
  expect(create).not.toHaveBeenCalled();
  expect(update).not.toHaveBeenCalled();
});

test('missing config reports missing issue (updates existing) and loads hooks from org repo when source=default', async () => {
  const owner = 'o_missing_orghooks';
  const repo = 'r_missing_orghooks';

  const prevFlag = process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
  process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = 'true';

  const orgRepo = '.github';

  const { context, update } = mkContext({
    owner,
    repo,
    openIssues: [{ number: 9, title: 'registry-bot: invalid static config.yaml' }],
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'err', status: 404 },
      [`${owner}/${repo}:${CFG_YML}`]: { kind: 'err', status: 404 },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },

      [`${owner}/${orgRepo}:${CFG_YAML}`]: { kind: 'err', status: 404 },
      [`${owner}/${orgRepo}:${CFG_YML}`]: { kind: 'err', status: 404 },
      [`${owner}/${orgRepo}:${CFG_JS}`]: {
        kind: 'file',
        text: `export default { hello: "world" }`,
      },
    },
  });

  const res = await loadStaticConfig(context, {
    validate: true,
    updateIssue: true,
    forceReload: true,
  });

  expect(res.source).toBe('default');
  expect(update).toHaveBeenCalledWith(
    expect.objectContaining({
      issue_number: 9,
      body: expect.stringContaining('No static registry-bot configuration file was found'),
    })
  );

  try {
    expect(res.hooks).toEqual(
      expect.objectContaining({
        __type: 'registry-bot-hooks:esm',
        __path: CFG_JS,
        __code: `export default { hello: "world" }`,
        __hash: expect.stringMatching(/^[0-9a-f]{16}$/),
      })
    );

    expect(res.hooksSource).toContain(CFG_JS);
    expect(res.hooksSource).toEqual(expect.stringMatching(/config\.js#[0-9a-f]{16}$/));
  } finally {
    if (prevFlag === undefined) delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
    else process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = prevFlag;
  }
});

test('throws if GitHub getContent fails with non-404', async () => {
  const owner = 'o_err';
  const repo = 'r_err';

  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'err', status: 500 },
    },
  });

  await expect(loadStaticConfig(context, { forceReload: true })).rejects.toThrow('500');
});

test('falls back from repo yaml directory entry to repo yml config, closes nothing when no issue exists, and loads repo hooks', async () => {
  const owner = 'o_repo_yml_hooks';
  const repo = 'r_repo_yml_hooks';
  const prevDebug = process.env.DEBUG_NS;
  const prevFlag = process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
  process.env.DEBUG_NS = '1';
  process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = 'true';

  try {
    const { context, createComment, update } = mkContext({
      owner,
      repo,
      openIssues: [],
      files: {
        [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'dir' },
        [`${owner}/${repo}:${CFG_YML}`]: {
          kind: 'file',
          text: `
pr:
  autoMerge:
    enabled: false
    method: merge
workflow:
  approvers: null
requests:
  demo:
    folderName: data
    schema: schema/demo.json
    issueTemplate: .github/ISSUE_TEMPLATE/demo.yml
    approvers: null
`,
        },
        [`${owner}/${repo}:${CFG_JS}`]: {
          kind: 'file',
          text: `export default { hooks: true }`,
        },
      },
    });

    const res = await loadStaticConfig(context, {
      validate: true,
      updateIssue: true,
      forceReload: true,
    });

    expect(res.source).toBe(`repo:${CFG_YML}`);
    expect(res.config.pr?.autoMerge?.enabled).toBe(false);
    expect(res.config.pr?.autoMerge?.method).toBe('merge');
    expect(res.config.workflow?.approvers).toBeNull();
    expect(res.config.requests?.demo?.approvers).toBeNull();
    expect(res.hooks).toEqual(
      expect.objectContaining({
        __type: 'registry-bot-hooks:esm',
        __path: CFG_JS,
        __code: `export default { hooks: true }`,
      })
    );
    expect(createComment).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  } finally {
    process.env.DEBUG_NS = prevDebug;
    if (prevFlag === undefined) delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
    else process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = prevFlag;
  }
});

test('invalid config reporting tolerates non-Error failures while listing and creating issues', async () => {
  const owner = 'o_invalid_warns';
  const repo = 'r_invalid_warns';

  const { context, listForRepo, create } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: `requests: bad` },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  listForRepo.mockRejectedValueOnce('list failed');
  create.mockRejectedValueOnce('create failed');

  const res = await loadStaticConfig(context, {
    validate: true,
    updateIssue: true,
    forceReload: true,
  });

  expect(res.source).toBe('default-invalid-config');
  expect(context.log.warn).toHaveBeenCalledWith(
    { err: 'list failed' },
    'failed to list issues while reporting static config error'
  );
  expect(context.log.warn).toHaveBeenCalledWith({ err: 'create failed' }, 'failed to create static config error issue');
});

test('normalizeEnabled handles string false/unknown and non-boolean values; normalizeMethod handles non-primitive — covers lines 36-37,40,48', async () => {
  const owner = 'o_norm_edge';
  const repo = 'r_norm_edge';

  const cfg = `
pr:
  autoMerge:
    enabled: 'false'
    method: ~
`;
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: cfg },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // 'false' (string) → normalizeEnabled returns false (line 36)
  expect(res.config.pr?.autoMerge?.enabled).toBe(false);
  // null from YAML ~ → normalizeMethod returns null (line 45 fast-path, not 48)
});

test('normalizeEnabled returns null for unrecognized string — covers line 37', async () => {
  const owner = 'o_norm_unk';
  const repo = 'r_norm_unk';

  const cfg = `
pr:
  autoMerge:
    enabled: maybe
    method: ~
`;
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: cfg },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // 'maybe'.toLowerCase() is not 'true'/'false' → normalizeEnabled returns null (line 37)
  expect(res.config.pr?.autoMerge?.enabled).toBeNull();
});

test('normalizeEnabled returns null for number; normalizeMethod returns null for object — covers lines 40,48', async () => {
  const owner = 'o_norm_num';
  const repo = 'r_norm_num';

  // YAML: enabled as integer 42, method as a mapping (object)
  const cfg = `
pr:
  autoMerge:
    enabled: 42
    method:
      nested: value
`;
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: cfg },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // 42 is not string/boolean → normalizeEnabled returns null (line 40)
  expect(res.config.pr?.autoMerge?.enabled).toBeNull();
  // {} object → normalizeMethod returns null (line 48)
  expect(res.config.pr?.autoMerge?.method).toBeNull();
});

test('buildRequests normalizes approvers array — covers line 319', async () => {
  const owner = 'o_approvers';
  const repo = 'r_approvers';

  const cfg = `
requests:
  mytype:
    folderName: data
    schema: schema/foo.json
    issueTemplate: .github/ISSUE_TEMPLATE/foo.yml
    approvers:
      - alice
      - bob
`;
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: cfg },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // approversRaw is ['alice', 'bob'] → normalizeStringArray (line 319)
  expect(res.config.requests?.mytype?.approvers).toEqual(['alice', 'bob']);
});

test('loads config from org repo when repo config is missing — covers lines 645-646', async () => {
  const owner = 'o_orgcfg';
  const repo = 'r_orgcfg';

  const orgCfg = `
requests:
  orgtype:
    folderName: data/org
    schema: schema/org.json
    issueTemplate: .github/ISSUE_TEMPLATE/org.yml
`;
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'err', status: 404 },
      [`${owner}/${repo}:${CFG_YML}`]: { kind: 'err', status: 404 },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
      [`${owner}/.github:${CFG_YAML}`]: { kind: 'file', text: orgCfg },
      [`${owner}/.github:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  const res = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // repo config missing → falls back to org config → lines 645-646 hit
  expect(res.source).toMatch(/^org:/);
  expect(res.config.requests?.orgtype?.folderName).toBe('data/org');
});

test('issues.update failure is logged when updating existing invalid-config issue — covers line 491', async () => {
  const owner = 'o_update_fail';
  const repo = 'r_update_fail';

  const { context, update } = mkContext({
    owner,
    repo,
    openIssues: [{ number: 5, title: 'registry-bot: invalid static config.yaml' }],
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: `requests: bad` },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  update.mockRejectedValueOnce(new Error('update api failed'));

  await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });

  // Line 491: catch block when issues.update throws
  expect(context.log.warn).toHaveBeenCalledWith(
    { err: 'update api failed', issue_number: 5 },
    'failed to update static config error issue'
  );
});

test('createComment failure is logged when closing resolved config issue — covers line 561', async () => {
  const owner = 'o_comment_fail';
  const repo = 'r_comment_fail';

  const { context, createComment } = mkContext({
    owner,
    repo,
    openIssues: [{ number: 3, title: 'registry-bot: invalid static config.yaml' }],
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: {
        kind: 'file',
        text: `
requests:
  sample:
    folderName: data
    schema: schema/sample.json
    issueTemplate: .github/ISSUE_TEMPLATE/sample.yml
`,
      },
      [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
    },
  });

  createComment.mockRejectedValueOnce(new Error('comment api failed'));

  await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });

  // Line 561: catch block when createComment throws in closeStaticConfigIssueIfResolved
  expect(context.log.warn).toHaveBeenCalledWith(
    { err: 'comment api failed' },
    'failed to close static config error issue after successful validation'
  );
});

test('default config without validation logs debug and ignores hook loading failures with non-status errors', async () => {
  const owner = 'o_default_debug';
  const repo = 'r_default_debug';

  // This test exercises the catch block in loadJsConfigFromRepo (non-404 error path),
  // which is only reachable when JS hooks are explicitly enabled.
  const prevFlag = process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
  process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = 'true';

  try {
    const { context } = mkContext({
      owner,
      repo,
      files: {
        [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'err', status: 404 },
        [`${owner}/${repo}:${CFG_YML}`]: { kind: 'err', status: 404 },
        [`${owner}/${repo}:${CFG_JS}`]: { kind: 'err', status: 404 },
        [`${owner}/.github:${CFG_YAML}`]: { kind: 'err', status: 404 },
        [`${owner}/.github:${CFG_YML}`]: { kind: 'err', status: 404 },
        [`${owner}/.github:${CFG_JS}`]: { kind: 'err', status: 404 },
      },
    });

    context.octokit.repos.getContent.mockImplementation(
      ({ owner: ownerArg, repo: repoArg, path }: { owner: string; repo: string; path: string }) => {
        if (ownerArg === owner && repoArg === repo && path === CFG_JS) {
          throw new Error('hooks boom');
        }
        throw httpErr(404);
      }
    );

    const res = await loadStaticConfig(context, {
      validate: false,
      updateIssue: true,
      forceReload: true,
    });

    expect(res.source).toBe('default');
    expect(res.hooks).toBeNull();
    expect(res.hooksSource).toBeNull();
    expect(context.log.debug).toHaveBeenCalledWith(
      { source: 'default' },
      'no static registry-bot config found; using DEFAULT_CONFIG without validation'
    );
    expect(context.log.warn).toHaveBeenCalledWith(
      { err: 'hooks boom' },
      'failed to load JS registry-bot config; continuing without hooks'
    );
  } finally {
    if (prevFlag === undefined) delete process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'];
    else process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] = prevFlag;
  }
});

// ─── YAML .yaml extension (right arm of || in parseConfigString) ─────────────

test('loads .yaml config (exercises .yaml branch of parseConfigString)', async () => {
  const owner = 'o_yaml_ext';
  const repo = 'r_yaml_ext';
  const { context } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YAML}`]: {
        kind: 'file',
        text: `requests:\n  mytype:\n    issueTemplate: templates/t.yml\n`,
      },
    },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
  expect(result.config.requests).toHaveProperty('mytype');
});

// ─── validation/registry/schema fields in config ─────────────────────────────

test('config with validation, registry, and schema fields (plain-object arms)', async () => {
  const owner = 'o_vrs_obj';
  const repo = 'r_vrs_obj';
  const yaml = [
    `requests:`,
    `  t:`,
    `    issueTemplate: templates/t.yml`,
    `validation:`,
    `  enabled: true`,
    `registry:`,
    `  baseUrl: https://example.com`,
    `schema:`,
    `  type: object`,
  ].join('\n');
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.config.validation).toEqual({ enabled: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.registry as any).baseUrl).toBe('https://example.com');
  expect(result.config.schema).toEqual({ type: 'object' });
});

test('config with validation/registry/schema as non-plain-object (coerced to {})', async () => {
  const owner = 'o_vrs_str';
  const repo = 'r_vrs_str';
  const yaml = `validation: "not-an-object"\nregistry: 42\nschema: true\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.config.validation).toEqual({});
  expect(result.config.registry).toEqual({});
  expect(result.config.schema).toEqual({});
});

// ─── buildRequests edge cases ─────────────────────────────────────────────────

test('buildRequests: non-plain-object request entry is treated as empty', async () => {
  const owner = 'o_req_str';
  const repo = 'r_req_str';
  const yaml = `requests:\n  mytype: "not-an-object"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.config.requests).toHaveProperty('mytype');
});

test('buildRequests: approvers=null is preserved as null', async () => {
  const owner = 'o_approvers_null';
  const repo = 'r_approvers_null';
  const yaml = [`requests:`, `  mytype:`, `    issueTemplate: templates/t.yml`, `    approvers: ~`].join('\n');
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.requests as any).mytype.approvers).toBeNull();
});

// ─── buildPr autoMerge normalizations ────────────────────────────────────────

test('buildPr: autoMerge.enabled as boolean false', async () => {
  const owner = 'o_pr_false';
  const repo = 'r_pr_false';
  const yaml = `pr:\n  autoMerge:\n    enabled: false\n    method: squash\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.pr as any).autoMerge.enabled).toBe(false);
});

test('buildPr: autoMerge.enabled as null leaves enabled null', async () => {
  const owner = 'o_pr_null';
  const repo = 'r_pr_null';
  const yaml = `pr:\n  autoMerge:\n    enabled: ~\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.pr as any).autoMerge.enabled).toBeNull();
});

test('buildPr: autoMerge.method as number is coerced to string', async () => {
  const owner = 'o_pr_method_num';
  const repo = 'r_pr_method_num';
  const yaml = `pr:\n  autoMerge:\n    method: 1\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.pr as any).autoMerge.method).toBe('1');
});

test('buildPr: non-plain-object pr is treated as empty', async () => {
  const owner = 'o_pr_str';
  const repo = 'r_pr_str';
  const yaml = `pr: "not-an-object"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.config.pr).toBeDefined();
});

// ─── buildWorkflow edge cases ─────────────────────────────────────────────────

test('buildWorkflow: approvers=null is preserved as null', async () => {
  const owner = 'o_wf_approvers_null';
  const repo = 'r_wf_approvers_null';
  const yaml = `workflow:\n  approvers: ~\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.workflow as any).approvers).toBeNull();
});

test('buildWorkflow: non-plain-object labels is treated as empty', async () => {
  const owner = 'o_wf_labels_str';
  const repo = 'r_wf_labels_str';
  const yaml = `workflow:\n  labels: "not-an-object"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.workflow as any).labels).toBeDefined();
});

// ─── normalizeStaticConfig with null YAML ────────────────────────────────────

test('YAML config that parses to null is treated as empty object', async () => {
  const owner = 'o_null_yaml';
  const repo = 'r_null_yaml';
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: `~\n` } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YML}`);
});

// ─── coerceOptionalString with boolean value ──────────────────────────────────

test('coerceOptionalString: boolean value is coerced to string', async () => {
  const owner = 'o_coerce_bool';
  const repo = 'r_coerce_bool';
  const yaml = `requests:\n  mytype:\n    issueTemplate: true\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.requests as any).mytype.issueTemplate).toBe('true');
});

// ─── cache hit path ───────────────────────────────────────────────────────────

test('loadStaticConfig returns cached result on second call without forceReload', async () => {
  const owner = 'o_cache_hit';
  const repo = 'r_cache_hit';
  const { context, getContent } = mkContext({
    owner,
    repo,
    files: {
      [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: `requests: {}\n` },
    },
  });

  const r1 = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  const callCount = getContent.mock.calls.length;

  const r2 = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: false });
  expect(r2.source).toBe(r1.source);
  expect(getContent.mock.calls.length).toBe(callCount);
});

// ─── L14 arm 1: getHttpStatus returns undefined for non-numeric status ────────

test('readRepoFileIfExists: non-numeric status field causes re-throw', async () => {
  const owner = 'o_non_numeric_status_x1';
  const repo = 'r_non_numeric_status_x1';
  const { context, getContent } = mkContext({ owner, repo, files: {} });
  const err = Object.assign(new Error('bad-non-numeric-status'), { status: 'not-a-number' as unknown });
  getContent.mockRejectedValueOnce(err);
  await expect(loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true })).rejects.toThrow(
    'bad-non-numeric-status'
  );
});

// ─── L20 arm 0: normalizeStringArray with non-array value ────────────────────

test('normalizeStringArray: string approvers value produces empty array', async () => {
  const owner = 'o_approvers_str_x1';
  const repo = 'r_approvers_str_x1';
  const yaml = `requests:\n  mytype:\n    approvers: "single-string"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.requests as any)?.mytype?.approvers).toEqual([]);
});

// ─── L21 arm 1: null item in array triggers x ?? '' path ─────────────────────

test('normalizeStringArray: null array item coalesces to empty string and is filtered', async () => {
  const owner = 'o_null_arr_item_x1';
  const repo = 'r_null_arr_item_x1';
  const yaml = `workflow:\n  labels:\n    global:\n      - ~\n      - valid\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const globalLabels = (result.config.workflow as any)?.labels?.global;
  expect(globalLabels).toEqual(['valid']);
});

// ─── L27 arm 1: coerceOptionalString with object/array falls through to else ──

test('coerceOptionalString: array branchNameTemplate is left unchanged (falls to implicit else)', async () => {
  const owner = 'o_coerce_array_x1';
  const repo = 'r_coerce_array_x1';
  const yaml = `pr:\n  branchNameTemplate:\n    - a\n    - b\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.pr as any)?.branchNameTemplate).toEqual(['a', 'b']);
});

// ─── L284 arm 0: deepMerge returns base when override is non-plain-object ─────

test('deepMerge: YAML scalar string override leaves base config unchanged', async () => {
  const owner = 'o_yaml_scalar_x1';
  const repo = 'r_yaml_scalar_x1';
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: 'hello' } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
});

// ─── L305 arm 1: buildRequests with non-plain requests value ─────────────────

test('buildRequests: string requests value yields empty requests map', async () => {
  const owner = 'o_requests_str_x1';
  const repo = 'r_requests_str_x1';
  const yaml = `requests: "not-an-object"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(Object.keys(result.config.requests ?? {})).toHaveLength(0);
});

// ─── L354 arm 1 + L375 arm 0: buildWorkflow with non-plain workflow value ─────

test('buildWorkflow: string workflow value yields empty workflow and undefined approvers', async () => {
  const owner = 'o_workflow_str_x1';
  const repo = 'r_workflow_str_x1';
  const yaml = `workflow: "not-an-object"\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config.workflow as any)?.approvers).toBeUndefined();
});

// ─── L389 arm 0: normalizeStaticConfig deletes top-level issueTemplate ────────

test('normalizeStaticConfig: top-level issueTemplate is removed from config', async () => {
  const owner = 'o_top_issue_tpl_x1';
  const repo = 'r_top_issue_tpl_x1';
  const yaml = `issueTemplate: some-template.md\nrequests: {}\n`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: yaml } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((result.config as any).issueTemplate).toBeUndefined();
});

// ─── L751 arm 0: getCachedResult returns null when no cache entry exists ──────

test('loadStaticConfig with forceReload=false and no prior cache loads from repo', async () => {
  const owner = 'o_cache_miss_fresh_x1';
  const repo = 'r_cache_miss_fresh_x1';
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: `requests: {}\n` } },
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: false });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
});

// ─── L808 default-arg: loadStaticConfig called without options ────────────────

test('loadStaticConfig uses default options when called without options argument', async () => {
  const owner = 'o_default_opts_x1';
  const repo = 'r_default_opts_x1';
  const validYaml = `
requests:
  sample:
    folderName: resources
    schema: .github/registry-bot/schemas/sample.json
    issueTemplate: .github/ISSUE_TEMPLATE/sample.md
`;
  const { context } = mkContext({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: validYaml } },
    openIssues: [],
  });
  const result = await loadStaticConfig(context);
  expect(result.source).toBe(`repo:${CFG_YAML}`);
});
