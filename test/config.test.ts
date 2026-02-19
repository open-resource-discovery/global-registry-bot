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
