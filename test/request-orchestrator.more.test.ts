/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { jest } from '@jest/globals';

const PREV_DEBUG_NS = process.env.DEBUG_NS;
process.env.DEBUG_NS = '1';

afterAll(() => {
  process.env.DEBUG_NS = PREV_DEBUG_NS;
});

const setStateLabel = jest.fn(async () => {});
const ensureAssigneesOnce = jest.fn(async () => {});
type PostOnceFn = (ctx: any, params: any, body: string, options?: any) => Promise<void>;

const postOnce = jest.fn<PostOnceFn>(async (_ctx, _params, _body, _options) => {});

const loadTemplate = jest.fn(async () => ({}));
const parseForm = jest.fn(() => ({}));
const validateRequestIssue = jest.fn(async () => ({}));
const calcSnapshotHash = jest.fn(() => 'h1');
const extractHashFromPrBody = jest.fn(() => 'h1');
type FindOpenIssuePrsFn = (
  context: any,
  repo: { owner: string; repo: string },
  issueNumber: number
) => Promise<{ number: number; body?: string | null; head: { ref: string; sha: string } }[]>;

const findOpenIssuePrs = jest.fn<FindOpenIssuePrsFn>(async () => []);

const createRequestPr = jest.fn(async () => ({ number: 10 }));
const tryMergeIfGreen = jest.fn(async () => {});
const loadStaticConfig = jest.fn(async () => ({}));
const getDocLinksFromConfig = jest.fn(() => '');

const DEFAULT_CONFIG = {
  workflow: { labels: {}, approvers: [] },
} as any;

let requestHandler: any;

function httpErr(status: number): Error & { status: number } {
  const e = new Error(String(status)) as Error & { status: number };
  e.status = status;
  return e;
}

function mkApp() {
  const handlers: Record<string, any[]> = {};
  const app: any = {
    log: {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
    on: (events: string | string[], fn: any) => {
      const arr = Array.isArray(events) ? events : [events];
      for (const e of arr) {
        handlers[e] = handlers[e] || [];
        handlers[e].push(fn);
      }
    },
  };
  return { app, handlers };
}

type IssuesGetFn = (args: { owner: string; repo: string; issue_number: number }) => Promise<{ data: unknown }>;
type IssuesUpdateFn = (args: unknown) => Promise<unknown>;
type IssuesAddLabelsFn = (args: unknown) => Promise<unknown>;
type IssuesRemoveLabelFn = (args: unknown) => Promise<unknown>;

type PullsListFn = (args: unknown) => Promise<{ data: unknown[] }>;
type PullsUpdateFn = (args: unknown) => Promise<unknown>;

type ChecksListAnnotationsFn = (args: any) => Promise<{ data: any[] }>;
type ChecksListForSuiteFn = (args: any) => Promise<{ data: { check_runs: any[] } }>;

type PullsGetFn = (args: unknown) => Promise<{ data: unknown }>;

type GitDeleteRefFn = (args: unknown) => Promise<unknown>;
type ReposGetContentFn = (args: unknown) => Promise<unknown>;

function mkBaseContext(args: { owner?: string; repo?: string; issue?: any; withCachedConfig?: boolean; config?: any }) {
  const owner = args.owner ?? 'o';
  const repo = args.repo ?? 'r';
  const issue = args.issue ?? {
    number: 1,
    title: 't',
    body: 'b',
    labels: [],
    user: { login: 'u' },
  };

  const ctx: any = {
    name: 'x',
    payload: {},
    log: {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
    repo: () => ({ owner, repo }),
    issue: () => ({ owner, repo, issue_number: issue.number }),
    octokit: {
      issues: {
        get: jest.fn<IssuesGetFn>(() => Promise.resolve({ data: issue })),
        update: jest.fn<IssuesUpdateFn>(() => Promise.resolve({})),
        addLabels: jest.fn<IssuesAddLabelsFn>(() => Promise.resolve({})),
        removeLabel: jest.fn<IssuesRemoveLabelFn>(() => Promise.resolve({})),
      },
      pulls: {
        get: jest.fn<PullsGetFn>((args: any) =>
          Promise.resolve({
            data: {
              number: Number(args?.pull_number ?? 5),
              node_id: 'PR_NODE',
              state: 'open',
              draft: false,
              body: 'source: #1',
              head: { ref: 'x', sha: 'sha1' },
            },
          })
        ),
        list: jest.fn<PullsListFn>(() => Promise.resolve({ data: [] })),
        update: jest.fn<PullsUpdateFn>(() => Promise.resolve({})),
      },
      git: {
        deleteRef: jest.fn<GitDeleteRefFn>(() => Promise.resolve({})),
      },
      repos: {
        getContent: jest.fn<ReposGetContentFn>(() => Promise.resolve({})),
      },
      checks: {
        listAnnotations: jest.fn<ChecksListAnnotationsFn>().mockResolvedValue({ data: [] as any[] }),
        listForSuite: jest.fn<ChecksListForSuiteFn>().mockResolvedValue({ data: { check_runs: [] } }),
      },
    },
  };

  if (args.withCachedConfig) {
    ctx.resourceBotConfig = args.config ?? DEFAULT_CONFIG;
    ctx.resourceBotHooks = null;
    ctx.resourceBotHooksSource = null;
  }

  return ctx;
}

function mkCommentContext(args: {
  event: 'issue_comment.created' | 'issue_comment.edited';
  issue: any;
  comment: any;
  sender?: any;
  withCachedConfig?: boolean;
  config?: any;
}) {
  const ctx = mkBaseContext({
    issue: args.issue,
    withCachedConfig: args.withCachedConfig,
    config: args.config,
  });
  ctx.name = args.event;
  ctx.payload = {
    action: args.event.endsWith('created') ? 'created' : 'edited',
    issue: args.issue,
    comment: args.comment,
    sender: args.sender ?? { type: 'User', login: 'someone' },
  };
  return ctx;
}

function mkCheckSuiteContext(args: {
  event: 'check_suite.completed' | 'check_run.completed';
  conclusion: string;
  sha: string;
  ownerLogin: string;
  repoName: string;
  withCachedConfig?: boolean;
  config?: any;
}) {
  const ctx = mkBaseContext({
    owner: args.ownerLogin,
    repo: args.repoName,
    withCachedConfig: args.withCachedConfig,
    config: args.config,
  });
  ctx.name = args.event;
  ctx.payload =
    args.event === 'check_suite.completed'
      ? {
          check_suite: {
            id: 123,
            conclusion: args.conclusion,
            head_sha: args.sha,
            pull_requests: [{ number: 77 }],
          },
          repository: { name: args.repoName, owner: { login: args.ownerLogin } },
        }
      : {
          check_run: { conclusion: args.conclusion, head_sha: args.sha },
          repository: { name: args.repoName, owner: { login: args.ownerLogin } },
        };
  return ctx;
}

function mkStatusContext(args: {
  state: string;
  sha: string;
  ownerLogin: string;
  repoName: string;
  withCachedConfig?: boolean;
  config?: any;
}) {
  const ctx = mkBaseContext({
    owner: args.ownerLogin,
    repo: args.repoName,
    withCachedConfig: args.withCachedConfig,
    config: args.config,
  });
  ctx.name = 'status';
  ctx.payload = {
    state: args.state,
    sha: args.sha,
    repository: { name: args.repoName, owner: { login: args.ownerLogin } },
  };
  return ctx;
}

beforeAll(async () => {
  await jest.unstable_mockModule('../src/handlers/request/state.js', () => ({
    setStateLabel,
    ensureAssigneesOnce,
  }));

  await jest.unstable_mockModule('../src/handlers/request/comments.js', () => ({
    postOnce,
  }));

  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    loadTemplate,
    parseForm,
  }));

  await jest.unstable_mockModule('../src/handlers/request/validation/run.js', () => ({
    validateRequestIssue,
  }));

  await jest.unstable_mockModule('../src/handlers/request/pr/snapshot.js', () => ({
    calcSnapshotHash,
    extractHashFromPrBody,
    findOpenIssuePrs,
  }));

  await jest.unstable_mockModule('../src/handlers/request/pr/create.js', () => ({
    createRequestPr,
  }));

  await jest.unstable_mockModule('../src/lib/auto-merge.js', () => ({
    tryMergeIfGreen,
  }));

  await jest.unstable_mockModule('../src/config.js', () => ({
    DEFAULT_CONFIG,
    loadStaticConfig,
  }));

  await jest.unstable_mockModule('../src/handlers/request/constants.js', () => ({
    getDocLinksFromConfig,
  }));

  const mod = await import('../src/handlers/request/index.js');
  requestHandler = mod.default;
});

beforeEach(() => {
  jest.clearAllMocks();

  loadStaticConfig.mockResolvedValue({
    config: DEFAULT_CONFIG,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  getDocLinksFromConfig.mockReturnValue('');

  loadTemplate.mockResolvedValue({
    title: 'Request',
    name: 'Request',
    body: [],
    labels: [],
    _meta: {
      requestType: 'product',
      root: 'resources',
      schema: 'schema.json',
      path: '.github/ISSUE_TEMPLATE/x.yml',
    },
  });

  parseForm.mockReturnValue({ 'product-id': 'ABC' });

  validateRequestIssue.mockResolvedValue({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'ABC',
    nsType: 'product',
  });

  calcSnapshotHash.mockReturnValue('h1');
  extractHashFromPrBody.mockReturnValue('h1');
  findOpenIssuePrs.mockResolvedValue([]);
  createRequestPr.mockResolvedValue({ number: 10 });
});

test('issue_comment: skips bot sender', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    sender: { type: 'Bot', login: 'x[bot]' },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(loadTemplate).not.toHaveBeenCalled();
  expect(postOnce).not.toHaveBeenCalled();
});

test('issue_comment: template load error -> returns (no post)', async () => {
  loadTemplate.mockRejectedValueOnce(new Error('boom'));

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).not.toHaveBeenCalled();
});

test('issue_comment: not a request issue -> skip', async () => {
  parseForm.mockReturnValueOnce({});

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(validateRequestIssue).not.toHaveBeenCalled();
  expect(postOnce).not.toHaveBeenCalled();
});

test('issue_comment: approval ignored when review label missing after auto-add attempt', async () => {
  const cfg = {
    workflow: {
      labels: { approvalRequested: ['needs-review'] },
      approvers: ['alice'],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const issue = { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } };
  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
    config: cfg,
  });

  ctx.octokit.issues.get.mockResolvedValueOnce({ data: { ...issue, labels: [] } });

  await handler(ctx);

  expect(ctx.octokit.issues.addLabels).toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('Approval ignored: review label missing');
});

test('issue_comment: approval ignored for non-approver when approvers configured', async () => {
  const cfg = {
    workflow: {
      labels: { approvalRequested: ['needs-review'] },
      approvers: ['alice'],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: {
      number: 1,
      title: 't',
      body: 'b',
      labels: [{ name: 'needs-review' }],
      user: { login: 'u' },
    },
    comment: { body: 'Approved', user: { login: 'bob' } },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('not an allowed approver');
});

test('issue_comment: approval ignored for self-approve when no approvers configured', async () => {
  const cfg = {
    workflow: {
      labels: {},
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'bob' } },
    comment: { body: 'Approved', user: { login: 'bob' } },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('not allowed to self-approve');
});

test('issue_comment: approval keyword from config triggers approval detection', async () => {
  const cfg = {
    workflow: {
      labels: { approvalSuccessful: ['ship it'] },
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
    comment: { body: '> quote\n\nShip it please', user: { login: 'alice' } },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('Opened PR');
});

test('issue_comment: approval fails when resource name missing', async () => {
  parseForm.mockReturnValueOnce({ foo: 'x' });

  const cfg = { workflow: { labels: {}, approvers: [] } };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('Cannot create PR: missing resource name');
});

test('issue_comment: approval short-circuits when PR already exists', async () => {
  findOpenIssuePrs.mockResolvedValueOnce([{ number: 9, body: 'x', head: { ref: 'r', sha: 's' } }]);

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(createRequestPr).not.toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('PR already open: #9');
});

test('issue_comment: approval create PR failure -> posts error', async () => {
  createRequestPr.mockRejectedValueOnce(new Error('boom'));

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('Failed to create PR automatically: boom');
});

test('issue_comment: author update comment triggers revalidation errors -> posts + author state', async () => {
  validateRequestIssue.mockResolvedValueOnce({
    errors: ['x'],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '- x',
    namespace: 'ABC',
    nsType: 'product',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
    comment: { body: 'updated', user: { login: 'author' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('## Detected issues');
  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'author');
});

test('issue_comment: author update (subcontext) missing parent -> posts + author state', async () => {
  loadTemplate.mockResolvedValueOnce({
    title: 'Request',
    name: 'Request',
    body: [],
    labels: [],
    _meta: { requestType: 'subcontext', root: '/resources/', schema: 's', path: 'p' },
  });

  parseForm.mockReturnValue({ namespace: 'a.b.c' });

  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'a.b.c',
    nsType: 'subcontext',
    template: {
      title: 'Request',
      name: 'Request',
      body: [],
      labels: [],
      _meta: { requestType: 'subcontext', root: '/resources/', schema: 's', path: 'p' },
    },
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const issue = { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } };
  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'updated', user: { login: 'author' } },
    withCachedConfig: true,
  });

  ctx.octokit.repos.getContent.mockImplementation(async (args: any) => {
    const p = String(args?.path || '');
    if (p.includes('a.b.yaml')) throw httpErr(404);
    return { data: {} };
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain("Parent resource 'a.b'");
  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'author');
});

test('issue_comment: author update closes outdated PRs and hands over', async () => {
  const cfg = {
    workflow: {
      labels: {
        approvalSuccessful: ['approved-label'],
        approvalRequested: ['needs-review'],
        global: ['registry-bot'],
      },
      approvers: ['alice'],
    },
  };

  loadTemplate.mockResolvedValueOnce({
    title: 'Request',
    name: 'Request',
    body: [],
    labels: [],
    _meta: { requestType: 'subcontext', root: 'resources', schema: 's', path: 'p' },
  });

  parseForm.mockReturnValue({ namespace: 'a.b.c' });

  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'a.b.c',
    nsType: 'subcontext',
    template: {
      title: 'Request',
      name: 'Request',
      body: [],
      labels: [],
      _meta: { requestType: 'subcontext', root: 'resources', schema: 's', path: 'p' },
    },
  });

  calcSnapshotHash.mockReturnValue('h_new');

  findOpenIssuePrs.mockResolvedValueOnce([
    { number: 11, body: 'old', head: { ref: 'ref-old', sha: 'x' } },
    { number: 12, body: 'new', head: { ref: 'ref-new', sha: 'x' } },
  ]);

  extractHashFromPrBody.mockImplementationOnce(() => 'oldhash').mockImplementationOnce(() => 'h_new');

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const issue = {
    number: 1,
    title: 't',
    body: 'b',
    labels: [{ name: 'approved-label' }],
    user: { login: 'author' },
  };
  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'updated', user: { login: 'author' } },
    withCachedConfig: true,
    config: cfg,
  });

  ctx.octokit.issues.get.mockResolvedValueOnce({ data: issue });

  await handler(ctx);

  expect(ctx.octokit.pulls.update).toHaveBeenCalled();
  expect(ctx.octokit.git.deleteRef).toHaveBeenCalled();

  expect(postOnce).toHaveBeenCalled();
  const bodies = postOnce.mock.calls.map((c: any[]) => String(c[2]));
  expect(bodies.some((b) => b.includes('Form updated → closing outdated PR(s)'))).toBe(true);
  expect(bodies.some((b) => b.includes('### ✅ No issues detected'))).toBe(true);
});

test('check_suite.success merges when hashes match', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'success',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.pull_requests = [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }];

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: {
      number: 1,
      title: 't',
      body: 'b',
      labels: [{ name: 'approved-label' }],
      user: { login: 'author' },
    },
  });

  calcSnapshotHash.mockReturnValue('h1');
  extractHashFromPrBody.mockReturnValue('h1');

  await handler(ctx);

  expect(tryMergeIfGreen).toHaveBeenCalledWith(ctx, expect.objectContaining({ prNumber: 5, mergeMethod: 'squash' }));
});

test('check_suite.success closes outdated when hashes mismatch', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'success',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [{ number: 5, body: 'issue #1', head: { ref: 'x', sha: 'sha1' } }],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: {
      number: 1,
      title: 't',
      body: 'b',
      labels: [{ name: 'approved-label' }],
      user: { login: 'author' },
    },
  });

  calcSnapshotHash.mockReturnValue('h1');
  extractHashFromPrBody.mockReturnValue('old');

  findOpenIssuePrs.mockResolvedValueOnce([{ number: 9, body: 'old', head: { ref: 'ref-old', sha: 'x' } }]);

  await handler(ctx);

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.update).toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
});

test('check_suite.completed failure posts PR comment when registry-validate annotations exist', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 500;
  ctx.payload.check_suite.pull_requests = [{ number: 42 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 9001, html_url: 'https://example/check/9001' }] },
  });

  ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
    data: [
      {
        path: 'data/namespaces/sap.css.yaml',
        title: 'registry-validate systemNamespace',
        message:
          "Property 'contact' is required for System. [file=data/namespaces/sap.css.yaml schema=.github/registry-bot/request-schemas/system-namespace.schema.json requestType=systemNamespace]",
        annotation_level: 'failure',
      },
    ],
  });
  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.com/o1/r1/pull/42' },
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalledTimes(1);
  const [, params, body] = (postOnce as jest.Mock).mock.calls[0];

  expect(params).toEqual({ owner: 'o1', repo: 'r1', issue_number: 42 });
  expect(String(body)).toContain('## Detected issues: data/namespaces/sap.css.yaml');
  expect(String(body)).toContain('### Contacts');
  expect(String(body)).toContain("Property 'contact' is required for System.");

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('check_suite.completed failure does nothing if there are no registry-validate annotations', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx: any = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'deadbeef',
    ownerLogin: 'o',
    repoName: 'r',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 55;
  ctx.payload.check_suite.pull_requests = [{ number: 7 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 123, html_url: 'https://example/check/123' }] },
  });

  ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
    data: [
      { title: 'lint', path: 'x.yaml', message: 'some lint error' },
      { title: 'build', path: 'y.yaml', message: 'some build error' },
    ],
  });

  await handler(ctx);

  expect(ctx.octokit.checks.listAnnotations).toHaveBeenCalledTimes(1);
  expect(postOnce).not.toHaveBeenCalled();
});

test('check_run.failure skips if check_run.id is missing', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const ctx: any = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'failure',
    sha: 'deadbeef',
    ownerLogin: 'o',
    repoName: 'r',
    withCachedConfig: true,
  });

  ctx.payload.check_run = {
    // id missing on purpose
    conclusion: 'failure',
    html_url: 'https://example/check/no-id',
    pull_requests: [{ number: 7 }],
  };

  ctx.octokit.checks = {
    listAnnotations: jest.fn(),
  };

  await handlers['check_run.completed'][0](ctx);

  expect(ctx.octokit.checks.listAnnotations).not.toHaveBeenCalled();
  expect(postOnce).not.toHaveBeenCalled();
});

test('check_suite.completed failure stops listing annotations after 20 pages (safety cap)', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx: any = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'deadbeef',
    ownerLogin: 'o',
    repoName: 'r',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 999;
  ctx.payload.check_suite.pull_requests = [{ number: 7 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 777, html_url: 'https://example/check/777' }] },
  });

  const pageData = Array.from({ length: 100 }, () => ({
    title: 'registry-validate',
    path: 'data/namespaces/a.yaml',
    message: '/name error [file=data/namespaces/a.yaml]',
  }));

  ctx.octokit.checks.listAnnotations.mockResolvedValue({ data: pageData });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://example/pr/7' },
  });

  await handler(ctx);

  // 20 pages max in listAllCheckRunAnnotations
  expect(ctx.octokit.checks.listAnnotations).toHaveBeenCalledTimes(20);
  expect(postOnce).toHaveBeenCalled();
});

test('check_suite ignored when conclusion != success', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('status success triggers tryAutoMerge flow', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['status'][0];

  const ctx = mkStatusContext({
    state: 'success',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
  });

  calcSnapshotHash.mockReturnValue('h1');
  extractHashFromPrBody.mockReturnValue('h1');

  await handler(ctx);

  expect(tryMergeIfGreen).toHaveBeenCalled();
});

test('status ignored when state != success', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['status'][0];

  const ctx = mkStatusContext({
    state: 'failure',
    sha: 'sha1',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});
