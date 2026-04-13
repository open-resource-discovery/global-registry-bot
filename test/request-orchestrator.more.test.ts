/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { afterAll, beforeAll, beforeEach, describe, expect, jest, test } from '@jest/globals';

const PREV_DEBUG_NS = process.env.DEBUG_NS;
process.env.DEBUG_NS = '1';

afterAll(() => {
  process.env.DEBUG_NS = PREV_DEBUG_NS;
});

type IssueParams = { owner: string; repo: string; issue_number: number };
const setStateLabel = jest.fn(async (_ctx: any, _params: any, _options: any, _state: any) => {});
const ensureAssigneesOnce = jest.fn(async () => {});
type PostOnceFn = (ctx: any, params: any, body: string, options?: any) => Promise<void>;

const postOnce = jest.fn<PostOnceFn>(async (_ctx, _params, _body, _options) => {});

type CollapseBotCommentsByPrefix = (
  ctx: unknown,
  params: IssueParams,
  opts: { perPage?: number; tagPrefix: string; keepTags?: string[]; collapseBody?: string; classifier?: string }
) => Promise<void>;

const collapseBotCommentsByPrefix = jest.fn() as unknown as jest.MockedFunction<CollapseBotCommentsByPrefix>;

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

type RunApprovalHookFn = (ctx: any, repo: { owner: string; repo: string }, opts: any) => Promise<boolean>;
const runApprovalHook = jest.fn<RunApprovalHookFn>(async () => false);

const createRequestPr = jest.fn(async () => ({ number: 10 }));
const tryMergeIfGreen = jest.fn(async (_ctx: any, _opts: any) => {});
const loadStaticConfig = jest.fn(async () => ({}));
const getDocLinksFromConfig = jest.fn(() => '');

const DEFAULT_CONFIG = {
  workflow: { labels: {}, approvers: [] },
} as any;

let requestHandler: any;

function postedBodies(): string {
  return postOnce.mock.calls.map((call) => call[2] ?? '').join('\n');
}

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
type PullsCreateReviewFn = (args: unknown) => Promise<unknown>;
type PullsListFilesFn = (args: unknown) => Promise<{ data: unknown[] }>;

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
        listFiles: jest.fn<PullsListFilesFn>(() => Promise.resolve({ data: [] })),
        createReview: jest.fn<PullsCreateReviewFn>(() => Promise.resolve({})),
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

function mkIssuesContext(args: {
  issue: any;
  action: 'opened' | 'edited' | 'reopened';
  sender?: any;
  changes?: any;
  withCachedConfig?: boolean;
  config?: any;
}) {
  const ctx = mkBaseContext({
    issue: args.issue,
    withCachedConfig: args.withCachedConfig,
    config: args.config,
  });
  ctx.name = 'issues';
  ctx.payload = {
    action: args.action,
    issue: args.issue,
    sender: args.sender ?? { type: 'User', login: 'someone' },
    changes: args.changes ?? {},
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
    collapseBotCommentsByPrefix,
  }));

  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    loadTemplate,
    parseForm,
  }));

  await jest.unstable_mockModule('../src/handlers/request/validation/run.js', () => ({
    validateRequestIssue,
    runApprovalHook,
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

  runApprovalHook.mockResolvedValue(false);
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

  expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('Approval ignored: request is not in review state.');
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
    comment: { body: '> quote\n\nShip it', user: { login: 'alice' } },
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
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/42' },
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

describe('parent owner approval gating', () => {
  function b64(s: string): string {
    return Buffer.from(s, 'utf8').toString('base64');
  }

  test('gates sub-namespace request and asks owners of the immediate parent namespace', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 550,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'requester' },
      state: 'open',
    };

    const tpl = {
      _meta: {
        requestType: 'subContextNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/sub-context-namespace.schema.json',
      },
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ identifier: target, description: 'x' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: target,
      nsType: 'subContextNamespace',
      template: tpl,
      formData: { identifier: target, description: 'x' },
    });

    const ctx = mkIssuesContext({ issue, action: 'opened' });

    // Parent chain exists, but owners differ per level.
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;

    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64(topYaml), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return { data: { content: b64(barYaml), encoding: 'base64' } };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await h['issues.opened'][0](ctx);

    // Marker is written to the issue body.
    expect(issue.body).toContain('nsreq:parent-approval');
    expect(issue.body).toContain('"parent":"sap.css.bar"');
    expect(issue.body).toContain(`"target":"${target}"`);

    // Bot asks the *immediate* parent owners (sap.css.bar), not sap.css.
    const posted = postedBodies();
    expect(posted).toContain('@barOwner');
    expect(posted).not.toContain('@topOwner');

    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
  });

  test('does not gate when the requester is already an owner of the parent namespace', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar';
    const issue = {
      number: 551,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'barOwner' },
      state: 'open',
    };

    const tpl = {
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ identifier: target, description: 'x' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: target,
      nsType: 'subContextNamespace',
      template: tpl,
      formData: { identifier: target, description: 'x' },
    });

    const ctx = mkIssuesContext({ issue, action: 'opened' });
    const parentYaml = `contacts:\n  - "@barOwner"\n`;

    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64(parentYaml), encoding: 'base64' } };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await h['issues.opened'][0](ctx);

    const posted = postedBodies();
    expect(posted).not.toContain('Parent owner approval required');
    expect(ensureAssigneesOnce).toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
  });

  test('ignores Approved comments from non-owners while gated', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 552,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'requester' },
      state: 'open',
    };

    const tpl = {
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ identifier: target, description: 'x' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: target,
      nsType: 'subContextNamespace',
      template: tpl,
      formData: { identifier: target, description: 'x' },
    });

    // Gate first
    const openCtx = mkIssuesContext({ issue, action: 'opened' });
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;
    (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/namespaces/sap.css.yaml') return { data: { content: b64(topYaml), encoding: 'base64' } };
      if (path === 'data/namespaces/sap.css.bar.yaml') return { data: { content: b64(barYaml), encoding: 'base64' } };
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });
    await h['issues.opened'][0](openCtx);

    // Now a random user tries to approve
    (postOnce as jest.Mock).mockClear();
    (ensureAssigneesOnce as jest.Mock).mockClear();

    const commentCtx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { type: 'User', login: 'randomUser' } },
    });

    await h['issue_comment.created'][0](commentCtx);

    const posted = postedBodies();
    expect(posted).toContain('Approval ignored');
    expect(posted).toContain('@barOwner');
    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
    expect(createRequestPr).not.toHaveBeenCalled();
  });

  test('accepts Approved comments from parent owners and then hands over to CPA review', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 553,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'requester' },
      state: 'open',
    };

    const tpl = {
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ identifier: target, description: 'x' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: target,
      nsType: 'subContextNamespace',
      template: tpl,
      formData: { identifier: target, description: 'x' },
    });

    // Gate first
    const openCtx = mkIssuesContext({ issue, action: 'opened' });
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;
    (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/namespaces/sap.css.yaml') return { data: { content: b64(topYaml), encoding: 'base64' } };
      if (path === 'data/namespaces/sap.css.bar.yaml') return { data: { content: b64(barYaml), encoding: 'base64' } };
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });
    await h['issues.opened'][0](openCtx);

    // Owner approves
    (postOnce as jest.Mock).mockClear();
    (ensureAssigneesOnce as jest.Mock).mockClear();
    (setStateLabel as jest.Mock).mockClear();

    const commentCtx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { type: 'User', login: 'barOwner' } },
    });

    await h['issue_comment.created'][0](commentCtx);

    // Body marker should now be enriched with approvedBy
    expect(issue.body).toContain('"approvedBy":"barOwner"');

    const posted = postedBodies();
    expect(posted).toContain('Parent namespace approved by @barOwner');
    expect(posted).toContain('Routing to an approver');

    expect(ensureAssigneesOnce).toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
    expect(createRequestPr).not.toHaveBeenCalled();
  });
  test('issue_comment: review feedback mentioning approved does not trigger approval', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 1,
      title: 'System Namespace: sap.0sac',
      body: `### Namespace

  sap.0sac

  ### System Description

  \`\`\`text
  Example system namespace used for testing request validation behavior
  \`\`\`

  ### Contacts

  \`\`\`text
  someone.else@sap.com
  \`\`\`

  ### CLD System Role

  _No response_

  ### STC Service ID

  _No response_

  ### PPMS Product Object Number

  _No response_

  ### Visibility

  public

  <!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: {
        body: `Thanks for the update.
  Please review the remaining details and align them with the expected naming rules.
  After all requested changes are completed, the responsible reviewer can post "Approved" as a separate comment.`,
        user: { login: 'alice' },
      },
      withCachedConfig: true,
      config: cfg,
    });

    await handler(ctx);

    expect(createRequestPr).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.stringContaining('Opened PR'),
      expect.anything()
    );
    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalledWith(
      expect.objectContaining({
        labels: expect.arrayContaining(['Approved']),
      })
    );
  });

  test('issue_comment: explicit approved does not create PR when validation still fails', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 2,
      title: 'System Namespace: sap.0sac',
      body: `### Namespace

  sap.0sac

  ### System Description

  \`\`\`text
  Example system namespace used for testing request validation behavior
  \`\`\`

  ### Contacts

  \`\`\`text
  someone.else@sap.com
  \`\`\`

  ### CLD System Role

  _No response_

  ### STC Service ID

  _No response_

  ### PPMS Product Object Number

  _No response_

  ### Visibility

  public

  <!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    validateRequestIssue.mockResolvedValueOnce({
      errors: ['namespace format is invalid'],
      errorsGrouped: null,
      errorsFormatted: '',
      errorsFormattedSingle: '- namespace format is invalid',
      namespace: 'sap.0sac',
      nsType: 'product',
    });

    await handler(ctx);

    expect(createRequestPr).not.toHaveBeenCalled();
    expect(postOnce).toHaveBeenCalled();
    expect(postOnce.mock.calls.some((c) => String(c[2] ?? '').includes('Detected issues'))).toBe(true);
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
  });

  test('issue_comment: explicit approved creates PR only after request is valid', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 3,
      title: 'System Namespace: sap.sac',
      body: `### Namespace

  sap.sac

  ### System Description

  \`\`\`text
  Example system namespace used for testing request validation behavior
  \`\`\`

  ### Contacts

  \`\`\`text
  someone.else@sap.com
  another.person@sap.com
  \`\`\`

  ### CLD System Role

  _No response_

  ### STC Service ID

  _No response_

  ### PPMS Product Object Number

  _No response_

  ### Visibility

  public

  <!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    createRequestPr.mockResolvedValueOnce({ number: 123 });

    await handler(ctx);

    expect(createRequestPr).toHaveBeenCalled();
    expect(postOnce.mock.calls.some((c) => String(c[2] ?? '').includes('Approved by @alice. Opened PR: #123'))).toBe(
      true
    );
  });
  test('accepts Approved comments from parent owners and auto-approves via onApproval hook before review handover', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 554,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'requester' },
      state: 'open',
    };

    const tpl = {
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ identifier: target, description: 'x' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: target,
      nsType: 'subContextNamespace',
      template: tpl,
      formData: { identifier: target, description: 'x' },
    } as any);

    const openCtx = mkIssuesContext({ issue, action: 'opened' });
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;

    (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64(topYaml), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return { data: { content: b64(barYaml), encoding: 'base64' } };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await h['issues.opened'][0](openCtx);

    (postOnce as jest.Mock).mockClear();
    (ensureAssigneesOnce as jest.Mock).mockClear();
    (createRequestPr as jest.Mock).mockClear();
    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    const commentCtx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { type: 'User', login: 'barOwner' } },
    });

    await h['issue_comment.created'][0](commentCtx);

    expect(runApprovalHook).toHaveBeenCalledWith(
      commentCtx,
      { owner: 'o', repo: 'r' },
      expect.objectContaining({ requestType: 'subContextNamespace', namespace: target })
    );
    expect(createRequestPr).toHaveBeenCalled();
    expect(ensureAssigneesOnce).not.toHaveBeenCalled();

    const posted = postedBodies();
    expect(posted).toContain('Opened PR: #10');
    expect(posted).not.toContain('Routing to an approver');
  });

  test('check_suite.success: direct PR without snapshot hash uses approved onApproval result and merges', async () => {
    const cfg = {
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

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
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({ data: [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }] })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: {
        number: 1,
        title: 'Request',
        body: 'Body',
        labels: [],
        user: { login: 'author' },
      },
    });

    extractHashFromPrBody.mockReturnValueOnce('');
    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalled();
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 5, mergeMethod: 'squash' })
    );
    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(expect.objectContaining({ labels: ['Approved'] }));
  });

  test('check_suite.success: standalone direct PR with changed registry yaml uses onApproval and merges', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 51,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/direct', sha: 'sha-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-one.yaml', status: 'modified' },
        { filename: 'README.md', status: 'modified' },
        { filename: 'resources/deleted.yaml', status: 'removed' },
      ],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from(
          'type: product\nname: product-one\ndescription: Example\ncontact: owner@example.com\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved from standalone hook' } as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.listFiles).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 51, per_page: 100, page: 1 })
    );
    expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', path: 'resources/product-one.yaml', ref: 'feature/direct' })
    );
    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-one',
        resourceName: 'product-one',
        formData: expect.objectContaining({
          identifier: 'product-one',
          namespace: 'product-one',
          contact: 'owner@example.com',
        }),
      })
    );
    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        pull_number: 51,
        event: 'APPROVE',
        body: 'approved from standalone hook',
      })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 51, mergeMethod: 'squash' })
    );
  });

  test('check_suite.success: standalone direct PR rejected by onApproval closes PR and posts rejection', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-reject-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 52,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/reject', sha: 'sha-reject-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-two.yaml', status: 'added' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-two\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'rejected',
      path: 'details',
      reason: 'policy denied',
      errors: [
        { field: 'details', message: 'policy denied' },
        { field: 'name', message: 'resource name requires additional review' },
      ],
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 52, state: 'closed' })
    );
    const posted = postedBodies();
    expect(posted).toContain('onApproval rejected this request');
    expect(posted).toContain('resource name requires additional review');
    expect(posted).toContain('policy denied');
  });

  test('check_suite.success: standalone direct PR unknown mix does not merge or post', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-unknown-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 53,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/unknown', sha: 'sha-unknown-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [
        { filename: 'resources/bad.yaml', status: 'modified' },
        { filename: 'resources/no-name.yaml', status: 'modified' },
      ],
    });

    ctx.octokit.repos.getContent
      .mockResolvedValueOnce({
        data: {
          content: Buffer.from('::: not yaml :::', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      })
      .mockResolvedValueOnce({
        data: {
          content: Buffer.from('type: product\ndescription: missing name\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

    await handler(ctx);

    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR approval review failure keeps PR open and does not merge', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-review-failure',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 54,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/review-fail', sha: 'sha-review-failure' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-review.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-review\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.createReview.mockRejectedValueOnce(new Error('review api failed'));
    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approve please' } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    const posted = postedBodies();
    expect(posted).toContain('automatic PR approval failed');
    expect(posted).toContain('approve please');
  });

  test('check_suite.success: direct PR without snapshot hash rejects and closes PR plus request', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const pr = { number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } };
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha1',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: {
        number: 1,
        title: 'Request',
        body: 'Body',
        labels: [],
        user: { login: 'author' },
        state: 'open',
      },
    });

    extractHashFromPrBody.mockReturnValueOnce('');
    findOpenIssuePrs.mockResolvedValueOnce([pr]);
    runApprovalHook.mockResolvedValueOnce({
      status: 'rejected',
      path: 'namespace',
      reason: 'policy denied',
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 5, state: 'closed' })
    );
    expect(ctx.octokit.issues.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 1, state: 'closed' })
    );

    const posted = postedBodies();
    expect(posted).toContain('onApproval rejected this request');
    expect(posted).toContain('policy denied');
  });

  test('check_suite.success: direct PR without snapshot hash posts unknown feedback and does not merge', async () => {
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
      .mockResolvedValueOnce({ data: [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }] })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: {
        number: 1,
        title: 'Request',
        body: 'Body',
        labels: [],
        user: { login: 'author' },
      },
    });

    extractHashFromPrBody.mockReturnValueOnce('');
    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      path: 'issue.author',
      reason: 'manual review required',
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 5 }),
      expect.stringContaining('## onApproval feedback'),
      expect.anything()
    );
    expect(postedBodies()).toContain('manual review required');
  });

  test('check_suite.success: standalone direct PR resolves request type from doc type and uses default approval review body', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
        vendor: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-vendor-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 55,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/vendor', sha: 'sha-vendor-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/vendor-one.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from(
          'type: vendor\nname: vendor-one\ncontact:\n  - owner1@example.com\n  - owner2@example.com\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'vendor',
        namespace: 'vendor-one',
        resourceName: 'vendor-one',
        formData: expect.objectContaining({
          name: 'vendor-one',
          identifier: 'vendor-one',
          namespace: 'vendor-one',
          contact: 'owner1@example.com\nowner2@example.com',
        }),
      })
    );
    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({ body: 'Approved automatically by onApproval hook.' })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 55, mergeMethod: 'squash' })
    );
  });

  test('check_suite.success: standalone direct PR skips merge when changed yaml cannot be read from repo ref', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-read-fail',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 56,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/read-fail', sha: 'sha-read-fail' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/unreadable.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockRejectedValueOnce(new Error('cannot load ref content'));

    await handler(ctx);

    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR ignores malformed and scalar yaml candidates', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-bad-yaml',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 57,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/bad-yaml', sha: 'sha-bad-yaml' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [
        { filename: 'resources/malformed.yaml', status: 'modified' },
        { filename: 'resources/scalar.yaml', status: 'modified' },
      ],
    });

    ctx.octokit.repos.getContent
      .mockResolvedValueOnce({
        data: {
          content: Buffer.from('type: [broken\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      })
      .mockResolvedValueOnce({
        data: {
          content: Buffer.from('plain scalar string', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

    await handler(ctx);

    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR paginates changed files and skips multi-match resources without matching doc type', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
        vendor: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-paginated-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 58,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/paginated', sha: 'sha-paginated-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles
      .mockResolvedValueOnce({
        data: [
          ...Array.from({ length: 99 }, (_, index) => ({ filename: `docs/file-${index}.md`, status: 'modified' })),
          { filename: 'resources/shared.yaml', status: 'modified' },
        ],
      })
      .mockResolvedValueOnce({
        data: [{ filename: 'resources/shared.yaml', status: 'modified' }],
      });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: unsupported\nname: shared-resource\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    await handler(ctx);

    expect(ctx.octokit.pulls.listFiles).toHaveBeenCalledTimes(2);
    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR with approved and unknown changed resources does not merge', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: { approvalSuccessful: ['Approved'] },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-mixed-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 59,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/mixed', sha: 'sha-mixed-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-ok.yaml', status: 'modified' },
        { filename: 'resources/product-missing.yaml', status: 'modified' },
      ],
    });

    ctx.octokit.repos.getContent
      .mockResolvedValueOnce({
        data: {
          content: Buffer.from('type: product\nname: product-ok\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      })
      .mockRejectedValueOnce(new Error('file missing'));

    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved product-ok' } as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });
});
