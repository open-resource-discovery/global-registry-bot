/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { afterAll, beforeAll, beforeEach, describe, expect, jest, test } from '@jest/globals';

const PREV_DEBUG_NS = process.env.DEBUG_NS;
process.env.DEBUG_NS = '1';

let currentTestNow = Date.parse('2026-04-22T09:00:00.000Z');
const dateNowSpy = jest.spyOn(Date, 'now').mockImplementation(() => currentTestNow);

afterAll(() => {
  process.env.DEBUG_NS = PREV_DEBUG_NS;
  dateNowSpy.mockRestore();
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

type TestConfig = {
  workflow?: {
    labels?: Record<string, unknown>;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

const TEST_WORKFLOW_LABELS = {
  authorAction: 'Requester Action',
  approverAction: 'Review Pending',
  parentOwnerAction: 'Parent Owner Action',
  approvalRequested: ['Review Pending'],
  approvalSuccessful: ['Approved'],
  approvalRejected: ['Rejected'],
};

function withWorkflowLabels<T extends TestConfig>(cfg: T): T {
  cfg.workflow = {
    ...(cfg.workflow || {}),
    labels: {
      ...TEST_WORKFLOW_LABELS,
      ...(cfg.workflow?.labels || {}),
    },
  };

  return cfg;
}

const DEFAULT_CONFIG = withWorkflowLabels({
  workflow: { labels: {}, approvers: [] },
} as any);

let requestHandler: any;

function postedBodies(): string {
  return postOnce.mock.calls.map((call) => call[2] ?? '').join('\n');
}

function reviewLabels(): { name: string }[] {
  return [{ name: 'Review Pending' }];
}

function inReview<T extends { labels?: any[] }>(issue: T): T {
  return {
    ...issue,
    labels: [...(Array.isArray(issue.labels) ? issue.labels : []), ...reviewLabels()],
  };
}

function expectHandoverPosted(): void {
  expect(postOnce.mock.calls.some((call) => call[3]?.minimizeTag === 'nsreq:handover')).toBe(true);
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
type IssuesAddAssigneesFn = (args: unknown) => Promise<unknown>;

type PullsListFn = (args: unknown) => Promise<{ data: unknown[] }>;
type PullsUpdateFn = (args: unknown) => Promise<unknown>;
type PullsCreateReviewFn = (args: unknown) => Promise<unknown>;
type PullsListFilesFn = (args: unknown) => Promise<{ data: unknown[] }>;
type PullsListCommitsFn = (args: unknown) => Promise<{ data: unknown[] }>;
type PullsListReviewsFn = (args: unknown) => Promise<{ data: unknown[] }>;
type PullsUpdateBranchFn = (args: unknown) => Promise<unknown>;

type ChecksListAnnotationsFn = (args: any) => Promise<{ data: any[] }>;
type ChecksListForSuiteFn = (args: any) => Promise<{ data: { check_runs: any[] } }>;
type ChecksListForRefFn = (args: any) => Promise<{ data: { check_runs: any[] } }>;

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
        addAssignees: jest.fn<IssuesAddAssigneesFn>(() => Promise.resolve({})),
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
        listCommits: jest.fn<PullsListCommitsFn>(() => Promise.resolve({ data: [] })),
        listReviews: jest.fn<PullsListReviewsFn>(() => Promise.resolve({ data: [] })),
        createReview: jest.fn<PullsCreateReviewFn>(() => Promise.resolve({})),
        update: jest.fn<PullsUpdateFn>(() => Promise.resolve({})),
        updateBranch: jest.fn<PullsUpdateBranchFn>(() => Promise.resolve({})),
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
        listForRef: jest.fn<ChecksListForRefFn>().mockResolvedValue({
          data: {
            check_runs: [{ id: 1, name: 'ci', status: 'completed', conclusion: 'success' }],
          },
        }),
      },
      request: jest.fn(async () => ({ data: { workflow_runs: [] } })),
    },
  };

  if (args.withCachedConfig) {
    ctx.resourceBotConfig = withWorkflowLabels(args.config ?? DEFAULT_CONFIG);
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

function mkPullRequestContext(args: {
  action?: 'opened' | 'synchronize' | 'reopened' | 'ready_for_review';
  pr?: any;
  config?: any;
}) {
  const pr = args.pr ?? {
    number: 2001,
    title: 'Direct registry PR',
    body: 'manual direct pr',
    state: 'open',
    draft: false,
    user: { login: 'external-user' },
    head: { ref: 'feature/registry-pr', sha: 'sha-workflow-waiting' },
    base: { ref: 'main' },
  };

  const ctx = mkBaseContext({
    owner: 'o',
    repo: 'r',
    issue: { number: pr.number, title: pr.title, body: pr.body, labels: [], user: pr.user },
    withCachedConfig: true,
    config:
      args.config ??
      withWorkflowLabels({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'schema.json',
            issueTemplate: 'template.yml',
          },
        },
        workflow: { labels: {}, approvers: [] },
      } as any),
  });

  ctx.name = `pull_request.${args.action ?? 'opened'}`;
  ctx.payload = {
    action: args.action ?? 'opened',
    repository: { name: 'r', owner: { login: 'o' } },
    pull_request: pr,
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

async function runIssueCommentWithoutJestWorker(handler: (ctx: any) => Promise<void>, ctx: any): Promise<void> {
  const previousJestWorkerId = process.env.JEST_WORKER_ID;

  try {
    delete process.env.JEST_WORKER_ID;
    await handler(ctx);
  } finally {
    if (previousJestWorkerId === undefined) {
      delete process.env.JEST_WORKER_ID;
    } else {
      process.env.JEST_WORKER_ID = previousJestWorkerId;
    }
  }
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
  currentTestNow += 7 * 60 * 60 * 1000;
  jest.resetAllMocks();
  dateNowSpy.mockImplementation(() => currentTestNow);

  setStateLabel.mockImplementation(async () => {});
  ensureAssigneesOnce.mockImplementation(async () => {});
  postOnce.mockImplementation(async () => {});
  collapseBotCommentsByPrefix.mockImplementation(async () => {});
  tryMergeIfGreen.mockImplementation(async () => {});

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
  tryMergeIfGreen.mockResolvedValue(undefined);
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
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'bob' } }),
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
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
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
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
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
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(createRequestPr).not.toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('PR already open: #9');
});

test('issue_comment: approval with existing PR only adds hook approvers missing from current assignees', async () => {
  findOpenIssuePrs.mockResolvedValueOnce([{ number: 9, body: 'x', head: { ref: 'r', sha: 's' } }]);

  const cfg = {
    requests: {
      product: { folderName: 'resources' },
    },
    workflow: {
      labels: {
        approvalSuccessful: ['Approved'],
      },
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  loadTemplate.mockResolvedValueOnce({
    _meta: { requestType: 'product', root: '/data/products', schema: 'x' },
    title: 'Product',
    labels: [],
    body: [],
    name: 'Product',
  });
  parseForm.mockReturnValueOnce({ identifier: 'product-existing-pr' });
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', approvers: ['reviewerA', 'ReviewerB'] } as any);

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
    config: cfg,
  });
  ctx.octokit.issues.get.mockImplementation(async ({ issue_number }: any) => {
    if (issue_number === 9) {
      return {
        data: {
          number: 9,
          labels: [{ name: 'Approved' }],
          assignees: [{ login: 'reviewerA' }],
        },
      };
    }

    return {
      data: {
        number: 1,
        title: 't',
        body: 'b',
        labels: reviewLabels(),
        user: { login: 'author' },
      },
    };
  });

  await handler(ctx);

  expect(createRequestPr).not.toHaveBeenCalled();
  expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 9, assignees: ['ReviewerB'] })
  );
  expect(String(postOnce.mock.calls[0][2])).toContain('PR already open: #9');
});

test('issue_comment: approval create PR failure -> posts error', async () => {
  createRequestPr.mockRejectedValueOnce(new Error('boom'));

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
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
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
    comment: { body: 'updated', user: { login: 'author' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('## Detected issues');
  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'author');
});

test('issue_comment: author update revalidation maps machine-readable validation issues', async () => {
  validateRequestIssue.mockResolvedValueOnce({
    errors: ['x'],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '- x',
    validationIssues: [{ path: 'contact', message: 'missing contact owner' }],
    namespace: 'ABC',
    nsType: 'product',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];

  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue: inReview({ number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } }),
    comment: { body: 'updated', user: { login: 'author' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  expect(String(postOnce.mock.calls[0][2])).toContain('missing contact owner');
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
    if (p === 'data/vendors/a.yaml') return { data: {} };
    if (p.includes('a.b.yaml') || p.includes('a.b.yml')) throw httpErr(404);
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
  expect(ctx.octokit.pulls.update).toHaveBeenCalled();
  expect(postOnce).toHaveBeenCalled();
});

test('check_suite.success treats matching default-branch head as default branch suite', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'success',
    sha: 'sha-default-suite',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.payload.action = 'completed';
  ctx.payload.repository.default_branch = 'main';
  ctx.payload.check_suite.status = 'completed';
  ctx.payload.check_suite.head_branch = 'feature/not-main';
  ctx.payload.check_suite.head_sha = 'sha-default-suite';
  ctx.payload.check_suite.pull_requests = [{ number: 77 }];
  ctx.octokit.repos.getBranch = jest.fn(async () => ({
    data: { commit: { sha: 'sha-default-suite' } },
  }));

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({ data: [] })
    .mockResolvedValueOnce({ data: [] })
    .mockResolvedValueOnce({ data: [] });

  await handler(ctx);

  expect(ctx.octokit.repos.getBranch).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', branch: 'main' })
  );
  expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1', issue_number: 77 },
    expect.objectContaining({ tagPrefix: 'nsreq:ci-validation' })
  );
  expect(ctx.octokit.pulls.list).toHaveBeenCalledTimes(3);

  setTimeoutSpy.mockRestore();
});

test('check_suite.success ignores default-branch fallback when branch head lookup fails', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'success',
    sha: 'sha-default-suite-fail',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.action = 'completed';
  ctx.payload.repository.default_branch = 'main';
  ctx.payload.check_suite.status = 'completed';
  ctx.payload.check_suite.head_branch = 'feature/not-main';
  ctx.payload.check_suite.head_sha = 'sha-default-suite-fail';
  ctx.payload.check_suite.pull_requests = [];
  ctx.octokit.repos.getBranch = jest.fn(async () => {
    throw httpErr(503);
  });

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] });

  await handler(ctx);

  expect(ctx.octokit.repos.getBranch).toHaveBeenCalledTimes(1);
  expect(ctx.octokit.pulls.list).toHaveBeenCalledTimes(2);
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

  ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
    if (path === '.github/registry-bot/request-schemas/system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              properties: {
                contacts: {
                  title: 'Contacts',
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/42' },
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalledTimes(1);
  const [, params, body] = (postOnce as jest.Mock).mock.calls[0];
  const bodyText = typeof body === 'string' ? body : JSON.stringify(body);

  expect(params).toEqual({ owner: 'o1', repo: 'r1', issue_number: 42 });
  expect(bodyText).toContain('### File: `data/namespaces/sap.css.yaml`');
  expect(bodyText).toContain('### Contacts');
  expect(bodyText).toContain("Property 'contact' is required for System.");
  expect(bodyText).toContain('"field": "contacts"');
});

test('check_suite.completed failure aggregates multi-file registry issues into one machine-readable PR comment', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-aggregate',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 501;
  ctx.payload.check_suite.pull_requests = [{ number: 77 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 9002, html_url: 'https://example/check/9002' }] },
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
      {
        path: 'data/products/product-one.yaml',
        title: 'registry-validate product',
        message:
          'Product Name is required. [file=data/products/product-one.yaml schema=.github/registry-bot/request-schemas/product.schema.json requestType=product]',
        annotation_level: 'failure',
      },
      {
        path: 'data/products/product-one.yaml',
        title: 'registry-validate product',
        message:
          '/identifier MUST match pattern. [file=data/products/product-one.yaml schema=.github/registry-bot/request-schemas/product.schema.json requestType=product]',
        annotation_level: 'failure',
      },
    ],
  });

  ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
    if (path === '.github/registry-bot/request-schemas/system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              properties: {
                contacts: {
                  title: 'Contacts',
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    if (path === '.github/registry-bot/request-schemas/product.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              properties: {
                title: {
                  'title': 'Product Name',
                  'x-form-field': 'title',
                },
                identifier: {
                  'title': 'Product ID',
                  'x-form-field': 'identifier',
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/77' },
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalledTimes(1);
  const [, params, body, options] = (postOnce as jest.Mock).mock.calls[0];
  const bodyText = typeof body === 'string' ? body : JSON.stringify(body);

  expect(params).toEqual({ owner: 'o1', repo: 'r1', issue_number: 77 });
  expect(options).toEqual(expect.objectContaining({ minimizeTag: 'nsreq:ci-validation' }));
  expect(bodyText).toContain('## Detected issues');
  expect(bodyText).toContain('### File: `data/namespaces/sap.css.yaml`');
  expect(bodyText).toContain('### File: `data/products/product-one.yaml`');
  expect(bodyText).toContain('Show as JSON (Robots Friendly)');
  expect(bodyText).toContain('"filePath": "data/namespaces/sap.css.yaml"');
  expect(bodyText).toContain('"filePath": "data/products/product-one.yaml"');
  expect(bodyText).toContain('"field": "contacts"');
  expect(bodyText).toContain('"field": "title"');
  expect(bodyText).toContain('"field": "identifier"');
  expect(bodyText).toContain('#### Product name');
});

test('check_suite.completed failure resolves machine-readable fields from nested allOf/oneOf schema aliases', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-nested-aliases',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 502;
  ctx.payload.check_suite.pull_requests = [{ number: 78 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 9003, html_url: 'https://example/check/9003' }] },
  });

  ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
    data: [
      {
        path: 'data/namespaces/sap.css.yaml',
        title: 'registry-validate systemNamespace',
        message:
          "Property 'contact' is required for System. [file=data/namespaces/sap.css.yaml schema=.github/registry-bot/request-schemas/nested-system-namespace.schema.json requestType=systemNamespace]",
        annotation_level: 'failure',
      },
      {
        path: 'data/products/product-one.yaml',
        title: 'registry-validate product',
        message:
          '/identifier MUST match pattern. [file=data/products/product-one.yaml schema=.github/registry-bot/request-schemas/nested-product.schema.json requestType=product]',
        annotation_level: 'failure',
      },
    ],
  });

  ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
    if (path === '.github/registry-bot/request-schemas/nested-system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              allOf: [
                {
                  properties: {
                    contacts: {
                      'title': 'Contacts',
                      'x-form-field': 'contact',
                    },
                  },
                },
              ],
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    if (path === '.github/registry-bot/request-schemas/nested-product.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              oneOf: [
                {
                  properties: {
                    identifier: {
                      'title': 'Product ID',
                      'x-form-field': 'identifier',
                    },
                  },
                },
              ],
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/78' },
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalledTimes(1);
  const [, params, body, options] = (postOnce as jest.Mock).mock.calls[0];
  const bodyText = typeof body === 'string' ? body : JSON.stringify(body);

  expect(params).toEqual({ owner: 'o1', repo: 'r1', issue_number: 78 });
  expect(options).toEqual(expect.objectContaining({ minimizeTag: 'nsreq:ci-validation' }));
  expect(bodyText).toContain('### Contacts');
  expect(bodyText).toContain('#### Identifier');
  expect(bodyText).toContain('"field": "contacts"');
  expect(bodyText).toContain('"field": "identifier"');
});

test('check_suite.completed failure resolves machine-readable fields from schema $defs aliases', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-defs-aliases',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 5021;
  ctx.payload.check_suite.pull_requests = [{ number: 781 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 90031, html_url: 'https://example/check/90031' }] },
  });

  ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
    data: [
      {
        path: 'data/namespaces/sap.css.yaml',
        title: 'registry-validate systemNamespace',
        message:
          "Property 'contact' is required for System. [file=data/namespaces/sap.css.yaml schema=.github/registry-bot/request-schemas/defs-system-namespace.schema.json requestType=systemNamespace]",
        annotation_level: 'failure',
      },
    ],
  });

  ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
    if (path === '.github/registry-bot/request-schemas/defs-system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              $defs: {
                contactFields: {
                  properties: {
                    contacts: {
                      'title': 'Contacts',
                      'x-form-field': 'contact',
                    },
                  },
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/781' },
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalledTimes(1);
  const [, params, body, options] = (postOnce as jest.Mock).mock.calls[0];
  const bodyText = typeof body === 'string' ? body : JSON.stringify(body);

  expect(params).toEqual({ owner: 'o1', repo: 'r1', issue_number: 781 });
  expect(options).toEqual(expect.objectContaining({ minimizeTag: 'nsreq:ci-validation' }));
  expect(bodyText).toContain('### Contacts');
  expect(bodyText).toContain('"field": "contacts"');
});

test('check_suite.completed failure falls back to the second schema candidate after invalid JSON', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-invalid-schema-fallback',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 503;
  ctx.payload.check_suite.pull_requests = [{ number: 79 }];

  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 9004, html_url: 'https://example/check/9004' }] },
  });

  ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
    data: [
      {
        path: 'data/namespaces/sap.css.yaml',
        title: 'registry-validate systemNamespace',
        message:
          "Property 'contact' is required for System. [file=data/namespaces/sap.css.yaml schema=request-schemas/fallback-system-namespace.schema.json requestType=systemNamespace]",
        annotation_level: 'failure',
      },
    ],
  });

  ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
    if (path === '.github/registry-bot/request-schemas/fallback-system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from('{ not valid json', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      };
    }

    if (path === 'request-schemas/fallback-system-namespace.schema.json') {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              properties: {
                contacts: {
                  'title': 'Contacts',
                  'x-form-field': 'contact',
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: { html_url: 'https://github.tools.sap/o1/r1/pull/79' },
  });

  await handler(ctx);

  const bodyText = String((postOnce as jest.Mock).mock.calls[0]?.[2] ?? '');
  expect(bodyText).toContain('### Contacts');
  expect(bodyText).toContain('"field": "contacts"');
  expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
    expect.objectContaining({ path: '.github/registry-bot/request-schemas/fallback-system-namespace.schema.json' })
  );
  expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
    expect.objectContaining({ path: 'request-schemas/fallback-system-namespace.schema.json' })
  );
});

test('check_suite.completed failure reuses cached schema alias lookup across repeated failures', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];
  const schemaPath = '.github/registry-bot/request-schemas/cached-system-namespace.schema.json';
  const getContent = jest.fn(async ({ path }: any) => {
    if (path === schemaPath) {
      return {
        data: {
          content: Buffer.from(
            JSON.stringify({
              properties: {
                contacts: {
                  'title': 'Contacts',
                  'x-form-field': 'contact',
                },
              },
            })
          ).toString('base64'),
          encoding: 'base64',
        },
      };
    }

    throw httpErr(404);
  });

  const makeCtx = (suiteId: number, prNumber: number, sha: string) => {
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'failure',
      sha,
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload.check_suite.id = suiteId;
    ctx.payload.check_suite.pull_requests = [{ number: prNumber }];
    ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
      data: { check_runs: [{ id: 9100 + suiteId, html_url: `https://example/check/${9100 + suiteId}` }] },
    });
    ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
      data: [
        {
          path: 'data/namespaces/sap.css.yaml',
          title: 'registry-validate systemNamespace',
          message: `Property 'contact' is required for System. [file=data/namespaces/sap.css.yaml schema=${schemaPath} requestType=systemNamespace]`,
          annotation_level: 'failure',
        },
      ],
    });
    ctx.octokit.repos.getContent = getContent;
    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: { html_url: `https://github.tools.sap/o1/r1/pull/${prNumber}` },
    });

    return ctx;
  };

  await handler(makeCtx(504, 80, 'sha-cached-schema-1'));
  await handler(makeCtx(505, 81, 'sha-cached-schema-2'));

  expect(postOnce).toHaveBeenCalledTimes(2);
  expect(getContent.mock.calls.filter(([args]) => args?.path === schemaPath)).toHaveLength(1);
  expect(String((postOnce as jest.Mock).mock.calls[0]?.[2] ?? '')).toContain('"field": "contacts"');
  expect(String((postOnce as jest.Mock).mock.calls[1]?.[2] ?? '')).toContain('"field": "contacts"');
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

test('check_run.success without repo info returns early', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const ctx: any = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'success',
    sha: 'deadbeef',
    ownerLogin: 'o',
    repoName: 'r',
    withCachedConfig: true,
  });

  ctx.payload = {
    action: 'completed',
    check_run: {
      conclusion: 'success',
      status: 'completed',
      head_sha: 'deadbeef',
      pull_requests: [{ number: 7 }],
    },
  };

  await handlers['check_run.completed'][0](ctx);

  expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
});

test('check_run.success collapses CI comments and auto-merges matching PR head', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const ctx: any = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'success',
    sha: 'sha-checkrun-success',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload = {
    action: 'completed',
    repository: { name: 'r1', owner: { login: 'o1' } },
    check_run: {
      conclusion: 'success',
      status: 'completed',
      head_sha: 'sha-checkrun-success',
      pull_requests: [{ number: 301 }],
    },
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [{ number: 301, body: 'source: #1', head: { ref: 'feature/checkrun', sha: 'sha-checkrun-success' } }],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'author' } },
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 301,
      state: 'open',
      body: 'source: #1',
      head: { ref: 'feature/checkrun', sha: 'sha-checkrun-success' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  calcSnapshotHash.mockReturnValue('h1');
  extractHashFromPrBody.mockReturnValue('h1');

  await handlers['check_run.completed'][0](ctx);

  expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1', issue_number: 301 },
    expect.objectContaining({ tagPrefix: 'nsreq:ci-validation' })
  );
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

test('check_suite.completed failure stops listing suite runs after 20 pages (safety cap)', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];

  const ctx: any = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-suite-run-cap',
    ownerLogin: 'o',
    repoName: 'r',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 56;
  ctx.payload.check_suite.pull_requests = [{ number: 8 }];

  ctx.octokit.checks.listForSuite.mockImplementation(async () => ({
    data: {
      check_runs: Array.from({ length: 100 }, (_, index) => ({
        id: 4000 + index,
        html_url: `https://example/check/${4000 + index}`,
      })),
    },
  }));

  ctx.octokit.checks.listAnnotations.mockResolvedValue({ data: [] });

  await handler(ctx);

  expect(ctx.octokit.checks.listForSuite).toHaveBeenCalledTimes(20);
  expect(ctx.octokit.checks.listAnnotations).toHaveBeenCalledTimes(2000);
  expect(postOnce).not.toHaveBeenCalled();
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
});

test('status success with missing sha skips auto-merge candidate lookup', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['status'][0];

  const ctx = mkStatusContext({
    state: 'success',
    sha: '',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
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
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }
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

    expect(setStateLabel).not.toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 550,
        labels: ['Parent Owner Action'],
      })
    );

    expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 550,
        assignees: ['barOwner'],
      })
    );

    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
  });

  test('issues.opened: subcontext requester who owns parent namespace creates PR without CPA review', async () => {
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

    findOpenIssuePrs.mockResolvedValue([]);
    createRequestPr.mockResolvedValue({ number: 777 });

    const ctx = mkIssuesContext({ issue, action: 'opened' });

    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }

      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@barOwner"\n'),
            encoding: 'base64',
          },
        };
      }

      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await h['issues.opened'][0](ctx);

    expect(postedBodies()).not.toContain('Parent owner approval required');
    expect(postedBodies()).not.toContain('### ➡️ Routing to an approver for review');

    expect(issue.body).toContain('nsreq:parent-approval');
    expect(issue.body).toContain('"parent":"sap.css"');
    expect(issue.body).toContain(`"target":"${target}"`);
    expect(issue.body).toContain('"approvedBy":"barOwner"');

    const createRequestPrCalls = (createRequestPr as unknown as { mock: { calls: unknown[][] } }).mock.calls;

    expect(createRequestPrCalls).toHaveLength(1);

    expect(createRequestPrCalls[0]).toEqual([
      expect.anything(),
      { owner: 'o', repo: 'r' },
      expect.objectContaining({ number: 551 }),
      expect.objectContaining({ identifier: target }),
      expect.objectContaining({ template: tpl }),
    ]);

    expect(postedBodies()).toContain('Approved by parent namespace owner @barOwner');
    expect(postedBodies()).toContain('Opened PR: #777');

    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalledWith(
      expect.objectContaining({
        labels: ['Parent Owner Action'],
      })
    );

    expect(postOnce.mock.calls.some((call) => call[3]?.minimizeTag === 'nsreq:handover')).toBe(false);
  });

  test('issue_comment: requester update creates subcontext PR when requester owns parent namespace', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar';
    const issue = {
      number: 552,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'barOwner' },
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

    findOpenIssuePrs.mockResolvedValue([]);
    createRequestPr.mockResolvedValue({ number: 778 });

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'updated', user: { login: 'barOwner' } },
      sender: { type: 'User', login: 'barOwner' },
      withCachedConfig: true,
      config: withWorkflowLabels({
        requests: {
          subContextNamespace: {
            folderName: 'data/namespaces',
            schema: 'schema.json',
            issueTemplate: 'template.yml',
          },
        },
        workflow: { labels: {}, approvers: [] },
      } as any),
    });

    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }

      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@barOwner"\n'),
            encoding: 'base64',
          },
        };
      }

      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await handlers['issue_comment.created'][0](ctx);

    expect(createRequestPr).toHaveBeenCalled();
    expect(postedBodies()).toContain('Approved by parent namespace owner @barOwner');
    expect(postedBodies()).toContain('Opened PR: #778');
    expect(postedBodies()).not.toContain('### ➡️ Routing to an approver for review');
  });

  test('gates sub-namespace request when parent owner email resolves via GraphQL fallback', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 5511,
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

    const ctx = mkIssuesContext({ issue, action: 'opened' });
    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: b64('type: vendor\nname: sap\ncontacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }

      if (path === 'data/namespaces/sap.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@topOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - owner@example.com\n'),
            encoding: 'base64',
          },
        };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });
    ctx.octokit.search = {
      users: jest.fn(() => Promise.resolve({ data: { items: [] } })),
    };
    ctx.octokit.graphql = jest.fn(() => Promise.resolve({ search: { nodes: [{ login: 'emailOwner' }] } }));

    await h['issues.opened'][0](ctx);

    const posted = postedBodies();
    expect(posted).toContain('@emailOwner');
    expect(ctx.octokit.search.users).toHaveBeenCalledWith({ q: 'owner@example.com in:email', per_page: 5 });
    expect(ctx.octokit.graphql).toHaveBeenCalledWith(expect.stringContaining('search(type: USER'), {
      q: 'owner@example.com in:email',
    });
  });

  test('gates sub-namespace request when structured parent contacts include url, hinted login, and bracketed email', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 55115,
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

    const ctx = mkIssuesContext({ issue, action: 'opened' });
    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: b64('type: vendor\nname: sap\ncontacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@topOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return {
          data: {
            content: b64(
              'contacts:\n  github: https://github.tools.sap/urlOwner\n  owner: HintedOwner\n  email: <bracket.owner@example.com>\n'
            ),
            encoding: 'base64',
          },
        };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });
    ctx.octokit.search = {
      users: jest.fn(() => Promise.resolve({ data: { items: [{ login: 'resolvedEmailOwner' }] } })),
    };
    ctx.octokit.graphql = jest.fn(() => Promise.resolve({ search: { nodes: [{ login: 'shouldNotRun' }] } }));

    await h['issues.opened'][0](ctx);

    const posted = postedBodies();
    expect(posted).toContain('@urlOwner');
    expect(posted).toContain('@HintedOwner');
    expect(posted).toContain('@resolvedEmailOwner');
    expect(ctx.octokit.search.users).toHaveBeenCalledWith({ q: 'bracket.owner@example.com in:email', per_page: 5 });
    expect(ctx.octokit.graphql).not.toHaveBeenCalled();
  });

  test('does not gate when the immediate parent yaml cannot be parsed', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 55116,
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

    const ctx = mkIssuesContext({ issue, action: 'opened' });
    (ctx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: b64('type: vendor\nname: sap\ncontacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@topOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return {
          data: {
            content: b64('contacts: [unclosed\n'),
            encoding: 'base64',
          },
        };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await h['issues.opened'][0](ctx);

    const posted = postedBodies();
    expect(posted).not.toContain('Parent owner approval required');
    expect(ensureAssigneesOnce).toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
  });

  test('reuses cached parent owner email resolution across repeated gated requests', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const cachedEmail = 'owner-cache@example.com';
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

    const getContent = jest.fn(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: b64('type: vendor\nname: sap\ncontacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return {
          data: {
            content: b64('contacts:\n  - "@topOwner"\n'),
            encoding: 'base64',
          },
        };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return {
          data: {
            content: b64(`contacts:\n  - ${cachedEmail}\n`),
            encoding: 'base64',
          },
        };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    const firstSearchUsers = jest.fn(() => Promise.resolve({ data: { items: [] } }));
    const firstGraphql = jest.fn(() => Promise.resolve({ search: { nodes: [{ login: 'emailOwner' }] } }));

    const firstIssue = {
      number: 5512,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'requester' },
      state: 'open',
    };

    const firstCtx = mkIssuesContext({ issue: firstIssue, action: 'opened' });
    (firstCtx.octokit.repos.getContent as jest.Mock) = getContent as any;
    firstCtx.octokit.search = { users: firstSearchUsers };
    firstCtx.octokit.graphql = firstGraphql;

    await h['issues.opened'][0](firstCtx);

    const secondSearchUsers = jest.fn(() => Promise.resolve({ data: { items: [] } }));
    const secondGraphql = jest.fn(() => Promise.resolve({ search: { nodes: [{ login: 'shouldNotBeUsed' }] } }));

    const secondIssue = {
      number: 5513,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { type: 'User', login: 'another-requester' },
      state: 'open',
    };

    const secondCtx = mkIssuesContext({ issue: secondIssue, action: 'opened' });
    (secondCtx.octokit.repos.getContent as jest.Mock) = getContent as any;
    secondCtx.octokit.search = { users: secondSearchUsers };
    secondCtx.octokit.graphql = secondGraphql;

    await h['issues.opened'][0](secondCtx);

    expect(firstSearchUsers).toHaveBeenCalledTimes(1);
    expect(firstGraphql).toHaveBeenCalledTimes(1);
    expect(secondSearchUsers).not.toHaveBeenCalled();
    expect(secondGraphql).not.toHaveBeenCalled();

    const posted = postedBodies();
    expect(posted).toContain('@emailOwner');
    expect(posted.match(/Parent owner approval required/g)).toHaveLength(2);
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
      if (path === 'data/vendors/sap.yaml') return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
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

  test('owner approval while gated posts validation issues and returns requester action when revalidation fails', async () => {
    const { app, handlers: h } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 5521,
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

    validateRequestIssue
      .mockResolvedValueOnce({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'subContextNamespace',
        template: tpl,
        formData: { identifier: target, description: 'x' },
      })
      .mockResolvedValueOnce({
        errors: ['name: validation failed'],
        errorsGrouped: {},
        errorsFormatted: 'name: validation failed',
        errorsFormattedSingle: 'name: validation failed',
        validationIssues: [{ path: 'name', message: 'validation failed' }],
        namespace: target,
        nsType: 'subContextNamespace',
        template: tpl,
        formData: { identifier: target, description: 'x' },
      });

    const openCtx = mkIssuesContext({ issue, action: 'opened' });
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;

    (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: Buffer.from('type: vendor\nname: sap\n', 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }
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
    (setStateLabel as jest.Mock).mockClear();
    (createRequestPr as jest.Mock).mockClear();

    const commentCtx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { type: 'User', login: 'barOwner' } },
    });

    await h['issue_comment.created'][0](commentCtx);

    const posted = postedBodies();
    expect(posted).toContain('## Detected issues');
    expect(posted).toContain('validation failed');
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
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
      if (path === 'data/vendors/sap.yaml') return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
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
    expect(posted).toContain('Approved by parent namespace owner @barOwner. Opened PR: #10');

    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
    expect(createRequestPr).toHaveBeenCalled();
  });
  describe('additional approval gate coverage', () => {
    const systemTemplate = {
      _meta: {
        requestType: 'systemNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        path: '.github/ISSUE_TEMPLATE/1-system-namespace-request.yaml',
      },
      title: 'System Namespace',
      labels: [],
      body: [],
      name: 'System Namespace',
    };

    test('issues.opened: system namespace requests require contact-owner approval after email resolution', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 560,
        title: 'System Namespace: sap.aiadm',
        body: 'body',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: 'owner@sap.com',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: 'owner@sap.com',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.search = {
        users: jest.fn(async () => ({ data: { items: [{ login: 'resolvedOwner' }] } })),
      };
      ctx.octokit.graphql = jest.fn(async () => ({ search: { nodes: [] } }));
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:contact-approval');
      expect(postedBodies()).toContain('@resolvedOwner');
      expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
    });

    test('issue_comment: contact-owner approval validation errors keep the request with the author', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 561,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: 'owner@sap.com',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: ['Missing contact'],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: 'Missing contact',
        validationIssues: [{ path: 'contact', message: 'Missing contact' }],
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: 'owner@sap.com',
          visibility: 'public',
        },
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'resolvedOwner' } },
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postedBodies()).toContain('Missing contact');
      expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
    });

    test('issue_comment: contact-owner approval resumes the normal review handover', async () => {
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

      const issue = {
        number: 562,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: ['needs-review'],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: 'owner@sap.com',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: 'owner@sap.com',
          visibility: 'public',
        },
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'resolvedOwner' } },
        withCachedConfig: true,
        config: cfg,
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(issue.body).toContain('"approvedBy":"resolvedOwner"');
      expect(postedBodies()).toContain('Contact owner approved by @resolvedOwner. Continuing with standard review.');
      expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
    });

    test('issue_comment: parent-owner approval resumes normal review for non-subcontext namespace requests', async () => {
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

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 563,
        title: 'System Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"]} -->\n',
        labels: ['needs-review'],
        user: { login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'System Namespace',
        labels: [],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ namespace: target, contact: '@owner', visibility: 'public' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'systemNamespace',
        template,
        formData: { namespace: target, contact: '@owner', visibility: 'public' },
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'barOwner' } },
        withCachedConfig: true,
        config: cfg,
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postedBodies()).toContain('Parent namespace approved by @barOwner. Continuing with standard review.');
      expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
    });

    test('issues.opened: non-system requests clear stale contact approval markers', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 564,
        title: 'Product: ABC',
        body:
          '### Product\n\nABC\n' +
          '\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx = mkIssuesContext({ issue, action: 'opened' });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).not.toContain('nsreq:contact-approval');
    });

    test('issues.opened: requester already being a contact owner skips the contact gate', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 565,
        title: 'System Namespace: sap.aiadm',
        body: 'body',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@requester',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: '@requester',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).not.toContain('Contact owner approval required');
      expect(issue.body).not.toContain('nsreq:contact-approval');
    });

    test('issue_comment: non-contact owners are ignored while contact approval is pending', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 566,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: 'owner@sap.com',
        visibility: 'public',
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'someoneElse' } },
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postedBodies()).toContain('Approval ignored: this request requires contact owner approval');
      expect(postedBodies()).toContain('@resolvedOwner');
    });

    test('issue_comment: parent-owner approval validation errors keep the request with the author', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 567,
        title: 'System Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'System Namespace',
        labels: [],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ namespace: target, contact: '@owner', visibility: 'public' });
      validateRequestIssue.mockResolvedValue({
        errors: ['Missing contact'],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: 'Missing contact',
        validationIssues: [{ path: 'contact', message: 'Missing contact' }],
        namespace: target,
        nsType: 'systemNamespace',
        template,
        formData: { namespace: target, contact: '@owner', visibility: 'public' },
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'barOwner' } },
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postedBodies()).toContain('Missing contact');
      expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
    });

    test('issues.opened: non-namespace requests clear stale parent approval markers', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 568,
        title: 'Product: ABC',
        body:
          '### Product\n\nABC\n' +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx = mkIssuesContext({ issue, action: 'opened' });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).not.toContain('nsreq:parent-approval');
    });

    test('issues.opened: tolerates contact approval marker cleanup failures', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 569,
        title: 'Product: ABC',
        body:
          '### Product\n\nABC\n' +
          '\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.issues.update.mockRejectedValueOnce(new Error('update failed'));

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:contact-approval');
    });

    test('issues.opened: identical pending contact approval markers are not rewritten', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 570,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@resolvedOwner',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: '@resolvedOwner',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:contact-approval');
      expect(postedBodies()).toContain('Contact owner approval required');
    });

    test('issues.opened: contact approval marker update failures still keep the request gated', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 571,
        title: 'System Namespace: sap.aiadm',
        body: 'body',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@resolvedOwner',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: '@resolvedOwner',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.issues.update.mockRejectedValueOnce(new Error('update failed'));
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).toContain('Contact owner approval required');
      expect(issue.body).not.toContain('nsreq:contact-approval');
    });

    test('issues.opened: already-approved contact markers skip renewed contact gating', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 572,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"],"approvedBy":"resolvedOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@resolvedOwner',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: '@resolvedOwner',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).not.toContain('Contact owner approval required');
    });

    test('issues.opened: already-approved contact markers ignore owner order differences', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 5721,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["zOwner","aOwner"],"approvedBy":"aOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@aOwner @zOwner',
        visibility: 'public',
      });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'sap.aiadm',
        nsType: 'systemNamespace',
        template: systemTemplate,
        formData: {
          namespace: 'sap.aiadm',
          description: 'Example description',
          contact: '@aOwner @zOwner',
          visibility: 'public',
        },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (String(path) === 'data/vendors/sap.yaml') {
          return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).not.toContain('Contact owner approval required');
      expect(ctx.octokit.issues.update).not.toHaveBeenCalledWith(
        expect.objectContaining({ body: expect.stringContaining('nsreq:contact-approval') })
      );
    });

    test('issue_comment: already-approved contact markers do not trigger approval handling again', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 573,
        title: 'System Namespace: sap.aiadm',
        body: 'body\n\n<!-- nsreq:contact-approval = {"v":1,"target":"sap.aiadm","owners":["resolvedOwner"],"approvedBy":"resolvedOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      loadTemplate.mockResolvedValue(systemTemplate);
      parseForm.mockReturnValue({
        namespace: 'sap.aiadm',
        description: 'Example description',
        contact: '@resolvedOwner',
        visibility: 'public',
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'hello', user: { login: 'resolvedOwner' } },
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postOnce).not.toHaveBeenCalled();
    });

    test('issues.opened: malformed parent approval markers are ignored safely', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 574,
        title: 'Product: ABC',
        body: '### Product\n\nABC\n\n<!-- nsreq:parent-approval = {not-json} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx = mkIssuesContext({ issue, action: 'opened' });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:parent-approval');
    });

    test('issues.opened: malformed contact approval markers are ignored safely', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 580,
        title: 'Product: ABC',
        body: '### Product\n\nABC\n\n<!-- nsreq:contact-approval = {not-json} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx = mkIssuesContext({ issue, action: 'opened' });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:contact-approval');
    });

    test('issues.opened: parent approval marker cleanup failures are tolerated', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 575,
        title: 'Product: ABC',
        body:
          '### Product\n\nABC\n' +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"]} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const productTemplate = {
        _meta: { requestType: 'product', root: '/data/products', schema: 'x', path: 'x' },
        title: 'Product',
        labels: [],
        body: [],
        name: 'Product',
      };

      loadTemplate.mockResolvedValue(productTemplate);
      parseForm.mockReturnValue({ identifier: 'ABC' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'ABC',
        nsType: 'product',
        template: productTemplate,
        formData: { identifier: 'ABC' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.issues.update.mockRejectedValueOnce(new Error('update failed'));

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:parent-approval');
    });

    test('issues.opened: identical pending parent approval markers are not rewritten', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 576,
        title: 'Sub-Context Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"]} -->\n',
        labels: [{ name: 'Sub-Context Namespace' }],
        user: { type: 'User', login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'Sub-Context Namespace',
        labels: ['Sub-Context Namespace'],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ identifier: target, description: 'x' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'subContextNamespace',
        template,
        formData: { identifier: target, description: 'x' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (path === 'data/vendors/sap.yaml') {
          return {
            data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' },
          };
        }
        if (path === 'data/namespaces/sap.css.yaml' || path === 'data/namespaces/sap.css.bar.yaml') {
          return {
            data: {
              content: Buffer.from('contacts:\n  - "@barOwner"\n', 'utf8').toString('base64'),
              encoding: 'base64',
            },
          };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(issue.body).toContain('nsreq:parent-approval');
      expect(postedBodies()).toContain('Parent owner approval required');
    });

    test('issues.opened: parent approval marker update failures still keep the request gated', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 577,
        title: 'Sub-Context Namespace',
        body: `### Namespace\n\n${target}\n`,
        labels: [{ name: 'Sub-Context Namespace' }],
        user: { type: 'User', login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'Sub-Context Namespace',
        labels: ['Sub-Context Namespace'],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ identifier: target, description: 'x' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'subContextNamespace',
        template,
        formData: { identifier: target, description: 'x' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.issues.update.mockImplementation(async (args: any) => {
        if (Object.prototype.hasOwnProperty.call(args, 'body')) throw new Error('update failed');
        return {};
      });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (path === 'data/vendors/sap.yaml') {
          return {
            data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' },
          };
        }
        if (path === 'data/namespaces/sap.css.yaml' || path === 'data/namespaces/sap.css.bar.yaml') {
          return {
            data: {
              content: Buffer.from('contacts:\n  - "@barOwner"\n', 'utf8').toString('base64'),
              encoding: 'base64',
            },
          };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).toContain('Parent owner approval required');
      expect(issue.body).not.toContain('nsreq:parent-approval');
    });

    test('issues.opened: already-approved parent markers skip renewed parent gating', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 578,
        title: 'Sub-Context Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"],"approvedBy":"barOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [{ name: 'Sub-Context Namespace' }],
        user: { type: 'User', login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'Sub-Context Namespace',
        labels: ['Sub-Context Namespace'],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ identifier: target, description: 'x' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'subContextNamespace',
        template,
        formData: { identifier: target, description: 'x' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (path === 'data/vendors/sap.yaml') {
          return {
            data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' },
          };
        }
        if (path === 'data/namespaces/sap.css.yaml' || path === 'data/namespaces/sap.css.bar.yaml') {
          return {
            data: {
              content: Buffer.from('contacts:\n  - "@barOwner"\n', 'utf8').toString('base64'),
              encoding: 'base64',
            },
          };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).not.toContain('Parent owner approval required');
    });

    test('issues.opened: already-approved parent markers ignore owner order differences', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 5781,
        title: 'Sub-Context Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["zOwner","aOwner","mOwner"],"approvedBy":"aOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [{ name: 'Sub-Context Namespace' }],
        user: { type: 'User', login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'Sub-Context Namespace',
        labels: ['Sub-Context Namespace'],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ identifier: target, description: 'x' });
      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: target,
        nsType: 'subContextNamespace',
        template,
        formData: { identifier: target, description: 'x' },
      });

      const ctx: any = mkIssuesContext({ issue, action: 'opened' });
      ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
        if (path === 'data/vendors/sap.yaml') {
          return {
            data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' },
          };
        }
        if (path === 'data/namespaces/sap.css.yaml' || path === 'data/namespaces/sap.css.bar.yaml') {
          return {
            data: {
              content: Buffer.from('contacts:\n  - "@mOwner"\n  - "@aOwner"\n  - "@zOwner"\n', 'utf8').toString(
                'base64'
              ),
              encoding: 'base64',
            },
          };
        }
        throw httpErr(404);
      });

      await handlers['issues.opened'][0](ctx);

      expect(postedBodies()).not.toContain('Parent owner approval required');
      expect(postedBodies()).not.toContain('Continuing with standard review.');
    });

    test('issue_comment: already-approved parent markers do not trigger approval handling again', async () => {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const target = 'sap.css.bar.foo';
      const issue = {
        number: 579,
        title: 'System Namespace',
        body:
          `### Namespace\n\n${target}\n` +
          '\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css.bar","target":"sap.css.bar.foo","owners":["barOwner"],"approvedBy":"barOwner","approvedAt":"2026-01-01T00:00:00.000Z"} -->\n',
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const template = {
        _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 'x' },
        title: 'System Namespace',
        labels: [],
        body: [],
      };

      loadTemplate.mockResolvedValue(template);
      parseForm.mockReturnValue({ namespace: target, contact: '@owner', visibility: 'public' });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'hello', user: { login: 'barOwner' } },
      });

      await handlers['issue_comment.created'][0](ctx);

      expect(postOnce).not.toHaveBeenCalled();
    });
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
  test('issue_comment: approval recovers from stale request branch when PR creation fails with no commits between', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
      pr: {
        branchNameTemplate: 'feat/resource-{resource}-issue-{issue}',
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 176,
      title: 'System Namespace: sap.aiadm',
      body: `### Namespace

  sap.aiadm

  ### System Description

  \`\`\`text
  Example description
  \`\`\`

  ### Contacts

  \`\`\`text
  owner@sap.com
  \`\`\`

  ### Visibility

  public

  <!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    loadTemplate.mockResolvedValueOnce({
      title: 'System Namespace',
      name: 'System Namespace',
      body: [],
      labels: ['System Namespace'],
      _meta: {
        requestType: 'systemNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        path: '.github/ISSUE_TEMPLATE/1-system-namespace-request.yaml',
      },
    });

    parseForm.mockReturnValueOnce({
      namespace: 'sap.aiadm',
      description: 'Example description',
      contact: 'owner@sap.com',
      visibility: 'public',
    });

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsGrouped: null,
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.aiadm',
      nsType: 'system',
      template: {
        _meta: {
          requestType: 'systemNamespace',
          root: '/data/namespaces',
          schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        },
      },
    });

    createRequestPr
      .mockRejectedValueOnce(
        new Error(
          'Validation Failed: {"resource":"PullRequest","code":"custom","message":"No commits between main and feat/resource-sap.aiadm-issue-176"} - https://docs.github.com/enterprise-server@3.17/rest/pulls/pulls#create-a-pull-request'
        )
      )
      .mockResolvedValueOnce({ number: 999 });

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    await handler(ctx);

    expect(ctx.octokit.git.deleteRef).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      ref: 'heads/feat/resource-sap.aiadm-issue-176',
    });

    expect(createRequestPr).toHaveBeenCalledTimes(2);
    expect(postOnce.mock.calls.some((c) => String(c[2] ?? '').includes('Approved by @alice. Opened PR: #999'))).toBe(
      true
    );
    expect(
      postOnce.mock.calls.some((c) =>
        String(c[2] ?? '').includes('No commits between main and feat/resource-sap.aiadm-issue-176')
      )
    ).toBe(false);
  });

  test('issue_comment: approval recovers from stale branch when PR creation reports resource already exists only on request branch', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
      pr: {
        branchNameTemplate: 'feat/resource-{resource}-issue-{issue}',
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 176,
      title: 'System Namespace: sap.aiadm',
      body: `### Namespace

sap.aiadm

### System Description

\`\`\`text
Example description
\`\`\`

### Contacts

\`\`\`text
owner@sap.com
\`\`\`

### Visibility

public

<!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    loadTemplate.mockResolvedValueOnce({
      title: 'System Namespace',
      name: 'System Namespace',
      body: [],
      labels: ['System Namespace'],
      _meta: {
        requestType: 'systemNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        path: '.github/ISSUE_TEMPLATE/1-system-namespace-request.yaml',
      },
    });

    parseForm.mockReturnValueOnce({
      namespace: 'sap.aiadm',
      description: 'Example description',
      contact: 'owner@sap.com',
      visibility: 'public',
    });

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsGrouped: null,
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.aiadm',
      nsType: 'system',
      template: {
        _meta: {
          requestType: 'systemNamespace',
          root: '/data/namespaces',
          schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        },
      },
    });

    createRequestPr
      .mockRejectedValueOnce(new Error("Resource 'sap.aiadm' already exists at data/namespaces"))
      .mockResolvedValueOnce({ number: 1001 });

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
      if (String(path) === 'data/vendors/sap.yaml') {
        return { data: { content: Buffer.from('name: sap\n', 'utf8').toString('base64'), encoding: 'base64' } };
      }
      if (String(path) === 'data/namespaces/sap.aiadm.yaml') {
        const err: any = new Error('Not Found');
        err.status = 404;
        throw err;
      }
      const err: any = new Error('Not Found');
      err.status = 404;
      throw err;
    });

    await handler(ctx);

    expect(ctx.octokit.git.deleteRef).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      ref: 'heads/feat/resource-sap.aiadm-issue-176',
    });

    expect(createRequestPr).toHaveBeenCalledTimes(2);
    expect(postOnce.mock.calls.some((c) => String(c[2] ?? '').includes('Approved by @alice. Opened PR: #1001'))).toBe(
      true
    );
  });

  test('issue_comment: approval shows human-readable exists message when resource already exists on default branch', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
      pr: {
        branchNameTemplate: 'feat/resource-{resource}-issue-{issue}',
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 176,
      title: 'System Namespace: sap.aiadm',
      body: `### Namespace

sap.aiadm

### System Description

\`\`\`text
Example description
\`\`\`

### Contacts

\`\`\`text
owner@sap.com
\`\`\`

### Visibility

public

<!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    loadTemplate.mockResolvedValueOnce({
      title: 'System Namespace',
      name: 'System Namespace',
      body: [],
      labels: ['System Namespace'],
      _meta: {
        requestType: 'systemNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        path: '.github/ISSUE_TEMPLATE/1-system-namespace-request.yaml',
      },
    });

    parseForm.mockReturnValueOnce({
      namespace: 'sap.aiadm',
      description: 'Example description',
      contact: 'owner@sap.com',
      visibility: 'public',
    });

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsGrouped: null,
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.aiadm',
      nsType: 'system',
      template: {
        _meta: {
          requestType: 'systemNamespace',
          root: '/data/namespaces',
          schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        },
      },
    });

    createRequestPr.mockRejectedValueOnce(new Error("Resource 'sap.aiadm' already exists at data/namespaces"));

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({ data: { content: 'x', encoding: 'base64' } });

    await handler(ctx);

    expect(createRequestPr).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.git.deleteRef).not.toHaveBeenCalled();

    const bodies = postOnce.mock.calls.map((c) => String(c[2] ?? '')).join('\n');
    expect(bodies).toContain("Failed to create PR automatically: Resource 'sap.aiadm' already exists in the registry.");
  });

  test('issue_comment: approval shows human-readable stale branch message when retry after no-commits also fails', async () => {
    const cfg = {
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['alice'],
      },
      pr: {
        branchNameTemplate: 'feat/resource-{resource}-issue-{issue}',
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 176,
      title: 'System Namespace: sap.aiadm',
      body: `### Namespace

sap.aiadm

### System Description

\`\`\`text
Example description
\`\`\`

### Contacts

\`\`\`text
owner@sap.com
\`\`\`

### Visibility

public

<!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->`,
      labels: ['needs-review'],
      user: { login: 'requester' },
    };

    loadTemplate.mockResolvedValueOnce({
      title: 'System Namespace',
      name: 'System Namespace',
      body: [],
      labels: ['System Namespace'],
      _meta: {
        requestType: 'systemNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        path: '.github/ISSUE_TEMPLATE/1-system-namespace-request.yaml',
      },
    });

    parseForm.mockReturnValueOnce({
      namespace: 'sap.aiadm',
      description: 'Example description',
      contact: 'owner@sap.com',
      visibility: 'public',
    });

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsGrouped: null,
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.aiadm',
      nsType: 'system',
      template: {
        _meta: {
          requestType: 'systemNamespace',
          root: '/data/namespaces',
          schema: '.github/registry-bot/request-schemas/system-namespace.schema.json',
        },
      },
    });

    createRequestPr
      .mockRejectedValueOnce(
        new Error(
          'Validation Failed: {"resource":"PullRequest","code":"custom","message":"No commits between main and feat/resource-sap.aiadm-issue-176"} - https://docs.github.com/enterprise-server@3.17/rest/pulls/pulls#create-a-pull-request'
        )
      )
      .mockRejectedValueOnce(
        new Error(
          'Validation Failed: {"resource":"PullRequest","code":"custom","message":"No commits between main and feat/resource-sap.aiadm-issue-176"} - https://docs.github.com/enterprise-server@3.17/rest/pulls/pulls#create-a-pull-request'
        )
      );

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
      config: cfg,
    });

    await handler(ctx);

    expect(ctx.octokit.git.deleteRef).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      ref: 'heads/feat/resource-sap.aiadm-issue-176',
    });

    const bodies = postOnce.mock.calls.map((c) => String(c[2] ?? '')).join('\n');
    expect(bodies).toContain(
      "Failed to create PR automatically: stale request branch 'feat/resource-sap.aiadm-issue-176' blocked PR creation. Please retry approval."
    );
    expect(bodies).not.toContain('Validation Failed: {"resource":"PullRequest"');
    expect(bodies).not.toContain('https://docs.github.com/');
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
    });

    const openCtx = mkIssuesContext({ issue, action: 'opened' });
    const topYaml = `contacts:\n  - "@topOwner"\n`;
    const barYaml = `contacts:\n  - "@barOwner"\n`;

    (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }
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

  test('check_suite.success: direct PR without snapshot hash uses approved onApproval result and calls tryMergeIfGreen', async () => {
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
      sha: 'sha1',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({ data: [{ number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } }] })
      .mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-five.yaml', status: 'modified' }],
    });
    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-five\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

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
    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'ignored-author' }, committer: { login: 'last-committer' } }],
    });
    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalled();

    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({ requestAuthorId: 'author' })
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
        { filename: '.github/workflows/review.yaml', status: 'modified' },
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

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'ignored-author' }, committer: { login: 'direct-last-committer' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 51,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/direct', sha: 'sha-standalone-direct' },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved from standalone hook' } as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.listFiles).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 51, per_page: 100, page: 1 })
    );
    expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        path: 'resources/product-one.yaml',
        ref: 'sha-standalone-direct',
      })
    );
    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-one',
        resourceName: 'product-one',
        requestAuthorId: 'ignored-author',
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
        body: expect.stringContaining('approved from standalone hook'),
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
  });

  test('check_suite.success: standalone direct PR unknown mix routes to review and does not merge', async () => {
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

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 53 }),
      expect.stringContaining('<summary>Decision details</summary>'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
    expect(postedBodies()).toContain('"status": "unknown"');
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

    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    const posted = postedBodies();
    expect(posted).toContain('automatic PR approval failed');
    expect(posted).toContain('approve please');
  });

  test('check_suite.success: direct PR without snapshot hash rejects and closes PR plus request', async () => {
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
    const pr = { number: 5, body: 'source: #1', head: { ref: 'x', sha: 'sha1' } };
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha1',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-five.yaml', status: 'modified' }],
    });
    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-five\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });
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
      sha: 'sha1',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 5,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'x', sha: 'sha1' },
            base: { ref: 'main' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-five.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-five\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    extractHashFromPrBody.mockReturnValue('');
    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      path: 'issue.author',
      reason: 'manual review required',
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.update).not.toHaveBeenCalled();
    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 5 }),
      expect.stringContaining('<summary>Decision details</summary>'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
    expect(postedBodies()).toContain('manual review required');
    expect(postedBodies()).not.toContain('## onApproval feedback');
  });

  test('check_suite.success: linked direct PR falls back to standalone approval when linked issue cannot be loaded', async () => {
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
      sha: 'sha-fallback-standalone',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 61,
            body: 'source: #999',
            title: 'Direct',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/fallback', sha: 'sha-fallback-standalone' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 61,
        body: 'source: #999',
        title: 'Direct',
        state: 'open',
        draft: false,
        user: { login: 'requester' },
        base: { ref: 'main' },
        head: { ref: 'feature/fallback', sha: 'sha-fallback-standalone' },
      },
    });

    ctx.octokit.issues.get.mockRejectedValueOnce(httpErr(500));

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-fallback.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-fallback\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'approved',
      comment: 'approved fallback',
    } as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        pull_number: 61,
        event: 'APPROVE',
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 61,
        mergeMethod: 'squash',
      })
    );
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
    const reviewCall = ctx.octokit.pulls.createReview.mock.calls[0]?.[0] as { body?: string };
    const reviewBody = String(reviewCall?.body ?? '');

    expect(reviewBody).toContain('nsreq:auto-approval:');
    expect(reviewBody).not.toContain('Approved automatically by onApproval hook.');
    expect(reviewBody).not.toContain('Manual approval required');
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 55, mergeMethod: 'squash' })
    );
    expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR requests branch update when merge helper returns false', async () => {
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
      sha: 'sha-merge-false',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 155,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/merge-false', sha: 'sha-merge-false' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-merge-false.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-merge-false\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ committer: { login: 'merge-helper-user' } }],
    });
    ctx.octokit.pulls.get.mockResolvedValue({
      data: {
        number: 155,
        state: 'open',
        body: 'manual direct pr',
        head: { ref: 'feature/merge-false', sha: 'sha-merge-false' },
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);
    tryMergeIfGreen.mockResolvedValueOnce(false as never);

    await handler(ctx);

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 155, mergeMethod: 'squash' })
    );
    expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR merge blocked by branch protection does not request branch update', async () => {
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
      sha: 'sha-branch-protection',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
      if (typeof callback === 'function') callback();
      return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 156,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/branch-protection', sha: 'sha-branch-protection' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-branch-protection.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-branch-protection\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ committer: { login: 'branch-protection-user' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValue({
      data: {
        number: 156,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/branch-protection', sha: 'sha-branch-protection' },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved by hook' } as any);
    tryMergeIfGreen.mockRejectedValueOnce(new Error('Required status check "ci" is expected'));

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 156, event: 'APPROVE' })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 156, mergeMethod: 'squash' })
    );
    expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();

    setTimeoutSpy.mockRestore();
  });

  test('check_suite.success: standalone direct PR skips merge when latest approval is missing', async () => {
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
      sha: 'sha-no-approval',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 157,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/no-approval', sha: 'sha-no-approval' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-no-approval.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-no-approval\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ committer: { login: 'no-approval-user' } }],
    });

    runApprovalHook.mockResolvedValueOnce(false);

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR skips merge when current head checks are not green', async () => {
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
      sha: 'sha-not-green',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 158,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/not-green', sha: 'sha-not-green' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-not-green.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: product\nname: product-not-green\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ committer: { login: 'green-check-user' } }],
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);
    ctx.octokit.checks.listForRef.mockResolvedValueOnce({
      data: {
        check_runs: [{ id: 2, name: 'ci', status: 'completed', conclusion: 'failure' }],
      },
    });

    await handler(ctx);

    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();
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

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 56 }),
      expect.stringContaining('<summary>Decision details</summary>'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
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

    expect(ctx.octokit.pulls.listFiles.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 58 }),
      expect.stringContaining('<summary>Decision details</summary>'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
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

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 59 }),
      expect.stringContaining('<summary>Decision details</summary>'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
  });

  test('check_suite.success: standalone direct PR ignores non-registry yaml files during onApproval evaluation', async () => {
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
      sha: 'sha-non-registry-yaml',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 60,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/non-registry-yaml', sha: 'sha-non-registry-yaml' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-ok.yaml', status: 'modified' },
        { filename: '.github/release.yml', status: 'modified' },
      ],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from(
          'type: product\nidentifier: product-ok\ntitle: Product OK\nvisibility: public\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ committer: { login: 'registry-committer' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 60,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/non-registry-yaml', sha: 'sha-non-registry-yaml' },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(ctx.octokit.repos.getContent).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        path: 'resources/product-ok.yaml',
        ref: 'sha-non-registry-yaml',
      })
    );
    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestAuthorId: 'registry-committer',
        formData: expect.objectContaining({
          identifier: 'product-ok',
          title: 'Product OK',
          visibility: 'public',
        }),
      })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 60, mergeMethod: 'squash' })
    );
  });

  test('check_suite.success: standalone cross-repo direct PR falls back to tree diff and reads yaml from head repo', async () => {
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
      sha: 'sha-cross-repo-direct',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const forkRepo = { full_name: 'fork-owner/fork-repo', name: 'fork-repo', owner: { login: 'fork-owner' } };

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 61,
            body: 'manual direct pr',
            title: 'Direct',
            head: { ref: 'feature/cross-repo', sha: 'sha-cross-repo-direct', repo: forkRepo },
            base: { ref: 'main', sha: 'base-sha' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({ data: [] });
    ctx.octokit.repos.getBranch = jest.fn(async () => ({ data: { commit: { sha: 'base-head-sha' } } }));
    ctx.octokit.git.getTree = jest.fn(async ({ owner, repo, tree_sha }: any) => {
      if (owner === 'o1' && repo === 'r1' && tree_sha === 'base-head-sha') {
        return {
          data: { tree: [{ path: 'resources/product-fork.yaml', type: 'blob', sha: 'base-file-sha' }] },
        };
      }

      if (owner === 'fork-owner' && repo === 'fork-repo' && tree_sha === 'sha-cross-repo-direct') {
        return {
          data: { tree: [{ path: 'resources/product-fork.yaml', type: 'blob', sha: 'fork-file-sha' }] },
        };
      }

      return { data: { tree: [] } };
    });

    ctx.octokit.repos.getContent = jest.fn(async ({ owner, repo, path, ref }: any) => {
      if (
        owner === 'fork-owner' &&
        repo === 'fork-repo' &&
        path === 'resources/product-fork.yaml' &&
        ref === 'sha-cross-repo-direct'
      ) {
        return {
          data: {
            content: Buffer.from('type: product\nname: product-fork\ncontact: fork@example.com\n', 'utf8').toString(
              'base64'
            ),
            encoding: 'base64',
          },
        };
      }

      throw httpErr(404);
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'fork-author' }, committer: { login: 'fork-committer' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 61,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/cross-repo', sha: 'sha-cross-repo-direct', repo: forkRepo },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved cross repo' } as any);

    await handler(ctx);

    expect(ctx.octokit.git.getTree).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', tree_sha: 'base-head-sha', recursive: 'true' })
    );
    expect(ctx.octokit.git.getTree).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'fork-owner',
        repo: 'fork-repo',
        tree_sha: 'sha-cross-repo-direct',
        recursive: 'true',
      })
    );
    expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'fork-owner',
        repo: 'fork-repo',
        path: 'resources/product-fork.yaml',
        ref: 'sha-cross-repo-direct',
      })
    );
    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-fork',
        resourceName: 'product-fork',
        requestAuthorId: 'fork-author',
        formData: expect.objectContaining({
          identifier: 'product-fork',
          namespace: 'product-fork',
          contact: 'fork@example.com',
        }),
      })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 61, mergeMethod: 'squash' })
    );
  });

  test('check_suite.success: standalone cross-repo direct PR ignores issue number pattern in head branch name', async () => {
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
      sha: 'sha-cross-repo-issue-branch',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const forkRepo = { full_name: 'fork-owner/fork-repo', name: 'fork-repo', owner: { login: 'fork-owner' } };

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 62,
            body: 'manual direct pr',
            title: 'Direct from fork',
            head: { ref: 'feature/issue-119-from-fork', sha: 'sha-cross-repo-issue-branch', repo: forkRepo },
            base: { ref: 'main', sha: 'base-sha' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/product-fork-issue.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent = jest.fn(async ({ owner, repo, path, ref }: any) => {
      if (
        owner === 'fork-owner' &&
        repo === 'fork-repo' &&
        path === 'resources/product-fork-issue.yaml' &&
        ref === 'sha-cross-repo-issue-branch'
      ) {
        return {
          data: {
            content: Buffer.from('type: product\nname: product-fork-issue\n', 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }

      throw httpErr(404);
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'fork-author-issue' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 62,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct from fork',
        head: { ref: 'feature/issue-119-from-fork', sha: 'sha-cross-repo-issue-branch', repo: forkRepo },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-fork-issue',
        resourceName: 'product-fork-issue',
        requestAuthorId: 'fork-author-issue',
      })
    );
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 62, mergeMethod: 'squash' })
    );
  });

  test('check_suite.success: standalone direct PR unknown approval assigns hook manual approvers instead of request-type pool', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-neutral-review',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 161,
            body: 'manual direct pr',
            title: 'Direct',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/neutral-review', sha: 'sha-neutral-review' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-neutral.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-neutral\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
      approvers: ['hookApproverShouldNotBeAssigned'],
    } as any);

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);

    Object.assign(ctx.octokit.issues, {
      addAssignees,
    });

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(setStateLabel).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 161 }),
      expect.objectContaining({ number: 161 }),
      'review'
    );

    expect(ensureAssigneesOnce).toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 161 }),
      expect.objectContaining({ number: 161 }),
      ['hookApproverShouldNotBeAssigned'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 161,
        assignees: ['hookApproverShouldNotBeAssigned'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['poolA']),
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 161,
        labels: ['registry-bot', 'needs-review'],
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('<!-- nsreq:snapshot:');
    expectHandoverPosted();
    expect(posted).toContain('manual review required');
    expect(posted).toContain('<summary>Decision details</summary>');
  });

  test('check_suite.success: standalone direct PR onApproval no-match assigns only deterministic request-type approversPool reviewer', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-direct-no-match-pool-assignment',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const pr = {
      number: 425,
      body: 'manual direct pr',
      title: 'Direct no-match',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-no-match', sha: 'sha-direct-no-match-pool-assignment' },
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-no-match.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-no-match\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    runApprovalHook.mockResolvedValue({} as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 425 }),
      expect.objectContaining({ number: 425 }),
      ['poolA'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 425,
        assignees: ['poolA'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['configuredApprover']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalApprover']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalPool']),
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('Manual review required because onApproval did not approve all changed registry files.');
    expectHandoverPosted();
  });

  test('check_suite.success: standalone direct PR onApproval unknown assigns only hook manual approvers and not approversPool', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-direct-hook-manual-assignment',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const pr = {
      number: 426,
      body: 'manual direct pr',
      title: 'Direct hook manual',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-hook-manual', sha: 'sha-direct-hook-manual-assignment' },
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-hook-manual.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-hook-manual\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
      approvers: ['hookManualApprover'],
    } as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 426 }),
      expect.objectContaining({ number: 426 }),
      ['hookManualApprover'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 426,
        assignees: ['hookManualApprover'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['poolA']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['poolB']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['configuredApprover']),
      })
    );

    expect(postedBodies()).toContain('manual review required');
  });

  test('check_suite.success: standalone direct PR without approversPool does not assign all configured approvers', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApproverA', 'configuredApproverB'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-direct-no-pool-no-assignment',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const pr = {
      number: 427,
      body: 'manual direct pr',
      title: 'Direct no pool',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-no-pool', sha: 'sha-direct-no-pool-no-assignment' },
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-no-pool.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-no-pool\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    runApprovalHook.mockResolvedValue({} as any);

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
    expect(addAssignees).not.toHaveBeenCalled();

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 427,
        labels: ['registry-bot', 'needs-review'],
      })
    );

    expect(postedBodies()).toContain('### ➡️ Routing to an approver for review');
  });

  test('issue_comment.created: standalone direct PR approversPool member can approve even when not assigned', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const ctx: any = {
      name: 'issue_comment.created',
      payload: {
        issue: {
          number: 428,
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          pull_request: {},
          labels: [{ name: 'needs-review' }],
        },
        comment: {
          body: 'Approved',
          user: { login: 'poolB' },
        },
        sender: {
          login: 'poolB',
          type: 'User',
        },
      },
      issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 428 }),
      octokit: mkCheckSuiteContext({
        event: 'check_suite.completed',
        conclusion: 'success',
        sha: 'sha-unused',
        ownerLogin: 'o1',
        repoName: 'r1',
        withCachedConfig: true,
        config: cfg,
      }).octokit,
      log: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
      resourceBotConfig: withWorkflowLabels(cfg),
      resourceBotHooks: {},
      resourceBotHooksSource: 'test',
    };

    ctx.octokit.pulls.get
      .mockResolvedValueOnce({
        data: {
          number: 428,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr-pool-approval', sha: 'sha-direct-pr-pool-approval' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      })
      .mockResolvedValue({
        data: {
          number: 428,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr-pool-approval', sha: 'sha-direct-pr-pool-approval' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'poolA' }],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-pool-approval.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-pool-approval\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    runApprovalHook.mockResolvedValue({} as any);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        pull_number: 428,
        event: 'APPROVE',
        body: expect.stringContaining('Approved by @poolB'),
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 428,
        mergeMethod: 'squash',
      })
    );

    expect(postedBodies()).not.toContain('not an allowed approver');
    expect(runApprovalHook).toHaveBeenCalled();

    expect(postOnce).not.toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 423 }),
      expect.stringContaining('Routing to an approver for review'),
      expect.anything()
    );

    expect(ensureAssigneesOnce).not.toHaveBeenCalled();
  });

  test('check_suite.success: standalone direct PR unknown approval without hook approvers falls back to request-type pool', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-neutral-review-pool-fallback',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 261,
            body: 'manual direct pr',
            title: 'Direct',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/neutral-review-pool-fallback', sha: 'sha-neutral-review-pool-fallback' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-neutral-pool.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-neutral-pool\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);

    Object.assign(ctx.octokit.issues, {
      addAssignees,
    });

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 261 }),
      expect.objectContaining({ number: 261 }),
      ['poolA'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 261,
        assignees: ['poolA'],
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 261,
        labels: ['registry-bot', 'needs-review'],
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('<!-- nsreq:snapshot:');
    expectHandoverPosted();
    expect(posted).toContain('manual review required');
    expect(posted).toContain('<summary>Decision details</summary>');
  });
  test('issue_comment: standalone direct PR Approved comment by hook approver creates automated approval review', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
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

      const issue = inReview({
        number: 162,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: cfg,
      });

      ctx.octokit.pulls.get.mockResolvedValue({
        data: {
          number: 162,
          body: 'manual direct pr',
          title: 'Direct PR',
          state: 'open',
          draft: false,
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-pr-comment', sha: 'sha-direct-pr-comment' },
        },
      });

      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-comment.yaml', status: 'modified' }],
      });

      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-comment\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

      ctx.octokit.pulls.listCommits.mockResolvedValue({
        data: [{ author: { login: 'requester' } }],
      });

      ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
      ctx.octokit.checks.listForRef.mockResolvedValue({
        data: {
          check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
        },
      });

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['reviewer1'],
      } as any);

      await handlers['issue_comment.created'][0](ctx);

      expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
        expect.objectContaining({
          owner: 'o',
          repo: 'r',
          pull_number: 162,
          event: 'APPROVE',
          body: expect.stringContaining('Approved by @reviewer1'),
        })
      );
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });

  test('issue_comment: standalone direct PR Approved comment reuses existing current-head auto approval review', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
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

      const issue = inReview({
        number: 1621,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: cfg,
      });

      ctx.octokit.pulls.get.mockResolvedValue({
        data: {
          number: 1621,
          body: 'manual direct pr',
          title: 'Direct PR',
          state: 'open',
          draft: false,
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-pr-comment-existing-review', sha: 'sha-direct-pr-comment-existing-review' },
        },
      });

      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-comment-existing-review.yaml', status: 'modified' }],
      });

      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-comment-existing-review\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

      ctx.octokit.pulls.listCommits.mockResolvedValue({
        data: [{ author: { login: 'requester' } }],
      });

      ctx.octokit.pulls.listReviews.mockResolvedValue({
        data: [
          {
            id: 1,
            state: 'APPROVED',
            user: { login: 'my-registry-bot[bot]' },
            body: 'Approved by @reviewer1\n\n<!-- nsreq:auto-approval:sha-direct-pr-comment-existing-review -->',
          },
        ],
      });
      ctx.octokit.checks.listForRef.mockResolvedValue({
        data: {
          check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
        },
      });

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['reviewer1'],
      } as any);

      await handlers['issue_comment.created'][0](ctx);

      expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
      expect(tryMergeIfGreen).toHaveBeenCalledWith(
        ctx,
        expect.objectContaining({ owner: 'o', repo: 'r', prNumber: 1621, mergeMethod: 'squash' })
      );
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });

  test('issue_comment: concurrent standalone direct PR approvals dedupe automated review creation in flight', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
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

      const issue = inReview({
        number: 16215,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const sharedPr = {
        number: 16215,
        body: 'manual direct pr',
        title: 'Direct PR',
        state: 'open',
        draft: false,
        user: { login: 'requester' },
        base: { ref: 'main' },
        head: { ref: 'feature/direct-pr-comment-dedupe', sha: 'sha-direct-pr-comment-dedupe' },
      };

      let releaseCreateReview: (() => void) | null = null;
      const createReview = jest.fn(
        () =>
          new Promise((resolve) => {
            releaseCreateReview = () => resolve({});
          })
      );

      const makeCtx = () => {
        const ctx = mkCommentContext({
          event: 'issue_comment.created',
          issue,
          comment: { body: 'Approved', user: { login: 'reviewer1' } },
          sender: { type: 'User', login: 'reviewer1' },
          withCachedConfig: true,
          config: cfg,
        });

        ctx.octokit.pulls.get.mockResolvedValue({ data: sharedPr });
        ctx.octokit.pulls.listFiles.mockResolvedValue({
          data: [{ filename: 'resources/product-comment-dedupe.yaml', status: 'modified' }],
        });
        ctx.octokit.repos.getContent.mockResolvedValue({
          data: {
            content: Buffer.from('type: product\nname: product-comment-dedupe\n', 'utf8').toString('base64'),
            encoding: 'base64',
          },
        });
        ctx.octokit.pulls.listCommits.mockResolvedValue({
          data: [{ author: { login: 'requester' } }],
        });
        ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
        ctx.octokit.checks.listForRef.mockResolvedValue({
          data: {
            check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
          },
        });
        ctx.octokit.pulls.createReview = createReview;

        return ctx;
      };

      const ctx1 = makeCtx();
      const ctx2 = makeCtx();

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['reviewer1'],
      } as any);

      const first = handlers['issue_comment.created'][0](ctx1);
      const second = handlers['issue_comment.created'][0](ctx2);

      await new Promise<void>((resolve) => process.nextTick(resolve));
      expect(createReview).toHaveBeenCalledTimes(1);

      const release = releaseCreateReview as (() => void) | null;
      if (release) {
        release();
      }

      await Promise.all([first, second]);

      expect(createReview).toHaveBeenCalledTimes(1);
      expect(
        ctx2.log.info.mock.calls.some((call: any[]) =>
          String(call[1] ?? call[0] ?? '').includes('automated PR approval review deduped: already in flight')
        )
      ).toBe(true);
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });

  test('issue_comment: second standalone direct PR Approved comment reuses marker review created by the first pass', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
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

      const issue = inReview({
        number: 1622,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: cfg,
      });

      const pr = {
        number: 1622,
        body: 'manual direct pr',
        title: 'Direct PR',
        state: 'open',
        draft: false,
        user: { login: 'requester' },
        base: { ref: 'main' },
        head: { ref: 'feature/direct-pr-comment-two-pass', sha: 'sha-direct-pr-comment-two-pass' },
      };

      ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-comment-two-pass.yaml', status: 'modified' }],
      });
      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-comment-two-pass\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });
      ctx.octokit.pulls.listCommits.mockResolvedValue({
        data: [{ author: { login: 'requester' } }],
      });
      ctx.octokit.checks.listForRef.mockResolvedValue({
        data: {
          check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
        },
      });

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['reviewer1'],
      } as any);

      ctx.octokit.pulls.listReviews.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
        data: [
          {
            id: 1,
            state: 'APPROVED',
            user: { login: 'my-registry-bot[bot]' },
            body: 'Approved by @reviewer1\n\n<!-- nsreq:auto-approval:sha-direct-pr-comment-two-pass -->',
          },
        ],
      });

      await handlers['issue_comment.created'][0](ctx);
      await handlers['issue_comment.created'][0](ctx);

      expect(ctx.octokit.pulls.createReview).toHaveBeenCalledTimes(1);
      expect(tryMergeIfGreen).toHaveBeenCalledTimes(2);
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });
  test('issue_comment: standalone direct PR Approved comment from unauthorized user is ignored', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
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

      const issue = inReview({
        number: 163,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'someone-else' } },
        sender: { type: 'User', login: 'someone-else' },
        withCachedConfig: true,
        config: cfg,
      });

      ctx.octokit.pulls.get.mockResolvedValue({
        data: {
          number: 163,
          body: 'manual direct pr',
          title: 'Direct PR',
          state: 'open',
          draft: false,
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-pr-comment-denied', sha: 'sha-direct-pr-comment-denied' },
        },
      });

      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-denied.yaml', status: 'modified' }],
      });

      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-denied\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['reviewer1'],
      } as any);

      await handlers['issue_comment.created'][0](ctx);

      expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
      expect(postOnce).toHaveBeenCalledWith(
        ctx,
        expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 163 }),
        expect.stringContaining('not an allowed approver'),
        expect.objectContaining({ minimizeTag: 'nsreq:approval-info' })
      );
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });

  test('issue_comment: standalone direct PR approval requests sequential branch update when the PR is behind', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
      const cfg = {
        requests: {
          product: { folderName: 'resources', approvers: ['reviewer1'] },
        },
        workflow: {
          labels: { approvalSuccessful: ['Approved'] },
          approvers: [],
        },
      };

      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = inReview({
        number: 1623,
        title: 'Direct PR',
        body: 'manual direct pr',
        labels: [],
        user: { login: 'requester' },
        pull_request: {},
      });

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: cfg,
      });

      const pr = {
        number: 1623,
        body: 'manual direct pr',
        title: 'Direct PR',
        state: 'open',
        draft: false,
        user: { login: 'requester' },
        base: { ref: 'main', sha: 'base-sha' },
        head: { ref: 'feature/direct-pr-sequential-branch-update', sha: 'sha-direct-pr-sequential-branch-update' },
        mergeable: true,
        mergeable_state: 'behind',
      };

      ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
      ctx.octokit.pulls.list.mockResolvedValue({ data: [pr] });
      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-direct-pr-sequential-branch-update.yaml', status: 'modified' }],
      });
      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-direct-pr-sequential-branch-update\n', 'utf8').toString(
            'base64'
          ),
          encoding: 'base64',
        },
      });
      ctx.octokit.pulls.listCommits.mockResolvedValue({
        data: [{ author: { login: 'requester' } }],
      });
      ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
      ctx.octokit.pulls.updateBranch.mockResolvedValueOnce({});

      await handlers['issue_comment.created'][0](ctx);

      expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
        expect.objectContaining({ owner: 'o', repo: 'r', pull_number: 1623, event: 'APPROVE' })
      );
      expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
        expect.objectContaining({
          owner: 'o',
          repo: 'r',
          pull_number: 1623,
          expected_head_sha: 'sha-direct-pr-sequential-branch-update',
        })
      );
      expect(tryMergeIfGreen).not.toHaveBeenCalled();
    } finally {
      if (previousJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = previousJestWorkerId;
      }
    }
  });
  test('check_suite.success: standalone direct PR approval label cleanup is skipped for cross-repo PRs', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-cross-repo-approved-cleanup',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    const forkRepo = {
      full_name: 'fork-owner/fork-repo',
      name: 'fork-repo',
      owner: { login: 'fork-owner' },
    };

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 201,
            body: 'manual direct pr',
            title: 'Direct from fork',
            user: { login: 'fork-author' },
            head: {
              ref: 'feature/issue-999-from-fork',
              sha: 'sha-cross-repo-approved-cleanup',
              repo: forkRepo,
            },
            base: { ref: 'main', sha: 'base-sha' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-cross-cleanup.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent = jest.fn(async ({ owner, repo, path, ref }: any) => {
      if (
        owner === 'fork-owner' &&
        repo === 'fork-repo' &&
        path === 'resources/product-cross-cleanup.yaml' &&
        ref === 'sha-cross-repo-approved-cleanup'
      ) {
        return {
          data: {
            content: Buffer.from('type: product\nname: product-cross-cleanup\n', 'utf8').toString('base64'),
            encoding: 'base64',
          },
        };
      }

      throw httpErr(404);
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'fork-author' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 201,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct from fork',
        user: { login: 'fork-author' },
        head: {
          ref: 'feature/issue-999-from-fork',
          sha: 'sha-cross-repo-approved-cleanup',
          repo: forkRepo,
        },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 201,
        labels: ['Approved'],
      })
    );

    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
    expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();

    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-cross-cleanup',
        resourceName: 'product-cross-cleanup',
        requestAuthorId: 'fork-author',
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 201,
        mergeMethod: 'squash',
      })
    );
  });

  test('check_suite.success: standalone same-repo direct PR approved path cleans review and rejected labels after approved label', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-same-repo-cleanup',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 202,
            body: 'manual direct pr',
            title: 'Direct same repo',
            user: { login: 'requester' },
            head: { ref: 'feature/direct-cleanup', sha: 'sha-same-repo-cleanup' },
            base: { ref: 'main', sha: 'base-sha' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-same-cleanup.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-same-cleanup\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 202,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct same repo',
        user: { login: 'requester' },
        head: { ref: 'feature/direct-cleanup', sha: 'sha-same-repo-cleanup' },
        base: { ref: 'main', sha: 'base-sha' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'Approved' }, { name: 'needs-review' }, { name: 'Requester Action' }, { name: 'Rejected' }],
        assignees: [],
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 202,
        labels: ['Approved'],
      })
    );

    expect(ctx.octokit.issues.get).toHaveBeenCalledWith({
      owner: 'o1',
      repo: 'r1',
      issue_number: 202,
    });

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 202,
        name: 'needs-review',
      })
    );

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 202,
        name: 'Requester Action',
      })
    );

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 202,
        name: 'Rejected',
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 202,
        mergeMethod: 'squash',
      })
    );
  });

  test('check_suite.success: standalone direct PR unknown approval falls back to global pool when registry doc cannot resolve request type', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPoolB', 'globalPoolA'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-unknown-global-pool',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 203,
            body: 'manual direct pr',
            title: 'Direct unresolved doc',
            user: { login: 'requester' },
            head: { ref: 'feature/unresolved-doc', sha: 'sha-unknown-global-pool' },
            base: { ref: 'main' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-unresolved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockRejectedValue(httpErr(404));

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    await handler(ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(setStateLabel).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 203 }),
      expect.objectContaining({ number: 203 }),
      'review'
    );

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 203 }),
      expect.objectContaining({ number: 203 }),
      ['globalPoolA'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 203,
        assignees: ['globalPoolA'],
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 203,
        labels: ['registry-bot', 'needs-review'],
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('<!-- nsreq:snapshot:');
    expectHandoverPosted();
  });
  test('issues.opened: onApproval unknown manual approvers override assignment pool only', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 301,
      title: 'Product Request',
      body: '### Product ID\n\nproduct-manual-assignee',
      labels: [],
      user: { login: 'requester' },
      state: 'open',
    };

    const ctx = mkIssuesContext({
      action: 'opened',
      issue,
      withCachedConfig: true,
      config: cfg,
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        ...issue,
        labels: [],
        assignees: [],
      },
    });

    parseForm.mockReturnValue({
      'product-id': 'product-manual-assignee',
    });

    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'product-manual-assignee',
      nsType: 'product',
      formData: {
        'product-id': 'product-manual-assignee',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
      approvers: ['hookManualApprover'],
    } as any);

    await handlers['issues.opened'][0](ctx);

    expect(setStateLabel).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 301 }),
      expect.objectContaining({ number: 301 }),
      'review'
    );

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 301 }),
      expect.objectContaining({ number: 301 }),
      ['hookManualApprover'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 301,
        assignees: ['hookManualApprover'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['poolA']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalPool']),
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('manual review required');
    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expectHandoverPosted();
    expect(posted).toContain('<summary>Decision details</summary>');
  });

  test('issues.opened: onApproval unknown without manual approvers falls back to request-type pool assignment', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 302,
      title: 'Product Request',
      body: '### Product ID\n\nproduct-pool-fallback',
      labels: [],
      user: { login: 'requester' },
      state: 'open',
    };

    const ctx = mkIssuesContext({
      action: 'opened',
      issue,
      withCachedConfig: true,
      config: cfg,
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        ...issue,
        labels: [],
        assignees: [],
      },
    });

    parseForm.mockReturnValue({
      'product-id': 'product-pool-fallback',
    });

    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'product-pool-fallback',
      nsType: 'product',
      formData: {
        'product-id': 'product-pool-fallback',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await handlers['issues.opened'][0](ctx);

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 302 }),
      expect.objectContaining({ number: 302 }),
      ['poolB'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 302,
        assignees: ['poolB'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalPool']),
      })
    );

    expect(postedBodies()).toContain('manual review required');
    expectHandoverPosted();
  });

  test('issues.opened: handover still auto-adds review labels when label refresh fails', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 3021,
      title: 'Product Request',
      body: '### Product ID\n\nproduct-label-fetch-failed',
      labels: [],
      user: { login: 'requester' },
      state: 'open',
    };

    const ctx = mkIssuesContext({
      action: 'opened',
      issue,
      withCachedConfig: true,
      config: cfg,
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    ctx.octokit.issues.get
      .mockResolvedValueOnce({
        data: {
          ...issue,
          labels: [],
          assignees: [],
        },
      })
      .mockRejectedValueOnce(new Error('label refresh failed'));

    parseForm.mockReturnValue({
      'product-id': 'product-label-fetch-failed',
    });

    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'product-label-fetch-failed',
      nsType: 'product',
      formData: {
        'product-id': 'product-label-fetch-failed',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await handlers['issues.opened'][0](ctx);

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 3021,
        labels: ['registry-bot', 'needs-review'],
      })
    );
    expect(
      ctx.log.warn.mock.calls.some((call: any[]) =>
        call.some((arg: unknown) => String(arg ?? '').includes('failed to ensure labels'))
      )
    ).toBe(false);
    expectHandoverPosted();
  });

  test('issues.opened: handover logs when auto-adding review labels fails with non-404', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 3022,
      title: 'Product Request',
      body: '### Product ID\n\nproduct-label-add-failed',
      labels: [],
      user: { login: 'requester' },
      state: 'open',
    };

    const ctx = mkIssuesContext({
      action: 'opened',
      issue,
      withCachedConfig: true,
      config: cfg,
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    ctx.octokit.issues.get
      .mockResolvedValueOnce({
        data: {
          ...issue,
          labels: [],
          assignees: [],
        },
      })
      .mockRejectedValueOnce(new Error('label refresh failed'));
    ctx.octokit.issues.addLabels.mockRejectedValueOnce(httpErr(500));

    parseForm.mockReturnValue({
      'product-id': 'product-label-add-failed',
    });

    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsGrouped: {},
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'product-label-add-failed',
      nsType: 'product',
      formData: {
        'product-id': 'product-label-add-failed',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await handlers['issues.opened'][0](ctx);

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 3022,
        labels: ['registry-bot', 'needs-review'],
      })
    );
    expect(
      ctx.log.warn.mock.calls.some((call: any[]) =>
        call.some((arg: unknown) => String(arg ?? '').includes('failed to ensure labels'))
      )
    ).toBe(true);
    expectHandoverPosted();
  });

  test('issues.opened: concurrent unknown approvals dedupe the unknown-feedback post', async () => {
    const cfg = {
      workflow: {
        approvers: ['cpa-user'],
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
        },
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 3030,
      title: 'Request',
      body: 'Body',
      labels: [],
      user: { login: 'author' },
      state: 'open',
    };

    const ctx1 = mkIssuesContext({ action: 'opened', issue, withCachedConfig: true, config: cfg });
    const ctx2 = mkIssuesContext({ action: 'opened', issue, withCachedConfig: true, config: cfg });

    let releaseUnknownPost: (() => void) | null = null;
    postOnce.mockImplementation(async (_ctx, _params, _body, options) => {
      if (options?.minimizeTag === 'nsreq:on-approval:unknown') {
        await new Promise<void>((resolve) => {
          releaseUnknownPost = resolve;
        });
      }
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    const first = handlers['issues.opened'][0](ctx1);
    const second = handlers['issues.opened'][0](ctx2);

    await new Promise<void>((resolve) => process.nextTick(resolve));

    expect(
      postOnce.mock.calls.filter((call: any[]) => call[3]?.minimizeTag === 'nsreq:on-approval:unknown')
    ).toHaveLength(1);

    const release = releaseUnknownPost as (() => void) | null;
    if (release) {
      release();
    }

    await Promise.all([first, second]);

    expect(
      postOnce.mock.calls.filter((call: any[]) => call[3]?.minimizeTag === 'nsreq:on-approval:unknown')
    ).toHaveLength(1);
  });

  test('issues.opened: request-type approversPool is sorted and selected deterministically by issue number', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApproverShouldNotBeAssigned'],
        approversPool: ['globalPoolShouldNotBeAssigned'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    parseForm.mockImplementation((body?: unknown) => {
      const rawBody = String(body ?? '');

      return {
        'product-id': rawBody.includes('product-303') ? 'product-303' : 'product-302',
      };
    });

    validateRequestIssue.mockImplementation(async (_ctx?: unknown, params?: any) => {
      const issueNumber = Number(params?.issue_number ?? 0);
      const productId = issueNumber === 303 ? 'product-303' : 'product-302';

      return {
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: productId,
        nsType: 'product',
        formData: {
          'product-id': productId,
        },
      };
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);

    const mkPoolCtx = (issueNumber: number, productId: string): any => {
      const issue = {
        number: issueNumber,
        title: 'Product Request',
        body: `### Product ID\n\n${productId}`,
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const ctx = mkIssuesContext({
        action: 'opened',
        issue,
        withCachedConfig: true,
        config: cfg,
      });

      Object.assign(ctx.octokit.issues, { addAssignees });

      ctx.octokit.issues.get.mockResolvedValue({
        data: {
          ...issue,
          labels: [],
          assignees: [],
        },
      });

      return ctx;
    };

    const ctx302 = mkPoolCtx(302, 'product-302');
    const ctx303 = mkPoolCtx(303, 'product-303');

    await handlers['issues.opened'][0](ctx302);
    await handlers['issues.opened'][0](ctx303);

    const assigneeCall302 = (ensureAssigneesOnce as jest.Mock).mock.calls.find(
      (call: any[]) => call[1]?.issue_number === 302
    );
    const assigneeCall303 = (ensureAssigneesOnce as jest.Mock).mock.calls.find(
      (call: any[]) => call[1]?.issue_number === 303
    );

    // Pool is sorted internally: ['poolA', 'poolB'].
    // #302 => (302 - 1) % 2 = 1 => poolB
    // #303 => (303 - 1) % 2 = 0 => poolA
    expect(assigneeCall302?.[3]).toEqual(['poolB']);
    expect(assigneeCall303?.[3]).toEqual(['poolA']);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 302,
        assignees: ['poolB'],
      })
    );

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 303,
        assignees: ['poolA'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalPoolShouldNotBeAssigned']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalApproverShouldNotBeAssigned']),
      })
    );
  });

  test('issues.opened: global approversPool fallback is sorted and selected deterministically by issue number', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApproverShouldNotBeAssignedWhenPoolExists'],
        approversPool: ['globalPoolB', 'globalPoolA'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    parseForm.mockImplementation((body?: unknown) => {
      const rawBody = String(body ?? '');

      return {
        'product-id': rawBody.includes('product-305') ? 'product-305' : 'product-304',
      };
    });

    validateRequestIssue.mockImplementation(async (_ctx?: unknown, params?: any) => {
      const issueNumber = Number(params?.issue_number ?? 0);
      const productId = issueNumber === 305 ? 'product-305' : 'product-304';

      return {
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: productId,
        nsType: 'product',
        formData: {
          'product-id': productId,
        },
      };
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);

    const mkPoolCtx = (issueNumber: number, productId: string): any => {
      const issue = {
        number: issueNumber,
        title: 'Product Request',
        body: `### Product ID\n\n${productId}`,
        labels: [],
        user: { login: 'requester' },
        state: 'open',
      };

      const ctx = mkIssuesContext({
        action: 'opened',
        issue,
        withCachedConfig: true,
        config: cfg,
      });

      Object.assign(ctx.octokit.issues, { addAssignees });

      ctx.octokit.issues.get.mockResolvedValue({
        data: {
          ...issue,
          labels: [],
          assignees: [],
        },
      });

      return ctx;
    };

    const ctx304 = mkPoolCtx(304, 'product-304');
    const ctx305 = mkPoolCtx(305, 'product-305');

    await handlers['issues.opened'][0](ctx304);
    await handlers['issues.opened'][0](ctx305);

    const assigneeCall304 = (ensureAssigneesOnce as jest.Mock).mock.calls.find(
      (call: any[]) => call[1]?.issue_number === 304
    );
    const assigneeCall305 = (ensureAssigneesOnce as jest.Mock).mock.calls.find(
      (call: any[]) => call[1]?.issue_number === 305
    );

    // Global pool is sorted internally: ['globalPoolA', 'globalPoolB'].
    // #304 => (304 - 1) % 2 = 1 => globalPoolB
    // #305 => (305 - 1) % 2 = 0 => globalPoolA
    expect(assigneeCall304?.[3]).toEqual(['globalPoolB']);
    expect(assigneeCall305?.[3]).toEqual(['globalPoolA']);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 304,
        assignees: ['globalPoolB'],
      })
    );

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 305,
        assignees: ['globalPoolA'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['globalApproverShouldNotBeAssignedWhenPoolExists']),
      })
    );
  });

  test.each([
    ['hook manual approver', 'hookManualApprover'],
    ['request configured approver', 'configuredApprover'],
    ['request approversPool member', 'poolApprover'],
  ])(
    'issue_comment.created: %s can approve request when onApproval returns hook manual approvers',
    async (_caseName, commenter) => {
      const cfg = {
        requests: {
          product: {
            folderName: 'resources',
            approvers: ['configuredApprover'],
            approversPool: ['poolApprover'],
          },
        },
        workflow: {
          labels: {
            approvalRequested: ['needs-review'],
            approvalSuccessful: ['Approved'],
          },
          approvers: ['globalApprover'],
          approversPool: ['globalPool'],
        },
      };

      const { app, handlers } = mkApp();
      requestHandler(app);

      const issue = {
        number: 303,
        title: 'Product Request',
        body: '### Product ID\n\nproduct-approval-access',
        labels: [{ name: 'needs-review' }],
        user: { login: 'requester' },
        state: 'open',
      };

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue,
        comment: {
          body: 'Approved',
          user: { login: commenter },
        },
        sender: {
          login: commenter,
          type: 'User',
        },
        withCachedConfig: true,
        config: cfg,
      });

      ctx.octokit.issues.get.mockResolvedValue({
        data: {
          ...issue,
          labels: [{ name: 'needs-review' }],
          assignees: [],
        },
      });

      parseForm.mockReturnValue({
        'product-id': 'product-approval-access',
      });

      validateRequestIssue.mockResolvedValue({
        errors: [],
        errorsGrouped: {},
        errorsFormatted: '',
        errorsFormattedSingle: '',
        namespace: 'product-approval-access',
        nsType: 'product',
        formData: {
          'product-id': 'product-approval-access',
        },
      });

      runApprovalHook.mockResolvedValue({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['hookManualApprover'],
      } as any);

      createRequestPr.mockResolvedValueOnce({ number: 1303 });

      await handlers['issue_comment.created'][0](ctx);

      expect(createRequestPr).toHaveBeenCalled();

      expect(
        postOnce.mock.calls.some((call) =>
          String(call[2] ?? '').includes(`Approved by @${commenter}. Opened PR: #1303`)
        )
      ).toBe(true);

      expect(postedBodies()).not.toContain(`commenter ${commenter} is not an allowed approver`);
    }
  );

  test('issue_comment.created: unrelated user cannot approve request when onApproval returns hook manual approvers', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 304,
      title: 'Product Request',
      body: '### Product ID\n\nproduct-denied-approval',
      labels: [{ name: 'needs-review' }],
      user: { login: 'requester' },
      state: 'open',
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: {
        body: 'Approved',
        user: { login: 'intruder' },
      },
      sender: {
        login: 'intruder',
        type: 'User',
      },
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        ...issue,
        labels: [{ name: 'needs-review' }],
        assignees: [],
      },
    });

    parseForm.mockReturnValue({
      'product-id': 'product-denied-approval',
    });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required',
      approvers: ['hookManualApprover'],
    } as any);

    await handlers['issue_comment.created'][0](ctx);

    expect(createRequestPr).not.toHaveBeenCalled();

    expect(postedBodies()).toContain(
      'Approval ignored: commenter intruder is not an allowed approver for this request type.'
    );
  });
  test('issue_comment.created: standalone direct PR approval is ignored before review handover', async () => {
    const cfg = {
      requests: {
        product: { folderName: 'resources' },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['allowedApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const ctx: any = {
      name: 'issue_comment.created',
      payload: {
        issue: {
          number: 204,
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          pull_request: {},
          labels: [],
        },
        comment: {
          body: 'Approved',
          user: { login: 'allowedApprover' },
        },
        sender: {
          login: 'allowedApprover',
          type: 'User',
        },
      },
      issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 204 }),
      octokit: mkCheckSuiteContext({
        event: 'check_suite.completed',
        conclusion: 'success',
        sha: 'sha-unused',
        ownerLogin: 'o1',
        repoName: 'r1',
        withCachedConfig: true,
        config: cfg,
      }).octokit,
      log: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
      resourceBotConfig: withWorkflowLabels(cfg),
      resourceBotHooks: {},
      resourceBotHooksSource: 'test',
    };

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 204,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct PR',
        user: { login: 'requester' },
        head: { ref: 'feature/direct-pr', sha: 'sha-direct-comment' },
        base: { ref: 'main' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: {
        labels: [],
        assignees: [],
      },
    });

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(postedBodies()).toContain(
      'Approval ignored: direct PR is not in review state. Please wait until validation has routed it to review.'
    );
  });

  test('issue_comment.created: standalone direct PR approval rejects unauthorized commenter in review state', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['allowedApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const ctx: any = {
      name: 'issue_comment.created',
      payload: {
        issue: {
          number: 205,
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          pull_request: {},
          labels: [{ name: 'needs-review' }],
        },
        comment: {
          body: 'Approved',
          user: { login: 'intruder' },
        },
        sender: {
          login: 'intruder',
          type: 'User',
        },
      },
      issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 205 }),
      octokit: mkCheckSuiteContext({
        event: 'check_suite.completed',
        conclusion: 'success',
        sha: 'sha-unused',
        ownerLogin: 'o1',
        repoName: 'r1',
        withCachedConfig: true,
        config: cfg,
      }).octokit,
      log: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
      resourceBotConfig: withWorkflowLabels(cfg),
      resourceBotHooks: {},
      resourceBotHooksSource: 'test',
    };

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        number: 205,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct PR',
        user: { login: 'requester' },
        head: { ref: 'feature/direct-pr', sha: 'sha-direct-comment-denied' },
        base: { ref: 'main' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-denied.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-denied\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(postedBodies()).toContain(
      'Approval ignored: commenter intruder is not an allowed approver for this direct PR.'
    );
  });

  test('issue_comment.created: standalone direct PR approval by allowed approver creates review and tries merge', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['allowedApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const ctx: any = {
      name: 'issue_comment.created',
      payload: {
        issue: {
          number: 206,
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          pull_request: {},
          labels: [{ name: 'needs-review' }],
        },
        comment: {
          body: 'Approved',
          user: { login: 'allowedApprover' },
        },
        sender: {
          login: 'allowedApprover',
          type: 'User',
        },
      },
      issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 206 }),
      octokit: mkCheckSuiteContext({
        event: 'check_suite.completed',
        conclusion: 'success',
        sha: 'sha-unused',
        ownerLogin: 'o1',
        repoName: 'r1',
        withCachedConfig: true,
        config: cfg,
      }).octokit,
      log: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
      resourceBotConfig: withWorkflowLabels(cfg),
      resourceBotHooks: {},
      resourceBotHooksSource: 'test',
    };

    ctx.octokit.pulls.get
      .mockResolvedValueOnce({
        data: {
          number: 206,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr', sha: 'sha-direct-comment-approved' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      })
      .mockResolvedValue({
        data: {
          number: 206,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr', sha: 'sha-direct-comment-approved' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-approved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-approved\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        pull_number: 206,
        event: 'APPROVE',
        body: expect.stringContaining('Approved by @allowedApprover'),
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 206,
        mergeMethod: 'squash',
      })
    );
  });

  test('issue_comment.created: standalone direct PR approval stops after review when Approved label cannot be added', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['allowedApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const ctx: any = {
      name: 'issue_comment.created',
      payload: {
        issue: {
          number: 2061,
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          pull_request: {},
          labels: [{ name: 'needs-review' }],
        },
        comment: {
          body: 'Approved',
          user: { login: 'allowedApprover' },
        },
        sender: {
          login: 'allowedApprover',
          type: 'User',
        },
      },
      issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 2061 }),
      octokit: mkCheckSuiteContext({
        event: 'check_suite.completed',
        conclusion: 'success',
        sha: 'sha-unused',
        ownerLogin: 'o1',
        repoName: 'r1',
        withCachedConfig: true,
        config: cfg,
      }).octokit,
      log: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
      },
      resourceBotConfig: withWorkflowLabels(cfg),
      resourceBotHooks: {},
      resourceBotHooksSource: 'test',
    };

    ctx.octokit.pulls.get
      .mockResolvedValueOnce({
        data: {
          number: 2061,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr-label-fail', sha: 'sha-direct-comment-approved-label-fail' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      })
      .mockResolvedValue({
        data: {
          number: 2061,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct PR',
          user: { login: 'requester' },
          head: { ref: 'feature/direct-pr-label-fail', sha: 'sha-direct-comment-approved-label-fail' },
          base: { ref: 'main' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [],
      },
    });

    ctx.octokit.issues.addLabels.mockRejectedValueOnce(new Error('cannot add approved label'));

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-approved-label-fail.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-approved-label-fail\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        pull_number: 2061,
        event: 'APPROVE',
        body: expect.stringContaining('Approved by @allowedApprover'),
      })
    );
    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 2061, labels: ['Approved'] })
    );
    expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 2061,
        mergeMethod: 'squash',
      })
    );
  });

  test('issue_comment.created: standalone direct PR approval by hook manual approver creates review and tries merge', async () => {
    const prevJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
      const cfg = {
        requests: {
          product: {
            folderName: 'resources',
            approvers: ['configuredApprover'],
          },
        },
        workflow: {
          labels: {
            approvalRequested: ['needs-review'],
            approvalSuccessful: ['Approved'],
          },
          approvers: ['globalApprover'],
        },
      };

      const { app, handlers } = mkApp();
      requestHandler(app);

      const handler = handlers['issue_comment.created'][0];

      const ctx: any = {
        name: 'issue_comment.created',
        payload: {
          issue: {
            number: 207,
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            pull_request: {},
            labels: [{ name: 'needs-review' }],
          },
          comment: {
            body: 'Approved',
            user: { login: 'hookManualApprover' },
          },
          sender: {
            login: 'hookManualApprover',
            type: 'User',
          },
        },
        issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 207 }),
        octokit: mkCheckSuiteContext({
          event: 'check_suite.completed',
          conclusion: 'success',
          sha: 'sha-unused',
          ownerLogin: 'o1',
          repoName: 'r1',
          withCachedConfig: true,
          config: cfg,
        }).octokit,
        log: {
          info: jest.fn(),
          warn: jest.fn(),
          error: jest.fn(),
          debug: jest.fn(),
        },
        resourceBotConfig: withWorkflowLabels(cfg),
        resourceBotHooks: {},
        resourceBotHooksSource: 'test',
      };

      ctx.octokit.pulls.get
        .mockResolvedValueOnce({
          data: {
            number: 207,
            state: 'open',
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            head: { ref: 'feature/direct-pr', sha: 'sha-hook-manual-approval' },
            base: { ref: 'main' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        })
        .mockResolvedValue({
          data: {
            number: 207,
            state: 'open',
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            head: { ref: 'feature/direct-pr', sha: 'sha-hook-manual-approval' },
            base: { ref: 'main' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        });

      ctx.octokit.issues.get.mockResolvedValue({
        data: {
          labels: [{ name: 'needs-review' }],
          assignees: [],
        },
      });

      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-hook-manual.yaml', status: 'modified' }],
      });

      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-hook-manual\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

      ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
        data: [{ author: { login: 'requester' } }],
      });

      ctx.octokit.pulls.listReviews.mockResolvedValue({
        data: [],
      });

      runApprovalHook.mockResolvedValueOnce({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['hookManualApprover'],
      } as any);

      await handler(ctx);

      expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
        expect.objectContaining({
          owner: 'o1',
          repo: 'r1',
          pull_number: 207,
          event: 'APPROVE',
          body: expect.stringContaining('Approved by @hookManualApprover'),
        })
      );

      expect(tryMergeIfGreen).toHaveBeenCalledWith(
        ctx,
        expect.objectContaining({
          owner: 'o1',
          repo: 'r1',
          prNumber: 207,
          mergeMethod: 'squash',
        })
      );
    } finally {
      if (prevJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = prevJestWorkerId;
      }
    }
  });

  test('issue_comment.created: configured approver can approve standalone direct PR even when onApproval returns hook manual approvers', async () => {
    const prevJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
      const cfg = {
        requests: {
          product: {
            folderName: 'resources',
            approvers: ['configuredApprover'],
          },
        },
        workflow: {
          labels: {
            approvalRequested: ['needs-review'],
            approvalSuccessful: ['Approved'],
          },
          approvers: ['globalApprover'],
        },
      };

      const { app, handlers } = mkApp();
      requestHandler(app);

      const handler = handlers['issue_comment.created'][0];

      const ctx: any = {
        name: 'issue_comment.created',
        payload: {
          issue: {
            number: 208,
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            pull_request: {},
            labels: [{ name: 'needs-review' }],
          },
          comment: {
            body: 'Approved',
            user: { login: 'configuredApprover' },
          },
          sender: {
            login: 'configuredApprover',
            type: 'User',
          },
        },
        issue: () => ({ owner: 'o1', repo: 'r1', issue_number: 208 }),
        octokit: mkCheckSuiteContext({
          event: 'check_suite.completed',
          conclusion: 'success',
          sha: 'sha-unused',
          ownerLogin: 'o1',
          repoName: 'r1',
          withCachedConfig: true,
          config: cfg,
        }).octokit,
        log: {
          info: jest.fn(),
          warn: jest.fn(),
          error: jest.fn(),
          debug: jest.fn(),
        },
        resourceBotConfig: withWorkflowLabels(cfg),
        resourceBotHooks: {},
        resourceBotHooksSource: 'test',
      };

      ctx.octokit.pulls.get
        .mockResolvedValueOnce({
          data: {
            number: 208,
            state: 'open',
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            head: { ref: 'feature/direct-pr', sha: 'sha-config-approval-with-hook-manual' },
            base: { ref: 'main' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        })
        .mockResolvedValue({
          data: {
            number: 208,
            state: 'open',
            body: 'manual direct pr',
            title: 'Direct PR',
            user: { login: 'requester' },
            head: { ref: 'feature/direct-pr', sha: 'sha-config-approval-with-hook-manual' },
            base: { ref: 'main' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        });

      ctx.octokit.issues.get.mockResolvedValue({
        data: {
          labels: [{ name: 'needs-review' }],
          assignees: [],
        },
      });

      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-config-approver.yaml', status: 'modified' }],
      });

      ctx.octokit.repos.getContent.mockResolvedValue({
        data: {
          content: Buffer.from('type: product\nname: product-config-approver\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      });

      ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
        data: [{ author: { login: 'requester' } }],
      });

      ctx.octokit.pulls.listReviews.mockResolvedValue({
        data: [],
      });

      runApprovalHook.mockResolvedValueOnce({
        status: 'unknown',
        message: 'manual review required',
        approvers: ['hookManualApprover'],
      } as any);

      await handler(ctx);

      expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
        expect.objectContaining({
          owner: 'o1',
          repo: 'r1',
          pull_number: 208,
          event: 'APPROVE',
          body: expect.stringContaining('Approved by @configuredApprover'),
        })
      );

      expect(tryMergeIfGreen).toHaveBeenCalledWith(
        ctx,
        expect.objectContaining({
          owner: 'o1',
          repo: 'r1',
          prNumber: 208,
          mergeMethod: 'squash',
        })
      );
    } finally {
      if (prevJestWorkerId === undefined) {
        delete process.env.JEST_WORKER_ID;
      } else {
        process.env.JEST_WORKER_ID = prevJestWorkerId;
      }
    }
  });

  test('check_suite.success: standalone direct PR onApproval no-match routes to review and assigns deterministic request-type pool member', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
        approversPool: ['globalPool'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-direct-no-match-review',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 421,
            body: 'manual direct pr',
            title: 'Direct fallback PR',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/direct-no-match', sha: 'sha-direct-no-match-review' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-no-match.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-no-match\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    // onApproval did not match this registry doc.
    runApprovalHook.mockResolvedValueOnce({} as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-no-match',
        resourceName: 'product-no-match',
        requestAuthorId: 'requester',
      })
    );

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(setStateLabel).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 421 }),
      expect.objectContaining({ number: 421 }),
      'review'
    );

    // pool is sorted to ['poolA', 'poolB']; issue/PR #421 => (421 - 1) % 2 = 0 => poolA
    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 421 }),
      expect.objectContaining({ number: 421 }),
      ['poolA'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 421,
        assignees: ['poolA'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['poolB', 'configuredApprover', 'globalApprover', 'globalPool']),
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 421,
        labels: ['registry-bot', 'needs-review'],
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('Manual review required because onApproval did not approve all changed registry files.');
    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('Once reviewed, please comment `Approved` to approve this PR for merge.');
    expectHandoverPosted();
  });

  test('issue_comment.created: standalone direct PR fallback allows any request-type pool approver, not only assigned one', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['globalApprover'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 421,
      title: 'Direct fallback PR',
      body: 'manual direct pr',
      labels: [{ name: 'needs-review' }],
      user: { login: 'requester' },
      pull_request: {},
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'poolB' } },
      sender: { type: 'User', login: 'poolB' },
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.get
      .mockResolvedValueOnce({
        data: {
          number: 421,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct fallback PR',
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-no-match', sha: 'sha-direct-no-match-approved' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      })
      .mockResolvedValue({
        data: {
          number: 421,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct fallback PR',
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-no-match', sha: 'sha-direct-no-match-approved' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'poolA' }],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-no-match.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-no-match\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    // no hook match; fallback approval path must be used
    runApprovalHook.mockResolvedValue({} as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        pull_number: 421,
        event: 'APPROVE',
        body: expect.stringContaining('Approved by @poolB'),
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        prNumber: 421,
        mergeMethod: 'squash',
      })
    );

    expect(
      postOnce.mock.calls.some((call) => String(call[2] ?? '').includes('not an allowed approver for this direct PR'))
    ).toBe(false);
  });

  test('issue_comment.created: standalone direct PR fallback rejects requester self-approval when requester is not configured approver', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 422,
      title: 'Direct fallback PR',
      body: 'manual direct pr',
      labels: [{ name: 'needs-review' }],
      user: { login: 'requester' },
      pull_request: {},
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'requester' } },
      sender: { type: 'User', login: 'requester' },
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.get.mockResolvedValue({
      data: {
        number: 422,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct fallback PR',
        user: { login: 'requester' },
        base: { ref: 'main' },
        head: { ref: 'feature/direct-no-match-self-approval', sha: 'sha-direct-no-match-self-approval' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'poolB' }],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-no-match-self.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-no-match-self\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    runApprovalHook.mockResolvedValue({} as any);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 422 }),
      expect.stringContaining('Approval ignored: commenter requester is not an allowed approver for this direct PR.'),
      expect.objectContaining({ minimizeTag: 'nsreq:approval-info' })
    );
  });

  test('check_run.success: standalone direct PR with existing current-head manual approval merges without fallback re-handover', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-current-head-approved',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-current-head-approved',
        pull_requests: [{ number: 423 }],
      },
    };

    const pr = {
      number: 423,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct fallback PR',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-current-head-approved', sha: 'sha-direct-current-head-approved' },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.get.mockResolvedValue({
      data: pr,
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-current-head-approved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-current-head-approved\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'Approved' }, { name: 'needs-review' }],
        assignees: [{ login: 'poolA' }],
      },
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'sha-direct-current-head-approved',
          user: { login: 'poolB' },
        },
      ],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    // Even if onApproval would not match anymore, current-head manual approval must win.
    runApprovalHook.mockResolvedValue({} as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await handlers['check_run.completed'][0](ctx);

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 423,
        mergeMethod: 'squash',
      })
    );

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

    expect(
      postOnce.mock.calls.some((call) =>
        String(call[2] ?? '').includes(
          'Manual review required because onApproval did not approve all changed registry files.'
        )
      )
    ).toBe(false);

    expect(
      postOnce.mock.calls.some((call) => String(call[2] ?? '').includes('Routing to an approver for review'))
    ).toBe(false);
  });

  test('check_run.success: standalone direct PR fallback merges after bot-created approval review from allowed pool approver', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-current-head-approved',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-current-head-approved',
        pull_requests: [{ number: 424 }],
      },
    };

    const pr = {
      number: 424,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct fallback PR',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-current-head-approved', sha: 'sha-direct-current-head-approved' },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-current-head-approved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-current-head-approved\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'Approved' }, { name: 'needs-review' }],
        assignees: [{ login: 'poolA' }],
      },
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'sha-direct-current-head-approved',
          user: { login: 'my-registry-bot[bot]' },
          body: 'Approved by @poolB\n\n<!-- nsreq:auto-approval:sha-direct-current-head-approved -->',
        },
      ],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    runApprovalHook.mockResolvedValue({} as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await handlers['check_run.completed'][0](ctx);

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 424,
        mergeMethod: 'squash',
      })
    );

    expect(
      postOnce.mock.calls.some((call) =>
        String(call[2] ?? '').includes(
          'Manual review required because onApproval did not approve all changed registry files.'
        )
      )
    ).toBe(false);

    expect(
      postOnce.mock.calls.some((call) => String(call[2] ?? '').includes('Routing to an approver for review'))
    ).toBe(false);
  });

  test('check_run.success: standalone direct PR unknown approval accepts current-head configured approver review', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-unknown-manual-approved',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-unknown-manual-approved',
        pull_requests: [{ number: 425 }],
      },
    };

    const pr = {
      number: 425,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct fallback PR',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-unknown-manual-approved', sha: 'sha-direct-unknown-manual-approved' },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-current-head-manual-approved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-current-head-manual-approved\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'configuredApprover' }],
      },
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'sha-direct-unknown-manual-approved',
          user: { login: 'configuredApprover' },
        },
      ],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await handlers['check_run.completed'][0](ctx);

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 425,
        mergeMethod: 'squash',
      })
    );
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(
      postOnce.mock.calls.some((call) => String(call[2] ?? '').includes('Routing to an approver for review'))
    ).toBe(false);
  });

  test('check_run.success: standalone direct PR unknown approval accepts bot review marker from hook approver', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-unknown-bot-approved',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-unknown-bot-approved',
        pull_requests: [{ number: 426 }],
      },
    };

    const pr = {
      number: 426,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct fallback PR',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: { ref: 'feature/direct-unknown-bot-approved', sha: 'sha-direct-unknown-bot-approved' },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-current-head-bot-approved.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-current-head-bot-approved\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'Approved' }, { name: 'needs-review' }],
        assignees: [{ login: 'poolB' }],
      },
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'stale-sha',
          user: { login: 'my-registry-bot[bot]' },
          body: 'Approved by @poolB\n\n<!-- nsreq:auto-approval:sha-direct-unknown-bot-approved -->',
        },
      ],
    });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
      approvers: ['poolB'],
    } as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await handlers['check_run.completed'][0](ctx);

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 426,
        mergeMethod: 'squash',
      })
    );
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(
      postOnce.mock.calls.some((call) => String(call[2] ?? '').includes('Routing to an approver for review'))
    ).toBe(false);
  });

  test('check_run.success: standalone direct PR unknown approval with blocking current-head changes requested routes to review', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
        approversPool: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_run.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-unknown-blocked',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-unknown-blocked',
        pull_requests: [{ number: 427 }],
      },
    };

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 427,
            body: 'manual direct pr',
            title: 'Direct fallback PR',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/direct-unknown-blocked', sha: 'sha-direct-unknown-blocked' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-unknown-blocked.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-unknown-blocked\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:45:00Z',
          commit_id: 'sha-direct-unknown-blocked',
          user: { login: 'configuredApprover' },
        },
        {
          id: 2,
          state: 'CHANGES_REQUESTED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'sha-direct-unknown-blocked',
          user: { login: 'configuredApprover' },
        },
      ],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 427 }),
      expect.objectContaining({ number: 427 }),
      'review'
    );
    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 427,
        assignees: ['poolA'],
      })
    );
    expect(postedBodies()).toContain('Routing to an approver for review');
  });

  test('check_run.success: standalone direct PR unknown approval with unauthorized current-head review routes to review', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'resources',
          approvers: ['configuredApprover'],
          approversPool: ['poolB', 'poolA'],
        },
      },
      workflow: {
        labels: {
          global: ['registry-bot'],
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
        approversPool: [],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_run.completed'][0];
    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-direct-unknown-unauthorized',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-direct-unknown-unauthorized',
        pull_requests: [{ number: 428 }],
      },
    };

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 428,
            body: 'manual direct pr',
            title: 'Direct fallback PR',
            user: { login: 'requester' },
            base: { ref: 'main' },
            head: { ref: 'feature/direct-unknown-unauthorized', sha: 'sha-direct-unknown-unauthorized' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-unknown-unauthorized.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-unknown-unauthorized\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-29T09:46:00Z',
          commit_id: 'sha-direct-unknown-unauthorized',
          user: { login: 'intruder' },
          body: 'Looks good',
        },
      ],
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'manual review required',
    } as any);

    await handler(ctx);

    expect(tryMergeIfGreen).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(postedBodies()).toContain('manual review required');
    expect(postedBodies()).toContain('Continuing with the standard review flow.');
    expect(postedBodies()).toContain('Routing to an approver for review');
  });

  test('check_run.success: product standalone direct PR no-match assigns request-type pool user even when requester is same user', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'data/products',
          approversPool: ['C5388932'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['CPA Action'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['D068547'],
        approversPool: ['D064709'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-product-pool-requester-fallback',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-product-pool-requester-fallback',
        pull_requests: [{ number: 430 }],
      },
    };

    const pr = {
      number: 430,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct product PR',
      user: { login: 'C5388932' },
      base: { ref: 'main' },
      head: {
        ref: 'feature/product-pool-requester-fallback',
        sha: 'sha-product-pool-requester-fallback',
      },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValue({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/products/product-pool-requester.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-pool-requester\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValue({
      data: [{ author: { login: 'C5388932' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    runApprovalHook.mockResolvedValue({} as any);

    await handlers['check_run.completed'][0](ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 430 }),
      expect.objectContaining({ number: 430 }),
      ['C5388932'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 430,
        assignees: ['C5388932'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['D064709']),
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 430,
        labels: ['CPA Action'],
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('### ✅ No issues detected');
    expect(posted).toContain('### ➡️ Routing to an approver for review');
    expect(posted).toContain('Manual review required because onApproval did not approve all changed registry files.');
    expectHandoverPosted();
  });

  test('issue_comment.created: product standalone direct PR approval by request-type pool requester creates approval review and merges', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'data/products',
          approversPool: ['C5388932'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['CPA Action'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['D068547'],
        approversPool: ['D064709'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];

    const issue = {
      number: 431,
      title: 'Direct product PR',
      body: 'manual direct pr',
      labels: [{ name: 'CPA Action' }],
      user: { login: 'C5388932' },
      pull_request: {},
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'C5388932' } },
      sender: { type: 'User', login: 'C5388932' },
      withCachedConfig: true,
      config: cfg,
    });

    const pr = {
      number: 431,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct product PR',
      user: { login: 'C5388932' },
      base: { ref: 'main' },
      head: {
        ref: 'feature/product-pool-requester-approval',
        sha: 'sha-product-pool-requester-approval',
      },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [{ name: 'CPA Action' }],
        assignees: [{ login: 'C5388932' }],
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/products/product-pool-requester-approval.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-pool-requester-approval\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValue({
      data: [{ author: { login: 'C5388932' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });

    ctx.octokit.checks.listForRef.mockResolvedValue({
      data: {
        check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
      },
    });

    runApprovalHook.mockResolvedValue({} as any);
    tryMergeIfGreen.mockResolvedValueOnce(true as never);

    await runIssueCommentWithoutJestWorker(handler, ctx);

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        pull_number: 431,
        event: 'APPROVE',
        body: expect.stringContaining('Approved by @C5388932'),
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 431,
        labels: ['Approved'],
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        prNumber: 431,
        mergeMethod: 'squash',
      })
    );
  });

  test('check_run.success: standalone direct PR hook manual approvers override request-type pool assignee', async () => {
    const cfg = {
      requests: {
        product: {
          folderName: 'data/products',
          approversPool: ['C5388932'],
        },
      },
      workflow: {
        labels: {
          approvalRequested: ['CPA Action'],
          approvalSuccessful: ['Approved'],
        },
        approvers: ['D068547'],
        approversPool: ['D064709'],
      },
    };

    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-product-hook-manual-override',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: 'sha-product-hook-manual-override',
        pull_requests: [{ number: 432 }],
      },
    };

    const pr = {
      number: 432,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct product PR',
      user: { login: 'requester' },
      base: { ref: 'main' },
      head: {
        ref: 'feature/product-hook-manual-override',
        sha: 'sha-product-hook-manual-override',
      },
      mergeable: true,
      mergeable_state: 'clean',
    };

    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValue({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/products/product-hook-manual-override.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-hook-manual-override\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValue({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        labels: [],
        assignees: [],
      },
    });

    const addAssignees = jest.fn(async (_params: any): Promise<void> => undefined);
    Object.assign(ctx.octokit.issues, { addAssignees });

    runApprovalHook.mockResolvedValue({
      status: 'unknown',
      message: 'manual review required by hook',
      approvers: ['hookManualReviewer'],
    } as any);

    await handlers['check_run.completed'][0](ctx);

    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    expect((ensureAssigneesOnce as jest.Mock).mock.calls).toContainEqual([
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 432 }),
      expect.objectContaining({ number: 432 }),
      ['hookManualReviewer'],
    ]);

    expect(addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        issue_number: 432,
        assignees: ['hookManualReviewer'],
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['C5388932']),
      })
    );

    expect(addAssignees).not.toHaveBeenCalledWith(
      expect.objectContaining({
        assignees: expect.arrayContaining(['D064709']),
      })
    );

    const posted = postedBodies();

    expect(posted).toContain('manual review required by hook');
    expectHandoverPosted();
  });

  test('check_suite.failure: registry validate annotations are grouped and posted to matching PR', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['check_suite.completed'][0];

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'failure',
      sha: 'sha-validation-failure',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: {
        requests: {
          product: { folderName: 'resources' },
        },
        workflow: {
          labels: { approvalSuccessful: ['Approved'] },
          approvers: [],
        },
      },
    });

    ctx.payload.check_suite.id = 777;
    ctx.payload.check_suite.pull_requests = [{ number: 301 }];

    ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
      data: {
        check_runs: [{ id: 888, conclusion: 'failure', html_url: 'https://example.test/checks/888' }],
      },
    });

    ctx.octokit.checks.listAnnotations.mockResolvedValueOnce({
      data: [
        {
          path: 'resources/product-bad.yaml',
          title: 'registry-validate: schema',
          message:
            "/name required property 'name' [file=resources/product-bad.yaml schema=schemas/product.schema.json]",
          annotation_level: 'failure',
        },
      ],
    });

    ctx.octokit.pulls.get.mockResolvedValueOnce({
      data: {
        html_url: 'https://github.example/o1/r1/pull/301',
      },
    });

    await handler(ctx);

    const posted = postedBodies();

    expect(posted).toContain('## Detected issues');
    expect(posted).toContain('### File: `resources/product-bad.yaml`');
    expect(posted).toContain('required property');
    expect(posted).toContain('<summary>Show as JSON (Robots Friendly)</summary>');
  });

  test('status.success: triggers direct PR auto merge for matching head sha', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers.status[0];

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-status-success',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: {
        requests: {
          product: { folderName: 'resources' },
        },
        workflow: {
          labels: { approvalSuccessful: ['Approved'] },
          approvers: [],
        },
      },
    });

    ctx.name = 'status';
    ctx.payload = {
      state: 'success',
      sha: 'sha-status-success',
      repository: {
        name: 'r1',
        owner: { login: 'o1' },
      },
    };

    ctx.octokit.checks.listForRef.mockResolvedValueOnce({
      data: {
        check_runs: [{ id: 1, name: 'ci', status: 'completed', conclusion: 'success' }],
      },
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 401,
            body: 'manual direct pr',
            title: 'Direct status PR',
            user: { login: 'requester' },
            head: { ref: 'feature/status-pr', sha: 'sha-status-success' },
            base: { ref: 'main' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-status.yaml', status: 'modified' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: product\nname: product-status\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'requester' } }],
    });

    ctx.octokit.pulls.get.mockResolvedValue({
      data: {
        number: 401,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct status PR',
        user: { login: 'requester' },
        head: { ref: 'feature/status-pr', sha: 'sha-status-success' },
        base: { ref: 'main' },
        mergeable: true,
        mergeable_state: 'clean',
      },
    });

    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    await handler(ctx);

    expect(runApprovalHook).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1' },
      expect.objectContaining({
        requestType: 'product',
        namespace: 'product-status',
        resourceName: 'product-status',
        requestAuthorId: 'requester',
      })
    );

    expect(tryMergeIfGreen).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        prNumber: 401,
        mergeMethod: 'squash',
      })
    );
  });

  test('status.non-success: is ignored', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers.status[0];

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-status-ignored',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: {},
    });

    ctx.name = 'status';
    ctx.payload = {
      state: 'failure',
      sha: 'sha-status-ignored',
      repository: {
        name: 'r1',
        owner: { login: 'o1' },
      },
    };

    await handler(ctx);

    expect(ctx.octokit.checks.listForRef).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(runApprovalHook).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });
  test('check_suite.success: promoted hook approver approval does not expose manual approval message in review body', async () => {
    const cfg = {
      requests: {
        systemNamespace: { folderName: 'resources' },
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
      sha: 'sha-hook-approver-promoted',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          {
            number: 164,
            body: 'manual direct pr',
            title: 'Direct',
            user: { login: 'C5388932' },
            base: { ref: 'main' },
            head: { ref: 'feature/hook-approver-promoted', sha: 'sha-hook-approver-promoted' },
          },
        ],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
      data: [{ filename: 'resources/sap.agtj100.yaml', status: 'modified' }],
    });

    ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
      data: [{ author: { login: 'C5388932' } }],
    });

    ctx.octokit.repos.getContent.mockResolvedValueOnce({
      data: {
        content: Buffer.from('type: system\nname: sap.agtj100\n', 'utf8').toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValueOnce({
      status: 'unknown',
      message: 'Manual approval required for this agent onboarding namespace request.',
      approvers: ['C5388932'],
    } as any);

    await handler(ctx);

    const reviewCall = ctx.octokit.pulls.createReview.mock.calls[0]?.[0] as { body?: string };
    const reviewBody = String(reviewCall?.body ?? '');

    expect(ctx.octokit.pulls.createReview).toHaveBeenCalled();
    expect(reviewBody).toContain('nsreq:auto-approval:');
    expect(reviewBody).not.toContain('Manual approval required');
    expect(reviewBody).not.toContain('agent onboarding namespace request');
  });
});

test('issues.opened: without jest worker id skips freeform issues outside test runtime', async () => {
  const prevWorkerId = process.env.JEST_WORKER_ID;
  delete process.env.JEST_WORKER_ID;

  try {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issues.opened'][0];
    const ctx = mkIssuesContext({
      action: 'opened',
      issue: { number: 70, title: 'Freeform', body: 'plain text only', labels: [], user: { login: 'user' } },
      withCachedConfig: true,
    });

    await handler(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  } finally {
    if (prevWorkerId !== undefined) process.env.JEST_WORKER_ID = prevWorkerId;
  }
});

test('issues.closed: without jest worker id skips freeform issues outside test runtime', async () => {
  const prevWorkerId = process.env.JEST_WORKER_ID;
  delete process.env.JEST_WORKER_ID;

  try {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issues.closed'][0];
    const issue = {
      number: 71,
      title: 'Freeform',
      body: 'plain text only',
      labels: [],
      state: 'closed',
      user: { login: 'u' },
    };
    const ctx = mkBaseContext({
      issue,
      withCachedConfig: true,
    });
    ctx.name = 'issues.closed';
    ctx.payload = { action: 'closed', issue };

    await handler(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
  } finally {
    if (prevWorkerId !== undefined) process.env.JEST_WORKER_ID = prevWorkerId;
  }
});

test('issues.labeled: without jest worker id skips freeform issues outside test runtime', async () => {
  const prevWorkerId = process.env.JEST_WORKER_ID;
  delete process.env.JEST_WORKER_ID;

  try {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issues.labeled'][0];
    const issue = {
      number: 72,
      title: 'Freeform',
      body: 'plain text only',
      labels: [],
      state: 'open',
      user: { login: 'u' },
    };
    const ctx = mkBaseContext({ issue, withCachedConfig: true });
    ctx.name = 'issues.labeled';
    ctx.payload = { action: 'labeled', issue, sender: { type: 'User', login: 'alice' }, label: { name: 'Approved' } };

    await handler(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  } finally {
    if (prevWorkerId !== undefined) process.env.JEST_WORKER_ID = prevWorkerId;
  }
});

test('issue_comment: without jest worker id skips freeform issues outside test runtime', async () => {
  const prevWorkerId = process.env.JEST_WORKER_ID;
  delete process.env.JEST_WORKER_ID;

  try {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['issue_comment.created'][0];
    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue: { number: 73, title: 'Freeform', body: 'plain text only', labels: [], user: { login: 'u' } },
      comment: { body: 'Approved', user: { login: 'alice' } },
      withCachedConfig: true,
    });

    await handler(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  } finally {
    if (prevWorkerId !== undefined) process.env.JEST_WORKER_ID = prevWorkerId;
  }
});

test('status: without jest worker id skips non-form linked issues outside test runtime', async () => {
  const prevWorkerId = process.env.JEST_WORKER_ID;
  delete process.env.JEST_WORKER_ID;

  try {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const handler = handlers['status'][0];
    const ctx = mkStatusContext({
      state: 'success',
      sha: 'sha-freeform-status',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [{ number: 74, body: 'source: #1', head: { ref: 'feature/freeform', sha: 'sha-freeform-status' } }],
      })
      .mockResolvedValueOnce({ data: [] });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: { number: 1, title: 'Freeform', body: 'plain text only', labels: [], state: 'open', user: { login: 'u' } },
    });

    await handler(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
  } finally {
    if (prevWorkerId !== undefined) process.env.JEST_WORKER_ID = prevWorkerId;
  }
});

test('push: default branch push updates approved green registry PR branches', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 202,
        state: 'open',
        body: 'manual direct pr',
        title: 'Ignored direct',
        head: { ref: 'feature/ignored-default-push', sha: '' },
        base: { ref: 'main', sha: 'base-sha' },
      },
      {
        number: 201,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/approved-green', sha: 'sha-approved-green' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-approved.yaml', status: 'modified' }],
  });

  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 201, labels: [{ name: 'Approved' }] },
  });

  ctx.octokit.pulls.listReviews.mockResolvedValue({
    data: [],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 201,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/approved-green', sha: 'sha-approved-green' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'behind',
    },
  });

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o1',
      repo: 'r1',
      pull_number: 201,
      expected_head_sha: 'sha-approved-green',
    })
  );

  setTimeoutSpy.mockRestore();
});

test('push: default branch push without repo info returns early', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ withCachedConfig: true });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  await handler(ctx);

  expect(loadStaticConfig).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
});

test('push: default branch delayed retry logs when approved branch update retry crashes', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-delayed-retry', withCachedConfig: true, config: cfg });
  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-delayed-retry', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list
    .mockResolvedValueOnce({ data: [] })
    .mockResolvedValueOnce({ data: [] })
    .mockRejectedValueOnce(new Error('retry crashed'));

  await handler(ctx);
  await Promise.resolve();
  await Promise.resolve();

  expect(
    ctx.log.warn.mock.calls.some((call: any[]) =>
      call.some((arg: unknown) =>
        String(arg ?? '').includes('failed to run delayed approved pull request branch update retry')
      )
    )
  ).toBe(true);

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation skips missing head sha', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 206,
          title: 'Direct',
          body: 'manual direct pr',
          head: { ref: 'feature/no-sha' },
          base: { ref: 'main' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  await handler(ctx);

  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.get).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation skips different base branch', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  extractHashFromPrBody.mockReturnValueOnce('');
  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 207,
          title: 'Direct',
          body: 'manual direct pr',
          head: { ref: 'feature/other-base', sha: 'sha-other-base' },
          base: { ref: 'release', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  await handler(ctx);

  expect(ctx.octokit.pulls.listFiles).not.toHaveBeenCalled();
  expect(runApprovalHook).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation uses fallback tree diff and skips closed refreshed PRs', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 208,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/tree-diff', sha: 'sha-tree-diff' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({ data: [] });
  ctx.octokit.repos.getBranch = jest.fn(async () => ({ data: { commit: { sha: 'base-head-sha' } } }));
  const getTreeMock: any = jest.fn();
  getTreeMock.mockResolvedValueOnce({
    data: { tree: [{ path: 'resources/product-tree.yaml', type: 'blob', sha: 'base-file-sha' }] },
  });
  getTreeMock.mockResolvedValueOnce({
    data: { tree: [{ path: 'resources/product-tree.yaml', type: 'blob', sha: 'head-file-sha' }] },
  });
  ctx.octokit.git.getTree = getTreeMock;

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: {
      number: 208,
      state: 'closed',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/tree-diff', sha: 'sha-tree-diff' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  await handler(ctx);

  expect(ctx.octokit.git.getTree).toHaveBeenCalledTimes(2);
  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation requests update when current base comparison shows PR is stale', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 210,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/current-base-stale', sha: 'sha-current-base-stale' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
    data: [{ filename: 'resources/product-current-base-stale.yaml', status: 'modified' }],
  });

  ctx.octokit.pulls.get.mockResolvedValueOnce({
    data: {
      number: 210,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/current-base-stale', sha: 'sha-current-base-stale' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.repos.getBranch = jest.fn(async () => ({ data: { commit: { sha: 'base-head-sha' } } }));
  ctx.octokit.repos.compareCommitsWithBasehead = jest.fn(async () => ({
    data: { status: 'ahead', ahead_by: 1 },
  }));

  await handler(ctx);

  expect(ctx.octokit.repos.compareCommitsWithBasehead).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', basehead: 'sha-current-base-stale...base-head-sha' })
  );
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 210, expected_head_sha: 'sha-current-base-stale' })
  );

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation skips when fallback tree diff cannot read current base', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 211,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/no-tree-diff', sha: 'sha-no-tree-diff' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({ data: [] });
  ctx.octokit.repos.getBranch = jest.fn(async () => {
    throw httpErr(500);
  });

  await handler(ctx);

  expect(ctx.octokit.repos.getBranch).toHaveBeenCalledTimes(1);
  expect(ctx.octokit.git.getTree).toBeUndefined();
  expect(runApprovalHook).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation does not run approval when approved head checks are not green', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 209,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/not-green', sha: 'sha-not-green' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-not-green.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-not-green\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'not-green-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 209,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/not-green', sha: 'sha-not-green' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved but wait for ci' } as any);
  ctx.octokit.checks.listForRef.mockResolvedValueOnce({
    data: { check_runs: [{ id: 1, name: 'ci', status: 'completed', conclusion: 'failure' }] },
  });

  await handler(ctx);

  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation waits when latest approved head check runs are not completed', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2091,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/not-completed', sha: 'sha-not-completed' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-not-completed.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-not-completed\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'waiting-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2091,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/not-completed', sha: 'sha-not-completed' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.checks.listForRef.mockResolvedValueOnce({
    data: { check_runs: [{ id: 1, name: 'ci', status: 'in_progress', conclusion: null }] },
  });

  await handler(ctx);

  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation waits when completed head check runs have no success conclusion', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2092,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/no-success', sha: 'sha-no-success' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-no-success.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-no-success\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'neutral-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2092,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/no-success', sha: 'sha-no-success' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.checks.listForRef.mockResolvedValueOnce({
    data: { check_runs: [{ id: 1, name: 'lint', status: 'completed', conclusion: 'neutral' }] },
  });

  await handler(ctx);

  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation falls back to combined status when head check runs cannot be fetched', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2093,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/combined-status', sha: 'sha-combined-status' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-combined-status.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-combined-status\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'combined-status-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2093,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/combined-status', sha: 'sha-combined-status' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.checks.listForRef.mockRejectedValueOnce(httpErr(500));
  ctx.octokit.repos.getCombinedStatusForRef = jest.fn(async () => ({ data: { state: 'pending' } }));

  await handler(ctx);

  expect(ctx.octokit.repos.getCombinedStatusForRef).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', ref: 'sha-combined-status' })
  );
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) =>
      call.some((arg: unknown) => String(arg ?? '').includes('head-green:check-runs-fetch-failed'))
    )
  ).toBe(true);
  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation gives up when both check runs and combined status cannot be fetched', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2094,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/combined-status-failed', sha: 'sha-combined-status-failed' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-combined-status-failed.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-combined-status-failed\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
    data: [{ committer: { login: 'combined-status-failed-user' } }],
  });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2094,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/combined-status-failed', sha: 'sha-combined-status-failed' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.checks.listForRef.mockRejectedValueOnce(httpErr(500));
  ctx.octokit.repos.getCombinedStatusForRef = jest.fn(async () => {
    throw httpErr(502);
  });

  await handler(ctx);

  expect(ctx.octokit.repos.getCombinedStatusForRef).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', ref: 'sha-combined-status-failed' })
  );
  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation stops when the current head sha is missing', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2095,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/missing-head-sha', sha: 'sha-missing-head-sha' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-missing-head-sha.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-missing-head-sha\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'missing-head-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2095,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/missing-head-sha', sha: '   ' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  await handler(ctx);

  expect(ctx.octokit.checks.listForRef).not.toHaveBeenCalled();
  expect(ctx.octokit.repos.getCombinedStatusForRef).toBeUndefined();
  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct PR reevaluation caps head check-run pagination at twenty pages', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r1', withCachedConfig: true, config: cfg });

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2096,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/check-runs-page-cap', sha: 'sha-check-runs-page-cap' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-check-runs-page-cap.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-check-runs-page-cap\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({ data: [{ committer: { login: 'page-cap-user' } }] });
  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2096,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/check-runs-page-cap', sha: 'sha-check-runs-page-cap' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  ctx.octokit.checks.listForRef.mockImplementation(async ({ page }: { page?: number }) => ({
    data: {
      check_runs: Array.from({ length: 100 }, (_, index) => ({
        id: ((page ?? 1) - 1) * 100 + index + 1,
        name: `ci-${(page ?? 1).toString()}-${index.toString()}`,
        status: 'completed',
        conclusion: 'neutral',
      })),
    },
  }));

  await handler(ctx);

  expect(ctx.octokit.checks.listForRef).toHaveBeenCalledTimes(20);
  expect(ctx.octokit.checks.listForRef).toHaveBeenLastCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', ref: 'sha-check-runs-page-cap', page: 20 })
  );
  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: stale direct registry PR retries updateBranch without expected head before approval reevaluation', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });
  extractHashFromPrBody.mockReturnValueOnce('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 204,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/stale-direct', sha: 'sha-stale-direct' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
    data: [{ filename: 'resources/product-stale.yaml', status: 'modified' }],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 204,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/stale-direct', sha: 'sha-stale-direct' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'behind',
    },
  });

  ctx.octokit.pulls.updateBranch
    .mockRejectedValueOnce(Object.assign(new Error('expected_head_sha mismatch'), { status: 422 }))
    .mockResolvedValueOnce({});

  await handler(ctx);

  expect(runApprovalHook).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(2);
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({
      owner: 'o1',
      repo: 'r1',
      pull_number: 204,
      expected_head_sha: 'sha-stale-direct',
    })
  );
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({
      owner: 'o1',
      repo: 'r1',
      pull_number: 204,
    })
  );
  expect(ctx.octokit.pulls.updateBranch.mock.calls[1]?.[0]).not.toHaveProperty('expected_head_sha');

  setTimeoutSpy.mockRestore();
});

test('push: direct registry PR runs approval after reevaluation when branch is already current', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });
  extractHashFromPrBody.mockReturnValue('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 205,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/direct-green', sha: 'sha-direct-green' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-direct-green.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValue({
    data: {
      content: Buffer.from('type: product\nname: product-direct-green\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValue({
    data: [{ committer: { login: 'direct-green-user' } }],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 205,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/direct-green', sha: 'sha-direct-green' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });

  runApprovalHook.mockResolvedValue({ status: 'approved', comment: 'approved after push reevaluation' } as any);

  await handler(ctx);

  expect(runApprovalHook).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1' },
    expect.objectContaining({
      requestType: 'product',
      resourceName: 'product-direct-green',
      namespace: 'product-direct-green',
    })
  );
  expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 205, event: 'APPROVE' })
  );
  expect(tryMergeIfGreen).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 205, mergeMethod: 'squash' })
  );
  expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: direct registry PR polls mergeability repeatedly before merging on the current head', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });
  extractHashFromPrBody.mockReturnValue('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2054,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/direct-mergeability-poll', sha: 'sha-direct-mergeability-poll' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-direct-mergeability-poll.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValue({
    data: {
      content: Buffer.from('type: product\nname: product-direct-mergeability-poll\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValue({
    data: [{ committer: { login: 'direct-mergeability-poll-user' } }],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2054,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/direct-mergeability-poll', sha: 'sha-direct-mergeability-poll' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: null,
      mergeable_state: 'checking',
    },
  });

  runApprovalHook.mockResolvedValue({ status: 'approved', comment: 'approved after mergeability polling' } as any);

  await handler(ctx);

  expect(ctx.octokit.pulls.get.mock.calls.length).toBeGreaterThanOrEqual(6);
  expect(tryMergeIfGreen).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 2054, mergeMethod: 'squash' })
  );

  setTimeoutSpy.mockRestore();
});

test('push: direct registry PR creates an approval review when prior review lookup fails', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });
  extractHashFromPrBody.mockReturnValue('');

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 2051,
          state: 'open',
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/direct-green-review-fetch-failed', sha: 'sha-direct-green-review-fetch-failed' },
          base: { ref: 'main', sha: 'base-sha' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-direct-green-review-fetch-failed.yaml', status: 'modified' }],
  });

  ctx.octokit.repos.getContent.mockResolvedValue({
    data: {
      content: Buffer.from('type: product\nname: product-direct-green-review-fetch-failed\n', 'utf8').toString(
        'base64'
      ),
      encoding: 'base64',
    },
  });

  ctx.octokit.pulls.listCommits.mockResolvedValue({
    data: [{ committer: { login: 'direct-green-review-fetch-failed-user' } }],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2051,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/direct-green-review-fetch-failed', sha: 'sha-direct-green-review-fetch-failed' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });
  ctx.octokit.pulls.listReviews.mockRejectedValueOnce(httpErr(500));

  runApprovalHook.mockResolvedValue({ status: 'approved', comment: 'approved after review lookup failure' } as any);

  await handler(ctx);

  expect(runApprovalHook).toHaveBeenCalled();
  expect(ctx.octokit.pulls.createReview).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 2051, event: 'APPROVE' })
  );
  expect(tryMergeIfGreen).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 2051, mergeMethod: 'squash' })
  );

  setTimeoutSpy.mockRestore();
});

test('push: approved review remains eligible for branch update after later comment-only review', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 202,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/review-commented', sha: 'sha-review-commented' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-commented.yaml', status: 'modified' }],
  });

  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 202, labels: [] },
  });

  ctx.octokit.pulls.listReviews.mockResolvedValue({
    data: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-20T10:00:00Z',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @reviewer\n\n<!-- nsreq:auto-approval:sha-review-commented -->',
      },
      {
        id: 2,
        state: 'COMMENTED',
        submitted_at: '2026-04-20T10:05:00Z',
        user: { login: 'reviewer' },
      },
    ],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 202,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/review-commented', sha: 'sha-review-commented' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'behind',
    },
  });

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o1',
      repo: 'r1',
      pull_number: 202,
      expected_head_sha: 'sha-review-commented',
    })
  );

  setTimeoutSpy.mockRestore();
});

test('push: paginated review history uses later review id as tie-breaker for same timestamp and blocks branch update', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 2021,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/review-pagination', sha: 'sha-review-pagination' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-review-pagination.yaml', status: 'modified' }],
  });

  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 2021, labels: [{ name: 'Approved' }] },
  });

  ctx.octokit.pulls.listReviews
    .mockResolvedValueOnce({
      data: [
        {
          id: 1,
          state: 'APPROVED',
          submitted_at: '2026-04-20T10:00:00Z',
          user: { login: 'reviewer' },
        },
        {
          id: 2,
          state: 'CHANGES_REQUESTED',
          submitted_at: '2026-04-20T10:00:00Z',
          user: { login: 'reviewer' },
        },
        ...Array.from({ length: 98 }, (_, index) => ({
          id: 10 + index,
          state: 'COMMENTED',
          submitted_at: '2026-04-20T10:01:00Z',
          user: { login: `reviewer-${index}` },
        })),
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  await handler(ctx);

  expect(ctx.octokit.pulls.listReviews).toHaveBeenCalledTimes(2);
  expect(ctx.octokit.pulls.listReviews).toHaveBeenNthCalledWith(2, expect.objectContaining({ page: 2 }));
  expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: review fetch and approved-label fallback fetch failures skip branch update', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 2022,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/review-fetch-failed', sha: 'sha-review-fetch-failed' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-review-fetch-failed.yaml', status: 'modified' }],
  });

  ctx.octokit.pulls.listReviews.mockRejectedValueOnce(httpErr(500));
  ctx.octokit.issues.get.mockRejectedValueOnce(httpErr(500));

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('push: branch update cooldown expires and allows a later retry', async () => {
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

  const handler = handlers['push'][0];
  const pr = {
    number: 20225,
    state: 'open',
    body: 'manual direct pr',
    title: 'Direct',
    head: { ref: 'feature/branch-cooldown-expiry', sha: 'sha-branch-cooldown-expiry' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const buildCtx = () => {
    const ctx = mkBaseContext({
      owner: 'o1',
      repo: 'r1',
      withCachedConfig: true,
      config: cfg,
    });
    ctx.name = 'push';
    ctx.payload = {
      ref: 'refs/heads/main',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
    };
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-branch-cooldown-expiry.yaml', status: 'modified' }],
    });
    ctx.octokit.issues.get.mockResolvedValue({ data: { number: 20225, labels: [{ name: 'Approved' }] } });
    ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
    return ctx;
  };

  const ctx1 = buildCtx();
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx1.octokit.pulls.updateBranch.mockResolvedValueOnce({});

  await handler(ctx1);

  expect(ctx1.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);

  currentTestNow += 16 * 1000;

  const ctx2 = buildCtx();
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx2.octokit.pulls.updateBranch.mockResolvedValueOnce({});

  await handler(ctx2);

  expect(ctx2.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);
  expect(
    ctx2.log.info.mock.calls.some(
      (call: any[]) => String(call[1] ?? '') === 'pull-request branch update skipped: cooldown active'
    )
  ).toBe(false);
});

test('push: plain approved review without bot marker is enough for branch update', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 2023,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/plain-approved-review', sha: 'sha-plain-approved-review' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-plain-approved-review.yaml', status: 'modified' }],
  });

  ctx.octokit.pulls.listReviews.mockResolvedValue({
    data: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-20T10:00:00Z',
        user: { login: 'reviewer' },
        body: 'Looks good to me',
      },
    ],
  });

  ctx.octokit.pulls.get.mockResolvedValue({
    data: {
      number: 2023,
      state: 'open',
      body: 'manual direct pr',
      title: 'Direct',
      head: { ref: 'feature/plain-approved-review', sha: 'sha-plain-approved-review' },
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'behind',
    },
  });

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o1',
      repo: 'r1',
      pull_number: 2023,
      expected_head_sha: 'sha-plain-approved-review',
    })
  );

  setTimeoutSpy.mockRestore();
});

test('push: changes requested review blocks approved-label based branch update', async () => {
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

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({
    owner: 'o1',
    repo: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  loadStaticConfig.mockResolvedValueOnce({
    config: cfg,
    source: 'mock',
    hooks: null,
    hooksSource: null,
  });

  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({
    data: [
      {
        number: 203,
        state: 'open',
        body: 'manual direct pr',
        title: 'Direct',
        head: { ref: 'feature/changes-requested', sha: 'sha-changes-requested' },
        base: { ref: 'main', sha: 'base-sha' },
      },
    ],
  });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
    data: [{ filename: 'resources/product-changes-requested.yaml', status: 'modified' }],
  });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: { number: 203, labels: [{ name: 'Approved' }] },
  });

  ctx.octokit.pulls.listReviews.mockResolvedValue({
    data: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-20T10:00:00Z',
        user: { login: 'reviewer' },
      },
      {
        id: 2,
        state: 'CHANGES_REQUESTED',
        submitted_at: '2026-04-20T10:05:00Z',
        user: { login: 'reviewer' },
      },
    ],
  });

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).not.toHaveBeenCalled();

  setTimeoutSpy.mockRestore();
});

test('issues.opened: partner namespace maps request type and matches normalized request config keys', async () => {
  const cfg = {
    workflow: {
      approvers: ['fallback-approver'],
      approversPool: ['fallback-pool'],
      labels: {},
    },
    requests: {
      subContextNamespace: {
        approversPool: ['zoe'],
      },
    },
  };

  loadTemplate.mockResolvedValueOnce({
    title: 'Partner Request',
    name: 'Partner Request',
    body: [],
    labels: [],
    _meta: { requestType: 'partnerNamespace', root: 'resources', schema: 'schema.json', path: 'p' },
  });
  parseForm.mockReturnValueOnce({ identifier: 'partner-resource', requestType: 'subcontext' });
  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'partner-resource',
    nsType: 'partner',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 74,
      title: 'Partner',
      body: '### Request Type\nsub_context',
      labels: [],
      user: { login: 'author' },
    },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect((ensureAssigneesOnce as jest.Mock).mock.calls.at(-1)?.[3]).toEqual(['zoe']);
});

test('issues.opened: request config fallback matches normalized request keys when direct lookup misses', async () => {
  const cfg = {
    workflow: {
      approvers: ['fallback-approver'],
      approversPool: ['fallback-pool'],
      labels: {},
    },
    requests: {
      subcontextnamespace: {
        approversPool: ['normalized-pool-user'],
      },
    },
  };

  loadTemplate.mockResolvedValueOnce({
    title: 'Partner Request',
    name: 'Partner Request',
    body: [],
    labels: [],
    _meta: { requestType: 'partnerNamespace', root: 'resources', schema: 'schema.json', path: 'p' },
  });
  parseForm.mockReturnValueOnce({ identifier: 'partner-resource-normalized', requestType: 'subcontext' });
  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'partner-resource-normalized',
    nsType: 'partner',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 741,
      title: 'Partner',
      body: '### Request Type\nsub_context',
      labels: [],
      user: { login: 'author' },
    },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect((ensureAssigneesOnce as jest.Mock).mock.calls.at(-1)?.[3]).toEqual(['normalized-pool-user']);
});

test('issues.opened: non-object request config falls back to sorted workflow approvers pool', async () => {
  const cfg = {
    workflow: {
      approvers: ['fallback-approver'],
      approversPool: ['zoe', 'amy'],
      labels: {},
    },
    requests: 'invalid',
  };

  loadTemplate.mockResolvedValueOnce({
    title: 'Request',
    name: 'Request',
    body: [],
    labels: [],
    _meta: { requestType: 'product', root: 'resources', schema: 'schema.json', path: 'p' },
  });
  parseForm.mockReturnValueOnce({ 'product-id': 'fallback-resource' });
  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'fallback-resource',
    nsType: 'generic',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 2,
      title: 'Fallback',
      body: '### Product ID\nfallback-resource',
      labels: [],
      user: { login: 'author' },
    },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect((ensureAssigneesOnce as jest.Mock).mock.calls.at(-1)?.[3]).toEqual(['zoe']);
});

test('issues.opened: routing lock marker update failure is tolerated', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 88,
      title: 'Request',
      body: 'Body',
      labels: [{ name: 'route-1' }],
      user: { login: 'alice' },
      state: 'open',
    },
    withCachedConfig: true,
  });

  ctx.octokit.issues.update.mockRejectedValueOnce(new Error('cannot persist routing lock')).mockResolvedValue({});

  await handler(ctx);

  expect(ctx.octokit.issues.update).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o',
      repo: 'r',
      issue_number: 88,
      body: expect.stringContaining('nsreq:routing-lock'),
    })
  );
});

test('issues.opened: routing label lock falls back to payload labels when label refresh fails', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 89,
      title: 'Request',
      body: 'Body\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
      labels: [{ name: 'route-1' }, { name: 'route-2' }],
      user: { login: 'alice' },
      state: 'open',
    },
    withCachedConfig: true,
  });

  ctx.octokit.issues.get.mockRejectedValueOnce(new Error('label refresh failed'));

  await handler(ctx);

  expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 89, name: 'route-2' })
  );
});

test('issues.opened: request config without own approver arrays falls back to workflow approvers', async () => {
  const cfg = {
    workflow: {
      approvers: ['fallback-approver'],
      labels: {},
    },
    requests: {
      product: {},
    },
  };

  loadTemplate.mockResolvedValueOnce({
    title: 'Product Request',
    name: 'Product Request',
    body: [],
    labels: [],
    _meta: { requestType: 'product', root: 'resources', schema: 'schema.json', path: 'p' },
  });
  parseForm.mockReturnValueOnce({ 'product-id': 'product-fallback' });
  validateRequestIssue.mockResolvedValueOnce({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'product-fallback',
    nsType: 'product',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];
  const ctx = mkIssuesContext({
    action: 'opened',
    issue: {
      number: 75,
      title: 'Product',
      body: '### Product ID\nproduct-fallback',
      labels: [],
      user: { login: 'author' },
    },
    withCachedConfig: true,
    config: cfg,
  });

  await handler(ctx);

  expect((ensureAssigneesOnce as jest.Mock).mock.calls.at(-1)?.[3]).toEqual(['fallback-approver']);
});

test('issue_comment: approval tolerates approved-label refresh failures after approval state is applied', async () => {
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
    number: 76,
    title: 'Request',
    body: '### Namespace\nsap.ok',
    labels: [{ name: 'needs-review' }],
    state: 'open',
    user: { login: 'author' },
  };
  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
    config: cfg,
  });

  ctx.octokit.issues.get
    .mockResolvedValueOnce({ data: { ...issue, labels: [{ name: 'needs-review' }] } })
    .mockRejectedValueOnce(httpErr(500))
    .mockRejectedValueOnce(httpErr(500));

  await handler(ctx);

  expect(createRequestPr).toHaveBeenCalled();
  expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 76, labels: ['Approved'] })
  );
});

test('issue_comment: already-existing resource retry failure reports stale branch contains resource name', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issue_comment.created'][0];
  const issue = inReview({
    number: 77,
    title: 'Request',
    body: '### Product ID\nproduct-stale',
    labels: [],
    state: 'open',
    user: { login: 'author' },
  });
  const ctx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'Approved', user: { login: 'alice' } },
    withCachedConfig: true,
    config: {
      workflow: { approvers: [], labels: {} },
    },
  });

  parseForm.mockReturnValueOnce({ 'product-id': 'product-stale' });

  createRequestPr
    .mockRejectedValueOnce(new Error("Resource 'product-stale' already exists at resources/product-stale.yaml"))
    .mockRejectedValueOnce(new Error("Resource 'product-stale' already exists at resources/product-stale.yaml"));

  ctx.octokit.repos.getContent.mockRejectedValueOnce(httpErr(404)).mockRejectedValueOnce(httpErr(404));

  await handler(ctx);

  expect(postedBodies()).toContain("a stale request branch already contains 'product-stale'");
});

test('check_suite.success: direct PR request author pagination falls back to last known committer on later page failure', async () => {
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
    sha: 'sha-paginated-author',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 78,
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/paginated-author', sha: 'sha-paginated-author' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
    data: [{ filename: 'resources/product-paginated.yaml', status: 'modified' }],
  });
  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from('type: product\nname: product-paginated\n', 'utf8').toString('base64'),
      encoding: 'base64',
    },
  });
  ctx.octokit.pulls.listCommits
    .mockResolvedValueOnce({
      data: Array.from({ length: 100 }, (_, index) => ({
        committer: { login: index === 99 ? 'page-one-user' : `user-${index}` },
      })),
    })
    .mockRejectedValueOnce(new Error('page 2 failed'));

  runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

  await handler(ctx);

  expect(runApprovalHook).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1' },
    expect.objectContaining({ requestAuthorId: 'page-one-user' })
  );
  expect(tryMergeIfGreen).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 78, mergeMethod: 'squash' })
  );
});

test('check_suite.success: direct PR serializes complex yaml form values before onApproval', async () => {
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
    sha: 'sha-complex-form-data',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
    config: cfg,
  });

  ctx.octokit.pulls.list
    .mockResolvedValueOnce({
      data: [
        {
          number: 79,
          body: 'manual direct pr',
          title: 'Direct',
          head: { ref: 'feature/complex', sha: 'sha-complex-form-data' },
        },
      ],
    })
    .mockResolvedValueOnce({ data: [] });

  ctx.octokit.pulls.listFiles.mockResolvedValueOnce({
    data: [{ filename: 'resources/product-complex.yaml', status: 'modified' }],
  });
  ctx.octokit.repos.getContent.mockResolvedValueOnce({
    data: {
      content: Buffer.from(
        'type: product\nname: product-complex\nmaintainers:\n  - name: Alice\n    github: alice\n',
        'utf8'
      ).toString('base64'),
      encoding: 'base64',
    },
  });
  ctx.octokit.pulls.listCommits.mockResolvedValueOnce({
    data: [{ committer: { login: 'complex-author' } }],
  });

  runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

  await handler(ctx);

  expect(runApprovalHook).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1' },
    expect.objectContaining({
      formData: expect.objectContaining({
        maintainers: expect.stringContaining('github: alice'),
      }),
    })
  );
});

test('check_suite.completed failure exits quietly when suite runs cannot be listed', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];
  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-suite-list-failure',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 880;
  ctx.payload.check_suite.pull_requests = [{ number: 80 }];
  ctx.octokit.checks.listForSuite.mockRejectedValueOnce(new Error('suite lookup failed'));

  await handler(ctx);

  expect(postOnce).not.toHaveBeenCalled();
});

test('check_suite.completed failure skips a run when its annotations cannot be listed', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['check_suite.completed'][0];
  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'failure',
    sha: 'sha-annotation-list-failure',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload.check_suite.id = 881;
  ctx.payload.check_suite.pull_requests = [{ number: 81 }];
  ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
    data: { check_runs: [{ id: 9009, html_url: 'https://example/check/9009' }] },
  });
  ctx.octokit.checks.listAnnotations.mockRejectedValueOnce(new Error('annotations failed'));

  await handler(ctx);

  expect(postOnce).not.toHaveBeenCalled();
});

test('issues.closed: template load failure is ignored for non-request issues', async () => {
  loadTemplate.mockRejectedValueOnce(new Error('no template'));

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.closed'][0];
  const issue = {
    number: 82,
    title: 'Freeform',
    body: '### Namespace\nsap.ignore',
    labels: [],
    state: 'closed',
    user: { login: 'author' },
  };
  const ctx = mkBaseContext({ issue, withCachedConfig: true });
  ctx.name = 'issues.closed';
  ctx.payload = { action: 'closed', issue };

  await handler(ctx);

  expect(postOnce).not.toHaveBeenCalled();
});

test('issues.labeled: closed approved issue removes rejected and progress labels after refresh', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.labeled'][0];
  const issue = {
    number: 83,
    title: 'Request',
    body: '### Product ID\nabc',
    labels: [],
    state: 'closed',
    user: { login: 'author' },
  };
  const ctx = mkBaseContext({
    issue,
    withCachedConfig: true,
    config: {
      workflow: {
        approvers: [],
        labels: { approvalSuccessful: ['Approved'] },
      },
    },
  });
  ctx.name = 'issues.labeled';
  ctx.payload = { action: 'labeled', issue, sender: { type: 'User', login: 'bob' }, label: { name: 'other' } };

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: {
      ...issue,
      labels: ['Approved', 'Rejected', 'Requester Action', 'Review Pending'],
    },
  });

  await handler(ctx);

  const removed = ctx.octokit.issues.removeLabel.mock.calls.map((call: any[]) => call[0]?.name).sort();
  expect(removed).toEqual(expect.arrayContaining(['Rejected', 'Requester Action', 'Review Pending']));
  expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
});

test('issues.labeled: closed non-approved issue adds rejected and removes approved after refresh', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.labeled'][0];
  const issue = {
    number: 84,
    title: 'Request',
    body: '### Product ID\nabc',
    labels: [],
    state: 'closed',
    user: { login: 'author' },
  };
  const ctx = mkBaseContext({
    issue,
    withCachedConfig: true,
    config: {
      workflow: {
        approvers: [],
        labels: { approvalSuccessful: ['Approved'] },
      },
    },
  });
  ctx.name = 'issues.labeled';
  ctx.payload = { action: 'labeled', issue, sender: { type: 'User', login: 'bob' }, label: { name: 'other' } };

  ctx.octokit.issues.get
    .mockResolvedValueOnce({
      data: {
        ...issue,
        labels: ['Requester Action'],
      },
    })
    .mockResolvedValueOnce({
      data: {
        ...issue,
        labels: ['Rejected', 'Approved', 'Requester Action'],
      },
    });

  await handler(ctx);

  expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 84, labels: ['Rejected'] })
  );
  const removed = ctx.octokit.issues.removeLabel.mock.calls.map((call: any[]) => call[0]?.name);
  expect(removed).toEqual(expect.arrayContaining(['Approved', 'Requester Action']));
});

test('issue_comment: parent-owner approval posts validation fallback errors and resets author state', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const target = 'sap.css.bar.foo';
  const issue = {
    number: 85,
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

  const openCtx = mkIssuesContext({ issue, action: 'opened' });
  (openCtx.octokit.repos.getContent as jest.Mock).mockImplementation(async ({ path }: any) => {
    if (path === 'data/vendors/sap.yaml') {
      return {
        data: {
          content: Buffer.from('type: vendor\nname: sap\n', 'utf8').toString('base64'),
          encoding: 'base64',
        },
      };
    }
    if (path === 'data/namespaces/sap.css.yaml') {
      return {
        data: { content: Buffer.from('contacts:\n  - "@barOwner"\n', 'utf8').toString('base64'), encoding: 'base64' },
      };
    }
    if (path === 'data/namespaces/sap.css.bar.yaml') {
      return {
        data: { content: Buffer.from('contacts:\n  - "@barOwner"\n', 'utf8').toString('base64'), encoding: 'base64' },
      };
    }
    throw Object.assign(new Error('Not Found'), { status: 404 });
  });
  await handlers['issues.opened'][0](openCtx);

  (postOnce as jest.Mock).mockClear();
  (setStateLabel as jest.Mock).mockClear();
  validateRequestIssue.mockResolvedValueOnce({
    errors: ['validation fallback error'],
    errorsGrouped: {},
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: target,
    nsType: 'subContextNamespace',
    template: tpl,
    validationIssues: [{ path: 'contacts', message: 'missing owner contact' }],
  });

  const commentCtx = mkCommentContext({
    event: 'issue_comment.created',
    issue,
    comment: { body: 'Approved', user: { type: 'User', login: 'barOwner' } },
  });

  await handlers['issue_comment.created'][0](commentCtx);

  expect(postedBodies()).toContain('validation fallback error');
  expect(postedBodies()).toContain('missing owner contact');
  expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
});

describe('request orchestrator edge coverage for defensive branches', () => {
  const b64 = (s: string): string => Buffer.from(s, 'utf8').toString('base64');

  function productCfg(extraLabels: Record<string, any> = {}) {
    return withWorkflowLabels({
      requests: { product: { folderName: 'resources' } },
      workflow: {
        labels: {
          ...extraLabels,
        },
        approvers: ['approver'],
      },
    } as any);
  }

  test('check_suite.completed failure paginates registry annotations until a partial page', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'failure',
      sha: 'sha-annotations-two-pages',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });
    ctx.payload.check_suite.id = 9901;
    ctx.payload.check_suite.pull_requests = [{ number: 901 }];

    ctx.octokit.checks.listForSuite.mockResolvedValueOnce({
      data: { check_runs: [{ id: 9902, html_url: 'https://example/check/9902' }] },
    });
    ctx.octokit.checks.listAnnotations
      .mockResolvedValueOnce({
        data: Array.from({ length: 100 }, () => ({
          title: 'registry-validate product',
          path: 'resources/product.yaml',
          message: 'first page issue [file=resources/product.yaml requestType=product]',
        })),
      })
      .mockResolvedValueOnce({
        data: [
          {
            title: 'registry-validate product',
            path: 'resources/product.yaml',
            message: 'final page issue [file=resources/product.yaml requestType=product]',
          },
        ],
      });

    ctx.octokit.pulls.get.mockResolvedValueOnce({ data: { html_url: 'https://example/pr/901' } });

    await handlers['check_suite.completed'][0](ctx);

    expect(ctx.octokit.checks.listAnnotations).toHaveBeenCalledTimes(2);
    expect(ctx.octokit.checks.listAnnotations).toHaveBeenNthCalledWith(2, expect.objectContaining({ page: 2 }));
    expect(postedBodies()).toContain('final page issue');
  });

  test('issue_comment: direct PR rejected by onApproval posts rejection without approving or merging', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue: inReview({
          number: 902,
          title: 'Direct PR',
          body: 'manual direct pr',
          labels: [],
          user: { login: 'requester' },
          pull_request: {},
        }),
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: {
          requests: { product: { folderName: 'resources' } },
          workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
        },
      });

      ctx.octokit.pulls.get.mockResolvedValue({
        data: {
          number: 902,
          body: 'manual direct pr',
          title: 'Direct PR',
          state: 'open',
          draft: false,
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-pr-rejected-comment', sha: 'sha-direct-pr-rejected-comment' },
        },
      });
      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-rejected-comment.yaml', status: 'modified' }],
      });
      ctx.octokit.pulls.listCommits.mockResolvedValue({ data: [{ author: { login: 'requester' } }] });
      ctx.octokit.repos.getContent.mockResolvedValue({
        data: { content: b64('type: product\nname: product-rejected-comment\n'), encoding: 'base64' },
      });
      runApprovalHook.mockResolvedValue({ status: 'rejected', reason: 'policy denied' } as any);

      await handlers['issue_comment.created'][0](ctx);

      expect(postOnce).toHaveBeenCalledWith(
        ctx,
        expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 902 }),
        expect.stringContaining('policy denied'),
        expect.objectContaining({ minimizeTag: 'nsreq:on-approval:rejected' })
      );
      expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
      expect(tryMergeIfGreen).not.toHaveBeenCalled();
    } finally {
      if (previousJestWorkerId === undefined) delete process.env.JEST_WORKER_ID;
      else process.env.JEST_WORKER_ID = previousJestWorkerId;
    }
  });

  test('issue_comment: direct PR rejected by onApproval sorts machine-readable fields and keeps details last', async () => {
    const previousJestWorkerId = process.env.JEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;

    try {
      const { app, handlers } = mkApp();
      requestHandler(app);

      const ctx = mkCommentContext({
        event: 'issue_comment.created',
        issue: inReview({
          number: 9021,
          title: 'Direct PR ordered rejection',
          body: 'manual direct pr',
          labels: [],
          user: { login: 'requester' },
          pull_request: {},
        }),
        comment: { body: 'Approved', user: { login: 'reviewer1' } },
        sender: { type: 'User', login: 'reviewer1' },
        withCachedConfig: true,
        config: {
          requests: { product: { folderName: 'resources' } },
          workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
        },
      });

      ctx.octokit.pulls.get.mockResolvedValue({
        data: {
          number: 9021,
          body: 'manual direct pr',
          title: 'Direct PR ordered rejection',
          state: 'open',
          draft: false,
          user: { login: 'requester' },
          base: { ref: 'main' },
          head: { ref: 'feature/direct-pr-rejected-ordered', sha: 'sha-direct-pr-rejected-ordered' },
        },
      });
      ctx.octokit.pulls.listFiles.mockResolvedValue({
        data: [{ filename: 'resources/product-rejected-ordered.yaml', status: 'modified' }],
      });
      ctx.octokit.pulls.listCommits.mockResolvedValue({ data: [{ author: { login: 'requester' } }] });
      ctx.octokit.repos.getContent.mockResolvedValue({
        data: { content: b64('type: product\nname: product-rejected-ordered\n'), encoding: 'base64' },
      });
      runApprovalHook.mockResolvedValue({
        status: 'rejected',
        reason: 'policy denied',
        errors: [
          { field: 'zeta', message: 'zeta message' },
          { field: 'alpha', message: 'alpha message' },
          { field: {}, message: 'details message' },
          { field: true, message: 'boolean field message' },
        ],
      } as any);

      await handlers['issue_comment.created'][0](ctx);

      const rejectionBody = postedBodies();
      expect(rejectionBody).toContain('## onApproval rejected this request');
      expect(rejectionBody).not.toContain('policy denied');
      expect(rejectionBody).toContain('### Alpha');
      expect(rejectionBody).toContain('### True');
      expect(rejectionBody).toContain('### Zeta');
      expect(rejectionBody).toContain('### Details');
      expect(rejectionBody.indexOf('### Alpha')).toBeLessThan(rejectionBody.indexOf('### True'));
      expect(rejectionBody.indexOf('### True')).toBeLessThan(rejectionBody.indexOf('### Zeta'));
      expect(rejectionBody.indexOf('### Zeta')).toBeLessThan(rejectionBody.indexOf('### Details'));
      expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
      expect(tryMergeIfGreen).not.toHaveBeenCalled();
    } finally {
      if (previousJestWorkerId === undefined) delete process.env.JEST_WORKER_ID;
      else process.env.JEST_WORKER_ID = previousJestWorkerId;
    }
  });

  test('check_suite.success: linked direct PR with unknown onApproval posts PR feedback and does not merge', async () => {
    const cfg = {
      requests: { product: { folderName: 'resources' } },
      workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
    } as any;

    const { app, handlers } = mkApp();
    requestHandler(app);

    const pr = {
      number: 903,
      body: 'source: #31',
      title: 'Linked direct PR',
      state: 'open',
      user: { login: 'requester' },
      head: { ref: 'feature/linked-unknown', sha: 'sha-linked-unknown' },
      base: { ref: 'main' },
      mergeable: true,
      mergeable_state: 'clean',
    };

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-linked-unknown',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    extractHashFromPrBody.mockReturnValue('');
    ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        number: 31,
        title: 'Request',
        body: '### Product ID\nproduct-linked-unknown',
        labels: [],
        user: { login: 'author' },
        state: 'open',
      },
    });
    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-linked-unknown.yaml', status: 'modified' }],
    });
    ctx.octokit.repos.getContent.mockResolvedValue({
      data: { content: b64('type: product\nname: product-linked-unknown\n'), encoding: 'base64' },
    });
    runApprovalHook.mockResolvedValue({ status: 'unknown', reason: 'manual review required' } as any);

    await handlers['check_suite.completed'][0](ctx);

    expect(postOnce).toHaveBeenCalledWith(
      ctx,
      expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 903 }),
      expect.stringContaining('manual review required'),
      expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
    );
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('pull_request.opened: approves waiting workflow run for safe registry-only PR', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2001,
        title: 'Direct registry PR',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/registry-pr', sha: 'sha-workflow-waiting' },
        base: { ref: 'main' },
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.agtwf01.yaml', status: 'added' }],
    });

    ctx.octokit.request.mockImplementation(async (route: string, _args: any) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12345,
                name: 'registry-validate',
                status: 'waiting',
                conclusion: null,
                head_sha: 'sha-workflow-waiting',
                pull_requests: [{ number: 2001 }],
              },
            ],
          },
        };
      }

      if (route === 'POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve') {
        return { data: {} };
      }

      return { data: {} };
    });

    extractHashFromPrBody.mockReturnValue('');

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from('type: system\nname: sap.agtwf01\ndescription: Workflow approval test\n', 'utf8').toString(
          'base64'
        ),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'approved',
      comment: 'trusted registry-only PR',
    } as any);

    await handlers['pull_request.opened'][0](ctx);

    expect(ctx.octokit.request).toHaveBeenCalledWith(
      'POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve',
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        run_id: 12345,
      })
    );

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);
    expect(infoMsgs).toContain('workflow-approval:run-approved');
  });

  test('pull_request.opened: does not approve waiting workflow run without trust signal', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2004,
        title: 'Direct registry PR without trust',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/untrusted-registry-pr', sha: 'sha-untrusted-waiting' },
        base: { ref: 'main' },
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.untrustedwf01.yaml', status: 'added' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from(
          'type: system\nname: sap.untrustedwf01\ndescription: Untrusted workflow approval test\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({} as any);

    ctx.octokit.request.mockImplementation(async (route: string) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12348,
                name: 'registry-validate',
                status: 'waiting',
                conclusion: null,
                head_sha: 'sha-untrusted-waiting',
                pull_requests: [{ number: 2004 }],
              },
            ],
          },
        };
      }

      return { data: {} };
    });

    await handlers['pull_request.opened'][0](ctx);

    expect(ctx.octokit.request).not.toHaveBeenCalledWith(
      'POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve',
      expect.anything()
    );

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);
    expect(infoMsgs).toContain('workflow-approval:skip-missing-trust-signal');
    expect(infoMsgs).toContain('workflow-approval:retry-skipped-no-trust-signal');
  });

  test('pull_request.opened: does not approve waiting workflow run for non-registry-only PR', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2002,
        title: 'Mixed PR',
        body: 'manual mixed pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/mixed-pr', sha: 'sha-mixed-waiting' },
        base: { ref: 'main' },
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [
        { filename: 'data/namespaces/sap.agtwf02.yaml', status: 'added' },
        { filename: 'README.md', status: 'modified' },
      ],
    });

    ctx.octokit.request.mockImplementation(async (route: string) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12346,
                name: 'registry-validate',
                status: 'waiting',
                conclusion: null,
                head_sha: 'sha-mixed-waiting',
                pull_requests: [{ number: 2002 }],
              },
            ],
          },
        };
      }

      return { data: {} };
    });

    await handlers['pull_request.opened'][0](ctx);

    expect(ctx.octokit.request).not.toHaveBeenCalledWith(
      'POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve',
      expect.anything()
    );

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);
    expect(infoMsgs).toContain('workflow-approval:skip-not-safe-registry-only-pr');
  });

  test('pull_request.opened: does not schedule retry when workflow run is already visible and completed', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2003,
        title: 'Direct registry PR',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/completed-run', sha: 'sha-completed-run' },
        base: { ref: 'main' },
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.agtwf03.yaml', status: 'added' }],
    });

    ctx.octokit.request.mockImplementation(async (route: string) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12347,
                name: 'registry-validate',
                status: 'completed',
                conclusion: 'success',
                head_sha: 'sha-completed-run',
                pull_requests: [{ number: 2003 }],
              },
            ],
          },
        };
      }

      return { data: {} };
    });

    extractHashFromPrBody.mockReturnValue('');

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from(
          'type: system\nname: sap.agtwf03\ndescription: Workflow approval completed-run test\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'approved',
      comment: 'trusted registry-only PR',
    } as any);

    await handlers['pull_request.opened'][0](ctx);

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);

    expect(infoMsgs).toContain('workflow-approval:no-waiting-runs');
    expect(infoMsgs).toContain('workflow-approval:retry-skipped-run-already-visible');
    expect(infoMsgs).not.toContain('workflow-approval:retry-scheduled');
  });

  test('pull_request.opened: does not approve waiting workflow run without trust signal', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2004,
        title: 'Direct registry PR without trust',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/untrusted-registry-pr', sha: 'sha-untrusted-waiting' },
        base: { ref: 'main' },
      },
    });

    extractHashFromPrBody.mockReturnValue('');

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.untrustedwf01.yaml', status: 'added' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from(
          'type: system\nname: sap.untrustedwf01\ndescription: Untrusted workflow approval test\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({} as any);

    ctx.octokit.request.mockImplementation(async (route: string) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12348,
                name: 'registry-validate',
                status: 'waiting',
                conclusion: null,
                head_sha: 'sha-untrusted-waiting',
                pull_requests: [{ number: 2004 }],
              },
            ],
          },
        };
      }

      return { data: {} };
    });

    await handlers['pull_request.opened'][0](ctx);

    expect(ctx.octokit.request).not.toHaveBeenCalledWith(
      'POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve',
      expect.anything()
    );

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);
    expect(infoMsgs).toContain('workflow-approval:skip-missing-trust-signal');
    expect(infoMsgs).toContain('workflow-approval:retry-skipped-no-trust-signal');
  });

  test('buildReviewHandoverBody does not embed handover marker in body (postOnce appends it)', async () => {
    const { buildReviewHandoverBody } = await import('../src/handlers/request/domain/review-handover-rendering.js');
    const body = buildReviewHandoverBody('');
    expect(body).not.toContain('<!-- nsreq:handover -->');
    expect(body).toContain('### ✅ No issues detected');
    expect(body).toContain('### ➡️ Routing to an approver for review');
  });

  test('pull_request.opened: schedules workflow approval retry when no runs are visible yet', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2005,
        title: 'Direct registry PR',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/no-runs-pr', sha: 'sha-no-runs-pr' },
        base: { ref: 'main' },
      },
    });

    extractHashFromPrBody.mockReturnValue('');

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.agtwf05.yaml', status: 'added' }],
    });

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from(
          'type: system\nname: sap.agtwf05\ndescription: No-runs workflow approval test\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'approved',
      comment: 'trusted registry-only PR',
    } as any);

    // ctx.octokit.request uses default: returns { data: { workflow_runs: [] } }

    await handlers['pull_request.opened'][0](ctx);

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);

    expect(infoMsgs).toContain('workflow-approval:no-waiting-runs');
    expect(infoMsgs).toContain('workflow-approval:retry-scheduled');
    expect(infoMsgs).not.toContain('workflow-approval:retry-skipped-run-already-visible');
  });

  test('pull_request.opened: skips retry when workflow run is already visible as queued', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkPullRequestContext({
      pr: {
        number: 2006,
        title: 'Direct registry PR',
        body: 'manual direct pr',
        state: 'open',
        draft: false,
        user: { login: 'external-user' },
        head: { ref: 'feature/queued-run-pr', sha: 'sha-queued-run-pr' },
        base: { ref: 'main' },
      },
    });

    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'data/namespaces/sap.agtwf06.yaml', status: 'added' }],
    });

    ctx.octokit.request.mockImplementation(async (route: string) => {
      if (route === 'GET /repos/{owner}/{repo}/actions/runs') {
        return {
          data: {
            workflow_runs: [
              {
                id: 12349,
                name: 'registry-validate',
                status: 'queued',
                conclusion: null,
                head_sha: 'sha-queued-run-pr',
                pull_requests: [{ number: 2006 }],
              },
            ],
          },
        };
      }

      return { data: {} };
    });

    extractHashFromPrBody.mockReturnValue('');

    ctx.octokit.repos.getContent.mockResolvedValue({
      data: {
        content: Buffer.from(
          'type: system\nname: sap.agtwf06\ndescription: Queued-run workflow approval test\n',
          'utf8'
        ).toString('base64'),
        encoding: 'base64',
      },
    });

    runApprovalHook.mockResolvedValue({
      status: 'approved',
      comment: 'trusted registry-only PR',
    } as any);

    await handlers['pull_request.opened'][0](ctx);

    const infoMsgs = ctx.log.info.mock.calls.map((call: any[]) => call[1]);

    expect(infoMsgs).toContain('workflow-approval:no-waiting-runs');
    expect(infoMsgs).toContain('workflow-approval:retry-skipped-run-already-visible');
    expect(infoMsgs).not.toContain('workflow-approval:retry-scheduled');
  });

  test('check_suite.success: rejected linked direct PR closes linked PRs and reports closed PR numbers', async () => {
    const cfg = {
      requests: { product: { folderName: 'resources' } },
      workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
    } as any;

    const { app, handlers } = mkApp();
    requestHandler(app);

    const pr = {
      number: 904,
      body: 'source: #32',
      title: 'Linked direct PR',
      state: 'open',
      user: { login: 'requester' },
      head: { ref: 'feature/linked-rejected', sha: 'sha-linked-rejected' },
      base: { ref: 'main' },
      mergeable: true,
      mergeable_state: 'clean',
    };
    const siblingPr = {
      ...pr,
      number: 905,
      head: { ref: 'feature/linked-rejected-sibling', sha: 'sha-linked-rejected-sibling' },
    };

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-linked-rejected',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
      config: cfg,
    });

    extractHashFromPrBody.mockReturnValue('');
    ctx.octokit.pulls.list
      .mockResolvedValueOnce({ data: [pr] })
      .mockResolvedValueOnce({ data: [pr, siblingPr] })
      .mockResolvedValueOnce({ data: [] });
    ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        number: 32,
        title: 'Request',
        body: '### Product ID\nproduct-linked-rejected',
        labels: [],
        user: { login: 'author' },
        state: 'open',
      },
    });
    ctx.octokit.pulls.listFiles.mockResolvedValue({
      data: [{ filename: 'resources/product-linked-rejected.yaml', status: 'modified' }],
    });
    ctx.octokit.repos.getContent.mockResolvedValue({
      data: { content: b64('type: product\nname: product-linked-rejected\n'), encoding: 'base64' },
    });
    runApprovalHook.mockResolvedValue({ status: 'rejected', reason: 'policy denied' } as any);

    await handlers['check_suite.completed'][0](ctx);

    expect(ctx.octokit.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 904, state: 'closed' })
    );
    expect(ctx.octokit.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 905, state: 'closed' })
    );
    expect(postedBodies()).toContain('Closed linked PR(s): #904, #905.');
  });

  test('issue_comment: approval reports missing parent resource and resets request to author action', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const tpl = {
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
    };
    const issue = {
      number: 906,
      title: 'Sub-Context Namespace',
      body: '### Namespace\nsap.missing.child',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'requester' },
      state: 'open',
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ namespace: 'sap.missing.child' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.missing.child',
      nsType: 'subContextNamespace',
      template: tpl,
    });

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'approver' } },
      sender: { type: 'User', login: 'approver' },
      withCachedConfig: true,
      config: productCfg(),
    });
    ctx.octokit.repos.getContent.mockRejectedValue(Object.assign(new Error('missing'), { status: 404 }));

    await handlers['issue_comment.created'][0](ctx);

    expect(postedBodies()).toContain("Parent resource 'sap.missing' is not present");
    expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), issue, 'author');
    expect(createRequestPr).not.toHaveBeenCalled();
  });

  test('issue_comment: approval tolerates approver hook failures and malformed stale parent marker', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 907,
      title: 'Request',
      body: '### Product ID\nABC\n\n<!-- nsreq:parent-approval = {not-json} -->',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'requester' },
      state: 'open',
    };

    runApprovalHook.mockRejectedValue(new Error('hook down'));
    createRequestPr.mockResolvedValueOnce({ number: 77 });

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'approver' } },
      sender: { type: 'User', login: 'approver' },
      withCachedConfig: true,
      config: productCfg(),
    });

    await handlers['issue_comment.created'][0](ctx);

    expect(createRequestPr).toHaveBeenCalled();
    expect(postedBodies()).toContain('Opened PR: #77');
  });

  test('issue_comment: stale branch cleanup ignores missing ref and reports non-json validation failure tail', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 908,
      title: 'Request',
      body: '### Product ID\nABC',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'requester' },
      state: 'open',
    };

    createRequestPr
      .mockRejectedValueOnce(
        new Error('Validation Failed: {"message":"No commits between main and refs/heads/feat/resource-abc-issue-908"}')
      )
      .mockRejectedValueOnce(new Error('Validation Failed: retry tail is not json - https://api.github.test/docs'));

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'approver' } },
      sender: { type: 'User', login: 'approver' },
      withCachedConfig: true,
      config: productCfg(),
    });
    ctx.octokit.git.deleteRef.mockRejectedValueOnce(Object.assign(new Error('already gone'), { status: 404 }));

    await handlers['issue_comment.created'][0](ctx);

    expect(ctx.octokit.git.deleteRef).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o', repo: 'r', ref: 'heads/feat/resource-abc-issue-908' })
    );
    expect(postedBodies()).toContain('retry tail is not json');
    expect(postedBodies()).not.toContain('https://api.github.test');
  });

  test('issue_comment: resource-exists recovery surfaces non-404 default-branch lookup errors', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const tpl = {
      title: 'Request',
      name: 'Request',
      body: [],
      labels: [],
      _meta: { requestType: 'product', root: 'resources', schema: 'schema.json' },
    };
    const issue = {
      number: 909,
      title: 'Request',
      body: '### Product ID\nABC',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'requester' },
      state: 'open',
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ 'product-id': 'ABC' });
    createRequestPr.mockRejectedValueOnce(
      new Error("Validation Failed: Resource 'ABC' already exists at resources/ABC.yaml")
    );

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'Approved', user: { login: 'approver' } },
      sender: { type: 'User', login: 'approver' },
      withCachedConfig: true,
      config: productCfg(),
    });
    ctx.octokit.repos.getContent.mockRejectedValueOnce(Object.assign(new Error('server exploded'), { status: 500 }));

    await handlers['issue_comment.created'][0](ctx);

    expect(ctx.octokit.repos.getContent).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o', repo: 'r', path: 'resources/ABC.yaml' })
    );
    expect(postedBodies()).toContain('server exploded');
  });

  test('issue_comment: author update validation issues default missing paths to details', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    validateRequestIssue.mockResolvedValueOnce({
      errors: ['fallback validation failed'],
      errorsFormatted: '',
      errorsFormattedSingle: '',
      validationIssues: [{ path: '', message: 'missing structured details' }],
    });

    const issue = {
      number: 910,
      title: 'Request',
      body: '### Product ID\nABC',
      labels: [],
      user: { login: 'author' },
      state: 'open',
    };
    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'updated', user: { login: 'author' } },
      sender: { type: 'User', login: 'author' },
      withCachedConfig: true,
    });

    await handlers['issue_comment.created'][0](ctx);

    expect(postedBodies()).toContain('fallback validation failed');
    expect(postedBodies()).toContain('"field": "details"');
    expect(postedBodies()).toContain('missing structured details');
    expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), issue, 'author');
  });

  test('issues.labeled: invalid payload label is ignored before template loading', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = { number: 911, title: 'Request', body: '### Product ID\nABC', labels: [], user: { login: 'author' } };
    const ctx = mkBaseContext({ issue, withCachedConfig: true, config: productCfg() });
    ctx.name = 'issues.labeled';
    ctx.payload = { action: 'labeled', issue, sender: { type: 'User', login: 'someone' }, label: 42 };

    await handlers['issues.labeled'][0](ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: invalid existing issue labels are ignored while routing lock still resolves valid labels', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const cfg = { workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] }, requests: {} } as any;
    const issue = {
      number: 9111,
      title: 'Request',
      body: '### Product ID\nABC\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
      labels: [42, { foo: 'bad' }, { name: 'route-1' }, { name: 'route-2' }],
      user: { login: 'author' },
      state: 'open',
    };
    const ctx = mkBaseContext({ issue, withCachedConfig: true, config: cfg });
    ctx.name = 'issues.labeled';
    ctx.payload = {
      action: 'labeled',
      issue,
      sender: { type: 'User', login: 'someone' },
      label: { name: 'broken-route' },
    };
    ctx.octokit.issues.get.mockResolvedValue({ data: issue });
    (loadTemplate as jest.Mock).mockImplementation(async (_context: any, args: any) => {
      const labels = Array.isArray(args?.issueLabels) ? args.issueLabels.map(String) : [];

      if (labels.length === 1 && (labels[0] === 'route-1' || labels[0] === 'route-2')) {
        return { labels, body: [], title: 'Request' };
      }

      throw new Error(`template lookup failed for labels: ${labels.join(',')}`);
    });

    await handlers['issues.labeled'][0](ctx);

    const loadTemplateLabelSets = (loadTemplate as jest.Mock).mock.calls.map(
      (call: any[]) => call[1]?.issueLabels ?? []
    );

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 9111, name: 'route-2' })
    );
    expect(loadTemplateLabelSets).toEqual(
      expect.arrayContaining([expect.arrayContaining(['route-1']), expect.arrayContaining(['route-2'])])
    );
    expect(loadTemplateLabelSets.flat()).not.toContain(42);
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.opened: non-namespace request removes stale parent marker best effort when body update fails', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 912,
      title: 'Request',
      body: '### Product ID\nABC\n\n<!-- nsreq:parent-approval = {"v":1,"parent":"sap.css","target":"sap.css.foo","owners":["owner"]} -->',
      labels: [],
      user: { login: 'requester' },
      state: 'open',
    };
    const ctx = mkIssuesContext({ issue, action: 'opened', withCachedConfig: true, config: productCfg() });
    ctx.octokit.issues.update.mockRejectedValueOnce(new Error('cannot update marker'));

    await handlers['issues.opened'][0](ctx);

    expect(ctx.octokit.issues.update).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 912,
        body: expect.not.stringContaining('parent-approval'),
      })
    );
    expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), issue, 'review');
  });

  test('issues.opened: parent approval gate still notifies when marker write fails', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const tpl = {
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
      _meta: { requestType: 'subContextNamespace', root: '/data/namespaces', schema: 'x' },
    };
    const issue = {
      number: 913,
      title: 'Sub-Context Namespace',
      body: '### Namespace\nsap.css.bar.foo',
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { login: 'requester' },
      state: 'open',
    };

    loadTemplate.mockResolvedValue(tpl);
    parseForm.mockReturnValue({ namespace: 'sap.css.bar.foo' });
    validateRequestIssue.mockResolvedValue({
      errors: [],
      errorsFormatted: '',
      errorsFormattedSingle: '',
      namespace: 'sap.css.bar.foo',
      nsType: 'subContextNamespace',
      template: tpl,
    });

    const ctx = mkIssuesContext({ issue, action: 'opened', withCachedConfig: true, config: productCfg() });
    ctx.octokit.issues.update.mockImplementation(async (args: any) => {
      if (String(args?.body || '').includes('nsreq:parent-approval')) {
        throw new Error('cannot add marker');
      }

      return {} as any;
    });
    ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return {
          data: {
            content: b64('type: vendor\nname: sap\ncontacts:\n  - "@vendorOwner"\n'),
            encoding: 'base64',
          },
        };
      }

      if (path === 'data/namespaces/sap.yaml') {
        return { data: { content: b64('contacts:\n  - "@vendorOwner"\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64('contacts:\n  - "@topOwner"\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return { data: { content: b64('contacts:\n  - "@parentOwner"\n'), encoding: 'base64' } };
      }
      throw Object.assign(new Error('missing parent'), { status: 404 });
    });

    await handlers['issues.opened'][0](ctx);

    expect(postedBodies()).toContain('Parent owner approval required');
    expect(postedBodies()).toContain('@parentOwner');
    expect(ctx.octokit.issues.update).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 913,
        body: expect.stringContaining('nsreq:parent-approval'),
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 913,
        labels: ['Parent Owner Action'],
      })
    );

    expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o',
        repo: 'r',
        issue_number: 913,
        assignees: ['parentOwner'],
      })
    );

    expect(setStateLabel).not.toHaveBeenCalledWith(ctx, expect.anything(), issue, 'author');
    expect(issue.body).not.toContain('nsreq:parent-approval');
  });

  test('issues.opened: parent approval state replaces other workflow state labels', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 914,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [
        { name: 'Requester Action' },
        { name: 'Review Pending' },
        { name: 'Approved' },
        { name: 'Sub-Context Namespace' },
      ],
      user: { login: 'requester' },
      state: 'open',
    };

    const tpl = {
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
      _meta: {
        requestType: 'subContextNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/sub-context-namespace.schema.json',
      },
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

    const ctx = mkIssuesContext({
      issue,
      action: 'opened',
      withCachedConfig: true,
      config: {
        workflow: {
          labels: {
            approvalRequested: ['Review Pending'],
            approvalSuccessful: ['Approved'],
            parentOwnerAction: 'Parent Owner Action',
          },
          approvers: [],
        },
        requests: {},
      },
    });

    ctx.octokit.issues.get.mockResolvedValue({
      data: {
        ...issue,
        labels: issue.labels,
        assignees: [],
      },
    });

    ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64('contacts:\n  - "@topOwner"\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return { data: { content: b64('contacts:\n  - "@parentOwner"\n'), encoding: 'base64' } };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await handlers['issues.opened'][0](ctx);

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 914,
        name: 'Requester Action',
      })
    );

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 914,
        name: 'Review Pending',
      })
    );

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 914,
        name: 'Approved',
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 914,
        labels: ['Parent Owner Action'],
      })
    );

    expect(setStateLabel).not.toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'author');
  });

  test('issues.opened: parent owner assignment failure still posts parent approval request', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const target = 'sap.css.bar.foo';
    const issue = {
      number: 915,
      title: 'Sub-Context Namespace',
      body: `### Namespace\n\n${target}\n`,
      labels: [{ name: 'Sub-Context Namespace' }],
      user: { login: 'requester' },
      state: 'open',
    };

    const tpl = {
      title: 'Sub-Context Namespace',
      labels: ['Sub-Context Namespace'],
      body: [],
      _meta: {
        requestType: 'subContextNamespace',
        root: '/data/namespaces',
        schema: '.github/registry-bot/request-schemas/sub-context-namespace.schema.json',
      },
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

    ctx.octokit.issues.addAssignees.mockRejectedValueOnce(new Error('Validation Failed'));

    ctx.octokit.repos.getContent.mockImplementation(async ({ path }: any) => {
      if (path === 'data/vendors/sap.yaml') {
        return { data: { content: b64('name: sap\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.yaml') {
        return { data: { content: b64('contacts:\n  - "@topOwner"\n'), encoding: 'base64' } };
      }
      if (path === 'data/namespaces/sap.css.bar.yaml') {
        return { data: { content: b64('contacts:\n  - "@parentOwner"\n'), encoding: 'base64' } };
      }
      throw Object.assign(new Error('Not Found'), { status: 404 });
    });

    await handlers['issues.opened'][0](ctx);

    expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 915,
        assignees: ['parentOwner'],
      })
    );

    expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
      expect.objectContaining({
        issue_number: 915,
        labels: ['Parent Owner Action'],
      })
    );

    expect(postedBodies()).toContain('Parent owner approval required');
    expect(postedBodies()).toContain('@parentOwner');
  });

  test('issues.labeled: routing lock treats template lookup failure for changed label as non-routing', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const cfg = { workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] }, requests: {} } as any;
    const issue = {
      number: 914,
      title: 'Request',
      body: '### Product ID\nABC\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
      labels: [{ name: 'route-1' }, { name: 'route-2' }],
      user: { login: 'requester' },
      state: 'open',
    };
    const ctx = mkBaseContext({ issue, withCachedConfig: true, config: cfg });
    ctx.name = 'issues.labeled';
    ctx.payload = {
      action: 'labeled',
      issue,
      sender: { type: 'User', login: 'someone' },
      label: { name: 'broken-route' },
    };
    ctx.octokit.issues.get.mockResolvedValue({ data: issue });
    (loadTemplate as jest.Mock).mockImplementation(async (_context: any, args: any) => {
      const labels = Array.isArray(args?.issueLabels) ? args.issueLabels.map(String) : [];

      if (labels.length === 1 && (labels[0] === 'route-1' || labels[0] === 'route-2')) {
        return { labels, body: [], title: 'Request' };
      }

      throw new Error('template lookup failed for changed label');
    });

    await handlers['issues.labeled'][0](ctx);

    expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o', repo: 'r', issue_number: 914, name: 'route-2' })
    );
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: routing lock notice is deduplicated while an identical notice is in flight', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const cfg = { workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] }, requests: {} } as any;
    const issue = {
      number: 915,
      title: 'Request',
      body: '### Product ID\nABC\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
      labels: [{ name: 'route-1' }, { name: 'route-2' }],
      user: { login: 'requester' },
      state: 'open',
    };
    const makeCtx = () => {
      const ctx = mkBaseContext({ issue: { ...issue, labels: issue.labels }, withCachedConfig: true, config: cfg });
      ctx.name = 'issues.labeled';
      ctx.payload = {
        action: 'labeled',
        issue: ctx.payload.issue ?? issue,
        sender: { type: 'User', login: 'someone' },
        label: { name: 'route-2' },
      };
      ctx.octokit.issues.get.mockResolvedValue({ data: issue });
      return ctx;
    };

    (loadTemplate as jest.Mock).mockImplementation(async (_context: any, args: any) => {
      const labels = Array.isArray(args?.issueLabels) ? args.issueLabels.map(String) : [];

      if (labels.length === 1 && (labels[0] === 'route-1' || labels[0] === 'route-2')) {
        return { labels, body: [], title: 'Request' };
      }

      if (labels.includes('route-1')) {
        return { labels, body: [], title: 'Request' };
      }

      throw new Error('no routing label found');
    });

    let releasePost!: () => void;
    postOnce.mockImplementationOnce(
      async () =>
        await new Promise<void>((resolve) => {
          releasePost = resolve;
        })
    );

    const ctx1 = makeCtx();
    const first = handlers['issues.labeled'][0](ctx1);
    for (let i = 0; i < 100 && postOnce.mock.calls.length === 0; i += 1) await Promise.resolve();
    expect(postOnce).toHaveBeenCalledTimes(1);
    await Promise.resolve();

    const ctx2 = makeCtx();
    const second = handlers['issues.labeled'][0](ctx2);
    for (let i = 0; i < 50 && postOnce.mock.calls.length === 1; i += 1) await Promise.resolve();
    expect(postOnce).toHaveBeenCalledTimes(1);

    releasePost();
    await Promise.all([first, second]);

    expect(postOnce).toHaveBeenCalledTimes(1);
    expect(String(postOnce.mock.calls[0]?.[2] ?? '')).toContain('Routing label is locked to "route-1"');
  });

  test('check_suite.success: missing head sha collapses PR validation comments but skips auto-merge', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: '',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload.check_suite.status = 'completed';
    ctx.payload.check_suite.head_sha = '';
    ctx.payload.check_suite.pull_requests = [{ number: 919 }];

    await handlers['check_suite.completed'][0](ctx);

    expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1', issue_number: 919 },
      expect.objectContaining({
        tagPrefix: 'nsreq:ci-validation',
        classifier: 'RESOLVED',
      })
    );
    expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_suite.failure: missing suite id returns before listing suite check runs', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'failure',
      sha: 'sha-missing-suite-id',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    delete ctx.payload.check_suite.id;
    ctx.payload.check_suite.pull_requests = [{ number: 920 }];

    await handlers['check_suite.completed'][0](ctx);

    expect(ctx.octokit.checks.listForSuite).not.toHaveBeenCalled();
    expect(ctx.octokit.checks.listAnnotations).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('check_suite.failure: missing PR numbers returns before listing suite check runs', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'failure',
      sha: '',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload.check_suite.id = 9201;
    ctx.payload.check_suite.head_sha = '';
    ctx.payload.check_suite.pull_requests = [];

    await handlers['check_suite.completed'][0](ctx);

    expect(ctx.octokit.checks.listForSuite).not.toHaveBeenCalled();
    expect(ctx.octokit.checks.listAnnotations).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('check_suite.success: resolves PR numbers from associated commit and ignores closed duplicates', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-associated-commit-pr',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload.check_suite.status = 'completed';
    ctx.payload.check_suite.pull_requests = [];

    const listPullRequestsAssociatedWithCommit = jest.fn(
      async (_params: any): Promise<{ data: { number: number; state: string }[] }> => ({
        data: [
          { number: 921, state: 'closed' },
          { number: 922, state: 'open' },
          { number: 922, state: 'open' },
        ],
      })
    );

    ctx.octokit.repos.listPullRequestsAssociatedWithCommit = listPullRequestsAssociatedWithCommit;

    await handlers['check_suite.completed'][0](ctx);

    expect(listPullRequestsAssociatedWithCommit).toHaveBeenCalledWith(
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        commit_sha: 'sha-associated-commit-pr',
        per_page: 100,
      })
    );
    expect(collapseBotCommentsByPrefix).toHaveBeenCalledTimes(1);
    expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1', issue_number: 922 },
      expect.objectContaining({ tagPrefix: 'nsreq:ci-validation' })
    );
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_suite.success: falls back to open PR scan when associated commit lookup fails', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_suite.completed',
      conclusion: 'success',
      sha: 'sha-fallback-pr-scan',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload.check_suite.status = 'completed';
    ctx.payload.check_suite.pull_requests = [];

    const listPullRequestsAssociatedWithCommit = jest.fn(async (_params: any): Promise<never> => {
      throw new Error('commit association lookup failed');
    });

    ctx.octokit.repos.listPullRequestsAssociatedWithCommit = listPullRequestsAssociatedWithCommit;

    ctx.octokit.pulls.list
      .mockResolvedValueOnce({
        data: [
          ...Array.from({ length: 99 }, (_, index) => ({
            number: 930 + index,
            head: { sha: `other-sha-${index}` },
          })),
          {
            number: 1029,
            head: { sha: 'sha-fallback-pr-scan' },
          },
        ],
      })
      .mockResolvedValueOnce({
        data: [
          {
            number: 1030,
            head: { sha: 'sha-fallback-pr-scan' },
          },
        ],
      });

    await handlers['check_suite.completed'][0](ctx);

    expect(listPullRequestsAssociatedWithCommit).toHaveBeenCalledTimes(1);
    expect(ctx.octokit.pulls.list).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        state: 'open',
        per_page: 100,
        page: 1,
      })
    );
    expect(ctx.octokit.pulls.list).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        owner: 'o1',
        repo: 'r1',
        state: 'open',
        per_page: 100,
        page: 2,
      })
    );

    expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1', issue_number: 1029 },
      expect.objectContaining({ tagPrefix: 'nsreq:ci-validation' })
    );
    expect(collapseBotCommentsByPrefix).toHaveBeenCalledWith(
      ctx,
      { owner: 'o1', repo: 'r1', issue_number: 1030 },
      expect.objectContaining({ tagPrefix: 'nsreq:ci-validation' })
    );
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_run.completed: non-completed status is ignored before collapse or merge', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: 'sha-check-run-in-progress',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'in_progress',
        head_sha: 'sha-check-run-in-progress',
        pull_requests: [{ number: 1040 }],
      },
    };

    await handlers['check_run.completed'][0](ctx);

    expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_run.completed: non-blocking neutral conclusion is ignored without marking sequential failures', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'neutral',
      sha: 'sha-check-run-neutral',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'neutral',
        status: 'completed',
        head_sha: 'sha-check-run-neutral',
        pull_requests: [{ number: 1041 }],
      },
    };

    await handlers['check_run.completed'][0](ctx);

    expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.listFiles).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('check_run.completed: success without head sha is ignored before collapsing PR comments', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const ctx = mkCheckSuiteContext({
      event: 'check_run.completed',
      conclusion: 'success',
      sha: '',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    ctx.payload = {
      action: 'completed',
      repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
      check_run: {
        conclusion: 'success',
        status: 'completed',
        head_sha: '',
        pull_requests: [{ number: 1042 }],
      },
    };

    await handlers['check_run.completed'][0](ctx);

    expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('status: non-success and incomplete payloads do not trigger auto-merge lookup', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const failureCtx = mkStatusContext({
      state: 'failure',
      sha: 'sha-status-failure',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    await handlers['status'][0](failureCtx);

    expect(failureCtx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    const missingShaCtx = mkStatusContext({
      state: 'success',
      sha: '',
      ownerLogin: 'o1',
      repoName: 'r1',
      withCachedConfig: true,
    });

    await handlers['status'][0](missingShaCtx);

    expect(missingShaCtx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();

    const missingRepoCtx = mkBaseContext({ withCachedConfig: true });
    missingRepoCtx.name = 'status';
    missingRepoCtx.payload = {
      state: 'success',
      sha: 'sha-status-missing-repo',
      repository: { owner: { login: 'o1' } },
    };

    await handlers['status'][0](missingRepoCtx);

    expect(missingRepoCtx.octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('issue_comment: non-approval comment from non-author is ignored after request recognition', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 1050,
      title: 'Request',
      body: '### Product ID\nABC',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'author' },
      state: 'open',
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'LGTM, but not an approval command', user: { login: 'reviewer' } },
      sender: { type: 'User', login: 'reviewer' },
      withCachedConfig: true,
      config: productCfg(),
    });

    await handlers['issue_comment.created'][0](ctx);

    expect(loadTemplate).toHaveBeenCalled();
    expect(createRequestPr).not.toHaveBeenCalled();
    expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('issue_comment: author comment without update keyword does not revalidate or change state', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app);

    const issue = {
      number: 1051,
      title: 'Request',
      body: '### Product ID\nABC',
      labels: [{ name: 'Review Pending' }],
      user: { login: 'author' },
      state: 'open',
    };

    const ctx = mkCommentContext({
      event: 'issue_comment.created',
      issue,
      comment: { body: 'I will look into this later', user: { login: 'author' } },
      sender: { type: 'User', login: 'author' },
      withCachedConfig: true,
      config: productCfg(),
    });

    await handlers['issue_comment.created'][0](ctx);

    expect(loadTemplate).toHaveBeenCalled();
    expect(validateRequestIssue).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
    expect(createRequestPr).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });
});
