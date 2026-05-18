/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { afterAll, beforeAll, beforeEach, expect, jest, test } from '@jest/globals';

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

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
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
      },
      pulls: {
        get: jest.fn<PullsGetFn>((pullArgs: any) =>
          Promise.resolve({
            data: {
              number: Number(pullArgs?.pull_number ?? 5),
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
    },
  };

  if (args.withCachedConfig) {
    ctx.resourceBotConfig = withWorkflowLabels(args.config ?? DEFAULT_CONFIG);
    ctx.resourceBotHooks = null;
    ctx.resourceBotHooksSource = null;
  }

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

test('check_run.completed failure marks failed sequential registry heads and advances the queue', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 916,
    body: 'manual direct pr',
    title: 'Direct',
    state: 'open',
    head: { ref: 'feature/check-run-failure', sha: 'sha-check-run-failure' },
    base: { ref: 'main' },
  };
  const ctx = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'failure',
    sha: 'sha-check-run-failure',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });
  ctx.payload = {
    action: 'completed',
    repository: { name: 'r1', owner: { login: 'o1' }, default_branch: 'main' },
    check_run: {
      conclusion: 'failure',
      status: 'completed',
      head_sha: 'sha-check-run-failure',
      pull_requests: [{ number: 916 }],
    },
  };
  extractHashFromPrBody.mockReturnValue('');
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-check-run-failure.yaml', status: 'modified' }],
  });

  await handlers['check_run.completed'][0](ctx);

  expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.listFiles).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', pull_number: 916 })
  );
  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('check_run.completed success releases active sequential PR when green head is not approved', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 917,
    body: 'manual direct pr',
    title: 'Direct',
    state: 'open',
    user: { login: 'requester' },
    head: { ref: 'feature/sequential-release', sha: 'sha-sequential-release' },
    base: { ref: 'main' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const pushCtx = mkBaseContext({ owner: 'o1', repo: 'r-sequential-release', withCachedConfig: true, config: cfg });
  pushCtx.name = 'push';
  pushCtx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-sequential-release', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };
  extractHashFromPrBody.mockReturnValue('');
  pushCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] });
  pushCtx.octokit.pulls.get.mockResolvedValue({ data: pr });
  pushCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-release.yaml', status: 'modified' }],
  });
  pushCtx.octokit.pulls.updateBranch.mockResolvedValueOnce({});
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });

  await handlers['push'][0](pushCtx);
  expect(pushCtx.octokit.pulls.updateBranch).toHaveBeenCalled();

  const checkCtx = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'success',
    sha: 'sha-sequential-release',
    ownerLogin: 'o1',
    repoName: 'r-sequential-release',
    withCachedConfig: true,
    config: cfg,
  });
  checkCtx.payload = {
    action: 'completed',
    repository: { name: 'r-sequential-release', owner: { login: 'o1' }, default_branch: 'main' },
    check_run: {
      conclusion: 'success',
      status: 'completed',
      head_sha: 'sha-sequential-release',
      pull_requests: [{ number: 917 }],
    },
  };
  extractHashFromPrBody.mockReturnValue('');
  checkCtx.octokit.pulls.list
    .mockResolvedValueOnce({ data: [{ ...pr, mergeable_state: 'clean' }] })
    .mockResolvedValueOnce({ data: [] });
  checkCtx.octokit.pulls.get.mockResolvedValue({ data: { ...pr, mergeable_state: 'clean' } });
  checkCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-release.yaml', status: 'modified' }],
  });
  checkCtx.octokit.repos.getContent.mockResolvedValue({
    data: { content: b64('type: product\nname: product-sequential-release\n'), encoding: 'base64' },
  });
  checkCtx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  runApprovalHook.mockResolvedValue({ status: 'unknown', reason: 'manual review required' } as any);

  await handlers['check_run.completed'][0](checkCtx);

  expect(postOnce).toHaveBeenCalledWith(
    checkCtx,
    expect.objectContaining({ owner: 'o1', repo: 'r-sequential-release', issue_number: 917 }),
    expect.stringContaining('manual review required'),
    expect.objectContaining({ minimizeTag: 'nsreq:on-approval:unknown' })
  );
  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('check_run.completed success handles sequential changed-file lookup failure defensively without merge', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 918,
    body: 'manual direct pr',
    title: 'Direct',
    state: 'open',
    user: { login: 'requester' },
    head: { ref: 'feature/sequential-processing-failure', sha: 'sha-sequential-processing-failure' },
    base: { ref: 'main' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const pushCtx = mkBaseContext({ owner: 'o1', repo: 'r-processing-failure', withCachedConfig: true, config: cfg });
  pushCtx.name = 'push';
  pushCtx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-processing-failure', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };
  extractHashFromPrBody.mockReturnValue('');
  pushCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] });
  pushCtx.octokit.pulls.get.mockResolvedValue({ data: pr });
  pushCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-processing-failure.yaml', status: 'modified' }],
  });
  pushCtx.octokit.pulls.updateBranch.mockResolvedValueOnce({});
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });

  await handlers['push'][0](pushCtx);
  expect(pushCtx.octokit.pulls.updateBranch).toHaveBeenCalled();

  const checkCtx = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'success',
    sha: 'sha-sequential-processing-failure',
    ownerLogin: 'o1',
    repoName: 'r-processing-failure',
    withCachedConfig: true,
    config: cfg,
  });
  checkCtx.payload = {
    action: 'completed',
    repository: { name: 'r-processing-failure', owner: { login: 'o1' }, default_branch: 'main' },
    check_run: {
      conclusion: 'success',
      status: 'completed',
      head_sha: 'sha-sequential-processing-failure',
      pull_requests: [{ number: 918 }],
    },
  };
  extractHashFromPrBody.mockReturnValue('');
  checkCtx.octokit.pulls.list
    .mockResolvedValueOnce({ data: [{ ...pr, mergeable_state: 'clean' }] })
    .mockResolvedValueOnce({ data: [] });
  checkCtx.octokit.pulls.get.mockResolvedValue({ data: { ...pr, mergeable_state: 'clean' } });
  checkCtx.octokit.pulls.listFiles
    .mockRejectedValueOnce(new Error('files failed before processing'))
    .mockResolvedValueOnce({ data: [{ filename: 'resources/product-processing-failure.yaml', status: 'modified' }] });

  await handlers['check_run.completed'][0](checkCtx);

  const warnMessages = checkCtx.log.warn.mock.calls.map((call: any[]) => String(call[1] ?? call[0] ?? '')).join('\n');

  expect(warnMessages).toContain('sequential-registry-pr:changed-files-lookup-failed');
  expect(warnMessages).not.toContain('auto-merge candidate processing failed');
  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('push: approval config change uses dedicated direct PR reevaluation reason', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-config-change', withCachedConfig: true, config: cfg });
  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  }) as unknown as typeof setTimeout);

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-config-change', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [
      {
        modified: ['.github/registry-bot/config.yaml'],
        added: [],
        removed: [],
      },
    ],
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValue({ data: [] });

  await handler(ctx);

  expect(
    ctx.log.info.mock.calls.some(
      (call: any[]) =>
        String(call[1] ?? '') === 'direct-pr-reeval:start' &&
        String((call[0] as { reason?: string })?.reason ?? '') === 'default-branch-push:approval-config-change'
    )
  ).toBe(true);

  setTimeoutSpy.mockRestore();
});

test('push: approved registry PR retries branch update after benign expected head failure', async () => {
  const cfg = {
    requests: {
      product: { folderName: 'resources' },
    },
    workflow: {
      labels: { approvalSuccessful: ['Approved'] },
      approvers: [],
    },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-retry', withCachedConfig: true, config: cfg });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-branch-retry', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const pr = {
    number: 301,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-retry', sha: 'sha-branch-retry' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-retry.yaml', status: 'modified' }],
  });
  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 301, labels: [{ name: 'Approved' }] },
  });
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
  ctx.octokit.pulls.updateBranch
    .mockRejectedValueOnce(Object.assign(new Error('expected_head_sha mismatch'), { status: 422 }))
    .mockResolvedValueOnce({});

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(2);
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({
      owner: 'o1',
      repo: 'r-branch-retry',
      pull_number: 301,
      expected_head_sha: 'sha-branch-retry',
    })
  );
  expect(ctx.octokit.pulls.updateBranch).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({
      owner: 'o1',
      repo: 'r-branch-retry',
      pull_number: 301,
    })
  );
  expect(ctx.octokit.pulls.updateBranch.mock.calls[1]?.[0]).not.toHaveProperty('expected_head_sha');
  expect(
    ctx.log.info.mock.calls.some(
      (call: any[]) => String(call[1] ?? '') === 'pull-request branch update requested after expected-head retry'
    )
  ).toBe(true);
}, 10000);

test('push: approved registry PR skips branch update when benign expected head failure sees a newer head', async () => {
  const cfg = {
    requests: {
      product: { folderName: 'resources' },
    },
    workflow: {
      labels: { approvalSuccessful: ['Approved'] },
      approvers: [],
    },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-head-changed', withCachedConfig: true, config: cfg });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-branch-head-changed', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const pr = {
    number: 3011,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-head-changed', sha: 'sha-branch-head-changed' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-head-changed.yaml', status: 'modified' }],
  });
  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 3011, labels: [{ name: 'Approved' }] },
  });
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValueOnce({ data: pr }).mockResolvedValueOnce({
    data: {
      ...pr,
      head: { ref: 'feature/branch-head-changed', sha: 'sha-branch-head-changed-newer' },
      mergeable_state: 'clean',
    },
  });
  ctx.octokit.pulls.updateBranch.mockRejectedValueOnce(
    Object.assign(new Error('expected_head_sha mismatch'), { status: 422 })
  );

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);
  expect(
    ctx.log.info.mock.calls.some(
      (call: any[]) => String(call[1] ?? '') === 'pull-request branch update skipped: head already changed'
    )
  ).toBe(true);
});

test('push: approved registry PR posts manual update notice after protected branch failure', async () => {
  const cfg = {
    requests: {
      product: { folderName: 'resources' },
    },
    workflow: {
      labels: { approvalSuccessful: ['Approved'] },
      approvers: [],
    },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-manual', withCachedConfig: true, config: cfg });
  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation((() => {
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  }) as unknown as typeof setTimeout);

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-branch-manual', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const pr = {
    number: 302,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-manual', sha: 'sha-branch-manual' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-manual.yaml', status: 'modified' }],
  });
  ctx.octokit.issues.get.mockResolvedValue({
    data: { number: 302, labels: [{ name: 'Approved' }] },
  });
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
  ctx.octokit.pulls.updateBranch.mockRejectedValueOnce(
    Object.assign(new Error('Protected branch policy'), { status: 403 })
  );

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);
  expect(postedBodies()).toContain('Could not update PR branch automatically');
  expect(postedBodies()).toContain('Protected branch policy');
  expect(postOnce).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r-branch-manual', issue_number: 302 }),
    expect.stringContaining('Please update the branch manually.'),
    expect.objectContaining({ minimizeTag: 'nsreq:update-branch-failed' })
  );

  setTimeoutSpy.mockRestore();
});

test('push: approved registry PR skips retry when benign expected head failure is no longer behind', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-benign-clean', withCachedConfig: true, config: cfg });

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-branch-benign-clean', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const pr = {
    number: 3041,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-benign-clean', sha: 'sha-branch-benign-clean' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-benign-clean.yaml', status: 'modified' }],
  });
  ctx.octokit.issues.get.mockResolvedValue({ data: { number: 3041, labels: [{ name: 'Approved' }] } });
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValueOnce({ data: pr }).mockResolvedValueOnce({
    data: {
      ...pr,
      mergeable_state: 'clean',
    },
  });
  ctx.octokit.pulls.updateBranch.mockRejectedValueOnce(
    Object.assign(new Error('expected_head_sha mismatch'), { status: 422 })
  );

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);
  expect(
    ctx.log.info.mock.calls.some(
      (call: any[]) => String(call[1] ?? '') === 'pull-request branch update skipped after benign failure'
    )
  ).toBe(true);
});

test('push: approved registry PR logs retry failure after benign expected head mismatch', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-retry-failed', withCachedConfig: true, config: cfg });
  const setTimeoutSpy = jest.spyOn(global, 'setTimeout').mockImplementation(((callback: TimerHandler) => {
    if (typeof callback === 'function') callback();
    return { unref: jest.fn() } as unknown as ReturnType<typeof setTimeout>;
  }) as unknown as typeof setTimeout);

  ctx.name = 'push';
  ctx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-branch-retry-failed', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };

  const pr = {
    number: 305,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-retry-failed', sha: 'sha-branch-retry-failed' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-retry-failed.yaml', status: 'modified' }],
  });
  ctx.octokit.issues.get.mockResolvedValue({ data: { number: 305, labels: [{ name: 'Approved' }] } });
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
  ctx.octokit.pulls.updateBranch
    .mockRejectedValueOnce(Object.assign(new Error('expected_head_sha mismatch'), { status: 422 }))
    .mockRejectedValueOnce(Object.assign(new Error('retry failed'), { status: 422 }));

  await handler(ctx);

  expect(ctx.octokit.pulls.updateBranch).toHaveBeenCalledTimes(2);
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) => String(call[1] ?? '') === 'pull-request branch update retry failed')
  ).toBe(true);

  setTimeoutSpy.mockRestore();
});

test('push: approved registry PR skips immediate second branch update while cooldown is active', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['push'][0];
  const buildCtx = () => {
    const ctx = mkBaseContext({ owner: 'o1', repo: 'r-branch-cooldown', withCachedConfig: true, config: cfg });
    ctx.name = 'push';
    ctx.payload = {
      ref: 'refs/heads/main',
      repository: { name: 'r-branch-cooldown', owner: { login: 'o1' }, default_branch: 'main' },
      commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
    };
    return ctx;
  };

  const pr = {
    number: 306,
    state: 'open',
    body: 'manual direct pr',
    title: 'Approved Direct',
    head: { ref: 'feature/branch-cooldown', sha: 'sha-branch-cooldown' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const ctx1 = buildCtx();
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx1.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx1.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-cooldown.yaml', status: 'modified' }],
  });
  ctx1.octokit.issues.get.mockResolvedValue({ data: { number: 306, labels: [{ name: 'Approved' }] } });
  ctx1.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx1.octokit.pulls.get.mockResolvedValue({ data: pr });
  ctx1.octokit.pulls.updateBranch.mockResolvedValueOnce({});

  await handler(ctx1);
  expect(ctx1.octokit.pulls.updateBranch).toHaveBeenCalledTimes(1);

  const ctx2 = buildCtx();
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  ctx2.octokit.pulls.list.mockResolvedValueOnce({ data: [] }).mockResolvedValueOnce({ data: [pr] });
  ctx2.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-branch-cooldown.yaml', status: 'modified' }],
  });
  ctx2.octokit.issues.get.mockResolvedValue({ data: { number: 306, labels: [{ name: 'Approved' }] } });
  ctx2.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  ctx2.octokit.pulls.get.mockResolvedValue({ data: pr });

  await handler(ctx2);

  expect(ctx2.octokit.pulls.updateBranch).not.toHaveBeenCalled();
  expect(
    ctx2.log.info.mock.calls.some(
      (call: any[]) => String(call[1] ?? '') === 'pull-request branch update skipped: cooldown active'
    )
  ).toBe(true);
});

test('check_suite.completed success without repo info returns before suite processing', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const ctx = mkCheckSuiteContext({
    event: 'check_suite.completed',
    conclusion: 'success',
    sha: 'sha-no-repo-info',
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
  });

  ctx.payload = {
    check_suite: {
      id: 123,
      conclusion: 'success',
      status: 'completed',
      head_sha: 'sha-no-repo-info',
      pull_requests: [{ number: 920 }],
    },
    repository: { default_branch: 'main' },
  };

  await handlers['check_suite.completed'][0](ctx);

  expect(collapseBotCommentsByPrefix).not.toHaveBeenCalled();
  expect(ctx.octokit.pulls.list).not.toHaveBeenCalled();
  expect(tryMergeIfGreen).not.toHaveBeenCalled();
});

test('status: active sequential registry PR failure clears active slot and advances queue', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 930,
    body: 'manual direct pr',
    title: 'Direct',
    state: 'open',
    user: { login: 'requester' },
    head: { ref: 'feature/sequential-failure-active', sha: 'sha-sequential-failure-active' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const pushCtx = mkBaseContext({
    owner: 'o1',
    repo: 'r-sequential-failure-active',
    withCachedConfig: true,
    config: cfg,
  });
  pushCtx.name = 'push';
  pushCtx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-sequential-failure-active', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };
  extractHashFromPrBody.mockReturnValue('');
  pushCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] });
  pushCtx.octokit.pulls.get.mockResolvedValue({ data: pr });
  pushCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-failure-active.yaml', status: 'modified' }],
  });
  pushCtx.octokit.issues.get.mockResolvedValue({ data: { number: 930, labels: [{ name: 'Approved' }] } });
  pushCtx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  pushCtx.octokit.pulls.updateBranch.mockResolvedValueOnce({});
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });

  await handlers['push'][0](pushCtx);
  expect(pushCtx.octokit.pulls.updateBranch).toHaveBeenCalled();

  const statusCtx = mkBaseContext({
    owner: 'o1',
    repo: 'r-sequential-failure-active',
    withCachedConfig: true,
    config: cfg,
  });
  statusCtx.name = 'status';
  statusCtx.payload = {
    state: 'success',
    sha: 'sha-sequential-failure-active',
    repository: { name: 'r-sequential-failure-active', owner: { login: 'o1' } },
  };
  extractHashFromPrBody.mockReturnValue('');
  statusCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
  statusCtx.octokit.pulls.get.mockResolvedValue({ data: { ...pr, mergeable_state: 'clean' } });
  statusCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-failure-active.yaml', status: 'modified' }],
  });
  statusCtx.octokit.repos.getContent.mockResolvedValue({
    data: { content: b64('type: product\nname: product-sequential-failure-active\n'), encoding: 'base64' },
  });
  statusCtx.octokit.pulls.listCommits.mockResolvedValue({ data: [{ author: { login: 'requester' } }] });
  statusCtx.octokit.checks.listForRef.mockResolvedValue({
    data: { check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }] },
  });
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved before sequential failure' } as any);
  tryMergeIfGreen.mockRejectedValueOnce(new Error('boom'));

  await handlers['status'][0](statusCtx);

  expect(statusCtx.octokit.pulls.list).toHaveBeenCalledTimes(2);
  expect(
    statusCtx.log.warn.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge candidate processing failed')
    )
  ).toBe(true);
});

test('status-approved head lets a later sequential push update the same direct PR without reviews or labels', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const cleanPr = {
    number: 931,
    body: 'manual direct pr',
    title: 'Direct',
    state: 'open',
    user: { login: 'requester' },
    head: { ref: 'feature/sequential-cached-approval', sha: 'sha-sequential-cached-approval' },
    base: { ref: 'main', sha: 'base-sha' },
    mergeable: true,
    mergeable_state: 'clean',
  };
  const behindPr = {
    ...cleanPr,
    mergeable_state: 'behind',
  };

  const statusCtx = mkBaseContext({
    owner: 'o1',
    repo: 'r-sequential-cached-approval',
    withCachedConfig: true,
    config: cfg,
  });
  statusCtx.name = 'status';
  statusCtx.payload = {
    state: 'success',
    sha: 'sha-sequential-cached-approval',
    repository: { name: 'r-sequential-cached-approval', owner: { login: 'o1' } },
  };
  extractHashFromPrBody.mockReturnValue('');
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  statusCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [cleanPr] }).mockResolvedValueOnce({ data: [] });
  statusCtx.octokit.pulls.get.mockResolvedValue({ data: cleanPr });
  statusCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-cached-approval.yaml', status: 'modified' }],
  });
  statusCtx.octokit.repos.getContent.mockResolvedValue({
    data: { content: b64('type: product\nname: product-sequential-cached-approval\n'), encoding: 'base64' },
  });
  statusCtx.octokit.pulls.listCommits.mockResolvedValue({ data: [{ author: { login: 'requester' } }] });
  statusCtx.octokit.checks.listForRef.mockResolvedValue({
    data: { check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }] },
  });
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved for cached sequential head' } as any);

  await handlers['status'][0](statusCtx);

  expect(statusCtx.octokit.pulls.createReview).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r-sequential-cached-approval', pull_number: 931, event: 'APPROVE' })
  );

  const pushCtx = mkBaseContext({
    owner: 'o1',
    repo: 'r-sequential-cached-approval',
    withCachedConfig: true,
    config: cfg,
  });
  pushCtx.name = 'push';
  pushCtx.payload = {
    ref: 'refs/heads/main',
    repository: { name: 'r-sequential-cached-approval', owner: { login: 'o1' }, default_branch: 'main' },
    commits: [{ modified: ['docs/readme.md'], added: [], removed: [] }],
  };
  extractHashFromPrBody.mockReturnValue('');
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  pushCtx.octokit.pulls.list.mockResolvedValueOnce({ data: [behindPr] });
  pushCtx.octokit.pulls.get.mockResolvedValue({ data: behindPr });
  pushCtx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: 'resources/product-sequential-cached-approval.yaml', status: 'modified' }],
  });
  pushCtx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  pushCtx.octokit.pulls.updateBranch.mockResolvedValueOnce({});
  pushCtx.octokit.issues.get.mockRejectedValueOnce(new Error('labels should not be needed'));

  await handlers['push'][0](pushCtx);

  expect(pushCtx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o1',
      repo: 'r-sequential-cached-approval',
      pull_number: 931,
      expected_head_sha: 'sha-sequential-cached-approval',
    })
  );
  expect(pushCtx.octokit.issues.get).not.toHaveBeenCalled();
});

test('check_run.completed failure advances the next approved sequential registry PR in sorted order', async () => {
  const cfg = {
    requests: { product: { folderName: 'resources' } },
    workflow: { labels: { approvalSuccessful: ['Approved'] }, approvers: [] },
  } as any;

  const { app, handlers } = mkApp();
  requestHandler(app);

  const activePr = {
    number: 940,
    body: 'manual direct pr',
    title: 'Active Direct',
    state: 'open',
    head: { ref: 'feature/active-failed', sha: 'sha-active-failed' },
    base: { ref: 'main' },
  };
  const nextPr = {
    number: 941,
    body: 'manual direct pr',
    title: 'Next Direct',
    state: 'open',
    head: { ref: 'feature/next-direct', sha: 'sha-next-direct' },
    base: { ref: 'main' },
    mergeable: true,
    mergeable_state: 'behind',
  };

  const checkCtx = mkCheckSuiteContext({
    event: 'check_run.completed',
    conclusion: 'failure',
    sha: 'sha-active-failed',
    ownerLogin: 'o1',
    repoName: 'r-sequential-resolve-base',
    withCachedConfig: true,
    config: cfg,
  });
  checkCtx.payload = {
    action: 'completed',
    repository: { name: 'r-sequential-resolve-base', owner: { login: 'o1' }, default_branch: 'main' },
    check_run: {
      conclusion: 'failure',
      status: 'completed',
      head_sha: 'sha-active-failed',
      pull_requests: [{ number: 940 }],
    },
  };
  extractHashFromPrBody.mockReturnValue('');
  loadStaticConfig.mockResolvedValueOnce({ config: cfg, source: 'mock', hooks: null, hooksSource: null });
  checkCtx.octokit.pulls.list
    .mockResolvedValueOnce({ data: [activePr, nextPr] })
    .mockResolvedValueOnce({ data: [activePr, nextPr] });
  checkCtx.octokit.pulls.listFiles.mockImplementation(async ({ pull_number }: any) => {
    if (pull_number === 940) {
      return { data: [{ filename: 'resources/product-active-failed.yaml', status: 'modified' }] };
    }
    return { data: [{ filename: 'resources/product-next-direct.yaml', status: 'modified' }] };
  });
  checkCtx.octokit.pulls.get.mockImplementation(async ({ pull_number }: any) => {
    if (pull_number === 941) {
      return { data: nextPr };
    }
    return { data: activePr };
  });
  checkCtx.octokit.issues.get.mockResolvedValue({ data: { number: 941, labels: [{ name: 'Approved' }] } });
  checkCtx.octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  checkCtx.octokit.pulls.updateBranch.mockResolvedValueOnce({});

  await handlers['check_run.completed'][0](checkCtx);

  expect(checkCtx.octokit.pulls.updateBranch).toHaveBeenCalledWith(
    expect.objectContaining({
      owner: 'o1',
      repo: 'r-sequential-resolve-base',
      pull_number: 941,
      expected_head_sha: 'sha-next-direct',
    })
  );
});
