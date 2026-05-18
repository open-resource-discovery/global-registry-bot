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

const setStateLabel = jest.fn(async (_ctx: any, _params: any, _issue: any, _state: any) => {});
const ensureAssigneesOnce = jest.fn(async () => {});
const postOnce = jest.fn(async (_ctx: any, _params: any, _body: string, _options?: any) => {});
const collapseBotCommentsByPrefix = jest.fn(async () => {});

const loadTemplate = jest.fn(async () => ({}));
const parseForm = jest.fn(() => ({}));
const validateRequestIssue = jest.fn(async () => ({}));
const calcSnapshotHash = jest.fn(() => 'h1');
const extractHashFromPrBody = jest.fn(() => 'h1');
const findOpenIssuePrs = jest.fn(async () => []);
const runApprovalHook = jest.fn(async () => false);

const createRequestPr = jest.fn(async () => ({ number: 10 }));
const tryMergeIfGreen = jest.fn(async () => undefined);
const loadStaticConfig = jest.fn(async () => ({}));
const getDocLinksFromConfig = jest.fn(() => '');

const DEFAULT_CONFIG = {
  workflow: {
    labels: {
      authorAction: 'Requester Action',
      approverAction: 'Review Pending',
      parentOwnerAction: 'Parent Owner Action',
      approvalRequested: ['Review Pending'],
      approvalSuccessful: ['Approved'],
      approvalRejected: ['Rejected'],
    },
    approvers: [],
  },
  requests: {},
} as any;

let requestHandler: any;

function postedBodies(): string {
  return postOnce.mock.calls.map((call) => String(call[2] ?? '')).join('\n');
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
      for (const event of Array.isArray(events) ? events : [events]) {
        handlers[event] = handlers[event] || [];
        handlers[event].push(fn);
      }
    },
  };

  return { app, handlers };
}

function mkBaseContext(args: { owner?: string; repo?: string; withCachedConfig?: boolean; config?: any }) {
  const owner = args.owner ?? 'o';
  const repo = args.repo ?? 'r';

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
    issue: () => ({ owner, repo, issue_number: 1 }),
    octokit: {
      issues: {
        get: jest.fn(async () => ({ data: { number: 1, labels: [] } })),
        update: jest.fn(async () => ({})),
        addLabels: jest.fn(async () => ({})),
        removeLabel: jest.fn(async () => ({})),
        addAssignees: jest.fn(async () => ({})),
      },
      pulls: {
        get: jest.fn(async (pullArgs: any) => ({
          data: {
            number: Number(pullArgs?.pull_number ?? 5),
            state: 'open',
            draft: false,
            body: 'manual direct pr',
            head: { ref: 'x', sha: 'sha1' },
            base: { ref: 'main', sha: 'base-sha' },
            mergeable: true,
            mergeable_state: 'clean',
          },
        })),
        list: jest.fn(async () => ({ data: [] })),
        listFiles: jest.fn(async () => ({ data: [] })),
        listCommits: jest.fn(async () => ({ data: [] })),
        listReviews: jest.fn(async () => ({ data: [] })),
        createReview: jest.fn(async () => ({})),
        update: jest.fn(async () => ({})),
        updateBranch: jest.fn(async () => ({})),
      },
      git: {
        deleteRef: jest.fn(async () => ({})),
      },
      repos: {
        getContent: jest.fn(async () => ({})),
      },
      checks: {
        listAnnotations: jest.fn(async () => ({ data: [] as any[] })),
        listForSuite: jest.fn(async () => ({ data: { check_runs: [] } })),
        listForRef: jest.fn(async () => ({
          data: {
            check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
          },
        })),
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

function mkCheckRunContext(args: {
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
  ctx.name = 'check_run.completed';
  ctx.payload = {
    action: 'completed',
    repository: { name: args.repoName, owner: { login: args.ownerLogin }, default_branch: 'main' },
    check_run: {
      conclusion: 'success',
      status: 'completed',
      head_sha: args.sha,
      pull_requests: [{ number: 1 }],
    },
  };
  return ctx;
}

function prepareUnknownStandaloneCurrentHeadCase(args: {
  prNumber: number;
  sha: string;
  fileName: string;
  resourceName: string;
  reviews: any[];
  config: any;
}) {
  const ctx = mkCheckRunContext({
    sha: args.sha,
    ownerLogin: 'o1',
    repoName: 'r1',
    withCachedConfig: true,
    config: args.config,
  });

  const pr = {
    number: args.prNumber,
    state: 'open',
    body: 'manual direct pr',
    title: 'Direct fallback PR',
    user: { login: 'requester' },
    base: { ref: 'main', sha: 'base-sha' },
    head: { ref: `feature/${args.resourceName}`, sha: args.sha },
    mergeable: true,
    mergeable_state: 'clean',
  };

  ctx.payload.check_run.pull_requests = [{ number: args.prNumber }];
  ctx.octokit.pulls.list.mockResolvedValueOnce({ data: [pr] }).mockResolvedValueOnce({ data: [] });
  ctx.octokit.pulls.get.mockResolvedValue({ data: pr });
  ctx.octokit.pulls.listFiles.mockResolvedValue({
    data: [{ filename: args.fileName, status: 'modified' }],
  });
  ctx.octokit.repos.getContent.mockResolvedValue({
    data: {
      content: b64(`type: product\nname: ${args.resourceName}\n`),
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
  ctx.octokit.pulls.listReviews.mockResolvedValue({ data: args.reviews });
  ctx.octokit.checks.listForRef.mockResolvedValue({
    data: {
      check_runs: [{ id: 1, name: 'validate', status: 'completed', conclusion: 'success' }],
    },
  });

  return { ctx, pr };
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

test('check_run.success: unknown approval accepts bot manual fallback review for current head without marker', async () => {
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

  const { ctx } = prepareUnknownStandaloneCurrentHeadCase({
    prNumber: 501,
    sha: 'sha-current-head-manual-bot-approved',
    fileName: 'resources/product-current-head-manual-bot-approved.yaml',
    resourceName: 'product-current-head-manual-bot-approved',
    config: cfg,
    reviews: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-29T09:46:00Z',
        commit_id: 'sha-current-head-manual-bot-approved',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @configuredApprover',
      },
    ],
  });

  runApprovalHook.mockResolvedValueOnce({ status: 'unknown', message: 'manual review required' } as any);
  tryMergeIfGreen.mockResolvedValueOnce(true as never);

  await handlers['check_run.completed'][0](ctx);

  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 501, mergeMethod: 'squash' })
  );
  expect(ctx.octokit.pulls.createReview).not.toHaveBeenCalled();
  expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 501, labels: ['Approved'] })
  );
  expect(postedBodies()).not.toContain('Routing to an approver for review');
});

test('check_run.success: current-head merge falls back to the listed PR when refresh fails', async () => {
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

  const { ctx } = prepareUnknownStandaloneCurrentHeadCase({
    prNumber: 5011,
    sha: 'sha-current-head-refresh-failed',
    fileName: 'resources/product-current-head-refresh-failed.yaml',
    resourceName: 'product-current-head-refresh-failed',
    config: cfg,
    reviews: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-29T09:46:00Z',
        commit_id: 'sha-current-head-refresh-failed',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @configuredApprover',
      },
    ],
  });

  ctx.octokit.pulls.get.mockRejectedValueOnce(new Error('refresh failed'));
  runApprovalHook.mockResolvedValueOnce({ status: 'unknown', message: 'manual review required' } as any);

  await handlers['check_run.completed'][0](ctx);

  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 5011, mergeMethod: 'squash' })
  );
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) => String(call[1] ?? '') === 'failed to refresh pull request')
  ).toBe(true);
});

test('check_run.success: unknown approval with bot current-head changes requested routes to review', async () => {
  const cfg = {
    requests: {
      product: {
        folderName: 'resources',
        approvers: ['configuredApprover'],
      },
    },
    workflow: {
      labels: {
        global: ['registry-bot'],
        approvalRequested: ['needs-review'],
        approvalSuccessful: ['Approved'],
      },
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const { ctx } = prepareUnknownStandaloneCurrentHeadCase({
    prNumber: 502,
    sha: 'sha-current-head-manual-bot-blocked',
    fileName: 'resources/product-current-head-manual-bot-blocked.yaml',
    resourceName: 'product-current-head-manual-bot-blocked',
    config: cfg,
    reviews: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-29T09:46:00Z',
        commit_id: 'sha-current-head-manual-bot-blocked',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @configuredApprover',
      },
      {
        id: 2,
        state: 'CHANGES_REQUESTED',
        submitted_at: '2026-04-29T09:47:00Z',
        commit_id: 'sha-current-head-manual-bot-blocked',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @configuredApprover',
      },
    ],
  });
  ctx.octokit.issues.get.mockResolvedValue({
    data: {
      labels: [{ name: 'needs-review' }],
      assignees: [],
    },
  });

  runApprovalHook.mockResolvedValueOnce({ status: 'unknown', message: 'manual review required' } as any);

  await handlers['check_run.completed'][0](ctx);

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
  expect(setStateLabel).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 502 }),
    expect.objectContaining({ number: 502 }),
    'review'
  );
  expect(postedBodies()).toContain('Routing to an approver for review');
});

test('check_run.success: unknown approval with unauthorized bot current-head review routes to review', async () => {
  const cfg = {
    requests: {
      product: {
        folderName: 'resources',
        approvers: ['configuredApprover'],
      },
    },
    workflow: {
      labels: {
        global: ['registry-bot'],
        approvalRequested: ['needs-review'],
        approvalSuccessful: ['Approved'],
      },
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const { ctx } = prepareUnknownStandaloneCurrentHeadCase({
    prNumber: 503,
    sha: 'sha-current-head-manual-bot-unauthorized',
    fileName: 'resources/product-current-head-manual-bot-unauthorized.yaml',
    resourceName: 'product-current-head-manual-bot-unauthorized',
    config: cfg,
    reviews: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-29T09:46:00Z',
        commit_id: 'sha-current-head-manual-bot-unauthorized',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @outsider',
      },
    ],
  });

  runApprovalHook.mockResolvedValueOnce({ status: 'unknown', message: 'manual review required' } as any);

  await handlers['check_run.completed'][0](ctx);

  expect(tryMergeIfGreen).not.toHaveBeenCalled();
  expect(setStateLabel).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 503 }),
    expect.objectContaining({ number: 503 }),
    'review'
  );
  expect(postedBodies()).toContain('manual review required');
  expect(postedBodies()).toContain('Routing to an approver for review');
});

test('check_run.success: unknown approval handover only adds assignees missing from the issue', async () => {
  const cfg = {
    requests: {
      product: {
        folderName: 'resources',
        approvers: ['configuredApprover'],
      },
    },
    workflow: {
      labels: {
        global: ['registry-bot'],
        approvalRequested: ['needs-review'],
        approvalSuccessful: ['Approved'],
      },
      approvers: [],
    },
  };

  const { app, handlers } = mkApp();
  requestHandler(app);

  const { ctx } = prepareUnknownStandaloneCurrentHeadCase({
    prNumber: 504,
    sha: 'sha-current-head-handover-missing-assignee',
    fileName: 'resources/product-current-head-handover-missing-assignee.yaml',
    resourceName: 'product-current-head-handover-missing-assignee',
    config: cfg,
    reviews: [
      {
        id: 1,
        state: 'APPROVED',
        submitted_at: '2026-04-29T09:46:00Z',
        commit_id: 'sha-current-head-handover-missing-assignee',
        user: { login: 'my-registry-bot[bot]' },
        body: 'Approved by @outsider',
      },
    ],
  });
  ctx.octokit.issues.get
    .mockResolvedValueOnce({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'reviewerA' }],
      },
    })
    .mockResolvedValueOnce({
      data: {
        labels: [{ name: 'needs-review' }],
        assignees: [{ login: 'reviewerA' }],
      },
    });

  runApprovalHook.mockResolvedValueOnce({
    status: 'unknown',
    message: 'manual review required',
    approvers: ['reviewerA', 'ReviewerB'],
  } as any);

  await handlers['check_run.completed'][0](ctx);

  expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith(
    expect.objectContaining({ owner: 'o1', repo: 'r1', issue_number: 504, assignees: ['ReviewerB'] })
  );
  expect(postedBodies()).toContain('Routing to an approver for review');
});
