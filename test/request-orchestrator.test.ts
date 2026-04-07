/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { test, expect, beforeAll, beforeEach, jest } from '@jest/globals';

process.env.DEBUG_NS = '0';

type IssueParams = { owner: string; repo: string; issue_number: number };
type CollapseBotCommentsByPrefix = (
  ctx: unknown,
  params: IssueParams,
  opts: { perPage?: number; tagPrefix: string; keepTags?: string[]; collapseBody?: string; classifier?: string }
) => Promise<void>;

const collapseBotCommentsByPrefix = jest.fn() as unknown as jest.MockedFunction<CollapseBotCommentsByPrefix>;
const setStateLabel = jest.fn(async (_ctx: any, _param1: any, _param2: any, _param3: any) => {});
const ensureAssigneesOnce = jest.fn(async (_ctx: any, _param1: any, _param2: any, _param3: any) => {});
const postOnce = jest.fn(async (..._args: any[]) => {});

const loadTemplate = jest.fn(async () => ({}));
const parseForm = jest.fn(() => ({}));
const validateRequestIssue = jest.fn(async () => ({}));
const calcSnapshotHash = jest.fn(() => 'h1');
const extractHashFromPrBody = jest.fn(() => 'h1');
const findOpenIssuePrs = jest.fn(async () => []);
const createRequestPr = jest.fn(async () => ({ number: 10 }));
const tryMergeIfGreen = jest.fn(async () => {});
const loadStaticConfig = jest.fn(async () => ({}));
const getDocLinksFromConfig = jest.fn(() => '');

const DEFAULT_CONFIG = {
  workflow: {
    labels: {
      routingLabelPrefix: 'registry-bot:',
      global: [],
      approvalRequested: [],
      approvalSuccessful: [],
      autoMergeCandidate: null,
    },
    approvers: [],
  },
} as any;

let requestHandler: any;

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

function mkIssueContext(args: {
  action: 'opened' | 'edited' | 'reopened';
  issue: any;
  changes?: any;
  owner?: string;
  repo?: string;
  config?: any;
  withCachedConfig?: boolean;
  sender?: any;
}) {
  const owner = args.owner ?? 'o';
  const repo = args.repo ?? 'r';
  const issue = args.issue ?? { number: 1 };

  const ctx: any = {
    name: `issues.${args.action}`,
    payload: {
      action: args.action,
      issue,
      ...(args.changes ? { changes: args.changes } : {}),
      ...(args.sender ? { sender: args.sender } : {}),
    },
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
        get: jest.fn(async () => ({ data: issue })),
        update: jest.fn(async () => ({})),
        addLabels: jest.fn(async () => ({})),
        removeLabel: jest.fn(async () => ({})),
      },
      pulls: {
        update: jest.fn(async () => ({})),
        list: jest.fn(async () => ({ data: [] })),
      },
      git: {
        deleteRef: jest.fn(async () => ({})),
      },
      repos: {
        getContent: jest.fn(async () => ({})),
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
    title: 'Product Request',
    body: [],
    labels: [],
    _meta: {
      requestType: 'product',
      root: 'resources',
      schema: '.github/registry-bot/schemas/sample.json',
      path: '.github/ISSUE_TEMPLATE/sample.md',
    },
  });

  parseForm.mockReturnValue({ 'product-id': 'ABC' });

  validateRequestIssue.mockResolvedValue({
    errors: [],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '',
    formData: { 'product-id': 'ABC' },
    template: null,
    namespace: 'ABC',
    nsType: 'product',
  });

  calcSnapshotHash.mockReturnValue('h1');
  findOpenIssuePrs.mockResolvedValue([]);
});

test('issues.edited: skips when changes do not include body or labels', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.edited'][0];

  const ctx = mkIssueContext({
    action: 'edited',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    changes: { title: { from: 'x' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(loadTemplate).not.toHaveBeenCalled();
  expect(validateRequestIssue).not.toHaveBeenCalled();
  expect(postOnce).not.toHaveBeenCalled();
});

test('issues.edited: ignores bot sender to prevent loops', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  loadTemplate.mockResolvedValue({
    _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 's' },
    title: 'T',
  });

  const ctx: any = mkIssueContext({
    action: 'edited',
    sender: { type: 'Bot', login: 'my-registry-bot[bot]' },
    changes: { body: { from: 'old' } }, // ensure it is NOT skipped by "no body/labels changed"
    issue: {
      number: 1,
      state: 'open',
      body: 'x',
      labels: ['route-1'],
      user: { login: 'alice' },
    },
  });

  await handlers['issues.edited'][0](ctx);

  expect(loadTemplate).not.toHaveBeenCalled();
  expect(parseForm).not.toHaveBeenCalled();
  expect(validateRequestIssue).not.toHaveBeenCalled();
});

test('issues.opened: adds routing lock marker when exactly one routing label is detected', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  loadTemplate.mockImplementation(async function (...args: any[]) {
    const opts = args[1];
    const labels = Array.isArray(opts?.issueLabels) ? opts.issueLabels : [];
    if (labels.includes('route-1')) {
      return {
        _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 's' },
        title: 'T',
      };
    }
    throw new Error('no routing label found');
  });

  parseForm.mockReturnValue({ identifier: 'x' });

  const ctx: any = mkIssueContext({
    action: 'opened',
    issue: {
      number: 1,
      state: 'closed', // early exit after routing-lock logic
      body: 'body',
      labels: ['route-1'],
      user: { login: 'alice' },
    },
  });

  await handlers['issues.opened'][0](ctx);

  expect(ctx.octokit.issues.update).toHaveBeenCalledWith(
    expect.objectContaining({
      issue_number: 1,
      body: expect.stringContaining('nsreq:routing-lock'),
    })
  );

  const bodyArg = ctx.octokit.issues.update.mock.calls[0][0].body;
  expect(bodyArg).toContain('"expected":"route-1"');
});

test('issues.opened: strips routing-lock marker before parsing and does not rewrite marker if already correct', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  loadTemplate.mockImplementation(async (...args: any[]) => {
    const opts = args[0] || {};
    const labels = Array.isArray(opts.issueLabels) ? opts.issueLabels : [];
    if (labels.includes('route-1')) {
      return {
        _meta: { requestType: 'systemNamespace', root: '/data/namespaces', schema: 's' },
        title: 'T',
      };
    }
    throw new Error('no routing label found');
  });

  parseForm.mockImplementation((...args: any[]) => {
    const body = args[0];
    expect(body).not.toContain('nsreq:routing-lock');
    return { identifier: 'x' };
  });

  const issueBody = 'my-form-body\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->\n';

  const ctx: any = mkIssueContext({
    action: 'opened',
    issue: {
      number: 1,
      state: 'closed',
      body: issueBody,
      labels: ['route-1'],
      user: { login: 'alice' },
    },
  });

  await handlers['issues.opened'][0](ctx);

  // marker already correct => no rewrite
  expect(ctx.octokit.issues.update).not.toHaveBeenCalled();
});

test('issues.opened: loads static config via loadStaticConfig when not cached', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];

  const ctx = mkIssueContext({
    action: 'opened',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    withCachedConfig: false,
  });

  await handler(ctx);

  expect(loadStaticConfig).toHaveBeenCalled();
  expect(ctx.resourceBotConfig).toBeTruthy();
});

test('issues.opened: template load error -> posts config error and sets author state', async () => {
  loadTemplate.mockRejectedValueOnce(new Error('boom'));

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];

  const ctx = mkIssueContext({
    action: 'opened',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  const body = postOnce.mock.calls[0][2] as string;
  expect(body).toContain('Configuration error: unable to load request template');

  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'author');
});

test('issues.opened: routing error triggers label refresh and retries loadTemplate', async () => {
  loadTemplate.mockRejectedValueOnce(new Error('no routing label found')).mockResolvedValueOnce({
    title: 'Product Request',
    body: [],
    labels: [],
    _meta: { requestType: 'product', root: 'resources', schema: 'x', path: 'p' },
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];

  const ctx = mkIssueContext({
    action: 'opened',
    issue: {
      number: 1,
      title: 't',
      body: 'b',
      labels: [],
      user: { login: 'u' },
    },
    withCachedConfig: true,
  });

  ctx.octokit.issues.get.mockResolvedValueOnce({
    data: { ...ctx.payload.issue, labels: [{ name: 'registry-bot:product' }] },
  });

  parseForm.mockReturnValueOnce({}); // so it exits early after template load
  await handler(ctx);

  expect(loadTemplate).toHaveBeenCalledTimes(2);
  expect(ctx.octokit.issues.get).toHaveBeenCalled();
});

test('issues.opened: validation errors -> posts and sets author state', async () => {
  validateRequestIssue.mockResolvedValueOnce({
    errors: ['x'],
    errorsGrouped: null,
    errorsFormatted: '',
    errorsFormattedSingle: '- x',
    formData: { 'product-id': 'ABC' },
    template: null,
    namespace: 'ABC',
    nsType: 'product',
  });

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];

  const ctx = mkIssueContext({
    action: 'opened',
    issue: { number: 1, title: 't', body: 'b', labels: [], user: { login: 'u' } },
    withCachedConfig: true,
  });

  await handler(ctx);

  expect(postOnce).toHaveBeenCalled();
  const body = postOnce.mock.calls[0][2] as string;
  expect(body).toContain('## Detected issues');

  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'author');
});

test('issues.opened: success -> normalizes title and hands over with labels and snapshot marker', async () => {
  const cfg = {
    workflow: {
      approvers: ['alice', 'bob'],
      labels: {
        global: ['registry-bot'],
        approvalRequested: ['needs-review'],
        approvalSuccessful: ['approved-label'],
      },
    },
  };

  getDocLinksFromConfig.mockReturnValueOnce('Docs');

  const { app, handlers } = mkApp();
  requestHandler(app);

  const handler = handlers['issues.opened'][0];

  const issue = {
    number: 1,
    title: 'Old',
    body: 'Body',
    labels: [{ name: 'approved-label' }],
    user: { login: 'author' },
  };

  const ctx = mkIssueContext({
    action: 'opened',
    issue,
    withCachedConfig: true,
    config: cfg,
  });

  parseForm.mockReturnValueOnce({ 'product-id': 'ABC' });

  await handler(ctx);

  expect(ctx.octokit.issues.update).toHaveBeenCalledWith(expect.objectContaining({ title: 'Product Request: ABC' }));

  expect(setStateLabel).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), 'review');

  expect(ensureAssigneesOnce).toHaveBeenCalledWith(ctx, expect.anything(), expect.anything(), ['alice', 'bob']);

  expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith(
    expect.objectContaining({ labels: ['registry-bot', 'needs-review'] })
  );

  expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith(expect.objectContaining({ name: 'approved-label' }));

  expect(postOnce).toHaveBeenCalled();
  const body = postOnce.mock.calls[0][2] as string;
  expect(body).toContain('### ✅ No issues detected');
  expect(body).toContain('<!-- nsreq:snapshot:h1 -->');
});
