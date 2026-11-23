import type { Probot } from 'probot';
/* eslint-disable require-await */
import { beforeAll, beforeEach, describe, expect, type MockedFunction, test, vi } from 'vitest';

type IssueParams = { owner: string; repo: string; issue_number: number };

type LabelLike = string | { name?: string | null };

type IssueLike = {
  number: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: LabelLike[];
  user?: { login?: string | null } | null;
};

type SenderLike = { type?: string | null; login?: string | null };

type LoggerFn = (obj: unknown, msg?: string) => void;
type LoggerLike = {
  debug: MockedFunction<LoggerFn>;
  info: MockedFunction<LoggerFn>;
  warn: MockedFunction<LoggerFn>;
  error: MockedFunction<LoggerFn>;
};

type OctokitLike = {
  rest: {
    issues: {
      get: MockedFunction<(p: IssueParams) => Promise<{ data: IssueLike }>>;
      addLabels: MockedFunction<(p: IssueParams & { labels: string[] }) => Promise<void>>;
      removeLabel: MockedFunction<(p: IssueParams & { name: string }) => Promise<void>>;
      update: MockedFunction<(p: IssueParams & { title: string }) => Promise<void>>;
    };
    pulls: {
      list: MockedFunction<
        (p: {
          owner: string;
          repo: string;
          state: string;
          per_page: number;
          page: number;
        }) => Promise<{ data: unknown[] }>
      >;
      update: MockedFunction<(p: { owner: string; repo: string; pull_number: number; state: string }) => Promise<void>>;
    };
    git: {
      deleteRef: MockedFunction<(p: { owner: string; repo: string; ref: string }) => Promise<void>>;
    };
    repos: {
      getContent: MockedFunction<(p: { owner: string; repo: string; path: string }) => Promise<void>>;
    };
  };
};

type StaticConfig = {
  workflow?: {
    labels?: Record<string, unknown>;
    approvers?: unknown;
    approversPool?: unknown;
  };
  requests?: Record<string, unknown>;
};

type TemplateLike = {
  title?: string | null;
  name?: string | null;
  body?: unknown[];
  labels?: unknown[];
  _meta?: {
    requestType?: string;
    root?: string;
    schema?: string;
    path?: string;
  };
};

type LoadTemplateArgs = {
  owner: string;
  repo: string;
  templatePath?: string;
  issueLabels?: unknown;
  issueTitle?: string;
};

type LoadTemplateFn = (context: unknown, args: LoadTemplateArgs) => Promise<TemplateLike>;
type ParseFormFn = (body: string, template: TemplateLike) => Record<string, string>;
type PostOnceFn = (
  context: unknown,
  params: IssueParams,
  body: string,
  options?: { minimizeTag?: string }
) => Promise<void>;
type CollapseBotCommentsByPrefix = (
  ctx: unknown,
  params: IssueParams,
  opts: { perPage?: number; tagPrefix: string; keepTags?: string[]; collapseBody?: string; classifier?: string }
) => Promise<void>;
type SetStateLabelFn = (
  context: unknown,
  params: IssueParams,
  issue: IssueLike,
  state: 'author' | 'review'
) => Promise<void>;
type EnsureAssigneesOnceFn = (
  context: unknown,
  params: IssueParams,
  issue: IssueLike,
  assignees: string[]
) => Promise<void>;

type Handler = (ctx: unknown) => Promise<void>;

type AppLike = {
  on: (events: string | string[], handler: Handler) => void;
  log: {
    warn: MockedFunction<(obj: unknown, msg?: string) => void>;
  };
};

// Mocks
const setStateLabel = vi.fn() as unknown as MockedFunction<SetStateLabelFn>;
const ensureAssigneesOnce = vi.fn() as unknown as MockedFunction<EnsureAssigneesOnceFn>;
const postOnce = vi.fn() as unknown as MockedFunction<PostOnceFn>;
const collapseBotCommentsByPrefix = vi.fn() as unknown as MockedFunction<CollapseBotCommentsByPrefix>;
const loadTemplate = vi.fn() as unknown as MockedFunction<LoadTemplateFn>;
const parseForm = vi.fn() as unknown as MockedFunction<ParseFormFn>;

// Not used directly in these tests, but request handler imports them
const runApprovalHook = vi.fn(async () => false);
const validateRequestIssue = vi.fn() as unknown as MockedFunction<
  (
    context: unknown,
    params: IssueParams,
    issue: IssueLike,
    options?: { template?: TemplateLike; formData?: Record<string, string> }
  ) => Promise<{
    errors: string[];
    errorsGrouped?: unknown;
    errorsFormatted: string;
    errorsFormattedSingle: string;
    formData?: Record<string, string>;
    template?: TemplateLike;
    namespace: string;
    nsType: string;
  }>
>;
const calcSnapshotHash = vi.fn() as unknown as MockedFunction<
  (formData: Record<string, string>, template: TemplateLike, rawBody: string) => string
>;
const extractHashFromPrBody = vi.fn() as unknown as MockedFunction<(body: string) => string>;
const findOpenIssuePrs = vi.fn() as unknown as MockedFunction<
  (
    context: unknown,
    repo: { owner: string; repo: string },
    issueNumber: number
  ) => Promise<{ number: number; body?: string | null; head: { ref: string; sha: string } }[]>
>;
const createRequestPr = vi.fn() as unknown as MockedFunction<
  (
    context: unknown,
    repo: { owner: string; repo: string },
    issue: IssueLike,
    formData: Record<string, string>,
    options?: { template?: TemplateLike }
  ) => Promise<{ number: number }>
>;
const tryMergeIfGreen = vi.fn() as unknown as MockedFunction<
  (
    context: unknown,
    args: {
      owner: string;
      repo: string;
      prNumber: number;
      mergeMethod: 'merge' | 'squash' | 'rebase';
      prData: { number: number; body?: string | null; head: { ref: string; sha: string } };
    }
  ) => Promise<void>
>;
const loadStaticConfig = vi.fn() as unknown as MockedFunction<
  (
    context: unknown,
    opts: { validate: boolean; updateIssue: boolean }
  ) => Promise<{ config: StaticConfig; hooks: unknown; hooksSource?: string }>
>;
const getDocLinksFromConfig = vi.fn() as unknown as MockedFunction<(cfg: StaticConfig) => string>;

const TEST_WORKFLOW_LABELS = {
  authorAction: 'Requester Action',
  approverAction: 'Review Pending',
  parentOwnerAction: 'Parent Owner Action',
  approvalRequested: ['Review Pending'],
  approvalSuccessful: ['Approved'],
  approvalRejected: ['Rejected'],
};

function withWorkflowLabels<T extends StaticConfig>(cfg: T): T {
  cfg.workflow = cfg.workflow || {};
  cfg.workflow.labels = {
    ...TEST_WORKFLOW_LABELS,
    ...(cfg.workflow.labels || {}),
  };
  return cfg;
}

// Provide a DEFAULT_CONFIG for the module import
const DEFAULT_CONFIG: StaticConfig = withWorkflowLabels({
  workflow: {
    labels: {},
    approvers: [],
  },
  requests: {},
});

vi.doMock('../src/handlers/request/state.js', () => ({
  setStateLabel,
  ensureAssigneesOnce,
}));

vi.doMock('../src/handlers/request/comments.js', () => ({
  postOnce,
  collapseBotCommentsByPrefix,
}));

vi.doMock('../src/handlers/request/template.js', () => ({
  loadTemplate,
  parseForm,
}));

vi.doMock('../src/handlers/request/validation/run.js', () => ({
  validateRequestIssue,
  runApprovalHook,
}));

vi.doMock('../src/handlers/request/pr/snapshot.js', () => ({
  calcSnapshotHash,
  extractHashFromPrBody,
  findOpenIssuePrs,
}));

vi.doMock('../src/handlers/request/pr/create.js', () => ({
  createRequestPr,
}));

vi.doMock('../src/lib/auto-merge.js', () => ({
  tryMergeIfGreen,
}));

vi.doMock('../src/config.js', () => ({
  loadStaticConfig,
  DEFAULT_CONFIG,
}));

vi.doMock('../src/handlers/request/constants.js', () => ({
  getDocLinksFromConfig,
}));

// ---- Import after mocks ----
let requestHandler: (app: Probot) => void;

const DEFAULT_TEMPLATE: TemplateLike = {
  title: 'Request',
  body: [],
  name: 'Request',
  _meta: {
    requestType: 'systemNamespace',
    root: 'data/namespaces',
    schema: 'schema.json',
    path: '.github/ISSUE_TEMPLATE/foo.yml',
  },
};

function mkLogger(): LoggerLike {
  return {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  };
}

function mkOctokit(): OctokitLike {
  return {
    rest: {
      issues: {
        get: vi.fn((_p: IssueParams) => Promise.resolve({ data: { number: 1, labels: [] } })),
        addLabels: vi.fn((_p: IssueParams & { labels: string[] }) => Promise.resolve()),
        removeLabel: vi.fn((_p: IssueParams & { name: string }) => Promise.resolve()),
        update: vi.fn((_p: IssueParams & { title: string }) => Promise.resolve()),
      },
      pulls: {
        list: vi.fn((_p) => Promise.resolve({ data: [] })),
        update: vi.fn((_p) => Promise.resolve()),
      },
      git: {
        deleteRef: vi.fn((_p) => Promise.resolve()),
      },
      repos: {
        getContent: vi.fn((_p) => Promise.resolve()),
      },
    },
  };
}

function mkApp(): { app: AppLike; handlers: Record<string, Handler[]> } {
  const handlers: Record<string, Handler[]> = {};

  const app: AppLike = {
    log: { warn: vi.fn() },
    on: (events: string | string[], handler: Handler) => {
      const list = Array.isArray(events) ? events : [events];
      for (const e of list) {
        (handlers[e] ??= []).push(handler);
      }
    },
  };

  return { app, handlers };
}

type CtxLike = {
  name: string;
  payload: {
    action?: string;
    issue: IssueLike;
    sender: SenderLike;
    label?: { name?: string | null } | string;
  };
  octokit: OctokitLike;
  issue: () => IssueParams;
  log: LoggerLike;
  resourceBotConfig?: StaticConfig;
  resourceBotHooks?: unknown;
  resourceBotHooksSource?: string | null;
};

function mkCtx(args: {
  eventName: 'issues.labeled' | 'issues.unlabeled';
  action: 'labeled' | 'unlabeled';
  issue: IssueLike;
  sender: SenderLike;
  labelName: string;
  config: StaticConfig;
  octokit?: OctokitLike;
}): CtxLike {
  const octokit = args.octokit ?? mkOctokit();
  return {
    name: args.eventName,
    payload: {
      action: args.action,
      issue: args.issue,
      sender: args.sender,
      label: { name: args.labelName },
    },
    octokit,
    issue: () => ({ owner: 'o', repo: 'r', issue_number: args.issue.number }),
    log: mkLogger(),
    // Cache config to skip loadStaticConfig
    resourceBotConfig: withWorkflowLabels(args.config),
    resourceBotHooks: null,
    resourceBotHooksSource: 'test',
  };
}

beforeAll(async () => {
  const mod = await import('../src/handlers/request/index.js');
  requestHandler = mod.default;
});

beforeEach(() => {
  vi.clearAllMocks();

  // Defaults: make it a request issue so labeled/unlabeled handler continues.
  loadTemplate.mockResolvedValue(DEFAULT_TEMPLATE);
  parseForm.mockReturnValue({ namespace: 'example' });

  // Keep unrelated mocks harmless
  validateRequestIssue.mockResolvedValue({
    errors: [],
    errorsGrouped: {},
    errorsFormatted: '',
    errorsFormattedSingle: '',
    namespace: 'example',
    nsType: 'system',
  });
  calcSnapshotHash.mockReturnValue('hash');
  extractHashFromPrBody.mockReturnValue('hash');
  findOpenIssuePrs.mockResolvedValue([]);
  createRequestPr.mockResolvedValue({ number: 123 });
  tryMergeIfGreen.mockResolvedValue(undefined);
  loadStaticConfig.mockResolvedValue({ config: DEFAULT_CONFIG, hooks: null, hooksSource: 'test' });
  getDocLinksFromConfig.mockReturnValue('');
});

describe('request handler label guards (workflow-label-lock + routing label lock)', () => {
  test('issues.labeled: ignores label changes made by bots (prevents loops)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: { number: 1, title: 'T', body: 'B', labels: [{ name: 'x' }], user: { login: 'u' } },
      sender: { type: 'Bot', login: 'some-bot[bot]' },
      labelName: 'Review Pending',
      config: {
        workflow: {
          approvers: [],
          labels: { approvalSuccessful: ['Approved'], approvalRequested: ['Review Pending'] },
        },
        requests: {},
      },
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(loadTemplate).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(ctx.octokit.rest.issues.addLabels).not.toHaveBeenCalled();
  });

  test('issues.labeled: reverts manual add of locked workflow label from config', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: [],
        labels: {
          authorAction: 'Requester Action',
          approverAction: 'Review Pending',
          approvalRequested: ['Needs Review'],
          approvalSuccessful: ['Approved'],
          autoMergeCandidate: 'auto-merge-candidate',
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 2,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Needs Review' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'Needs Review',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledTimes(1);
    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 2,
      name: 'Needs Review',
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const body = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(body)).toContain('Workflow labels from config');
    expect(opts?.minimizeTag).toBe('nsreq:workflow-label-lock');

    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.unlabeled: re-adds locked workflow label if a user removes it', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: [],
        labels: {
          approvalRequested: ['Needs Review'],
          approvalSuccessful: ['Approved'],
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.unlabeled',
      action: 'unlabeled',
      issue: {
        number: 3,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'Needs Review',
      config: cfg,
      octokit,
    });

    await handlers['issues.unlabeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.addLabels).toHaveBeenCalledTimes(1);
    expect(octokit.rest.issues.addLabels).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 3,
      labels: ['Needs Review'],
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const body = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(body)).toContain('Workflow labels from config');
    expect(opts?.minimizeTag).toBe('nsreq:workflow-label-lock');

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: manual "Approved" label uses label-guard (not workflow-label-lock)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: [],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 4,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Approved' }, { name: 'Needs Review' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'Approved',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 4,
      name: 'Approved',
    });

    expect(setStateLabel).toHaveBeenCalledTimes(1);
    expect(setStateLabel.mock.calls[0]?.[3]).toBe('review');

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Approved label change reverted');
    expect(opts?.minimizeTag).toBe('nsreq:label-guard');
  });

  test('issues.unlabeled: global approver may remove Approved label without bot rollback', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: ['alice'],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.unlabeled',
      action: 'unlabeled',
      issue: {
        number: 41,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Needs Review' }],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'Approved',
      config: cfg,
      octokit,
    });

    await handlers['issues.unlabeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.unlabeled: global approversPool user may remove Approved label without bot rollback', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approversPool: ['alice'],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.unlabeled',
      action: 'unlabeled',
      issue: {
        number: 45,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Needs Review' }],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'Approved',
      config: cfg,
      octokit,
    });

    await handlers['issues.unlabeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: request-type approversPool user may add Approved label without bot rollback', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: ['bob'],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
        },
      },
      requests: {
        systemNamespace: {
          approversPool: ['alice'],
        },
      },
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 46,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Approved' }, { name: 'Needs Review' }],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'Approved',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.unlabeled: global approver may remove locked workflow label without bot rollback', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: ['alice'],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
          authorAction: 'Requester Action',
          approverAction: 'Review Pending',
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.unlabeled',
      action: 'unlabeled',
      issue: {
        number: 42,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'Needs Review',
      config: cfg,
      octokit,
    });

    await handlers['issues.unlabeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: request-type approver may add Approved label without bot rollback', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: ['bob'],
        labels: {
          approvalSuccessful: ['Approved'],
          approvalRequested: ['Needs Review'],
        },
      },
      requests: {
        systemNamespace: {
          approvers: ['alice'],
        },
      },
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 43,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Approved' }, { name: 'Needs Review' }],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'Approved',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: configured approver still cannot bypass routing label lock', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: ['alice'],
        labels: {
          approvalSuccessful: ['Approved'],
        },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 44,
        title: 'T',
        body: 'B',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'requester' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 44,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'requester' },
      },
      sender: { type: 'User', login: 'alice' },
      labelName: 'route-2',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 44,
      name: 'route-2',
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Routing label is locked to "route-1"');
    expect(opts?.minimizeTag).toBe('nsreq:routing-label-lock');

    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: manual "Rejected" label on open issue gets rolled back', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        approvers: [],
        labels: { approvalSuccessful: ['Approved'] },
      },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 5,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'Rejected' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'Rejected',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 5,
      name: 'Rejected',
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Rejected state is managed automatically');
    expect(opts?.minimizeTag).toBe('nsreq:label-guard');
  });

  test('issues.unlabeled: restores locked routing label and removes other routing labels (swap attack)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 6,
        title: 'T',
        body: 'B',
        labels: [{ name: 'route-2' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.unlabeled',
      action: 'unlabeled',
      issue: {
        number: 6,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        // swap attack: expected label removed, other routing label present
        labels: [{ name: 'route-2' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-1',
      config: cfg,
      octokit,
    });

    await handlers['issues.unlabeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 6,
      name: 'route-2',
    });

    expect(octokit.rest.issues.addLabels).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 6,
      labels: ['route-1'],
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Routing label is locked to "route-1"');
    expect(opts?.minimizeTag).toBe('nsreq:routing-label-lock');
  });

  test('issues.labeled: removes extra routing label when routing lock is present', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 7,
        title: 'T',
        body: 'B',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 7,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-2',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 7,
      name: 'route-2',
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Routing label is locked to "route-1"');
    expect(opts?.minimizeTag).toBe('nsreq:routing-label-lock');

    // Should return early, no further label-guard actions
    expect(setStateLabel).not.toHaveBeenCalled();
  });

  test('issues.labeled: routing lock falls back to payload labels when label refresh fails', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockRejectedValueOnce(new Error('boom'));
    octokit.rest.issues.get
      .mockResolvedValueOnce({
        data: {
          number: 9,
          title: 'T',
          body: 'B',
          labels: [{ name: 'route-1' }, { name: 'route-2' }],
          user: { login: 'alice' },
        },
      })
      .mockRejectedValueOnce(new Error('label refresh failed'));

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 9,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-2',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 9,
      name: 'route-2',
    });
    expect(postOnce).toHaveBeenCalledTimes(1);
  });

  test('issues.labeled: skips routing-lock notice when the changed label is not a routing label', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 9,
        title: 'T',
        body: 'B',
        labels: [{ name: 'route-1' }, { name: 'route-2' }, { name: 'other' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 9,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-2' }, { name: 'other' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'other',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 9,
      name: 'route-2',
    });
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: returns early when payload label is malformed', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 10,
        title: 'T',
        body: 'B',
        state: 'open',
        labels: [{ name: 'route-1' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-1',
      config: cfg,
      octokit,
    });
    // biome-ignore lint/suspicious/noExplicitAny: Existing exception retained during tooling migration.
    (ctx.payload as any).label = 42;

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.get).not.toHaveBeenCalled();
    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: closed non-approved issues tolerate label refresh failures while clearing progress labels', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
          authorAction: 'Requester Action',
          approverAction: 'Review Pending',
        },
        approvers: [],
      },
      requests: {},
    };

    const octokit = mkOctokit();

    octokit.rest.issues.get.mockRejectedValue(new Error('label refresh failed'));

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 11,
        title: 'T',
        body: 'B',
        state: 'closed',
        labels: [{ name: 'Review Pending' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'Review Pending',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.addLabels).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 11,
      labels: ['Rejected'],
    });

    expect(octokit.rest.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 11,
      name: 'Review Pending',
    });
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: template load failure while checking routing label returns without lock notice', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();

    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 12,
        title: 'T',
        body: 'B',
        labels: [{ name: 'route-1' }, { name: 'route-bad' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];

      if (lbls.length === 1 && lbls[0] === 'route-1') return DEFAULT_TEMPLATE;
      if (lbls.length === 1 && lbls[0] === 'route-bad') throw new Error('template lookup failed');
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');

      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 12,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-bad' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-bad',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: malformed routing lock in refreshed issue disables lock handling', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 13,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = not-json -->',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && (lbls[0] === 'route-1' || lbls[0] === 'route-2')) return DEFAULT_TEMPLATE;
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 13,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = not-json -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-2' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-2',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: refreshed routing label template lookup failure is treated as non-routing', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    const octokit = mkOctokit();
    octokit.rest.issues.get.mockResolvedValueOnce({
      data: {
        number: 14,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        labels: [{ name: 'route-1' }, { name: 'route-bad' }],
        user: { login: 'alice' },
      },
    });

    loadTemplate.mockImplementation(async (_ctx: unknown, args: LoadTemplateArgs) => {
      const lbls = Array.isArray(args.issueLabels) ? args.issueLabels.map(String) : [];
      if (lbls.length === 1 && lbls[0] === 'route-1') return DEFAULT_TEMPLATE;
      if (lbls.length === 1 && lbls[0] === 'route-bad') throw new Error('template lookup failed');
      if (lbls.length > 1) throw new Error('Cannot resolve template: multiple routing label');
      throw new Error('no routing label found');
    });

    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 14,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-1' }, { name: 'route-bad' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-bad',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.rest.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.rest.issues.addLabels).not.toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
  });
});
