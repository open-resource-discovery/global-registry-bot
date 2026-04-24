/* eslint-disable require-await */
import { beforeAll, beforeEach, describe, expect, jest, test } from '@jest/globals';
import type { Probot } from 'probot';

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
  debug: jest.MockedFunction<LoggerFn>;
  info: jest.MockedFunction<LoggerFn>;
  warn: jest.MockedFunction<LoggerFn>;
  error: jest.MockedFunction<LoggerFn>;
};

type OctokitLike = {
  issues: {
    get: jest.MockedFunction<(p: IssueParams) => Promise<{ data: IssueLike }>>;
    addLabels: jest.MockedFunction<(p: IssueParams & { labels: string[] }) => Promise<void>>;
    removeLabel: jest.MockedFunction<(p: IssueParams & { name: string }) => Promise<void>>;
    update: jest.MockedFunction<(p: IssueParams & { title: string }) => Promise<void>>;
  };
  pulls: {
    list: jest.MockedFunction<
      (p: {
        owner: string;
        repo: string;
        state: string;
        per_page: number;
        page: number;
      }) => Promise<{ data: unknown[] }>
    >;
    update: jest.MockedFunction<
      (p: { owner: string; repo: string; pull_number: number; state: string }) => Promise<void>
    >;
  };
  git: {
    deleteRef: jest.MockedFunction<(p: { owner: string; repo: string; ref: string }) => Promise<void>>;
  };
  repos: {
    getContent: jest.MockedFunction<(p: { owner: string; repo: string; path: string }) => Promise<void>>;
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
    warn: jest.MockedFunction<(obj: unknown, msg?: string) => void>;
  };
};

// Mocks
const setStateLabel = jest.fn() as unknown as jest.MockedFunction<SetStateLabelFn>;
const ensureAssigneesOnce = jest.fn() as unknown as jest.MockedFunction<EnsureAssigneesOnceFn>;
const postOnce = jest.fn() as unknown as jest.MockedFunction<PostOnceFn>;
const collapseBotCommentsByPrefix = jest.fn() as unknown as jest.MockedFunction<CollapseBotCommentsByPrefix>;
const loadTemplate = jest.fn() as unknown as jest.MockedFunction<LoadTemplateFn>;
const parseForm = jest.fn() as unknown as jest.MockedFunction<ParseFormFn>;

// Not used directly in these tests, but request handler imports them
const runApprovalHook = jest.fn(async () => false);
const validateRequestIssue = jest.fn() as unknown as jest.MockedFunction<
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
const calcSnapshotHash = jest.fn() as unknown as jest.MockedFunction<
  (formData: Record<string, string>, template: TemplateLike, rawBody: string) => string
>;
const extractHashFromPrBody = jest.fn() as unknown as jest.MockedFunction<(body: string) => string>;
const findOpenIssuePrs = jest.fn() as unknown as jest.MockedFunction<
  (
    context: unknown,
    repo: { owner: string; repo: string },
    issueNumber: number
  ) => Promise<{ number: number; body?: string | null; head: { ref: string; sha: string } }[]>
>;
const createRequestPr = jest.fn() as unknown as jest.MockedFunction<
  (
    context: unknown,
    repo: { owner: string; repo: string },
    issue: IssueLike,
    formData: Record<string, string>,
    options?: { template?: TemplateLike }
  ) => Promise<{ number: number }>
>;
const tryMergeIfGreen = jest.fn() as unknown as jest.MockedFunction<
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
const loadStaticConfig = jest.fn() as unknown as jest.MockedFunction<
  (
    context: unknown,
    opts: { validate: boolean; updateIssue: boolean }
  ) => Promise<{ config: StaticConfig; hooks: unknown; hooksSource?: string }>
>;
const getDocLinksFromConfig = jest.fn() as unknown as jest.MockedFunction<(cfg: StaticConfig) => string>;

// Provide a DEFAULT_CONFIG for the module import
const DEFAULT_CONFIG: StaticConfig = {
  workflow: {
    labels: {
      approvalSuccessful: ['Approved'],
    },
    approvers: [],
  },

  requests: {},
};

jest.unstable_mockModule('../src/handlers/request/state.js', () => ({
  setStateLabel,
  ensureAssigneesOnce,
}));

jest.unstable_mockModule('../src/handlers/request/comments.js', () => ({
  postOnce,
  collapseBotCommentsByPrefix,
}));

jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
  loadTemplate,
  parseForm,
}));

jest.unstable_mockModule('../src/handlers/request/validation/run.js', () => ({
  validateRequestIssue,
  runApprovalHook,
}));

jest.unstable_mockModule('../src/handlers/request/pr/snapshot.js', () => ({
  calcSnapshotHash,
  extractHashFromPrBody,
  findOpenIssuePrs,
}));

jest.unstable_mockModule('../src/handlers/request/pr/create.js', () => ({
  createRequestPr,
}));

jest.unstable_mockModule('../src/lib/auto-merge.js', () => ({
  tryMergeIfGreen,
}));

jest.unstable_mockModule('../src/config.js', () => ({
  loadStaticConfig,
  DEFAULT_CONFIG,
}));

jest.unstable_mockModule('../src/handlers/request/constants.js', () => ({
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
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  };
}

function mkOctokit(): OctokitLike {
  return {
    issues: {
      get: jest.fn((_p: IssueParams) => Promise.resolve({ data: { number: 1, labels: [] } })),
      addLabels: jest.fn((_p: IssueParams & { labels: string[] }) => Promise.resolve()),
      removeLabel: jest.fn((_p: IssueParams & { name: string }) => Promise.resolve()),
      update: jest.fn((_p: IssueParams & { title: string }) => Promise.resolve()),
    },
    pulls: {
      list: jest.fn((_p) => Promise.resolve({ data: [] })),
      update: jest.fn((_p) => Promise.resolve()),
    },
    git: {
      deleteRef: jest.fn((_p) => Promise.resolve()),
    },
    repos: {
      getContent: jest.fn((_p) => Promise.resolve()),
    },
  };
}

function mkApp(): { app: AppLike; handlers: Record<string, Handler[]> } {
  const handlers: Record<string, Handler[]> = {};

  const app: AppLike = {
    log: { warn: jest.fn() },
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
    resourceBotConfig: args.config,
    resourceBotHooks: null,
    resourceBotHooksSource: 'test',
  };
}

beforeAll(async () => {
  const mod = await import('../src/handlers/request/index.js');
  requestHandler = mod.default as unknown as (app: Probot) => void;
});

beforeEach(() => {
  jest.clearAllMocks();

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
    expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
    expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledTimes(1);
    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
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

    expect(octokit.issues.addLabels).toHaveBeenCalledTimes(1);
    expect(octokit.issues.addLabels).toHaveBeenCalledWith({
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

    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
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

    expect(octokit.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
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

    expect(octokit.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
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

    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.issues.addLabels).not.toHaveBeenCalled();
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

    expect(octokit.issues.addLabels).not.toHaveBeenCalled();
    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
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

    expect(octokit.issues.removeLabel).not.toHaveBeenCalled();
    expect(octokit.issues.addLabels).not.toHaveBeenCalled();
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
    octokit.issues.get.mockResolvedValueOnce({
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 5,
      name: 'Rejected',
    });

    expect(postOnce).toHaveBeenCalledTimes(1);
    const msg = postOnce.mock.calls[0]?.[2] ?? '';
    const opts = postOnce.mock.calls[0]?.[3];
    expect(String(msg)).toContain('Rejected label change reverted');
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
    octokit.issues.get.mockResolvedValueOnce({
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 6,
      name: 'route-2',
    });

    expect(octokit.issues.addLabels).toHaveBeenCalledWith({
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
    octokit.issues.get.mockResolvedValueOnce({
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
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
    octokit.issues.get
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

    expect(octokit.issues.removeLabel).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 9,
      name: 'route-2',
    });
  });

  test('issues.labeled: malformed routing lock marker is ignored safely', async () => {
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
        body: 'B\n\n<!-- nsreq:routing-lock = {oops} -->',
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

    expect(postOnce).not.toHaveBeenCalled();
  });

  test('issues.labeled: template load failure while checking routing label returns without lock notice', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const cfg: StaticConfig = {
      workflow: { labels: { approvalSuccessful: ['Approved'] } },
      requests: {},
    };

    loadTemplate.mockRejectedValue(new Error('template lookup failed'));

    const octokit = mkOctokit();
    const ctx = mkCtx({
      eventName: 'issues.labeled',
      action: 'labeled',
      issue: {
        number: 11,
        title: 'T',
        body: 'B\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"route-1"} -->',
        state: 'open',
        labels: [{ name: 'route-bad' }],
        user: { login: 'alice' },
      },
      sender: { type: 'User', login: 'bob' },
      labelName: 'route-bad',
      config: cfg,
      octokit,
    });

    await handlers['issues.labeled']?.[0]?.(ctx);

    expect(octokit.issues.addLabels).toHaveBeenCalledWith({
      owner: 'o',
      repo: 'r',
      issue_number: 11,
      labels: ['route-1'],
    });
    expect(postOnce).not.toHaveBeenCalled();
  });
});
