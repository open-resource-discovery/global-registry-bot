import { describe, test, expect, beforeAll, beforeEach, jest } from '@jest/globals';
import type { Probot } from 'probot';

process.env.DEBUG_NS = '1';

type Handler = (ctx: unknown) => Promise<void>;

type AppWarn = (...args: unknown[]) => void;

type AppLike = {
  on: (event: string | string[], handler: Handler) => void;
  log: { warn: jest.MockedFunction<AppWarn> };
};

type IssueLabel = string | { name?: string | null } | null | undefined;

type IssueUser = { login: string };

type Issue = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  labels?: IssueLabel[];
  user?: IssueUser | null;
};

type Comment = {
  body?: string | null;
  user?: IssueUser | null;
};

type Sender = {
  type?: string | null;
  login?: string | null;
};

type Repository = { name: string; owner: { login: string } };

type IssueParams = { owner: string; repo: string; issue_number: number };

type IssuesGet = (args: IssueParams) => Promise<{ data: Issue }>;
type IssuesUpdate = (args: IssueParams & { title: string }) => Promise<void>;
type IssuesAddLabels = (args: IssueParams & { labels: string[] }) => Promise<void>;
type IssuesRemoveLabel = (args: IssueParams & { name: string }) => Promise<void>;

type PullRequest = { number: number; body?: string | null; head: { sha: string; ref: string } };
type PullsList = (args: {
  owner: string;
  repo: string;
  state: string;
  per_page: number;
  page: number;
}) => Promise<{ data: PullRequest[] }>;
type PullsUpdate = (args: {
  owner: string;
  repo: string;
  pull_number: number;
  state: 'open' | 'closed';
}) => Promise<void>;

type ChecksListForRef = (args: {
  owner: string;
  repo: string;
  ref: string;
  per_page?: number;
  page?: number;
}) => Promise<{ data: { check_runs: unknown[] } }>;

type GitDeleteRef = (args: { owner: string; repo: string; ref: string }) => Promise<void>;

type ReposGetContent = (args: { owner: string; repo: string; path: string }) => Promise<{ data: unknown }>;

type Octokit = {
  issues: {
    get: jest.MockedFunction<IssuesGet>;
    update: jest.MockedFunction<IssuesUpdate>;
    addLabels: jest.MockedFunction<IssuesAddLabels>;
    removeLabel: jest.MockedFunction<IssuesRemoveLabel>;
  };
  pulls: {
    list: jest.MockedFunction<PullsList>;
    update: jest.MockedFunction<PullsUpdate>;
  };
  checks: {
    listForRef: jest.MockedFunction<ChecksListForRef>;
  };
  git: {
    deleteRef: jest.MockedFunction<GitDeleteRef>;
  };
  repos: {
    getContent: jest.MockedFunction<ReposGetContent>;
  };
};

type LoggerFn = (obj: unknown, msg?: string) => void;

type Logger = {
  debug: jest.MockedFunction<LoggerFn>;
  info: jest.MockedFunction<LoggerFn>;
  warn: jest.MockedFunction<LoggerFn>;
  error: jest.MockedFunction<LoggerFn>;
};

type StaticConfig = {
  requests?: Record<string, unknown>;
  workflow: {
    labels: Record<string, unknown>;
    approvers: string[];
    approversPool?: string[];
  };
};

type Template = {
  title?: string;
  name?: string;
  _meta?: { requestType?: string; root?: string; schema?: string };
};

type ValidateResult = {
  errors: string[];
  errorsFormattedSingle: string;
  errorsFormatted: string;
  validationIssues?: { path: string; message: string }[];
  namespace: string;
  nsType: string;
  template: Template;
};

type LoadStaticConfig = (
  ctx: unknown,
  opts?: { useCache?: boolean }
) => Promise<{ config: StaticConfig; hooks: Record<string, unknown>; hooksSource: string }>;

type PostOnce = (ctx: unknown, params: IssueParams, body: string, opts?: { minimizeTag?: string }) => Promise<void>;

type CollapseBotCommentsByPrefix = (
  ctx: unknown,
  params: IssueParams,
  opts: { perPage?: number; tagPrefix: string; keepTags?: string[]; collapseBody?: string; classifier?: string }
) => Promise<void>;

type SetStateLabel = (ctx: unknown, params: IssueParams, issue: Issue, state: string) => Promise<void>;

type EnsureAssigneesOnce = (ctx: unknown, params: IssueParams, issue: Issue, assignees: string[]) => Promise<void>;

type LoadTemplate = (
  ctx: unknown,
  args: { owner: string; repo: string; issueLabels?: unknown; issueTitle?: string }
) => Promise<Template>;

type ParseForm = (body: string, template: Template) => Record<string, string>;

type ValidateRequestIssue = (ctx: unknown, params: IssueParams, issue: Issue, eff: unknown) => Promise<ValidateResult>;

type CalcSnapshotHash = (data: Record<string, string>, template: Template, issueBody: string) => string;

type ExtractHashFromPrBody = (body: string) => string;

type FindOpenIssuePrs = (ctx: unknown, owner: string, repo: string, issueNumber: number) => Promise<PullRequest[]>;

type CreateRequestPr = (
  ctx: unknown,
  args: {
    owner: string;
    repo: string;
    issueNumber: number;
    issueTitle: string;
    issueBody: string;
    template: Template;
  }
) => Promise<{ number: number }>;

type TryMergeIfGreen = (ctx: unknown, args: { owner: string; repo: string; prNumber: number }) => Promise<void>;

const DEFAULT_CONFIG_MOCK: StaticConfig = {
  workflow: { labels: {}, approvers: [] },
  requests: {},
};

const setStateLabel = jest.fn<SetStateLabel>(async () => {});
const ensureAssigneesOnce = jest.fn<EnsureAssigneesOnce>(async () => {});
const postOnce = jest.fn<PostOnce>(async () => {});
const collapseBotCommentsByPrefix = jest.fn<CollapseBotCommentsByPrefix>(async () => {});

const loadTemplate = jest.fn<LoadTemplate>();
const parseForm = jest.fn<ParseForm>();
const validateRequestIssue = jest.fn<ValidateRequestIssue>();
const runApprovalHook = jest.fn<() => Promise<boolean>>(() => Promise.resolve(false));

const calcSnapshotHash = jest.fn<CalcSnapshotHash>();
const extractHashFromPrBody = jest.fn<ExtractHashFromPrBody>();
const findOpenIssuePrs = jest.fn<FindOpenIssuePrs>();

const createRequestPr = jest.fn<CreateRequestPr>();
const tryMergeIfGreen = jest.fn<TryMergeIfGreen>();

const loadStaticConfig = jest.fn<LoadStaticConfig>();

const getDocLinksFromConfig = jest.fn<(cfg: unknown) => string>(() => '');

function mkApp(): { app: AppLike; handlers: Record<string, Handler> } {
  const handlers: Record<string, Handler> = {};

  const app: AppLike = {
    log: {
      warn: jest.fn<AppWarn>(() => {}),
    },
    on: (event: string | string[], handler: Handler) => {
      const evs = Array.isArray(event) ? event : [event];
      for (const e of evs) handlers[e] = handler;
    },
  };

  return { app, handlers };
}

function mkLogger(): Logger {
  return {
    debug: jest.fn<LoggerFn>(() => {}),
    info: jest.fn<LoggerFn>(() => {}),
    warn: jest.fn<LoggerFn>(() => {}),
    error: jest.fn<LoggerFn>(() => {}),
  };
}

function mkOctokit(): Octokit {
  return {
    issues: {
      get: jest.fn<IssuesGet>(),
      update: jest.fn<IssuesUpdate>(),
      addLabels: jest.fn<IssuesAddLabels>(),
      removeLabel: jest.fn<IssuesRemoveLabel>(),
    },
    pulls: {
      list: jest.fn<PullsList>(),
      update: jest.fn<PullsUpdate>(),
    },
    checks: {
      listForRef: jest.fn<ChecksListForRef>().mockResolvedValue({
        data: {
          check_runs: [{ id: 1, name: 'ci', status: 'completed', conclusion: 'success' }],
        },
      }),
    },
    git: {
      deleteRef: jest.fn<GitDeleteRef>(),
    },
    repos: {
      getContent: jest.fn<ReposGetContent>(),
    },
  };
}

function httpError(status: number, message = 'http error'): Error & { status: number } {
  return Object.assign(new Error(message), { status });
}

const DEFAULT_TEMPLATE: Template = {
  title: 'Request',
  _meta: {
    requestType: 'systemNamespace',
    root: 'data/namespaces',
    schema: '.github/registry-bot/request-schemas/system.schema.json',
  },
};

function mkIssueParams(owner = 'o', repo = 'r', issueNumber = 1): IssueParams {
  return { owner, repo, issue_number: issueNumber };
}

type Ctx = {
  name: string;
  payload: Record<string, unknown>;
  octokit: Octokit;
  log: Logger;
  issue: () => IssueParams;
  resourceBotConfig?: StaticConfig;
  resourceBotHooks?: Record<string, unknown>;
  resourceBotHooksSource?: string | null;
};

function mkCtx(args: {
  name: string;
  payload: Record<string, unknown>;
  owner?: string;
  repo?: string;
  issueNumber?: number;
  config?: StaticConfig;
  octokit?: Octokit;
  log?: Logger;
}): Ctx {
  const owner = args.owner ?? 'o';
  const repo = args.repo ?? 'r';
  const issueNumber = args.issueNumber ?? 1;

  return {
    name: args.name,
    payload: args.payload,
    octokit: args.octokit ?? mkOctokit(),
    log: args.log ?? mkLogger(),
    issue: () => mkIssueParams(owner, repo, issueNumber),
    resourceBotConfig: args.config,
    resourceBotHooks: {},
    resourceBotHooksSource: 'mock',
  };
}

let requestHandler: (app: Probot) => void;

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
    DEFAULT_CONFIG: DEFAULT_CONFIG_MOCK,
    loadStaticConfig,
  }));

  await jest.unstable_mockModule('../src/handlers/request/constants.js', () => ({
    getDocLinksFromConfig,
  }));

  ({ default: requestHandler } = await import('../src/handlers/request/index.js'));
});

beforeEach(() => {
  for (const mock of [
    setStateLabel,
    ensureAssigneesOnce,
    postOnce,
    collapseBotCommentsByPrefix,
    loadTemplate,
    parseForm,
    validateRequestIssue,
    runApprovalHook,
    calcSnapshotHash,
    extractHashFromPrBody,
    findOpenIssuePrs,
    createRequestPr,
    tryMergeIfGreen,
    loadStaticConfig,
    getDocLinksFromConfig,
  ]) {
    mock.mockReset();
  }

  loadStaticConfig.mockResolvedValue({
    config: DEFAULT_CONFIG_MOCK,
    hooks: {},
    hooksSource: 'mock',
  });

  loadTemplate.mockResolvedValue(DEFAULT_TEMPLATE);
  parseForm.mockReturnValue({ namespace: 'sap.test' });

  validateRequestIssue.mockResolvedValue({
    errors: [],
    errorsFormattedSingle: '',
    errorsFormatted: '',
    namespace: 'sap.test',
    nsType: 'system',
    template: DEFAULT_TEMPLATE,
  });

  runApprovalHook.mockResolvedValue(false);
  calcSnapshotHash.mockReturnValue('hash1');
  extractHashFromPrBody.mockReturnValue('hash1');
  findOpenIssuePrs.mockResolvedValue([]);

  createRequestPr.mockResolvedValue({ number: 42 });
  tryMergeIfGreen.mockResolvedValue(undefined);
});

describe('request-orchestrator additional coverage', () => {
  test('issues.opened: debug payload + isRequestIssue debug + skipped (not a request issue)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const issue: Issue = {
      number: 1,
      title: 'x',
      body: 'y',
      state: 'open',
      labels: [],
      user: { login: 'a' },
    };

    parseForm.mockReturnValueOnce({});

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'opened', issue },
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const debugMsgs = ctx.log.debug.mock.calls.map((c) => String(c[1] ?? '')).join('\n');
    expect(debugMsgs).toContain('dbg:issues:payload.issue');
    expect(debugMsgs).toContain('isRequestIssue(new-requests-only)');
    expect(debugMsgs).toContain('requestHandler:issues-event skipped (not a request issue)');
  });

  test('issues.opened: removeRejectedStatusLabel fetch failure is swallowed when issue has no labels', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const issue: Issue = {
      number: 2,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      user: { login: 'a' },
    };

    const octokit = mkOctokit();
    octokit.issues.get.mockRejectedValue(httpError(500, 'boom'));

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 2,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();
    expect(octokit.issues.get).toHaveBeenCalled();
  });

  test('issues.opened: closeOutdatedRequestPRs exception is caught and logged', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const issue: Issue = {
      number: 3,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: [],
      user: { login: 'a' },
    };

    findOpenIssuePrs.mockRejectedValueOnce(new Error('find prs failed'));

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'opened', issue },
      issueNumber: 3,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const warnCalls = app.log.warn.mock.calls.map((c) => c.map((x) => String(x)).join(' | ')).join('\n');
    expect(warnCalls).toContain('closeOutdatedRequestPRs skipped');
  });

  test('issues.opened: subcontext missing parent posts detected issues + sets author state', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const tpl: Template = {
      title: 'SubContext',
      _meta: { requestType: 'subContextNamespace', root: 'data/namespaces', schema: 'x' },
    };

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsFormattedSingle: '',
      errorsFormatted: '',
      namespace: 'sap.grc.ap',
      nsType: 'subcontext',
      template: tpl,
    });

    parseForm.mockReturnValueOnce({ namespace: 'sap.grc.ap' });
    loadTemplate.mockResolvedValueOnce(tpl);

    const octokit = mkOctokit();
    octokit.repos.getContent.mockRejectedValue(httpError(404, 'not found'));

    const issue: Issue = {
      number: 4,
      title: 'SubContext',
      body: '### Namespace\nsap.grc.ap',
      state: 'open',
      labels: [],
      user: { login: 'a' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 4,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    expect(postOnce).toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalledWith(ctx, mkIssueParams('o', 'r', 4), issue, 'author');
  });

  test('issues.opened: subcontext parent check non-404 error is caught and logged', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const tpl: Template = {
      title: 'SubContext',
      _meta: { requestType: 'subContextNamespace', root: 'data/namespaces', schema: 'x' },
    };

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsFormattedSingle: '',
      errorsFormatted: '',
      namespace: 'sap.grc.ap',
      nsType: 'subcontext',
      template: tpl,
    });

    parseForm.mockReturnValueOnce({ namespace: 'sap.grc.ap' });
    loadTemplate.mockResolvedValueOnce(tpl);

    const octokit = mkOctokit();
    octokit.repos.getContent.mockRejectedValue(httpError(500, 'server error'));

    const issue: Issue = {
      number: 5,
      title: 'SubContext',
      body: '### Namespace\nsap.grc.ap',
      state: 'open',
      labels: [],
      user: { login: 'a' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 5,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const warnCalls = app.log.warn.mock.calls.map((c) => c.map((x) => String(x)).join(' | ')).join('\n');
    expect(warnCalls).toContain('parent chain check failed');
  });

  test('issue_comment: approval ignored when request is not in review state', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const issue: Issue = {
      number: 6,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };
    const comment: Comment = { body: 'Approved', user: { login: 'approver' } };
    const sender: Sender = { type: 'User', login: 'approver' };

    const octokit = mkOctokit();
    octokit.issues.get.mockResolvedValueOnce({ data: { ...issue, labels: [] } });

    const ctx = mkCtx({
      name: 'issue_comment.created',
      config,
      octokit,
      payload: { action: 'created', issue, comment, sender },
      issueNumber: 6,
    });

    await expect(handlers['issue_comment.created']?.(ctx)).resolves.toBeUndefined();

    expect(octokit.issues.addLabels).not.toHaveBeenCalled();

    const warnMsgs = ctx.log.warn.mock.calls.map((c) => String(c[1] ?? '')).join('\n');
    expect(warnMsgs).not.toContain('failed to auto-add review labels on approval');

    const bodies = postOnce.mock.calls.map((c) => String(c[2] ?? '')).join('\n');
    expect(bodies).toContain('Approval ignored: request is not in review state.');
  });

  test('issue_comment: approval removes pending/progress/rejected labels and logs on removeLabel failures', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalRequested: ['needs-review'],
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const issue: Issue = {
      number: 7,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: ['needs-review'],
      user: { login: 'author' },
    };

    const comment: Comment = { body: 'Approved', user: { login: 'approver' } };
    const sender: Sender = { type: 'User', login: 'approver' };

    const octokit = mkOctokit();

    octokit.issues.get
      .mockResolvedValueOnce({ data: { ...issue, labels: ['needs-review'] } }) // review state check
      .mockResolvedValueOnce({ data: { ...issue, labels: ['Approved', 'needs-review'] } }) // pending label cleanup
      .mockResolvedValueOnce({
        data: { ...issue, labels: ['Approved', 'Requester Action', 'Review Pending', 'Rejected'] },
      }); // progress/rejected cleanup

    octokit.issues.removeLabel.mockImplementation(async ({ name }) => {
      // Add a dummy await to satisfy lint rule
      await Promise.resolve();
      if (name === 'needs-review') throw httpError(500, 'remove pending failed');
      if (name === 'Requester Action') throw httpError(500, 'remove progress failed');
    });

    const ctx = mkCtx({
      name: 'issue_comment.created',
      config,
      octokit,
      payload: { action: 'created', issue, comment, sender },
      issueNumber: 7,
    });

    await expect(handlers['issue_comment.created']?.(ctx)).resolves.toBeUndefined();

    const warnMsgs = ctx.log.warn.mock.calls.map((c) => String(c[1] ?? '')).join('\n');
    expect(warnMsgs).toContain('failed to remove review pending label after approval');
    expect(warnMsgs).toContain('failed to remove label');

    const removed = octokit.issues.removeLabel.mock.calls.map((c) => String(c[0]?.name ?? '')).sort();
    expect(removed).toEqual(expect.arrayContaining(['Rejected', 'Requester Action', 'Review Pending', 'needs-review']));
  });

  test('issue_comment: approval keyword from config (string) triggers approval detection', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: 'ship it',
        },
        approvers: [],
      },
    };

    const issue: Issue = {
      number: 8,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const comment: Comment = { body: 'ship it', user: { login: 'approver' } };
    const sender: Sender = { type: 'User', login: 'approver' };

    const ctx = mkCtx({
      name: 'issue_comment.created',
      config,
      payload: { action: 'created', issue, comment, sender },
      issueNumber: 8,
    });

    await expect(handlers['issue_comment.created']?.(ctx)).resolves.toBeUndefined();
    expect(createRequestPr).toHaveBeenCalled();
  });

  test('issue_comment: author update logs warnings for parent check failure and closeOutdatedRequestPRs failure', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: { labels: {}, approvers: [] },
    };

    const tpl: Template = {
      title: 'SubContext',
      _meta: { requestType: 'subContextNamespace', root: 'data/namespaces', schema: 'x' },
    };

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsFormattedSingle: '',
      errorsFormatted: '',
      namespace: 'sap.grc.ap',
      nsType: 'subcontext',
      template: tpl,
    });

    parseForm.mockReturnValue({ namespace: 'sap.grc.ap' });
    loadTemplate.mockResolvedValueOnce(tpl);

    findOpenIssuePrs.mockRejectedValueOnce(new Error('close prs failed'));

    const octokit = mkOctokit();
    octokit.repos.getContent.mockRejectedValue(httpError(500, 'server error'));

    const issue: Issue = {
      number: 9,
      title: 'SubContext',
      body: '### Namespace\nsap.grc.ap',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const comment: Comment = { body: 'updated', user: { login: 'author' } };
    const sender: Sender = { type: 'User', login: 'author' };

    const ctx = mkCtx({
      name: 'issue_comment.created',
      config,
      octokit,
      payload: { action: 'created', issue, comment, sender },
      issueNumber: 9,
    });

    await expect(handlers['issue_comment.created']?.(ctx)).resolves.toBeUndefined();

    const warnMsgs = [...app.log.warn.mock.calls, ...ctx.log.warn.mock.calls]
      .map((call) => String(call[1] ?? call[0] ?? ''))
      .join('\n');

    expect(warnMsgs).toContain('parent chain check failed');
    expect(warnMsgs).toContain('closeOutdatedRequestPRs skipped');
  });

  test('issue_comment: author update revalidation failure is caught and logged', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    validateRequestIssue.mockRejectedValueOnce(new Error('revalidation failed'));

    const issue: Issue = {
      number: 10,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const comment: Comment = { body: 'updated', user: { login: 'author' } };
    const sender: Sender = { type: 'User', login: 'author' };

    const ctx = mkCtx({
      name: 'issue_comment.created',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'created', issue, comment, sender },
      issueNumber: 10,
    });

    await expect(handlers['issue_comment.created']?.(ctx)).resolves.toBeUndefined();

    const warnCalls = app.log.warn.mock.calls.map((c) => c.map((x) => String(x)).join(' | ')).join('\n');
    expect(warnCalls).toContain('Revalidation failed:');
  });

  test('issues.opened: normalizeIssueTitle update failure is caught and logged', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();
    octokit.issues.update.mockRejectedValueOnce(new Error('update failed'));

    const tpl: Template = {
      title: 'Add System Namespace',
      _meta: { requestType: 'systemNamespace', root: 'data/namespaces', schema: 'x' },
    };

    loadTemplate.mockResolvedValueOnce(tpl);
    parseForm.mockReturnValueOnce({ identifier: 'sap.foo' });

    const issue: Issue = {
      number: 11,
      title: 'Wrong Title',
      body: '### Namespace\nsap.foo',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 11,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const warnMsgs = ctx.log.warn.mock.calls.map((c) => String(c[1] ?? '')).join('\n');
    expect(warnMsgs).toContain('Failed to normalize issue title');
  });

  test('getStaticConfig: loadStaticConfig failure falls back to DEFAULT_CONFIG', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    loadStaticConfig.mockRejectedValueOnce(new Error('config load failed'));

    const issue: Issue = {
      number: 12,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      payload: { action: 'opened', issue },
      issueNumber: 12,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const warnCalls = app.log.warn.mock.calls.map((c) => c.map((x) => String(x)).join(' | ')).join('\n');
    expect(warnCalls).toContain('failed to load resource-bot static config, using defaults');
    expect(ctx.resourceBotConfig).toBe(DEFAULT_CONFIG_MOCK);
  });

  test('issues.closed: when approved label exists, removes rejected + progress labels', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const octokit = mkOctokit();
    octokit.issues.get.mockResolvedValueOnce({
      data: {
        number: 13,
        labels: ['Approved', 'Rejected', 'Requester Action'],
      },
    });

    const issue: Issue = {
      number: 13,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'closed',
      labels: [],
      user: { login: 'author' },
    };

    const ctx = mkCtx({
      name: 'issues.closed',
      config,
      octokit,
      payload: { action: 'closed', issue },
      issueNumber: 13,
    });

    await expect(handlers['issues.closed']?.(ctx)).resolves.toBeUndefined();

    const removed = octokit.issues.removeLabel.mock.calls.map((c) => String(c[0]?.name ?? '')).sort();
    expect(removed).toEqual(expect.arrayContaining(['Rejected', 'Requester Action']));
  });

  test('issues.closed: when not approved, adds rejected (logs on failure) and removes approved if both exist after refresh', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const octokit = mkOctokit();

    octokit.issues.get
      .mockResolvedValueOnce({
        data: {
          number: 14,
          labels: ['Review Pending', 'Requester Action'],
        },
      })
      .mockResolvedValueOnce({
        data: {
          number: 14,
          labels: ['Rejected', 'Approved', 'Review Pending', 'Requester Action'],
        },
      });

    octokit.issues.addLabels.mockRejectedValueOnce(httpError(500, 'add rejected failed'));

    const issue: Issue = {
      number: 14,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'closed',
      labels: [],
      user: { login: 'author' },
    };

    const ctx = mkCtx({
      name: 'issues.closed',
      config,
      octokit,
      payload: { action: 'closed', issue },
      issueNumber: 14,
    });

    await expect(handlers['issues.closed']?.(ctx)).resolves.toBeUndefined();

    const warnMsgs = ctx.log.warn.mock.calls.map((c) => String(c[1] ?? '')).join('\n');
    expect(warnMsgs).toContain('failed to add rejected status label');

    const removed = octokit.issues.removeLabel.mock.calls.map((c) => String(c[0]?.name ?? '')).sort();
    expect(removed).toEqual(expect.arrayContaining(['Approved', 'Requester Action', 'Review Pending']));
  });

  test('issues.closed: handles repeated label fetch failures (removeProgressStatusLabels fetch failure path)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const octokit = mkOctokit();
    octokit.issues.get.mockRejectedValue(httpError(500, 'labels fetch failed'));

    const issue: Issue = {
      number: 15,
      title: 'Request',
      body: '### Namespace\nsap.ok',
      state: 'closed',
      labels: [],
      user: { login: 'author' },
    };

    const ctx = mkCtx({
      name: 'issues.closed',
      config,
      octokit,
      payload: { action: 'closed', issue },
      issueNumber: 15,
    });

    await expect(handlers['issues.closed']?.(ctx)).resolves.toBeUndefined();
  });

  test('status: tryAutoMerge paginates when first page has 100 PRs', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();

    const makePr = (n: number): PullRequest => ({
      number: n,
      body: `source: #${n}`,
      head: { sha: `sha-${n}`, ref: `ref-${n}` },
    });

    const page1 = Array.from({ length: 100 }, (_, i) => makePr(i + 1));

    octokit.pulls.list.mockResolvedValueOnce({ data: page1 }).mockResolvedValueOnce({ data: [] });

    const repository: Repository = { name: 'r', owner: { login: 'o' } };

    const ctx = mkCtx({
      name: 'status',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { state: 'success', repository, sha: 'head-sha' },
    });

    await expect(handlers['status']?.(ctx)).resolves.toBeUndefined();

    expect(octokit.pulls.list).toHaveBeenCalledTimes(2);
    expect(octokit.pulls.list.mock.calls[0]?.[0]?.page).toBe(1);
    expect(octokit.pulls.list.mock.calls[1]?.[0]?.page).toBe(2);
  });

  test('status: tryAutoMerge skips when issue cannot be loaded', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();

    octokit.pulls.list.mockResolvedValueOnce({
      data: [{ number: 1, body: 'source: #123', head: { sha: 'head-sha', ref: 'ref-1' } }],
    });

    octokit.issues.get.mockRejectedValueOnce(httpError(500, 'issue load failed'));

    const repository: Repository = { name: 'r', owner: { login: 'o' } };

    const ctx = mkCtx({
      name: 'status',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { state: 'success', repository, sha: 'head-sha' },
    });

    await expect(handlers['status']?.(ctx)).resolves.toBeUndefined();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('status: tryAutoMerge skips when template cannot be resolved for issue', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();

    octokit.pulls.list.mockResolvedValueOnce({
      data: [{ number: 1, body: 'source: #123', head: { sha: 'head-sha', ref: 'ref-1' } }],
    });

    const issue: Issue = {
      number: 123,
      title: 'Request',
      body: 'x',
      state: 'open',
      labels: [],
      user: { login: 'a' },
    };
    octokit.issues.get.mockResolvedValueOnce({ data: issue });

    loadTemplate.mockRejectedValueOnce(new Error('template load failed'));

    const repository: Repository = { name: 'r', owner: { login: 'o' } };

    const ctx = mkCtx({
      name: 'status',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { state: 'success', repository, sha: 'head-sha' },
    });

    await expect(handlers['status']?.(ctx)).resolves.toBeUndefined();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('status: ignores non-success state before auto-merge evaluation', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();
    const repository: Repository = { name: 'r', owner: { login: 'o' } };

    const ctx = mkCtx({
      name: 'status',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { state: 'failure', repository, sha: 'head-sha' },
    });

    await expect(handlers['status']?.(ctx)).resolves.toBeUndefined();

    expect(octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('status: ignores success payload without sha', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const octokit = mkOctokit();
    const repository: Repository = { name: 'r', owner: { login: 'o' } };

    const ctx = mkCtx({
      name: 'status',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { state: 'success', repository },
    });

    await expect(handlers['status']?.(ctx)).resolves.toBeUndefined();

    expect(octokit.pulls.list).not.toHaveBeenCalled();
    expect(tryMergeIfGreen).not.toHaveBeenCalled();
  });

  test('issues.opened: routing error is silently ignored for non-form issues', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    loadTemplate.mockRejectedValueOnce(new Error('no routing label found'));

    const issue: Issue = {
      number: 1,
      title: 'Other',
      body: 'plain text (no form headings)',
      labels: [],
      user: { login: 'u1' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'opened', issue, sender: { type: 'User', login: 'u1' } },
    });

    ctx.octokit.issues.get.mockResolvedValueOnce({
      data: { number: 1, labels: [] },
    });

    loadTemplate
      .mockRejectedValueOnce(new Error('no routing label found'))
      .mockRejectedValueOnce(new Error('no routing label found'));

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    expect(loadTemplate).toHaveBeenCalled();
    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });
  test('issues.opened: routing error ignored when headings only inside code fence', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    loadTemplate.mockRejectedValueOnce(new Error('Cannot resolve template: no routing label found'));

    const issue: Issue = {
      number: 2,
      title: 'Other',
      body: '```md\n### Namespace\nsap.test\n```',
      labels: [],
      user: { login: 'u1' },
    };

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'opened', issue, sender: { type: 'User', login: 'u1' } },
    });

    // ensure the label-refresh retry path doesn't crash
    ctx.octokit.issues.get.mockResolvedValueOnce({ data: { number: 2, labels: [] } });

    // loadTemplate is called twice: initial + retry after label refresh
    loadTemplate
      .mockRejectedValueOnce(new Error('Cannot resolve template: no routing label found'))
      .mockRejectedValueOnce(new Error('Cannot resolve template: no routing label found'));

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    expect(postOnce).not.toHaveBeenCalled();
    expect(setStateLabel).not.toHaveBeenCalled();
  });
  test('issues.opened: routing error posts message for form issues (not skipped)', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    // label refresh retry path must not crash
    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: {
        action: 'opened',
        sender: { type: 'User', login: 'u1' },
        issue: {
          number: 3,
          title: 'Form-ish issue',
          labels: [],
          user: { login: 'u1' },
          body: '### Namespace\nsap.test\n', // <-- looks like issue form
        },
      },
    });

    ctx.octokit.issues.get.mockResolvedValueOnce({ data: { number: 3, labels: [] } });

    loadTemplate
      .mockRejectedValueOnce(new Error('no routing label found'))
      .mockRejectedValueOnce(new Error('no routing label found'));

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    expect(postOnce).toHaveBeenCalled();
    expect(setStateLabel).toHaveBeenCalled();
  });
  test('issues.opened: detected issues comment includes machine readable metadata', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const issue: Issue = {
      number: 21,
      title: 'Request',
      body: '### Namespace\nsap.bad',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    validateRequestIssue.mockResolvedValueOnce({
      errors: ['bad request'],
      errorsFormattedSingle: '### Name\n- Name MUST match the expected namespace format.',
      errorsFormatted: '### Name\n- Name MUST match the expected namespace format.',
      validationIssues: [{ path: 'name', message: 'Vendor is incorrect because XYZ' }],
      namespace: 'sap.bad',
      nsType: 'system',
      template: DEFAULT_TEMPLATE,
    });

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      payload: { action: 'opened', issue },
      issueNumber: 21,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const body = String(postOnce.mock.calls[0]?.[2] ?? '');
    expect(body).toContain('<summary>Show as JSON (Robots Friendly)</summary>');
    expect(body).toContain('"field": "name"');
    expect(body).toContain('"message": "Vendor is incorrect because XYZ"');
  });
  test('issues.opened: direct PR without snapshot hash is not closed as outdated and approved flow keeps existing PR', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const directPr: PullRequest = {
      number: 88,
      body: 'source: #22',
      head: { sha: 'sha-direct', ref: 'manual-pr' },
    };

    findOpenIssuePrs.mockResolvedValue([directPr]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    runApprovalHook.mockResolvedValueOnce({ status: 'approved' } as any);

    const issue: Issue = {
      number: 22,
      title: 'Request',
      body: '### Namespace\nsap.agt',
      state: 'open',
      labels: [],
      user: { login: 'author' },
    };

    const octokit = mkOctokit();

    const ctx = mkCtx({
      name: 'issues.opened',
      config,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 22,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    expect(octokit.pulls.update).not.toHaveBeenCalled();
    expect(octokit.git.deleteRef).not.toHaveBeenCalled();
    expect(createRequestPr).not.toHaveBeenCalled();

    const bodies = postOnce.mock.calls.map((c) => String(c[2] ?? '')).join('\n');
    expect(bodies).toContain('PR already open: #88');
  });

  test('issues.opened: product request keeps stale parent marker but continues normal review flow', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const productTemplate: Template = {
      title: 'Product',
      _meta: {
        requestType: 'product',
        root: 'resources',
        schema: 'x',
      },
    };

    loadTemplate.mockResolvedValueOnce(productTemplate);
    parseForm.mockReturnValueOnce({ identifier: 'product-five', title: 'Product Five' });

    validateRequestIssue.mockResolvedValueOnce({
      errors: [],
      errorsFormattedSingle: '',
      errorsFormatted: '',
      validationIssues: [],
      namespace: 'product-five',
      nsType: 'product',
      template: productTemplate,
    });

    const issue: Issue = {
      number: 24,
      title: 'Product: product-five',
      body: '### Product ID\n\nproduct-five\n\n<!-- nsreq:parent-approval = {"v":1,"state":"pending","parent":"sap.css","owners":["barOwner"],"target":"sap.css.foo"} -->',
      state: 'open',
      labels: [{ name: 'Product' }],
      user: { login: 'requester' },
    };

    const octokit = mkOctokit();
    octokit.issues.get.mockResolvedValue({ data: issue });

    const ctx = mkCtx({
      name: 'issues.opened',
      config: DEFAULT_CONFIG_MOCK,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 24,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const bodyUpdateArgs = octokit.issues.update.mock.calls
      .map(([args]) => args as { body?: string })
      .find((args) => typeof args.body === 'string');

    const updatedBody = String(bodyUpdateArgs?.body ?? '');

    expect(updatedBody).toContain('nsreq:parent-approval');
    expect(updatedBody).toContain('nsreq:routing-lock');
    expect(setStateLabel).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.anything(), 'review');
  });

  test('issues.opened: outdated snapshot cleanup tolerates deleteRef and approved-label removal failures', async () => {
    const { app, handlers } = mkApp();
    requestHandler(app as unknown as Probot);

    const config: StaticConfig = {
      requests: {},
      workflow: {
        labels: {
          approvalSuccessful: ['Approved'],
        },
        approvers: [],
      },
    };

    const issue: Issue = {
      number: 25,
      title: 'Request',
      body: '### Namespace\n\nsap.test\n',
      state: 'open',
      labels: ['Approved'],
      user: { login: 'author' },
    };

    const octokit = mkOctokit();
    octokit.issues.get.mockResolvedValue({ data: issue });
    octokit.pulls.update.mockResolvedValueOnce();
    octokit.git.deleteRef.mockRejectedValueOnce(httpError(500, 'delete failed'));
    octokit.issues.removeLabel.mockRejectedValueOnce(httpError(500, 'remove approved failed'));

    findOpenIssuePrs.mockResolvedValueOnce([
      {
        number: 89,
        body: '<!-- nsreq:snapshot:old-hash -->',
        head: { sha: 'sha-old', ref: 'old-ref' },
      },
    ]);

    extractHashFromPrBody.mockReturnValueOnce('old-hash');
    calcSnapshotHash.mockReturnValueOnce('new-hash');

    const ctx = mkCtx({
      name: 'issues.opened',
      config,
      octokit,
      payload: { action: 'opened', issue },
      issueNumber: 25,
    });

    await expect(handlers['issues.opened']?.(ctx)).resolves.toBeUndefined();

    const bodies = postOnce.mock.calls.map((c) => String(c[2] ?? '')).join('\n');

    expect(octokit.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ owner: 'o', repo: 'r', pull_number: 89, state: 'closed' })
    );
    expect(bodies).toContain('Form updated → closing outdated PR(s): #89');
  });
});
