/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */
import { beforeAll, beforeEach, expect, jest, test } from '@jest/globals';

type IssueParams = { owner: string; repo: string; issue_number: number };

const setStateLabel = jest.fn(async () => {});
const ensureAssigneesOnce = jest.fn(async () => {});
const postOnce = jest.fn(async () => {});
const collapseBotCommentsByPrefix = jest.fn(async () => {});

const loadTemplate = jest.fn(async () => ({}));
const parseForm = jest.fn(() => ({}));
const validateRequestIssue = jest.fn(async () => ({}));
const runApprovalHook = jest.fn(async () => false);

const calcSnapshotHash = jest.fn(() => 'h1');
const extractHashFromPrBody = jest.fn(() => '');
const findOpenIssuePrs = jest.fn(async () => []);

const createRequestPr = jest.fn(async () => ({ number: 10 }));
const tryMergeIfGreen = jest.fn(async () => undefined);

const loadStaticConfig = jest.fn(async () => ({}));
const getDocLinksFromConfig = jest.fn(() => '');

const DEFAULT_CONFIG = {
  requests: {
    product: { folderName: 'resources' },
  },
  workflow: {
    labels: {
      approvalSuccessful: ['Approved'],
    },
    approvers: [],
  },
} as any;

let requestHandler: any;

function httpErr(status: number, message: string): Error & { status: number } {
  return Object.assign(new Error(message), { status });
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

function mkOctokit() {
  return {
    issues: {
      get: jest.fn(async () => ({ data: { number: 1, labels: [] } })),
      update: jest.fn(async () => ({})),
      addLabels: jest.fn(async () => ({})),
      removeLabel: jest.fn(async () => ({})),
    },
    pulls: {
      list: jest.fn(async () => ({ data: [] })),
      get: jest.fn(async (args: any) => ({
        data: {
          number: Number(args?.pull_number ?? 1),
          state: 'open',
          title: 'Direct',
          body: 'manual direct pr',
          head: { ref: 'feature/direct', sha: 'head-sha' },
          base: { ref: 'main', sha: 'base-sha' },
          mergeable: true,
          mergeable_state: 'clean',
        },
      })),
      listFiles: jest.fn(async () => ({ data: [] })),
      listCommits: jest.fn(async () => ({ data: [] })),
      listReviews: jest.fn(async () => ({ data: [] })),
      createReview: jest.fn(async () => ({})),
      updateBranch: jest.fn(async () => ({})),
    },
    checks: {
      listForRef: jest.fn(async () => ({
        data: {
          check_runs: [{ id: 1, name: 'ci', status: 'completed', conclusion: 'success' }],
        },
      })),
    },
    git: {
      deleteRef: jest.fn(async () => ({})),
    },
    repos: {
      getContent: jest.fn(async () => ({})),
    },
  };
}

function mkStatusContext(args: { octokit?: any; sha?: string; config?: any }) {
  const octokit = args.octokit ?? mkOctokit();

  return {
    name: 'status',
    payload: {
      state: 'success',
      sha: args.sha ?? 'head-sha',
      repository: { name: 'r1', owner: { login: 'o1' } },
    },
    log: {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
    octokit,
    issue: (): IssueParams => ({ owner: 'o1', repo: 'r1', issue_number: 1 }),
    repo: () => ({ owner: 'o1', repo: 'r1' }),
    resourceBotConfig: args.config ?? DEFAULT_CONFIG,
    resourceBotHooks: null,
    resourceBotHooksSource: 'test',
  } as any;
}

function wireStandaloneApprovedRegistryPr(octokit: any, pr: any): void {
  octokit.pulls.get.mockResolvedValue({
    data: {
      ...pr,
      state: 'open',
      base: { ref: 'main', sha: 'base-sha' },
      mergeable: true,
      mergeable_state: 'clean',
    },
  });
  octokit.pulls.listFiles.mockResolvedValue({
    data: [
      { filename: 'resources/product-one.yaml', status: 'modified' },
      { filename: '.github/workflows/review.yaml', status: 'modified' },
    ],
  });
  octokit.pulls.listCommits.mockResolvedValue({
    data: [{ author: { login: 'direct-author' }, committer: { login: 'direct-last-committer' } }],
  });
  octokit.pulls.listReviews.mockResolvedValue({ data: [] });
  octokit.repos.getContent.mockResolvedValue({
    data: {
      content: Buffer.from(
        'type: product\nname: product-one\ndescription: Example\ncontact: owner@example.com\n',
        'utf8'
      ).toString('base64'),
      encoding: 'base64',
    },
  });
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

  ({ default: requestHandler } = await import('../src/handlers/request/index.js'));
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
  extractHashFromPrBody.mockReturnValue('');
  findOpenIssuePrs.mockResolvedValue([]);
  createRequestPr.mockResolvedValue({ number: 10 });
  tryMergeIfGreen.mockResolvedValue(undefined);
});

test('status: body-linked PR falls back to standalone approval when linked issue read fails', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 61,
    body: 'source: #123',
    title: 'Direct',
    head: { ref: 'feature/direct-body', sha: 'head-sha-body' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  (octokit.issues.get as any).mockImplementation(async ({ issue_number }: any) => {
    if (issue_number === 123) throw httpErr(500, 'issue load failed');
    return { data: { number: issue_number, labels: [] } };
  });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved from fallback body' } as any);

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-body' });

  await handlers['status'][0](ctx);

  expect(runApprovalHook as any).toHaveBeenCalledWith(
    ctx,
    { owner: 'o1', repo: 'r1' },
    expect.objectContaining({ requestType: 'product', namespace: 'product-one', resourceName: 'product-one' })
  );
  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 61, mergeMethod: 'squash' })
  );
});

test('status: missing sha skips auto-merge evaluation immediately', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const octokit = mkOctokit();
  const ctx = mkStatusContext({ octokit, sha: '' });

  await handlers['status'][0](ctx);

  expect(octokit.pulls.list).not.toHaveBeenCalled();
});

test('status: title-linked PR falls back to standalone approval when template load fails', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 62,
    body: 'manual direct pr',
    title: 'Fixes #124',
    head: { ref: 'feature/direct-title', sha: 'head-sha-title' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const issue = {
    number: 124,
    title: 'Request',
    body: '### Namespace\nproduct-one',
    state: 'open',
    labels: [],
    user: { login: 'requester' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  (octokit.issues.get as any).mockResolvedValue({ data: issue });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  loadTemplate.mockRejectedValueOnce(new Error('template load failed'));
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved from fallback title' } as any);

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-title' });

  await handlers['status'][0](ctx);

  expect(octokit.issues.get as any).toHaveBeenCalledWith(expect.objectContaining({ issue_number: 124 }));
  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 62, mergeMethod: 'squash' })
  );
});

test('status: head-ref linked PR falls back to standalone approval when linked issue is not a request', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 63,
    body: 'manual direct pr',
    title: 'Direct',
    head: { ref: 'feature/issue-125-direct', sha: 'head-sha-headref' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const issue = {
    number: 125,
    title: 'Discussion',
    body: 'plain text body',
    state: 'open',
    labels: [],
    user: { login: 'requester' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  (octokit.issues.get as any).mockResolvedValue({ data: issue });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  parseForm.mockReturnValueOnce({});
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved from fallback head-ref' } as any);

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-headref' });

  await handlers['status'][0](ctx);

  expect(octokit.issues.get as any).toHaveBeenCalledWith(expect.objectContaining({ issue_number: 125 }));
  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 63, mergeMethod: 'squash' })
  );
});

test('status: auto-merge evaluation dedupes concurrent requests for the same sha', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  let releaseList: (() => void) | null = null;
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockImplementation(
    () =>
      new Promise((resolve) => {
        releaseList = () => resolve({ data: [] });
      })
  );

  const ctx1 = mkStatusContext({ octokit, sha: 'head-sha-dedupe' });
  const ctx2 = mkStatusContext({ octokit, sha: 'head-sha-dedupe' });

  const first = handlers['status'][0](ctx1);
  const second = handlers['status'][0](ctx2);

  await new Promise<void>((resolve) => process.nextTick(resolve));
  if (releaseList) {
    (releaseList as () => void)();
  }

  await Promise.all([first, second]);

  expect(octokit.pulls.list).toHaveBeenCalledTimes(1);
  expect(
    ctx2.log.info.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge:evaluation deduped')
    )
  ).toBe(true);
});

test('status: auto-merge evaluation skips a repeated sha immediately after completion', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [] });

  const ctx1 = mkStatusContext({ octokit, sha: 'head-sha-recent' });
  const ctx2 = mkStatusContext({ octokit, sha: 'head-sha-recent' });

  await handlers['status'][0](ctx1);
  await handlers['status'][0](ctx2);

  expect(octokit.pulls.list).toHaveBeenCalledTimes(1);
  expect(
    ctx2.log.info.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge:evaluation skipped: recently completed')
    )
  ).toBe(true);
});

test('status: unexpected auto-merge candidate processing failure is logged and swallowed', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 65,
    body: 'source: #127',
    title: 'Direct',
    head: { ref: 'feature/direct-processing-failure', sha: 'head-sha-processing-failure' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  (octokit.issues.get as any).mockImplementation(async ({ issue_number }: any) => {
    if (issue_number === 127) throw httpErr(500, 'issue load failed');
    return { data: { number: issue_number, labels: [] } };
  });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved before failure' } as any);
  tryMergeIfGreen.mockRejectedValueOnce(new Error('boom'));

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-processing-failure' });

  await handlers['status'][0](ctx);

  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 65, mergeMethod: 'squash' })
  );
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge candidate processing failed')
    )
  ).toBe(true);
});

test('status: processing failure continues without sequential recovery when refreshed PR is no longer a registry change', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 67,
    body: 'source: #128',
    title: 'Direct',
    head: { ref: 'feature/direct-non-sequential-after-failure', sha: 'head-sha-non-sequential-after-failure' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  (octokit.issues.get as any).mockImplementation(async ({ issue_number }: any) => {
    if (issue_number === 128) throw httpErr(500, 'issue load failed');
    return { data: { number: issue_number, labels: [] } };
  });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  (octokit.pulls.listFiles as any)
    .mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-one.yaml', status: 'modified' },
        { filename: '.github/workflows/review.yaml', status: 'modified' },
      ],
    })
    .mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-one.yaml', status: 'modified' },
        { filename: '.github/workflows/review.yaml', status: 'modified' },
      ],
    })
    .mockResolvedValueOnce({
      data: [
        { filename: 'resources/product-one.yaml', status: 'modified' },
        { filename: '.github/workflows/review.yaml', status: 'modified' },
      ],
    })
    .mockResolvedValue({
      data: [{ filename: 'docs/readme.md', status: 'modified' }],
    });
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved before failure' } as any);
  tryMergeIfGreen.mockRejectedValueOnce(new Error('boom'));

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-non-sequential-after-failure' });

  await handlers['status'][0](ctx);

  expect(tryMergeIfGreen as any).toHaveBeenCalledWith(
    ctx,
    expect.objectContaining({ owner: 'o1', repo: 'r1', prNumber: 67, mergeMethod: 'squash' })
  );
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge candidate processing failed')
    )
  ).toBe(true);
});

test('status: snapshot-managed PR failure skips sequential recovery handling', async () => {
  const { app, handlers } = mkApp();
  requestHandler(app);

  const pr = {
    number: 66,
    body: 'manual direct pr\n\n<!-- nsreq:snapshot:h1 -->',
    title: 'Direct',
    head: { ref: 'feature/direct-snapshot-failure', sha: 'head-sha-snapshot-failure' },
    base: { ref: 'main', sha: 'base-sha' },
  };
  const octokit = mkOctokit();
  (octokit.pulls.list as any).mockResolvedValue({ data: [pr] });
  wireStandaloneApprovedRegistryPr(octokit, pr);
  extractHashFromPrBody.mockReturnValueOnce('h1');
  runApprovalHook.mockResolvedValueOnce({ status: 'approved', comment: 'approved before snapshot failure' } as any);
  tryMergeIfGreen.mockRejectedValueOnce(new Error('boom'));

  const ctx = mkStatusContext({ octokit, sha: 'head-sha-snapshot-failure' });

  await handlers['status'][0](ctx);

  expect(octokit.pulls.list).toHaveBeenCalledTimes(1);
  expect(
    ctx.log.warn.mock.calls.some((call: any[]) =>
      String(call[1] ?? call[0] ?? '').includes('auto-merge candidate processing failed')
    )
  ).toBe(true);
});
