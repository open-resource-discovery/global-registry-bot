import { type ApprovalDecision } from '../domain/approval-decision.js';

type WorkflowRunPullRequestRef = { number?: number | null };

export type WorkflowRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  pull_requests?: WorkflowRunPullRequestRef[] | null;
};

type PullRequestLikeBase = {
  number: number;
  state?: string | null;
  draft?: boolean | null;
  head: { sha?: string | null; ref?: string | null };
  base?: { ref?: string | null } | null;
};

type PullRequestFileLikeBase = {
  filename?: string | null;
  status?: string | null;
};

type RepoInfoBase = { owner: string; repo: string };

type WorkflowApprovalTrustSignal = {
  trusted: boolean;
  reason: string;
  decision?: ApprovalDecision;
};

export type WorkflowApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
> = {
  isPullRequestOpen: (pr: PullRequestType | null | undefined) => boolean;
  isSafeRegistryWorkflowApprovalFile: (context: ContextType, file: PullRequestFileLikeType) => boolean;
  listChangedFilesForPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestFileLikeType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  evaluateDirectPrOnApproval: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    requestAuthorIdOverride: string | undefined,
    options: { baseBranch?: string }
  ) => Promise<ApprovalDecision>;
  hasAllowedStandaloneDirectPrApprovalForCurrentHead: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    decision: ApprovalDecision,
    options: { baseBranch?: string }
  ) => Promise<boolean>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  log: (
    context: ContextType | undefined,
    level: 'debug' | 'info' | 'warn' | 'error',
    obj: unknown,
    msg: string
  ) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  toStringTrim: (value: unknown) => string;
};

const WORKFLOW_APPROVAL_RETRY_INFLIGHT = new Map<string, NodeJS.Timeout>();
const WORKFLOW_APPROVAL_RETRY_DELAYS_MS = [10_000, 30_000];
const WORKFLOW_APPROVAL_LAST_RUNS = new Map<string, WorkflowRunLike[]>();

function workflowApprovalHeadKey(
  repoInfo: RepoInfoBase,
  pr: PullRequestLikeBase,
  toStringTrim: (v: unknown) => string
): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${pr.number}:${toStringTrim(pr.head?.sha)}`.toLowerCase();
}

function workflowApprovalRetryKey(
  repoInfo: RepoInfoBase,
  pr: PullRequestLikeBase,
  attempt: number,
  toStringTrim: (v: unknown) => string
): string {
  return `${workflowApprovalHeadKey(repoInfo, pr, toStringTrim)}:${attempt}`;
}

function rememberWorkflowApprovalRuns(
  repoInfo: RepoInfoBase,
  pr: PullRequestLikeBase,
  runs: WorkflowRunLike[],
  toStringTrim: (v: unknown) => string
): void {
  WORKFLOW_APPROVAL_LAST_RUNS.set(workflowApprovalHeadKey(repoInfo, pr, toStringTrim), runs || []);
}

function shouldRetryWorkflowApproval(
  repoInfo: RepoInfoBase,
  pr: PullRequestLikeBase,
  toStringTrim: (v: unknown) => string
): boolean {
  const runs = WORKFLOW_APPROVAL_LAST_RUNS.get(workflowApprovalHeadKey(repoInfo, pr, toStringTrim));
  return Array.isArray(runs) && runs.length === 0;
}

function isWorkflowRunWaitingForApproval(run: WorkflowRunLike, toStringTrim: (v: unknown) => string): boolean {
  const status = toStringTrim(run?.status).toLowerCase();
  const conclusion = toStringTrim(run?.conclusion).toLowerCase();

  return status === 'waiting' || conclusion === 'action_required';
}

function workflowRunTargetsPullRequest(
  run: WorkflowRunLike,
  pr: PullRequestLikeBase,
  toStringTrim: (v: unknown) => string
): boolean {
  const headSha = toStringTrim(pr.head?.sha);
  const runHeadSha = toStringTrim(run?.head_sha);

  if (headSha && runHeadSha && headSha === runHeadSha) return true;

  const prs = Array.isArray(run?.pull_requests) ? run.pull_requests : [];
  return prs.some((item) => item?.number === pr.number);
}

async function listWorkflowRunsForPullRequestHead<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<WorkflowRunLike[]> {
  const { toStringTrim, isPlainObject, log, getErrorMessage, getHttpStatus } = callbacks;
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return [];

  try {
    const client = (
      context as unknown as {
        octokit: {
          request: (route: string, args: Record<string, unknown>) => Promise<{ data?: unknown }>;
        };
      }
    ).octokit;
    const res = await client.request('GET /repos/{owner}/{repo}/actions/runs', {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      head_sha: headSha,
      per_page: 100,
    });
    const data = isPlainObject(res?.data) ? res.data : {};
    const runs = Array.isArray(data['workflow_runs']) ? data['workflow_runs'] : [];

    return (runs as WorkflowRunLike[]).filter((run) => workflowRunTargetsPullRequest(run, pr, toStringTrim));
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        prNumber: pr.number,
        headSha,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'workflow-approval:runs-read-failed'
    );

    return [];
  }
}

async function approveWorkflowRun<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  runId: number,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<boolean> {
  const { log, getErrorMessage, getHttpStatus } = callbacks;
  try {
    const client = (
      context as unknown as {
        octokit: {
          request: (route: string, args: Record<string, unknown>) => Promise<unknown>;
        };
      }
    ).octokit;

    await client.request('POST /repos/{owner}/{repo}/actions/runs/{run_id}/approve', {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      run_id: runId,
    });

    return true;
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        runId,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'workflow-approval:approve-run-failed'
    );

    return false;
  }
}

async function isSafeRegistryOnlyPullRequest<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<boolean> {
  const files = await callbacks.listChangedFilesForPr(context, repoInfo, pr.number);
  if (!files.length) return false;

  return files.every((file) => callbacks.isSafeRegistryWorkflowApprovalFile(context, file));
}

async function resolveWorkflowApprovalTrustSignalForRegistryPr<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<WorkflowApprovalTrustSignal> {
  const {
    parseLinkedIssueNumberFromPr,
    isSnapshotManagedRequestPr,
    evaluateDirectPrOnApproval,
    hasAllowedStandaloneDirectPrApprovalForCurrentHead,
    log,
    getErrorMessage,
    getHttpStatus,
    toStringTrim,
  } = callbacks;

  if (parseLinkedIssueNumberFromPr(pr, repoInfo) !== null || isSnapshotManagedRequestPr(pr)) {
    return { trusted: false, reason: 'not-standalone-direct-pr' };
  }

  const baseBranch = toStringTrim(pr.base?.ref);

  try {
    const decision = await evaluateDirectPrOnApproval(context, repoInfo, pr, undefined, { baseBranch });

    if (decision.status === 'approved') {
      return { trusted: true, reason: 'onApproval-approved', decision };
    }

    const hasCurrentHeadApproval = await hasAllowedStandaloneDirectPrApprovalForCurrentHead(
      context,
      repoInfo,
      pr,
      decision,
      { baseBranch }
    );

    if (hasCurrentHeadApproval) {
      return { trusted: true, reason: 'allowed-current-head-approval', decision };
    }

    return {
      trusted: false,
      reason: `missing-trust-signal:${toStringTrim(decision.status) || 'none'}`,
      decision,
    };
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        err: getErrorMessage(error),
        status: getHttpStatus(error),
        reason,
      },
      'workflow-approval:trust-check-failed'
    );

    return { trusted: false, reason: 'trust-check-failed' };
  }
}

async function maybeApprovePendingWorkflowRunsForRegistryPr<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<boolean> {
  const { isPullRequestOpen, log, toStringTrim } = callbacks;

  if (!isPullRequestOpen(pr) || pr.draft === true) return false;

  const safeRegistryOnly = await isSafeRegistryOnlyPullRequest(context, repoInfo, pr, callbacks);
  if (!safeRegistryOnly) {
    log(context, 'info', { prNumber: pr.number, reason }, 'workflow-approval:skip-not-safe-registry-only-pr');
    return false;
  }

  const trustSignal = await resolveWorkflowApprovalTrustSignalForRegistryPr(context, repoInfo, pr, reason, callbacks);
  if (!trustSignal.trusted) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        trustReason: trustSignal.reason,
        decisionStatus: toStringTrim(trustSignal.decision?.status) || 'none',
        reason,
      },
      'workflow-approval:skip-missing-trust-signal'
    );

    return false;
  }

  const runs = await listWorkflowRunsForPullRequestHead(context, repoInfo, pr, callbacks);
  rememberWorkflowApprovalRuns(repoInfo, pr, runs, toStringTrim);

  const waitingRuns = runs.filter((run) => isWorkflowRunWaitingForApproval(run, toStringTrim));
  if (!waitingRuns.length) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        runs: runs.map((run) => ({
          id: run.id,
          name: toStringTrim(run.name),
          status: toStringTrim(run.status),
          conclusion: toStringTrim(run.conclusion),
        })),
        reason,
      },
      'workflow-approval:no-waiting-runs'
    );

    return false;
  }

  let approvedAny = false;

  for (const run of waitingRuns) {
    const runId = typeof run.id === 'number' && Number.isFinite(run.id) ? run.id : 0;
    if (!runId) continue;

    const approved = await approveWorkflowRun(context, repoInfo, runId, callbacks);
    approvedAny = approvedAny || approved;

    log(
      context,
      approved ? 'info' : 'warn',
      {
        prNumber: pr.number,
        runId,
        runName: toStringTrim(run.name),
        headSha: toStringTrim(pr.head?.sha),
        reason,
      },
      approved ? 'workflow-approval:run-approved' : 'workflow-approval:run-approval-failed'
    );
  }

  return approvedAny;
}

function scheduleWorkflowApprovalRetry<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>,
  attempt = 0
): void {
  const { toStringTrim, isPullRequestOpen, readFreshPullRequest, log, getErrorMessage, getHttpStatus } = callbacks;
  const delay = WORKFLOW_APPROVAL_RETRY_DELAYS_MS[attempt];
  if (delay === undefined) return;

  const key = workflowApprovalRetryKey(repoInfo, pr, attempt, toStringTrim);
  if (WORKFLOW_APPROVAL_RETRY_INFLIGHT.has(key)) return;

  const originalHeadSha = toStringTrim(pr.head?.sha);
  const timer = setTimeout(() => {
    WORKFLOW_APPROVAL_RETRY_INFLIGHT.delete(key);

    void (async (): Promise<void> => {
      const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
      const freshHeadSha = toStringTrim(freshPr.head?.sha);

      if (!isPullRequestOpen(freshPr)) return;
      if (originalHeadSha && freshHeadSha && originalHeadSha !== freshHeadSha) return;

      const approved = await maybeApprovePendingWorkflowRunsForRegistryPr(
        context,
        repoInfo,
        freshPr,
        `${reason}:retry-${attempt + 1}`,
        callbacks
      );

      if (!approved) {
        scheduleWorkflowApprovalRetry(context, repoInfo, freshPr, reason, callbacks, attempt + 1);
      }
    })().catch((error: unknown) => {
      log(
        context,
        'warn',
        {
          prNumber: pr.number,
          err: getErrorMessage(error),
          status: getHttpStatus(error),
          reason,
          attempt: attempt + 1,
        },
        'workflow-approval:retry-failed'
      );
    });
  }, delay);

  if (typeof timer.unref === 'function') {
    timer.unref();
  }

  WORKFLOW_APPROVAL_RETRY_INFLIGHT.set(key, timer);
  log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha: originalHeadSha,
      delayMs: delay,
      attempt: attempt + 1,
      reason,
    },
    'workflow-approval:retry-scheduled'
  );
}

export async function maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<boolean> {
  const { toStringTrim, log } = callbacks;
  const approved = await maybeApprovePendingWorkflowRunsForRegistryPr(context, repoInfo, pr, reason, callbacks);
  if (approved) return true;

  if (shouldRetryWorkflowApproval(repoInfo, pr, toStringTrim)) {
    scheduleWorkflowApprovalRetry(context, repoInfo, pr, reason, callbacks);
  } else {
    const hasRunSnapshot = WORKFLOW_APPROVAL_LAST_RUNS.has(workflowApprovalHeadKey(repoInfo, pr, toStringTrim));
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        reason,
      },
      hasRunSnapshot
        ? 'workflow-approval:retry-skipped-run-already-visible'
        : 'workflow-approval:retry-skipped-no-trust-signal'
    );
  }

  return false;
}

export async function maybeApprovePendingWorkflowRunsForPrNumbersApplication<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  PullRequestFileLikeType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  prNumbers: number[],
  headSha: string,
  reason: string,
  callbacks: WorkflowApprovalCallbacks<ContextType, RepoInfoType, PullRequestType, PullRequestFileLikeType>
): Promise<boolean> {
  const { toStringTrim, readFreshPullRequest, isPullRequestOpen } = callbacks;
  const sha = toStringTrim(headSha);
  const uniquePrNumbers = Array.from(new Set((prNumbers || []).filter((n) => Number.isFinite(n))));

  for (const prNumber of uniquePrNumbers) {
    const pr = await readFreshPullRequest(context, repoInfo, prNumber);
    if (!pr || !isPullRequestOpen(pr)) continue;

    const prHeadSha = toStringTrim(pr.head?.sha);
    if (sha && prHeadSha && prHeadSha !== sha) continue;

    const approved = await maybeApprovePendingWorkflowRunsForRegistryPrWithRetryApplication(
      context,
      repoInfo,
      pr,
      reason,
      callbacks
    );
    if (approved) return true;
  }

  return false;
}
