import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  head?: {
    sha?: string | null;
  } | null;
  base?: {
    ref?: string | null;
  } | null;
};

type SequentialRegistryPrActiveLike = {
  prNumber: number;
};

type HeadGreenEvaluationLike = {
  green: boolean;
  reason: string;
  latestRuns: unknown[];
  blockingRuns: unknown[];
  statusState?: string;
};

const AUTO_MERGE_EVALUATION_INFLIGHT = new Map<string, Promise<void>>();
const AUTO_MERGE_EVALUATION_RECENT_UNTIL = new Map<string, number>();
const AUTO_MERGE_EVALUATION_RECENT_TTL_MS = 30_000;

function isAutoMergeEvaluationRecentlyCompleted(key: string): boolean {
  const until = AUTO_MERGE_EVALUATION_RECENT_UNTIL.get(key);

  if (!until) return false;

  if (until <= Date.now()) {
    AUTO_MERGE_EVALUATION_RECENT_UNTIL.delete(key);
    return false;
  }

  return true;
}

function markAutoMergeEvaluationRecentlyCompleted(key: string): void {
  AUTO_MERGE_EVALUATION_RECENT_UNTIL.set(key, Date.now() + AUTO_MERGE_EVALUATION_RECENT_TTL_MS);
}

export type AutoMergeTriggerCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
> = {
  getStaticConfig: (context: ContextType) => Promise<unknown>;
  evaluateHeadGreenForApprovalReevaluation: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string
  ) => Promise<HeadGreenEvaluationType>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  processPullRequestForAutoMerge: (context: ContextType, repoInfo: RepoInfoType, pr: PullRequestType) => Promise<void>;
  releaseSequentialRegistryPrIfNotApprovedAfterGreen: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<void>;
  advanceSequentialRegistryPrQueueAfterTerminalState: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<void>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  isSequentialDirectRegistryPr: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<boolean>;
  getSequentialRegistryPrActive: (repoInfo: RepoInfoType) => ActiveStateType;
  clearSequentialRegistryPrActive: (repoInfo: RepoInfoType) => void;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<unknown>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
};

export async function runAutoMergeEvaluation<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  normalizedHeadSha: string,
  callbacks: AutoMergeTriggerCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<void> {
  await callbacks.getStaticConfig(context);

  const greenResult = await callbacks.evaluateHeadGreenForApprovalReevaluation(context, repoInfo, normalizedHeadSha);

  callbacks.log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      headSha: normalizedHeadSha,
      green: greenResult.green,
      greenReason: greenResult.reason,
      statusState: greenResult.statusState,
      blockingRuns: greenResult.blockingRuns,
      latestRuns: greenResult.latestRuns.slice(0, 30),
    },
    'auto-merge:head-green'
  );

  if (!greenResult.green) return;

  const candidates = (await callbacks.listOpenPullRequests(context, repoInfo)).filter(
    (pr) => toStringTrim(pr.head?.sha) === normalizedHeadSha
  );

  callbacks.log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      headSha: normalizedHeadSha,
      candidatePrNumbers: candidates.map((pr) => pr.number),
    },
    'auto-merge:candidates'
  );

  for (const pr of candidates) {
    try {
      await callbacks.processPullRequestForAutoMerge(context, repoInfo, pr);
      await callbacks.releaseSequentialRegistryPrIfNotApprovedAfterGreen(context, repoInfo, pr);
      await callbacks.advanceSequentialRegistryPrQueueAfterTerminalState(
        context,
        repoInfo,
        pr,
        'sequential-direct-pr:advance-after-terminal-state'
      );
    } catch (e: unknown) {
      callbacks.log(
        context,
        'warn',
        {
          err: e instanceof Error ? e.message : String(e),
          prNumber: pr.number,
        },
        'auto-merge candidate processing failed'
      );

      const freshPr = (await callbacks.readFreshPullRequest(context, repoInfo, pr.number)) || pr;
      const baseBranch = toStringTrim(freshPr.base?.ref) || toStringTrim(pr.base?.ref);
      const isSequentialDirectRegistry = baseBranch
        ? await callbacks.isSequentialDirectRegistryPr(context, repoInfo, freshPr, baseBranch)
        : false;

      if (!isSequentialDirectRegistry) {
        continue;
      }

      const active = callbacks.getSequentialRegistryPrActive(repoInfo);
      const wasActiveSequentialPr = active?.prNumber === freshPr.number || active?.prNumber === pr.number;

      callbacks.markSequentialRegistryPrHeadSkipped(
        context,
        repoInfo,
        freshPr,
        'auto-merge-candidate-processing-failed'
      );

      if (wasActiveSequentialPr) {
        callbacks.clearSequentialRegistryPrActive(repoInfo);

        if (baseBranch) {
          await callbacks.runOneSequentialDirectRegistryPrMaintenance(
            context,
            repoInfo,
            baseBranch,
            'sequential-direct-pr:advance-after-processing-failure'
          );
        }
      }
    }
  }
}

export async function tryAutoMerge<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  headSha: string,
  runAutoMergeEvaluationFn: (context: ContextType, repoInfo: RepoInfoType, normalizedHeadSha: string) => Promise<void>,
  callbacks: AutoMergeTriggerCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<void> {
  const normalizedHeadSha = toStringTrim(headSha);
  if (!normalizedHeadSha) {
    callbacks.log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
      },
      'auto-merge:skip-missing-head-sha'
    );
    return;
  }

  const key = `${repoInfo.owner}/${repoInfo.repo}:${normalizedHeadSha}:auto-merge-evaluation`.toLowerCase();

  const existing = AUTO_MERGE_EVALUATION_INFLIGHT.get(key);
  if (existing) {
    callbacks.log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: normalizedHeadSha,
      },
      'auto-merge:evaluation deduped: already in flight'
    );

    await existing;
    return;
  }

  if (isAutoMergeEvaluationRecentlyCompleted(key)) {
    callbacks.log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: normalizedHeadSha,
      },
      'auto-merge:evaluation skipped: recently completed'
    );

    return;
  }

  const pending = runAutoMergeEvaluationFn(context, repoInfo, normalizedHeadSha).finally(() => {
    AUTO_MERGE_EVALUATION_INFLIGHT.delete(key);
    markAutoMergeEvaluationRecentlyCompleted(key);
  });

  AUTO_MERGE_EVALUATION_INFLIGHT.set(key, pending);
  await pending;
}
