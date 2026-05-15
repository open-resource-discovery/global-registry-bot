import {
  clearSequentialRegistryPrQueueInflight,
  getSequentialRegistryPrQueueInflight,
  setSequentialRegistryPrQueueInflight,
} from './sequential-registry-pr-state.js';
import { isPullRequestDirty, isPullRequestOpen, readMergeableState } from '../domain/pull-request-merge-state.js';
import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  title?: string | null;
  head?: {
    sha?: string | null;
    ref?: string | null;
  } | null;
  base?: {
    ref?: string | null;
  } | null;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
  state?: string | null;
};

type HeadGreenEvaluationLike = {
  green: boolean;
  reason: string;
  latestRuns: unknown[];
  blockingRuns: unknown[];
};

type SequentialRegistryPrCandidate<PullRequestType extends PullRequestLikeBase> = {
  pr: PullRequestType;
  freshPr: PullRequestType;
  changedRegistryFiles: string[];
  mustUpdate: boolean;
  approvedForUpdate: boolean;
};

type SequentialRegistryPrResultLike = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

export type SequentialRegistryPrQueueCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
> = {
  isSequentialRegistryPrActiveBlocking: (context: ContextType, repoInfo: RepoInfoType) => Promise<boolean>;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  isSnapshotManagedRequestPr: (pr: PullRequestType) => boolean;
  pullRequestTargetsBranch: (pr: PullRequestType, branchName: string) => boolean;
  isSequentialRegistryPrHeadSkipped: (repoInfo: RepoInfoType, pr: PullRequestType) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  shouldUpdatePullRequestBranch: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<boolean>;
  isPullRequestApprovedForBranchMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<boolean>;
  requestPullRequestBranchUpdate: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => Promise<boolean>;
  markSequentialRegistryPrActive: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  evaluateHeadGreenForApprovalReevaluation: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string
  ) => Promise<HeadGreenEvaluationType>;
  processPullRequestForAutoMerge: (context: ContextType, repoInfo: RepoInfoType, pr: PullRequestType) => Promise<void>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
};

async function collectSequentialDirectRegistryPrCandidates<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  baseBranch: string,
  reason: string,
  callbacks: SequentialRegistryPrQueueCallbacks<ContextType, RepoInfoType, PullRequestType, HeadGreenEvaluationType>
): Promise<SequentialRegistryPrCandidate<PullRequestType>[]> {
  const openPrs = await callbacks.listOpenPullRequests(context, repoInfo);
  const candidates: SequentialRegistryPrCandidate<PullRequestType>[] = [];

  for (const pr of openPrs.sort((a, b) => b.number - a.number)) {
    const headSha = toStringTrim(pr.head?.sha);
    const linkedIssueNumber = callbacks.parseLinkedIssueNumberFromPr(pr, repoInfo);
    const snapshotManaged = callbacks.isSnapshotManagedRequestPr(pr);

    const baseLog = {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      prNumber: pr.number,
      title: toStringTrim(pr.title),
      headSha,
      headRef: toStringTrim(pr.head?.ref),
      prBase: toStringTrim(pr.base?.ref),
      baseBranch,
      linkedIssueNumber,
      snapshotManaged,
      reason,
    };

    if (snapshotManaged) {
      callbacks.log(
        context,
        'info',
        { ...baseLog, skipReason: 'snapshot-managed-request-pr' },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    if (!headSha) {
      callbacks.log(context, 'info', { ...baseLog, skipReason: 'missing-head-sha' }, 'direct-pr-reeval:skip');
      continue;
    }

    if (!callbacks.pullRequestTargetsBranch(pr, baseBranch)) {
      callbacks.log(context, 'info', { ...baseLog, skipReason: 'different-base-branch' }, 'direct-pr-reeval:skip');
      continue;
    }

    if (callbacks.isSequentialRegistryPrHeadSkipped(repoInfo, pr)) {
      callbacks.log(context, 'info', { ...baseLog, skipReason: 'head-temporarily-skipped' }, 'direct-pr-reeval:skip');
      continue;
    }

    const changedRegistryFiles = await callbacks.listChangedYamlFilesForPrWithFallback(
      context,
      repoInfo,
      pr,
      baseBranch
    );

    if (!changedRegistryFiles.length) {
      callbacks.log(
        context,
        'info',
        {
          ...baseLog,
          changedRegistryFiles,
          skipReason: 'no-registry-yaml-files-changed',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    const freshPr = (await callbacks.readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const freshHeadSha = toStringTrim(freshPr.head?.sha);

    if (!isPullRequestOpen(freshPr)) {
      callbacks.log(
        context,
        'info',
        {
          ...baseLog,
          freshHeadSha,
          changedRegistryFiles,
          skipReason: 'pr-not-open',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    if (isPullRequestDirty(freshPr)) {
      callbacks.log(
        context,
        'warn',
        {
          ...baseLog,
          freshHeadSha,
          changedRegistryFiles,
          mergeableState: readMergeableState(freshPr),
          skipReason: 'pr-has-merge-conflicts',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    const mustUpdate = await callbacks.shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch);
    const approvedForUpdate = mustUpdate
      ? await callbacks.isPullRequestApprovedForBranchMaintenance(context, repoInfo, freshPr)
      : false;

    callbacks.log(
      context,
      'info',
      {
        ...baseLog,
        freshHeadSha,
        changedRegistryFiles,
        mergeable: freshPr.mergeable,
        mergeableState: readMergeableState(freshPr),
        mustUpdate,
        approvedForUpdate,
      },
      'direct-pr-reeval:update-check'
    );

    candidates.push({
      pr,
      freshPr,
      changedRegistryFiles,
      mustUpdate,
      approvedForUpdate,
    });
  }

  return candidates;
}

export async function runOneSequentialDirectRegistryPrMaintenance<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  baseBranch: string,
  reason: string,
  callbacks: SequentialRegistryPrQueueCallbacks<ContextType, RepoInfoType, PullRequestType, HeadGreenEvaluationType>
): Promise<SequentialRegistryPrResultLike> {
  const existing = getSequentialRegistryPrQueueInflight<SequentialRegistryPrResultLike, RepoInfoType>(repoInfo);

  if (existing) return await existing;

  const pending = (async (): Promise<SequentialRegistryPrResultLike> => {
    if (await callbacks.isSequentialRegistryPrActiveBlocking(context, repoInfo)) {
      return { updated: false, processed: false, blockedByActive: true };
    }

    const candidates = await collectSequentialDirectRegistryPrCandidates(
      context,
      repoInfo,
      baseBranch,
      reason,
      callbacks
    );

    for (const candidate of candidates.filter((item) => item.mustUpdate)) {
      const requested = await callbacks.requestPullRequestBranchUpdate(
        context,
        repoInfo,
        candidate.freshPr,
        candidate.approvedForUpdate
          ? `${reason}:sequential-direct-pr-update-approved`
          : `${reason}:sequential-direct-pr-refresh-stale`
      );

      callbacks.log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          prNumber: candidate.freshPr.number,
          title: toStringTrim(candidate.freshPr.title),
          headSha: toStringTrim(candidate.freshPr.head?.sha),
          headRef: toStringTrim(candidate.freshPr.head?.ref),
          baseBranch,
          changedRegistryFiles: candidate.changedRegistryFiles,
          requested,
          reason,
        },
        'direct-pr-reeval:update-before-approval-result'
      );

      if (requested) {
        callbacks.markSequentialRegistryPrActive(context, repoInfo, candidate.freshPr, reason);
        return { updated: true, processed: true, blockedByActive: false };
      }

      callbacks.markSequentialRegistryPrHeadSkipped(
        context,
        repoInfo,
        candidate.freshPr,
        'branch-update-request-failed'
      );
    }

    for (const candidate of candidates.filter((item) => !item.mustUpdate)) {
      const headSha = toStringTrim(candidate.freshPr.head?.sha);
      const greenResult = headSha
        ? await callbacks.evaluateHeadGreenForApprovalReevaluation(context, repoInfo, headSha)
        : {
            green: false,
            reason: 'missing-head-sha',
            latestRuns: [],
            blockingRuns: [],
          };

      callbacks.log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          prNumber: candidate.freshPr.number,
          headSha,
          baseBranch,
          changedRegistryFiles: candidate.changedRegistryFiles,
          green: greenResult.green,
          greenReason: greenResult.reason,
          blockingRuns: greenResult.blockingRuns,
          latestRuns: greenResult.latestRuns.slice(0, 30),
          reason,
        },
        'direct-pr-reeval:head-green'
      );

      if (!greenResult.green) {
        continue;
      }

      await callbacks.processPullRequestForAutoMerge(context, repoInfo, candidate.freshPr);
      return { updated: false, processed: true, blockedByActive: false };
    }

    return { updated: false, processed: false, blockedByActive: false };
  })().finally(() => {
    clearSequentialRegistryPrQueueInflight(repoInfo);
  });

  setSequentialRegistryPrQueueInflight(repoInfo, pending);
  return await pending;
}
