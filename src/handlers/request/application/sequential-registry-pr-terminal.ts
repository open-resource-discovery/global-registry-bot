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
};

export type SequentialRegistryPrTerminalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
> = {
  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number
  ) => Promise<PullRequestType | null>;
  isPullRequestOpen: (pr: PullRequestType | null | undefined) => boolean;
  getSequentialRegistryPrActive: (repoInfo: RepoInfoType) => ActiveStateType;
  clearSequentialRegistryPrActive: (repoInfo: RepoInfoType) => void;
  markSequentialRegistryPrHeadSkipped: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    reason: string
  ) => void;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  pullRequestTargetsBranch: (pr: PullRequestType, branchName: string) => boolean;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  runOneSequentialDirectRegistryPrMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<unknown>;
  evaluateHeadGreenForApprovalReevaluation: (
    context: ContextType,
    repoInfo: RepoInfoType,
    headSha: string
  ) => Promise<HeadGreenEvaluationType>;
  isPullRequestApprovedForBranchMaintenance: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<boolean>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
};

async function markFailedRegistryPrHeadsForSha<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  headSha: string,
  baseBranch: string,
  reason: string,
  callbacks: SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<boolean> {
  const sha = toStringTrim(headSha);
  if (!sha) return false;

  const openPrs = await callbacks.listOpenPullRequests(context, repoInfo);
  const matching = openPrs.filter((pr) => toStringTrim(pr.head?.sha) === sha);

  let marked = false;

  for (const pr of matching) {
    if (!callbacks.pullRequestTargetsBranch(pr, baseBranch)) continue;

    const changedRegistryFiles = await callbacks.listChangedYamlFilesForPrWithFallback(
      context,
      repoInfo,
      pr,
      baseBranch
    );
    if (!changedRegistryFiles.length) continue;

    callbacks.markSequentialRegistryPrHeadSkipped(context, repoInfo, pr, reason);

    const active = callbacks.getSequentialRegistryPrActive(repoInfo);
    if (active?.prNumber === pr.number) {
      callbacks.clearSequentialRegistryPrActive(repoInfo);
    }

    marked = true;

    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: sha,
        changedRegistryFiles,
        reason,
      },
      'sequential-registry-pr:failed-head-marked'
    );
  }

  return marked;
}

async function resolveSequentialRegistryQueueBaseBranchForHead<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  headSha: string,
  fallbackBaseBranch: string,
  callbacks: SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<string> {
  const fallback = toStringTrim(fallbackBaseBranch);
  if (fallback) return fallback;

  const sha = toStringTrim(headSha);
  if (!sha) return '';

  try {
    const openPrs = await callbacks.listOpenPullRequests(context, repoInfo);
    const matchingPr = openPrs.find((pr) => toStringTrim(pr.head?.sha) === sha);
    const baseBranch = toStringTrim(matchingPr?.base?.ref);

    if (baseBranch) return baseBranch;
  } catch {
    return '';
  }

  return '';
}

export async function advanceSequentialRegistryPrQueueAfterTerminalState<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<void> {
  const active = callbacks.getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber !== pr.number) return;

  const freshPr = await callbacks.readFreshPullRequest(context, repoInfo, pr.number);

  if (freshPr && callbacks.isPullRequestOpen(freshPr)) {
    return;
  }

  callbacks.clearSequentialRegistryPrActive(repoInfo);

  const baseBranch = toStringTrim(freshPr?.base?.ref) || toStringTrim(pr.base?.ref);
  if (!baseBranch) return;

  await callbacks.runOneSequentialDirectRegistryPrMaintenance(context, repoInfo, baseBranch, reason);
}

export async function handleBlockingRegistryHeadConclusion<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  headSha: string,
  baseBranch: string,
  reason: string,
  callbacks: SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<boolean> {
  const sha = toStringTrim(headSha);
  if (!sha) return false;

  const marked = await markFailedRegistryPrHeadsForSha(context, repoInfo, sha, baseBranch, reason, callbacks);
  if (!marked) {
    callbacks.log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: sha,
        baseBranch: toStringTrim(baseBranch),
        reason,
      },
      'sequential-registry-pr:blocking-head-not-marked'
    );

    return false;
  }

  const advanceBaseBranch = await resolveSequentialRegistryQueueBaseBranchForHead(
    context,
    repoInfo,
    sha,
    baseBranch,
    callbacks
  );

  if (!advanceBaseBranch) {
    callbacks.log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: sha,
        reason,
      },
      'sequential-registry-pr:advance-skipped-missing-base-branch'
    );

    return true;
  }

  await callbacks.runOneSequentialDirectRegistryPrMaintenance(
    context,
    repoInfo,
    advanceBaseBranch,
    `${reason}:advance-next-registry-pr`
  );

  return true;
}

export async function releaseSequentialRegistryPrIfNotApprovedAfterGreen<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  ActiveStateType extends SequentialRegistryPrActiveLike | null,
  HeadGreenEvaluationType extends HeadGreenEvaluationLike,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: SequentialRegistryPrTerminalCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    ActiveStateType,
    HeadGreenEvaluationType
  >
): Promise<void> {
  const active = callbacks.getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber !== pr.number) return;

  const freshPr = await callbacks.readFreshPullRequest(context, repoInfo, pr.number);
  if (!freshPr || !callbacks.isPullRequestOpen(freshPr)) {
    callbacks.clearSequentialRegistryPrActive(repoInfo);
    return;
  }

  const headSha = toStringTrim(freshPr.head?.sha);
  if (!headSha) return;

  const approvedForMaintenance = await callbacks.isPullRequestApprovedForBranchMaintenance(context, repoInfo, freshPr);
  if (approvedForMaintenance) {
    return;
  }

  const greenResult = await callbacks.evaluateHeadGreenForApprovalReevaluation(context, repoInfo, headSha);
  if (!greenResult.green) return;

  callbacks.markSequentialRegistryPrHeadSkipped(context, repoInfo, freshPr, 'green-head-did-not-qualify-for-approval');
  callbacks.clearSequentialRegistryPrActive(repoInfo);

  await callbacks.runOneSequentialDirectRegistryPrMaintenance(
    context,
    repoInfo,
    toStringTrim(freshPr.base?.ref),
    'sequential-direct-pr:advance-after-not-approved'
  );
}
