import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  head?: {
    sha?: string | null;
  } | null;
};

export type SequentialRegistryPrActive = {
  prNumber: number;
  startedHeadSha: string;
  startedAt: number;
  expiresAt: number;
  reason: string;
};

type StateLogCallbacks<ContextType> = {
  log: (context: ContextType, level: 'info', obj: unknown, msg: string) => void;
};

const SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT = new Map<string, Promise<unknown>>();
const SEQUENTIAL_REGISTRY_PR_ACTIVE = new Map<string, SequentialRegistryPrActive>();
const SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS = new Map<string, number>();

const SEQUENTIAL_REGISTRY_PR_ACTIVE_TTL_MS = 30 * 60 * 1000;
const SEQUENTIAL_REGISTRY_PR_SKIP_TTL_MS = 6 * 60 * 60 * 1000;

function sequentialRegistryPrRepoKey<RepoInfoType extends RepoInfoBase>(repoInfo: RepoInfoType): string {
  return `${repoInfo.owner}/${repoInfo.repo}`.toLowerCase();
}

function sequentialRegistryPrHeadKey<RepoInfoType extends RepoInfoBase>(
  repoInfo: RepoInfoType,
  prNumber: number,
  headSha: string
): string {
  return `${sequentialRegistryPrRepoKey(repoInfo)}#${prNumber}:${toStringTrim(headSha)}`;
}

function pruneSequentialRegistryPrSkipState(): void {
  const now = Date.now();

  for (const [key, until] of SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.entries()) {
    if (until <= now) SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.delete(key);
  }
}

export function isSequentialRegistryPrHeadSkipped<
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(repoInfo: RepoInfoType, pr: PullRequestType): boolean {
  pruneSequentialRegistryPrSkipState();

  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = sequentialRegistryPrHeadKey(repoInfo, pr.number, headSha);
  return (SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.get(key) || 0) > Date.now();
}

export function markSequentialRegistryPrHeadSkipped<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: StateLogCallbacks<ContextType>
): void {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return;

  const key = sequentialRegistryPrHeadKey(repoInfo, pr.number, headSha);
  SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.set(key, Date.now() + SEQUENTIAL_REGISTRY_PR_SKIP_TTL_MS);

  callbacks.log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha,
      reason,
    },
    'sequential-registry-pr:head-skipped'
  );
}

export function getSequentialRegistryPrActive<RepoInfoType extends RepoInfoBase>(
  repoInfo: RepoInfoType
): SequentialRegistryPrActive | null {
  return SEQUENTIAL_REGISTRY_PR_ACTIVE.get(sequentialRegistryPrRepoKey(repoInfo)) || null;
}

export function clearSequentialRegistryPrActive<RepoInfoType extends RepoInfoBase>(repoInfo: RepoInfoType): void {
  SEQUENTIAL_REGISTRY_PR_ACTIVE.delete(sequentialRegistryPrRepoKey(repoInfo));
}

export function markSequentialRegistryPrActive<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  reason: string,
  callbacks: StateLogCallbacks<ContextType>
): void {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return;

  const startedAt = Date.now();
  const active: SequentialRegistryPrActive = {
    prNumber: pr.number,
    startedHeadSha: headSha,
    startedAt,
    expiresAt: startedAt + SEQUENTIAL_REGISTRY_PR_ACTIVE_TTL_MS,
    reason,
  };

  SEQUENTIAL_REGISTRY_PR_ACTIVE.set(sequentialRegistryPrRepoKey(repoInfo), active);

  callbacks.log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha,
      expiresAt: active.expiresAt,
      reason,
    },
    'sequential-registry-pr:active-set'
  );
}

export function getSequentialRegistryPrQueueInflight<ResultType, RepoInfoType extends RepoInfoBase>(
  repoInfo: RepoInfoType
): Promise<ResultType> | undefined {
  return SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.get(sequentialRegistryPrRepoKey(repoInfo)) as
    | Promise<ResultType>
    | undefined;
}

export function setSequentialRegistryPrQueueInflight<ResultType, RepoInfoType extends RepoInfoBase>(
  repoInfo: RepoInfoType,
  pending: Promise<ResultType>
): void {
  SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.set(sequentialRegistryPrRepoKey(repoInfo), pending);
}

export function clearSequentialRegistryPrQueueInflight<RepoInfoType extends RepoInfoBase>(
  repoInfo: RepoInfoType
): void {
  SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.delete(sequentialRegistryPrRepoKey(repoInfo));
}
