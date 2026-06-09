import { toStringTrim } from '../domain/login-utils.js';
import { isMergeabilityPending, isPullRequestOpen, readMergeableState } from '../domain/pull-request-merge-state.js';

const MERGEABILITY_POLL_ATTEMPTS = 6;
const MERGEABILITY_POLL_DELAY_MS = 1500;

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  state?: string | null;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
  head?: { sha?: string | null } | null;
};

export type PullRequestMergeabilityCallbacks<ContextType, PullRequestType> = {
  readFreshPullRequest: (context: ContextType, repoInfo: RepoInfo, prNumber: number) => Promise<PullRequestType | null>;
  delayMs: (ms: number) => Promise<void>;
  logMergeabilityState: (
    context: ContextType,
    args: {
      prNumber: number;
      attempt: number;
      headSha: string;
      mergeable: boolean | null | undefined;
      mergeableState: string;
      reason: string;
    }
  ) => void;
};

export async function waitForPullRequestMergeability<ContextType, PullRequestType extends PullRequestLike>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  reason: string,
  callbacks: PullRequestMergeabilityCallbacks<ContextType, PullRequestType>
): Promise<PullRequestType> {
  let current = pr;

  for (let attempt = 1; attempt <= MERGEABILITY_POLL_ATTEMPTS; attempt += 1) {
    const fresh = await callbacks.readFreshPullRequest(context, repoInfo, pr.number);
    if (fresh) current = fresh;

    const mergeable = current.mergeable;
    const mergeableState = readMergeableState(current);

    callbacks.logMergeabilityState(context, {
      prNumber: current.number,
      attempt,
      headSha: toStringTrim(current.head?.sha),
      mergeable,
      mergeableState,
      reason,
    });

    if (!isPullRequestOpen(current)) return current;
    if (!isMergeabilityPending(current)) return current;

    await callbacks.delayMs(MERGEABILITY_POLL_DELAY_MS);
  }

  return current;
}
