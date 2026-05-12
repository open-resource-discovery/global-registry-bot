import { toStringTrim } from './login-utils.js';

type PullRequestLike = {
  mergeable_state?: string | null;
  head?: { sha?: string | null } | null;
};

export type BranchUpdateRefreshOutcome = {
  freshHeadSha: string;
  freshMergeableState: string;
  headChanged: boolean;
  stillBehind: boolean;
  shouldRetry: boolean;
};

export type BranchUpdateRefreshOutcomeCallbacks<PullRequestType> = {
  readMergeableState: (pr: PullRequestType | null | undefined) => string;
  isPullRequestBehindBase: (pr: PullRequestType | null | undefined) => boolean;
};

export function evaluateBranchUpdateRefreshOutcome<PullRequestType extends PullRequestLike>(
  headSha: string,
  fresh: PullRequestType | null,
  callbacks: BranchUpdateRefreshOutcomeCallbacks<PullRequestType>
): BranchUpdateRefreshOutcome {
  const freshHeadSha = toStringTrim(fresh?.head?.sha);
  const freshMergeableState = callbacks.readMergeableState(fresh);
  const stillBehind = Boolean(fresh && callbacks.isPullRequestBehindBase(fresh));
  const headChanged = Boolean(freshHeadSha && freshHeadSha !== headSha);

  return {
    freshHeadSha,
    freshMergeableState,
    headChanged,
    stillBehind,
    shouldRetry: stillBehind,
  };
}
