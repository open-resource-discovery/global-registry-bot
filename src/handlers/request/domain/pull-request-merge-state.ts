import { toStringTrim } from './login-utils.js';

type PullRequestLike = {
  state?: string | null;
  mergeable?: boolean | null;
  mergeable_state?: string | null;
};

export function readMergeableState(pr: PullRequestLike | null | undefined): string {
  return toStringTrim(pr?.mergeable_state).toLowerCase();
}

export function isPullRequestOpen(pr: PullRequestLike | null | undefined): boolean {
  return toStringTrim(pr?.state).toLowerCase() === 'open';
}

export function isMergeabilityPending(pr: PullRequestLike | null | undefined): boolean {
  const state = readMergeableState(pr);

  return pr?.mergeable === null || state === 'unknown' || state === 'checking';
}

export function isPullRequestBehindBase(pr: PullRequestLike | null | undefined): boolean {
  return readMergeableState(pr) === 'behind';
}

export function isPullRequestDirty(pr: PullRequestLike | null | undefined): boolean {
  const state = readMergeableState(pr);

  return state === 'dirty' || state === 'conflicting';
}
