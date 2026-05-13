export type MergeFailureClassificationCallbacks = {
  getErrorMessage: (error: unknown) => string;
};

export function shouldTryBranchUpdateAfterMergeFailure(
  error: unknown,
  callbacks: MergeFailureClassificationCallbacks
): boolean {
  const msg = callbacks.getErrorMessage(error).toLowerCase();

  return (
    msg.includes('branch is out-of-date') ||
    msg.includes('branch is out of date') ||
    msg.includes('update branch') ||
    msg.includes('must be up to date') ||
    msg.includes('must be up-to-date') ||
    msg.includes('behind the base branch')
  );
}

export function isMergeBlockedByBranchProtection(
  error: unknown,
  callbacks: MergeFailureClassificationCallbacks
): boolean {
  const msg = callbacks.getErrorMessage(error).toLowerCase();

  return (
    msg.includes('at least 1 approving review is required') ||
    msg.includes('approving review is required') ||
    msg.includes('required status check') ||
    msg.includes('is expected') ||
    msg.includes('protected branch') ||
    msg.includes('pull request is not mergeable')
  );
}
