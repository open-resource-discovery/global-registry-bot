export type BranchUpdateErrorClassificationCallbacks = {
  getHttpStatus: (error: unknown) => number | undefined;
  getErrorMessage: (error: unknown) => string;
};

export function isBenignUpdateBranchFailure(
  error: unknown,
  callbacks: BranchUpdateErrorClassificationCallbacks
): boolean {
  const status = callbacks.getHttpStatus(error);
  const msg = callbacks.getErrorMessage(error).toLowerCase();

  if (status !== 422) return false;

  return (
    msg.includes('expected_head_sha') ||
    msg.includes('head sha') ||
    msg.includes('head branch was modified') ||
    msg.includes('not behind') ||
    msg.includes('up to date') ||
    msg.includes('up-to-date') ||
    msg.includes('already up') ||
    msg.includes('already up-to-date') ||
    msg.includes('already up to date')
  );
}

export function isManualUpdateBranchFailure(
  error: unknown,
  callbacks: BranchUpdateErrorClassificationCallbacks
): boolean {
  const status = callbacks.getHttpStatus(error);
  const msg = callbacks.getErrorMessage(error).toLowerCase();

  return (
    status === 403 ||
    status === 404 ||
    msg.includes('conflict') ||
    msg.includes('merge conflict') ||
    msg.includes('protected branch') ||
    msg.includes('permission') ||
    msg.includes('forbidden')
  );
}
