import type { BranchUpdateRefreshOutcome } from './branch-update-refresh-outcome.js';

export type BranchUpdateRetryPlan = {
  action: 'skip-head-changed' | 'retry' | 'skip-not-behind';
  shouldRetry: boolean;
};

export function planBranchUpdateRetryAfterRefresh(refreshOutcome: BranchUpdateRefreshOutcome): BranchUpdateRetryPlan {
  if (refreshOutcome.headChanged) {
    return {
      action: 'skip-head-changed',
      shouldRetry: false,
    };
  }

  if (refreshOutcome.shouldRetry) {
    return {
      action: 'retry',
      shouldRetry: true,
    };
  }

  return {
    action: 'skip-not-behind',
    shouldRetry: false,
  };
}
