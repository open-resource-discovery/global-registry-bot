const UPDATE_BRANCH_INFLIGHT = new Map<string, Promise<boolean>>();

export function getUpdateBranchInflight(key: string): Promise<boolean> | undefined {
  return UPDATE_BRANCH_INFLIGHT.get(key);
}

export function setUpdateBranchInflight(key: string, pending: Promise<boolean>): void {
  UPDATE_BRANCH_INFLIGHT.set(key, pending);
}

export function clearUpdateBranchInflight(key: string): void {
  UPDATE_BRANCH_INFLIGHT.delete(key);
}
