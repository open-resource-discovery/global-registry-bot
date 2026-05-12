class CooldownUntilMap extends Map<string, number> {
  public override get(key: string): number | undefined {
    const until = super.get(key);
    if (until !== undefined && until <= Date.now()) {
      super.delete(key);
      return undefined;
    }
    return until;
  }

  public override has(key: string): boolean {
    return this.get(key) !== undefined;
  }
}

const UPDATE_BRANCH_COOLDOWN_UNTIL = new CooldownUntilMap();
const UPDATE_BRANCH_COOLDOWN_MS = 15000;

export function isUpdateBranchCooldownActive(key: string): boolean {
  const until = UPDATE_BRANCH_COOLDOWN_UNTIL.get(key);
  // eslint-disable-next-line eqeqeq
  if (until == null) return false;

  if (until <= Date.now()) {
    UPDATE_BRANCH_COOLDOWN_UNTIL.delete(key);
    return false;
  }

  return true;
}

export function markUpdateBranchCooldown(key: string): void {
  UPDATE_BRANCH_COOLDOWN_UNTIL.set(key, Date.now() + UPDATE_BRANCH_COOLDOWN_MS);
}
