type RepoInfo = { owner: string; repo: string };

const AUTO_APPROVED_PR_HEADS = new Set<string>();

export function autoApprovedPrHeadKey(repoInfo: RepoInfo, prNumber: number, headSha: string): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${prNumber}:${headSha.trim()}`.toLowerCase();
}

export function markAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): void {
  const key = autoApprovedPrHeadKey(repoInfo, prNumber, headSha);
  if (key) AUTO_APPROVED_PR_HEADS.add(key);
}

export function hasAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): boolean {
  const key = autoApprovedPrHeadKey(repoInfo, prNumber, headSha);
  return Boolean(key && AUTO_APPROVED_PR_HEADS.has(key));
}
