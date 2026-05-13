import { normalizeLogin, toStringTrim } from './login-utils.js';

type RepoInfo = { owner: string; repo: string };

type UserLike = { login?: string | null };

type PullRequestRepoLike = {
  name?: string | null;
  full_name?: string | null;
  owner?: UserLike | null;
};

type PullRequestLike = {
  head?: {
    repo?: PullRequestRepoLike | null;
  } | null;
};

function resolveRepoInfoFromRepoLike(repoLike: PullRequestRepoLike | null | undefined): RepoInfo | null {
  const fullName = toStringTrim(repoLike?.full_name);
  if (fullName) {
    const parts = fullName
      .split('/')
      .map((part) => toStringTrim(part))
      .filter(Boolean);

    if (parts.length === 2) {
      return { owner: parts[0], repo: parts[1] };
    }
  }

  const owner = normalizeLogin(repoLike?.owner?.login);
  const repo = toStringTrim(repoLike?.name);

  return owner && repo ? { owner, repo } : null;
}

export function sameRepoInfo(a: RepoInfo, b: RepoInfo): boolean {
  return a.owner.toLowerCase() === b.owner.toLowerCase() && a.repo.toLowerCase() === b.repo.toLowerCase();
}

export function resolvePullRequestHeadRepoInfo(pr: PullRequestLike, fallbackRepoInfo: RepoInfo): RepoInfo {
  return resolveRepoInfoFromRepoLike(pr.head?.repo) || fallbackRepoInfo;
}

export function isCrossRepositoryPullRequest(pr: PullRequestLike, baseRepoInfo: RepoInfo): boolean {
  return !sameRepoInfo(resolvePullRequestHeadRepoInfo(pr, baseRepoInfo), baseRepoInfo);
}
