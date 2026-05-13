import { toStringTrim } from './login-utils.js';
import { sameRepoInfo } from './pull-request-repo-info.js';

type RepoInfo = { owner: string; repo: string };

export type PullRequestCompareCandidateArgs = {
  headSha: string;
  baseHeadSha: string;
  headRef: string;
  headRepoInfo: RepoInfo;
  repoInfo: RepoInfo;
  baseRef: string;
};

export function buildPullRequestCompareCandidates(args: PullRequestCompareCandidateArgs): string[] {
  const headSha = toStringTrim(args.headSha);
  const baseHeadSha = toStringTrim(args.baseHeadSha);
  const headRef = toStringTrim(args.headRef);
  const baseRef = toStringTrim(args.baseRef);
  const candidates: string[] = [`${headSha}...${baseHeadSha}`];

  if (!sameRepoInfo(args.headRepoInfo, args.repoInfo) && headRef) {
    candidates.push(`${args.headRepoInfo.owner}:${headRef}...${args.repoInfo.owner}:${baseRef}`);
  }

  return candidates;
}
