import { toStringTrim } from '../domain/login-utils.js';

type RepoInfo = { owner: string; repo: string };

export type PullRequestBranchUpdateCallContext = {
  octokit: {
    rest: {
      pulls: {
        updateBranch: (args: {
          owner: string;
          repo: string;
          pull_number: number;
          expected_head_sha?: string;
        }) => Promise<unknown>;
      };
    };
  };
};

export async function callPullRequestBranchUpdate<ContextType extends PullRequestBranchUpdateCallContext>(
  context: ContextType,
  repoInfo: RepoInfo,
  prNumber: number,
  expectedHeadSha?: string
): Promise<void> {
  const args: {
    owner: string;
    repo: string;
    pull_number: number;
    expected_head_sha?: string;
  } = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    pull_number: prNumber,
  };

  const normalizedExpectedHeadSha = toStringTrim(expectedHeadSha);
  if (normalizedExpectedHeadSha) {
    args.expected_head_sha = normalizedExpectedHeadSha;
  }

  await context.octokit.rest.pulls.updateBranch(args);
}
