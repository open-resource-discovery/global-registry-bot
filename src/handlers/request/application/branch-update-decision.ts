import { readBranchHeadShaFromResponse } from '../domain/branch-head-response.js';
import { buildPullRequestCompareCandidates } from '../domain/pull-request-compare-candidates.js';
import { evaluatePullRequestCompareResult } from '../domain/pull-request-compare-result.js';
import { isCrossRepositoryPullRequest, resolvePullRequestHeadRepoInfo } from '../domain/pull-request-repo-info.js';
import { isPullRequestBehindBase } from '../domain/pull-request-merge-state.js';
import { toStringTrim } from '../domain/login-utils.js';

type RepoInfo = { owner: string; repo: string };

type UserLike = { login?: string | null };

type PullRequestRepoLike = {
  name?: string | null;
  full_name?: string | null;
  owner?: UserLike | null;
};

type PullRequestBranchLike = {
  ref?: string | null;
  sha?: string | null;
  repo?: PullRequestRepoLike | null;
};

type PullRequestLike = {
  number: number;
  head?: PullRequestBranchLike | null;
  base?: PullRequestBranchLike | null;
  mergeable_state?: string | null;
};

type BranchHeadResponseLike = {
  data?: {
    commit?: {
      sha?: string | null;
    };
  };
};

type CompareCommitsResponseLike = {
  data?: {
    status?: string | null;
    ahead_by?: number | null;
  };
};

export type BranchUpdateDecisionCallbacks<ContextType> = {
  getBranch: (
    context: ContextType,
    args: { owner: string; repo: string; branch: string }
  ) => Promise<BranchHeadResponseLike>;
  compareCommitsWithBasehead: (
    context: ContextType,
    args: { owner: string; repo: string; basehead: string }
  ) => Promise<CompareCommitsResponseLike>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
};

export async function readBranchHeadSha<ContextType>(
  context: ContextType,
  repoInfo: RepoInfo,
  branchName: string,
  callbacks: BranchUpdateDecisionCallbacks<ContextType>
): Promise<string> {
  const branch = toStringTrim(branchName);
  if (!branch) return '';

  try {
    const res = await callbacks.getBranch(context, {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      branch,
    });

    return readBranchHeadShaFromResponse(res);
  } catch (error: unknown) {
    callbacks.log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        branch,
        err: callbacks.getErrorMessage(error),
        status: callbacks.getHttpStatus(error),
      },
      'branch-head-sha:read-failed'
    );

    return '';
  }
}

export async function isPullRequestBehindCurrentBase<ContextType>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string,
  callbacks: BranchUpdateDecisionCallbacks<ContextType>
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
  const headRef = toStringTrim(pr.head?.ref);
  const baseRef = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);

  if (!headSha || !baseRef) return false;

  const baseHeadSha = await readBranchHeadSha(context, repoInfo, baseRef, callbacks);
  if (!baseHeadSha || baseHeadSha === headSha) return false;

  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const candidates = buildPullRequestCompareCandidates({
    headSha,
    baseHeadSha,
    headRef,
    headRepoInfo,
    repoInfo,
    baseRef,
  });

  for (const basehead of candidates) {
    try {
      const res = await callbacks.compareCommitsWithBasehead(context, {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        basehead,
      });

      const compareResult = evaluatePullRequestCompareResult(res?.data);

      callbacks.log(
        context,
        'info',
        {
          prNumber: pr.number,
          basehead,
          status: compareResult.status,
          aheadBy: compareResult.aheadBy,
          headSha,
          baseHeadSha,
          crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        },
        'pull-request behind-current-base compare'
      );

      if (compareResult.isBehindCurrentBase === true) return true;
      if (compareResult.isBehindCurrentBase === false) return false;
    } catch (error: unknown) {
      callbacks.log(
        context,
        'warn',
        {
          prNumber: pr.number,
          basehead,
          headSha,
          baseBranch: baseRef,
          baseHeadSha,
          err: callbacks.getErrorMessage(error),
          status: callbacks.getHttpStatus(error),
          crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        },
        'pull-request behind-current-base compare failed'
      );
    }
  }

  return isPullRequestBehindBase(pr);
}

export async function shouldUpdatePullRequestBranch<ContextType>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string,
  callbacks: BranchUpdateDecisionCallbacks<ContextType>
): Promise<boolean> {
  if (isPullRequestBehindBase(pr)) return true;
  return await isPullRequestBehindCurrentBase(context, repoInfo, pr, baseBranch, callbacks);
}
