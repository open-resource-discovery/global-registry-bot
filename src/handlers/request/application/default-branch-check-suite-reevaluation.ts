import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type CheckSuiteLikeBase = {
  head_branch?: string | null;
  head_sha?: string | null;
  conclusion?: string | null;
  status?: string | null;
};

type SequentialRegistryPrResultLike = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

export type DefaultBranchCheckSuiteReevaluationCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  ResultType extends SequentialRegistryPrResultLike,
> = {
  readDefaultBranchFromPayload: (payload: unknown) => string;
  getBranch: (
    context: ContextType,
    args: { owner: string; repo: string; branch: string }
  ) => Promise<{ data?: { commit?: { sha?: string | null } } }>;
  logBranchHeadReadFailed: (
    context: ContextType,
    args: {
      repoInfo: RepoInfoType;
      defaultBranch: string;
      headSha: string;
      errorMessage: string;
      status: number | undefined;
    }
  ) => void;
  logEvaluated: (
    context: ContextType,
    args: {
      repoInfo: RepoInfoType;
      defaultBranch: string;
      headBranch: string;
      headSha: string;
      defaultBranchHeadSha: string;
      conclusion: string;
      status: string;
      isDefaultBranchSuite: boolean;
    }
  ) => void;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  getStaticConfig: (context: ContextType, options: { forceReload: true }) => Promise<unknown>;
  reevaluateOpenDirectPullRequestsAfterDefaultBranchPush: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<ResultType>;
  updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string
  ) => Promise<boolean>;
};

export async function maybeHandleDefaultBranchCheckSuiteSuccess<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  ResultType extends SequentialRegistryPrResultLike,
>(
  context: ContextType,
  payload: unknown,
  checkSuite: CheckSuiteType | null,
  repoInfo: RepoInfoType,
  callbacks: DefaultBranchCheckSuiteReevaluationCallbacks<ContextType, RepoInfoType, ResultType>
): Promise<void> {
  const defaultBranch = callbacks.readDefaultBranchFromPayload(payload);
  const headBranch = toStringTrim(checkSuite?.head_branch);
  const headSha = toStringTrim(checkSuite?.head_sha);
  const conclusion = toStringTrim(checkSuite?.conclusion).toLowerCase();
  const status = toStringTrim(checkSuite?.status).toLowerCase();

  let isDefaultBranchSuite = Boolean(defaultBranch && headBranch && headBranch === defaultBranch);
  let defaultBranchHeadSha = '';

  if (!isDefaultBranchSuite && defaultBranch && headSha) {
    try {
      const branch = await callbacks.getBranch(context, {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        branch: defaultBranch,
      });

      defaultBranchHeadSha = toStringTrim(branch?.data?.commit?.sha);
      isDefaultBranchSuite = Boolean(defaultBranchHeadSha && defaultBranchHeadSha === headSha);
    } catch (error: unknown) {
      callbacks.logBranchHeadReadFailed(context, {
        repoInfo,
        defaultBranch,
        headSha,
        errorMessage: callbacks.getErrorMessage(error),
        status: callbacks.getHttpStatus(error),
      });
    }
  }

  callbacks.logEvaluated(context, {
    repoInfo,
    defaultBranch,
    headBranch,
    headSha,
    defaultBranchHeadSha,
    conclusion,
    status,
    isDefaultBranchSuite,
  });

  if (!isDefaultBranchSuite) return;
  if (status && status !== 'completed') return;
  if (conclusion !== 'success') return;

  await callbacks.getStaticConfig(context, { forceReload: true });

  const directResult = await callbacks.reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
    context,
    repoInfo,
    defaultBranch,
    'default-branch-check-suite:direct-pr-reevaluation'
  );

  if (!directResult.updated && !directResult.processed && !directResult.blockedByActive) {
    await callbacks.updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(
      context,
      repoInfo,
      defaultBranch
    );
  }
}
