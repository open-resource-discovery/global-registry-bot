import type { DefaultBranchCheckSuiteReevaluationCallbacks } from '../application/default-branch-check-suite-reevaluation.js';
import {
  createGitHubRepoBranchGateway,
  type GitHubRepoBranchGatewayContext,
  type RepoBranchResult,
} from '../infrastructure/github-gateway.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type BranchLookupArgs = {
  owner: string;
  repo: string;
  branch: string;
};

type BranchLookupResult = RepoBranchResult;

type BranchLookupContextBase = GitHubRepoBranchGatewayContext;

type SequentialRegistryPrResultBase = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type LogFn<ContextType> = (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;

export type DefaultBranchCheckSuiteCompositionDependencies<
  ContextType extends BranchLookupContextBase,
  RepoInfoType extends RepoInfoBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
> = Omit<
  DefaultBranchCheckSuiteReevaluationCallbacks<ContextType, RepoInfoType, SequentialRegistryPrResultType>,
  'getBranch' | 'logBranchHeadReadFailed' | 'logEvaluated'
> & {
  log: LogFn<ContextType>;
};

export function composeDefaultBranchCheckSuiteReevaluationCallbacks<
  ContextType extends BranchLookupContextBase,
  RepoInfoType extends RepoInfoBase,
  SequentialRegistryPrResultType extends SequentialRegistryPrResultBase,
>(
  dependencies: DefaultBranchCheckSuiteCompositionDependencies<
    ContextType,
    RepoInfoType,
    SequentialRegistryPrResultType
  >
): DefaultBranchCheckSuiteReevaluationCallbacks<ContextType, RepoInfoType, SequentialRegistryPrResultType> {
  return {
    readDefaultBranchFromPayload: dependencies.readDefaultBranchFromPayload,

    getBranch: async (context: ContextType, args: BranchLookupArgs): Promise<BranchLookupResult> =>
      await createGitHubRepoBranchGateway(context).getBranch(args),

    logBranchHeadReadFailed: (
      context: ContextType,
      args: {
        repoInfo: RepoInfoType;
        defaultBranch: string;
        headSha: string;
        errorMessage: string;
        status: number | undefined;
      }
    ): void => {
      dependencies.log(
        context,
        'warn',
        {
          owner: args.repoInfo.owner,
          repo: args.repoInfo.repo,
          defaultBranch: args.defaultBranch,
          headSha: args.headSha,
          err: args.errorMessage,
          status: args.status,
        },
        'default-branch-check-suite:branch-head-read-failed'
      );
    },

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
    ): void => {
      dependencies.log(
        context,
        'info',
        {
          owner: args.repoInfo.owner,
          repo: args.repoInfo.repo,
          defaultBranch: args.defaultBranch,
          headBranch: args.headBranch,
          headSha: args.headSha,
          defaultBranchHeadSha: args.defaultBranchHeadSha,
          conclusion: args.conclusion,
          status: args.status,
          isDefaultBranchSuite: args.isDefaultBranchSuite,
        },
        'default-branch-check-suite:evaluated'
      );
    },

    getErrorMessage: dependencies.getErrorMessage,
    getHttpStatus: dependencies.getHttpStatus,
    getStaticConfig: dependencies.getStaticConfig,
    reevaluateOpenDirectPullRequestsAfterDefaultBranchPush:
      dependencies.reevaluateOpenDirectPullRequestsAfterDefaultBranchPush,
    updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry:
      dependencies.updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry,
  };
}
