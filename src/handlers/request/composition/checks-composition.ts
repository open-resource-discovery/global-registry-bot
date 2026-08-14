import type { CheckCompletedHandlerCallbacks } from '../application/check-completed-handler.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type CheckRunLikeBase = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  html_url?: string | null;
};

type CheckSuiteLikeBase = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  head_branch?: string | null;
};

type RegistryValidationArtifactsBase<MachineReadableSourceType> = {
  byFile: Map<string, string[]>;
  machineReadableSources: MachineReadableSourceType[];
};

type PullRequestHtmlContextBase = {
  octokit: {
    rest: {
      pulls: {
        get: (params: { owner: string; repo: string; pull_number: number }) => Promise<{ data: unknown }>;
      };
    };
  };
};

export type CheckCompletedCompositionDependencies<
  ContextType extends PullRequestHtmlContextBase,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
> = Omit<
  CheckCompletedHandlerCallbacks<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >,
  'readPullRequestHtmlUrl'
> & {
  toStringTrim: (value: unknown) => string;
};

export function composeCheckCompletedHandlerCallbacks<
  ContextType extends PullRequestHtmlContextBase,
  RepoInfoType extends RepoInfoBase,
  CheckRunType extends CheckRunLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
  RegistryValidationArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
  MachineReadableSourceType,
>(
  dependencies: CheckCompletedCompositionDependencies<
    ContextType,
    RepoInfoType,
    CheckRunType,
    CheckSuiteType,
    RegistryValidationArtifactsType,
    MachineReadableSourceType
  >
): CheckCompletedHandlerCallbacks<
  ContextType,
  RepoInfoType,
  CheckRunType,
  CheckSuiteType,
  RegistryValidationArtifactsType,
  MachineReadableSourceType
> {
  const { toStringTrim, ...callbacks } = dependencies;

  return {
    ...callbacks,

    readPullRequestHtmlUrl: async (context: ContextType, repoInfo: RepoInfoType, prNumber: number): Promise<string> => {
      const pr = await context.octokit.rest.pulls.get({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: prNumber,
      });

      return toStringTrim((pr.data as { html_url?: string })?.html_url);
    },
  };
}
