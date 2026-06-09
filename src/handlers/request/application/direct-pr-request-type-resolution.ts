type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  base?: { ref?: string | null } | null;
};

type DirectPrApprovalOptions = {
  baseBranch?: string;
};

export type DirectPrRequestTypeResolutionCallbacks<ContextType, PullRequestType extends PullRequestLike> = {
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  readRegistryDocForApproval: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    filePath: string
  ) => Promise<Record<string, unknown> | null>;
  pickRequestTypeForChangedResource: (context: ContextType, filePath: string, doc: Record<string, unknown>) => string;
};

export async function resolveDirectPrRequestTypes<ContextType, PullRequestType extends PullRequestLike>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  options: DirectPrApprovalOptions = {},
  callbacks: DirectPrRequestTypeResolutionCallbacks<ContextType, PullRequestType>
): Promise<string[]> {
  const changedFiles = await callbacks.listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, options.baseBranch);
  const requestTypes: string[] = [];

  for (const filePath of changedFiles) {
    const parsed = await callbacks.readRegistryDocForApproval(context, repoInfo, pr, filePath);
    if (!parsed) continue;

    const requestType = callbacks.pickRequestTypeForChangedResource(context, filePath, parsed);
    if (!requestType) continue;

    requestTypes.push(requestType);
  }

  return Array.from(new Set(requestTypes));
}
