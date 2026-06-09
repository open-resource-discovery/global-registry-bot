type RepoInfoBase = { owner: string; repo: string };

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type RegistryValidationArtifactsBase<MachineReadableSourceType> = {
  byFile: Map<string, string[]>;
  machineReadableSources: MachineReadableSourceType[];
};

export type CheckSuiteCiCommentingCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  MachineReadableSourceType,
> = {
  collapseBotCommentsByPrefix: (
    context: ContextType,
    params: IssueParamsBase,
    options: {
      tagPrefix: string;
      keepTags?: string[];
      collapseBody?: string;
      classifier?: 'OUTDATED' | 'RESOLVED' | 'DUPLICATE' | 'OFF_TOPIC' | 'SPAM' | 'ABUSE';
    }
  ) => Promise<void>;
  buildRegistryValidationAggregatePrCommentBody: (
    context: ContextType,
    repoInfo: RepoInfoType,
    byFile: Map<string, string[]>,
    machineReadableSources: MachineReadableSourceType[]
  ) => Promise<string>;
  postOnce: (
    context: ContextType,
    params: IssueParamsBase,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  onBeforePost?: (context: ContextType, args: { prNumber: number; files: string[]; bodyLength: number }) => void;
};

export async function postCheckSuiteRegistryValidationComments<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  MachineReadableSourceType,
  ArtifactsType extends RegistryValidationArtifactsBase<MachineReadableSourceType>,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  prNumbers: number[],
  artifacts: ArtifactsType,
  minimizeTag: string,
  callbacks: CheckSuiteCiCommentingCallbacks<ContextType, RepoInfoType, MachineReadableSourceType>
): Promise<void> {
  const currentCiTags = [minimizeTag];

  for (const prNumber of prNumbers) {
    await callbacks.collapseBotCommentsByPrefix(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: prNumber },
      {
        tagPrefix: minimizeTag,
        keepTags: currentCiTags,
        collapseBody: 'Validation issues resolved.',
        classifier: 'RESOLVED',
      }
    );
  }

  const body = await callbacks.buildRegistryValidationAggregatePrCommentBody(
    context,
    repoInfo,
    artifacts.byFile,
    artifacts.machineReadableSources
  );
  if (!body) return;

  for (const prNumber of prNumbers) {
    callbacks.onBeforePost?.(context, {
      prNumber,
      files: Array.from(artifacts.byFile.keys()),
      bodyLength: body.length,
    });

    await callbacks.postOnce(context, { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: prNumber }, body, {
      minimizeTag,
    });
  }
}
