import YAML from 'yaml';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number: number;
  head?: {
    ref?: string | null;
    sha?: string | null;
  } | null;
};

export type ReadRegistryDocForApprovalCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  readPullRequestHeadFileText: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    filePath: string
  ) => Promise<string | null>;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
};

export async function readRegistryDocForApproval<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  filePath: string,
  callbacks: ReadRegistryDocForApprovalCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<Record<string, unknown> | null> {
  const raw = await callbacks.readPullRequestHeadFileText(context, repoInfo, pr, filePath);
  if (!raw) return null;

  try {
    const parsed = YAML.parse(raw) as unknown;
    return callbacks.isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}
