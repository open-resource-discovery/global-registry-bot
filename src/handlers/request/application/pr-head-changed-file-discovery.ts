import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type PullRequestRepoLikeBase = {
  name?: string | null;
  full_name?: string | null;
  owner?: {
    login?: string | null;
  } | null;
};

type PullRequestBranchLikeBase = {
  ref?: string | null;
  sha?: string | null;
  repo?: PullRequestRepoLikeBase | null;
};

type PullRequestLikeBase = {
  number: number;
  head: PullRequestBranchLikeBase;
  base?: PullRequestBranchLikeBase | null;
};

type PullRequestFileLikeBase = {
  filename?: string | null;
  status?: string | null;
};

type GitTreeEntryLikeBase = {
  path?: string | null;
  type?: string | null;
  sha?: string | null;
};

type RepoContentFileBase = {
  content?: string;
  encoding?: string;
};

type ContextWithPullFiles<PullRequestFileType extends PullRequestFileLikeBase> = {
  octokit: {
    pulls: {
      listFiles: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: PullRequestFileType[] }>;
    };
  };
};

type ContextWithGitTree<GitTreeEntryType extends GitTreeEntryLikeBase> = {
  octokit: {
    git: {
      getTree: (args: {
        owner: string;
        repo: string;
        tree_sha: string;
        recursive?: 'true';
      }) => Promise<{ data?: { tree?: GitTreeEntryType[] } }>;
    };
  };
};

type ContextWithRepoContent = {
  octokit: {
    repos: {
      getContent: (args: { owner: string; repo: string; path: string; ref?: string }) => Promise<{ data?: unknown }>;
    };
  };
};

export type PullRequestHeadReadCandidate<RepoInfoType extends RepoInfoBase = RepoInfoBase> = {
  repoInfo: RepoInfoType;
  ref: string;
  source: string;
};

type YamlCandidateCallbacks = {
  normalizeRepoPath: (value: unknown) => string;
  isYamlPath: (path: string) => boolean;
};

type ListChangedYamlFilesForPrCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestFileType extends PullRequestFileLikeBase,
> = {
  listChangedYamlFilesPage: (
    context: ContextType,
    repoInfo: RepoInfoType,
    prNumber: number,
    page: number
  ) => Promise<PullRequestFileType[]>;
  isChangedYamlCandidate: (file: PullRequestFileType) => string;
  isRegistryEntryPath: (context: ContextType, filePath: string) => boolean;
};

type GitTreeReadCallbacks<ContextType> = {
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  log: (context: ContextType, level: 'warn', obj: unknown, msg: string) => void;
};

type RegistryYamlTreeEntryPathCallbacks<ContextType> = {
  normalizeRepoPath: (value: unknown) => string;
  isYamlPath: (path: string) => boolean;
  isRegistryEntryPath: (context: ContextType, filePath: string) => boolean;
};

type BuildPullRequestHeadReadCandidatesCallbacks<
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  resolvePullRequestHeadRepoInfo: (pr: PullRequestType, fallbackRepoInfo: RepoInfoType) => RepoInfoType;
  sameRepoInfo: (a: RepoInfoType, b: RepoInfoType) => boolean;
};

type ReadRepoFileTextAtRefCallbacks = {
  normalizeRepoPath: (value: unknown) => string;
  isRepoContentFile: (value: unknown) => boolean;
};

type ReadPullRequestHeadFileTextCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  normalizeRepoPath: (value: unknown) => string;
  buildPullRequestHeadReadCandidates: (
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => PullRequestHeadReadCandidate<RepoInfoType>[];
  readRepoFileTextAtRef: (
    context: ContextType,
    repoInfo: RepoInfoType,
    path: string,
    ref: string
  ) => Promise<string | null>;
  resolvePullRequestHeadRepoInfo: (pr: PullRequestType, fallbackRepoInfo: RepoInfoType) => RepoInfoType;
  isCrossRepositoryPullRequest: (pr: PullRequestType, baseRepoInfo: RepoInfoType) => boolean;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
};

type ReadPullRequestHeadTreeEntriesCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  GitTreeEntryType extends GitTreeEntryLikeBase,
> = {
  resolvePullRequestHeadRepoInfo: (pr: PullRequestType, fallbackRepoInfo: RepoInfoType) => RepoInfoType;
  sameRepoInfo: (a: RepoInfoType, b: RepoInfoType) => boolean;
  readRecursiveGitTreeEntries: (
    context: ContextType,
    repoInfo: RepoInfoType,
    ref: string
  ) => Promise<GitTreeEntryType[]>;
  isCrossRepositoryPullRequest: (pr: PullRequestType, baseRepoInfo: RepoInfoType) => boolean;
  log: (context: ContextType, level: 'info', obj: unknown, msg: string) => void;
};

type ListChangedYamlFilesForPrAgainstCurrentBaseCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  GitTreeEntryType extends GitTreeEntryLikeBase,
> = {
  readBranchHeadSha: (context: ContextType, repoInfo: RepoInfoType, branchName: string) => Promise<string>;
  readRecursiveGitTreeEntries: (
    context: ContextType,
    repoInfo: RepoInfoType,
    ref: string
  ) => Promise<GitTreeEntryType[]>;
  readPullRequestHeadTreeEntries: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType
  ) => Promise<GitTreeEntryType[]>;
  registryYamlTreeEntryPath: (context: ContextType, entry: GitTreeEntryType) => string;
};

type ListChangedYamlFilesForPrWithFallbackCallbacks<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
> = {
  listChangedYamlFilesForPr: (context: ContextType, repoInfo: RepoInfoType, prNumber: number) => Promise<string[]>;
  listChangedYamlFilesForPrAgainstCurrentBase: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pr: PullRequestType,
    baseBranch: string
  ) => Promise<string[]>;
  log: (context: ContextType, level: 'info', obj: unknown, msg: string) => void;
};

export function isChangedYamlCandidate<PullRequestFileType extends PullRequestFileLikeBase>(
  file: PullRequestFileType,
  callbacks: YamlCandidateCallbacks
): string {
  const filename = callbacks.normalizeRepoPath(file?.filename);
  const status = toStringTrim(file?.status).toLowerCase();

  if (!filename || !callbacks.isYamlPath(filename) || status === 'removed') return '';
  return filename;
}

export async function listChangedYamlFilesPage<
  ContextType extends ContextWithPullFiles<PullRequestFileType>,
  RepoInfoType extends RepoInfoBase,
  PullRequestFileType extends PullRequestFileLikeBase,
>(context: ContextType, repoInfo: RepoInfoType, prNumber: number, page: number): Promise<PullRequestFileType[]> {
  const res = await context.octokit.pulls.listFiles({
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    pull_number: prNumber,
    per_page: 100,
    page,
  });

  return Array.isArray(res?.data) ? res.data : [];
}

export async function listChangedYamlFilesForPr<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestFileType extends PullRequestFileLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  prNumber: number,
  callbacks: ListChangedYamlFilesForPrCallbacks<ContextType, RepoInfoType, PullRequestFileType>
): Promise<string[]> {
  const out: string[] = [];
  let page = 1;

  while (true) {
    const files = await callbacks.listChangedYamlFilesPage(context, repoInfo, prNumber, page);
    if (!files.length) break;

    for (const file of files) {
      const filename = callbacks.isChangedYamlCandidate(file);
      if (filename && callbacks.isRegistryEntryPath(context, filename)) out.push(filename);
    }

    if (files.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return Array.from(new Set(out));
}

export async function readRecursiveGitTreeEntries<
  ContextType extends ContextWithGitTree<GitTreeEntryType>,
  RepoInfoType extends RepoInfoBase,
  GitTreeEntryType extends GitTreeEntryLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  ref: string,
  callbacks: GitTreeReadCallbacks<ContextType>
): Promise<GitTreeEntryType[]> {
  const treeSha = toStringTrim(ref);
  if (!treeSha) return [];

  try {
    const res = await context.octokit.git.getTree({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      tree_sha: treeSha,
      recursive: 'true',
    });

    return Array.isArray(res?.data?.tree) ? res.data.tree : [];
  } catch (error: unknown) {
    callbacks.log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        ref: treeSha,
        err: callbacks.getErrorMessage(error),
        status: callbacks.getHttpStatus(error),
      },
      'git-tree:read-failed'
    );

    return [];
  }
}

export function registryYamlTreeEntryPath<ContextType, GitTreeEntryType extends GitTreeEntryLikeBase>(
  context: ContextType,
  entry: GitTreeEntryType,
  callbacks: RegistryYamlTreeEntryPathCallbacks<ContextType>
): string {
  const path = callbacks.normalizeRepoPath(entry?.path);
  const type = toStringTrim(entry?.type).toLowerCase();

  if (type !== 'blob') return '';
  if (!path || !callbacks.isYamlPath(path)) return '';
  if (!callbacks.isRegistryEntryPath(context, path)) return '';

  return path;
}

export function buildPullRequestHeadReadCandidates<
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: BuildPullRequestHeadReadCandidatesCallbacks<RepoInfoType, PullRequestType>
): PullRequestHeadReadCandidate<RepoInfoType>[] {
  const headRepoInfo = callbacks.resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const headSha = toStringTrim(pr.head?.sha);
  const headRef = toStringTrim(pr.head?.ref);
  const isCrossRepo = !callbacks.sameRepoInfo(headRepoInfo, repoInfo);

  const out: PullRequestHeadReadCandidate<RepoInfoType>[] = [];
  const seen = new Set<string>();

  const add = (candidateRepoInfo: RepoInfoType, ref: string, source: string): void => {
    const normalizedRef = toStringTrim(ref);
    if (!normalizedRef) return;

    const key = `${candidateRepoInfo.owner}/${candidateRepoInfo.repo}:${normalizedRef}`;
    if (seen.has(key)) return;
    seen.add(key);

    out.push({
      repoInfo: candidateRepoInfo,
      ref: normalizedRef,
      source,
    });
  };

  add(repoInfo, headSha, 'base-repo:head-sha');
  add(repoInfo, `refs/pull/${pr.number}/head`, 'base-repo:pull-ref-full');
  add(repoInfo, `pull/${pr.number}/head`, 'base-repo:pull-ref-short');

  if (!isCrossRepo) {
    add(repoInfo, headRef, 'base-repo:head-ref');
  }

  add(headRepoInfo, headSha, 'head-repo:head-sha');
  add(headRepoInfo, headRef, 'head-repo:head-ref');

  return out;
}

export async function readRepoFileTextAtRef<
  ContextType extends ContextWithRepoContent,
  RepoInfoType extends RepoInfoBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  path: string,
  ref: string,
  callbacks: ReadRepoFileTextAtRefCallbacks
): Promise<string | null> {
  const p = callbacks.normalizeRepoPath(path);
  const branchRef = toStringTrim(ref);
  if (!p || !branchRef) return null;

  try {
    const res = await context.octokit.repos.getContent({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      path: p,
      ref: branchRef,
    });

    const data = res?.data;
    if (Array.isArray(data) || !callbacks.isRepoContentFile(data)) return null;

    const file = data as RepoContentFileBase;
    const enc = typeof file.encoding === 'string' ? file.encoding : 'base64';
    return Buffer.from(String(file.content || ''), enc as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

export async function readPullRequestHeadFileText<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  path: string,
  callbacks: ReadPullRequestHeadFileTextCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<string | null> {
  const candidates = callbacks.buildPullRequestHeadReadCandidates(repoInfo, pr);
  const normalizedPath = callbacks.normalizeRepoPath(path);

  for (const candidate of candidates) {
    const raw = await callbacks.readRepoFileTextAtRef(context, candidate.repoInfo, normalizedPath, candidate.ref);
    if (raw === null) continue;

    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        path: normalizedPath,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'pull-request head file resolved'
    );

    return raw;
  }

  const headRepoInfo = callbacks.resolvePullRequestHeadRepoInfo(pr, repoInfo);

  callbacks.log(
    context,
    'warn',
    {
      prNumber: pr.number,
      path: normalizedPath,
      baseOwner: repoInfo.owner,
      baseRepo: repoInfo.repo,
      headOwner: headRepoInfo.owner,
      headRepo: headRepoInfo.repo,
      headRef: toStringTrim(pr.head?.ref),
      headSha: toStringTrim(pr.head?.sha),
      crossRepo: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
      candidates: candidates.map((candidate) => ({
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
      })),
    },
    'pull-request head file read failed'
  );

  return null;
}

export async function readPullRequestHeadTreeEntries<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  GitTreeEntryType extends GitTreeEntryLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  callbacks: ReadPullRequestHeadTreeEntriesCallbacks<ContextType, RepoInfoType, PullRequestType, GitTreeEntryType>
): Promise<GitTreeEntryType[]> {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return [];

  const headRepoInfo = callbacks.resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const candidates: PullRequestHeadReadCandidate<RepoInfoType>[] = [
    {
      repoInfo,
      ref: headSha,
      source: 'base-repo:head-sha',
    },
  ];

  if (!callbacks.sameRepoInfo(headRepoInfo, repoInfo)) {
    candidates.push({
      repoInfo: headRepoInfo,
      ref: headSha,
      source: 'head-repo:head-sha',
    });
  }

  for (const candidate of candidates) {
    const entries = await callbacks.readRecursiveGitTreeEntries(context, candidate.repoInfo, candidate.ref);
    if (!entries.length) continue;

    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: callbacks.isCrossRepositoryPullRequest(pr, repoInfo),
        entries: entries.length,
      },
      'pull-request head tree resolved'
    );

    return entries;
  }

  return [];
}

export async function listChangedYamlFilesForPrAgainstCurrentBase<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  GitTreeEntryType extends GitTreeEntryLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  baseBranch: string,
  callbacks: ListChangedYamlFilesForPrAgainstCurrentBaseCallbacks<
    ContextType,
    RepoInfoType,
    PullRequestType,
    GitTreeEntryType
  >
): Promise<string[]> {
  const baseRef = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!baseRef) return [];

  const baseSha = await callbacks.readBranchHeadSha(context, repoInfo, baseRef);
  if (!baseSha) return [];

  const [baseEntries, headEntries] = await Promise.all([
    callbacks.readRecursiveGitTreeEntries(context, repoInfo, baseSha),
    callbacks.readPullRequestHeadTreeEntries(context, repoInfo, pr),
  ]);

  const baseByPath = new Map<string, string>();

  for (const entry of baseEntries) {
    const path = callbacks.registryYamlTreeEntryPath(context, entry);
    if (!path) continue;

    const sha = toStringTrim(entry.sha);
    if (sha) baseByPath.set(path, sha);
  }

  const changed: string[] = [];
  const seen = new Set<string>();

  for (const entry of headEntries) {
    const path = callbacks.registryYamlTreeEntryPath(context, entry);
    if (!path || seen.has(path)) continue;

    const headEntrySha = toStringTrim(entry.sha);
    const baseEntrySha = baseByPath.get(path) || '';

    if (!headEntrySha) continue;
    if (baseEntrySha && baseEntrySha === headEntrySha) continue;

    seen.add(path);
    changed.push(path);
  }

  return changed;
}

export async function listChangedYamlFilesForPrWithFallback<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  pr: PullRequestType,
  baseBranch: string | undefined,
  callbacks: ListChangedYamlFilesForPrWithFallbackCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<string[]> {
  const fromPullFiles = await callbacks.listChangedYamlFilesForPr(context, repoInfo, pr.number);
  if (fromPullFiles.length) return fromPullFiles;

  const fallbackBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!fallbackBaseBranch) return [];

  const fromTreeDiff = await callbacks.listChangedYamlFilesForPrAgainstCurrentBase(
    context,
    repoInfo,
    pr,
    fallbackBaseBranch
  );

  if (fromTreeDiff.length) {
    callbacks.log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        baseBranch: fallbackBaseBranch,
        changedRegistryFiles: fromTreeDiff,
      },
      'changed-registry-files:fallback-tree-diff'
    );
  }

  return fromTreeDiff;
}
