import YAML from 'yaml';

export type RepoInfoBase = {
  owner: string;
  repo: string;
};

export type RepoContentFile = {
  content?: string;
  encoding?: string;
};

type RepoContentResult = {
  data?: unknown;
};

type RepoFilesContext = {
  octokit: {
    repos: {
      getContent: (params: { owner: string; repo: string; path: string }) => Promise<RepoContentResult>;
    };
  };
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

export function isRepoContentFile(value: unknown): value is RepoContentFile {
  return isPlainObject(value) && typeof value['content'] === 'string';
}

export async function readRepoFileText<ContextType extends RepoFilesContext, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repo: RepoInfoType,
  filePath: string
): Promise<string | null> {
  const path = toStringTrim(filePath).replace(/^\/+/, '');
  if (!path) return null;

  try {
    const res = await context.octokit.repos.getContent({
      owner: repo.owner,
      repo: repo.repo,
      path,
    });

    const data = (res as unknown as { data?: unknown }).data;

    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    const encoding = typeof data.encoding === 'string' ? data.encoding : 'base64';
    return Buffer.from(String(data.content || ''), encoding as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

export async function readYamlFromRepo<ContextType extends RepoFilesContext, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repo: RepoInfoType,
  filePath: string
): Promise<unknown | null> {
  const text = await readRepoFileText(context, repo, filePath);
  if (!text) return null;

  try {
    return YAML.parse(text);
  } catch {
    return null;
  }
}
