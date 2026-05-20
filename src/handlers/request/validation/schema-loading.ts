import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

type RepoContentFile = { content: string; encoding?: string };
type RepoContentResponse = RepoContentFile | RepoContentFile[];

type SchemaLoaderContext = {
  octokit?: {
    repos: {
      getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data: RepoContentResponse }>;
    };
  };
};

const REPO_SCHEMA_CACHE = new Map<string, unknown>();

export async function loadSchemaLocal(args: {
  dirName: string;
  schemaPath: unknown;
  toStringSafe: (value: unknown) => string;
}): Promise<unknown> {
  const want = args.toStringSafe(args.schemaPath) || 'namespace.schema.json';
  const cleanedWant = want.replace(/^\.?\//, '');

  const srcDir = resolve(args.dirName, '../../..');
  const projectRoot = resolve(srcDir, '..');

  const candidates: string[] = [
    cleanedWant,
    `./${cleanedWant}`,
    `../${cleanedWant}`,
    `../../${cleanedWant}`,
    `../../../${cleanedWant}`,
    resolve(srcDir, 'schemas', cleanedWant),
    resolve(projectRoot, 'src', 'schemas', cleanedWant),
    resolve(process.cwd(), 'src', 'schemas', cleanedWant),
    resolve(process.cwd(), cleanedWant),
  ];

  const errors: string[] = [];
  for (const cand of candidates) {
    const abs = resolve(args.dirName, cand);
    try {
      const buf = await readFile(abs, 'utf8');
      return JSON.parse(buf);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`'${cand}': ${msg}`);
    }
  }

  throw new Error(`Failed to load schema (tried: ${candidates.join(', ')}). Errors: ${errors.join(' | ')}`);
}

export async function loadSchemaFromRepoOrLocal(args: {
  context: SchemaLoaderContext;
  owner: string;
  repo: string;
  schemaPath: unknown;
  dirName: string;
  configBaseDir: string;
  getHttpStatus: (err: unknown) => number | undefined;
  isRepoContentFile: (value: unknown) => value is RepoContentFile;
  toStringSafe: (value: unknown) => string;
}): Promise<unknown> {
  const raw = args.toStringSafe(args.schemaPath);
  if (!raw) return null;

  const octokit = args.context?.octokit;
  const searchPaths = ['.github/registry-bot/request-schemas', 'schema', '.'];

  const addCandidate = (set: Set<string>, p: unknown): void => {
    const cleaned = args.toStringSafe(p).replace(/^\/+/, '');
    if (cleaned) set.add(cleaned);
  };

  const cacheRepoKeyBase = octokit && args.owner && args.repo ? `${String(args.owner)}/${String(args.repo)}` : '';

  if (octokit && args.owner && args.repo) {
    const candidates = new Set<string>();

    if (raw.startsWith('/')) {
      // Explicit repo-absolute path
      addCandidate(candidates, raw);
    } else {
      const cleaned = raw.replace(/^\.?\//, '');

      const isRepoRelativeConfigPath = cleaned.startsWith(`${args.configBaseDir}/`) || cleaned.startsWith('.github/');

      if (isRepoRelativeConfigPath) {
        // Already a repo-relative path -> use as-is only
        addCandidate(candidates, cleaned);
      } else {
        // Relative short path -> search through known schema locations
        addCandidate(candidates, `${args.configBaseDir}/${cleaned}`);
        for (const base of searchPaths) {
          addCandidate(candidates, `${base.replace(/^\.?\//, '')}/${cleaned}`);
        }
        addCandidate(candidates, cleaned);
      }
    }

    for (const p of candidates) {
      const cacheKey = cacheRepoKeyBase ? `${cacheRepoKeyBase}:${p}` : '';
      if (cacheKey && REPO_SCHEMA_CACHE.has(cacheKey)) return REPO_SCHEMA_CACHE.get(cacheKey);

      try {
        const res = await octokit.repos.getContent({ owner: args.owner, repo: args.repo, path: p });
        const data = res.data;

        if (!Array.isArray(data) && args.isRepoContentFile(data)) {
          const text = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
          const obj = JSON.parse(text);
          if (cacheKey) REPO_SCHEMA_CACHE.set(cacheKey, obj);
          return obj;
        }
      } catch (e: unknown) {
        if (args.getHttpStatus(e) === 404) continue;
        break;
      }
    }
  }

  return loadSchemaLocal({
    dirName: args.dirName,
    schemaPath: raw,
    toStringSafe: args.toStringSafe,
  });
}
