/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import {
  listChangedYamlFilesPage,
  readRecursiveGitTreeEntries,
  registryYamlTreeEntryPath,
  buildPullRequestHeadReadCandidates,
  readRepoFileTextAtRef,
  readPullRequestHeadTreeEntries,
  listChangedYamlFilesForPrAgainstCurrentBase,
} from '../src/handlers/request/application/pr-head-changed-file-discovery.js';

const repoInfo = { owner: 'org', repo: 'repo' };
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const normalizeRepoPath = (v: unknown) => String(v ?? '').trim();
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const isYamlPath = (p: string) => p.endsWith('.yaml') || p.endsWith('.yml');
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const isRegistryEntryPath = (_ctx: unknown, p: string) => p.startsWith('data/');

// ---------------------------------------------------------------------------
// listChangedYamlFilesPage — L227 false arm (non-array data)
// ---------------------------------------------------------------------------

describe('listChangedYamlFilesPage', () => {
  it('L227: returns [] when res.data is not an array', async () => {
    const ctx = { octokit: { pulls: { listFiles: jest.fn().mockResolvedValue({ data: null }) } } };
    const result = await listChangedYamlFilesPage(ctx as any, repoInfo, 1, 1);
    expect(result).toEqual([]);
  });

  it('returns the array when res.data is an array', async () => {
    const files = [{ filename: 'data/a.yaml', status: 'modified' }];
    const ctx = { octokit: { pulls: { listFiles: jest.fn().mockResolvedValue({ data: files }) } } };
    const result = await listChangedYamlFilesPage(ctx as any, repoInfo, 1, 1);
    expect(result).toEqual(files);
  });
});

// ---------------------------------------------------------------------------
// readRecursiveGitTreeEntries — L271 (empty ref) and L281 (non-array tree)
// ---------------------------------------------------------------------------

describe('readRecursiveGitTreeEntries', () => {
  const cbs = {
    log: jest.fn(),
    getErrorMessage: (e: unknown) => (e instanceof Error ? e.message : String(e)),
    getHttpStatus: jest.fn().mockReturnValue(undefined),
  } as any;

  it('L271: returns [] when ref is empty string', async () => {
    const ctx = { octokit: { git: { getTree: jest.fn() } } } as any;
    const result = await readRecursiveGitTreeEntries(ctx, repoInfo, '', cbs);
    expect(result).toEqual([]);
    expect(ctx.octokit.git.getTree).not.toHaveBeenCalled();
  });

  it('L281: returns [] when res.data.tree is not an array', async () => {
    const ctx = {
      octokit: { git: { getTree: jest.fn().mockResolvedValue({ data: { tree: null } }) } },
    } as any;
    const result = await readRecursiveGitTreeEntries(ctx, repoInfo, 'abc123', cbs);
    expect(result).toEqual([]);
  });

  it('returns entries when tree is an array', async () => {
    const entries = [{ path: 'data/a.yaml', type: 'blob', sha: 'sha1' }];
    const ctx = {
      octokit: { git: { getTree: jest.fn().mockResolvedValue({ data: { tree: entries } }) } },
    } as any;
    const result = await readRecursiveGitTreeEntries(ctx, repoInfo, 'abc123', cbs);
    expect(result).toEqual(entries);
  });

  it('returns [] and logs when getTree throws', async () => {
    const ctx = {
      octokit: { git: { getTree: jest.fn().mockRejectedValue(new Error('network error')) } },
    } as any;
    const result = await readRecursiveGitTreeEntries(ctx, repoInfo, 'abc123', cbs);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// registryYamlTreeEntryPath — L308 (not blob), L309 (path/yaml), L310 (not registry)
// ---------------------------------------------------------------------------

describe('registryYamlTreeEntryPath', () => {
  const baseCbs = { normalizeRepoPath, isYamlPath, isRegistryEntryPath };

  it('L308: returns "" when type is not blob', () => {
    const entry = { path: 'data/a.yaml', type: 'tree', sha: 'sha1' };
    expect(registryYamlTreeEntryPath({}, entry, baseCbs as any)).toBe('');
  });

  it('L309 left: returns "" when normalized path is empty', () => {
    const entry = { path: '', type: 'blob', sha: 'sha1' };
    expect(registryYamlTreeEntryPath({}, entry, baseCbs as any)).toBe('');
  });

  it('L309 right: returns "" when path is not yaml', () => {
    const entry = { path: 'data/a.json', type: 'blob', sha: 'sha1' };
    expect(registryYamlTreeEntryPath({}, entry, baseCbs as any)).toBe('');
  });

  it('L310: returns "" when not a registry entry path', () => {
    const entry = { path: 'src/a.yaml', type: 'blob', sha: 'sha1' };
    expect(registryYamlTreeEntryPath({}, entry, baseCbs as any)).toBe('');
  });

  it('returns path when all checks pass', () => {
    const entry = { path: 'data/ns/a.yaml', type: 'blob', sha: 'sha1' };
    expect(registryYamlTreeEntryPath({}, entry, baseCbs as any)).toBe('data/ns/a.yaml');
  });
});

// ---------------------------------------------------------------------------
// buildPullRequestHeadReadCandidates — L333 (empty ref skipped)
// ---------------------------------------------------------------------------

describe('buildPullRequestHeadReadCandidates', () => {
  const cbs = {
    resolvePullRequestHeadRepoInfo: (_pr: any, ri: any) => ri,
    sameRepoInfo: (a: any, b: any) => a.owner === b.owner && a.repo === b.repo,
  } as any;

  it('L333: skips add when headSha is empty', () => {
    const pr = { number: 1, head: { sha: '', ref: '' }, base: { ref: 'main' } };
    const candidates = buildPullRequestHeadReadCandidates(repoInfo, pr as any, cbs);
    const sources = candidates.map((c) => c.source);
    expect(sources).toContain('base-repo:pull-ref-full');
    expect(sources).not.toContain('base-repo:head-sha');
    expect(sources).not.toContain('base-repo:head-ref');
  });

  it('deduplicates same ref', () => {
    const pr = { number: 1, head: { sha: 'same-sha', ref: 'same-sha' }, base: { ref: 'main' } };
    const candidates = buildPullRequestHeadReadCandidates(repoInfo, pr as any, cbs);
    const bySha = candidates.filter((c) => c.ref === 'same-sha');
    expect(bySha.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// readRepoFileTextAtRef — L372 (empty path/ref), L386 (non-string encoding), L387 (empty content)
// ---------------------------------------------------------------------------

describe('readRepoFileTextAtRef', () => {
  const cbs = {
    normalizeRepoPath,
    isRepoContentFile: jest.fn().mockReturnValue(true),
  } as any;

  it('L372 left: returns null when path is empty', async () => {
    const ctx = { octokit: { repos: { getContent: jest.fn() } } } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, '', 'main', cbs);
    expect(result).toBeNull();
    expect(ctx.octokit.repos.getContent).not.toHaveBeenCalled();
  });

  it('L372 right: returns null when ref is empty', async () => {
    const ctx = { octokit: { repos: { getContent: jest.fn() } } } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, 'data/a.yaml', '', cbs);
    expect(result).toBeNull();
  });

  it('L386: uses "base64" fallback when encoding is not a string', async () => {
    const fileData = { content: 'aGVsbG8=', encoding: 42 };
    const ctx = {
      octokit: { repos: { getContent: jest.fn().mockResolvedValue({ data: fileData }) } },
    } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, 'data/a.yaml', 'main', cbs);
    expect(result).toBe('hello');
  });

  it('L387: uses empty string when file.content is undefined', async () => {
    const fileData = { content: undefined, encoding: 'base64' };
    const ctx = {
      octokit: { repos: { getContent: jest.fn().mockResolvedValue({ data: fileData }) } },
    } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, 'data/a.yaml', 'main', cbs);
    expect(result).toBe('');
  });

  it('returns null when data is an array', async () => {
    const ctx = {
      octokit: { repos: { getContent: jest.fn().mockResolvedValue({ data: [] }) } },
    } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, 'data/a.yaml', 'main', cbs);
    expect(result).toBeNull();
  });

  it('returns null when getContent throws', async () => {
    const ctx = {
      octokit: { repos: { getContent: jest.fn().mockRejectedValue(new Error('not found')) } },
    } as any;
    const result = await readRepoFileTextAtRef(ctx, repoInfo, 'data/a.yaml', 'main', cbs);
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// readPullRequestHeadTreeEntries — L469 (empty headSha)
// ---------------------------------------------------------------------------

describe('readPullRequestHeadTreeEntries', () => {
  it('L469: returns [] when headSha is empty', async () => {
    const pr = { number: 1, head: { sha: '' } };
    const cbs = {
      resolvePullRequestHeadRepoInfo: jest.fn(),
      sameRepoInfo: jest.fn(),
      readRecursiveGitTreeEntries: jest.fn(),
      isCrossRepositoryPullRequest: jest.fn(),
      log: jest.fn(),
    } as any;
    const result = await readPullRequestHeadTreeEntries({}, repoInfo, pr as any, cbs);
    expect(result).toEqual([]);
    expect(cbs.readRecursiveGitTreeEntries).not.toHaveBeenCalled();
  });

  it('returns entries from base repo when same repo', async () => {
    const pr = { number: 1, head: { sha: 'abc123', ref: 'feat' } };
    const entries = [{ path: 'data/a.yaml', type: 'blob', sha: 'sha1' }];
    const cbs = {
      resolvePullRequestHeadRepoInfo: jest.fn().mockReturnValue(repoInfo),
      sameRepoInfo: jest.fn().mockReturnValue(true),
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue(entries),
      isCrossRepositoryPullRequest: jest.fn().mockReturnValue(false),
      log: jest.fn(),
    } as any;
    const result = await readPullRequestHeadTreeEntries({}, repoInfo, pr as any, cbs);
    expect(result).toEqual(entries);
  });
});

// ---------------------------------------------------------------------------
// listChangedYamlFilesForPrAgainstCurrentBase — multiple branches
// ---------------------------------------------------------------------------

describe('listChangedYamlFilesForPrAgainstCurrentBase', () => {
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  function makeCbs(overrides: Record<string, any> = {}) {
    return {
      readBranchHeadSha: jest.fn().mockResolvedValue('base-sha'),
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([]),
      registryYamlTreeEntryPath: jest.fn().mockReturnValue(''),
      ...overrides,
    } as any;
  }

  it('L531: returns [] when baseBranch and pr.base.ref are both empty', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: '' } };
    const cbs = makeCbs();
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, '', cbs);
    expect(result).toEqual([]);
    expect(cbs.readBranchHeadSha).not.toHaveBeenCalled();
  });

  it('L530 arm 1: uses pr.base.ref when baseBranch is empty', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const cbs = makeCbs({ readBranchHeadSha: jest.fn().mockResolvedValue('base-sha') });
    await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, '', cbs);
    expect(cbs.readBranchHeadSha).toHaveBeenCalledWith({}, repoInfo, 'main');
  });

  it('returns [] when readBranchHeadSha returns empty string', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const cbs = makeCbs({ readBranchHeadSha: jest.fn().mockResolvedValue('') });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toEqual([]);
  });

  it('L545: skips base entry when registryYamlTreeEntryPath returns empty string', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const baseEntry = { path: 'src/notregistry.yaml', type: 'blob', sha: 'sha1' };
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([baseEntry]),
      registryYamlTreeEntryPath: jest.fn().mockReturnValue(''),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toEqual([]);
  });

  it('L548: skips baseByPath.set when entry sha is empty', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const baseEntry = { path: 'data/a.yaml', type: 'blob', sha: '' };
    let callIdx = 0;
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([baseEntry]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([]),
      registryYamlTreeEntryPath: jest.fn().mockImplementation(() => {
        callIdx++;
        return callIdx === 1 ? 'data/a.yaml' : '';
      }),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toEqual([]);
  });

  it('L559 arm 1: new head entry not in base map → included as changed', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const headEntry = { path: 'data/new.yaml', type: 'blob', sha: 'newsha' };
    let _callIdx = 0;
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([headEntry]),
      registryYamlTreeEntryPath: jest.fn().mockImplementation(() => {
        _callIdx++;
        return 'data/new.yaml';
      }),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toContain('data/new.yaml');
  });

  it('L561: skips head entry when headEntrySha is empty', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const headEntry = { path: 'data/a.yaml', type: 'blob', sha: '' };
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([headEntry]),
      registryYamlTreeEntryPath: jest.fn().mockReturnValue('data/a.yaml'),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toEqual([]);
  });

  it('L562: skips unchanged file when base sha matches head sha', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const sha = 'matching-sha';
    const baseEntry = { path: 'data/a.yaml', type: 'blob', sha };
    const headEntry = { path: 'data/a.yaml', type: 'blob', sha };
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([baseEntry]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([headEntry]),
      registryYamlTreeEntryPath: jest.fn().mockReturnValue('data/a.yaml'),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result).toEqual([]);
  });

  it('L556: skips duplicate path in head entries', async () => {
    const pr = { number: 1, head: { sha: 'sha' }, base: { ref: 'main' } };
    const headEntry1 = { path: 'data/a.yaml', type: 'blob', sha: 'sha-a' };
    const headEntry2 = { path: 'data/a.yaml', type: 'blob', sha: 'sha-b' };
    const cbs = makeCbs({
      readRecursiveGitTreeEntries: jest.fn().mockResolvedValue([]),
      readPullRequestHeadTreeEntries: jest.fn().mockResolvedValue([headEntry1, headEntry2]),
      registryYamlTreeEntryPath: jest.fn().mockReturnValue('data/a.yaml'),
    });
    const result = await listChangedYamlFilesForPrAgainstCurrentBase({}, repoInfo, pr as any, 'main', cbs);
    expect(result.filter((p) => p === 'data/a.yaml')).toHaveLength(1);
  });
});
