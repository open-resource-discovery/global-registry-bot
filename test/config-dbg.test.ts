/**
 * Tests for config.ts branches that are only reachable when DBG=true
 * (const DBG = process.env.DEBUG_NS === '1', evaluated at module load time).
 *
 * This file sets DEBUG_NS before dynamically importing config.ts so that
 * Jest's per-file module registry creates a fresh config instance with DBG=true.
 */
import { jest } from '@jest/globals';

process.env['DEBUG_NS'] = '1';

const { loadStaticConfig } = await import('../src/config.js');

const CFG_YAML = '.github/registry-bot/config.yaml';
const CFG_YML = '.github/registry-bot/config.yml';

type FileEntry = { kind: 'file'; text: string } | { kind: 'dir' } | { kind: 'bad' } | { kind: 'err'; status: number };

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function httpErr(status: number): Error & { status: number } {
  const e = new Error(String(status)) as Error & { status: number };
  e.status = status;
  return e;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkCtx(args: {
  owner: string;
  repo: string;
  files: Record<string, FileEntry>;
  openIssues?: { number: number; title: string }[];
}) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const getContent = jest.fn(async ({ owner, repo, path }: any) => {
    const key = `${owner}/${repo}:${path}`;
    const entry = args.files[key];
    if (!entry) throw httpErr(404);
    if (entry.kind === 'dir') return { data: [] };
    if (entry.kind === 'bad') return { data: { foo: 'bar' } };
    if (entry.kind === 'err') throw httpErr(entry.status);
    return { data: { content: b64(entry.text), encoding: 'base64' } };
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const listForRepo = jest.fn(async (_p: any) => ({ data: args.openIssues ?? [] }));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const update = jest.fn(async (_p: any) => ({}));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const create = jest.fn(async (_p: any) => ({}));
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, require-await
  const createComment = jest.fn(async (_p: any) => ({}));

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const context: any = {
    octokit: {
      repos: { getContent },
      issues: { listForRepo, update, create, createComment },
    },
    log: { debug: jest.fn(), warn: jest.fn(), info: jest.fn() },
    repo: () => ({ owner: args.owner, repo: args.repo }),
  };

  return { context, getContent, listForRepo, update, create, createComment };
}

const VALID_YAML = `
requests:
  sample:
    folderName: resources
    schema: .github/registry-bot/schemas/sample.json
    issueTemplate: .github/ISSUE_TEMPLATE/sample.md
`;

// ─── DBG: repo config found (source = 'repo:...') ─────────────────────────────
// Covers: L670×2 (validateOrFallbackAndReport DBG), L820×2, L830×2, L833 arm 0

test('DBG=true: repo config triggers debug logging (source=repo)', async () => {
  const owner = 'dbg_repo_owner_x1';
  const repo = 'dbg_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: VALID_YAML } },
    openIssues: [],
  });
  const result = await loadStaticConfig(context, { validate: true, updateIssue: false, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: org config found (source = 'org:...') ───────────────────────────────
// Covers: L830 again, L832 arm 0 (source.startsWith('org:'))

test('DBG=true: org-level config triggers debug logging (source=org)', async () => {
  const owner = 'dbg_org_owner_x1';
  const repo = 'dbg_org_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: {
      [`${owner}/.github:${CFG_YAML}`]: { kind: 'file', text: VALID_YAML },
    },
    openIssues: [],
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toMatch(/^org:/);
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: default config (no user config found) ───────────────────────────────
// Covers: L830 default origin, L832 arm 1 (not org), L833 arm 1 (not repo)

test('DBG=true: no config found uses default, logs debug (source=default)', async () => {
  const owner = 'dbg_default_owner_x1';
  const repo = 'dbg_default_repo_x1';
  const { context } = mkCtx({ owner, repo, files: {}, openIssues: [] });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toBe('default');
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: validate=false with user config → L731 else-if arm ─────────────────
// Covers: L731 all binary-expr arms (hasUserConfig && !validate && DBG && log.debug)

test('DBG=true: validate=false with repo config triggers validation-skipped debug log', async () => {
  const owner = 'dbg_skip_val_owner_x1';
  const repo = 'dbg_skip_val_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: `requests: {}\n` } },
    openIssues: [],
  });
  const result = await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YML}`);
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: cache hit triggers debug log ────────────────────────────────────────
// Covers: L753 if + binary-expr (DBG inside getCachedResult cache-hit block)

test('DBG=true: cache hit triggers debug log inside getCachedResult', async () => {
  const owner = 'dbg_cache_hit_owner_x1';
  const repo = 'dbg_cache_hit_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YML}`]: { kind: 'file', text: `requests: {}\n` } },
    openIssues: [],
  });
  await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: true });
  context.log.debug.mockClear();
  await loadStaticConfig(context, { validate: false, updateIssue: false, forceReload: false });
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: missing config + validate=true + updateIssue=true → L712 ────────────
// Covers: L712 if arm 0 + binary-expr (DBG inside reportMissingIfNeeded when !hasUserConfig)

test('DBG=true: missing config with validate+updateIssue triggers missing-report debug log', async () => {
  const owner = 'dbg_missing_owner_x1';
  const repo = 'dbg_missing_repo_x1';
  const { context } = mkCtx({ owner, repo, files: {}, openIssues: [] });
  await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: closeStaticConfigIssueIfResolved – no existing issue → L528 ─────────
// Covers: L528 if arm 0 + binary-expr (DBG when no issue found to close)

test('DBG=true: validate+updateIssue with valid repo config and no existing issue triggers close-skip debug', async () => {
  const owner = 'dbg_close_skip_owner_x1';
  const repo = 'dbg_close_skip_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: VALID_YAML } },
    openIssues: [],
  });
  const result = await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: closeStaticConfigIssueIfResolved – existing issue found → L534 ──────
// Covers: L534 if arm 0 + binary-expr (DBG when existing issue IS found to close)

test('DBG=true: validate+updateIssue with valid repo config and existing error issue triggers close debug', async () => {
  const owner = 'dbg_close_existing_owner_x1';
  const repo = 'dbg_close_existing_repo_x1';
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: VALID_YAML } },
    openIssues: [{ number: 42, title: 'registry-bot: invalid static config.yaml' }],
  });
  const result = await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });
  expect(result.source).toBe(`repo:${CFG_YAML}`);
  expect(context.log.debug).toHaveBeenCalled();
});

// ─── DBG: createOrUpdateStaticConfigIssue – existing issue found → L477 ───────
// Covers: L477 if arm 0 + binary-expr (DBG when existing issue found during update path)

test('DBG=true: validation error with existing issue triggers update-existing debug log', async () => {
  const owner = 'dbg_update_issue_owner_x1';
  const repo = 'dbg_update_issue_repo_x1';
  const invalidYaml = `requests:\n  bad:\n    folderName: 42\n`;
  const { context } = mkCtx({
    owner,
    repo,
    files: { [`${owner}/${repo}:${CFG_YAML}`]: { kind: 'file', text: invalidYaml } },
    openIssues: [{ number: 99, title: 'registry-bot: invalid static config.yaml' }],
  });
  await loadStaticConfig(context, { validate: true, updateIssue: true, forceReload: true });
  expect(context.log.debug).toHaveBeenCalled();
});
