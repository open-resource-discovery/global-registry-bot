import path from 'node:path';
import os from 'node:os';
import { mkdtemp, mkdir, writeFile, rm, readFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import YAML from 'yaml';

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { main, TEST_UTILS } from '../src/ci/pr-reviewers.js';

function git(cwd: string, args: string[]): string {
  const env: NodeJS.ProcessEnv = { ...process.env };
  delete env.GIT_DIR;
  delete env.GIT_WORK_TREE;
  delete env.GIT_INDEX_FILE;
  delete env.GIT_OBJECT_DIRECTORY;
  delete env.GIT_ALTERNATE_OBJECT_DIRECTORIES;

  return execFileSync('git', args, { cwd, encoding: 'utf8', env }).trim();
}

async function mkdirp(p: string): Promise<void> {
  await mkdir(p, { recursive: true });
}

async function writeText(filePath: string, content: string): Promise<void> {
  await mkdirp(path.dirname(filePath));
  await writeFile(filePath, content, 'utf8');
}

async function writeYaml(filePath: string, obj: unknown): Promise<void> {
  await writeText(filePath, YAML.stringify(obj));
}

function initRepo(cwd: string): void {
  git(cwd, ['init', '-b', 'main']);
  git(cwd, ['config', 'user.email', 'ci@test.local']);
  git(cwd, ['config', 'user.name', 'CI']);
}

function commitAll(cwd: string, msg: string): string {
  git(cwd, ['add', '.']);
  git(cwd, ['commit', '-m', msg]);
  return git(cwd, ['rev-parse', 'HEAD']);
}

function setEnv(env: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(env)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

function restoreEnvSnapshot(snapshot: NodeJS.ProcessEnv): void {
  for (const k of Object.keys(process.env)) {
    if (!(k in snapshot)) delete process.env[k];
  }
  for (const [k, v] of Object.entries(snapshot)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

async function readOutputs(outPath: string): Promise<Record<string, string>> {
  const raw = await readFile(outPath, 'utf8');
  const out: Record<string, string> = {};

  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const i = line.indexOf('=');
    if (i < 0) continue;
    const k = line.slice(0, i).trim();
    const v = line.slice(i + 1);
    out[k] = v;
  }

  return out;
}

describe('pr-reviewers', () => {
  const originalCwd: string = process.cwd();
  const originalEnv: NodeJS.ProcessEnv = { ...process.env };

  let tmpDir = '';
  let logSpy: ReturnType<typeof jest.spyOn> | null = null;
  let errorSpy: ReturnType<typeof jest.spyOn> | null = null;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'registry-pr-reviewers-test-'));
    initRepo(tmpDir);
    process.chdir(tmpDir);

    logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(async () => {
    restoreEnvSnapshot(originalEnv);
    process.chdir(originalCwd);

    logSpy?.mockRestore();
    errorSpy?.mockRestore();
    logSpy = null;
    errorSpy = null;

    if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
  });

  it('detects bot senders (type + login patterns)', () => {
    expect(TEST_UTILS.isBotSender({ type: 'Bot', login: 'whatever' })).toBe(true);
    expect(TEST_UTILS.isBotSender({ type: 'User', login: 'my-registry-bot[bot]' })).toBe(true);
    expect(TEST_UTILS.isBotSender({ type: 'User', login: 'release-bot' })).toBe(true);
    expect(TEST_UTILS.isBotSender({ type: 'User', login: 'alice' })).toBe(false);
  });

  it('bot-created PR (no human touch) returns no reviewers and does not require config', async () => {
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'my-registry-bot[bot]',
      PR_SENDER_LOGIN: 'my-registry-bot[bot]',
      PR_SENDER_TYPE: 'Bot',
      PR_CREATOR_LOGIN: 'my-registry-bot[bot]',
      PR_CREATOR_TYPE: 'Bot',

      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
    expect(out.pr_creator_is_bot).toBe('true');
    expect(out.sender_is_bot).toBe('true');
    expect(out.bot_login).toBe('my-registry-bot[bot]');
    expect(out.pr_human_touched).toBe('false');
  });

  it('human-created PR requests config approvers and excludes PR author', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['bob'] },
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
            approvers: ['bob', 'carol'],
          },
        },
      })
    );

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'systemNamespace',
      name: 'sap.base',
    });
    const baseSha = commitAll(tmpDir, 'base');

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'systemNamespace',
      name: 'sap.base',
      updated: true,
    });
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'carol',
      PR_SENDER_LOGIN: 'carol',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'carol',
      PR_CREATOR_TYPE: 'User',

      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.pr_creator_is_bot).toBe('false');
    expect(JSON.parse(out.reviewers_json)).toEqual(['bob']);
  });

  it('bot-created PR with PR_HUMAN_TOUCHED=true uses config approvers', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['C5388932', 'D068547'] },
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'systemNamespace',
      name: 'sap.base',
    });
    const baseSha = commitAll(tmpDir, 'base');

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.new.yaml'), {
      type: 'systemNamespace',
      name: 'sap.new',
    });
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'synchronize',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'my-registry-bot[bot]',
      PR_SENDER_LOGIN: 'my-registry-bot[bot]',
      PR_SENDER_TYPE: 'Bot',
      PR_CREATOR_LOGIN: 'my-registry-bot[bot]',
      PR_CREATOR_TYPE: 'Bot',

      PR_HUMAN_TOUCHED: 'true',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.pr_creator_is_bot).toBe('true');
    expect(out.pr_human_touched).toBe('true');
    expect(JSON.parse(out.reviewers_json)).toEqual(['C5388932', 'D068547']);
  });

  it('bot-created PR fallback: human synchronize triggers reviewer computation even without PR_HUMAN_TOUCHED', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['C5388932', 'D068547'] },
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'systemNamespace',
      name: 'sap.base',
    });
    const baseSha = commitAll(tmpDir, 'base');

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.new.yaml'), {
      type: 'systemNamespace',
      name: 'sap.new',
    });
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'synchronize',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'my-registry-bot[bot]',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'my-registry-bot[bot]',
      PR_CREATOR_TYPE: 'Bot',

      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.pr_creator_is_bot).toBe('true');
    expect(out.sender_is_bot).toBe('false');
    expect(JSON.parse(out.reviewers_json)).toEqual(['C5388932', 'D068547']);
  });

  it('auto mode: uses event name when REGISTRY_VALIDATE_MODE is unset (non-PR event => early return)', async () => {
    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: undefined,
      GITHUB_EVENT_NAME: 'push',

      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
    expect(out.sender_is_bot).toBe('false');
    expect(out.pr_creator_is_bot).toBe('false');
    expect(out.bot_login).toBe('');
    expect(out.pr_human_touched).toBe('false');
  });

  it('PR mode: fork PR => early return with no reviewers', async () => {
    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: undefined,
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'true',

      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('PR mode: missing base/head SHA => early return with no reviewers', async () => {
    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',

      PR_BASE_SHA: '',
      PR_HEAD_SHA: 'deadbeef',

      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('throws if registry-bot config is missing', async () => {
    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: 'deadbeef',
      PR_HEAD_SHA: 'cafebabe',

      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).rejects.toThrow('Missing registry-bot config');
  });

  it('multi-candidate routing: selects requestType by doc.type (trim + lowercase) and uses override approvers', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['default-user'] },
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: './request-schemas/system.schema.json',
            approvers: ['sys-approver'],
          },
          subContextNamespace: {
            folderName: 'data/namespaces',
            schema: './request-schemas/sub.schema.json',
            approvers: ['sub-approver'],
          },
        },
      })
    );

    await writeText(path.join(tmpDir, 'data/namespaces/item.yaml'), 'type: systemNamespace\nname: base\n');
    const baseSha = commitAll(tmpDir, 'base');

    await writeText(path.join(tmpDir, 'data/namespaces/item.yaml'), 'type: " SubContextNamespace "\nname: head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'eve',
      PR_SENDER_LOGIN: 'eve',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'eve',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(JSON.parse(out.reviewers_json)).toEqual(['sub-approver']);
  });

  it('needsDefault: unresolved doc.type (non-string, non-object, parse error) triggers default approvers and filters bot + author', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['carol', 'release-bot', 'alice', 'carol'] },
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: './request-schemas/system.schema.json',
          },
          subContextNamespace: {
            folderName: 'data/namespaces',
            schema: './request-schemas/sub.schema.json',
          },
        },
      })
    );

    commitAll(tmpDir, 'base');
    const baseSha = git(tmpDir, ['rev-parse', 'HEAD']);

    await writeText(path.join(tmpDir, 'data/namespaces/bad-type.yaml'), 'type: 123\nname: x\n');
    await writeText(path.join(tmpDir, 'data/namespaces/scalar.yaml'), 'justastring\n');
    await writeText(path.join(tmpDir, 'data/namespaces/invalid.yaml'), 'type: "unterminated\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,

      PR_AUTHOR: 'carol',
      PR_SENDER_LOGIN: 'carol',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'carol',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(JSON.parse(out.reviewers_json)).toEqual(['alice']);
  });

  it('REGISTRY_VALIDATE_OK=false skips reviewer computation and outputs empty reviewers (L305-309, L58 arm=0)', async () => {
    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: 'deadbeef',
      PR_HEAD_SHA: 'cafebabe',

      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',

      // 'false' => readRegistryValidateOkEnv returns false => L305: registryValidateOk===false => early return
      REGISTRY_VALIDATE_OK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
    expect(JSON.parse(out.reviewers_json)).toEqual([]);
  });

  it('REGISTRY_VALIDATE_OK=true continues past the gate (L55 arm=0) and throws on missing config', async () => {
    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: 'deadbeef',
      PR_HEAD_SHA: 'cafebabe',

      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',

      // 'true' => readRegistryValidateOkEnv L55 arm=0: returns true => L305 not triggered => continues
      REGISTRY_VALIDATE_OK: 'true',
    });

    // No config in tmpDir => throws "Missing registry-bot config"
    await expect(main()).rejects.toThrow('Missing registry-bot config');
  });

  it('REGISTRY_VALIDATE_OK unknown value returns null and continues (L55 arm=1, L58 arm=1, L62)', async () => {
    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_EVENT_ACTION: 'opened',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: 'deadbeef',
      PR_HEAD_SHA: 'cafebabe',

      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',

      // 'unknown-value' => not truthy, not falsy => L55 arm=1, L58 arm=1, L62: return null
      // null => L305: registryValidateOk===false is false => continues => throws on missing config
      REGISTRY_VALIDATE_OK: 'unknown-value',
    });

    // No config in tmpDir => throws "Missing registry-bot config"
    await expect(main()).rejects.toThrow('Missing registry-bot config');
  });

  it('loadConfig throws when YAML root is not a plain object (L113)', async () => {
    await writeText(path.join(tmpDir, '.github/registry-bot/config.yaml'), '42\n');
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).rejects.toThrow('Invalid YAML in');
  });

  it('loadConfig skips workflow block when workflow is not a plain object (L118 false arm)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({ workflow: 'not-an-object', requests: {} })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('loadConfig skips defaultApprovers when workflow.approvers is null (L120 false arm)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({ workflow: { approvers: null }, requests: {} })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('loadConfig returns early when requests is not a plain object (L126 early return)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({ requests: 'not-an-object' })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('loadConfig skips non-plain request entries (L129 continue)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({ requests: { myType: 'not-an-object' } })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('loadConfig skips request entry with non-string folderName (L134 continue)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({ requests: { myType: { folderName: 123, schema: 'schema.json' } } })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('loadConfig sets request entry approvers to null when explicitly null in config (L146 arm)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        workflow: { approvers: ['default-approver'] },
        requests: { myType: { folderName: 'data/stuff', schema: 'stuff.json', approvers: null } },
      })
    );
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    const baseSha = commitAll(tmpDir, 'base');
    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    const headSha = commitAll(tmpDir, 'head');

    const outPath = path.join(tmpDir, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'false',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_AUTHOR: 'alice',
      PR_SENDER_LOGIN: 'alice',
      PR_SENDER_TYPE: 'User',
      PR_CREATOR_LOGIN: 'alice',
      PR_CREATOR_TYPE: 'User',
      PR_HUMAN_TOUCHED: 'false',
      REGISTRY_VALIDATE_OK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });
});

describe('TEST_UTILS pure functions', () => {
  it('isBotSender returns false for null and undefined (L26-27 optional chain null arm)', () => {
    expect(TEST_UTILS.isBotSender(null)).toBe(false);
    expect(TEST_UTILS.isBotSender(undefined)).toBe(false);
  });

  it('normalizeRepoPath coerces null/undefined via ?? to empty string (L66 nullish arm)', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(TEST_UTILS.normalizeRepoPath(null as any)).toBe('');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(TEST_UTILS.normalizeRepoPath(undefined as any)).toBe('');
  });

  it('matchRequestTypesForFile matches exact folder path fp===folder (L166 arm=0)', () => {
    const requests = { ns: { folderName: 'data/ns', schema: 's.json' } };
    const result = TEST_UTILS.matchRequestTypesForFile('data/ns', requests);
    expect(result).toHaveLength(1);
    expect(result[0].requestType).toBe('ns');
  });

  it('matchRequestTypesForFile matches sub-path via startsWith (L166 arm=1)', () => {
    const requests = { ns: { folderName: 'data/ns', schema: 's.json' } };
    const result = TEST_UTILS.matchRequestTypesForFile('data/ns/file.yaml', requests);
    expect(result).toHaveLength(1);
    expect(result[0].requestType).toBe('ns');
  });

  it('matchRequestTypesForFile returns empty when path does not match any folder', () => {
    const requests = { ns: { folderName: 'data/ns', schema: 's.json' } };
    const result = TEST_UTILS.matchRequestTypesForFile('other/path.yaml', requests);
    expect(result).toHaveLength(0);
  });

  it('matchRequestTypesForFile skips entry with empty folderName (L164 continue arm)', () => {
    const requests = {
      emptyFolder: { folderName: '', schema: 's.json' },
      realFolder: { folderName: 'data/real', schema: 's.json' },
    };
    const result = TEST_UTILS.matchRequestTypesForFile('data/real/file.yaml', requests);
    expect(result).toHaveLength(1);
    expect(result[0].requestType).toBe('realFolder');
  });
});

describe('main() env-var ?? arms + mode coverage', () => {
  const originalCwd2: string = process.cwd();
  const originalEnv2: NodeJS.ProcessEnv = { ...process.env };
  let tmpDir2 = '';

  beforeEach(async () => {
    tmpDir2 = await mkdtemp(path.join(os.tmpdir(), 'registry-pr-mode-test-'));
    initRepo(tmpDir2);
    process.chdir(tmpDir2);
  });

  afterEach(async () => {
    restoreEnvSnapshot(originalEnv2);
    process.chdir(originalCwd2);
    if (tmpDir2) await rm(tmpDir2, { recursive: true, force: true });
  });

  it('returns empty reviewers when mode is not pr (L88 main arm, L239-257 ?? arms)', async () => {
    const outPath = path.join(tmpDir2, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: undefined,
      GITHUB_EVENT_NAME: undefined,
      PR_SENDER_TYPE: undefined,
      PR_SENDER_LOGIN: undefined,
      PR_CREATOR_TYPE: undefined,
      PR_CREATOR_LOGIN: undefined,
      PR_HUMAN_TOUCHED: undefined,
      PR_IS_FORK: undefined,
      PR_BASE_SHA: undefined,
      PR_HEAD_SHA: undefined,
      PR_AUTHOR: undefined,
      PR_EVENT_ACTION: undefined,
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });

  it('returns empty reviewers when PR_IS_FORK/BASE_SHA/HEAD_SHA are unset (L274/L282/L283 ?? arms)', async () => {
    const outPath = path.join(tmpDir2, 'gh_out.txt');
    await rm(outPath, { force: true });

    setEnv({
      GITHUB_OUTPUT: outPath,
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_SENDER_TYPE: undefined,
      PR_SENDER_LOGIN: undefined,
      PR_CREATOR_TYPE: undefined,
      PR_CREATOR_LOGIN: undefined,
      PR_HUMAN_TOUCHED: undefined,
      PR_IS_FORK: undefined,
      PR_BASE_SHA: undefined,
      PR_HEAD_SHA: undefined,
      PR_AUTHOR: undefined,
      PR_EVENT_ACTION: undefined,
    });

    await expect(main()).resolves.toBeUndefined();
    const out = await readOutputs(outPath);
    expect(out.reviewers_count).toBe('0');
  });
});
