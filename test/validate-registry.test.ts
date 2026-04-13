import path from 'node:path';
import os from 'node:os';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import type { ErrorObject } from 'ajv';
import YAML from 'yaml';

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { main, TEST_UTILS } from '../src/ci/validate-registry.js';

type ValidateOneFileFn = typeof TEST_UTILS.validateOneFile;
type BotValidationContext = Parameters<ValidateOneFileFn>[3];
type RepoInfo = Parameters<ValidateOneFileFn>[4];

function mkRepoInfo(): RepoInfo {
  return { owner: 'o', repo: 'r' };
}

function mkBotValidationContext(repoInfo: RepoInfo): BotValidationContext {
  return {
    // Octokit shape only needs to satisfy types; hooks are null so it's not used in the happy path.
    octokit: {
      repos: {
        getContent(): Promise<never> {
          const e = new Error('Not Found') as Error & { status: number };
          e.status = 404;
          return Promise.reject(e);
        },
      },
      issues: {
        get: () => Promise.resolve({ data: {} }),
        listForRepo: () => Promise.resolve({ data: [] }),
        update: () => Promise.resolve({}),
        create: () => Promise.resolve({}),
        createComment: () => Promise.resolve({}),
        addLabels: () => Promise.resolve({}),
        removeLabel: () => Promise.resolve({}),
      },
    },
    log: console,
    resourceBotConfig: { requests: {} } as unknown as BotValidationContext['resourceBotConfig'],
    resourceBotHooks: null,
    resourceBotHooksSource: null,
    repo: () => repoInfo,
    issue: () => ({ owner: repoInfo.owner, repo: repoInfo.repo, issue_number: 0 }),
  };
}

function git(cwd: string, args: string[]): string {
  return execFileSync('git', args, { cwd, encoding: 'utf8' }).trim();
}

async function mkdirp(p: string): Promise<void> {
  await mkdir(p, { recursive: true });
}

async function writeText(filePath: string, content: string): Promise<void> {
  await mkdirp(path.dirname(filePath));
  await writeFile(filePath, content, 'utf8');
}

async function writeJson(filePath: string, obj: unknown): Promise<void> {
  await writeText(filePath, JSON.stringify(obj, null, 2));
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
  // remove keys not in snapshot
  for (const k of Object.keys(process.env)) {
    if (!(k in snapshot)) delete process.env[k];
  }
  // restore snapshot keys
  for (const [k, v] of Object.entries(snapshot)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

describe('validate-registry', () => {
  const originalCwd: string = process.cwd();
  const originalEnv: NodeJS.ProcessEnv = { ...process.env };

  let tmpDir = '';
  let logSpy: ReturnType<typeof jest.spyOn> | null = null;
  let errorSpy: ReturnType<typeof jest.spyOn> | null = null;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'registry-validate-test-'));
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

  it('normalizes repo paths (windows + leading ./ + trailing /)', () => {
    expect(TEST_UTILS.normalizeRepoPath('./a\\b/')).toBe('a/b');
    expect(TEST_UTILS.normalizeRepoPath('/a/b/')).toBe('a/b');
    expect(TEST_UTILS.normalizeRepoPath('a//b///c')).toBe('a/b/c');
  });

  it('extracts schema type const/enum (normalized)', () => {
    expect(TEST_UTILS.extractSchemaTypeConst({ properties: { type: { const: 'System' } } })).toBe('system');

    expect(TEST_UTILS.extractSchemaTypeConst({ properties: { type: { enum: ['Authority'] } } })).toBe('authority');

    expect(TEST_UTILS.extractSchemaTypeConst({})).toBe('');
  });

  it('matchRequestTypesForFile returns null when file is outside configured folders', () => {
    const res = TEST_UTILS.matchRequestTypesForFile('data/other/x.yaml', {
      systemNamespace: {
        folderName: 'data/namespaces',
        schema: 'request-schemas/system.schema.json',
      },
    });

    expect(res).toBeNull();
  });

  it('main mode: validates all tracked YAML files and selects schema by doc.type', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          // Intentionally "wrong" order to prove doc.type prioritization works
          authorityNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/authority.schema.json',
          },
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
          subContextNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/subcontext.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/authority.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', enum: ['authority'] },
        name: { type: 'string' },
      },
    });

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/subcontext.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'subContext' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.yaml'), {
      type: 'system',
      name: 'sap',
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.ccm.yaml'), {
      type: 'subContext',
      name: 'sap.ccm',
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.abap.yaml'), {
      type: 'system',
      name: 'sap.abap',
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.odm.yaml'), {
      type: 'authority',
      name: 'sap.odm',
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.ccm.ctrl.yaml'), {
      type: 'subContext',
      name: 'sap.ccm.ctrl',
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      GITHUB_EVENT_NAME: 'push',
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('mode=main');
    expect(logs).toContain('failed=0');
  });

  it('main mode: loads registry-bot config from config.yml when config.yaml is missing', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.yaml'), {
      type: 'system',
      name: 'sap',
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      GITHUB_EVENT_NAME: 'push',
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('mode=main');
    expect(logs).toContain('failed=0');
  });

  it('PR mode: validates only changed YAML files (git diff base..head)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.yaml'), {
      type: 'system',
      name: 'sap',
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'system',
      name: 'sap.base',
    });
    const baseSha = commitAll(tmpDir, 'base');

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.new.yaml'), {
      type: 'system',
      name: 'sap.new',
    });
    const headSha = commitAll(tmpDir, 'head');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('mode=pr');
    expect(logs).toMatch(/Validated 1 file\(s\)/);
  });

  it('PR mode: falls back to HEAD when PR_HEAD_SHA cannot be diffed', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'system',
      name: 'sap.base',
    });
    const baseSha = commitAll(tmpDir, 'base');

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.head.yaml'), {
      type: 'system',
      name: 'sap.head',
    });
    commitAll(tmpDir, 'head');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: 'deadbeef',
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('mode=pr');
    expect(logs).toMatch(/Validated 1 file\(s\)/);
    expect(logs).toContain('failed=0');
  });

  it('fork PR mode requires PR_BASE_SHA', async () => {
    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_IS_FORK: 'true',
      PR_BASE_SHA: undefined,
    });

    await expect(main()).rejects.toThrow('Missing required env var PR_BASE_SHA');
  });

  it('main mode: de-dups same file returned by overlapping folders', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          rootNamespace: {
            folderName: 'data',
            schema: 'request-schemas/system.schema.json',
          },
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.yaml'), {
      type: 'system',
      name: 'sap',
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      GITHUB_EVENT_NAME: 'push',
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toMatch(/Validated 1 file\(s\)/);
  });

  it('allows fork PR validation using trusted base config and schema', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
        hooks: {
          allowedHosts: ['api.sap.com'],
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.base.yaml'), {
      type: 'system',
      name: 'sap.base',
    });

    const baseSha = commitAll(tmpDir, 'base');

    // Malicious / broken PR-side config and schema changes must be ignored for fork validation
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          wrongNamespace: {
            folderName: 'data/wrong',
            schema: 'request-schemas/broken.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      type: 'object',
      required: ['nonExisting'],
      properties: {
        nonExisting: { type: 'string' },
      },
    });

    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.js'),
      `
  export async function onValidate() {
    return [{ field: 'name', message: 'malicious hook should not run for fork PRs' }];
  }
  export default { onValidate };
  `
    );

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.new.yaml'), {
      type: 'system',
      name: 'sap.new',
    });

    commitAll(tmpDir, 'head');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_BASE_SHA: baseSha,
      PR_IS_FORK: 'true',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('Fork PR detected -> using trusted config/hooks/schemas from base');
    expect(logs).toContain('mode=pr');
    expect(logs).toMatch(/Validated 1 file\(s\)/);
    expect(logs).toContain('failed=0');
  });
  it('fails when registry-bot config has no "requests" mapping', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        notRequests: {},
      })
    );

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      PR_IS_FORK: 'false',
    });

    await expect(main()).rejects.toThrow('Invalid config: missing "requests" mapping');
  });

  it('fails when config has requests but no usable folderName/schema entries', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          brokenA: { folderName: 1, schema: true },
          brokenB: { folderName: 'data/namespaces' },
        },
      })
    );

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      PR_IS_FORK: 'false',
    });

    await expect(main()).rejects.toThrow('no usable requests.*.folderName + requests.*.schema');
  });

  it('fails with schema load error and emits GitHub annotation', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/does-not-exist.json',
          },
        },
      })
    );

    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.fail.yaml'), {
      type: 'system',
      name: 'sap.fail',
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      PR_IS_FORK: 'false',
    });

    await expect(main()).rejects.toThrow(/Registry validation failed/);

    const errs: string = (errorSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(errs).toContain('::error');
    expect(errs).toContain('schema load failed');
    expect(errs).toContain('sap.fail.yaml');
  });

  it('fails when flat parent chain is missing (prints GitHub annotation)', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    // Missing parent: sap.cds.yaml is NOT created
    await writeYaml(path.join(tmpDir, 'data/namespaces/sap.cds.foo.yaml'), {
      type: 'system',
      name: 'sap.cds.foo',
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      PR_IS_FORK: 'false',
    });

    await expect(main()).rejects.toThrow(/Registry validation failed/);

    const errs: string = (errorSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');

    expect(errs).toContain('::error');
    expect(errs).toContain("Parent resource 'sap.cds'");
    expect(errs).toContain('sap.cds.foo.yaml');
  });

  it('prints "No registry files" when folder has no tracked YAML files', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    commitAll(tmpDir, 'init');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'main',
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('No registry files to validate');
  });

  it('prints "No registry files" in PR mode when no matching registry YAML files changed', async () => {
    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          systemNamespace: {
            folderName: 'data/namespaces',
            schema: 'request-schemas/system.schema.json',
          },
        },
      })
    );

    await writeJson(path.join(tmpDir, '.github/registry-bot/request-schemas/system.schema.json'), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeText(path.join(tmpDir, 'README.md'), 'base');
    const baseSha = commitAll(tmpDir, 'base');

    await writeText(path.join(tmpDir, 'README.md'), 'changed');
    const headSha = commitAll(tmpDir, 'head');

    setEnv({
      REGISTRY_VALIDATE_MODE: 'pr',
      GITHUB_EVENT_NAME: 'pull_request',
      PR_BASE_SHA: baseSha,
      PR_HEAD_SHA: headSha,
      PR_IS_FORK: 'false',
    });

    await expect(main()).resolves.toBeUndefined();

    const logs: string = (logSpy?.mock.calls ?? []).map((c: unknown[]) => String(c[0])).join('\n');
    expect(logs).toContain('No registry files to validate in mode=pr');
  });

  it('validateOneFile: main mode relaxes required fields marked with x-sap-main-disable-validation', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/namespaces'));

    const schemaPath = '.github/registry-bot/request-schemas/system.schema.json';
    const filePath = 'data/namespaces/sap.yaml';

    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name', 'legacyField'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
        legacyField: { 'type': 'string', 'x-sap-main-disable-validation': true },
      },
    });

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'system',
      name: 'sap',
      // legacyField is intentionally missing
    });

    type ValidateOneFileFn2 = typeof TEST_UTILS.validateOneFile;
    type TargetArg2 = Parameters<ValidateOneFileFn2>[0];
    type AjvArg2 = Parameters<ValidateOneFileFn2>[1];
    type CacheArg2 = Parameters<ValidateOneFileFn2>[2];

    const target = {
      filePath,
      candidates: [{ requestType: 'systemNamespace', schemaPath }],
    } as unknown as TargetArg2;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg2;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg2;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);

    const resMain = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'main');
    expect(resMain.ok).toBe(true);

    const resPr = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'pr');
    expect(resPr.ok).toBe(false);
    expect(resPr.errors.join('\n')).toMatch(/required/i);
  });

  it('validateOneFile prefers schema matching doc.type even if candidates are ordered differently', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/namespaces'));

    const systemSchemaPath = '.github/registry-bot/request-schemas/system.schema.json';
    const authoritySchemaPath = '.github/registry-bot/request-schemas/authority.schema.json';

    await writeJson(path.join(tmpDir, systemSchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeJson(path.join(tmpDir, authoritySchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'authority' },
        name: { type: 'string' },
      },
    });

    const filePath = 'data/namespaces/sap.bad-system.yaml';

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'system',
      name: 'sap.bad-system',
      extra: 'nope',
    });

    type ValidateOneFileFn = typeof TEST_UTILS.validateOneFile;
    type TargetArg = Parameters<ValidateOneFileFn>[0];
    type AjvArg = Parameters<ValidateOneFileFn>[1];
    type CacheArg = Parameters<ValidateOneFileFn>[2];

    const target = {
      filePath,
      candidates: [
        { requestType: 'authorityNamespace', schemaPath: authoritySchemaPath },
        { requestType: 'systemNamespace', schemaPath: systemSchemaPath },
      ],
    } as unknown as TargetArg;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);

    const res = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo);

    expect(res.ok).toBe(false);
    expect(res.requestType).toBe('systemNamespace');
    expect(res.schemaPath).toBe(systemSchemaPath);
    expect(res.tries.some((t) => t.reason === 'type-match-failed')).toBe(true);
  });
  it('validateOneFile: PR mode returns custom hook validation errors', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/namespaces'));

    const schemaPath = '.github/registry-bot/request-schemas/system.schema.json';
    const filePath = 'data/namespaces/sap.hook.yaml';

    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'system',
      name: 'sap.hook',
    });

    type ValidateOneFileFn3 = typeof TEST_UTILS.validateOneFile;
    type TargetArg3 = Parameters<ValidateOneFileFn3>[0];
    type AjvArg3 = Parameters<ValidateOneFileFn3>[1];
    type CacheArg3 = Parameters<ValidateOneFileFn3>[2];

    const target = {
      filePath,
      candidates: [{ requestType: 'systemNamespace', schemaPath }],
    } as unknown as TargetArg3;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg3;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg3;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);
    botCtx.resourceBotConfig = {
      requests: {},
      hooks: { allowedHosts: ['api.sap.com'] },
    } as unknown as BotValidationContext['resourceBotConfig'];

    botCtx.resourceBotHooks = {
      onValidate: () => [{ field: 'identifier', message: 'hook rejected candidate' }],
    } as unknown as BotValidationContext['resourceBotHooks'];

    const res = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'pr');

    expect(res.ok).toBe(false);
    expect(res.errors).toContain('identifier: hook rejected candidate');
  });

  it('validateOneFile: fails when file name does not match resource identifier', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/namespaces'));

    const schemaPath = '.github/registry-bot/request-schemas/system.schema.json';
    const filePath = 'data/namespaces/wrong-file-name.yaml';

    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'name'],
      properties: {
        type: { type: 'string', const: 'system' },
        name: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'system',
      name: 'sap.correct.name',
    });

    type ValidateOneFileFn4 = typeof TEST_UTILS.validateOneFile;
    type TargetArg4 = Parameters<ValidateOneFileFn4>[0];
    type AjvArg4 = Parameters<ValidateOneFileFn4>[1];
    type CacheArg4 = Parameters<ValidateOneFileFn4>[2];

    const target = {
      filePath,
      candidates: [{ requestType: 'systemNamespace', schemaPath }],
    } as unknown as TargetArg4;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg4;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg4;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);

    const res = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'main');

    expect(res.ok).toBe(false);
    expect(res.errors.join('\n')).toContain('must match the resource identifier');
    expect(res.errors.join('\n')).toContain('sap.correct.name');
  });
  it('validateOneFile: filename validation prefers identifier over name for product resources', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/products'));

    const schemaPath = '.github/registry-bot/request-schemas/product.schema.json';
    const filePath = 'data/products/SAPAribaBuying.yaml';

    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'identifier', 'title'],
      properties: {
        type: { type: 'string', const: 'product' },
        identifier: { type: 'string' },
        name: { type: 'string' },
        title: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'product',
      identifier: 'SAPAribaBuying2',
      name: 'SAPAribaBuying',
      title: 'SAP Ariba Buying',
    });

    type ValidateOneFileFn5 = typeof TEST_UTILS.validateOneFile;
    type TargetArg5 = Parameters<ValidateOneFileFn5>[0];
    type AjvArg5 = Parameters<ValidateOneFileFn5>[1];
    type CacheArg5 = Parameters<ValidateOneFileFn5>[2];

    const target = {
      filePath,
      candidates: [{ requestType: 'product', schemaPath }],
    } as unknown as TargetArg5;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg5;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg5;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);

    const res = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'main');

    expect(res.ok).toBe(false);
    expect(res.errors.join('\n')).toContain(
      "File name 'SAPAribaBuying' must match the resource identifier 'SAPAribaBuying2'"
    );
  });

  it('validateOneFile: accepts matching identifier-based filename for product resources', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/products'));

    const schemaPath = '.github/registry-bot/request-schemas/product.schema.json';
    const filePath = 'data/products/SAPAribaBuying.yaml';

    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      additionalProperties: false,
      required: ['type', 'identifier', 'title'],
      properties: {
        type: { type: 'string', const: 'product' },
        identifier: { type: 'string' },
        title: { type: 'string' },
      },
    });

    await writeYaml(path.join(tmpDir, filePath), {
      type: 'product',
      identifier: 'SAPAribaBuying',
      title: 'SAP Ariba Buying',
    });

    type ValidateOneFileFn6 = typeof TEST_UTILS.validateOneFile;
    type TargetArg6 = Parameters<ValidateOneFileFn6>[0];
    type AjvArg6 = Parameters<ValidateOneFileFn6>[1];
    type CacheArg6 = Parameters<ValidateOneFileFn6>[2];

    const target = {
      filePath,
      candidates: [{ requestType: 'product', schemaPath }],
    } as unknown as TargetArg6;

    const ajv = TEST_UTILS.buildAjv() as unknown as AjvArg6;
    const schemaCache = new Map<string, unknown>() as unknown as CacheArg6;

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);

    const res = await TEST_UTILS.validateOneFile(target, ajv, schemaCache, botCtx, repoInfo, 'main');

    expect(res.ok).toBe(true);
    expect(res.errors).toEqual([]);
  });

  it('covers helper edge cases for mode, config parsing and repo metadata', async () => {
    expect(TEST_UTILS.normalizeRepoPath(undefined as unknown as string)).toBe('');

    setEnv({ REGISTRY_VALIDATE_MODE: '', GITHUB_EVENT_NAME: 'pull_request_target', GITHUB_REPOSITORY: 'acme/demo' });
    expect(TEST_UTILS.pickMode()).toBe('pr');
    expect(TEST_UTILS.readRepoInfoFromEnv()).toEqual({ owner: 'acme', repo: 'demo' });

    setEnv({ REGISTRY_VALIDATE_MODE: '', GITHUB_EVENT_NAME: 'workflow_dispatch', GITHUB_REPOSITORY: undefined });
    expect(TEST_UTILS.pickMode()).toBe('main');
    expect(TEST_UTILS.readRepoInfoFromEnv()).toEqual({ owner: 'local', repo: 'repo' });

    expect(TEST_UTILS.readDocType('not-an-object')).toBe('');
    expect(TEST_UTILS.extractSchemaTypeConst(null)).toBe('');
    expect(TEST_UTILS.extractSchemaTypeConst({ properties: { type: 'system' } })).toBe('');
    expect(TEST_UTILS.extractSchemaTypeConst({ properties: { type: { enum: ['a', 'b'] } } })).toBe('');

    await expect(TEST_UTILS.loadValidationConfig()).rejects.toThrow(
      'Missing registry-bot config: expected .github/registry-bot/config.yaml or .yml'
    );

    await writeText(path.join(tmpDir, '.github/registry-bot/config.yaml'), '- just\n- a\n- list\n');
    await expect(TEST_UTILS.loadValidationConfig()).rejects.toThrow('Invalid YAML in .github/registry-bot/config.yaml');

    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.yaml'),
      YAML.stringify({
        requests: {
          skipMe: 'not-an-object',
          alreadyRooted: {
            folderName: './data/namespaces/',
            schema: '.github/registry-bot/request-schemas/system.schema.json',
          },
          prefixedFromBaseDir: {
            folderName: '/data/products/',
            schema: 'request-schemas/product.schema.json',
          },
          blankFolder: {
            folderName: '',
            schema: 'request-schemas/blank.schema.json',
          },
        },
      }) + '\nhooks:\n  allowedHosts:\n    -\n    - " api.sap.com "\n'
    );

    const cfg = await TEST_UTILS.loadValidationConfig();
    expect(cfg).toEqual({
      requests: {
        alreadyRooted: {
          folderName: 'data/namespaces',
          schema: '.github/registry-bot/request-schemas/system.schema.json',
        },
        prefixedFromBaseDir: {
          folderName: 'data/products',
          schema: '.github/registry-bot/request-schemas/product.schema.json',
        },
        blankFolder: {
          folderName: '',
          schema: '.github/registry-bot/request-schemas/blank.schema.json',
        },
      },
      hooksAllowedHosts: ['api.sap.com'],
    });

    expect(TEST_UTILS.matchRequestTypesForFile('data/products/item.yaml', cfg.requests)).toEqual({
      filePath: 'data/products/item.yaml',
      candidates: [
        {
          requestType: 'prefixedFromBaseDir',
          schemaPath: '.github/registry-bot/request-schemas/product.schema.json',
        },
      ],
    });
  });

  it('covers git helper edge cases and filters deleted files from PR validation', async () => {
    await writeText(path.join(tmpDir, 'README.md'), 'base\n');
    await writeYaml(path.join(tmpDir, 'data/namespaces/keep.yaml'), { type: 'system', name: 'keep' });
    await writeYaml(path.join(tmpDir, 'data/namespaces/delete-me.yaml'), { type: 'system', name: 'delete-me' });
    const baseSha = commitAll(tmpDir, 'base');

    expect(await TEST_UTILS.repoPathExists('README.md')).toBe(true);
    expect(await TEST_UTILS.repoPathExists('missing.txt')).toBe(false);
    expect(await TEST_UTILS.repoPathExists('')).toBe(false);

    expect(await TEST_UTILS.readTextFromGitRevision('', 'README.md')).toBeNull();
    expect(await TEST_UTILS.readTextFromGitRevision('HEAD', '')).toBeNull();
    expect(await TEST_UTILS.readTextFromGitRevision(baseSha, 'README.md')).toBe('base\n');

    await expect(TEST_UTILS.readTrustedRepoFileText('', '')).rejects.toThrow("Invalid repository path ''");
    await expect(TEST_UTILS.readTrustedRepoFileText('missing.txt', baseSha)).rejects.toThrow(
      `Missing trusted file 'missing.txt' at revision '${baseSha}'`
    );

    await writeText(path.join(tmpDir, 'README.md'), 'head\n');
    await writeYaml(path.join(tmpDir, 'data/namespaces/new-file.yaml'), { type: 'system', name: 'new-file' });
    await rm(path.join(tmpDir, 'data/namespaces/delete-me.yaml'));
    commitAll(tmpDir, 'head');

    await expect(TEST_UTILS.resolveMergeBase('')).rejects.toThrow('Missing base ref for merge-base calculation');
    expect(await TEST_UTILS.resolveMergeBase(baseSha, undefined as unknown as string)).toBe(baseSha);

    const changedWithDefaultHead = await TEST_UTILS.getChangedFiles(baseSha, undefined as unknown as string);
    expect(changedWithDefaultHead).toContain('README.md');
    expect(changedWithDefaultHead).toContain('data/namespaces/new-file.yaml');
    expect(changedWithDefaultHead).not.toContain('data/namespaces/delete-me.yaml');

    expect(await TEST_UTILS.getAllTrackedFilesUnder('')).toEqual([]);
    expect(await TEST_UTILS.getAllTrackedFilesUnder('data/namespaces')).toEqual(
      expect.arrayContaining(['data/namespaces/keep.yaml', 'data/namespaces/new-file.yaml'])
    );
  });

  it('covers local hook loading, local octokit behavior and GitHub annotation escaping', async () => {
    expect(await TEST_UTILS.loadLocalHooksDescriptor()).toEqual({ hooks: null, hooksSource: null });

    await writeText(
      path.join(tmpDir, '.github/registry-bot/config.js'),
      'export async function onValidate() { return []; }\nexport default { onValidate };\n'
    );

    const hooksDesc = await TEST_UTILS.loadLocalHooksDescriptor();
    expect(hooksDesc.hooks).not.toBeNull();
    expect(hooksDesc.hooksSource).toMatch(/^repo:.github\/registry-bot\/config\.js#/);

    await writeText(path.join(tmpDir, 'docs/readme.txt'), 'hello');
    await mkdirp(path.join(tmpDir, 'docs/subdir'));

    const octokit = TEST_UTILS.createLocalOctokit();
    const fileData = await octokit.repos.getContent({ owner: 'o', repo: 'r', path: 'docs/readme.txt' });
    expect(Array.isArray(fileData.data)).toBe(false);
    if (!Array.isArray(fileData.data)) {
      expect(Buffer.from(fileData.data.content, 'base64').toString('utf8')).toBe('hello');
    }

    const dirData = await octokit.repos.getContent({ owner: 'o', repo: 'r', path: 'docs' });
    expect(Array.isArray(dirData.data)).toBe(true);
    if (Array.isArray(dirData.data)) {
      expect((dirData.data as unknown as { name: string }[]).map((entry) => entry.name)).toEqual(
        expect.arrayContaining(['readme.txt', 'subdir'])
      );
    }

    await expect(octokit.repos.getContent({ owner: 'o', repo: 'r', path: 'missing' })).rejects.toMatchObject({
      status: 404,
    });

    await expect(octokit.issues.get({ owner: 'o', repo: 'r', issue_number: 1 })).resolves.toEqual({ data: {} });
    await expect(octokit.issues.listForRepo({ owner: 'o', repo: 'r', state: 'open' })).resolves.toEqual({ data: [] });
    await expect(octokit.issues.update({ owner: 'o', repo: 'r', issue_number: 1 })).resolves.toEqual({});
    await expect(octokit.issues.create({ owner: 'o', repo: 'r', title: 't', body: 'b' })).resolves.toEqual({});
    await expect(
      octokit.issues.createComment({ owner: 'o', repo: 'r', issue_number: 1, body: 'note' })
    ).resolves.toEqual({});
    await expect(octokit.issues.addLabels({ owner: 'o', repo: 'r', issue_number: 1, labels: ['x'] })).resolves.toEqual(
      {}
    );
    await expect(octokit.issues.removeLabel({ owner: 'o', repo: 'r', issue_number: 1, name: 'x' })).resolves.toEqual(
      {}
    );

    TEST_UTILS.ghAnnotateError('a:b,c.yaml', 'line1\nline2');
    const errs: string = (errorSpy?.mock.calls ?? [])
      .map((c: unknown[]) => (typeof c[0] === 'string' ? c[0] : JSON.stringify(c[0])))
      .join('\n');
    expect(errs).toContain('::error file=a%3Ab%2Cc.yaml::line1 line2');
  });

  it('covers validateOneFile fallback selection, first-valid mode and hook exception handling', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));
    await mkdirp(path.join(tmpDir, 'data/misc'));

    const multiErrorSchemaPath = '.github/registry-bot/request-schemas/multi-error.schema.json';
    const singleErrorSchemaPath = '.github/registry-bot/request-schemas/single-error.schema.json';
    const disabledOnlySchemaPath = '.github/registry-bot/request-schemas/disabled-only.schema.json';
    const stringSchemaPath = '.github/registry-bot/request-schemas/string.schema.json';

    await writeJson(path.join(tmpDir, multiErrorSchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      required: ['a', 'b'],
      properties: {
        a: { type: 'string' },
        b: { type: 'string' },
      },
    });

    await writeJson(path.join(tmpDir, singleErrorSchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      required: ['a'],
      properties: {
        a: { type: 'string' },
      },
    });

    await writeJson(path.join(tmpDir, disabledOnlySchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      required: ['legacyField'],
      properties: {
        legacyField: { 'type': 'string', 'x-sap-main-disable-validation': true },
      },
    });

    await writeJson(path.join(tmpDir, stringSchemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'string',
    });

    const emptyDocPath = 'data/misc/empty.yaml';
    const scalarDocPath = 'data/misc/scalar.yaml';
    const noCandidatesPath = 'data/misc/no-candidates.yaml';

    await writeText(path.join(tmpDir, emptyDocPath), '');
    await writeText(path.join(tmpDir, scalarDocPath), 'plain scalar');
    await writeText(path.join(tmpDir, noCandidatesPath), '{}\n');

    const ajv = TEST_UTILS.buildAjv();

    const repoInfo = mkRepoInfo();
    const botCtx = mkBotValidationContext(repoInfo);
    botCtx.resourceBotHooks = {
      onValidate: () => {
        throw new Error('hook blew up');
      },
    } as unknown as BotValidationContext['resourceBotHooks'];

    const bestScoreRes = await TEST_UTILS.validateOneFile(
      {
        filePath: emptyDocPath,
        candidates: [
          { requestType: 'multi', schemaPath: multiErrorSchemaPath },
          { requestType: 'single', schemaPath: singleErrorSchemaPath },
        ],
      },
      ajv,
      new Map(),
      botCtx,
      repoInfo,
      'main'
    );

    expect(bestScoreRes.ok).toBe(false);
    expect(bestScoreRes.requestType).toBe('single');
    expect(bestScoreRes.errors).toHaveLength(1);

    const disabledOnlyRes = await TEST_UTILS.validateOneFile(
      {
        filePath: emptyDocPath,
        candidates: [{ requestType: 'disabled', schemaPath: disabledOnlySchemaPath }],
      },
      ajv,
      new Map(),
      botCtx,
      repoInfo,
      'main'
    );

    expect(disabledOnlyRes.ok).toBe(true);

    const stringDocRes = await TEST_UTILS.validateOneFile(
      {
        filePath: scalarDocPath,
        candidates: [{ requestType: 'freeform', schemaPath: stringSchemaPath }],
      },
      ajv,
      new Map(),
      botCtx,
      repoInfo,
      'pr'
    );

    expect(stringDocRes.ok).toBe(false);
    expect(stringDocRes.tries.at(-1)?.reason).toBe('first-valid');
    expect(stringDocRes.errors).toContain('Hook onValidate failed: hook blew up');

    const noCandidatesRes = await TEST_UTILS.validateOneFile(
      {
        filePath: noCandidatesPath,
        candidates: [],
      },
      ajv,
      new Map(),
      mkBotValidationContext(repoInfo),
      repoInfo,
      'main'
    );

    expect(noCandidatesRes.ok).toBe(false);
    expect(noCandidatesRes.requestType).toBe('unknown');
    expect(noCandidatesRes.schemaPath).toBe('unknown');
    expect(noCandidatesRes.errors).toEqual(['No candidate schema available for this file']);
  });

  it('covers schema cache defaults and AJV error formatting helpers', async () => {
    await mkdirp(path.join(tmpDir, '.github/registry-bot/request-schemas'));

    const schemaPath = '.github/registry-bot/request-schemas/system.schema.json';
    await writeJson(path.join(tmpDir, schemaPath), {
      $schema: 'https://json-schema.org/draft/2020-12/schema',
      type: 'object',
      required: ['type'],
      properties: {
        type: { type: 'string', const: 'system' },
      },
    });

    setEnv({ REGISTRY_VALIDATE_MODE: 'main' });

    const ajv = TEST_UTILS.buildAjv();
    const cache = new Map();
    const first = await TEST_UTILS.getSchemaEntry(schemaPath, ajv, cache);
    const second = await TEST_UTILS.getSchemaEntry(schemaPath, ajv, cache);

    expect(first).toBe(second);
    expect(first.typeConst).toBe('system');

    expect(TEST_UTILS.scoreErrors([])).toBe(9999);
    expect(
      TEST_UTILS.pickBestTry([
        { requestType: 'a', schemaPath: 'a', ok: false, errors: ['one', 'two'] },
        { requestType: 'b', schemaPath: 'b', ok: false, errors: ['one'] },
      ])
    ).toMatchObject({ requestType: 'b' });

    expect(TEST_UTILS.formatAjvErrors(undefined)).toEqual([]);
    const ajvErrors: ErrorObject[] = [
      {
        keyword: 'required',
        instancePath: '/name',
        schemaPath: '#/required',
        params: { missingProperty: 'type' },
        message: 'is required',
      },
      {
        keyword: 'errorMessage',
        instancePath: '/',
        schemaPath: '#',
        params: {},
        message: 'bad root',
      },
    ];
    expect(TEST_UTILS.formatAjvErrors(ajvErrors)).toEqual(['/name is required', 'bad root']);
  });
});
