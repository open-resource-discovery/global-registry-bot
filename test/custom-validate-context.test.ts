/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import { buildCustomValidateContextArgs } from '../src/handlers/request/validation/custom-validate-context.js';

// ── local callback implementations ────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function b64(s: string) {
  return Buffer.from(s, 'utf8').toString('base64');
}

function toStringSafe(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isRepoContentFile(value: unknown): value is { content: string; encoding?: string } {
  return isPlainObject(value) && typeof value.content === 'string';
}

function getHttpStatus(err: unknown): number | undefined {
  if (err && typeof err === 'object' && 'status' in (err as Record<string, unknown>)) {
    const s = (err as { status?: unknown }).status;
    return typeof s === 'number' ? s : undefined;
  }
  return undefined;
}

function mk404(): Error & { status: number } {
  const err = new Error('Not Found') as Error & { status: number };
  err.status = 404;
  return err;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkContext(files: Record<string, string> = {}) {
  // eslint-disable-next-line require-await
  const getContent = jest.fn(async ({ path }: { owner: string; repo: string; path: string }) => {
    if (path in files) {
      return { data: { content: b64(files[path]), encoding: 'base64' } };
    }
    throw mk404();
  });
  return {
    octokit: { repos: { getContent } },
    log: undefined as any,
    repo: (): { owner: string; repo: string } => ({ owner: 'org', repo: 'repo' }),
    issue: (): { owner: string; repo: string; issue_number: number } => ({
      owner: 'org',
      repo: 'repo',
      issue_number: 1,
    }),
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function mkArgs(overrides: Partial<Parameters<typeof buildCustomValidateContextArgs>[0]> = {}) {
  return {
    context: mkContext() as any,
    owner: 'org',
    repo: 'repo',
    issue: { number: 1, title: 'Test', body: 'body', state: 'open', labels: [], user: { login: 'user1' } },
    template: {},
    requestCfg: { folderName: 'data/ns' },
    resourceName: 'simple-resource',
    resolveRegistryRootForTemplate: () => 'data/ns',
    toStringSafe,
    isPlainObject,
    isRepoContentFile,
    getHttpStatus,
    ...overrides,
  } as any;
}

// ── basic result structure ─────────────────────────────────────────────────────

describe('buildCustomValidateContextArgs — basic paths', () => {
  test('short resource name (≤2 parts) returns no parent', async () => {
    const result = await buildCustomValidateContextArgs(mkArgs({ resourceName: 'myresource' }));
    expect(result.requestAuthor.id).toBe('user1');
    expect(result.issue.number).toBe(1);
    expect(result.issue.author).toBe('user1');
    expect(result.parentResourceName).toBe('');
    expect(result.parentCandidate).toBeNull();
    expect(result.parentOwners).toEqual([]);
  });

  test('two-part resource name returns no parent — covers resolveUpperNamespaceName ≤2 parts check', async () => {
    const result = await buildCustomValidateContextArgs(mkArgs({ resourceName: 'a.b' }));
    expect(result.parentResourceName).toBe('');
  });

  test('issue with string labels calls approvalIssueLabelName — covers line 134', async () => {
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        issue: {
          number: 2,
          title: 'T',
          body: '',
          state: 'open',
          labels: ['existing-label'],
          user: { login: 'user2' },
        },
      })
    );
    expect(result.issue.labels).toContain('existing-label');
  });

  test('issue with object labels hits isPlainObject branch — covers line 135', async () => {
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        issue: {
          number: 3,
          title: 'T',
          body: '',
          state: 'open',
          labels: [{ name: 'object-label' }, { name: 'second-label' }],
          user: null,
        },
      })
    );
    expect(result.issue.labels).toContain('object-label');
    expect(result.issue.labels).toContain('second-label');
    expect(result.issue.author).toBe(''); // user is null
  });

  test('issue with null label hits fallthrough return — covers line 136', async () => {
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        issue: {
          number: 4,
          title: 'T',
          body: '',
          state: 'open',
          labels: [null as any, 'real-label'],
          user: { login: 'u' },
        },
      })
    );
    // null label → approvalIssueLabelName returns '' → filtered out
    expect(result.issue.labels).toEqual(['real-label']);
  });
});

// ── parent resolution paths ───────────────────────────────────────────────────

describe('buildCustomValidateContextArgs — parent resolution', () => {
  test('three-part name triggers parent lookup — covers lines 259-273', async () => {
    // 'com.sap.product' → parent is 'com.sap'
    // Mock returns 404 for both yaml and yml → parentCandidate is null
    const result = await buildCustomValidateContextArgs(mkArgs({ resourceName: 'com.sap.product' }));
    expect(result.parentResourceName).toBe('com.sap');
    expect(result.parentCandidate).toBeNull();
    expect(result.parentOwners).toEqual([]);
  });

  test('parent YAML found (yaml ext) — covers parseYamlObject and readRepoYamlObject lines 182-234', async () => {
    const parentYaml = `
contacts: "owner1 owner2"
description: Parent resource
`;
    const ctx = mkContext({ 'data/ns/com.sap.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'com.sap.product',
        resolveRegistryRootForTemplate: () => 'data/ns',
      })
    );
    expect(result.parentResourceName).toBe('com.sap');
    expect(result.parentCandidate).not.toBeNull();
    // owners from contacts field
    expect(result.parentOwners).toContain('owner1');
    expect(result.parentOwners).toContain('owner2');
  });

  test('parent YAML found (yml ext after yaml 404) — covers 404 continue in readRepoYamlObject line 229', async () => {
    const parentYaml = `
owners: alice
`;
    const ctx = mkContext({ 'data/ns/com.sap.yml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'com.sap.product',
      })
    );
    expect(result.parentResourceName).toBe('com.sap');
    expect(result.parentOwners).toContain('alice');
  });

  test('GitHub URL in contacts normalizes to username — covers lines 288-289', async () => {
    const parentYaml = `
contacts: "https://github.com/my-username"
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
        resolveRegistryRootForTemplate: () => 'data/ns',
      })
    );
    expect(result.parentOwners).toContain('my-username');
  });

  test('array contacts recurses through items — covers lines 315-317', async () => {
    const parentYaml = `
contacts:
  - alice
  - bob
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    expect(result.parentOwners).toContain('alice');
    expect(result.parentOwners).toContain('bob');
  });

  test('nested object contacts recurses through values — covers lines 320-322', async () => {
    const parentYaml = `
contacts:
  lead: alice
  deputy: bob
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    expect(result.parentOwners).toContain('alice');
    expect(result.parentOwners).toContain('bob');
  });

  test('email in contacts is included — covers line 297', async () => {
    const parentYaml = `
contacts: "user@example.com alice"
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    expect(result.parentOwners).toContain('user@example.com');
    expect(result.parentOwners).toContain('alice');
  });

  test('@-prefixed handles are normalized — covers normalizeLoginValue stripping @', async () => {
    const parentYaml = `
contacts: "@alice @bob"
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    expect(result.parentOwners).toContain('alice');
    expect(result.parentOwners).toContain('bob');
  });

  test('non-404 error from getContent rethrows — covers line 230', async () => {
    const serverErr = new Error('Server Error') as Error & { status: number };
    serverErr.status = 500;
    const ctx = {
      ...mkContext(),
      octokit: {
        repos: {
          getContent: jest.fn((): Promise<never> => Promise.reject(serverErr)),
        },
      },
    };
    await expect(
      buildCustomValidateContextArgs(
        mkArgs({
          context: ctx as any,
          resourceName: 'a.b.c',
        })
      )
    ).rejects.toThrow('Server Error');
  });

  test('invalid YAML content returns null candidate — covers parseYamlObject catch line 200', async () => {
    // Buffer.from invalid base64 content still decodes, but could produce non-object YAML
    // Pass content that parses to non-object (e.g., a plain string)
    const ctx = mkContext({ 'data/ns/a.b.yaml': '"just a string"' });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    // "just a string" parses to string, not object → isPlainObject returns false → null candidate
    expect(result.parentCandidate).toBeNull();
  });

  test('values with invalid chars are rejected; valid handles are kept — covers regex branch in addNormalizedOwnerReference', async () => {
    const parentYaml = `
contacts: "valid-user trailing-dash-"
`;
    const ctx = mkContext({ 'data/ns/a.b.yaml': parentYaml });
    const result = await buildCustomValidateContextArgs(
      mkArgs({
        context: ctx as any,
        resourceName: 'a.b.c',
      })
    );
    // 'valid-user' passes regex → added
    expect(result.parentOwners).toContain('valid-user');
    // 'trailing-dash-' ends with '-' → fails regex → not added
    expect(result.parentOwners).not.toContain('trailing-dash-');
  });
});
