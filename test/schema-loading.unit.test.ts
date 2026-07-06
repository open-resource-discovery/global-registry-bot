/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect } from '@jest/globals';
import { loadSchemaLocal, loadSchemaFromRepoOrLocal } from '../src/handlers/request/validation/schema-loading.js';

const toStringSafe = (v: unknown): string => (v === null || v === undefined ? '' : String(v));

const getHttpStatus = (e: unknown): number | undefined => {
  if (typeof e === 'object' && e !== null && 'status' in e) {
    const s = (e as Record<string, unknown>)['status'];
    return typeof s === 'number' ? s : undefined;
  }
  return undefined;
};

const isRepoContentFile = (v: unknown): v is { content: string; encoding?: string } =>
  typeof v === 'object' && v !== null && 'content' in v && typeof (v as any).content === 'string';

describe('loadSchemaLocal', () => {
  it('throws when all candidate paths fail (L52 throw arm)', async () => {
    await expect(
      loadSchemaLocal({
        dirName: '/nonexistent-schema-dir-xyz',
        schemaPath: 'nonexistent-schema.json',
        toStringSafe,
      })
    ).rejects.toThrow('Failed to load schema');
  });
});

describe('loadSchemaFromRepoOrLocal', () => {
  const baseArgs = {
    owner: 'schema-unit-owner',
    repo: 'schema-unit-repo',
    dirName: '/nonexistent-schema-dir-xyz',
    configBaseDir: '.github/registry-bot',
    getHttpStatus,
    isRepoContentFile,
    toStringSafe,
  };

  it('returns null when schemaPath is empty', async () => {
    const result = await loadSchemaFromRepoOrLocal({
      ...baseArgs,
      context: {},
      schemaPath: '',
    });
    expect(result).toBeNull();
  });

  it('uses repo-relative path when path starts with configBaseDir (L92 isRepoRelativeConfigPath true arm)', async () => {
    const schemaObj = { type: 'object', properties: {} };
    const b64 = Buffer.from(JSON.stringify(schemaObj)).toString('base64');
    const getContent = (_args: unknown): Promise<{ data: { content: string; encoding: string } }> =>
      Promise.resolve({ data: { content: b64, encoding: 'base64' } });
    const result = await loadSchemaFromRepoOrLocal({
      ...baseArgs,
      context: { octokit: { repos: { getContent } } },
      schemaPath: '.github/registry-bot/my-schema.json',
    });
    expect(result).toEqual(schemaObj);
  });

  it('breaks loop on non-404 error then falls to loadSchemaLocal (L119 break arm)', async () => {
    const getContent = (_args: unknown): Promise<never> => Promise.reject({ status: 500 });
    await expect(
      loadSchemaFromRepoOrLocal({
        ...baseArgs,
        context: { octokit: { repos: { getContent } } },
        schemaPath: 'some-schema.json',
      })
    ).rejects.toThrow('Failed to load schema');
  });

  it('L22 arm1: loadSchemaLocal with toStringSafe returning "" → fallback to namespace.schema.json', async () => {
    await expect(
      loadSchemaLocal({
        dirName: '/nonexistent-schema-dir-xyz',
        schemaPath: null,
        toStringSafe: () => '',
      })
    ).rejects.toThrow('Failed to load schema');
  });

  it('L74 arm1: schemaPath "/" → addCandidate strips leading slash to "" → no candidates → throws', async () => {
    const getContent = (_args: unknown): Promise<never> => Promise.reject({ status: 404 });
    await expect(
      loadSchemaFromRepoOrLocal({
        ...baseArgs,
        context: { octokit: { repos: { getContent } } },
        schemaPath: '/',
      })
    ).rejects.toThrow('Failed to load schema');
  });

  it('L77+L79 arm1: no octokit in context → skips repo fetch → falls to loadSchemaLocal → throws', async () => {
    await expect(
      loadSchemaFromRepoOrLocal({ ...baseArgs, context: {}, schemaPath: 'some-schema.json' })
    ).rejects.toThrow('Failed to load schema');
  });

  it('L111 arm1: getContent returns array data → not a repo content file → continue → falls to local', async () => {
    const getContent = (_args: unknown): Promise<{ data: { type: string; name: string }[] }> =>
      Promise.resolve({ data: [{ type: 'dir', name: 'schemas' }] });
    await expect(
      loadSchemaFromRepoOrLocal({
        ...baseArgs,
        context: { octokit: { repos: { getContent } } },
        schemaPath: 'array-schema-xyz.json',
      })
    ).rejects.toThrow('Failed to load schema');
  });

  it('L112 arm1: getContent returns file without encoding field → encoding || "base64" fallback fires', async () => {
    const schemaObj = { type: 'object', title: 'no-encoding-test' };
    const b64 = Buffer.from(JSON.stringify(schemaObj)).toString('base64');
    const getContent = (_args: unknown): Promise<{ data: { content: string } }> =>
      Promise.resolve({ data: { content: b64 } });
    const result = await loadSchemaFromRepoOrLocal({
      ...baseArgs,
      owner: 'schema-unit-owner-b',
      repo: 'schema-unit-repo-b',
      context: { octokit: { repos: { getContent } } },
      schemaPath: 'no-encoding-test.json',
    });
    expect(result).toEqual(schemaObj);
  });
});
