/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import { collectDuplicateRegistryErrors } from '../src/handlers/request/validation/duplicate-registry-validation.js';

const getHttpStatus = (error: unknown): number | undefined => {
  if (typeof error === 'object' && error !== null && 'status' in error) {
    const s = (error as Record<string, unknown>)['status'];
    return typeof s === 'number' ? s : undefined;
  }
  return undefined;
};

const resolveRegistryRoot = (): string => 'data/namespaces';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeOctokit(getContent: jest.MockedFunction<any>) {
  return { repos: { getContent } };
}

describe('collectDuplicateRegistryErrors', () => {
  it('returns [] immediately when namespace is absent (L54 || "" arm + L55)', async () => {
    const getContent = jest.fn();
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: {},
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
    expect(getContent).not.toHaveBeenCalled();
  });

  it('returns [] when namespace is whitespace-only (L54 trim, L55)', async () => {
    const getContent = jest.fn();
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: '   ' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
    expect(getContent).not.toHaveBeenCalled();
  });

  it('returns [] when resourceName trims to empty (L57-58) even when namespace is present', async () => {
    const getContent = jest.fn();
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base', identifier: '   ' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
    expect(getContent).not.toHaveBeenCalled();
  });

  it('uses namespace as resourceName when identifier is absent (L57 || namespace fallback arm)', async () => {
    const getContent = jest.fn().mockResolvedValue({});
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual(["Resource 'sap.base' already exists in registry"]);
    expect(getContent).toHaveBeenCalledWith(expect.objectContaining({ path: 'data/namespaces/sap.base.yaml' }));
  });

  it('returns error message when getContent resolves (resource already exists)', async () => {
    const getContent = jest.fn().mockResolvedValue({});
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base', identifier: 'sap.base.v1' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual(["Resource 'sap.base.v1' already exists in registry"]);
  });

  it('returns [] when getContent throws 404 (resource does not exist)', async () => {
    const getContent = jest.fn().mockRejectedValue({ status: 404 });
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base', identifier: 'sap.base.v1' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
  });

  it('calls log.warn and returns [] when getContent throws non-404 error (L70 warn arm)', async () => {
    const warnFn = jest.fn();
    const getContent = jest.fn().mockRejectedValue({ status: 500 });
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent), log: { warn: warnFn } } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base', identifier: 'sap.base.v1' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
    expect(warnFn).toHaveBeenCalledWith(
      expect.objectContaining({ err: expect.any(String) }),
      'registry existence check failed'
    );
  });

  it('does not crash when log is undefined and getContent throws non-404 (L70 optional chain null arm)', async () => {
    const getContent = jest.fn().mockRejectedValue({ status: 503 });
    const result = await collectDuplicateRegistryErrors({
      context: { octokit: makeOctokit(getContent) } as any,
      owner: 'org',
      repo: 'repo',
      template: {},
      requestCfg: {},
      normalizedFormData: { namespace: 'sap.base', identifier: 'sap.base.v1' },
      getHttpStatus,
      resolveRegistryRoot,
    });
    expect(result).toEqual([]);
  });
});
