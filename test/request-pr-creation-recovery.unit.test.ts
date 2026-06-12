/* eslint-disable @typescript-eslint/no-explicit-any */
import { jest, describe, test, expect } from '@jest/globals';
import { createRequestPrWithRecovery } from '../src/handlers/request/application/request-pr-creation-recovery.js';

const repoParams = { owner: 'org', repo: 'repo', issue_number: 1 };
const issue = { number: 1 };
const formData = {};

const templateWithRoot = { _meta: { root: 'data' } } as any;
const templateNoRoot = {} as any;

function make404Error(): Error & { status: number } {
  return Object.assign(new Error('Not Found'), { status: 404 });
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCtx(gitOverrides: Partial<{ deleteRef: any }> = {}) {
  return {
    octokit: {
      repos: {
        getContent: jest.fn().mockRejectedValue(make404Error()),
      },
      git: {
        deleteRef: jest.fn().mockResolvedValue({}),
        ...gitOverrides,
      },
    },
  } as any;
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, any> = {}) {
  return {
    createRequestPr: jest.fn().mockResolvedValue({ number: 123 }),
    getHttpStatus: jest.fn().mockImplementation((e: unknown) => (e as any)?.status),
    renderConfiguredRequestBranchName: jest.fn().mockReturnValue('feature/test-branch'),
    ...overrides,
  } as any;
}

// Success path
test('createRequestPr resolves immediately → returns PR number', async () => {
  const cbs = makeCallbacks({ createRequestPr: jest.fn().mockResolvedValue({ number: 42 }) });
  const result = await createRequestPrWithRecovery(makeCtx(), repoParams, issue, formData, templateWithRoot, 'ns', cbs);
  expect(result).toEqual({ number: 42 });
});

describe('L61 arm1: non-Error rejection', () => {
  test('createRequestPr rejects with string → String(error) taken', async () => {
    const cbs = makeCallbacks({
      createRequestPr: jest.fn().mockRejectedValue('plain-string-error'),
    });
    await expect(
      createRequestPrWithRecovery(makeCtx(), repoParams, issue, formData, templateWithRoot, 'ns', cbs)
    ).rejects.toThrow('Failed to create PR automatically: plain-string-error');
  });

  test('createRequestPr rejects with number → String(42)', async () => {
    const cbs = makeCallbacks({ createRequestPr: jest.fn().mockRejectedValue(42) });
    await expect(
      createRequestPrWithRecovery(makeCtx(), repoParams, issue, formData, templateWithRoot, 'ns', cbs)
    ).rejects.toThrow('Failed to create PR automatically: 42');
  });
});

test('L73 arm1 + L78 arm0: Validation Failed JSON without message → tail returned', async () => {
  const error = new Error('Validation Failed: {"code":"already_exists"}');
  const cbs = makeCallbacks({ createRequestPr: jest.fn().mockRejectedValue(error) });
  await expect(
    createRequestPrWithRecovery(makeCtx(), repoParams, issue, formData, templateWithRoot, 'ns', cbs)
  ).rejects.toThrow('Failed to create PR automatically: {"code":"already_exists"}');
});

test('L78 arm1: Validation Failed: empty tail → withoutUrl returned', async () => {
  const error = new Error('Validation Failed:');
  const cbs = makeCallbacks({ createRequestPr: jest.fn().mockRejectedValue(error) });
  await expect(
    createRequestPrWithRecovery(makeCtx(), repoParams, issue, formData, templateWithRoot, 'ns', cbs)
  ).rejects.toThrow('Failed to create PR automatically: Validation Failed:');
});

test('L110 arm0: resource already exists + template without root → getContent NOT called', async () => {
  const firstError = new Error("Resource 'ns' already exists at /data/ns.yaml");
  const retryError = new Error('retry-generic-fail');
  const cbs = makeCallbacks({
    createRequestPr: jest.fn().mockRejectedValueOnce(firstError).mockRejectedValueOnce(retryError),
  });
  const ctx = makeCtx();
  await expect(
    createRequestPrWithRecovery(ctx, repoParams, issue, formData, templateNoRoot, '', cbs)
  ).rejects.toThrow();
  expect(ctx.octokit.repos.getContent).not.toHaveBeenCalled();
});

test('L139 arm0 + L157 arm1: empty staleNoCommitsBranch → deleteRef skipped; suffix empty', async () => {
  const firstError = new Error('No commits between');
  const retryError = new Error('No commits between');
  const cbs = makeCallbacks({
    createRequestPr: jest.fn().mockRejectedValueOnce(firstError).mockRejectedValueOnce(retryError),
    renderConfiguredRequestBranchName: jest.fn().mockReturnValue(''),
  });
  const ctx = makeCtx();

  const err = await createRequestPrWithRecovery(ctx, repoParams, issue, formData, templateWithRoot, 'ns', cbs).catch(
    (e: unknown) => e
  );

  expect(ctx.octokit.git.deleteRef).not.toHaveBeenCalled();
  const msg = err instanceof Error ? err.message : String(err);
  expect(msg).toContain('stale request branch blocked PR creation');
  expect(msg).not.toContain("stale request branch '");
});

test('L148 arm0: deleteRef fails with 500 → error rethrown → outer catch formats message', async () => {
  const firstError = new Error('No commits between main and feature-branch');
  const deleteError = Object.assign(new Error('Internal Server Error'), { status: 500 });
  const cbs = makeCallbacks({
    createRequestPr: jest.fn().mockRejectedValueOnce(firstError),
    getHttpStatus: jest.fn().mockImplementation((e: unknown) => (e as any)?.status),
    renderConfiguredRequestBranchName: jest.fn().mockReturnValue('feature-branch'),
  });
  const ctx = makeCtx({ deleteRef: jest.fn().mockRejectedValue(deleteError) });

  await expect(
    createRequestPrWithRecovery(ctx, repoParams, issue, formData, templateWithRoot, 'ns', cbs)
  ).rejects.toThrow('Failed to create PR automatically');

  expect(ctx.octokit.git.deleteRef).toHaveBeenCalled();
});

test('L162 arm1: empty resourceName → suffix empty in stale-branch already-contains message', async () => {
  const firstError = new Error("Resource 'foo' already exists at /data/foo.yaml");
  const retryError = new Error("Resource 'x' already exists at /data/x.yaml");
  const cbs = makeCallbacks({
    createRequestPr: jest.fn().mockRejectedValueOnce(firstError).mockRejectedValueOnce(retryError),
  });
  const ctx = makeCtx();

  const err = await createRequestPrWithRecovery(ctx, repoParams, issue, formData, templateNoRoot, '', cbs).catch(
    (e: unknown) => e
  );

  const msg = err instanceof Error ? err.message : String(err);
  expect(msg).toContain('already contains.');
  expect(msg).not.toContain("already contains '");
});
