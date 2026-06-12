/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
import { describe, test, expect, jest } from '@jest/globals';
import { closeOutdatedRequestPrs } from '../src/handlers/request/application/outdated-request-pr-cleanup.js';

const params = { owner: 'org', repo: 'repo', issue_number: 1 };
const template = {};

function makeOctokit() {
  return {
    issues: {
      get: jest.fn().mockResolvedValue({ data: { number: 1, body: 'body' } }),
      removeLabel: jest.fn().mockResolvedValue(undefined),
    },
    pulls: {
      update: jest.fn().mockResolvedValue(undefined),
    },
    git: {
      deleteRef: jest.fn().mockResolvedValue(undefined),
    },
  };
}

function makeCallbacks(overrides: Record<string, unknown> = {}) {
  return {
    parseForm: jest.fn().mockReturnValue({}),
    readIssueBodyForProcessing: jest.fn().mockReturnValue('body'),
    buildCompatibleRequestSnapshotHashes: jest.fn().mockReturnValue(['current-hash']),
    calcSnapshotHash: jest.fn().mockReturnValue('calc-hash'),
    extractHashFromPrBody: jest.fn().mockReturnValue(''),
    findOpenIssuePrs: jest.fn().mockResolvedValue([]),
    resolveEffectiveConstants: jest.fn().mockReturnValue({ labelOnApproved: null }),
    postOnce: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  } as any;
}

describe('closeOutdatedRequestPrs', () => {
  test('default-arg arm0: call without options uses the {} default', async () => {
    const cbs = makeCallbacks();
    const ctx = { octokit: makeOctokit() };
    await (closeOutdatedRequestPrs as any)(ctx, params, template, undefined, cbs);
    expect(ctx.octokit.issues.get).toHaveBeenCalled();
  });

  test('L148 binary-expr arm1: undefined givenAcceptedHashes falls back to [givenHash]', async () => {
    const cbs = makeCallbacks();
    const ctx = { octokit: makeOctokit() };
    await closeOutdatedRequestPrs(
      ctx as any,
      params,
      template,
      { parsedFormData: {} as any, currentHash: 'hash1', acceptedHashes: undefined },
      cbs
    );
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  test('L154 cond-expr arm1: empty acceptedHashes array falls back to [givenHash]', async () => {
    const cbs = makeCallbacks();
    const ctx = { octokit: makeOctokit() };
    await closeOutdatedRequestPrs(
      ctx as any,
      params,
      template,
      { parsedFormData: {} as any, currentHash: 'hash1', acceptedHashes: [] },
      cbs
    );
    expect(ctx.octokit.issues.get).not.toHaveBeenCalled();
  });

  test('L159 binary-expr arm1: issues.get returning null data falls back to {}', async () => {
    const octokit = makeOctokit();
    (octokit.issues.get as jest.Mock).mockResolvedValue({ data: null });
    const cbs = makeCallbacks();
    const ctx = { octokit };
    await closeOutdatedRequestPrs(ctx as any, params, template, undefined, cbs);
    expect(cbs.readIssueBodyForProcessing).toHaveBeenCalledWith(undefined);
  });

  test('L163 arm1 + L175 arm1: empty hashes list uses calcSnapshotHash; falsy currentHash skips set.add', async () => {
    const cbs = makeCallbacks({
      buildCompatibleRequestSnapshotHashes: jest.fn().mockReturnValue([]),
      calcSnapshotHash: jest.fn().mockReturnValue(''),
    });
    const ctx = { octokit: makeOctokit() };
    await closeOutdatedRequestPrs(ctx as any, params, template, undefined, cbs);
    expect(cbs.calcSnapshotHash).toHaveBeenCalled();
  });

  test('L187 arm0 + L204 arm0: PR with non-accepted hash gets closed; approved label removed', async () => {
    const octokit = makeOctokit();
    const cbs = makeCallbacks({
      extractHashFromPrBody: jest.fn().mockReturnValue('outdated-hash'),
      findOpenIssuePrs: jest
        .fn()
        .mockResolvedValue([{ number: 42, head: { ref: 'nsreq/issue-1' }, body: '<!-- snapshot:outdated-hash -->' }]),
      resolveEffectiveConstants: jest.fn().mockReturnValue({ labelOnApproved: 'Approved' }),
    });
    const ctx = { octokit };
    await closeOutdatedRequestPrs(
      ctx as any,
      params,
      template,
      { parsedFormData: {} as any, currentHash: 'current-hash' },
      cbs
    );
    expect(octokit.pulls.update).toHaveBeenCalledWith(expect.objectContaining({ pull_number: 42, state: 'closed' }));
    expect(cbs.postOnce).toHaveBeenCalled();
    expect(octokit.issues.removeLabel).toHaveBeenCalledWith(expect.objectContaining({ name: 'Approved' }));
  });
});
