import { describe, expect, jest, test } from '@jest/globals';
import { createRequestPrWithRecovery } from '../src/handlers/request/application/request-pr-creation-recovery.js';

const params = { owner: 'org', repo: 'repo', issue_number: 1 };
const issue = { number: 1 };
const formData = {};
const template = { _meta: { root: 'data' } };

function makeFixture(error?: unknown): {
  context: { octokit: { rest: { git: { deleteRef: jest.Mock }; repos: { getContent: jest.Mock } } } };
  callbacks: {
    createRequestPr: jest.Mock<() => Promise<{ number: number }>>;
    getHttpStatus: () => undefined;
    renderConfiguredRequestBranchName: jest.Mock<() => string>;
  };
} {
  const createRequestPr = jest.fn<() => Promise<{ number: number }>>();
  if (error === undefined) createRequestPr.mockResolvedValue({ number: 42 });
  else createRequestPr.mockRejectedValue(error);
  return {
    context: { octokit: { rest: { git: { deleteRef: jest.fn() }, repos: { getContent: jest.fn() } } } },
    callbacks: {
      createRequestPr,
      getHttpStatus: () => undefined,
      renderConfiguredRequestBranchName: jest.fn<() => string>().mockReturnValue('feature/test-branch'),
    },
  };
}

describe('request PR creation error formatting', () => {
  test('returns the created PR', async () => {
    const { context, callbacks } = makeFixture();
    await expect(
      createRequestPrWithRecovery(context, params, issue, formData, template, 'ns', callbacks)
    ).resolves.toEqual({ number: 42 });
    expect(callbacks.createRequestPr).toHaveBeenCalledTimes(1);
  });

  test.each([
    ['plain string rejection', 'plain-string-error', 'plain-string-error'],
    ['numeric rejection', 42, '42'],
    [
      'validation JSON without a message',
      new Error('Validation Failed: {"code":"already_exists"}'),
      '{"code":"already_exists"}',
    ],
    ['empty validation detail', new Error('Validation Failed:'), 'Validation Failed:'],
    ['empty rejection', new Error(' '), 'PR creation failed.'],
    ['URL-only rejection', new Error('https://user:secret@example.com/path'), 'PR creation failed.'],
  ])('formats %s without leaking URLs', async (_name, error, detail) => {
    const { context, callbacks } = makeFixture(error);
    await expect(
      createRequestPrWithRecovery(context, params, issue, formData, template, 'ns', callbacks)
    ).rejects.toThrow(`Failed to create Pull Request: ${detail}`);
    expect(callbacks.createRequestPr).toHaveBeenCalledTimes(1);
  });

  test.each([
    ['existing resource', new Error("Resource 'ns' already exists at /data/ns.yaml"), 'feature/test-branch', 'ns'],
    ['no commits', new Error('No commits between main and feature-branch'), 'feature-branch', 'ns'],
    ['unresolved branch', new Error('No commits between'), '', 'ns'],
    ['empty resource name', new Error("Resource 'foo' already exists at /data/foo.yaml"), 'feature/test-branch', ''],
  ])('leaves %s reconciliation to the idempotent create operation', async (_name, error, branchName, resourceName) => {
    const { context, callbacks } = makeFixture(error);
    callbacks.renderConfiguredRequestBranchName.mockReturnValue(branchName);
    await expect(
      createRequestPrWithRecovery(context, params, issue, formData, template, resourceName, callbacks)
    ).rejects.toThrow(`Failed to create Pull Request: ${error.message}`);
    expect(callbacks.createRequestPr).toHaveBeenCalledTimes(1);
    expect(context.octokit.rest.git.deleteRef).not.toHaveBeenCalled();
    expect(context.octokit.rest.repos.getContent).not.toHaveBeenCalled();
  });
});
