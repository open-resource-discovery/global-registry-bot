import { describe, test, expect, jest } from '@jest/globals';
import { handleApprovalComment } from '../src/handlers/request/application/approval-comment-handling.js';

const ctx = {};
const params = { owner: 'org', repo: 'repo', issue_number: 1 };
const issue = { number: 1, user: { login: 'author' } };
const template = {};
const formData: Record<string, string> = {};

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, unknown> = {}) {
  return {
    resolveEffectiveConstants: jest.fn().mockReturnValue({
      approverUsernames: ['approver'],
      approverPoolUsernames: [],
    }),
    resolveEffectiveRequestType: jest.fn().mockReturnValue('namespace-type'),
    resolveApproversForRequestType: jest.fn().mockReturnValue(['approver']),
    ensureReviewLabelsPresentOnIssue: jest.fn().mockResolvedValue(true),
    postOnce: jest.fn().mockResolvedValue(undefined),
    uniqLogins: jest.fn((v: string[]) => [...new Set(v)]),
    isAuthorizedApprover: jest.fn().mockReturnValue(true),
    resolveAdditionalIssueApproversFromApprovalHook: jest.fn().mockResolvedValue([]),
    validateRequestIssue: jest.fn().mockResolvedValue({ namespace: 'test' }),
    setStateLabel: jest.fn().mockResolvedValue(undefined),
    checkParentChainExistsInFlatStructure: jest.fn().mockResolvedValue(null),
    log: jest.fn(),
    finalizeApprovedRequest: jest.fn().mockResolvedValue(undefined),
    ...overrides,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}

describe('handleApprovalComment — branch coverage', () => {
  test('L169: configuredApprovers null uses empty-array fallback', async () => {
    const cbs = makeCallbacks({
      resolveApproversForRequestType: jest.fn().mockReturnValue(null),
      isAuthorizedApprover: jest.fn().mockReturnValue(true),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.finalizeApprovedRequest).toHaveBeenCalled();
  });

  test('L182: hookApprovers null uses empty-array fallback', async () => {
    const cbs = makeCallbacks({
      resolveApproversForRequestType: jest.fn().mockReturnValue(['approver1']),
      isAuthorizedApprover: jest.fn().mockReturnValueOnce(false).mockReturnValueOnce(true),
      resolveAdditionalIssueApproversFromApprovalHook: jest.fn().mockResolvedValue(null),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.finalizeApprovedRequest).toHaveBeenCalled();
  });

  test('L203 arm1: errorsFormatted used when errorsFormattedSingle is empty', async () => {
    const cbs = makeCallbacks({
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['error one'],
        errorsFormattedSingle: '',
        errorsFormatted: 'Full formatted error body',
        validationIssues: [],
        namespace: 'test',
      }),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.postOnce).toHaveBeenCalled();
    expect(cbs.setStateLabel).toHaveBeenCalledWith(ctx, params, issue, 'author');
  });

  test('L203 arm2: listFallback used when both formatted strings are empty', async () => {
    const cbs = makeCallbacks({
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['error one'],
        errorsFormattedSingle: '',
        errorsFormatted: '',
        validationIssues: [],
        namespace: 'test',
      }),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.postOnce).toHaveBeenCalled();
    expect(cbs.setStateLabel).toHaveBeenCalledWith(ctx, params, issue, 'author');
  });

  test('L209 arm0: validationIssue.path non-empty passes through toStringTrim', async () => {
    const cbs = makeCallbacks({
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['error'],
        errorsFormattedSingle: 'Error summary',
        validationIssues: [{ path: 'namespace.field', message: 'field is invalid' }],
        namespace: 'test',
      }),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('L209 arm1: validationIssue.path null falls back to "details"', async () => {
    const cbs = makeCallbacks({
      validateRequestIssue: jest.fn().mockResolvedValue({
        errors: ['error'],
        errorsFormattedSingle: 'Error summary',
        validationIssues: [{ path: null, message: 'generic error' }],
        namespace: 'test',
      }),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.postOnce).toHaveBeenCalled();
  });

  test('L250 arm0: catch block logs Error.message when checkParentChain throws Error', async () => {
    const cbs = makeCallbacks({
      checkParentChainExistsInFlatStructure: jest.fn().mockRejectedValue(new Error('parent chain failed')),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.log).toHaveBeenCalledWith(ctx, 'warn', { err: 'parent chain failed' }, expect.any(String));
    expect(cbs.finalizeApprovedRequest).toHaveBeenCalled();
  });

  test('L250 arm1: catch block uses String(e) when non-Error is thrown', async () => {
    const cbs = makeCallbacks({
      checkParentChainExistsInFlatStructure: jest.fn().mockRejectedValue('string error value'),
    });
    await handleApprovalComment(ctx, params, issue, template, formData, 'approver', cbs);
    expect(cbs.log).toHaveBeenCalledWith(ctx, 'warn', { err: 'string error value' }, expect.any(String));
    expect(cbs.finalizeApprovedRequest).toHaveBeenCalled();
  });
});
