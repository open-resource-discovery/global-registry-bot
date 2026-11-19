import { describe, test, expect } from '@jest/globals';
import {
  normalizeApprovalDecision,
  promoteUnknownApprovalDecisionForDirectPrRequester,
} from '../src/handlers/request/domain/approval-decision.js';
import {
  getUnknownManualApprovers,
  getVisibleApprovalText,
  isApprovalDecisionAuthorizedByHookApprovers,
} from '../src/handlers/request/domain/approval-policy.js';

describe('normalizeApprovalDecision', () => {
  test('true returns approved status', () => {
    expect(normalizeApprovalDecision(true)).toEqual({ status: 'approved' });
  });

  test('false returns empty object', () => {
    expect(normalizeApprovalDecision(false)).toEqual({});
  });

  test('normalizes approvers — number in list covers toStringTrim non-string path', () => {
    // toStringTrim(42) → L16 arm1 (not string), L17 arm1 (number), L17 binary-expr arm0
    const result = normalizeApprovalDecision({ approvers: [42 as unknown as string] });
    expect(result.approvers).toEqual(['42']);
  });

  test('normalizes approvers — boolean in list covers toStringTrim boolean path', () => {
    // toStringTrim(true) → L16 arm1, L17 binary-expr arm1 (not number, is boolean)
    const result = normalizeApprovalDecision({ approvers: [true as unknown as string] });
    expect(result.approvers).toEqual(['true']);
  });

  test('normalizes approvers — object in list covers toStringTrim fallback', () => {
    // toStringTrim({}) → L16 arm1, L17 if arm0 (not number/boolean), returns ''
    const result = normalizeApprovalDecision({ approvers: [{} as unknown as string, 'valid'] });
    expect(result.approvers).toEqual(['valid']);
  });

  test('omits approvers key when approvers list is empty after filtering', () => {
    const result = normalizeApprovalDecision({ approvers: [] });
    expect(result).not.toHaveProperty('approvers');
  });

  test('preserves non-approvers fields', () => {
    const result = normalizeApprovalDecision({ status: 'approved', reason: 'all good' });
    expect(result.status).toBe('approved');
    expect(result.reason).toBe('all good');
  });
});

describe('promoteUnknownApprovalDecisionForDirectPrRequester', () => {
  test('returns unchanged when status is not unknown', () => {
    const decision = { status: 'approved' as const };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'user1');
    expect(result.status).toBe('approved');
  });

  test('returns unchanged when requester is not in approvers', () => {
    const decision = { status: 'unknown' as const, approvers: ['other'] };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'user1');
    expect(result.status).toBe('unknown');
  });

  test('promotes to approved when requester is in approvers and gets visible text', () => {
    const decision = {
      status: 'unknown' as const,
      approvers: ['requester'],
      comment: 'LGTM',
    };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'requester');
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('LGTM');
  });

  test('L52 binary-expr arm1: skips reason containing "manual approval required"', () => {
    // comment='', message='', reason='manual approval required'
    // reason is truthy but isManualApprovalRequiredText(reason) is true → fallback to ''
    const decision = {
      status: 'unknown' as const,
      approvers: ['user1'],
      comment: '',
      message: '',
      reason: 'manual approval required',
    };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'user1');
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('');
  });
});

describe('approval-policy — branch coverage', () => {
  test('getUnknownManualApprovers: returns [] when status is not unknown', () => {
    expect(getUnknownManualApprovers({ status: 'approved' })).toEqual([]);
  });

  test('getUnknownManualApprovers: returns approvers when status is unknown', () => {
    expect(getUnknownManualApprovers({ status: 'unknown', approvers: ['alice'] })).toEqual(['alice']);
  });

  test('getUnknownManualApprovers: L21 arm0 — duplicate login is skipped (continue path)', () => {
    // Duplicate login → !login || seen.has(key) is true → continue
    const result = getUnknownManualApprovers({ status: 'unknown', approvers: ['alice', 'alice'] });
    expect(result).toEqual(['alice']);
  });

  test('getVisibleApprovalText: L5 arm1 + L6 arm0 — non-string comment (number) triggers fallback path', () => {
    // toStringTrim(42) → L5 arm1 (not string) → L6 arm0 (number)
    const result = getVisibleApprovalText({ comment: 42 as unknown as string });
    expect(result).toBe('42');
  });

  test('getVisibleApprovalText: L6 arm1 — boolean value triggers boolean path', () => {
    // toStringTrim(true) → L5 arm1 (not string) → L6 arm1 (boolean, not number)
    const result = getVisibleApprovalText({ comment: true as unknown as string });
    expect(result).toBe('true');
  });

  test('isApprovalDecisionAuthorizedByHookApprovers: L61 arm1 — null configuredApprovers falls back to []', () => {
    const result = isApprovalDecisionAuthorizedByHookApprovers(
      { status: 'approved', approvers: ['alice'] },
      null as unknown as string[],
      ['alice']
    );
    expect(result).toBe(true);
  });

  test('isApprovalDecisionAuthorizedByHookApprovers: L68 arm1 — null reviewerLogins falls back to []', () => {
    const result = isApprovalDecisionAuthorizedByHookApprovers(
      { status: 'approved', approvers: ['alice'] },
      ['alice'],
      null as unknown as string[]
    );
    expect(result).toBe(false);
  });

  test('isApprovalDecisionAuthorizedByHookApprovers: returns false when no allowed approvers', () => {
    const result = isApprovalDecisionAuthorizedByHookApprovers({ status: 'approved' }, [], []);
    expect(result).toBe(false);
  });

  test('getVisibleApprovalText: L6 false arm — object value → toStringTrim returns ""', () => {
    // toStringTrim({}) → not null/undefined/string/number/boolean → L6 false → return ''
    const result = getVisibleApprovalText({ comment: {} as unknown as string });
    expect(result).toBe('');
  });
});

describe('promoteUnknownApprovalDecisionForDirectPrRequester — L52 if-body', () => {
  test('L52 if-body: reason (non-MAR) used as visible text when comment and message absent', () => {
    // comment absent → L46 skipped; message absent → L49 skipped
    // reason='Looks good' truthy and not MAR → L52 if-body fires → returns reason
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(
      { status: 'unknown', approvers: ['user1'], reason: 'Looks good' },
      'user1'
    );
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('Looks good');
  });
});
