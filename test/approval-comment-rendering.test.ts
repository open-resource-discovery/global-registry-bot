import {
  buildApprovalDecisionJson,
  normalizeApprovalHookErrorsForComment,
  buildApprovalHookIssueList,
  buildAutoApprovalReviewBody,
  buildApprovalUnknownBody,
  buildApprovalRejectedBody,
} from '../src/handlers/request/domain/approval-comment-rendering.js';
import type { ApprovalDecision } from '../src/handlers/request/domain/approval-decision.js';

// ── buildApprovalDecisionJson ─────────────────────────────────────────────────

test('buildApprovalDecisionJson: empty decision produces empty object', () => {
  const result = JSON.parse(buildApprovalDecisionJson({}));
  expect(result).toEqual({});
});

test('buildApprovalDecisionJson: includes all truthy fields', () => {
  const decision: ApprovalDecision = {
    status: 'approved',
    path: 'some/path',
    reason: 'looks good',
    comment: 'LGTM',
    message: 'all clear',
    approvers: ['alice', 'bob'],
    errors: [{ field: 'name', message: 'bad' }],
  };
  const result = JSON.parse(buildApprovalDecisionJson(decision));
  expect(result.status).toBe('approved');
  expect(result.path).toBe('some/path');
  expect(result.reason).toBe('looks good');
  expect(result.comment).toBe('LGTM');
  expect(result.message).toBe('all clear');
  expect(result.approvers).toEqual(['alice', 'bob']);
  expect(result.errors).toEqual([{ field: 'name', message: 'bad' }]);
});

test('buildApprovalDecisionJson: omits empty approvers and errors arrays', () => {
  const decision: ApprovalDecision = { status: 'rejected', approvers: [], errors: [] };
  const result = JSON.parse(buildApprovalDecisionJson(decision));
  expect(result).not.toHaveProperty('approvers');
  expect(result).not.toHaveProperty('errors');
});

// ── normalizeApprovalHookErrorsForComment ─────────────────────────────────────

test('normalizeApprovalHookErrorsForComment: uses decision.errors when array', () => {
  const decision: ApprovalDecision = {
    errors: [{ field: 'name', message: 'value required' }],
  };
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result.length).toBeGreaterThan(0);
  expect(result[0].message).toBe('value required');
});

test('normalizeApprovalHookErrorsForComment: falls back to message when errors is not an array', () => {
  const decision = { errors: 'not-an-array', message: 'something failed' } as unknown as ApprovalDecision;
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result.length).toBe(1);
  expect(result[0].message).toBe('something failed');
});

test('normalizeApprovalHookErrorsForComment: uses reason when message is empty', () => {
  const decision: ApprovalDecision = { reason: 'bad reason' };
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result[0].message).toBe('bad reason');
});

test('normalizeApprovalHookErrorsForComment: uses comment when message and reason are empty', () => {
  const decision: ApprovalDecision = { comment: 'bad comment' };
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result[0].message).toBe('bad comment');
});

test('normalizeApprovalHookErrorsForComment: returns empty array when no fallback text', () => {
  expect(normalizeApprovalHookErrorsForComment({})).toEqual([]);
});

test('normalizeApprovalHookErrorsForComment: uses path as fallback field', () => {
  const decision: ApprovalDecision = { path: 'field/path', message: 'failed' };
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result[0].field).toBe('field/path');
});

test('normalizeApprovalHookErrorsForComment: uses "details" as field when path is empty', () => {
  const decision: ApprovalDecision = { message: 'failed' };
  const result = normalizeApprovalHookErrorsForComment(decision);
  expect(result[0].field).toBe('details');
});

// ── buildApprovalHookIssueList ────────────────────────────────────────────────

test('buildApprovalHookIssueList: returns empty string for empty issues array', () => {
  expect(buildApprovalHookIssueList([])).toBe('');
});

test('buildApprovalHookIssueList: returns empty string when all issues have no message', () => {
  expect(buildApprovalHookIssueList([{ field: 'name', message: '' }])).toBe('');
});

test('buildApprovalHookIssueList: groups issues by field and deduplicates messages', () => {
  const issues = [
    { field: 'name', message: 'required' },
    { field: 'name', message: 'required' },
    { field: 'version', message: 'invalid' },
  ];
  const result = buildApprovalHookIssueList(issues);
  expect(result).toContain('### Name');
  expect(result).toContain('- required');
  expect(result).toContain('### Version');
  expect((result.match(/- required/g) || []).length).toBe(1);
});

test('buildApprovalHookIssueList: "details" field sorts last', () => {
  const issues = [
    { field: 'details', message: 'general error' },
    { field: 'name', message: 'required' },
  ];
  const result = buildApprovalHookIssueList(issues);
  expect(result.indexOf('### Name')).toBeLessThan(result.indexOf('### Details'));
});

test('buildApprovalHookIssueList: contact field renders as "Contacts" section title', () => {
  expect(buildApprovalHookIssueList([{ field: 'contact', message: 'missing' }])).toContain('### Contacts');
});

test('buildApprovalHookIssueList: contacts field also renders as "Contacts"', () => {
  expect(buildApprovalHookIssueList([{ field: 'contacts', message: 'missing' }])).toContain('### Contacts');
});

test('buildApprovalHookIssueList: empty field name renders as "Details"', () => {
  expect(buildApprovalHookIssueList([{ field: '', message: 'an error' }])).toContain('### Details');
});

test('buildApprovalHookIssueList: camelCase field is space-separated in title', () => {
  expect(buildApprovalHookIssueList([{ field: 'requestType', message: 'invalid' }])).toContain('### Request Type');
});

// ── buildAutoApprovalReviewBody ───────────────────────────────────────────────

test('buildAutoApprovalReviewBody: returns only marker when no visible text', () => {
  const result = buildAutoApprovalReviewBody({ status: 'approved' }, 'abc123');
  expect(result).toContain('nsreq:auto-approval:abc123');
  expect(result).not.toContain('\n\n');
});

test('buildAutoApprovalReviewBody: includes visible text before marker when present', () => {
  const result = buildAutoApprovalReviewBody({ status: 'approved', comment: 'Looks great!' }, 'sha123');
  expect(result).toContain('Looks great!');
  expect(result).toContain('nsreq:auto-approval:sha123');
  expect(result.indexOf('Looks great!')).toBeLessThan(result.indexOf('nsreq:auto-approval'));
});

// ── buildApprovalUnknownBody ──────────────────────────────────────────────────

test('buildApprovalUnknownBody: includes message as lead', () => {
  const result = buildApprovalUnknownBody({ message: 'Needs manual review' });
  expect(result).toContain('Needs manual review');
  expect(result).toContain('Decision details');
});

test('buildApprovalUnknownBody: no lead when all text fields empty', () => {
  const result = buildApprovalUnknownBody({});
  expect(result).not.toContain('\n\n<details>');
  expect(result).toContain('Decision details');
  expect(result).toContain('Continuing with the standard review flow');
});

// ── buildApprovalRejectedBody ─────────────────────────────────────────────────

test('buildApprovalRejectedBody: includes structured issues block when errors present', () => {
  const result = buildApprovalRejectedBody({ errors: [{ field: 'name', message: 'required' }] });
  expect(result).toContain('## onApproval rejected this request');
  expect(result).toContain('Detected issues');
  expect(result).toContain('Closing this request automatically.');
});

test('buildApprovalRejectedBody: includes lead message when no structured issues', () => {
  const result = buildApprovalRejectedBody({ message: 'Custom rejection' });
  expect(result).toContain('Custom rejection');
  expect(result).toContain('## onApproval rejected this request');
});

test('buildApprovalRejectedBody: does not duplicate lead when detectedIssuesBlock is present', () => {
  const result = buildApprovalRejectedBody({
    message: 'Custom message',
    errors: [{ field: 'name', message: 'required' }],
  });
  // leadBlock = '' when detectedIssuesBlock is non-empty
  expect(result.indexOf('Custom message')).toBe(-1);
});

test('buildApprovalRejectedBody: falls back to comment for lead', () => {
  expect(buildApprovalRejectedBody({ comment: 'Rejected by policy' })).toContain('Rejected by policy');
});

test('buildApprovalRejectedBody: falls back to reason for lead', () => {
  expect(buildApprovalRejectedBody({ reason: 'Policy violation' })).toContain('Policy violation');
});

test('buildApprovalHookIssueList: L29 if-body — "---" field normalizes to "Details"', () => {
  // toSectionTitle('---'): '---'.replace(/[_-]+/g, ' ').trim() = '' → !spaced → return 'Details'
  const result = buildApprovalHookIssueList([{ field: '---', message: 'some error' }]);
  expect(result).toContain('### Details');
});

test('buildApprovalHookIssueList: L71 else — duplicate message across different filePaths not pushed twice', () => {
  // normalizeMachineReadableIssues keeps both (different filePath keys)
  // grouped loop: second issue has same message → arr.includes(msg) true → L71 else (no push)
  const result = buildApprovalHookIssueList([
    { field: 'name', message: 'required', filePath: 'a.yaml' },
    { field: 'name', message: 'required', filePath: 'b.yaml' },
  ]);
  expect((result.match(/- required/g) || []).length).toBe(1);
});

test('buildApprovalRejectedBody: L118+L122 false arms — no issues and no fallback text', () => {
  // issues = [] → groupedIssues = '' → L118 false → detectedIssuesBlock = ''
  // lead = '' → leadBlock = '' → L122 false → issuesBlock = ''
  const result = buildApprovalRejectedBody({});
  expect(result).toContain('## onApproval rejected this request');
  expect(result).not.toContain('Detected issues');
  expect(result).toContain('Closing this request automatically.');
});
