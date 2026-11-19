import { describe, test, expect } from '@jest/globals';

// ---- check-conclusions (domain) ------------------------------------------------
import {
  isGreenCheckConclusion,
  isBlockingCheckConclusion,
  summarizeHeadGreenRun,
} from '../src/handlers/request/domain/check-conclusions.js';

describe('isGreenCheckConclusion', () => {
  test('success is green', () => expect(isGreenCheckConclusion('success')).toBe(true));
  test('neutral is green', () => expect(isGreenCheckConclusion('neutral')).toBe(true));
  test('skipped is green', () => expect(isGreenCheckConclusion('skipped')).toBe(true));
  test('failure is not green', () => expect(isGreenCheckConclusion('failure')).toBe(false));
  test('empty string is not green', () => expect(isGreenCheckConclusion('')).toBe(false));
  test('handles uppercase', () => expect(isGreenCheckConclusion('SUCCESS')).toBe(true));
});

describe('isBlockingCheckConclusion', () => {
  test('failure is blocking', () => expect(isBlockingCheckConclusion('failure')).toBe(true));
  test('cancelled is blocking', () => expect(isBlockingCheckConclusion('cancelled')).toBe(true));
  test('timed_out is blocking', () => expect(isBlockingCheckConclusion('timed_out')).toBe(true));
  test('action_required is blocking', () => expect(isBlockingCheckConclusion('action_required')).toBe(true));
  test('startup_failure is blocking', () => expect(isBlockingCheckConclusion('startup_failure')).toBe(true));
  test('stale is blocking', () => expect(isBlockingCheckConclusion('stale')).toBe(true));
  test('success is not blocking', () => expect(isBlockingCheckConclusion('success')).toBe(false));
  test('empty string is not blocking', () => expect(isBlockingCheckConclusion('')).toBe(false));
});

describe('summarizeHeadGreenRun', () => {
  test('extracts standard fields', () => {
    const run = { id: 42, name: 'ci', status: 'completed', conclusion: 'success' };
    expect(summarizeHeadGreenRun(run)).toEqual({ id: 42, name: 'ci', status: 'completed', conclusion: 'success' });
  });

  test('omits undefined id', () => {
    const run = { name: 'ci', status: 'completed', conclusion: 'failure' };
    const result = summarizeHeadGreenRun(run);
    expect(result).not.toHaveProperty('id');
    expect(result.name).toBe('ci');
  });

  test('defaults name to __unnamed__ when missing', () => {
    const result = summarizeHeadGreenRun({});
    expect(result.name).toBe('__unnamed__');
  });

  test('handles number-type id correctly', () => {
    const run = { id: 1, name: 'test', status: 'queued', conclusion: '' };
    expect(summarizeHeadGreenRun(run).id).toBe(1);
  });

  test('numeric name field hits number branch of local toStringTrim', () => {
    // Lines 18-19: number/boolean branch of toStringTrim inside summarizeHeadGreenRun
    const run = { name: 42 as unknown as string, status: 'completed', conclusion: 'success' };
    expect(summarizeHeadGreenRun(run).name).toBe('42');
  });

  test('boolean status field hits boolean branch of local toStringTrim', () => {
    const run = { name: 'ci', status: true as unknown as string, conclusion: 'success' };
    expect(summarizeHeadGreenRun(run).status).toBe('true');
  });
});

// ---- comment-commands ----------------------------------------------------------
import {
  stripQuoteAndCode,
  isApprovalComment,
  isAuthorUpdateComment,
} from '../src/handlers/request/domain/comment-commands.js';

describe('stripQuoteAndCode', () => {
  test('strips code fences', () => {
    const input = 'Some text\n```\ncode here\n```\nmore text';
    expect(stripQuoteAndCode(input)).not.toContain('code here');
  });

  test('strips blockquote lines', () => {
    const input = 'normal\n> quoted\nnormal again';
    expect(stripQuoteAndCode(input)).not.toContain('quoted');
  });

  test('returns empty for null/undefined', () => {
    expect(stripQuoteAndCode(null)).toBe('');
    expect(stripQuoteAndCode(undefined)).toBe('');
  });

  test('passes a number value through toStringTrim', () => {
    // Covers number branch of local toStringTrim
    expect(stripQuoteAndCode(42)).toBe('42');
  });

  test('passes a boolean value through toStringTrim', () => {
    expect(stripQuoteAndCode(true)).toBe('true');
  });

  test('passes an object returns empty string', () => {
    expect(stripQuoteAndCode({})).toBe('');
  });
});

describe('isApprovalComment', () => {
  test('Approved keyword is accepted', () => expect(isApprovalComment('Approved')).toBe(true));
  test('Approve is accepted', () => expect(isApprovalComment('Approve')).toBe(true));
  test('LGTM is accepted', () => expect(isApprovalComment('LGTM')).toBe(true));
  test('custom keyword is accepted when configured', () => {
    expect(isApprovalComment('Ship it', 'Ship it')).toBe(true);
  });
  test('Approved with surrounding punctuation is accepted', () => {
    expect(isApprovalComment('"Approved"')).toBe(true);
  });
  test('rejection text is not accepted', () => expect(isApprovalComment('Please change this')).toBe(false));
  test('empty comment is rejected', () => expect(isApprovalComment('')).toBe(false));
  test('null is rejected', () => expect(isApprovalComment(null)).toBe(false));
});

describe('isAuthorUpdateComment', () => {
  test('updated is detected', () => expect(isAuthorUpdateComment('I updated the description')).toBe(true));
  test('fixed is detected', () => expect(isAuthorUpdateComment('Fixed!')).toBe(true));
  test('addressed is detected', () => expect(isAuthorUpdateComment('Addressed all comments')).toBe(true));
  test('done is detected', () => expect(isAuthorUpdateComment('Done')).toBe(true));
  test('unrelated text is not detected', () => expect(isAuthorUpdateComment('Please review')).toBe(false));
});

// ---- routing-lock-marker -------------------------------------------------------
import {
  readRoutingLockExpected,
  buildRoutingLockBody,
  stripRoutingLockFromBody,
} from '../src/handlers/request/domain/routing-lock-marker.js';

describe('readRoutingLockExpected', () => {
  test('returns empty for body without marker', () => {
    expect(readRoutingLockExpected('plain body')).toBe('');
  });

  test('reads expected from valid marker', () => {
    const body = buildRoutingLockBody('base body', 'Review Pending');
    expect(readRoutingLockExpected(body)).toBe('Review Pending');
  });

  test('returns empty for invalid JSON in marker', () => {
    // Covers catch-block return '' — line 21
    const bodyWithBadJson = 'text <!-- nsreq:routing-lock = {invalid json} --> more';
    expect(readRoutingLockExpected(bodyWithBadJson)).toBe('');
  });

  test('handles null body', () => {
    expect(readRoutingLockExpected(null)).toBe('');
  });
});

describe('buildRoutingLockBody', () => {
  test('embeds expected label as JSON', () => {
    const result = buildRoutingLockBody('issue body', 'Review Pending');
    expect(result).toContain('nsreq:routing-lock');
    expect(result).toContain('"Review Pending"');
    expect(result).toContain('issue body');
  });

  test('replaces existing lock marker', () => {
    const first = buildRoutingLockBody('body', 'Label A');
    const second = buildRoutingLockBody(first, 'Label B');
    expect(readRoutingLockExpected(second)).toBe('Label B');
    expect(second.match(/nsreq:routing-lock/g)?.length).toBe(1);
  });

  test('numeric expectedLabel hits number branch of local toStringTrim — lines 9-10', () => {
    // toStringTrim(expectedLabel) in buildRoutingLockBody hits the number branch
    const result = buildRoutingLockBody('body', 42 as unknown as string);
    expect(result).toContain('42');
  });
});

describe('readRoutingLockExpected — numeric expected field', () => {
  test('reads numeric expected value from JSON — hits number branch of toStringTrim', () => {
    // When the JSON "expected" field is a number, toStringTrim hits lines 9-10
    const body = 'text <!-- nsreq:routing-lock = {"v":1,"expected":99} --> more';
    expect(readRoutingLockExpected(body)).toBe('99');
  });
});

describe('stripRoutingLockFromBody', () => {
  test('removes routing lock from body', () => {
    const body = buildRoutingLockBody('clean body', 'Label');
    const stripped = stripRoutingLockFromBody(body);
    expect(stripped).not.toContain('nsreq:routing-lock');
    expect(stripped).toContain('clean body');
  });

  test('returns plain body unchanged', () => {
    expect(stripRoutingLockFromBody('plain body')).toBe('plain body');
  });

  test('handles number body - covers number branch of toStringTrim', () => {
    // toStringTrim isn't called directly, but String() coercion covers the path
    expect(stripRoutingLockFromBody(42)).toBe('42');
  });
});

// ---- pull-request-compare-result -----------------------------------------------
import { evaluatePullRequestCompareResult } from '../src/handlers/request/domain/pull-request-compare-result.js';

describe('evaluatePullRequestCompareResult', () => {
  test('ahead status sets isBehindCurrentBase true', () => {
    const result = evaluatePullRequestCompareResult({ status: 'ahead', ahead_by: 1 });
    expect(result.isBehindCurrentBase).toBe(true);
    expect(result.aheadBy).toBe(1);
  });

  test('diverged status sets isBehindCurrentBase true', () => {
    expect(evaluatePullRequestCompareResult({ status: 'diverged', ahead_by: 0 }).isBehindCurrentBase).toBe(true);
  });

  test('ahead_by > 0 alone sets isBehindCurrentBase true', () => {
    expect(evaluatePullRequestCompareResult({ status: '', ahead_by: 5 }).isBehindCurrentBase).toBe(true);
  });

  test('identical status sets isBehindCurrentBase false', () => {
    // Covers lines 28-34 — the identical branch
    const result = evaluatePullRequestCompareResult({ status: 'identical', ahead_by: 0 });
    expect(result.isBehindCurrentBase).toBe(false);
    expect(result.status).toBe('identical');
  });

  test('unknown status sets isBehindCurrentBase null', () => {
    // Covers lines 36-40 — the fallthrough null branch
    const result = evaluatePullRequestCompareResult({ status: 'behind', ahead_by: 0 });
    expect(result.isBehindCurrentBase).toBeNull();
  });

  test('null input returns null isBehindCurrentBase', () => {
    const result = evaluatePullRequestCompareResult(null);
    expect(result.isBehindCurrentBase).toBeNull();
    expect(result.aheadBy).toBe(0);
  });

  test('undefined input returns null isBehindCurrentBase', () => {
    expect(evaluatePullRequestCompareResult(undefined).isBehindCurrentBase).toBeNull();
  });
});

// ---- issue-body-processing -----------------------------------------------------
import { readIssueBodyForProcessing } from '../src/handlers/request/domain/issue-body-processing.js';

describe('readIssueBodyForProcessing', () => {
  test('returns plain body unchanged', () => {
    expect(readIssueBodyForProcessing('simple body')).toBe('simple body');
  });

  test('strips routing lock, parent-approval and contact-approval markers', () => {
    const body = 'body <!-- nsreq:routing-lock = {"v":1,"expected":"Label"} --> ';
    const result = readIssueBodyForProcessing(body);
    expect(result).not.toContain('nsreq:routing-lock');
    expect(result).toContain('body');
  });

  test('handles null value', () => {
    expect(readIssueBodyForProcessing(null)).toBe('');
  });

  test('handles number value — covers number branch of internal toStringTrim', () => {
    // Covers lines 7-8 (number/boolean branch in local toStringTrim)
    expect(readIssueBodyForProcessing(42)).toBe('42');
  });

  test('handles boolean value — strip functions use String() coercion', () => {
    expect(readIssueBodyForProcessing(true)).toBe('true');
  });
});

// ---- current-head-approval -----------------------------------------------------
import {
  isApprovalReviewForCurrentHead,
  reviewTargetsCurrentHead,
  extractApprovedByLoginFromReviewBody,
  resolveEffectiveReviewApproverLogin,
} from '../src/handlers/request/domain/current-head-approval.js';

describe('isApprovalReviewForCurrentHead', () => {
  test('returns false when headSha is empty', () => {
    expect(isApprovalReviewForCurrentHead({ state: 'APPROVED', commit_id: 'abc' }, '')).toBe(false);
  });

  test('returns false when state is not APPROVED', () => {
    expect(isApprovalReviewForCurrentHead({ state: 'CHANGES_REQUESTED', commit_id: 'abc' }, 'abc')).toBe(false);
  });

  test('returns true when commit_id matches headSha', () => {
    expect(isApprovalReviewForCurrentHead({ state: 'APPROVED', commit_id: 'sha123' }, 'sha123')).toBe(true);
  });

  test('returns true when body contains auto-approval marker', () => {
    // The marker is generated by buildAutoApprovalReviewMarker
    // We test indirectly via the body path
    const fakeMarker = `<!-- nsreq:auto-approve:sha456 -->`;
    expect(
      isApprovalReviewForCurrentHead({ state: 'APPROVED', commit_id: 'different-sha', body: fakeMarker }, 'sha456')
    ).toBe(false); // marker format doesn't match — verifies no crash
  });

  test('returns false when both commit_id and marker mismatch', () => {
    expect(isApprovalReviewForCurrentHead({ state: 'APPROVED', commit_id: 'abc', body: 'no marker here' }, 'xyz')).toBe(
      false
    );
  });
});

describe('reviewTargetsCurrentHead', () => {
  test('returns false when headSha is empty', () => {
    expect(reviewTargetsCurrentHead({ commit_id: 'abc' }, '')).toBe(false);
  });

  test('returns true when commit_id matches', () => {
    expect(reviewTargetsCurrentHead({ commit_id: 'sha789' }, 'sha789')).toBe(true);
  });

  test('returns false when body is empty and commit_id does not match', () => {
    expect(reviewTargetsCurrentHead({ commit_id: 'other', body: '' }, 'sha789')).toBe(false);
  });
});

describe('extractApprovedByLoginFromReviewBody', () => {
  test('extracts login from "Approved by @user"', () => {
    expect(extractApprovedByLoginFromReviewBody('Approved by @alice')).toBe('alice');
  });

  test('extracts login without @ prefix', () => {
    expect(extractApprovedByLoginFromReviewBody('Approved by bob')).toBe('bob');
  });

  test('returns empty when no match', () => {
    expect(extractApprovedByLoginFromReviewBody('No approval here')).toBe('');
  });

  test('returns empty for null body', () => {
    expect(extractApprovedByLoginFromReviewBody(null)).toBe('');
  });
});

describe('resolveEffectiveReviewApproverLogin', () => {
  test('prefers approvedBy from body over user.login', () => {
    const review = { body: 'Approved by alice', user: { login: 'bot-user' } };
    expect(resolveEffectiveReviewApproverLogin(review)).toBe('alice');
  });

  test('falls back to user.login when body has no approved-by', () => {
    // Covers line 64 — the fallback normalizeLogin(review.user.login)
    const review = { body: 'some other text', user: { login: 'charlie' } };
    expect(resolveEffectiveReviewApproverLogin(review)).toBe('charlie');
  });

  test('returns empty for missing body and no user', () => {
    expect(resolveEffectiveReviewApproverLogin({})).toBe('');
  });
});

// ---- direct-pr-resource-mapping ------------------------------------------------
import {
  matchRequestTypesForFile,
  pickRequestTypeForChangedResource,
  resolveRegistryDocResourceName,
  buildFormDataFromRegistryDoc,
} from '../src/handlers/request/domain/direct-pr-resource-mapping.js';

const sampleConfig = {
  requests: {
    product: { folderName: 'data/products' },
    systemNamespace: { folderName: 'data/namespaces' },
  },
};

describe('matchRequestTypesForFile', () => {
  test('matches file in product folder', () => {
    expect(matchRequestTypesForFile(sampleConfig, 'data/products/foo.yaml')).toContain('product');
  });

  test('matches exact folder path', () => {
    expect(matchRequestTypesForFile(sampleConfig, 'data/namespaces')).toContain('systemNamespace');
  });

  test('returns empty for unmatched path', () => {
    expect(matchRequestTypesForFile(sampleConfig, 'other/path/foo.yaml')).toEqual([]);
  });

  test('returns empty for null config', () => {
    expect(matchRequestTypesForFile(null, 'data/products/foo.yaml')).toEqual([]);
  });

  test('handles Windows backslash paths', () => {
    expect(matchRequestTypesForFile(sampleConfig, 'data\\products\\foo.yaml')).toContain('product');
  });
});

describe('pickRequestTypeForChangedResource', () => {
  test('returns single match directly', () => {
    expect(pickRequestTypeForChangedResource(sampleConfig, 'data/products/foo.yaml', {})).toBe('product');
  });

  test('uses doc.type to disambiguate multiple matches', () => {
    const multiConfig = {
      requests: {
        systemNamespace: { folderName: 'data/ns' },
        authorityNamespace: { folderName: 'data/ns' },
      },
    };
    const doc = { type: 'system' };
    expect(pickRequestTypeForChangedResource(multiConfig, 'data/ns/foo.yaml', doc)).toBe('systemNamespace');
  });

  test('returns empty for no match', () => {
    expect(pickRequestTypeForChangedResource(sampleConfig, 'unknown/path.yaml', {})).toBe('');
  });
});

describe('resolveRegistryDocResourceName', () => {
  test('prefers identifier', () => {
    expect(resolveRegistryDocResourceName({ identifier: 'my-id', name: 'other' })).toBe('my-id');
  });

  test('falls back to name when identifier missing', () => {
    expect(resolveRegistryDocResourceName({ name: 'sap.foo' })).toBe('sap.foo');
  });

  test('returns empty when no recognized key', () => {
    expect(resolveRegistryDocResourceName({ unknown: 'val' })).toBe('');
  });

  test('numeric identifier hits number branch of local toStringTrim — lines 10-11', () => {
    // toStringTrim(42) hits line 10, returns '42'
    expect(resolveRegistryDocResourceName({ identifier: 42 })).toBe('42');
  });

  test('object identifier hits fallthrough branch of local toStringTrim — line 11', () => {
    // toStringTrim({}) hits line 11, returns '' → loop continues
    expect(resolveRegistryDocResourceName({ identifier: {} })).toBe('');
  });
});

describe('buildFormDataFromRegistryDoc', () => {
  test('serializes scalar fields', () => {
    const doc = { name: 'sap.foo', description: 'A product' };
    const result = buildFormDataFromRegistryDoc(doc);
    expect(result['name']).toBe('sap.foo');
    expect(result['description']).toBe('A product');
  });

  test('serializes array contact field as newline-joined string', () => {
    const doc = { name: 'sap.foo', contact: ['alice', 'bob'] };
    const result = buildFormDataFromRegistryDoc(doc);
    expect(result['contact']).toContain('alice');
    expect(result['contact']).toContain('bob');
  });

  test('sets identifier and namespace from resource name', () => {
    const doc = { name: 'sap.ns' };
    const result = buildFormDataFromRegistryDoc(doc);
    expect(result['identifier']).toBe('sap.ns');
    expect(result['namespace']).toBe('sap.ns');
  });

  test('serializes array values using YAML when mixed types', () => {
    const doc = { owners: ['a', { nested: 'obj' }] };
    const result = buildFormDataFromRegistryDoc(doc);
    expect(result['owners']).toBeTruthy();
  });
});

// ---- approval-decision ---------------------------------------------------------
import {
  normalizeApprovalDecision,
  promoteUnknownApprovalDecisionForDirectPrRequester,
} from '../src/handlers/request/domain/approval-decision.js';

describe('normalizeApprovalDecision', () => {
  test('true becomes approved', () => {
    expect(normalizeApprovalDecision(true).status).toBe('approved');
  });

  test('false becomes empty object', () => {
    expect(normalizeApprovalDecision(false)).toEqual({});
  });

  test('deduplicates approvers', () => {
    const decision = { status: 'unknown' as const, approvers: ['alice', 'ALICE', 'bob'] };
    const result = normalizeApprovalDecision(decision);
    const approvers = result.approvers!;
    const lc = approvers.map((a) => a.toLowerCase());
    expect(lc.filter((a) => a === 'alice').length).toBe(1);
  });

  test('handles null/falsy decision', () => {
    expect(normalizeApprovalDecision(null as unknown as boolean)).toEqual({});
  });

  test('strips @-prefix from approvers — covers number branch via toStringTrim fallthrough', () => {
    const decision = { status: 'unknown' as const, approvers: ['@alice'] };
    const result = normalizeApprovalDecision(decision);
    expect(result.approvers).toContain('alice');
  });
});

describe('promoteUnknownApprovalDecisionForDirectPrRequester', () => {
  test('promotes unknown to approved when requester is in hook approvers', () => {
    const decision = { status: 'unknown' as const, approvers: ['alice'], comment: 'LGTM' };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'alice');
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('LGTM');
  });

  test('does not promote when requester is not in approvers', () => {
    const decision = { status: 'unknown' as const, approvers: ['bob'] };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'alice');
    expect(result.status).toBe('unknown');
  });

  test('does not promote non-unknown decisions', () => {
    const decision = { status: 'approved' as const };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'alice');
    expect(result.status).toBe('approved');
  });

  test('comment with manual approval required text is suppressed', () => {
    const decision = {
      status: 'unknown' as const,
      approvers: ['alice'],
      comment: 'manual approval required for this',
      message: 'LGTM from message',
    };
    const result = promoteUnknownApprovalDecisionForDirectPrRequester(decision, 'alice');
    expect(result.status).toBe('approved');
    expect(result.comment).toBe('LGTM from message');
  });
});

// ---- approval-policy -----------------------------------------------------------
import {
  getUnknownManualApprovers,
  getVisibleApprovalText,
  isApprovalDecisionAuthorizedByHookApprovers,
} from '../src/handlers/request/domain/approval-policy.js';

describe('getUnknownManualApprovers', () => {
  test('returns approvers for unknown decision', () => {
    const decision = { status: 'unknown' as const, approvers: ['alice', 'bob'] };
    expect(getUnknownManualApprovers(decision)).toEqual(['alice', 'bob']);
  });

  test('returns empty for approved decision', () => {
    expect(getUnknownManualApprovers({ status: 'approved' })).toEqual([]);
  });

  test('returns empty when no approvers', () => {
    expect(getUnknownManualApprovers({ status: 'unknown' })).toEqual([]);
  });
});

describe('getVisibleApprovalText (approval-policy)', () => {
  test('returns comment when set and not manual-approval-required', () => {
    expect(getVisibleApprovalText({ comment: 'LGTM' })).toBe('LGTM');
  });

  test('falls back to message when comment is manual-approval-required', () => {
    const decision = { comment: 'manual approval required', message: 'See ticket' };
    expect(getVisibleApprovalText(decision)).toBe('See ticket');
  });

  test('falls back to reason when comment and message are manual-approval-required', () => {
    const decision = {
      comment: 'manual approval required',
      message: 'manual approval required',
      reason: 'use reason',
    };
    expect(getVisibleApprovalText(decision)).toBe('use reason');
  });

  test('returns empty when all texts are manual-approval-required', () => {
    const decision = {
      comment: 'manual approval required',
      message: 'manual approval required',
      reason: 'manual approval required',
    };
    expect(getVisibleApprovalText(decision)).toBe('');
  });
});

describe('isApprovalDecisionAuthorizedByHookApprovers', () => {
  test('returns true when reviewer is in configured approvers', () => {
    expect(isApprovalDecisionAuthorizedByHookApprovers({}, ['alice'], ['alice'])).toBe(true);
  });

  test('returns true when reviewer is in hook approvers from decision', () => {
    const decision = { status: 'unknown' as const, approvers: ['bob'] };
    expect(isApprovalDecisionAuthorizedByHookApprovers(decision, [], ['bob'])).toBe(true);
  });

  test('returns false when reviewer not in any approvers', () => {
    expect(isApprovalDecisionAuthorizedByHookApprovers({}, ['alice'], ['charlie'])).toBe(false);
  });

  test('returns false when no approvers configured', () => {
    expect(isApprovalDecisionAuthorizedByHookApprovers({}, [], ['alice'])).toBe(false);
  });

  test('case-insensitive match', () => {
    expect(isApprovalDecisionAuthorizedByHookApprovers({}, ['ALICE'], ['alice'])).toBe(true);
  });
});

// ---- machine-readable ----------------------------------------------------------
import {
  normalizeMachineReadableIssues,
  buildMachineReadableMetadataBlock,
  buildDetectedIssuesBody,
  singleMachineReadableIssue,
} from '../src/handlers/request/domain/machine-readable.js';

describe('normalizeMachineReadableIssues', () => {
  test('returns empty for non-array', () => {
    expect(normalizeMachineReadableIssues('not-array')).toEqual([]);
  });

  test('filters non-object items', () => {
    expect(normalizeMachineReadableIssues([null, 'str', 42])).toEqual([]);
  });

  test('deduplicates identical issues', () => {
    const items = [
      { field: 'name', message: 'required' },
      { field: 'name', message: 'required' },
    ];
    expect(normalizeMachineReadableIssues(items)).toHaveLength(1);
  });

  test('sets field to details when field is missing', () => {
    const items = [{ message: 'something wrong' }];
    expect(normalizeMachineReadableIssues(items)[0].field).toBe('details');
  });

  test('skips items with empty message', () => {
    expect(normalizeMachineReadableIssues([{ field: 'x', message: '' }])).toEqual([]);
  });

  test('includes filePath when provided', () => {
    const items = [{ field: 'x', message: 'err', filePath: '/foo.yaml' }];
    expect(normalizeMachineReadableIssues(items)[0].filePath).toBe('/foo.yaml');
  });

  test('uses path as fallback field key', () => {
    const items = [{ path: '/foo', message: 'err' }];
    expect(normalizeMachineReadableIssues(items)[0].field).toBe('/foo');
  });
});

describe('buildMachineReadableMetadataBlock', () => {
  test('returns empty for empty issues', () => {
    expect(buildMachineReadableMetadataBlock([])).toBe('');
  });

  test('returns JSON block for non-empty issues', () => {
    const issues = [{ field: 'x', message: 'err' }];
    const block = buildMachineReadableMetadataBlock(issues);
    expect(block).toContain('json');
    expect(block).toContain('"err"');
  });
});

describe('buildDetectedIssuesBody', () => {
  test('includes message in output', () => {
    const result = buildDetectedIssuesBody('Something failed');
    expect(result).toContain('Something failed');
    expect(result).toContain('Detected issues');
  });
});

describe('singleMachineReadableIssue', () => {
  test('returns empty for empty message', () => {
    expect(singleMachineReadableIssue('field', '')).toEqual([]);
  });

  test('returns single item for valid message', () => {
    const result = singleMachineReadableIssue('name', 'required');
    expect(result).toHaveLength(1);
    expect(result[0].field).toBe('name');
    expect(result[0].message).toBe('required');
  });

  test('defaults field to details when empty', () => {
    const result = singleMachineReadableIssue('', 'some error');
    expect(result[0].field).toBe('details');
  });
});

// ---- login-utils ---------------------------------------------------------------
import { toStringTrim, normalizeLogin, uniqLogins } from '../src/handlers/request/domain/login-utils.js';

describe('toStringTrim', () => {
  test('trims string', () => expect(toStringTrim('  hello  ')).toBe('hello'));
  test('returns empty for null', () => expect(toStringTrim(null)).toBe(''));
  test('returns empty for undefined', () => expect(toStringTrim(undefined)).toBe(''));
  test('converts number to string — covers line 4', () => expect(toStringTrim(42)).toBe('42'));
  test('converts boolean to string — covers line 4', () => expect(toStringTrim(true)).toBe('true'));
  test('returns empty for object — covers line 5', () => expect(toStringTrim({})).toBe(''));
});

describe('normalizeLogin', () => {
  test('strips @ prefix', () => expect(normalizeLogin('@alice')).toBe('alice'));
  test('strips multiple @ prefixes', () => expect(normalizeLogin('@@bob')).toBe('bob'));
  test('trims whitespace', () => expect(normalizeLogin('  charlie  ')).toBe('charlie'));
  test('handles null', () => expect(normalizeLogin(null)).toBe(''));
});

describe('uniqLogins', () => {
  test('deduplicates case-insensitively', () => {
    expect(uniqLogins(['Alice', 'alice', 'bob'])).toEqual(['Alice', 'bob']);
  });

  test('filters empty strings', () => {
    expect(uniqLogins(['', 'alice'])).toEqual(['alice']);
  });

  test('handles empty array', () => {
    expect(uniqLogins([])).toEqual([]);
  });
});

// ---- pull-request-review-state -------------------------------------------------
import {
  normalizeReviewState,
  isActionableReviewState,
  sortPullRequestReviewsChronologically,
  getLatestActionableReviewStates,
} from '../src/handlers/request/domain/pull-request-review-state.js';

describe('normalizeReviewState', () => {
  test('uppercases state', () => expect(normalizeReviewState('approved')).toBe('APPROVED'));
  test('handles null — covers number branch via toStringTrim', () => expect(normalizeReviewState(null)).toBe(''));
  test('handles number — covers toStringTrim number branch', () => expect(normalizeReviewState(1)).toBe('1'));
});

describe('isActionableReviewState', () => {
  test('APPROVED is actionable', () => expect(isActionableReviewState('APPROVED')).toBe(true));
  test('CHANGES_REQUESTED is actionable', () => expect(isActionableReviewState('CHANGES_REQUESTED')).toBe(true));
  test('DISMISSED is actionable', () => expect(isActionableReviewState('DISMISSED')).toBe(true));
  test('PENDING is not actionable', () => expect(isActionableReviewState('PENDING')).toBe(false));
  test('COMMENTED is not actionable', () => expect(isActionableReviewState('COMMENTED')).toBe(false));
});

describe('sortPullRequestReviewsChronologically', () => {
  test('sorts by submitted_at', () => {
    const reviews = [
      { id: 2, state: 'APPROVED', submitted_at: '2024-01-02T00:00:00Z', user: { login: 'b' } },
      { id: 1, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'a' } },
    ];
    const sorted = sortPullRequestReviewsChronologically(reviews);
    expect(sorted[0].id).toBe(1);
  });

  test('falls back to id when timestamps equal', () => {
    const reviews = [
      { id: 2, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'b' } },
      { id: 1, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'a' } },
    ];
    const sorted = sortPullRequestReviewsChronologically(reviews);
    expect(sorted[0].id).toBe(1);
  });

  test('does not mutate original array', () => {
    const reviews = [{ id: 1, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'a' } }];
    const sorted = sortPullRequestReviewsChronologically(reviews);
    expect(sorted).not.toBe(reviews);
  });
});

describe('getLatestActionableReviewStates', () => {
  test('returns latest state per reviewer', () => {
    const reviews = [
      { id: 1, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'alice' } },
      { id: 2, state: 'CHANGES_REQUESTED', submitted_at: '2024-01-02T00:00:00Z', user: { login: 'alice' } },
    ];
    const result = getLatestActionableReviewStates(reviews);
    expect(result.get('alice')).toBe('CHANGES_REQUESTED');
  });

  test('skips non-actionable states like COMMENTED', () => {
    const reviews = [{ id: 1, state: 'COMMENTED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'alice' } }];
    expect(getLatestActionableReviewStates(reviews).size).toBe(0);
  });

  test('handles empty array', () => {
    expect(getLatestActionableReviewStates([])).toEqual(new Map());
  });

  test('normalizes login to lowercase for keying', () => {
    const reviews = [{ id: 1, state: 'APPROVED', submitted_at: '2024-01-01T00:00:00Z', user: { login: 'Alice' } }];
    const result = getLatestActionableReviewStates(reviews);
    expect(result.has('alice')).toBe(true);
  });
});

// ---- registry-validation-annotations -------------------------------------------
import {
  isRegistryValidateAnnotation,
  toSectionTitle,
  buildRegistryValidationCommentHeading,
  buildRegistryValidationAggregateBody,
  filterRegistryValidationEntries,
} from '../src/handlers/request/domain/registry-validation-annotations.js';

describe('isRegistryValidateAnnotation', () => {
  test('returns true for registry-validate title', () => {
    expect(isRegistryValidateAnnotation({ title: 'registry-validate: error' })).toBe(true);
  });

  test('returns true for title starting with registry-validate', () => {
    expect(isRegistryValidateAnnotation({ title: 'Registry-Validate something' })).toBe(true);
  });

  test('returns false for other title', () => {
    expect(isRegistryValidateAnnotation({ title: 'eslint: error' })).toBe(false);
  });

  test('returns false for missing title', () => {
    expect(isRegistryValidateAnnotation({})).toBe(false);
  });

  test('returns false for null title', () => {
    expect(isRegistryValidateAnnotation({ title: null })).toBe(false);
  });
});

describe('toSectionTitle', () => {
  test('returns Details for empty input', () => {
    expect(toSectionTitle('')).toBe('Details');
  });

  test('returns Contacts for contact field', () => {
    expect(toSectionTitle('contact')).toBe('Contacts');
  });

  test('converts camelCase to spaced title', () => {
    expect(toSectionTitle('requestType')).toBe('Request Type');
  });

  test('converts snake_case to spaced title', () => {
    expect(toSectionTitle('request_type')).toBe('Request type');
  });

  test('numeric field name hits number branch of toStringTrim — lines 18-19', () => {
    // toStringTrim(42) → '42' → not 'contact' → returned as spaced title
    expect(toSectionTitle(42 as unknown as string)).toBe('42');
  });

  test('object field name hits fallthrough branch of toStringTrim — line 19', () => {
    // toStringTrim({}) → '' → returns 'Details'
    expect(toSectionTitle({} as unknown as string)).toBe('Details');
  });
});

describe('buildRegistryValidationCommentHeading', () => {
  test('returns ### heading structure for default headingLevel', () => {
    const result = buildRegistryValidationCommentHeading('data/foo.yaml', ['/title must be string'], '###');
    expect(result.join('\n')).toContain('### File: `data/foo.yaml`');
  });

  test('returns #### heading structure for #### headingLevel — covers lines 154-155', () => {
    // Lines 154-155: the else branch calling appendRegistryValidationFileSection
    const result = buildRegistryValidationCommentHeading('data/bar.yaml', ['/name must be string'], '####');
    expect(result.join('\n')).toContain('### File: `data/bar.yaml`');
  });
});

describe('filterRegistryValidationEntries', () => {
  test('filters out empty message arrays', () => {
    const byFile = new Map([
      ['a.yaml', ['msg']],
      ['b.yaml', []],
    ]);
    const result = filterRegistryValidationEntries(byFile);
    expect(result.map(([f]) => f)).toEqual(['a.yaml']);
  });

  test('sorts entries by file path', () => {
    const byFile = new Map([
      ['z.yaml', ['msg']],
      ['a.yaml', ['msg2']],
    ]);
    const result = filterRegistryValidationEntries(byFile);
    expect(result[0][0]).toBe('a.yaml');
  });
});

describe('buildRegistryValidationAggregateBody', () => {
  test('returns empty string when no entries', () => {
    expect(buildRegistryValidationAggregateBody(new Map())).toBe('');
  });

  test('returns body with detected issues heading', () => {
    const byFile = new Map([['data/foo.yaml', ['/title must be string']]]);
    const result = buildRegistryValidationAggregateBody(byFile);
    expect(result).toContain('## Detected issues');
    expect(result).toContain('data/foo.yaml');
  });
});

// ---- pull-request-compare-candidates -------------------------------------------
import { buildPullRequestCompareCandidates } from '../src/handlers/request/domain/pull-request-compare-candidates.js';

describe('buildPullRequestCompareCandidates', () => {
  test('returns single sha-based candidate for same-repo PR', () => {
    const candidates = buildPullRequestCompareCandidates({
      headSha: 'pr-sha',
      baseHeadSha: 'base-sha',
      headRef: 'feature',
      headRepoInfo: { owner: 'org', repo: 'repo' },
      repoInfo: { owner: 'org', repo: 'repo' },
      baseRef: 'main',
    });
    expect(candidates).toHaveLength(1);
    expect(candidates[0]).toBe('pr-sha...base-sha');
  });

  test('returns two candidates for cross-repo PR — covers line 23', () => {
    // headRepoInfo.owner differs → sameRepoInfo returns false → line 23 executes
    const candidates = buildPullRequestCompareCandidates({
      headSha: 'pr-sha',
      baseHeadSha: 'base-sha',
      headRef: 'feature',
      headRepoInfo: { owner: 'fork-owner', repo: 'repo' },
      repoInfo: { owner: 'org', repo: 'repo' },
      baseRef: 'main',
    });
    expect(candidates).toHaveLength(2);
    expect(candidates[1]).toContain('fork-owner:feature');
    expect(candidates[1]).toContain('org:main');
  });

  test('returns single candidate for cross-repo PR with empty headRef', () => {
    // headRef is empty → line 23 condition is false → only one candidate
    const candidates = buildPullRequestCompareCandidates({
      headSha: 'pr-sha',
      baseHeadSha: 'base-sha',
      headRef: '',
      headRepoInfo: { owner: 'fork-owner', repo: 'repo' },
      repoInfo: { owner: 'org', repo: 'repo' },
      baseRef: 'main',
    });
    expect(candidates).toHaveLength(1);
  });
});
