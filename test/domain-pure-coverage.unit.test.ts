/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect } from '@jest/globals';
import { readRoutingLockExpected } from '../src/handlers/request/domain/routing-lock-marker.js';
import { summarizeHeadGreenRun } from '../src/handlers/request/domain/check-conclusions.js';
import {
  normalizeMachineReadableIssues,
  singleMachineReadableIssue,
} from '../src/handlers/request/domain/machine-readable.js';
import { sortPullRequestReviewsChronologically } from '../src/handlers/request/domain/pull-request-review-state.js';

// ── routing-lock-marker: toStringTrim branches ────────────────────────────────

describe('readRoutingLockExpected — toStringTrim branches', () => {
  test('L7 if-body: meta has no "expected" key → toStringTrim(undefined) → empty string', () => {
    const body = '<!-- nsreq:routing-lock = {"v":1} -->';
    expect(readRoutingLockExpected(body)).toBe('');
  });

  test('L9 false arm: meta.expected is an object → toStringTrim returns ""', () => {
    const body = '<!-- nsreq:routing-lock = {"v":1,"expected":{"nested":"val"}} -->';
    expect(readRoutingLockExpected(body)).toBe('');
  });
});

// ── check-conclusions: toStringTrim L18 false arm ─────────────────────────────

describe('summarizeHeadGreenRun — toStringTrim L18 false arm', () => {
  test('L18 false arm: name is an object → toStringTrim returns "" → falls back to __unnamed__', () => {
    const result = summarizeHeadGreenRun({ name: {} as any });
    expect(result.name).toBe('__unnamed__');
  });
});

// ── machine-readable: toStringTrim L14 false arm and L77 true arm ─────────────

describe('normalizeMachineReadableIssues — toStringTrim L14 false arm', () => {
  test('L14 false arm: message is an object → toStringTrim returns "" → item skipped', () => {
    const result = normalizeMachineReadableIssues([{ field: 'name', message: {} as any }]);
    expect(result).toEqual([]);
  });
});

describe('singleMachineReadableIssue — L77 true arm', () => {
  test('L77 true arm: filePath provided → { filePath } included in result', () => {
    const result = singleMachineReadableIssue('field', 'message', 'resources/foo.yaml');
    expect(result).toHaveLength(1);
    expect(result[0].filePath).toBe('resources/foo.yaml');
  });
});

// ── pull-request-review-state: id and toStringTrim branches ──────────────────

describe('sortPullRequestReviewsChronologically — L11 false arm and L36/L37 false arms', () => {
  test('L11 false arm: submitted_at is an object → toStringTrim returns "" → Date.parse(NaN) → id fallback', () => {
    const sorted = sortPullRequestReviewsChronologically([
      { submitted_at: {} as any, id: 2 },
      { submitted_at: {} as any, id: 1 },
    ]);
    expect(sorted[0].id).toBe(1);
    expect(sorted[1].id).toBe(2);
  });

  test('L36+L37 false arms: id is not a number → fallback to 0', () => {
    const sorted = sortPullRequestReviewsChronologically([
      { submitted_at: 'not-a-valid-date', id: 'abc' as unknown as number },
      { submitted_at: 'not-a-valid-date', id: undefined as unknown as number },
    ]);
    expect(sorted).toHaveLength(2);
  });
});
