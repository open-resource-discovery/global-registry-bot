import {
  readContactApprovalMeta,
  stripContactApprovalFromBody,
  buildContactApprovalBody,
  readParentApprovalMeta,
  stripParentApprovalFromBody,
  buildParentApprovalBody,
  type ContactApprovalMeta,
  type ParentApprovalMeta,
} from '../src/handlers/request/domain/approval-markers.js';

function contactBody(json: unknown): string {
  return `body text\n\n<!-- nsreq:contact-approval = ${JSON.stringify(json)} -->`;
}

function parentBody(json: unknown): string {
  return `body text\n\n<!-- nsreq:parent-approval = ${JSON.stringify(json)} -->`;
}

// ── stripContactApprovalFromBody ─────────────────────────────────────────────

test('stripContactApprovalFromBody: removes embedded comment', () => {
  const body = 'Text\n\n<!-- nsreq:contact-approval = {"v":1} -->\nMore';
  expect(stripContactApprovalFromBody(body)).not.toContain('nsreq:contact-approval');
  expect(stripContactApprovalFromBody(body)).toContain('Text');
});

test('stripContactApprovalFromBody: null/undefined input returns empty string', () => {
  expect(stripContactApprovalFromBody(null)).toBe('');
  expect(stripContactApprovalFromBody(undefined)).toBe('');
});

test('stripContactApprovalFromBody: body without comment returned trimmed', () => {
  expect(stripContactApprovalFromBody('plain text')).toBe('plain text');
});

// ── readContactApprovalMeta ──────────────────────────────────────────────────

test('readContactApprovalMeta: returns null when no match in body', () => {
  expect(readContactApprovalMeta('no comment here')).toBeNull();
  expect(readContactApprovalMeta(null)).toBeNull();
  expect(readContactApprovalMeta(undefined)).toBeNull();
});

test('readContactApprovalMeta: returns null for invalid JSON (catch path)', () => {
  expect(readContactApprovalMeta('<!-- nsreq:contact-approval = {invalid} -->')).toBeNull();
});

test('readContactApprovalMeta: returns null when parsed value is array (not plain object)', () => {
  expect(readContactApprovalMeta('<!-- nsreq:contact-approval = [1,2,3] -->')).toBeNull();
});

test('readContactApprovalMeta: returns null when v !== 1', () => {
  expect(readContactApprovalMeta(contactBody({ v: 2, target: 'foo', owners: ['alice'] }))).toBeNull();
});

test('readContactApprovalMeta: returns null when target is empty string', () => {
  expect(readContactApprovalMeta(contactBody({ v: 1, target: '', owners: ['alice'] }))).toBeNull();
});

test('readContactApprovalMeta: returns null when target is object (toStringTrim fallthrough → "")', () => {
  // toStringTrim({}): not null, not string, not number/boolean → return ''
  expect(readContactApprovalMeta(contactBody({ v: 1, target: {}, owners: ['alice'] }))).toBeNull();
});

test('readContactApprovalMeta: returns null when owners is empty array', () => {
  expect(readContactApprovalMeta(contactBody({ v: 1, target: 'some/path', owners: [] }))).toBeNull();
});

test('readContactApprovalMeta: returns null when owners is not array (ownersRaw fallback → [])', () => {
  // ownersRaw = 'not-array' → !Array.isArray → owners = [] → !owners.length → null
  expect(readContactApprovalMeta(contactBody({ v: 1, target: 'some/path', owners: 'not-array' }))).toBeNull();
});

test('readContactApprovalMeta: parses valid meta without optional fields', () => {
  const body = contactBody({ v: 1, target: 'some/path', owners: ['alice', 'bob'] });
  expect(readContactApprovalMeta(body)).toEqual({ v: 1, target: 'some/path', owners: ['alice', 'bob'] });
});

test('readContactApprovalMeta: includes approvedBy and approvedAt when present', () => {
  const body = contactBody({
    v: 1,
    target: 'some/path',
    owners: ['alice'],
    approvedBy: '@charlie',
    approvedAt: '2024-01-01T00:00:00Z',
  });
  const result = readContactApprovalMeta(body);
  expect(result?.approvedBy).toBe('charlie');
  expect(result?.approvedAt).toBe('2024-01-01T00:00:00Z');
});

test('readContactApprovalMeta: numeric target triggers toStringTrim number branch → "42"', () => {
  // toStringTrim(42): L25 arm 1 (not string), L26 arm 0 (number) → '42'
  const body = contactBody({ v: 1, target: 42, owners: ['alice'] });
  const result = readContactApprovalMeta(body);
  expect(result?.target).toBe('42');
});

test('readContactApprovalMeta: boolean approvedAt triggers toStringTrim boolean branch → "true"', () => {
  // toStringTrim(true): typeof true === 'number' false (arm 1), typeof true === 'boolean' true → 'true'
  const body = contactBody({ v: 1, target: 'some/path', owners: ['alice'], approvedAt: true });
  const result = readContactApprovalMeta(body);
  expect(result?.approvedAt).toBe('true');
});

test('readContactApprovalMeta: deduplicates owners and strips @ prefix', () => {
  // '@' → normalizeLogin → '' → !login arm in uniqLogins
  // 'Alice' + 'ALICE' → same key → seen.has arm in uniqLogins
  const body = contactBody({ v: 1, target: 'some/path', owners: ['Alice', 'ALICE', '@bob', '@'] });
  const result = readContactApprovalMeta(body);
  expect(result?.owners).toEqual(['Alice', 'bob']);
});

// ── buildContactApprovalBody ─────────────────────────────────────────────────

test('buildContactApprovalBody: returns cleaned body + newline when meta is null', () => {
  const body = 'text<!-- nsreq:contact-approval = {"v":1,"target":"x","owners":[]} -->';
  expect(buildContactApprovalBody(body, null)).toBe('text\n');
});

test('buildContactApprovalBody: appends meta comment without optional fields', () => {
  const meta: ContactApprovalMeta = { v: 1, target: 'some/path', owners: ['alice'] };
  const result = buildContactApprovalBody('body', meta);
  expect(result).toContain('nsreq:contact-approval');
  expect(result).toContain('"target":"some/path"');
  expect(result).not.toContain('approvedBy');
  expect(result).not.toContain('approvedAt');
});

test('buildContactApprovalBody: includes approvedBy and approvedAt when truthy', () => {
  const meta: ContactApprovalMeta = {
    v: 1,
    target: 'some/path',
    owners: ['alice'],
    approvedBy: 'charlie',
    approvedAt: '2024-01-01',
  };
  const result = buildContactApprovalBody('body', meta);
  expect(result).toContain('"approvedBy":"charlie"');
  expect(result).toContain('"approvedAt":"2024-01-01"');
});

test('buildContactApprovalBody: handles undefined meta.owners via || []', () => {
  const meta = { v: 1, target: 'some/path', owners: undefined } as unknown as ContactApprovalMeta;
  const result = buildContactApprovalBody('body', meta);
  expect(result).toContain('"owners":[]');
});

// ── stripParentApprovalFromBody ──────────────────────────────────────────────

test('stripParentApprovalFromBody: removes embedded parent-approval comment', () => {
  const body = 'Text\n\n<!-- nsreq:parent-approval = {"v":1} -->\nMore';
  expect(stripParentApprovalFromBody(body)).not.toContain('nsreq:parent-approval');
});

test('stripParentApprovalFromBody: null/undefined input returns empty string', () => {
  expect(stripParentApprovalFromBody(null)).toBe('');
  expect(stripParentApprovalFromBody(undefined)).toBe('');
});

// ── readParentApprovalMeta ───────────────────────────────────────────────────

test('readParentApprovalMeta: returns null when no match', () => {
  expect(readParentApprovalMeta('no comment')).toBeNull();
  expect(readParentApprovalMeta(null)).toBeNull();
});

test('readParentApprovalMeta: returns null for invalid JSON', () => {
  expect(readParentApprovalMeta('<!-- nsreq:parent-approval = {bad} -->')).toBeNull();
});

test('readParentApprovalMeta: returns null when not plain object', () => {
  expect(readParentApprovalMeta('<!-- nsreq:parent-approval = [1,2] -->')).toBeNull();
});

test('readParentApprovalMeta: returns null when v !== 1', () => {
  expect(
    readParentApprovalMeta(parentBody({ v: 2, parent: 'org/repo', target: 'sub/path', owners: ['a'] }))
  ).toBeNull();
});

test('readParentApprovalMeta: returns null when parent is empty', () => {
  expect(readParentApprovalMeta(parentBody({ v: 1, parent: '', target: 'sub/path', owners: ['a'] }))).toBeNull();
});

test('readParentApprovalMeta: returns null when target is empty', () => {
  expect(readParentApprovalMeta(parentBody({ v: 1, parent: 'org/repo', target: '', owners: ['a'] }))).toBeNull();
});

test('readParentApprovalMeta: owners not array falls back to [] (Array.isArray arm 1)', () => {
  const body = parentBody({ v: 1, parent: 'org/repo', target: 'sub/path', owners: 'string-not-array' });
  expect(readParentApprovalMeta(body)).toEqual({ v: 1, parent: 'org/repo', target: 'sub/path', owners: [] });
});

test('readParentApprovalMeta: parses valid meta', () => {
  const body = parentBody({ v: 1, parent: 'org/repo', target: 'sub/path', owners: ['alice', 'bob'] });
  expect(readParentApprovalMeta(body)).toEqual({
    v: 1,
    parent: 'org/repo',
    target: 'sub/path',
    owners: ['alice', 'bob'],
  });
});

test('readParentApprovalMeta: includes approvedBy and approvedAt when present', () => {
  const body = parentBody({
    v: 1,
    parent: 'org/repo',
    target: 'sub/path',
    owners: ['alice'],
    approvedBy: '@charlie',
    approvedAt: '2024-01-01',
  });
  const result = readParentApprovalMeta(body);
  expect(result?.approvedBy).toBe('charlie');
  expect(result?.approvedAt).toBe('2024-01-01');
});

test('readParentApprovalMeta: numeric parent triggers toStringTrim number branch → "42"', () => {
  // toStringTrim(42): L26 arm 0 (number) → '42'
  const body = parentBody({ v: 1, parent: 42, target: 'sub/path', owners: ['alice'] });
  expect(readParentApprovalMeta(body)?.parent).toBe('42');
});

test('readParentApprovalMeta: boolean approvedAt triggers toStringTrim boolean branch → "false"', () => {
  // toStringTrim(false): typeof false === 'number' → false (arm 1), typeof false === 'boolean' → true → 'false'
  const body = parentBody({
    v: 1,
    parent: 'org/repo',
    target: 'sub/path',
    owners: ['alice'],
    approvedAt: false,
  });
  expect(readParentApprovalMeta(body)?.approvedAt).toBe('false');
});

test('readParentApprovalMeta: object parent triggers toStringTrim fallthrough → "" → null', () => {
  // toStringTrim({}): not null, not string, not number/boolean → ''
  const body = parentBody({ v: 1, parent: {}, target: 'sub/path', owners: ['alice'] });
  expect(readParentApprovalMeta(body)).toBeNull();
});

// ── buildParentApprovalBody ──────────────────────────────────────────────────

test('buildParentApprovalBody: returns cleaned body + newline when meta is null', () => {
  const body = 'text <!-- nsreq:parent-approval = {"v":1,"parent":"x","target":"y","owners":[]} -->';
  expect(buildParentApprovalBody(body, null)).toBe('text\n');
});

test('buildParentApprovalBody: appends meta comment without optional fields', () => {
  const meta: ParentApprovalMeta = { v: 1, parent: 'org/repo', target: 'sub/path', owners: ['alice'] };
  const result = buildParentApprovalBody('body', meta);
  expect(result).toContain('nsreq:parent-approval');
  expect(result).toContain('"parent":"org/repo"');
  expect(result).not.toContain('approvedBy');
  expect(result).not.toContain('approvedAt');
});

test('buildParentApprovalBody: includes approvedBy and approvedAt when truthy', () => {
  const meta: ParentApprovalMeta = {
    v: 1,
    parent: 'org/repo',
    target: 'sub/path',
    owners: ['alice'],
    approvedBy: 'charlie',
    approvedAt: '2024-01-01',
  };
  const result = buildParentApprovalBody('body', meta);
  expect(result).toContain('"approvedBy":"charlie"');
  expect(result).toContain('"approvedAt":"2024-01-01"');
});

test('buildParentApprovalBody: handles undefined meta.owners via || []', () => {
  const meta = { v: 1, parent: 'org/repo', target: 'sub/path', owners: undefined } as unknown as ParentApprovalMeta;
  const result = buildParentApprovalBody('body', meta);
  expect(result).toContain('"owners":[]');
});
