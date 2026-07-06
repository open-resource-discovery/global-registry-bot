/* eslint-disable require-await */
import { jest } from '@jest/globals';
import {
  extractParentContactCandidates,
  lookupGithubLoginsByEmail,
} from '../src/handlers/request/application/parent-contact-resolution.js';

// ── extractParentContactCandidates ────────────────────────────────────────────

test('returns empty when input is null', () => {
  expect(extractParentContactCandidates(null)).toEqual({ logins: [], emails: [] });
});

test('returns empty when input is undefined', () => {
  expect(extractParentContactCandidates(undefined)).toEqual({ logins: [], emails: [] });
});

test('extracts @login from string', () => {
  const { logins } = extractParentContactCandidates('@alice');
  expect(logins).toContain('alice');
});

test('extracts plain login when single token (no dot, LOGIN_RE matches)', () => {
  const { logins } = extractParentContactCandidates('alice');
  expect(logins).toContain('alice');
});

test('skips @login containing a dot (fails LOGIN_RE)', () => {
  const { logins } = extractParentContactCandidates('@alice.bob');
  expect(logins).not.toContain('alice.bob');
});

test('extracts email from string', () => {
  const { emails } = extractParentContactCandidates('user@example.com');
  expect(emails).toContain('user@example.com');
});

test('ignores non-email non-login token', () => {
  const result = extractParentContactCandidates('not-an-email-or-login.with.dots');
  expect(result.emails).toHaveLength(0);
  expect(result.logins).toHaveLength(0);
});

test('extracts login from GitHub URL', () => {
  const { logins } = extractParentContactCandidates('https://github.com/alice-user');
  expect(logins).toContain('alice-user');
});

test('extracts multiple logins from comma-separated string', () => {
  const { logins } = extractParentContactCandidates('@alice, @bob');
  expect(logins).toContain('alice');
  expect(logins).toContain('bob');
});

test('extracts from number value (walk number/boolean branch)', () => {
  // number → String(42) → single token, LOGIN_RE accepts alphanum start → login
  const result = extractParentContactCandidates(42 as unknown as string);
  expect(result.logins).toContain('42');
});

test('extracts from boolean value (walk boolean branch)', () => {
  const result = extractParentContactCandidates(false as unknown as string);
  expect(result.logins).toContain('false');
});

test('extracts from array (walk array branch)', () => {
  const { logins } = extractParentContactCandidates(['@alice', '@bob']);
  expect(logins).toContain('alice');
  expect(logins).toContain('bob');
});

test('extracts from plain object (walk object branch)', () => {
  const { logins } = extractParentContactCandidates({ github: '@carol' });
  expect(logins).toContain('carol');
});

test('strong hint from key "github" — pushes plain login token', () => {
  const { logins } = extractParentContactCandidates({ github: 'alice' });
  expect(logins).toContain('alice');
});

test('email in angle brackets is stripped before validation', () => {
  const { emails } = extractParentContactCandidates('<user@example.com>');
  expect(emails).toContain('user@example.com');
});

test('deduplicates logins case-insensitively', () => {
  const { logins } = extractParentContactCandidates(['@Alice', '@alice']);
  expect(logins).toHaveLength(1);
});

test('deduplicates emails', () => {
  const { emails } = extractParentContactCandidates(['user@example.com', 'USER@EXAMPLE.COM']);
  expect(emails).toHaveLength(1);
});

// ── lookupGithubLoginsByEmail ─────────────────────────────────────────────────

test('returns empty for empty email', async () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect(await lookupGithubLoginsByEmail({} as any, '')).toEqual([]);
});

test('returns empty for email without @', async () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect(await lookupGithubLoginsByEmail({} as any, 'not-an-email')).toEqual([]);
});

test('returns logins from REST search when found', async () => {
  const searchUsers = jest.fn(async () => ({
    data: { items: [{ login: 'alice' }, { login: 'bob' }] },
  }));
  const ctx = { octokit: { search: { users: searchUsers } } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'rest-hit-unique-a1@example.com');
  expect(result).toContain('alice');
  expect(result).toContain('bob');
});

test('falls back to GraphQL when REST returns empty items', async () => {
  const searchUsers = jest.fn(async () => ({ data: { items: [] } }));
  const graphql = jest.fn(async () => ({
    search: { nodes: [{ login: 'graphql-user' }] },
  }));
  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'graphql-fallback-unique-b2@example.com');
  expect(result).toContain('graphql-user');
});

test('returns empty when both REST and GraphQL throw', async () => {
  const searchUsers = jest.fn(async () => {
    throw new Error('REST failed');
  });
  const graphql = jest.fn(async () => {
    throw new Error('GraphQL failed');
  });
  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'both-fail-unique-c3@example.com');
  expect(result).toEqual([]);
});

test('returns cached result on second call with same email', async () => {
  const searchUsers = jest.fn(async () => ({
    data: { items: [{ login: 'cached-user' }] },
  }));
  const ctx = { octokit: { search: { users: searchUsers } } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  await lookupGithubLoginsByEmail(ctx as any, 'cache-test-unique-d4@example.com');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'cache-test-unique-d4@example.com');
  expect(result).toContain('cached-user');
  expect(searchUsers).toHaveBeenCalledTimes(1);
});

// ── extractParentContactCandidates edge cases ─────────────────────────────────

test('L46: strips bracket-only token to empty string, skips it (continue arm)', () => {
  const { logins, emails } = extractParentContactCandidates('<>');
  expect(logins).toHaveLength(0);
  expect(emails).toHaveLength(0);
});

test('L82: non-plain-object value (e.g. function) is silently ignored', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = extractParentContactCandidates((() => {}) as any);
  expect(result).toEqual({ logins: [], emails: [] });
});

// ── lookupGithubLoginsByEmail – null/empty branch arms ─────────────────────────

test('L112: REST returns null items → ?? [] arm, falls through to GraphQL', async () => {
  const searchUsers = jest.fn(async () => ({ data: {} }));

  const graphql = jest.fn(async () => ({ search: { nodes: [] } }));

  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'null-items-e5@example.com');
  expect(result).toEqual([]);
});

test('L115: REST items contain empty login → if(login) false arm', async () => {
  const searchUsers = jest.fn(async () => ({ data: { items: [{ login: '' }, { login: null }] } }));

  const graphql = jest.fn(async () => ({ search: { nodes: [] } }));

  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'empty-login-f6@example.com');
  expect(result).toEqual([]);
});

test('L139: GraphQL returns null nodes → ?? [] arm', async () => {
  const searchUsers = jest.fn(async () => ({ data: { items: [] } }));

  const graphql = jest.fn(async () => ({ search: {} }));

  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'null-nodes-g7@example.com');
  expect(result).toEqual([]);
});

test('L142: GraphQL nodes contain empty login → if(login) false arm', async () => {
  const searchUsers = jest.fn(async () => ({ data: { items: [] } }));

  const graphql = jest.fn(async () => ({ search: { nodes: [{ login: '' }, { login: null }] } }));

  const ctx = { octokit: { search: { users: searchUsers }, graphql } };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await lookupGithubLoginsByEmail(ctx as any, 'empty-gql-login-h8@example.com');
  expect(result).toEqual([]);
});
