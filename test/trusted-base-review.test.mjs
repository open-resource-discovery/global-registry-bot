import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  allowedDataPath,
  verifyReviewSnapshot,
  verifyTree,
  approveTrustedReview,
  verifyGitChanges,
} from '../scripts/trusted-base-review.mjs';

const HEAD = 'a'.repeat(40);
const BASE = 'b'.repeat(40);
const repo = { id: 123, default_branch: 'main' };
const pr = {
  number: 2,
  state: 'open',
  draft: false,
  changed_files: 1,
  user: { id: 148587, type: 'Bot' },
  head: { sha: HEAD, repo },
  base: { sha: BASE, ref: 'main', repo },
};
const event = { pull_request: pr, sender: { id: 148587, type: 'Bot' } };
const files = [{ filename: 'data/namespaces/a.yaml', status: 'added' }];
const base = { branch: 'main', sha: BASE };
test('accepts complete static metadata changes at the current same-repository head', () => {
  assert.equal(verifyReviewSnapshot(event, pr, repo, base, files).head, HEAD);
});
test('rejects mixed metadata/code changes and rename sources outside data', () => {
  assert.throws(
    () =>
      verifyReviewSnapshot(event, { ...pr, changed_files: 2 }, repo, base, [
        ...files,
        { filename: 'scripts/execute.mjs', status: 'added' },
      ]),
    /allowlist/
  );
  assert.throws(
    () =>
      verifyReviewSnapshot(event, pr, repo, base, [
        { ...files[0], status: 'renamed', previous_filename: '.github/workflows/ci.yml' },
      ]),
    /allowlist/
  );
});
test('rejects traversal and Git control paths', () => {
  for (const value of [
    'data/../run.yaml',
    'data/.git/hooks/run.yaml',
    'data/x\\file.yaml',
    'data/code.js',
    'data/.gitconfig.yaml',
    'data/namespaces/a.yaml ',
  ])
    assert.equal(allowedDataPath(value), false, value);
});
test('rejects incomplete lists, fork origins, drafts, changed heads and bases', () => {
  assert.throws(() => verifyReviewSnapshot(event, { ...pr, changed_files: 2 }, repo, base, files), /allowlist/);
  for (const change of [
    { draft: true },
    { head: { ...pr.head, sha: 'c'.repeat(40) } },
    { head: { ...pr.head, repo: { id: 456 } } },
    { base: { ...pr.base, sha: 'c'.repeat(40) } },
  ])
    assert.throws(() => verifyReviewSnapshot(event, { ...pr, ...change }, repo, base, files), /revision/);
});
const tree = {
  tree: [
    { path: 'data', type: 'tree', mode: '040000' },
    { path: 'data/namespaces', type: 'tree', mode: '040000' },
    { path: files[0].filename, type: 'blob', mode: '100644', sha: HEAD, size: 10 },
  ],
};
test('rejects symlinks, executable files, oversized blobs and truncated trees before materializing data', () => {
  assert.ok(verifyTree(tree, files).has(files[0].filename));
  for (const change of [{ mode: '120000' }, { mode: '100755' }, { size: 50 * 1024 * 1024 }])
    assert.throws(
      () => verifyTree({ tree: [...tree.tree.slice(0, 2), { ...tree.tree[2], ...change }] }, files),
      /bounded regular/
    );
  assert.throws(() => verifyTree({ ...tree, truncated: true }, files), /Incomplete/);
});
test('approval binds to the verified head and rejects a different service identity or human touch', async () => {
  const temp = await mkdtemp(path.join(os.tmpdir(), 'review-test-'));
  const originalFetch = globalThis.fetch;
  const writes = [];
  try {
    const eventPath = path.join(temp, 'event.json');
    await writeFile(eventPath, JSON.stringify(event));
    globalThis.fetch = (url, options) => {
      const key = url.replace('https://api.example.test/repos/ORD/repo/', '').split('?')[0];
      const records = {
        '': repo,
        'git/ref/heads/main': { object: { sha: BASE } },
        'pulls/2': pr,
        'pulls/2/files': files,
        'pulls/2/requested_reviewers': { users: [], teams: [] },
      };
      if (options.method === 'POST') {
        writes.push(JSON.parse(options.body));
        return new Response('{}');
      }
      assert.ok(key in records, key);
      return new Response(JSON.stringify(records[key]));
    };
    const env = {
      GITHUB_API_URL: 'https://api.example.test',
      GITHUB_REPOSITORY: 'ORD/repo',
      GITHUB_TOKEN: 'fixture',
      GITHUB_EVENT_NAME: 'pull_request_target',
      GITHUB_EVENT_PATH: eventPath,
      VERIFIED_PR_HEAD: HEAD,
      VERIFIED_PR_BASE: BASE,
      EXPECTED_BOT_ID: '148587',
      REGISTRY_VALIDATE_OK: 'success',
      VALIDATED_FILE_COUNT: '1',
    };
    assert.equal((await approveTrustedReview(env)).approved, true);
    assert.equal(writes[0].commit_id, HEAD);
    assert.equal((await approveTrustedReview({ ...env, EXPECTED_BOT_ID: '149929' })).skipped, true);
    assert.equal((await approveTrustedReview({ ...env, PR_HUMAN_TOUCHED: 'true' })).skipped, true);
    assert.equal((await approveTrustedReview({ ...env, VERIFIED_PR_HEAD: 'c'.repeat(40) })).skipped, true);
    assert.equal(writes.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
    await rm(temp, { recursive: true, force: true });
  }
});

test('the immutable Git diff detects API omissions and inconsistent change types', () => {
  verifyGitChanges([{ status: 'A', path: files[0].filename }], files);
  assert.throws(
    () =>
      verifyGitChanges(
        [
          { status: 'A', path: files[0].filename },
          { status: 'M', path: '.github/workflows/ci.yml' },
        ],
        files
      ),
    /immutable Git diff/
  );
  assert.throws(() => verifyGitChanges([{ status: 'M', path: files[0].filename }], files), /immutable Git diff/);
  assert.throws(() => verifyGitChanges([{ status: 'A', path: files[0].filename }], [files[0], files[0]]), /Duplicate/);
});
test('rename validation compares both source and destination with immutable Git changes', () => {
  const renamed = [{ filename: 'data/new.yaml', previous_filename: 'data/old.yaml', status: 'renamed' }];
  verifyGitChanges(
    [
      { path: 'data/new.yaml', status: 'A' },
      { path: 'data/old.yaml', status: 'D' },
    ],
    renamed
  );
  assert.throws(() => verifyGitChanges([{ path: 'data/new.yaml', status: 'A' }], renamed), /immutable Git diff/);
});
