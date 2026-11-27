import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { appendFile, lstat, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const exec = promisify(execFile);
const SHA = /^[a-f0-9]{40}$/;
export const LIMITS = Object.freeze({ files: 3000, bytes: 32 * 1024 * 1024, fileBytes: 4 * 1024 * 1024 });
export function allowedDataPath(value) {
  return (
    typeof value === 'string' &&
    /^data[/].+[.](?:json|ya?ml)$/.test(value) &&
    ![...value].some(
      (character) =>
        character.charCodeAt(0) < 32 ||
        character.charCodeAt(0) === 127 ||
        character === ':' ||
        character === String.fromCharCode(92)
    ) &&
    value === value.normalize('NFC') &&
    value
      .split('/')
      .every(
        (part) => part && part !== '.' && part !== '..' && !part.toLowerCase().startsWith('.git') && !/[. ]$/.test(part)
      )
  );
}
export function verifyReviewSnapshot(event, pr, repo, base, files) {
  if (
    repo.default_branch !== base.branch ||
    base.sha !== pr.base?.sha ||
    pr.base?.ref !== base.branch ||
    pr.base?.repo?.id !== repo.id ||
    pr.head?.repo?.id !== repo.id ||
    pr.number !== event.pull_request?.number ||
    pr.state !== 'open' ||
    pr.draft ||
    pr.head.sha !== event.pull_request.head.sha ||
    pr.user?.id !== event.pull_request.user?.id ||
    !SHA.test(pr.head.sha) ||
    !SHA.test(base.sha)
  )
    throw new Error('PR or protected base revision changed or is not permitted');
  if (
    !Array.isArray(files) ||
    !files.length ||
    files.length > LIMITS.files ||
    files.length !== pr.changed_files ||
    !files.every(
      (file) =>
        ['added', 'modified', 'removed', 'renamed'].includes(file.status) &&
        allowedDataPath(file.filename) &&
        (!file.previous_filename || allowedDataPath(file.previous_filename))
    )
  ) {
    throw new Error('Complete metadata-only file allowlist failed');
  }
  return { head: pr.head.sha, base: base.sha, number: pr.number, authorId: pr.user.id };
}
export function verifyTree(tree, files) {
  if (tree.truncated || !Array.isArray(tree.tree)) throw new Error('Incomplete PR Git tree');
  const entries = new Map(tree.tree.map((entry) => [entry.path, entry]));
  for (const file of files) {
    if (file.status === 'removed') continue;
    const entry = entries.get(file.filename);
    if (
      !entry ||
      entry.type !== 'blob' ||
      entry.mode !== '100644' ||
      !SHA.test(entry.sha) ||
      !Number.isSafeInteger(entry.size) ||
      entry.size < 0 ||
      entry.size > LIMITS.fileBytes
    )
      throw new Error('Metadata must be bounded regular files');
    const parts = file.filename.split('/');
    for (let i = 1; i < parts.length; i++) {
      const parent = entries.get(parts.slice(0, i).join('/'));
      if (!parent || parent.type !== 'tree' || parent.mode !== '040000')
        throw new Error('Metadata parent is not a directory');
    }
  }
  return entries;
}
export function verifyGitChanges(changes, files) {
  const expected = new Map();
  for (const file of files) {
    if (expected.has(file.filename)) throw new Error('Duplicate API changed file');
    expected.set(file.filename, file.status === 'removed' ? 'D' : file.status === 'modified' ? 'M' : 'A');
    if (file.previous_filename) {
      if (expected.has(file.previous_filename)) throw new Error('Conflicting rename source');
      expected.set(file.previous_filename, 'D');
    }
  }
  if (
    changes.length !== expected.size ||
    new Set(changes.map((change) => change.path)).size !== changes.length ||
    !changes.every(
      (change) =>
        allowedDataPath(change.path) &&
        (change.status === expected.get(change.path) || (change.status === 'T' && expected.get(change.path) === 'M'))
    )
  ) {
    throw new Error('API file list does not match the complete immutable Git diff');
  }
}
function client(env, fetchImpl = globalThis.fetch) {
  const api = new URL(env.GITHUB_API_URL);
  if (
    api.protocol !== 'https:' ||
    api.username ||
    api.password ||
    !/^[A-Za-z0-9_.-]+[/][A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY || '')
  )
    throw new Error('Invalid GitHub source');
  const root = api.href.replace(/[/]+$/, '') + '/repos/' + env.GITHUB_REPOSITORY;
  return async (resource, method = 'GET', body) => {
    const response = await fetchImpl(root + '/' + resource, {
      method,
      redirect: 'manual',
      headers: {
        'authorization': 'Bearer ' + env.GITHUB_TOKEN,
        'accept': 'application/vnd.github+json',
        'content-type': 'application/json',
      },
      signal: AbortSignal.timeout(60000),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (!response.ok) throw new Error('GitHub review request failed: HTTP ' + response.status);
    return response.json();
  };
}
async function snapshot(env, event, request) {
  const number = event.pull_request?.number;
  if (env.GITHUB_EVENT_NAME !== 'pull_request_target' || !Number.isSafeInteger(number) || number <= 0)
    throw new Error('Trusted review requires a base-defined PR target event');
  const repo = await request('');
  const branch = env.EXPECTED_BASE_BRANCH || 'main';
  const ref = await request('git/ref/heads/' + encodeURIComponent(branch));
  const pr = await request('pulls/' + number);
  const files = [];
  for (let page = 1; page <= 31; page++) {
    const batch = await request('pulls/' + number + '/files?per_page=100&page=' + page);
    if (!Array.isArray(batch)) throw new Error('Invalid changed files response');
    files.push(...batch);
    if (batch.length < 100) break;
  }
  const verified = verifyReviewSnapshot(event, pr, repo, { branch, sha: ref.object?.sha }, files);
  return { ...verified, files, pr };
}
async function safeDestination(root, name) {
  if (!allowedDataPath(name)) throw new Error('Unsafe metadata path');
  const parts = name.split('/');
  let current = root;
  for (let i = 0; i < parts.length - 1; i++) {
    current = path.join(current, parts[i]);
    try {
      if (!(await lstat(current)).isDirectory()) throw new Error('Metadata parent is not a regular directory');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      await mkdir(current);
    }
  }
  const target = path.join(current, parts.at(-1));
  try {
    if (!(await lstat(target)).isFile()) throw new Error('Metadata destination is not a regular file');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  return target;
}
export async function prepareTrustedReview(env = process.env) {
  const event = JSON.parse(await readFile(env.GITHUB_EVENT_PATH, 'utf8'));
  const request = client(env);
  const verified = await snapshot(env, event, request);
  const root = path.resolve(env.GITHUB_WORKSPACE);
  const { stdout } = await exec('git', ['rev-parse', 'HEAD'], { cwd: root });
  if (stdout.trim() !== verified.base) throw new Error('Only the protected base revision may be checked out');
  const tree = await request('git/trees/' + verified.head + '?recursive=1');
  const entries = verifyTree(tree, verified.files);
  // Git receives authentication through process environment, never token-bearing arguments.
  const server = new URL(env.GITHUB_SERVER_URL);
  if (server.protocol !== 'https:' || server.username || server.password) throw new Error('Invalid Git server');
  const gitEnv = {
    ...process.env,
    GIT_TERMINAL_PROMPT: '0',
    GIT_CONFIG_COUNT: '3',
    GIT_CONFIG_KEY_0: 'http.' + server.origin + '/.extraheader',
    GIT_CONFIG_VALUE_0: 'AUTHORIZATION: basic ' + Buffer.from('x-access-token:' + env.GITHUB_TOKEN).toString('base64'),
    GIT_CONFIG_KEY_1: 'core.hooksPath',
    GIT_CONFIG_VALUE_1: '/dev/null',
    GIT_CONFIG_KEY_2: 'credential.helper',
    GIT_CONFIG_VALUE_2: '',
  };
  try {
    await exec(
      'git',
      [
        'fetch',
        '--no-tags',
        '--no-recurse-submodules',
        server.origin + '/' + env.GITHUB_REPOSITORY + '.git',
        verified.head,
      ],
      { cwd: root, env: gitEnv, maxBuffer: 1024 * 1024 }
    );
  } catch {
    throw new Error('Could not fetch verified PR Git objects');
  }
  const { stdout: ancestor } = await exec('git', ['merge-base', verified.base, verified.head], { cwd: root });
  if (!SHA.test(ancestor.trim())) throw new Error('Missing protected PR merge base');
  const { stdout: diff } = await exec(
    'git',
    ['diff', '--name-status', '-z', '--no-renames', ancestor.trim(), verified.head],
    { cwd: root, maxBuffer: 5 * 1024 * 1024 }
  );
  const fields = diff.split(String.fromCharCode(0));
  if (fields.pop() !== '' || fields.length % 2) throw new Error('Invalid immutable Git diff');
  const changes = [];
  for (let index = 0; index < fields.length; index += 2)
    changes.push({ status: fields[index], path: fields[index + 1] });
  verifyGitChanges(changes, verified.files);
  const buffers = new Map();
  let bytes = 0;
  for (const file of verified.files) {
    if (file.status === 'removed') continue;
    const entry = entries.get(file.filename);
    const { stdout: blob } = await exec('git', ['cat-file', 'blob', entry.sha], {
      cwd: root,
      encoding: 'buffer',
      maxBuffer: LIMITS.fileBytes + 1,
    });
    bytes += blob.length;
    if (blob.length !== entry.size || bytes > LIMITS.bytes) throw new Error('Metadata exceeds byte limits');
    buffers.set(file.filename, blob);
  }
  const latest = await snapshot(env, event, request);
  if (latest.head !== verified.head || latest.base !== verified.base)
    throw new Error('PR changed during metadata preparation');
  verifyGitChanges(changes, latest.files);
  for (const file of verified.files) {
    for (const name of [file.status === 'removed' ? file.filename : null, file.previous_filename].filter(Boolean))
      await rm(await safeDestination(root, name), { force: true });
    if (file.status !== 'removed')
      await writeFile(await safeDestination(root, file.filename), buffers.get(file.filename), { mode: 0o644 });
  }
  if (env.GITHUB_OUTPUT)
    await appendFile(env.GITHUB_OUTPUT, 'head=' + verified.head + '\nbase=' + verified.base + '\n');
  return verified;
}
export async function approveTrustedReview(env = process.env) {
  const event = JSON.parse(await readFile(env.GITHUB_EVENT_PATH, 'utf8'));
  const request = client(env);
  const verified = await snapshot(env, event, request);
  const identity = Number(env.EXPECTED_BOT_ID);
  if (
    !Number.isSafeInteger(identity) ||
    identity <= 0 ||
    verified.head !== env.VERIFIED_PR_HEAD ||
    verified.base !== env.VERIFIED_PR_BASE ||
    verified.pr.user.id !== identity ||
    verified.pr.user.type !== 'Bot' ||
    event.sender?.id !== identity ||
    event.sender?.type !== 'Bot' ||
    env.PR_HUMAN_TOUCHED === 'true' ||
    String(verified.pr.body || '').includes('<!-- registry-bot:human-content-change -->') ||
    env.REGISTRY_VALIDATE_OK !== 'success' ||
    Number(env.VALIDATED_FILE_COUNT) !== verified.files.filter((file) => file.status !== 'removed').length ||
    Number(env.VALIDATED_FILE_COUNT) <= 0
  )
    return { skipped: true };
  const existing = await request('pulls/' + verified.number + '/requested_reviewers');
  if ((existing.teams || []).length || (existing.users || []).some((user) => user.id !== identity))
    return { skipped: true };
  await request('pulls/' + verified.number + '/reviews', 'POST', {
    event: 'APPROVE',
    commit_id: verified.head,
    body: 'Auto approved registry metadata after protected-base validation.',
  });
  return { approved: true };
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    if (process.argv[2] === 'prepare') await prepareTrustedReview();
    else if (process.argv[2] === 'approve') await approveTrustedReview();
    else throw new Error('Expected prepare or approve mode');
  } catch (error) {
    // eslint-disable-next-line no-console -- Report a sanitized CLI failure to the runner.
    console.error('::error::' + error.message.replace(/[\r\n]/g, ' '));
    process.exitCode = 1;
  }
}
