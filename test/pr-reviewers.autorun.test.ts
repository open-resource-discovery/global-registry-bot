import path from 'node:path';
import os from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';

function restoreEnvSnapshot(snapshot: NodeJS.ProcessEnv): void {
  for (const k of Object.keys(process.env)) {
    if (!(k in snapshot)) delete process.env[k];
  }
  for (const [k, v] of Object.entries(snapshot)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

describe('pr-reviewers auto-run guard', () => {
  const originalEnv: NodeJS.ProcessEnv = { ...process.env };
  const originalCwd: string = process.cwd();
  const originalExitCode: number | undefined = process.exitCode as number | undefined;

  let tmpDir = '';

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'registry-pr-reviewers-autorun-'));
    process.chdir(tmpDir);
    process.exitCode = undefined;
  });

  afterEach(async () => {
    restoreEnvSnapshot(originalEnv);
    process.chdir(originalCwd);
    process.exitCode = originalExitCode;

    if (tmpDir) await rm(tmpDir, { recursive: true, force: true });
  });

  it('runs main on import when JEST_WORKER_ID is unset and sets exitCode on failure', async () => {
    delete process.env.JEST_WORKER_ID;

    process.env.REGISTRY_VALIDATE_MODE = 'pr';
    process.env.GITHUB_EVENT_NAME = 'pull_request';
    process.env.PR_EVENT_ACTION = 'opened';
    process.env.PR_IS_FORK = 'false';
    process.env.PR_BASE_SHA = 'deadbeef';
    process.env.PR_HEAD_SHA = 'cafebabe';

    process.env.PR_AUTHOR = 'alice';
    process.env.PR_SENDER_LOGIN = 'alice';
    process.env.PR_SENDER_TYPE = 'User';
    process.env.PR_CREATOR_LOGIN = 'alice';
    process.env.PR_CREATOR_TYPE = 'User';
    process.env.PR_HUMAN_TOUCHED = 'false';

    await import('../src/ci/pr-reviewers.js');

    await new Promise((r) => setTimeout(r, 25));

    expect(process.exitCode).toBe(1);
  });
});
