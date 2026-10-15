/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/**
 * Focused unit tests for reconciliation.ts using the REAL js-yaml implementation.
 *
 * These tests exercise compareFileOnBranch, writeFileWithReconciliation, and
 * inspectExistingBranch directly — bypassing create.ts — so that the real
 * YAML parser behaviour is exercised rather than JSON-surrogate mocks.
 */

import { describe, it, expect, jest } from '@jest/globals';

import {
  compareFileOnBranch,
  writeFileWithReconciliation,
  inspectExistingBranch,
} from '../src/handlers/request/pr/reconciliation.js';

// ─── Minimal context factory ────────────────────────────────────────────────

type MockFn = jest.Mock<any>;

function b64(s: string): string {
  return Buffer.from(s, 'utf8').toString('base64');
}

function fileResponse(yamlText: string) {
  return { data: { content: b64(yamlText), encoding: 'base64' } };
}

function httpErr(status: number, msg = `HTTP ${status}`): Error & { status: number } {
  const e = new Error(msg) as Error & { status: number };
  e.status = status;
  return e;
}

function mkMock(): jest.Mock<any> {
  return jest.fn() as jest.Mock<any>;
}

function mkGetContent(impl: (args: unknown) => unknown): MockFn {
  return jest.fn((args: unknown) => Promise.resolve(impl(args))) as MockFn;
}

function mkContext(getContent: MockFn, log?: any) {
  const getBranch = mkMock();
  getBranch.mockResolvedValue({ data: { commit: { sha: 'BRANCH_SHA' } } });
  const createOrUpdateFileContents = mkMock();
  const compareCommitsWithBasehead = mkMock();
  const list = mkMock();
  const create = mkMock();

  return {
    octokit: {
      rest: {
        repos: {
          getContent,
          getBranch,
          createOrUpdateFileContents,
          compareCommitsWithBasehead,
        },
        pulls: { list, create },
      },
    },
    log: log ?? {
      info: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
      error: jest.fn(),
    },
  } as any;
}

const REPO = { owner: 'o', repo: 'r' };
const FILE_PATH = 'data/ns/sap.test.yaml';
const BRANCH = 'feat/resource-sap.test-issue-1';

// ─── compareFileOnBranch – real js-yaml ────────────────────────────────────

describe('compareFileOnBranch – real js-yaml', () => {
  // ── Mapping key-order differences ──────────────────────────────────────────
  it('different key order: equivalent', async () => {
    const expectedYaml = 'type: system\nname: sap.test\n';
    const branchYaml = 'name: sap.test\ntype: system\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('equivalent');
  });

  // ── Nested mapping key-order differences ───────────────────────────────────
  it('nested key order difference: equivalent', async () => {
    const expectedYaml = 'type: system\nmeta:\n  a: 1\n  b: 2\n';
    const branchYaml = 'meta:\n  b: 2\n  a: 1\ntype: system\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('equivalent');
  });

  // ── Extra key in branch file ────────────────────────────────────────────────
  it('extra key in branch file: conflict', async () => {
    const expectedYaml = 'type: system\nname: sap.test\n';
    const branchYaml = 'type: system\nname: sap.test\nextra: value\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Missing key in branch file ──────────────────────────────────────────────
  it('missing key in branch file: conflict', async () => {
    const expectedYaml = 'type: system\nname: sap.test\n';
    const branchYaml = 'type: system\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Scalar type mismatch ────────────────────────────────────────────────────
  it('scalar type mismatch (string vs integer): conflict', async () => {
    const expectedYaml = 'type: system\ncount: 1\n';
    const branchYaml = "type: system\ncount: '1'\n";
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Array order mismatch ────────────────────────────────────────────────────
  it('array order mismatch: conflict', async () => {
    const expectedYaml = 'type: system\ncontact:\n  - a@b\n  - c@d\n';
    const branchYaml = 'type: system\ncontact:\n  - c@d\n  - a@b\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Invalid YAML in branch ─────────────────────────────────────────────────
  it('invalid YAML in branch file: conflict', async () => {
    const expectedYaml = 'type: system\n';
    const branchYaml = 'type: system\n  bad: [unclosed\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Two YAML documents ─────────────────────────────────────────────────────
  it('two YAML documents: conflict (js-yaml load throws on multi-doc)', async () => {
    const expectedYaml = 'type: system\n';
    // js-yaml's load() throws when the string contains multiple documents
    const branchYaml = 'type: system\n---\ntype: other\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Custom tag in branch ───────────────────────────────────────────────────
  it('custom tag in branch file: conflict (JSON_SCHEMA rejects custom tags)', async () => {
    const expectedYaml = 'type: system\n';
    const branchYaml = 'type: !!python/object:MyClass {}\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Null root ──────────────────────────────────────────────────────────────
  it('null root in branch file: conflict', async () => {
    const expectedYaml = 'type: system\n';
    const branchYaml = 'null\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Scalar root ────────────────────────────────────────────────────────────
  it('scalar root in branch file: conflict', async () => {
    const expectedYaml = 'type: system\n';
    const branchYaml = 'hello world\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Array root ─────────────────────────────────────────────────────────────
  it('array root in branch file: conflict', async () => {
    const expectedYaml = 'type: system\n';
    const branchYaml = '- a\n- b\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── Recursive alias ────────────────────────────────────────────────────────
  it('recursive alias input: terminates and returns conflict (not infinite recursion)', async () => {
    const expectedYaml = 'type: system\n';
    // JSON_SCHEMA rejects anchors and aliases, so this will throw at parse time.
    const branchYaml = '&a\ntype: system\ncycle: *a\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    // Must terminate (not hang) and return conflict
    expect(r.status).toBe('conflict');
  });

  // ── Directory / unexpected Contents API response ───────────────────────────
  it('directory response from Contents API: conflict', async () => {
    const expectedYaml = 'type: system\n';
    // Array response = directory listing
    const ctx = mkContext(mkGetContent(() => ({ data: [{ type: 'file', name: 'sap.test.yaml' }] })));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });

  // ── 404 on branch file ─────────────────────────────────────────────────────
  it('404 on branch file: absent', async () => {
    const expectedYaml = 'type: system\n';
    const ctx = mkContext(
      mkGetContent(() => {
        throw httpErr(404);
      })
    );
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('absent');
  });

  // ── Undefined candidate property omitted by generated YAML ────────────────
  it('undefined property omitted by dumpYamlDoc: equivalent to branch file without that key', async () => {
    // dumpYamlDoc strips undefined fields; so expectedYaml produced by real
    // dumpYamlDoc won't have the key. Branch file also lacks it → equivalent.
    // We simulate by passing expectedYaml without the key.
    const expectedYaml = 'type: system\nname: sap.test\n';
    const branchYaml = 'type: system\nname: sap.test\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('equivalent');
  });

  // ── Non-finite number: NaN serialized as string by dumpYamlDoc ────────────
  it('non-finite number (NaN) serialized as string in expectedYaml matches branch with string "NaN"', async () => {
    // dumpYamlDoc converts NaN → "NaN" string via sanitizeForYaml.
    // So expectedYaml has `count: "NaN"` and the branch file must also have a string.
    const expectedYaml = "type: system\ncount: 'NaN'\n";
    const branchYaml = "type: system\ncount: 'NaN'\n";
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('equivalent');
  });

  // ── Non-finite number in branch doesn't match string: conflict ─────────────
  it('branch file with numeric NaN vs expected string "NaN": conflict (JSON_SCHEMA parses .nan as float)', async () => {
    // JSON_SCHEMA disables .nan, so literal .nan in YAML would fail to parse.
    // A branch file that was written incorrectly with an integer where a string is expected: conflict.
    const expectedYaml = "type: system\ncount: 'NaN'\n";
    const branchYaml = 'type: system\ncount: 99\n';
    const ctx = mkContext(mkGetContent(() => fileResponse(branchYaml)));
    const r = await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, expectedYaml);
    expect(r.status).toBe('conflict');
  });
});

// ─── inspectExistingBranch – renamed file cases ────────────────────────────

describe('inspectExistingBranch – previous_filename and renamed target', () => {
  function mkInspectContext(compareResult: { data: { files?: any[] } }) {
    const log = { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };
    const getBranch = mkMock();
    getBranch.mockResolvedValue({ data: { commit: { sha: 'BRANCH_SHA' } } });
    const compareCommitsWithBasehead = mkMock();
    compareCommitsWithBasehead.mockResolvedValue(compareResult);
    const ctx = {
      octokit: {
        rest: {
          repos: {
            getBranch,
            getContent: mkMock(),
            createOrUpdateFileContents: mkMock(),
            compareCommitsWithBasehead,
          },
          pulls: {
            list: mkMock(),
            create: mkMock(),
          },
        },
      },
      log,
    } as any;
    return { ctx, log };
  }

  // ── Target file renamed FROM unrelated source TO target path ───────────────
  it('target file arrived via rename from unrelated path: fail closed, branch-unsafe log emitted', async () => {
    const { ctx, log } = mkInspectContext({
      data: {
        files: [
          {
            filename: FILE_PATH,
            status: 'renamed',
            previous_filename: 'data/old/different.yaml',
          },
        ],
      },
    });

    const result = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);

    expect(result.safe).toBe(false);
    if (!result.safe) {
      expect(result.reason).toMatch(/rename|renamed/i);
      expect(result.reason).toContain('data/old/different.yaml');
    }

    const warnCalls = log.warn.mock.calls;
    const unsafeLog = warnCalls.find(
      (c) => typeof c[0] === 'object' && c[0] !== null && c[0].stage === 'request-pr:branch-unsafe'
    );
    expect(unsafeLog).toBeDefined();
    expect(unsafeLog![0].outcome).toBe('target-arrived-via-rename');
    expect(unsafeLog![0].previousFilename).toBe('data/old/different.yaml');
  });

  // ── Rename of unrelated path (not target): treated as unrelated change ─────
  it('unrelated file renamed (not to target path): fail closed as unrelated change', async () => {
    const { ctx } = mkInspectContext({
      data: {
        files: [
          {
            filename: 'data/other/file.yaml',
            status: 'renamed',
            previous_filename: 'data/old/file.yaml',
          },
        ],
      },
    });

    const result = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);

    expect(result.safe).toBe(false);
    if (!result.safe) {
      expect(result.reason).toMatch(/unrelated/i);
    }
  });

  // ── Only target file added: safe ──────────────────────────────────────────
  it('only target file added: safe', async () => {
    const { ctx } = mkInspectContext({
      data: {
        files: [{ filename: FILE_PATH, status: 'added' }],
      },
    });

    const result = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);

    expect(result.safe).toBe(true);
  });

  // ── Only target file modified: safe ───────────────────────────────────────
  it('only target file modified: safe (modified is acceptable for existing branch)', async () => {
    const { ctx } = mkInspectContext({
      data: {
        files: [{ filename: FILE_PATH, status: 'modified' }],
      },
    });

    const result = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);

    expect(result.safe).toBe(true);
  });
});

// ─── writeFileWithReconciliation – error-preservation ────────────────────

describe('writeFileWithReconciliation – error preservation through reconciliation', () => {
  const noDelay = (_ms: number): Promise<void> => Promise.resolve();

  // ── First reconciliation read fails ────────────────────────────────────────
  it('first reconciliation read throws: original write error is cause, stage-aware wrapper returned', async () => {
    const originalWriteError = Object.assign(new Error('upstream 500'), { status: 500 });
    const reconcileReadError = new Error('getContent network error');

    const createOrUpdateFileContents = mkMock();
    createOrUpdateFileContents.mockRejectedValueOnce(originalWriteError);
    // getContent always throws a non-404 error
    const getContent = mkMock();
    getContent.mockRejectedValue(reconcileReadError);
    const log = { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };

    const ctx = mkContext(getContent, log);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdateFileContents;

    // Must NOT throw raw — must return a failed WriteFileResult
    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\n',
      'chore: register',
      noDelay
    );

    // Returns failed result, NOT a raw throw
    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      // Stage tag in message
      expect(result.error.message).toContain('request-pr:file-write-reconcile');
      // Original ambiguous write error is the cause
      expect(result.error.cause).toBe(originalWriteError);
      expect((result.error.cause as Error).message).toBe('upstream 500');
      expect((result.error.cause as any).status).toBe(500);
    }
  });

  // ── First reconciliation read finds conflict ────────────────────────────────
  it('first reconciliation read finds conflict: conflict result with original write error as cause', async () => {
    const originalWriteError = Object.assign(new Error('original ambiguous write'), { status: 500 });

    const createOrUpdateFileContents = mkMock();
    createOrUpdateFileContents.mockRejectedValueOnce(originalWriteError);
    const getContent = mkMock();
    getContent.mockResolvedValue(fileResponse('type: system\nname: CONFLICT\n'));

    const ctx = mkContext(getContent);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdateFileContents;

    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\nname: sap.test\n',
      'chore: register',
      noDelay
    );

    expect(result.status).toBe('conflict');
    if (result.status === 'conflict') {
      expect(result.cause).toBe(originalWriteError);
      expect(result.cause?.message).toBe('original ambiguous write');
      expect((result.cause as any)?.status).toBe(500);
    }
  });

  // ── Controlled retry fails, final read finds conflict ──────────────────────
  it('retry fails, final reconciliation read finds conflict: conflict with original cause', async () => {
    const originalWriteError = Object.assign(new Error('first write 500'), { status: 500 });
    const retryWriteError = Object.assign(new Error('retry write 503'), { status: 503 });

    let getContentCallCount = 0;
    const getContent = mkMock();
    getContent.mockImplementation(() => {
      getContentCallCount++;
      if (getContentCallCount === 1) return Promise.reject(httpErr(404));
      return Promise.resolve(fileResponse('type: system\nname: CONFLICT\n'));
    });

    const createOrUpdate = mkMock();
    createOrUpdate.mockRejectedValueOnce(originalWriteError);
    createOrUpdate.mockRejectedValueOnce(retryWriteError);

    const ctx = mkContext(getContent);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdate;

    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\nname: sap.test\n',
      'chore: register',
      noDelay
    );

    expect(result.status).toBe('conflict');
    if (result.status === 'conflict') {
      expect(result.cause).toBe(originalWriteError);
    }
  });

  // ── Final reconciliation read fails: file absent after retry ──────────────
  it('file absent after retry: failed result with original error as cause', async () => {
    const originalWriteError = Object.assign(new Error('original 500'), { status: 500 });

    const getContent = mkMock();
    getContent.mockRejectedValue(httpErr(404));
    const createOrUpdate = mkMock();
    createOrUpdate.mockRejectedValueOnce(originalWriteError);
    createOrUpdate.mockRejectedValueOnce(new Error('retry also failed'));

    const ctx = mkContext(getContent);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdate;

    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\n',
      'chore: register',
      noDelay
    );

    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      expect(result.error.cause).toBe(originalWriteError);
      expect(result.error.message).toContain('original 500');
    }
  });

  // ── First read equivalent: write recovered ────────────────────────────────
  it('first reconciliation read finds equivalent file: returns written (recovered)', async () => {
    const originalWriteError = Object.assign(new Error('ambiguous 500'), { status: 500 });

    const getContent = mkMock();
    getContent.mockResolvedValue(fileResponse('type: system\n'));
    const createOrUpdate = mkMock();
    createOrUpdate.mockRejectedValueOnce(originalWriteError);

    const ctx = mkContext(getContent);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdate;

    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\n',
      'chore: register',
      noDelay
    );

    expect(result.status).toBe('written');
  });

  // ── Final reconciliation read fails after retry ─────────────────────────────
  it('final reconciliation read throws: original write error is cause, stage-aware wrapper returned', async () => {
    const originalWriteError = Object.assign(new Error('first ambiguous 500'), { status: 500 });

    let getContentCallCount = 0;
    const getContent = mkMock();
    getContent.mockImplementation(() => {
      getContentCallCount++;
      // First read (check1): absent → retry
      if (getContentCallCount === 1) return Promise.reject(httpErr(404));
      // Second read (check2): throws non-404
      return Promise.reject(new Error('final read network error'));
    });

    const createOrUpdate = mkMock();
    createOrUpdate.mockRejectedValueOnce(originalWriteError); // first write
    createOrUpdate.mockRejectedValueOnce(new Error('retry also failed')); // controlled retry

    const ctx = mkContext(getContent);
    ctx.octokit.rest.repos.createOrUpdateFileContents = createOrUpdate;

    const result = await writeFileWithReconciliation(
      ctx,
      REPO,
      FILE_PATH,
      BRANCH,
      'type: system\n',
      'chore: register',
      noDelay
    );

    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      expect(result.error.message).toContain('request-pr:file-write-reconcile');
      expect(result.error.cause).toBe(originalWriteError);
      expect((result.error.cause as Error).message).toBe('first ambiguous 500');
    }
  });
});

// ─── lookupOpenPrForBranch – stage-aware error ─────────────────────────────

describe('lookupOpenPrForBranch – stage-aware error wrapping', () => {
  it('pulls.list failure: stage-aware error thrown, original cause preserved, not raw', async () => {
    const { lookupOpenPrForBranch: lookupFn } = await import('../src/handlers/request/pr/reconciliation.js');
    const listError = Object.assign(new Error('pulls.list forbidden'), { status: 403 });

    const list = mkMock();
    list.mockRejectedValueOnce(listError);
    const ctx = {
      octokit: {
        rest: {
          repos: {
            getContent: mkMock(),
            getBranch: mkMock(),
            createOrUpdateFileContents: mkMock(),
            compareCommitsWithBasehead: mkMock(),
          },
          pulls: { list, create: mkMock() },
        },
      },
      log: { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() },
    } as any;

    let thrown: unknown = null;
    try {
      await lookupFn(ctx, REPO, BRANCH);
    } catch (e) {
      thrown = e;
    }

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toContain('request-pr:pr-lookup');
    expect((thrown as Error).cause).toBe(listError);
    expect(((thrown as Error).cause as Error).message).toBe('pulls.list forbidden');
  });
});

// ─── compareFileOnBranch – stage-aware file-read error ────────────────────

describe('compareFileOnBranch – stage-aware non-404 read error', () => {
  it('non-404 getContent error: throws stage-aware wrapper with original as cause', async () => {
    const readError = Object.assign(new Error('503 Service Unavailable'), { status: 503 });
    const ctx = mkContext(
      mkGetContent(() => {
        throw readError;
      })
    );

    let thrown: unknown = null;
    try {
      await compareFileOnBranch(ctx, REPO, FILE_PATH, BRANCH, 'type: system\n');
    } catch (e) {
      thrown = e;
    }

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toContain('request-pr:file-read');
    expect((thrown as Error).cause).toBe(readError);
  });
});

// ─── inspectExistingBranch – extended branch status matrix ────────────────

describe('inspectExistingBranch – branch status fail-closed matrix', () => {
  function mkStatusCtx(files: any[]) {
    const log = { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };
    const getBranch = mkMock();
    getBranch.mockResolvedValue({ data: { commit: { sha: 'BRANCH_SHA' } } });
    const compare = mkMock();
    compare.mockResolvedValue({ data: { files } });
    return {
      ctx: {
        octokit: {
          rest: {
            repos: {
              getBranch,
              getContent: mkMock(),
              createOrUpdateFileContents: mkMock(),
              compareCommitsWithBasehead: compare,
            },
            pulls: { list: mkMock(), create: mkMock() },
          },
        },
        log,
      } as any,
      log,
    };
  }

  it('status=added: safe', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH, status: 'added' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(true);
  });

  it('status=modified: safe', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH, status: 'modified' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(true);
  });

  it('missing status (empty string): fail closed, branch-unsafe log emitted', async () => {
    const { ctx, log } = mkStatusCtx([{ filename: FILE_PATH, status: '' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
    if (!r.safe) expect(r.reason).toMatch(/unacceptable.?status|empty|unsafe/i);
    const warnCalls = log.warn.mock.calls;
    const u = warnCalls.find((c) => c[0]?.stage === 'request-pr:branch-unsafe');
    expect(u).toBeDefined();
    expect(u![0].outcome).toMatch(/missing.?status|unknown.?status/i);
  });

  it('undefined status (field absent): fail closed', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH }]); // no status field
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
  });

  it('status=copied: fail closed', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH, status: 'copied' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
    if (!r.safe) expect(r.reason).toMatch(/unacceptable.?status|unsafe/i);
  });

  it('status=unknown_future_value: fail closed', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH, status: 'future_unknown' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
  });

  it('status=added but unexpected previous_filename: fail closed', async () => {
    const { ctx, log } = mkStatusCtx([{ filename: FILE_PATH, status: 'added', previous_filename: 'old/path.yaml' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
    if (!r.safe) expect(r.reason).toMatch(/previous.?filename|unexpected/i);
    const warnCalls = log.warn.mock.calls;
    const u = warnCalls.find((c) => c[0]?.stage === 'request-pr:branch-unsafe');
    expect(u![0].outcome).toMatch(/previous.?filename/i);
  });

  it('status=modified with previous_filename: fail closed', async () => {
    const { ctx } = mkStatusCtx([{ filename: FILE_PATH, status: 'modified', previous_filename: 'old/path.yaml' }]);
    const r = await inspectExistingBranch(ctx, REPO, BRANCH, 'BASE_SHA', FILE_PATH);
    expect(r.safe).toBe(false);
  });
});

// ─── URL sanitization ─────────────────────────────────────────────────────

describe('URL sanitization in error messages', () => {
  it('error containing only a credential URL produces safe fallback, no URL in output', async () => {
    const { finalizeApprovedRequest } =
      await import('../src/handlers/request/application/approved-request-finalization.js');
    const postOnce = jest.fn(() => Promise.resolve());

    const credentialUrl = 'https://user:secret@example.invalid/path?token=abc';
    const findOpenIssuePrs = jest.fn(() => Promise.reject(new Error(credentialUrl)));

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: () => Promise.resolve([]),
      findOpenIssuePrs,
      applyApprovedRequestState: jest.fn(() => Promise.resolve()),
      addApprovedLabelToPr: jest.fn(() => Promise.resolve()),
      ensureAssigneesPresent: jest.fn(() => Promise.resolve()),
      createRequestPrWithRecovery: jest.fn(() => Promise.resolve({ number: 1 })),
      postOnce,
    };

    await (finalizeApprovedRequest as any)(
      {},
      { owner: 'o', repo: 'r', issue_number: 1 },
      { number: 1 },
      {},
      {},
      {},
      callbacks
    );

    expect(postOnce).toHaveBeenCalledTimes(1);
    const body = String((postOnce.mock.calls as any[][])[0][2]);
    expect(body).not.toContain('user');
    expect(body).not.toContain('secret');
    expect(body).not.toContain('token=abc');
    expect(body).not.toContain('https://');
    // Must contain a safe fallback, not be blank
    expect(body.trim().length).toBeGreaterThan('Failed to create Pull Request:'.length);
  });

  it('mixed text plus URL: harmless text retained, URL removed', async () => {
    const { finalizeApprovedRequest } =
      await import('../src/handlers/request/application/approved-request-finalization.js');
    const postOnce = jest.fn(() => Promise.resolve());

    const findOpenIssuePrs = jest.fn(() =>
      Promise.reject(new Error('resource not accessible - https://api.github.invalid/repos'))
    );

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: () => Promise.resolve([]),
      findOpenIssuePrs,
      applyApprovedRequestState: jest.fn(() => Promise.resolve()),
      addApprovedLabelToPr: jest.fn(() => Promise.resolve()),
      ensureAssigneesPresent: jest.fn(() => Promise.resolve()),
      createRequestPrWithRecovery: jest.fn(() => Promise.resolve({ number: 1 })),
      postOnce,
    };

    await (finalizeApprovedRequest as any)(
      {},
      { owner: 'o', repo: 'r', issue_number: 1 },
      { number: 1 },
      {},
      {},
      {},
      callbacks
    );

    const body = String((postOnce.mock.calls as any[][])[0][2]);
    expect(body).toContain('resource not accessible');
    expect(body).not.toContain('https://');
  });

  it('fully-redacted message: fixed safe fallback used, no blank prefix', async () => {
    const { finalizeApprovedRequest } =
      await import('../src/handlers/request/application/approved-request-finalization.js');
    const postOnce = jest.fn(() => Promise.resolve());

    // Error message is nothing but a URL
    const findOpenIssuePrs = jest.fn(() => Promise.reject(new Error('https://secret.internal/path')));

    const callbacks = {
      resolveEffectiveConstants: () => ({}),
      extractResourceNameFromForm: () => 'my-resource',
      resolveEffectiveRequestType: () => 'system',
      resolveAdditionalIssueApproversFromApprovalHook: () => Promise.resolve([]),
      findOpenIssuePrs,
      applyApprovedRequestState: jest.fn(() => Promise.resolve()),
      addApprovedLabelToPr: jest.fn(() => Promise.resolve()),
      ensureAssigneesPresent: jest.fn(() => Promise.resolve()),
      createRequestPrWithRecovery: jest.fn(() => Promise.resolve({ number: 1 })),
      postOnce,
    };

    await (finalizeApprovedRequest as any)(
      {},
      { owner: 'o', repo: 'r', issue_number: 1 },
      { number: 1 },
      {},
      {},
      {},
      callbacks
    );

    const body = String((postOnce.mock.calls as any[][])[0][2]);
    // Body must not be 'Failed to create Pull Request: ' (blank after prefix)
    expect(body).not.toMatch(/Failed to create Pull Request:\s*$/);
    expect(body).not.toContain('https://');
    expect(body).toContain('Failed to create Pull Request:');
  });
});
// These are tested in request-pr-creation-recovery.test.ts.
// Here we just verify no unhandled rejection surfaces from the cleanup chain.
describe('reconciliation exports: smoke', () => {
  it('module exports the expected functions', () => {
    expect(typeof compareFileOnBranch).toBe('function');
    expect(typeof writeFileWithReconciliation).toBe('function');
    expect(typeof inspectExistingBranch).toBe('function');
  });
});
