import * as jsYamlModule from 'js-yaml';

// ─── Type surface ──────────────────────────────────────────────────────────────

type JsonObject = Record<string, unknown>;

type RepoRef = { owner: string; repo: string };

type RepoContentFile = { content: string; encoding?: string; sha?: string };
type RepoContentResponse = RepoContentFile | RepoContentFile[] | unknown;

type PullRequestLike = {
  number: number;
  node_id: string;
  body?: string | null;
  draft?: boolean;
  state?: string;
  head: { ref: string; sha: string };
};

type CompareCommitsResponse = {
  files?: { filename: string; status?: string; previous_filename?: string }[];
};

type BranchGetResponse = { commit?: { sha?: string } };

type OctokitForReconciliation = {
  rest: {
    repos: {
      getBranch: (args: { owner: string; repo: string; branch: string }) => Promise<{ data: BranchGetResponse }>;
      getContent: (args: {
        owner: string;
        repo: string;
        path: string;
        ref?: string;
      }) => Promise<{ data: RepoContentResponse }>;
      createOrUpdateFileContents: (args: {
        owner: string;
        repo: string;
        path: string;
        message: string;
        content: string;
        branch: string;
        sha?: string;
      }) => Promise<unknown>;
      compareCommitsWithBasehead?: (args: {
        owner: string;
        repo: string;
        basehead: string;
      }) => Promise<{ data: CompareCommitsResponse }>;
    };
    pulls: {
      list: (args: {
        owner: string;
        repo: string;
        state?: 'open' | 'closed' | 'all';
        head?: string;
      }) => Promise<{ data: PullRequestLike[] }>;
      create: (args: {
        owner: string;
        repo: string;
        title: string;
        head: string;
        base: string;
        body?: string;
        maintainer_can_modify?: boolean;
      }) => Promise<{ data: PullRequestLike }>;
    };
  };
};

type LoggerLike = {
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  debug?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
};

type ContextForReconciliation = {
  octokit: OctokitForReconciliation;
  log?: LoggerLike;
};

// ─── Internal helpers ──────────────────────────────────────────────────────────

function isPlainObject(v: unknown): v is JsonObject {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const s = err['status'];
  return typeof s === 'number' ? s : undefined;
}

function getGitHubErrorMessage(err: unknown): string {
  if (!isPlainObject(err)) return String(err);
  const msg = err['message'];
  if (typeof msg === 'string') return msg;
  return String(err);
}

function isRepoContentFile(data: unknown): data is RepoContentFile {
  return isPlainObject(data) && typeof data.content === 'string';
}

/** Detect whether a write failure is potentially ambiguous (write may have applied). */
export function isAmbiguousWriteError(err: unknown): boolean {
  const status = getHttpStatus(err);

  // 500, 502, 503, 504: server-side failure — write may have applied.
  if (status === 500 || status === 502 || status === 503 || status === 504) return true;

  // 422 specifically when GitHub says "sha wasn't supplied" — the file already exists on
  // the branch (write applied) but the caller didn't provide the blob SHA for an update.
  if (status === 422) {
    const msg = getGitHubErrorMessage(err).toLowerCase();
    if (msg.includes('sha') && msg.includes('supplied')) return true;
    return false; // other 422 validation failures are NOT ambiguous
  }

  // Network-level aborts: timeout-like, ECONNRESET, etc.
  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'ECONNABORTED') return true;
    const msg = err.message.toLowerCase();
    if (msg.includes('econnreset') || msg.includes('socket hang up') || msg.includes('timeout')) return true;
  }

  return false;
}

/** Detect whether a PR-create 422 means a PR already exists. */
function isPrAlreadyExistsError(err: unknown): boolean {
  if (getHttpStatus(err) !== 422) return false;
  const msg = getGitHubErrorMessage(err).toLowerCase();
  return (
    msg.includes('already exists') ||
    msg.includes('pull request already exists') ||
    // GitHub Enterprise: "A pull request already exists for <owner>:<branch>"
    msg.includes('a pull request already exists')
  );
}

/** Detect whether a pr-create error is ambiguous (write may have created the PR). */
function isAmbiguousPrCreateError(err: unknown): boolean {
  const status = getHttpStatus(err);
  if (status === 500 || status === 502 || status === 503 || status === 504) return true;
  if (isPrAlreadyExistsError(err)) return true;

  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'ECONNABORTED') return true;
    const msg = err.message.toLowerCase();
    if (msg.includes('econnreset') || msg.includes('socket hang up') || msg.includes('timeout')) return true;
  }

  return false;
}

// ─── Semantic YAML equality ─────────────────────────────────────────────────

type JsYamlApi = {
  load: (src: string, opts?: Record<string, unknown>) => unknown;
  dump: (obj: unknown, opts?: Record<string, unknown>) => string;
  JSON_SCHEMA?: unknown;
};

// js-yaml v5 is ESM-only with no default export; Jest's CJS-style mocks may supply one.
const jsYaml = ((jsYamlModule as { default?: unknown }).default ?? jsYamlModule) as JsYamlApi;

function parseYamlSafe(src: string): { ok: true; value: unknown } | { ok: false; reason: string } {
  try {
    const schema = jsYaml.JSON_SCHEMA;
    // Require JSON_SCHEMA to prevent YAML-specific types (!!js/regexp, merge keys, etc.)
    // from being accepted when parsing untrusted branch file content.
    if (!schema) {
      return { ok: false, reason: 'js-yaml JSON_SCHEMA not available; refusing unsafe YAML parse.' };
    }
    const parseOpts: Record<string, unknown> = { schema };
    const parsed = jsYaml.load(src, parseOpts);
    return { ok: true, value: parsed };
  } catch (e: unknown) {
    return { ok: false, reason: e instanceof Error ? e.message : String(e) };
  }
}

/**
 * Deep-equality for JSON-compatible values.
 * - Mapping key order: ignored
 * - Array order: significant
 * - Exact key sets required (no missing, no extra keys)
 * - Cycle-safe: cyclic objects are treated as non-equal (comparison cannot complete)
 */
function deepEqual(a: unknown, b: unknown, seen = new WeakSet<object>()): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (seen.has(a) || seen.has(b)) return false;
    seen.add(a);
    seen.add(b);
    if (a.length !== b.length) {
      seen.delete(a);
      seen.delete(b);
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i], seen)) {
        seen.delete(a);
        seen.delete(b);
        return false;
      }
    }
    seen.delete(a);
    seen.delete(b);
    return true;
  }

  if (Array.isArray(a) || Array.isArray(b)) return false;

  if (isPlainObject(a) && isPlainObject(b)) {
    if (seen.has(a) || seen.has(b)) return false;
    seen.add(a);
    seen.add(b);
    const keysA = Object.keys(a).sort();
    const keysB = Object.keys(b).sort();
    if (keysA.length !== keysB.length) {
      seen.delete(a);
      seen.delete(b);
      return false;
    }
    if (keysA.some((k, i) => k !== keysB[i])) {
      seen.delete(a);
      seen.delete(b);
      return false;
    }
    for (const k of keysA) {
      if (!deepEqual(a[k], b[k], seen)) {
        seen.delete(a);
        seen.delete(b);
        return false;
      }
    }
    seen.delete(a);
    seen.delete(b);
    return true;
  }

  return false;
}

export type FileComparisonResult =
  | { status: 'absent' }
  | { status: 'equivalent' }
  | { status: 'conflict'; reason: string }
  | { status: 'unreadable'; reason: string };

/**
 * Compare an already-generated YAML text against the existing file on a branch.
 *
 * Both the existing branch file and expectedYamlText are parsed via the same safe schema,
 * then compared with deepEqual. This ensures the comparison reflects exactly what was written.
 */
export async function compareFileOnBranch(
  context: ContextForReconciliation,
  repoRef: RepoRef,
  filePath: string,
  branchName: string,
  expectedYamlText: string
): Promise<FileComparisonResult> {
  const { owner, repo } = repoRef;

  let raw: string;
  try {
    const res = await context.octokit.rest.repos.getContent({
      owner,
      repo,
      path: filePath,
      ref: branchName,
    });
    const data = res.data;
    if (!isRepoContentFile(data)) {
      return { status: 'conflict', reason: 'Branch file is a directory or unexpected response shape.' };
    }
    raw = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
  } catch (e: unknown) {
    if (getHttpStatus(e) === 404) return { status: 'absent' };
    throw new Error(`[request-pr:file-read] Branch file read failed: ${getGitHubErrorMessage(e)}`, {
      cause: e instanceof Error ? e : undefined,
    });
  }

  const parsed = parseYamlSafe(raw);
  if (!parsed.ok) {
    return { status: 'conflict', reason: `Existing branch file is not valid YAML: ${parsed.reason}` };
  }

  const existing = parsed.value;

  // Reject multi-document, non-object, or null shapes.
  if (!isPlainObject(existing)) {
    return { status: 'conflict', reason: 'Existing branch file does not parse to a plain object.' };
  }

  // Parse the expected YAML text (already generated by dumpYamlDoc, with all sanitization applied).
  const expectedParsed = parseYamlSafe(expectedYamlText);
  if (!expectedParsed.ok) {
    return { status: 'conflict', reason: `Expected YAML text is not valid: ${expectedParsed.reason}` };
  }

  const expected = expectedParsed.value;
  if (!isPlainObject(expected)) {
    return { status: 'conflict', reason: 'Expected YAML text does not parse to a plain object.' };
  }

  if (!deepEqual(existing, expected)) {
    return {
      status: 'conflict',
      reason: 'Existing branch file has different content from the generated candidate.',
    };
  }

  return { status: 'equivalent' };
}

// ─── Branch safety ─────────────────────────────────────────────────────────────

export type BranchSafetyResult =
  | { safe: true; branchExisted: boolean; headSha: string }
  | { safe: false; reason: string };

/**
 * After a createRef 422, verify the branch actually exists and is safe to reuse.
 *
 * A branch is safe to reuse when every changed file (compared against baseSha) is
 * limited to exactly the expected target file — or when the branch head IS baseSha
 * (empty branch, no divergence).
 *
 * Fails closed on any ambiguity: missing files array, empty diff with diverged SHA,
 * file status removed/renamed, unrelated changed paths, API errors.
 */
export async function inspectExistingBranch(
  context: ContextForReconciliation,
  repoRef: RepoRef,
  branchName: string,
  baseSha: string,
  targetFilePath: string
): Promise<BranchSafetyResult> {
  const { owner, repo } = repoRef;

  // Confirm the branch actually exists and get its head SHA.
  let headSha: string;
  try {
    const res = await context.octokit.rest.repos.getBranch({ owner, repo, branch: branchName });
    headSha = String(res.data?.commit?.sha || '').trim();
    if (!headSha) {
      return { safe: false, reason: `Branch '${branchName}' exists but has no commit SHA.` };
    }
  } catch (e: unknown) {
    if (getHttpStatus(e) === 404) {
      return {
        safe: false,
        reason: `Branch '${branchName}' could not be confirmed: 404 after 422 on createRef.`,
      };
    }
    return {
      safe: false,
      reason: `Branch '${branchName}' could not be inspected: ${getGitHubErrorMessage(e)}`,
    };
  }

  // Empty branch: head is exactly the base SHA — safe, nothing diverged.
  if (headSha === baseSha) {
    return { safe: true, branchExisted: true, headSha };
  }

  // If the Octokit instance does not expose compareCommitsWithBasehead, we cannot
  // verify branch safety — fail closed rather than silently assume safe.
  if (!context.octokit.rest.repos.compareCommitsWithBasehead) {
    const reason = `Branch '${branchName}' diverged from base but compareCommitsWithBasehead is unavailable. Cannot verify branch safety.`;
    context.log?.warn?.(
      { stage: 'request-pr:branch-unsafe', owner, repo, branch: branchName, baseSha, headSha, targetFilePath },
      reason
    );
    return { safe: false, reason };
  }

  // Compare branch against base to determine changed paths.
  try {
    const compareRes = await context.octokit.rest.repos.compareCommitsWithBasehead({
      owner,
      repo,
      basehead: `${baseSha}...${headSha}`,
    });

    const files = compareRes.data?.files;

    // Fail closed if the files array is absent or not an array — the API response is
    // incomplete and we cannot conclude the branch is safe.
    if (!Array.isArray(files)) {
      const reason = `Branch '${branchName}' diverged from base but the compare response has no files array. Cannot verify branch safety.`;
      context.log?.warn?.(
        {
          stage: 'request-pr:branch-unsafe',
          owner,
          repo,
          branch: branchName,
          baseSha,
          headSha,
          targetFilePath,
          outcome: 'missing-files-array',
        },
        reason
      );
      return { safe: false, reason };
    }

    // Fail closed if the branch has diverged but shows no changed files — this is
    // suspicious (truncated response, unusual merge situation) and we cannot conclude safe.
    if (files.length === 0) {
      const reason = `Branch '${branchName}' diverged from base (SHA differs) but compare shows zero changed files. Cannot verify branch safety.`;
      context.log?.warn?.(
        {
          stage: 'request-pr:branch-unsafe',
          owner,
          repo,
          branch: branchName,
          baseSha,
          headSha,
          targetFilePath,
          outcome: 'diverged-empty-diff',
        },
        reason
      );
      return { safe: false, reason };
    }

    // Normalize paths for comparison (strip leading slashes).
    const normalize = (p: string): string => p.replace(/^\/+/, '');
    const normalizedTarget = normalize(targetFilePath);

    const changedPaths: string[] = [];
    const statuses: string[] = [];

    for (const f of files) {
      const filename = typeof f.filename === 'string' ? f.filename : '';
      const status = typeof f.status === 'string' ? f.status : '';
      const previousFilename = typeof f.previous_filename === 'string' ? f.previous_filename : undefined;
      changedPaths.push(filename);
      if (status) statuses.push(status);

      // If the target file arrived via rename (something was renamed TO it), fail closed.
      // This means a file from an unrelated path was moved to the target path — not a fresh add by this bot.
      if (status === 'renamed' && normalize(filename) === normalizedTarget) {
        const reason =
          `Branch '${branchName}': target file '${targetFilePath}' arrived via rename` +
          (previousFilename ? ` from '${previousFilename}'` : '') +
          '. Cannot safely reuse this branch.';
        context.log?.warn?.(
          {
            stage: 'request-pr:branch-unsafe',
            owner,
            repo,
            branch: branchName,
            baseSha,
            headSha,
            targetFilePath,
            previousFilename,
            changedPaths,
            statuses,
            outcome: 'target-arrived-via-rename',
          },
          reason
        );
        return { safe: false, reason };
      }

      // If the target file was removed, fail closed.
      if (normalize(filename) === normalizedTarget && status === 'removed') {
        const reason = `Branch '${branchName}': target file '${targetFilePath}' has unsafe status '${status}'.`;
        context.log?.warn?.(
          {
            stage: 'request-pr:branch-unsafe',
            owner,
            repo,
            branch: branchName,
            baseSha,
            headSha,
            targetFilePath,
            changedPaths,
            statuses,
            outcome: 'target-unsafe-status',
          },
          reason
        );
        return { safe: false, reason };
      }
    }

    // Fail closed for target file entries with unexpected statuses or previous_filename.
    for (const f of files) {
      const filename = typeof f.filename === 'string' ? f.filename : '';
      const status = typeof f.status === 'string' ? f.status : '';
      const previousFilename = typeof f.previous_filename === 'string' ? f.previous_filename : undefined;

      if (normalize(filename) !== normalizedTarget) continue;

      if (status !== 'added' && status !== 'modified') {
        const outcome = !status ? 'target-missing-status' : 'target-unknown-status';
        const reason = `Branch '${branchName}': target file '${targetFilePath}' has unacceptable status '${status || '(empty)'}'. Cannot safely reuse this branch.`;
        context.log?.warn?.(
          {
            stage: 'request-pr:branch-unsafe',
            owner,
            repo,
            branch: branchName,
            baseSha,
            headSha,
            targetFilePath,
            changedPaths,
            statuses,
            outcome,
          },
          reason
        );
        return { safe: false, reason };
      }

      if (previousFilename !== undefined) {
        const reason =
          `Branch '${branchName}': target file '${targetFilePath}' has an unexpected previous_filename '${previousFilename}' on a '${status}' entry.` +
          ' Cannot safely reuse this branch.';
        context.log?.warn?.(
          {
            stage: 'request-pr:branch-unsafe',
            owner,
            repo,
            branch: branchName,
            baseSha,
            headSha,
            targetFilePath,
            previousFilename,
            changedPaths,
            statuses,
            outcome: 'target-unexpected-previous-filename',
          },
          reason
        );
        return { safe: false, reason };
      }
    }

    const unrelated = changedPaths.filter((p) => normalize(p) !== normalizedTarget);

    if (unrelated.length > 0) {
      const reason =
        `The request branch '${branchName}' contains unrelated changes: ${unrelated.slice(0, 3).join(', ')}` +
        (unrelated.length > 3 ? ` (and ${unrelated.length - 3} more)` : '') +
        '. It was left unchanged and requires manual review.';
      context.log?.warn?.(
        {
          stage: 'request-pr:branch-unsafe',
          owner,
          repo,
          branch: branchName,
          baseSha,
          headSha,
          targetFilePath,
          changedPaths,
          statuses,
          outcome: 'unrelated-changes',
        },
        reason
      );
      return { safe: false, reason };
    }

    return { safe: true, branchExisted: true, headSha };
  } catch (e: unknown) {
    const reason = `Could not verify branch safety for '${branchName}': ${getGitHubErrorMessage(e)}`;
    context.log?.warn?.(
      {
        stage: 'request-pr:branch-unsafe',
        owner,
        repo,
        branch: branchName,
        baseSha,
        targetFilePath,
        outcome: 'compare-failed',
      },
      reason
    );
    return {
      safe: false,
      reason,
    };
  }
}

// ─── File write with reconciliation ───────────────────────────────────────────

export type WriteFileResult =
  | { status: 'written' }
  | { status: 'skipped-equivalent' }
  | { status: 'conflict'; reason: string; cause?: Error }
  | { status: 'failed'; error: Error };

/** Injectable delay for tests; defaults to a real backoff. */
export type DelayFn = (ms: number) => Promise<void>;
const defaultDelay: DelayFn = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const RECONCILE_BACKOFF_MS = 200;
const MAX_CREATE_RETRIES = 1;

/**
 * Write a file to the request branch, with bounded post-write reconciliation for
 * ambiguous API failures.
 *
 * contentText is the already-generated YAML (from dumpYamlDoc) and is used directly
 * as the expected representation for all reconciliation reads.
 */
export async function writeFileWithReconciliation(
  context: ContextForReconciliation,
  repoRef: RepoRef,
  filePath: string,
  branchName: string,
  contentText: string,
  commitMessage: string,
  delay: DelayFn = defaultDelay
): Promise<WriteFileResult> {
  const { owner, repo } = repoRef;

  const doWrite = async (): Promise<void> => {
    await context.octokit.rest.repos.createOrUpdateFileContents({
      owner,
      repo,
      path: filePath,
      message: commitMessage,
      content: Buffer.from(contentText, 'utf8').toString('base64'),
      branch: branchName,
    });
  };

  try {
    await doWrite();
    context.log?.debug?.({ stage: 'request-pr:file-write', path: filePath }, 'File written');
    return { status: 'written' };
  } catch (firstError: unknown) {
    if (!isAmbiguousWriteError(firstError)) {
      // Definitive failure — do not retry.
      const err = firstError instanceof Error ? firstError : new Error(String(firstError));
      return { status: 'failed', error: err };
    }

    context.log?.info?.(
      { stage: 'request-pr:file-write-reconcile', path: filePath, status: getHttpStatus(firstError) },
      'Ambiguous write error — reconciling branch file state'
    );

    // Backoff to allow GHES consistency.
    try {
      await delay(RECONCILE_BACKOFF_MS);
    } catch (delayErr: unknown) {
      const msg = delayErr instanceof Error ? delayErr.message : String(delayErr);
      return {
        status: 'failed',
        error: new Error(`[request-pr:file-write-reconcile] Reconciliation delay failed: ${msg}`, {
          cause: firstError instanceof Error ? firstError : undefined,
        }),
      };
    }

    // Reconciliation read #1.
    let check1: Awaited<ReturnType<typeof compareFileOnBranch>>;
    try {
      check1 = await compareFileOnBranch(context, repoRef, filePath, branchName, contentText);
    } catch (readErr: unknown) {
      const msg = readErr instanceof Error ? readErr.message : String(readErr);
      return {
        status: 'failed',
        error: new Error(`[request-pr:file-write-reconcile] Reconciliation read failed: ${msg}`, {
          cause: firstError instanceof Error ? firstError : undefined,
        }),
      };
    }

    if (check1.status === 'equivalent') {
      context.log?.info?.(
        { stage: 'request-pr:file-write-recovered', path: filePath },
        'Write reconciled: equivalent file already on branch'
      );
      return { status: 'written' };
    }

    if (check1.status === 'conflict') {
      return {
        status: 'conflict',
        reason: `Write reconciliation found a conflicting file: ${check1.reason}`,
        cause: firstError instanceof Error ? firstError : undefined,
      };
    }

    // File still absent — attempt one controlled retry.
    let retryError: unknown = firstError;
    for (let attempt = 0; attempt < MAX_CREATE_RETRIES; attempt++) {
      try {
        await doWrite();
        retryError = null;
        break;
      } catch (e: unknown) {
        retryError = e;
      }
    }

    // Final reconciliation read.
    try {
      await delay(RECONCILE_BACKOFF_MS);
    } catch (delayErr: unknown) {
      const msg = delayErr instanceof Error ? delayErr.message : String(delayErr);
      return {
        status: 'failed',
        error: new Error(`[request-pr:file-write-reconcile] Reconciliation delay failed: ${msg}`, {
          cause: firstError instanceof Error ? firstError : undefined,
        }),
      };
    }

    let check2: Awaited<ReturnType<typeof compareFileOnBranch>>;
    try {
      check2 = await compareFileOnBranch(context, repoRef, filePath, branchName, contentText);
    } catch (readErr: unknown) {
      const msg = readErr instanceof Error ? readErr.message : String(readErr);
      return {
        status: 'failed',
        error: new Error(`[request-pr:file-write-reconcile] Reconciliation read failed: ${msg}`, {
          cause: firstError instanceof Error ? firstError : undefined,
        }),
      };
    }

    if (check2.status === 'equivalent') {
      context.log?.info?.(
        { stage: 'request-pr:file-write-recovered', path: filePath },
        'Write reconciled after controlled retry: equivalent file on branch'
      );
      return { status: 'written' };
    }

    if (check2.status === 'conflict') {
      return {
        status: 'conflict',
        reason: `Write retry produced a conflicting file: ${check2.reason}`,
        cause: firstError instanceof Error ? firstError : undefined,
      };
    }

    // File still absent after retry — preserve the original error with context.
    const retryMsg =
      retryError instanceof Error
        ? retryError.message
        : retryError !== null
          ? String(retryError)
          : 'no error (write appeared to succeed)';

    const originalMsg = firstError instanceof Error ? firstError.message : String(firstError);
    const combined = new Error(
      `The registry file could not be written to the request branch after reconciliation. ` +
        `Original error: ${originalMsg}. Retry outcome: ${retryMsg}`,
      { cause: firstError instanceof Error ? firstError : undefined }
    );
    return { status: 'failed', error: combined };
  }
}

// ─── Live PR lookup ────────────────────────────────────────────────────────────

/**
 * Look up an open PR for the exact head branch.
 * Propagates errors — never treats a lookup failure as "no PR".
 */
export async function lookupOpenPrForBranch(
  context: ContextForReconciliation,
  repoRef: RepoRef,
  branchName: string
): Promise<PullRequestLike | null> {
  const { owner, repo } = repoRef;

  let res: { data: PullRequestLike[] };
  try {
    res = await context.octokit.rest.pulls.list({
      owner,
      repo,
      state: 'open',
      head: `${owner}:${branchName}`,
    });
  } catch (e: unknown) {
    throw new Error(`[request-pr:pr-lookup] PR lookup failed: ${getGitHubErrorMessage(e)}`, {
      cause: e instanceof Error ? e : undefined,
    });
  }

  const pr = res.data.find((p) => p.head.ref === branchName);
  return pr ?? null;
}

// ─── PR create with reconciliation ────────────────────────────────────────────

export type CreatePrArgs = {
  owner: string;
  repo: string;
  title: string;
  head: string;
  base: string;
  body?: string;
  maintainer_can_modify?: boolean;
};

/**
 * Create a PR, with post-error reconciliation for ambiguous failures.
 * Always performs a live lookup before creating.
 */
export async function createPrWithReconciliation(
  context: ContextForReconciliation,
  args: CreatePrArgs,
  branchName: string
): Promise<PullRequestLike> {
  const { owner, repo } = args;

  // Live lookup before create.
  const existing = await lookupOpenPrForBranch(context, { owner, repo }, branchName);
  if (existing) {
    context.log?.debug?.({ stage: 'request-pr:pr-lookup', prNumber: existing.number }, 'Reusing existing open PR');
    return existing;
  }

  try {
    const res = await context.octokit.rest.pulls.create(args);
    context.log?.debug?.({ stage: 'request-pr:pr-create', prNumber: res.data.number }, 'PR created');
    return res.data;
  } catch (createError: unknown) {
    if (!isAmbiguousPrCreateError(createError)) {
      throw new Error(`[request-pr:pr-create] PR creation failed: ${getGitHubErrorMessage(createError)}`, {
        cause: createError instanceof Error ? createError : undefined,
      });
    }

    context.log?.info?.(
      { stage: 'request-pr:pr-create-reconcile', status: getHttpStatus(createError) },
      'Ambiguous PR create error — re-listing to check if PR was created'
    );

    // Re-list to check if the PR was created despite the error.
    let relistPr: PullRequestLike | null = null;
    try {
      relistPr = await lookupOpenPrForBranch(context, { owner, repo }, branchName);
    } catch (listError: unknown) {
      // Lookup itself failed — preserve original create error as cause, log both.
      context.log?.warn?.(
        {
          stage: 'request-pr:pr-create-reconcile',
          createStatus: getHttpStatus(createError),
          listError: listError instanceof Error ? listError.message : String(listError),
        },
        'PR create reconciliation list also failed'
      );
      throw new Error(
        `[request-pr:pr-create] PR creation failed (reconciliation list also failed): ${getGitHubErrorMessage(createError)}`,
        { cause: createError instanceof Error ? createError : undefined }
      );
    }

    if (relistPr) {
      context.log?.info?.(
        { stage: 'request-pr:pr-create-recovered', prNumber: relistPr.number },
        'PR create reconciled: existing PR found after ambiguous create error'
      );
      return relistPr;
    }

    // No PR found — rethrow original create error with stage context.
    throw new Error(
      `[request-pr:pr-create] PR creation failed (not found after reconciliation): ${getGitHubErrorMessage(createError)}`,
      { cause: createError instanceof Error ? createError : undefined }
    );
  }
}
