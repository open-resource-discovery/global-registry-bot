import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = RepoInfoBase & {
  issue_number: number;
};

type IssueLikeBase = {
  number: number;
};

type TemplateMetaLikeBase = {
  root?: string | null;
};

type TemplateLikeBase = {
  _meta?: TemplateMetaLikeBase;
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

export type RequestPrCreationRecoveryCallbacks<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
> = {
  createRequestPr: (
    context: ContextType,
    repoInfo: RepoType,
    issue: IssueType,
    parsedFormData: FormDataType,
    options: { template: TemplateType }
  ) => Promise<{ number: number }>;
  getHttpStatus: (error: unknown) => number | undefined;
  renderConfiguredRequestBranchName: (context: ContextType, issue: IssueType, resourceName: string) => string;
};

function extractCreatePrFailureMessage(error: unknown): string {
  const raw = (error instanceof Error ? error.message : String(error)).trim();
  const withoutUrl = raw.replace(/https?:\/\/\S+/gi, '').trim();

  const sanitized = withoutUrl || 'PR creation failed.';

  const marker = 'Validation Failed:';
  const idx = sanitized.indexOf(marker);

  if (idx >= 0) {
    const tail = sanitized.slice(idx + marker.length).trim();

    try {
      const parsed = JSON.parse(tail) as Record<string, unknown>;
      const msg = toStringTrim(parsed['message']);
      if (msg) return msg;
    } catch {
      // ignore
    }

    return tail || sanitized;
  }

  return sanitized;
}

function formatCreateRequestFailureForUser(error: unknown, _branchName = '', _resourceName = ''): string {
  const msg = extractCreatePrFailureMessage(error);
  return `Failed to create Pull Request: ${msg}`;
}

// ─── In-process single-flight guard ─────────────────────────────────────────
//
// Prevents two concurrent same-key calls from each attempting their own
// branch/file/PR write. State-based idempotency inside createRequestPr remains
// the authoritative guard across processes.

const inflight = new Map<string, Promise<{ number: number }>>();

function singleFlightKey(owner: string, repo: string, issueNumber: number, branchName: string): string {
  return `${owner}/${repo}/${issueNumber}/${branchName}`;
}

function runWithSingleFlight<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  key: string,
  fn: () => Promise<{ number: number }>,
  context: ContextType,
  _callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>,
  branchName: string,
  resourceName: string
): Promise<{ number: number }> {
  const existing = inflight.get(key);
  if (existing) {
    if ((context as { log?: { debug?: (obj: unknown, msg?: string) => void } }).log?.debug) {
      (context as { log: { debug: (obj: unknown, msg?: string) => void } }).log.debug(
        { stage: 'request-pr:single-flight-reused', key },
        'Reusing in-flight PR creation promise'
      );
    }
    return existing;
  }

  // Wrap fn so every caller — including ones that join after the promise is already
  // in flight — receives the formatted user-facing error, not a raw internal error.
  const wrapped = fn().catch((error: unknown) => {
    const msg = formatCreateRequestFailureForUser(error, branchName, resourceName);
    throw new Error(msg, { cause: error instanceof Error ? error : undefined });
  });

  inflight.set(key, wrapped);

  // Run cleanup directly from wrapped's settlement.
  // .finally() runs on wrapped itself — cleanup fires whether wrapped resolves or rejects.
  // The derived .catch(() => {}) suppresses any rejection from the finally callback
  // without affecting the rejection returned to callers of wrapped.
  void wrapped
    .finally(() => {
      if (inflight.get(key) === wrapped) {
        inflight.delete(key);
      }
    })
    .catch(() => {});

  return wrapped;
}

export function createRequestPrWithRecovery<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  parsedFormData: FormDataType,
  template: TemplateType,
  resourceName: string,
  callbacks: RequestPrCreationRecoveryCallbacks<ContextType, RepoInfoBase, IssueType, TemplateType, FormDataType>
): Promise<{ number: number }> {
  const repoInfo: RepoInfoBase = { owner: params.owner, repo: params.repo };
  const branchName = callbacks.renderConfiguredRequestBranchName(context, issue, resourceName);
  const key = singleFlightKey(params.owner, params.repo, issue.number, branchName);

  return runWithSingleFlight(
    key,
    () => callbacks.createRequestPr(context, repoInfo, issue, parsedFormData, { template }),
    context,
    callbacks,
    branchName,
    resourceName
  );
}
