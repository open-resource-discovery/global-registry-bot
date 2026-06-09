import { type RegistryBotHooks as StaticRegistryBotHooks } from '../../../config.js';
import { runHookInWorker } from './hook-pool.js';

type HookSecrets = Readonly<Record<string, string | undefined>>;

type LoggerLike = {
  debug?: (obj: unknown, msg?: string) => void;
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
};

type HookLogger = {
  debug: (obj: unknown, msg?: string) => void;
  info: (obj: unknown, msg?: string) => void;
  warn: (obj: unknown, msg?: string) => void;
  error: (obj: unknown, msg?: string) => void;
};

type RepoRef = { owner: string; repo: string };
type IssueRef = { owner: string; repo: string; issue_number: number };

type RequestConfigEntry = {
  folderName?: string;
  schema?: string;
  issueTemplate?: string;
  [k: string]: unknown;
};

type ResourceBotConfig = {
  requests?: Record<string, RequestConfigEntry>;
  hooks?: { allowedHosts?: string[] };
  workflow?: Record<string, unknown>;
  [k: string]: unknown;
};

type FormData = Record<string, string>;

type ApprovalHookStatus = 'approved' | 'rejected' | 'unknown';

type ApprovalHookDecision = Readonly<{
  status?: ApprovalHookStatus;
  path?: string;
  reason?: string;
  comment?: string;
  message?: string;
  approvers?: readonly string[];
  error?: readonly {
    field?: string;
    message?: string;
  }[];
  errors?: readonly {
    field?: string;
    message?: string;
  }[];
}>;

type ApprovalHookResult =
  | ApprovalHookStatus
  | boolean
  | {
      status?: unknown;
      path?: unknown;
      reason?: unknown;
      comment?: unknown;
      message?: unknown;
      approvers?: unknown;
      error?: unknown;
      errors?: unknown;
      approved?: unknown;
    }
  | undefined
  | void;

type OnApprovalArgs = Readonly<{
  requestType: string;
  namespace: string;
  resourceName: string;
  form: FormData;
  data: Readonly<Record<string, unknown>>;
  requestAuthor: Readonly<{
    id: string;
    email: string;
  }>;
  config: Readonly<{
    raw: Readonly<Record<string, unknown>>;
    approvers: string[];
  }>;
  issue: Readonly<{
    number: number;
    title: string;
    body: string;
    state: string;
    author: string;
    labels: string[];
  }>;
  log?: LoggerLike | undefined;
}>;

type ResourceBotHooks = {
  onApproval?: (args: OnApprovalArgs) => ApprovalHookResult | Promise<ApprovalHookResult>;
  [k: string]: unknown;
};

type HookDescriptor = Readonly<{
  __type: string;
  __path: string;
  __hash: string;
  __code: string;
}>;

type ValidationContext = {
  octokit: unknown;
  log?: LoggerLike;
  repo: () => RepoRef;
  issue: () => IssueRef;
  resourceBotConfig?: ResourceBotConfig;
  resourceBotHooks?: ResourceBotHooks | StaticRegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type IssueLike = {
  number?: number;
  body?: string | null;
  title?: string | null;
  state?: string | null;
  labels?: (string | { name?: string | null })[] | null;
  user?: { login?: string | null } | null;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringSafe(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v).trim();
  return '';
}

function getRecordProp(obj: unknown, key: string): unknown {
  if (!isPlainObject(obj)) return undefined;
  return obj[key];
}

function getStringProp(obj: unknown, key: string): string | undefined {
  const v = getRecordProp(obj, key);
  return typeof v === 'string' ? v : undefined;
}

function getHookLogger(log?: LoggerLike): HookLogger {
  const noop = (_obj: unknown, _msg?: string): void => {};
  return {
    debug: typeof log?.debug === 'function' ? log.debug : noop,
    info: typeof log?.info === 'function' ? log.info : noop,
    warn: typeof log?.warn === 'function' ? log.warn : noop,
    error: typeof log?.error === 'function' ? log.error : noop,
  };
}

function pickHookSecretsForWorker(secrets: HookSecrets): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(secrets || {})) {
    if (typeof v !== 'string') continue;
    const s = v.trim();
    if (!s) continue;
    out[k] = s;
  }
  return out;
}

function approvalIssueLabelName(value: unknown): string {
  if (typeof value === 'string') return toStringSafe(value);
  if (isPlainObject(value)) return toStringSafe(value['name']);
  return '';
}

function toApprovalIssueLabelNames(labels: IssueLike['labels']): string[] {
  const items = Array.isArray(labels) ? labels : [];
  return items.map((label) => approvalIssueLabelName(label)).filter(Boolean);
}

function normalizeApprovalHookErrors(value: unknown): readonly {
  field?: string;
  message?: string;
}[] {
  if (!Array.isArray(value)) return [];

  const out: { field?: string; message?: string }[] = [];
  const seen = new Set<string>();

  for (const item of value) {
    if (!isPlainObject(item)) continue;

    const field = toStringSafe(item['field']);
    const message = toStringSafe(item['message']);
    if (!message) continue;

    const key = `${field}\u0000${message}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      ...(field ? { field } : {}),
      message,
    });
  }

  return out;
}

function normalizeLoginValue(value: unknown): string {
  return toStringSafe(value).replace(/^@+/, '').trim();
}

function uniqLogins(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  for (const value of values || []) {
    const login = normalizeLoginValue(value);
    if (!login) continue;

    const key = login.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(login);
  }

  return out;
}

function toLoginArray(value: unknown): string[] {
  return Array.isArray(value) ? uniqLogins(value.map((item) => normalizeLoginValue(item)).filter(Boolean)) : [];
}

function getApprovalHookApprovers(context: ValidationContext, requestType: string): string[] {
  const cfg = context.resourceBotConfig ?? {};
  const workflow = isPlainObject(cfg['workflow']) ? cfg['workflow'] : {};

  const fallbackApprovers = uniqLogins([
    ...toLoginArray(workflow['approvers']),
    ...toLoginArray(workflow['approversPool']),
  ]);

  const reqs = isPlainObject(cfg.requests) ? cfg.requests : {};
  const entry = isPlainObject(reqs[requestType]) ? reqs[requestType] : null;

  if (!entry) return fallbackApprovers;

  const hasOwnApprovers = Array.isArray(entry['approvers']);
  const hasOwnApproversPool = Array.isArray(entry['approversPool']);

  if (!hasOwnApprovers && !hasOwnApproversPool) return fallbackApprovers;

  return uniqLogins([...toLoginArray(entry['approvers']), ...toLoginArray(entry['approversPool'])]);
}

function buildApprovalHookData(
  args: {
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
  },
  namespace: string,
  resourceName: string
): Readonly<Record<string, unknown>> {
  return {
    ...args.formData,
    name: resourceName || namespace,
    identifier: toStringSafe(args.formData['identifier']) || resourceName || namespace,
    namespace: toStringSafe(args.formData['namespace']) || namespace || resourceName,
  };
}

function normalizeApprovalHookResult(value: unknown): ApprovalHookDecision {
  if (value === true) return { status: 'approved' };
  if (value === false || value === undefined || value === null) return {};

  const token = toStringSafe(value).toLowerCase();
  if (token === 'approved' || token === 'rejected' || token === 'unknown') {
    return { status: token as ApprovalHookStatus };
  }

  if (!isPlainObject(value)) return {};

  const approvers = toLoginArray(value['approvers']);

  if (value['approved'] === true) {
    const comment = toStringSafe(value['comment']);
    const message = toStringSafe(value['message']);

    return {
      status: 'approved',
      ...(comment ? { comment } : {}),
      ...(message ? { message } : {}),
      ...(approvers.length ? { approvers } : {}),
    };
  }

  const status = toStringSafe(value['status']).toLowerCase();
  const path = toStringSafe(value['path']);
  const reason = toStringSafe(value['reason']);
  const comment = toStringSafe(value['comment']);
  const message = toStringSafe(value['message']);
  const errors = normalizeApprovalHookErrors(value['errors'] ?? value['error']);

  if (status === 'approved' || status === 'rejected' || status === 'unknown') {
    return {
      status: status as ApprovalHookStatus,
      ...(path ? { path } : {}),
      ...(reason ? { reason } : {}),
      ...(comment ? { comment } : {}),
      ...(message ? { message } : {}),
      ...(approvers.length ? { approvers } : {}),
      ...(errors.length ? { errors } : {}),
    };
  }

  return {};
}

function logApprovalHookMessages(
  context: ValidationContext,
  logs: { level: 'debug' | 'info' | 'warn' | 'error'; obj: unknown; msg?: string }[] | undefined
): void {
  if (!logs?.length) return;

  for (const entry of logs) {
    const msg = entry.msg || 'hook:onApproval';
    if (entry.level === 'error') context.log?.error?.(entry.obj, msg);
    else if (entry.level === 'warn') context.log?.warn?.(entry.obj, msg);
    else if (entry.level === 'debug') context.log?.debug?.(entry.obj, msg);
    else context.log?.info?.(entry.obj, msg);
  }
}

function getApprovalAllowedHosts(context: ValidationContext): string[] {
  return Array.isArray(context.resourceBotConfig?.hooks?.allowedHosts)
    ? context.resourceBotConfig.hooks.allowedHosts
    : [];
}

function resolveApprovalNamespace(args: {
  namespace?: string | null;
  resourceName?: string | null;
  formData: FormData;
}): string {
  return (
    toStringSafe(args.namespace) ||
    toStringSafe(args.formData['namespace']) ||
    toStringSafe(args.formData['identifier']) ||
    toStringSafe(args.resourceName)
  );
}

function resolveApprovalResourceName(
  args: {
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
  },
  namespace: string
): string {
  return (
    toStringSafe(args.resourceName) ||
    toStringSafe(args.formData['identifier']) ||
    toStringSafe(args.formData['namespace']) ||
    namespace
  );
}

function buildApprovalHookArgs(
  context: ValidationContext,
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  }
): OnApprovalArgs {
  const namespace = resolveApprovalNamespace(args);
  const resourceName = resolveApprovalResourceName(args, namespace);

  const hasExplicitRequestAuthorId = args.requestAuthorId !== undefined && args.requestAuthorId !== null;
  const requesterId = hasExplicitRequestAuthorId
    ? toStringSafe(args.requestAuthorId)
    : toStringSafe(args.issue?.user?.login);

  return {
    requestType: toStringSafe(args.requestType),
    namespace,
    resourceName,
    form: args.formData,
    data: buildApprovalHookData(args, namespace, resourceName),
    requestAuthor: {
      id: requesterId,
      email: '',
    },
    config: {
      raw: isPlainObject(context.resourceBotConfig) ? context.resourceBotConfig : {},
      approvers: getApprovalHookApprovers(context, toStringSafe(args.requestType)),
    },
    issue: {
      number: typeof args.issue?.number === 'number' ? args.issue.number : 0,
      title: toStringSafe(args.issue?.title),
      body: toStringSafe(args.issue?.body),
      state: toStringSafe(args.issue?.state),
      author: requesterId,
      labels: toApprovalIssueLabelNames(args.issue?.labels),
    },
    log: undefined,
  };
}

function isHookDescriptor(v: unknown): v is HookDescriptor {
  return (
    isPlainObject(v) &&
    typeof v.__type === 'string' &&
    typeof v.__path === 'string' &&
    typeof v.__hash === 'string' &&
    typeof v.__code === 'string'
  );
}

function getResourceBotHooks(context: ValidationContext): ResourceBotHooks | HookDescriptor | null {
  const hooks = context?.resourceBotHooks;
  return isPlainObject(hooks) ? (hooks as ResourceBotHooks | HookDescriptor) : null;
}

async function runApprovalHookDescriptor(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  hooks: HookDescriptor,
  hookArgs: OnApprovalArgs,
  allowedHosts: string[],
  hookSecrets: HookSecrets
): Promise<ApprovalHookDecision> {
  const workerSecrets = pickHookSecretsForWorker(hookSecrets || {});
  const res = await runHookInWorker(
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      path: hooks.__path,
      hash: hooks.__hash,
      code: hooks.__code,
      fn: 'onApproval',
      args: hookArgs,
      allowedHosts,
      secrets: workerSecrets,
    },
    { timeoutMs: 8000 }
  );

  logApprovalHookMessages(context, res.logs);

  const hookErr = getStringProp(res.value, '__hookError');
  if (hookErr) {
    context.log?.warn?.({ err: hookErr }, 'resource-bot hooks.onApproval failed');
    return {};
  }

  if (!res.found) return {};
  return normalizeApprovalHookResult(res.value);
}

async function runApprovalHookInProcess(
  context: ValidationContext,
  hooks: ResourceBotHooks,
  hookArgs: OnApprovalArgs
): Promise<ApprovalHookDecision> {
  const onApprovalHook = hooks.onApproval;
  if (typeof onApprovalHook !== 'function') return {};

  try {
    const ret = await onApprovalHook({
      ...hookArgs,
      log: getHookLogger(context.log),
    });

    return normalizeApprovalHookResult(ret);
  } catch (err: unknown) {
    context.log?.warn?.(
      { err: err instanceof Error ? err.message : String(err) },
      'resource-bot hooks.onApproval failed'
    );
    return {};
  }
}

export function runApprovalHookRuntime(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  },
  options: {
    hookSecrets: HookSecrets;
  }
): Promise<ApprovalHookDecision> {
  const hooks = getResourceBotHooks(context);
  if (!hooks) return Promise.resolve({});

  const allowedHosts = getApprovalAllowedHosts(context);
  const hookArgs = buildApprovalHookArgs(context, args);

  if (isHookDescriptor(hooks)) {
    return runApprovalHookDescriptor(context, repoInfo, hooks, hookArgs, allowedHosts, options.hookSecrets);
  }

  return runApprovalHookInProcess(context, hooks, hookArgs);
}
