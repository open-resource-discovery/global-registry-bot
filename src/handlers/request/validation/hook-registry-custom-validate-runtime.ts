import { runHookInWorker } from './hook-pool.js';

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

type HookSecrets = Readonly<Record<string, string | undefined>>;

type HookWorkerConfig = Readonly<Record<string, string>>;

type HookRuntimeConfig = Readonly<
  Record<string, string | ((key: string) => string)> & {
    getSecret: (key: string) => string;
  }
>;

type HookConfig = HookWorkerConfig | HookRuntimeConfig;

type FormData = Record<string, string>;

type CandidateData = Record<string, unknown>;

type TemplateLike = {
  [k: string]: unknown;
};

type CustomValidateArgs = Readonly<{
  requestType: string;
  resourceName: string;
  candidate: Record<string, unknown>;
  form: FormData;
  api: unknown;
  config: HookConfig;
  requestAuthor: Readonly<{
    id: string;
    email?: string;
  }>;
  issue: Readonly<{
    number: number;
    title: string;
    body: string;
    state: string;
    author: string;
    labels: string[];
  }>;
  parentResourceName?: string;
  parentCandidate?: Readonly<Record<string, unknown>> | null;
  parentOwners?: readonly string[];
  log?: LoggerLike | undefined;
}>;

type HookValidationItem =
  | string
  | {
      field?: unknown;
      message?: unknown;
    };

type HookValidationResult = HookValidationItem[] | undefined | void;

type HooksWithOnValidate = {
  onValidate?: (args: CustomValidateArgs) => HookValidationResult | Promise<HookValidationResult>;
  [k: string]: unknown;
};

type HookDescriptor = Readonly<{
  __type: string;
  __path: string;
  __hash: string;
  __code: string;
}>;

type RepoInfo = { owner: string; repo: string };

type RuntimeContext = {
  log?: LoggerLike;
};

export async function runRegistryCustomValidateRuntime<Context extends RuntimeContext>(args: {
  context: Context;
  hooks: HooksWithOnValidate | HookDescriptor;
  repoInfo: RepoInfo;
  requestType: string;
  schema: unknown;
  candidate: CandidateData;
  resourceName?: string | null;
  formData?: FormData | null;
  allowedHosts: string[];
  hookSecrets: HookSecrets;
  hookWorkerConfig: HookWorkerConfig;
  hookRuntimeConfig: HookRuntimeConfig;
  isHookDescriptor: (value: unknown) => value is HookDescriptor;
  getHookLogger: (log?: LoggerLike) => HookLogger;
  normalizeHookErrors: (value: unknown) => string[];
  buildFormDataForHookValidationFromCandidate: (
    requestType: string,
    schema: unknown,
    candidate: CandidateData
  ) => Promise<FormData>;
  normalizeFormDataForHookValidation: (
    requestType: string,
    formData: FormData,
    schema: unknown,
    template?: TemplateLike | null
  ) => FormData;
  resolvePrimaryIdFromCandidate: (candidate: CandidateData, schema: unknown) => string;
  getRecordProp: (obj: unknown, key: string) => unknown;
  toStringSafe: (value: unknown) => string;
  pickHookSecretsForWorker: (secrets: HookSecrets) => Record<string, string>;
  createHookApi: (context: Context, args: { secrets: HookSecrets; allowedHosts: string[] }) => unknown;
}): Promise<string[]> {
  const form =
    args.formData && isPlainObject(args.formData)
      ? args.normalizeFormDataForHookValidation(args.requestType, args.formData, args.schema, null)
      : await args.buildFormDataForHookValidationFromCandidate(args.requestType, args.schema, args.candidate);

  const normalizedResourceName = args.toStringSafe(form.identifier) || args.toStringSafe(form.namespace);
  const inferredResourceName = args.resolvePrimaryIdFromCandidate(args.candidate, args.schema);

  const resourceName =
    normalizedResourceName ||
    inferredResourceName ||
    args.toStringSafe(args.resourceName) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'product-id')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'productId')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'id')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'name')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'identifier')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'namespace')) ||
    args.toStringSafe(args.getRecordProp(args.candidate, 'vendor'));

  if (!args.isHookDescriptor(args.hooks)) {
    const onValidateHook = args.hooks.onValidate;
    const isLegacyHook = typeof onValidateHook === 'function';

    if (!isLegacyHook) return [];

    try {
      const extra = await onValidateHook({
        requestType: args.requestType,
        resourceName,
        candidate: args.candidate,
        form,
        api: args.createHookApi(args.context, {
          secrets: args.hookSecrets || {},
          allowedHosts: args.allowedHosts,
        }),
        config: args.hookRuntimeConfig,
        requestAuthor: { id: '' },
        issue: {
          number: 0,
          title: '',
          body: '',
          state: '',
          author: '',
          labels: [],
        },
        parentResourceName: '',
        parentCandidate: null,
        parentOwners: [],
        log: args.getHookLogger(args.context.log),
      });

      return args.normalizeHookErrors(extra);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return [`Hook onValidate failed: ${msg}`];
    }
  }

  const workerSecrets = args.pickHookSecretsForWorker(args.hookSecrets || {});
  const hookArgs: CustomValidateArgs = {
    requestType: args.requestType,
    resourceName,
    candidate: args.candidate,
    form,
    api: null,
    config: args.hookWorkerConfig,
    requestAuthor: { id: '' },
    issue: {
      number: 0,
      title: '',
      body: '',
      state: '',
      author: '',
      labels: [],
    },
    parentResourceName: '',
    parentCandidate: null,
    parentOwners: [],
    log: undefined,
  };

  const res = await runHookInWorker(
    {
      owner: args.repoInfo.owner,
      repo: args.repoInfo.repo,
      path: args.hooks.__path,
      hash: args.hooks.__hash,
      code: args.hooks.__code,
      fn: 'onValidate',
      args: hookArgs,
      allowedHosts: args.allowedHosts,
      secrets: workerSecrets,
    },
    { timeoutMs: 8000 }
  );

  if (res.logs?.length && args.context.log?.info) {
    for (const l of res.logs) {
      const msg = l.msg || 'hook:onValidate';
      if (l.level === 'error') args.context.log.error?.(l.obj, msg);
      else if (l.level === 'warn') args.context.log.warn?.(l.obj, msg);
      else if (l.level === 'debug') args.context.log.debug?.(l.obj, msg);
      else args.context.log.info?.(l.obj, msg);
    }
  }

  const msgs = args.normalizeHookErrors(res.value);
  return msgs.length ? msgs : [];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
