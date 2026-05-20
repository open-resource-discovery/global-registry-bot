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

type HookWorkerConfig = Readonly<Record<string, string>>;

type HookRuntimeConfig = Readonly<
  Record<string, string | ((key: string) => string)> & {
    getSecret: (key: string) => string;
  }
>;

type HookConfig = HookWorkerConfig | HookRuntimeConfig;

type FormData = Record<string, string>;

type CandidateData = Record<string, unknown>;

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

type ValidationHooks = {
  customValidate?: (args: CustomValidateArgs) => HookValidationResult | Promise<HookValidationResult>;
  onValidate?: (args: CustomValidateArgs) => HookValidationResult | Promise<HookValidationResult>;
  [k: string]: unknown;
};

type HookDescriptor = Readonly<{
  __type: string;
  __path: string;
  __hash: string;
  __code: string;
}>;

type CustomValidateContextArgs = Pick<
  CustomValidateArgs,
  'requestAuthor' | 'issue' | 'parentResourceName' | 'parentCandidate' | 'parentOwners'
>;

export async function runValidationHookRuntime(args: {
  hooks: ValidationHooks | HookDescriptor;
  owner: string;
  repo: string;
  requestType: string;
  rawIdOrNs: string;
  candidate: CandidateData;
  normalizedFormData: FormData;
  customValidateContextArgs: CustomValidateContextArgs;
  hookApi: unknown;
  hookWorkerConfig: HookWorkerConfig;
  hookRuntimeConfig: HookRuntimeConfig;
  allowedHosts: string[];
  workerSecrets: Record<string, string>;
  log?: LoggerLike;
  isHookDescriptor: (value: unknown) => value is HookDescriptor;
  getStringProp: (obj: unknown, key: string) => string | undefined;
  normalizeHookErrors: (value: unknown) => string[];
  getHookLogger: (log?: LoggerLike) => HookLogger;
  rulesBucket: string[];
  errors: string[];
}): Promise<void> {
  // Worker path: hooks is just a descriptor (raw code + hash)
  if (args.isHookDescriptor(args.hooks)) {
    const nameVal = args.candidate['name'];

    const hookArgs: CustomValidateArgs = {
      requestType: args.requestType,
      resourceName: args.rawIdOrNs || (typeof nameVal === 'string' ? nameVal : ''),
      candidate: args.candidate,
      form: args.normalizedFormData,
      api: null,
      config: args.hookWorkerConfig,
      ...args.customValidateContextArgs,
      log: undefined,
    };

    // Prefer the new entrypoint name first
    const fnsToTry = ['onValidate', 'customValidate'] as const;

    for (const fn of fnsToTry) {
      const res = await runHookInWorker(
        {
          owner: args.owner,
          repo: args.repo,
          path: args.hooks.__path,
          hash: args.hooks.__hash,
          code: args.hooks.__code,
          fn,
          args: hookArgs,
          allowedHosts: args.allowedHosts,
          secrets: args.workerSecrets,
        },
        { timeoutMs: 8000 }
      );

      // Optional: forward worker logs into main logger
      if (res.logs?.length && args.log?.info) {
        for (const l of res.logs) {
          const msg = l.msg || `hook:${fn}`;
          if (l.level === 'error') args.log.error?.(l.obj, msg);
          else if (l.level === 'warn') args.log.warn?.(l.obj, msg);
          else if (l.level === 'debug') args.log.debug?.(l.obj, msg);
          else args.log.info?.(l.obj, msg);
        }
      }

      const hookErr = args.getStringProp(res.value, '__hookError');
      if (hookErr) {
        args.log?.warn?.({ err: hookErr, fn }, 'resource-bot hook validation failed');
        // if the function existed, do not fall back
        if (res.found) break;
        continue;
      }

      const msgs = args.normalizeHookErrors(res.value);
      if (msgs.length) {
        args.rulesBucket.push(...msgs);
        args.errors.push(...msgs);
      }

      // IMPORTANT: stop if the function existed
      if (res.found) break;
    }

    return;
  }

  // Legacy path
  const validateHook =
    typeof args.hooks.onValidate === 'function'
      ? args.hooks.onValidate
      : typeof args.hooks.customValidate === 'function'
        ? args.hooks.customValidate
        : null;

  if (!validateHook) return;

  try {
    const nameVal = args.candidate['name'];

    const extra = await validateHook({
      requestType: args.requestType,
      resourceName: args.rawIdOrNs || (typeof nameVal === 'string' ? nameVal : ''),
      candidate: args.candidate,
      form: args.normalizedFormData,
      api: args.hookApi,
      config: args.hookRuntimeConfig,
      ...args.customValidateContextArgs,
      log: args.getHookLogger(args.log),
    });

    const msgs = args.normalizeHookErrors(extra);
    if (msgs.length) {
      args.rulesBucket.push(...msgs);
      args.errors.push(...msgs);
    }
  } catch (err: unknown) {
    args.log?.warn?.(
      { err: err instanceof Error ? err.message : String(err) },
      'resource-bot hooks custom validation failed'
    );
  }
}
