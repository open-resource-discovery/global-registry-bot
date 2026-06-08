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

type BeforeValidateArgs = Readonly<{
  requestType: string;
  form: FormData;
  api: unknown;
  config: HookConfig;
  log?: LoggerLike | undefined;
}>;

type HookDescriptor = Readonly<{
  __type: string;
  __path: string;
  __hash: string;
  __code: string;
}>;

type HooksWithBeforeValidate = {
  beforeValidate?: (args: BeforeValidateArgs) => void | Promise<void>;
  [k: string]: unknown;
};

export async function runBeforeValidateHookRuntime(args: {
  hooks: HooksWithBeforeValidate | HookDescriptor;
  owner: string;
  repo: string;
  requestType: string;
  formData: FormData;
  allowedHosts: string[];
  workerSecrets: Record<string, string>;
  hookWorkerConfig: HookWorkerConfig;
  hookRuntimeConfig: HookRuntimeConfig;
  hookApi: unknown;
  log?: LoggerLike;
  isHookDescriptor: (value: unknown) => value is HookDescriptor;
  getHookLogger: (log?: LoggerLike) => HookLogger;
  getStringProp: (obj: unknown, key: string) => string | undefined;
  getObjectProp: (obj: unknown, key: string) => Record<string, unknown> | null;
}): Promise<void> {
  if (args.isHookDescriptor(args.hooks)) {
    const beforeValidateArgs: BeforeValidateArgs = {
      requestType: args.requestType,
      form: args.formData,
      api: null,
      config: args.hookWorkerConfig,
      log: undefined,
    };
    const res = await runHookInWorker(
      {
        owner: args.owner,
        repo: args.repo,
        path: args.hooks.__path,
        hash: args.hooks.__hash,
        code: args.hooks.__code,
        fn: 'beforeValidate',
        args: beforeValidateArgs,
        allowedHosts: args.allowedHosts,
        secrets: args.workerSecrets,
      },
      { timeoutMs: 8000 }
    );

    if (res.logs.length) {
      for (const l of res.logs) {
        const msg = l.msg || 'hook:beforeValidate';
        if (l.level === 'error') args.log?.error?.(l.obj, msg);
        else if (l.level === 'warn') args.log?.warn?.(l.obj, msg);
        else if (l.level === 'debug') args.log?.debug?.(l.obj, msg);
        else args.log?.info?.(l.obj, msg);
      }
    }

    const hookErr = args.getStringProp(res.value, '__hookError');
    if (hookErr) {
      args.log?.warn?.({ err: hookErr }, 'resource-bot hooks.beforeValidate failed');
    }

    const workerForm = args.getObjectProp(res.value, 'form');
    if (workerForm) {
      const dst = args.formData as Record<string, string>;

      for (const k of Object.keys(dst)) delete dst[k];

      for (const [k, v] of Object.entries(workerForm)) {
        if (v === null || v === undefined) continue;

        if (typeof v === 'string') dst[k] = v;
        else if (typeof v === 'number' || typeof v === 'boolean') dst[k] = String(v);
        else dst[k] = String(v);
      }
    }

    return;
  }

  if (typeof args.hooks.beforeValidate !== 'function') return;

  try {
    await args.hooks.beforeValidate({
      requestType: args.requestType,
      form: args.formData,
      api: args.hookApi,
      config: args.hookRuntimeConfig,
      log: args.getHookLogger(args.log),
    });
  } catch (err: unknown) {
    args.log?.warn?.(
      { err: err instanceof Error ? err.message : String(err) },
      'resource-bot hooks.beforeValidate failed'
    );
  }
}
