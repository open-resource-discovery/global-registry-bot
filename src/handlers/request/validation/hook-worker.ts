import { tmpdir } from 'node:os';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHookApi } from './hook-api.js';

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

type Task = {
  owner: string;
  repo: string;

  // Stable identity for caching in worker
  path: string;
  hash: string;

  // Raw ESM module source code from repo
  code: string;

  // Function to call
  fn: string;

  // Plain JSON args
  args: unknown;

  allowedHosts?: string[];
  secrets?: Record<string, string>;
};

type HookWorkerResult = {
  found: boolean;
  value: unknown;
  logs: { level: 'debug' | 'info' | 'warn' | 'error'; obj: unknown; msg?: string }[];
};

// Per-worker cache
const MODULE_CACHE = new Map<string, Record<string, unknown>>();

function makeCapturingLog(): {
  log: {
    debug: (obj: unknown, msg?: string) => void;
    info: (obj: unknown, msg?: string) => void;
    warn: (obj: unknown, msg?: string) => void;
    error: (obj: unknown, msg?: string) => void;
  };
  logs: HookWorkerResult['logs'];
} {
  const logs: HookWorkerResult['logs'] = [];

  const push = (level: HookWorkerResult['logs'][number]['level'], obj: unknown, msg?: string): void => {
    logs.push({ level, obj, msg });
  };

  const log = {
    debug: (obj: unknown, msg?: string): void => push('debug', obj, msg),
    info: (obj: unknown, msg?: string): void => push('info', obj, msg),
    warn: (obj: unknown, msg?: string): void => push('warn', obj, msg),
    error: (obj: unknown, msg?: string): void => push('error', obj, msg),
  };

  return { log, logs };
}

function getGlobalFetch(): typeof fetch {
  const f = (globalThis as { fetch?: unknown }).fetch;
  if (typeof f !== 'function') throw new Error('fetch is not available in this runtime');
  return f as typeof fetch;
}

type HookApi = ReturnType<typeof createHookApi>;

function installSafeFetch(allowedHosts: string[], secrets: Record<string, string>): HookApi {
  const api = createHookApi({}, { secrets: secrets ?? {}, allowedHosts: allowedHosts ?? [] });
  const baseFetch = getGlobalFetch();

  const g = globalThis as typeof globalThis & {
    fetch: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  };

  // Override only inside worker thread
  g.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    // Only allow URLs approved by hook-api
    const urlObj = api.assertAllowedUrl(String(input));
    const url = typeof urlObj === 'string' ? urlObj : urlObj.toString();

    // Force redirect blocking
    return baseFetch(url, { ...(init ?? {}), redirect: 'manual' });
  };

  return api;
}

async function loadModule(task: Task): Promise<Record<string, unknown>> {
  const key = `${task.owner}/${task.repo}:${task.path}#${task.hash}`;
  const cached = MODULE_CACHE.get(key);
  if (cached) return cached;

  const dir = join(tmpdir(), 'registry-bot-hooks');
  await mkdir(dir, { recursive: true });

  // Stable filename per hash. New hash => new file => safe cache bust.
  const file = join(dir, `${task.owner}__${task.repo}__${task.hash}.mjs`);
  await writeFile(file, task.code, 'utf8');

  const href = pathToFileURL(file).href;

  const imported: unknown = await import(href);
  if (!isPlainObject(imported)) {
    throw new Error('Hook module did not evaluate to an object namespace');
  }

  const mod = imported;
  MODULE_CACHE.set(key, mod);
  return mod;
}

function disableProcessExitAndKill(): void {
  process.exit = ((..._args: Parameters<NodeJS.Process['exit']>): never => {
    throw new Error('process.exit is disabled in hook worker');
  }) as NodeJS.Process['exit'];

  process.kill = ((..._args: Parameters<NodeJS.Process['kill']>): never => {
    throw new Error('process.kill is disabled in hook worker');
  }) as NodeJS.Process['kill'];
}

export default async function hookWorker(task: Task): Promise<HookWorkerResult> {
  disableProcessExitAndKill();

  const { log, logs } = makeCapturingLog();

  const api = installSafeFetch(task.allowedHosts ?? [], task.secrets ?? {});

  try {
    const mod = await loadModule(task);

    const def = mod.default;
    const candidate: Record<string, unknown> = isPlainObject(def) ? def : mod;

    const fnValue = candidate[task.fn] ?? mod[task.fn];

    if (typeof fnValue !== 'function') {
      return { found: false, value: undefined, logs };
    }

    const argsObj: Record<string, unknown> = isPlainObject(task.args)
      ? { ...task.args, api, log }
      : { value: task.args, api, log };

    const fn = fnValue as (a: unknown) => unknown;
    const ret = await fn(argsObj);

    // beforeValidate may mutate args.form -> return it so main can re-apply
    if (task.fn === 'beforeValidate') {
      const formVal = argsObj.form;
      if (isPlainObject(formVal)) {
        return { found: true, value: { form: formVal }, logs };
      }
    }

    return { found: true, value: ret, logs };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    logs.push({ level: 'error', obj: { err: msg }, msg: 'hook-worker:failed' });
    return { found: true, value: { __hookError: msg }, logs };
  }
}
