import { mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { getErrorMessage, isPlainObject } from '../infrastructure/errors.js';
import { createHookApi, type HookApi } from './hook-api.js';
import type { HookWorkerResult, HookWorkerTask } from './hook-pool.js';

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

function installSafeFetch(
  allowedHosts: string[],
  secrets: Record<string, string>
): { api: HookApi; restore: () => void } {
  const api = createHookApi({}, { secrets, allowedHosts });
  const baseFetch = globalThis.fetch;
  if (typeof baseFetch !== 'function') throw new Error('fetch is not available in this runtime');

  globalThis.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> =>
    baseFetch(api.assertAllowedUrl(String(input)).toString(), { ...init, redirect: 'manual' });

  return {
    api,
    restore: (): void => {
      globalThis.fetch = baseFetch;
    },
  };
}

function normalizeHookSecretName(value: unknown): string {
  return String(value ?? '')
    .replace(/^HOOK_SECRET_/i, '')
    .trim();
}

function withHookGetSecret(configValue: unknown, secrets: Record<string, string>): Record<string, unknown> {
  const config: Record<string, unknown> = isPlainObject(configValue) ? { ...configValue } : {};
  const values: Record<string, string> = {};
  for (const [key, value] of [...Object.entries(secrets), ...Object.entries(config)]) {
    const name = normalizeHookSecretName(key);
    if (name && typeof value === 'string' && value.trim()) values[name] = value.trim();
  }

  Object.defineProperty(config, 'getSecret', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: (key: string): string => values[normalizeHookSecretName(key)] || '',
  });

  return config;
}

async function loadModule(task: HookWorkerTask): Promise<Record<string, unknown>> {
  const key = `${task.owner}/${task.repo}:${task.path}#${task.hash}`;
  const cached = MODULE_CACHE.get(key);
  if (cached) return cached;

  const dir = join(tmpdir(), 'registry-bot-hooks');
  await mkdir(dir, { recursive: true });

  // A changed source hash gives Node a new module URL.
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
  process.exit = (..._args: Parameters<NodeJS.Process['exit']>): never => {
    throw new Error('process.exit is disabled in hook worker');
  };

  process.kill = (..._args: Parameters<NodeJS.Process['kill']>): never => {
    throw new Error('process.kill is disabled in hook worker');
  };
}

export default async function hookWorker(task: HookWorkerTask): Promise<HookWorkerResult> {
  if (process.env['REGISTRY_BOT_ENABLE_JS_HOOKS'] !== 'true') {
    return { found: false, value: undefined, logs: [] };
  }

  disableProcessExitAndKill();

  const { log, logs } = makeCapturingLog();

  const { api, restore } = installSafeFetch(task.allowedHosts ?? [], task.secrets ?? {});

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

    argsObj.config = withHookGetSecret(argsObj.config, task.secrets ?? {});

    const fn = fnValue as (a: unknown) => unknown;
    const ret = await fn(argsObj);

    // Return worker-side form changes to the main thread.
    if (task.fn === 'beforeValidate') {
      const formVal = argsObj.form;
      if (isPlainObject(formVal)) {
        return { found: true, value: { form: formVal }, logs };
      }
    }

    return { found: true, value: ret, logs };
  } catch (e: unknown) {
    const msg = getErrorMessage(e);
    logs.push({ level: 'error', obj: { err: msg }, msg: 'hook-worker:failed' });
    return { found: true, value: { __hookError: msg }, logs };
  } finally {
    // Reused workers must not retain another task's allowlist.
    restore();
  }
}
