import { Piscina } from 'piscina';
import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

let pool: Piscina | null = null;

const defaultModuleFileName = resolve(process.cwd(), 'dist', 'handlers', 'request', 'validation', 'hook-pool.js');
const moduleFileName = typeof __filename === 'string' ? __filename : defaultModuleFileName;

function resolveWorkerFilePath(): string | null {
  const candidates = [
    resolve(process.cwd(), 'dist', 'handlers', 'request', 'validation', 'hook-worker.js'),
    resolve(dirname(moduleFileName), 'hook-worker.js'),
    resolve(process.cwd(), 'src', 'handlers', 'request', 'validation', 'hook-worker.js'),
  ];

  return candidates.find((candidate) => existsSync(candidate)) ?? null;
}

function getPool(): Piscina {
  if (pool) return pool;

  const workerFilePath = resolveWorkerFilePath();
  if (!workerFilePath) {
    throw new Error('Hook worker runtime artifact not found');
  }

  pool = new Piscina({
    filename: pathToFileURL(workerFilePath).href,

    // Critical for CF memory quotas:
    minThreads: 1,
    maxThreads: 1,

    // Avoid churn / repeated spawn on low traffic
    idleTimeout: 30_000,

    // Prevent unbounded queue memory growth
    maxQueue: 25,

    // Our hooks are not CPU-parallelizable, keep it 1
    concurrentTasksPerWorker: 1,

    // Optional hard cap per worker heap
    resourceLimits: {
      maxOldGenerationSizeMb: 64,
      // stackSizeMb default is 4
    },
  });

  return pool;
}

export type HookWorkerTask = {
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

export type HookWorkerResult = {
  found: boolean;
  value: unknown;
  logs: { level: 'debug' | 'info' | 'warn' | 'error'; obj: unknown; msg?: string }[];
};

export async function runHookInWorker(task: HookWorkerTask, opts: { timeoutMs: number }): Promise<HookWorkerResult> {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), Math.max(1, opts.timeoutMs));

  try {
    const p = getPool();

    const runOpts: { abortSignal: AbortSignal; signal: AbortSignal } = {
      abortSignal: ac.signal,
      signal: ac.signal,
    };

    const res = await p.run(task, runOpts);
    return (res ?? { found: false, value: undefined, logs: [] }) as HookWorkerResult;
  } catch (err: unknown) {
    // Never throw out of hook runner. Convert timeouts/aborts into hook error payload.
    const e = err as { name?: unknown; message?: unknown };
    const name = typeof e?.name === 'string' ? e.name : '';
    const msgRaw = err instanceof Error ? err.message : String(err);

    const msg =
      name === 'AbortError'
        ? `Hook timed out after ${Math.max(1, opts.timeoutMs)}ms`
        : msgRaw || 'Hook execution failed';

    return { found: true, value: { __hookError: msg }, logs: [] };
  } finally {
    clearTimeout(timer);
  }
}
