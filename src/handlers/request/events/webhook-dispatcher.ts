import type { RequestEventHandler } from './types.js';

type LoggerLike = {
  debug?: (obj: unknown, msg?: string) => void;
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
};

type WebhookContextLike = {
  id?: string;
  name?: string;
  payload?: unknown;
  log?: LoggerLike;
};

type WebhookTaskMeta = {
  eventFamily: string;
  eventName: string;
  action: string;
  deliveryId: string;
  owner: string | undefined;
  repo: string | undefined;
};

type QueuedWebhookTask = {
  meta: WebhookTaskMeta;
  log?: LoggerLike;
  run: () => Promise<void>;
};

const DEFAULT_WEBHOOK_ASYNC_CONCURRENCY = 2;

const queue: QueuedWebhookTask[] = [];
let active = 0;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (isPlainObject(error) && typeof error['message'] === 'string') return String(error['message']);
  return String(error);
}

function readAsyncModeEnabled(): boolean {
  const raw = toStringTrim(process.env['REQUEST_WEBHOOK_ASYNC']).toLowerCase();

  if (raw === '0' || raw === 'false' || raw === 'off' || raw === 'sync') return false;
  if (raw === '1' || raw === 'true' || raw === 'on' || raw === 'async') return true;

  if (process.env['NODE_ENV'] === 'test' || process.env['JEST_WORKER_ID']) return false;

  return true;
}

function readConcurrency(): number {
  const raw = Number(process.env['REQUEST_WEBHOOK_ASYNC_CONCURRENCY'] || '');
  if (Number.isFinite(raw) && raw >= 1) return Math.floor(raw);
  return DEFAULT_WEBHOOK_ASYNC_CONCURRENCY;
}

function readMeta(context: WebhookContextLike, eventFamily: string): WebhookTaskMeta {
  const payload = context.payload;
  const payloadObj = isPlainObject(payload) ? payload : {};

  const repository = isPlainObject(payloadObj['repository']) ? payloadObj['repository'] : {};
  const repo = toStringTrim(repository['name']) || undefined;

  const ownerObj = isPlainObject(repository['owner']) ? repository['owner'] : {};
  const owner = toStringTrim(ownerObj['login']) || undefined;

  return {
    eventFamily,
    eventName: toStringTrim(context.name) || eventFamily,
    action: toStringTrim(payloadObj['action']) || '',
    deliveryId: toStringTrim(context.id),
    owner,
    repo,
  };
}

function enqueueWebhookTask(task: QueuedWebhookTask): void {
  queue.push(task);
  drainWebhookQueue();
}

function drainWebhookQueue(): void {
  const concurrency = readConcurrency();

  while (active < concurrency && queue.length > 0) {
    const task = queue.shift();
    if (!task) return;

    active += 1;

    setImmediate(() => {
      const startedAt = Date.now();

      void task
        .run()
        .then(() => {
          const durationMs = Date.now() - startedAt;

          task.log?.debug?.(
            {
              ...task.meta,
              durationMs,
              active,
              queued: queue.length,
            },
            'webhook:async-handler-completed'
          );
        })
        .catch((error: unknown) => {
          const durationMs = Date.now() - startedAt;

          task.log?.error?.(
            {
              ...task.meta,
              err: getErrorMessage(error),
              durationMs,
              active,
              queued: queue.length,
            },
            'webhook:async-handler-failed'
          );
        })
        .finally(() => {
          active -= 1;
          drainWebhookQueue();
        });
    });
  }
}

export async function dispatchWebhookHandler<ContextType>(
  context: ContextType,
  handler: RequestEventHandler<ContextType>,
  options: { eventFamily: string }
): Promise<void> {
  if (!readAsyncModeEnabled()) {
    await handler(context);
    return;
  }

  const contextLike = context as WebhookContextLike;
  const meta = readMeta(contextLike, options.eventFamily);

  enqueueWebhookTask({
    meta,
    log: contextLike.log,
    run: async (): Promise<void> => {
      await handler(context);
    },
  });
}
