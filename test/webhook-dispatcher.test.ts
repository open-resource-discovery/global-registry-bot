import { jest } from '@jest/globals';
import { dispatchWebhookHandler } from '../src/handlers/request/events/webhook-dispatcher.js';
import type { RequestEventHandler } from '../src/handlers/request/events/types.js';

type TestContext = {
  id: string;
  name: string;
  payload: Record<string, unknown>;
  log: {
    debug: jest.Mock;
    info: jest.Mock;
    warn: jest.Mock;
    error: jest.Mock;
  };
};

function makeContext(overrides: Partial<TestContext> = {}): TestContext {
  return {
    id: 'delivery-1',
    name: 'check_suite.completed',
    payload: { repository: { name: 'my-repo', owner: { login: 'my-org' } }, action: 'completed' },
    log: {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
    ...overrides,
  };
}

function flushSetImmediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('dispatchWebhookHandler', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  test('runs handler synchronously inside Jest (NODE_ENV=test is set by default)', async () => {
    const calls: string[] = [];
    const handler: RequestEventHandler<TestContext> = (_ctx): Promise<void> => {
      calls.push('handler-called');
      return Promise.resolve();
    };

    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'checks' });

    expect(calls).toEqual(['handler-called']);
  });

  test('runs handler synchronously when REQUEST_WEBHOOK_ASYNC=0', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '0';
    delete process.env['JEST_WORKER_ID'];

    const calls: string[] = [];
    const handler: RequestEventHandler<TestContext> = (_ctx): Promise<void> => {
      calls.push('handler-called');
      return Promise.resolve();
    };

    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'checks' });

    expect(calls).toEqual(['handler-called']);
  });

  test('returns before handler runs when REQUEST_WEBHOOK_ASYNC=1', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';

    const handler = jest.fn(async (_ctx: TestContext) => {
      /* intentionally empty */
    });

    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'checks' });

    expect(handler).not.toHaveBeenCalled();

    await flushSetImmediate();

    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('logs webhook:async-handler-failed and does not rethrow when async handler throws', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';

    const boom = new Error('boom');
    const handler: RequestEventHandler<TestContext> = (_ctx): Promise<void> => Promise.reject(boom);

    const context = makeContext();
    await expect(dispatchWebhookHandler(context, handler, { eventFamily: 'checks' })).resolves.toBeUndefined();

    await flushSetImmediate();

    expect(context.log.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: 'boom',
        eventFamily: 'checks',
        deliveryId: 'delivery-1',
        owner: 'my-org',
        repo: 'my-repo',
        active: 0,
        queued: 0,
      }),
      'webhook:async-handler-failed'
    );
  });

  test('propagates error to caller in synchronous mode', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '0';
    delete process.env['JEST_WORKER_ID'];

    const handler: RequestEventHandler<TestContext> = (_ctx): Promise<void> => Promise.reject(new Error('sync-boom'));

    await expect(dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'push' })).rejects.toThrow('sync-boom');
  });

  test('logs webhook:async-handler-completed on success when REQUEST_WEBHOOK_ASYNC=1', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';

    const handler: RequestEventHandler<TestContext> = async (_ctx) => {
      /* intentionally empty */
    };

    const context = makeContext();
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();

    expect(context.log.debug).toHaveBeenCalledWith(
      expect.objectContaining({
        eventFamily: 'push',
        deliveryId: 'delivery-1',
        owner: 'my-org',
        repo: 'my-repo',
        active: 0,
        queued: 0,
      }),
      'webhook:async-handler-completed'
    );
  });

  test('readMeta: null payload (non-plain-object) uses fallback + numeric id covers toStringTrim number path (L43/L44/L73)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext({
      id: 42 as unknown as string,
      name: '',
      payload: null as unknown as Record<string, unknown>,
    });
    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('readMeta: boolean action covers toStringTrim boolean path (L44 boolean arm)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext({
      payload: { action: true as unknown as string, repository: { name: 'r', owner: { login: 'org' } } },
    });
    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('readMeta: object id covers toStringTrim object path → returns empty string (L44 false → L45)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext({
      id: {} as unknown as string,
      payload: { repository: { name: '', owner: {} as unknown as { login: string } } },
    });
    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('readMeta: non-object repository falls back to empty + non-object owner (L75/L78 cond-expr false)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext({
      payload: { repository: 'not-an-object' as unknown as Record<string, unknown>, action: 'done' },
    });
    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  test('getErrorMessage: plain object with string message (L49 false → L50 true)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext();
    const handler = (): Promise<void> => Promise.reject({ message: 'plain-err' });
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(context.log.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'plain-err' }),
      'webhook:async-handler-failed'
    );
  });

  test('getErrorMessage: plain object without string message falls through to String() (L50 binary-expr second false)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext();
    const handler = (): Promise<void> => Promise.reject({ code: 42 });
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(context.log.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: '[object Object]' }),
      'webhook:async-handler-failed'
    );
  });

  test('getErrorMessage: non-Error non-object (string) falls through to String() (L50 false → L51)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    const context = makeContext();
    const handler = (): Promise<void> => Promise.reject('string-error');
    await dispatchWebhookHandler(context, handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(context.log.error).toHaveBeenCalledWith(
      expect.objectContaining({ err: 'string-error' }),
      'webhook:async-handler-failed'
    );
  });

  test('readAsyncModeEnabled: returns true when unrecognized value and not test NODE_ENV (L60 false → L62)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = 'maybe';
    const savedNodeEnv = process.env['NODE_ENV'];
    const savedWorker = process.env['JEST_WORKER_ID'];
    delete process.env['NODE_ENV'];
    delete process.env['JEST_WORKER_ID'];

    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'push' });

    expect(handler).not.toHaveBeenCalled();
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);

    process.env['NODE_ENV'] = savedNodeEnv ?? 'test';
    if (savedWorker !== undefined) process.env['JEST_WORKER_ID'] = savedWorker;
  });

  test('readConcurrency: falls back to default when concurrency env is 0 (L67 false)', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    process.env['REQUEST_WEBHOOK_ASYNC_CONCURRENCY'] = '0';

    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);

    delete process.env['REQUEST_WEBHOOK_ASYNC_CONCURRENCY'];
  });

  test('readConcurrency: L67 true arm — valid positive concurrency env is used', async () => {
    process.env['REQUEST_WEBHOOK_ASYNC'] = '1';
    process.env['REQUEST_WEBHOOK_ASYNC_CONCURRENCY'] = '5';

    const handler = jest.fn(async () => {});
    await dispatchWebhookHandler(makeContext(), handler, { eventFamily: 'push' });
    await flushSetImmediate();
    expect(handler).toHaveBeenCalledTimes(1);

    delete process.env['REQUEST_WEBHOOK_ASYNC_CONCURRENCY'];
  });
});
