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
});
