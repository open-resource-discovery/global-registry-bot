import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';

export function registerPushEvents<PushContext>(app: Probot, handler: RequestEventHandler<PushContext>): void {
  app.on('push', async (context): Promise<void> => {
    await handler(context as PushContext);
  });
}
