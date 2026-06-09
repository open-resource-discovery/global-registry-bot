import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';
import { dispatchWebhookHandler } from './webhook-dispatcher.js';

export type CheckEventDependencies<ContextType> = {
  handleCheckCompletedEvent: (context: ContextType, payload: unknown, eventName: string) => Promise<void>;
  toStringTrim: (value: unknown) => string;
};

export function createCheckEventHandler<ContextType extends { payload: unknown }>(
  dependencies: CheckEventDependencies<ContextType>
): RequestEventHandler<ContextType> {
  return async function handleCheck(context: ContextType): Promise<void> {
    const payload = context.payload;
    const eventName = dependencies.toStringTrim((context as unknown as { name?: string }).name);

    await dependencies.handleCheckCompletedEvent(context, payload, eventName);
  };
}

export function registerCheckEvents<CheckContext>(app: Probot, handler: RequestEventHandler<CheckContext>): void {
  app.on(['check_suite.completed', 'check_run.completed'], async (context): Promise<void> => {
    await dispatchWebhookHandler(context as CheckContext, handler, { eventFamily: 'checks' });
  });
}
