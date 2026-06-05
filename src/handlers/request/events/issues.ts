import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';

export type IssueEventHandlers<IssueLifecycleContext, IssueClosedContext, IssueLabelContext> = {
  handleIssueLifecycle: RequestEventHandler<IssueLifecycleContext>;
  handleIssueClosed: RequestEventHandler<IssueClosedContext>;
  handleIssueLabelChange: RequestEventHandler<IssueLabelContext>;
};

export function registerIssueEvents<IssueLifecycleContext, IssueClosedContext, IssueLabelContext>(
  app: Probot,
  handlers: IssueEventHandlers<IssueLifecycleContext, IssueClosedContext, IssueLabelContext>
): void {
  app.on(['issues.opened', 'issues.edited', 'issues.reopened'], async (context): Promise<void> => {
    await handlers.handleIssueLifecycle(context as IssueLifecycleContext);
  });

  app.on('issues.closed', async (context): Promise<void> => {
    await handlers.handleIssueClosed(context as IssueClosedContext);
  });

  app.on(['issues.labeled', 'issues.unlabeled'], async (context): Promise<void> => {
    await handlers.handleIssueLabelChange(context as IssueLabelContext);
  });
}
