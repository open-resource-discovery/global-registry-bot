import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';

export function registerIssueCommentEvents<IssueCommentContext>(
  app: Probot,
  handler: RequestEventHandler<IssueCommentContext>
): void {
  app.on(['issue_comment.created', 'issue_comment.edited'], async (context): Promise<void> => {
    await handler(context as IssueCommentContext);
  });
}
