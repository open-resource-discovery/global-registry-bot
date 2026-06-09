import type { Probot } from 'probot';
import { registerCheckEvents } from './checks.js';
import { registerIssueCommentEvents } from './issue-comments.js';
import { registerIssueEvents } from './issues.js';
import { registerPullRequestEvents } from './pull-requests.js';
import { registerPushEvents } from './push.js';
import { registerStatusEvents } from './status.js';
import type { RequestEventHandler } from './types.js';

export type RequestEventRegistrationHandlers<
  PushContext,
  PullRequestContext,
  CheckContext,
  StatusContext,
  IssueLifecycleContext,
  IssueClosedContext,
  IssueLabelContext,
  IssueCommentContext,
> = {
  handlePush: RequestEventHandler<PushContext>;
  handlePullRequest: RequestEventHandler<PullRequestContext>;
  handleCheck: RequestEventHandler<CheckContext>;
  handleStatus: RequestEventHandler<StatusContext>;
  handleIssueLifecycle: RequestEventHandler<IssueLifecycleContext>;
  handleIssueClosed: RequestEventHandler<IssueClosedContext>;
  handleIssueLabelChange: RequestEventHandler<IssueLabelContext>;
  handleIssueComment: RequestEventHandler<IssueCommentContext>;
};

export function registerRequestEvents<
  PushContext,
  PullRequestContext,
  CheckContext,
  StatusContext,
  IssueLifecycleContext,
  IssueClosedContext,
  IssueLabelContext,
  IssueCommentContext,
>(
  app: Probot,
  handlers: RequestEventRegistrationHandlers<
    PushContext,
    PullRequestContext,
    CheckContext,
    StatusContext,
    IssueLifecycleContext,
    IssueClosedContext,
    IssueLabelContext,
    IssueCommentContext
  >
): void {
  registerPushEvents(app, handlers.handlePush);
  registerPullRequestEvents(app, handlers.handlePullRequest);
  registerCheckEvents(app, handlers.handleCheck);
  registerStatusEvents(app, handlers.handleStatus);

  registerIssueEvents(app, {
    handleIssueLifecycle: handlers.handleIssueLifecycle,
    handleIssueClosed: handlers.handleIssueClosed,
    handleIssueLabelChange: handlers.handleIssueLabelChange,
  });

  registerIssueCommentEvents(app, handlers.handleIssueComment);
}
