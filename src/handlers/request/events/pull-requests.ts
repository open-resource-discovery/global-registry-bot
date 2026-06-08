import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';

export type PullRequestEventDependencies<ContextType, RepoInfoType, PullRequestType> = {
  getStaticConfig: (context: ContextType) => Promise<unknown>;
  readRepoInfoFromPayload: (payload: unknown) => RepoInfoType | null;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  isPullRequestOpen: (pullRequest: PullRequestType) => boolean;
  maybeApprovePendingWorkflowRunsForRegistryPrWithRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    pullRequest: PullRequestType,
    reason: string
  ) => Promise<unknown>;
  toStringTrim: (value: unknown) => string;
};

export function createPullRequestEventHandler<ContextType extends { payload: unknown }, RepoInfoType, PullRequestType>(
  dependencies: PullRequestEventDependencies<ContextType, RepoInfoType, PullRequestType>
): RequestEventHandler<ContextType> {
  return async function handlePullRequest(context: ContextType): Promise<void> {
    await dependencies.getStaticConfig(context);

    const payload = context.payload;
    const repoInfo = dependencies.readRepoInfoFromPayload(payload);
    if (!repoInfo) return;

    const prRaw = dependencies.isPlainObject(payload) ? payload['pull_request'] : null;
    if (!dependencies.isPlainObject(prRaw)) return;

    const pr = prRaw as PullRequestType;
    if (!dependencies.isPullRequestOpen(pr)) return;

    await dependencies.maybeApprovePendingWorkflowRunsForRegistryPrWithRetry(
      context,
      repoInfo,
      pr,
      `pull-request:${dependencies.toStringTrim((payload as Record<string, unknown>)['action']) || 'event'}`
    );
  };
}

export function registerPullRequestEvents<PullRequestContext>(
  app: Probot,
  handler: RequestEventHandler<PullRequestContext>
): void {
  app.on(
    ['pull_request.opened', 'pull_request.synchronize', 'pull_request.reopened', 'pull_request.ready_for_review'],
    async (context): Promise<void> => {
      await handler(context as PullRequestContext);
    }
  );
}
