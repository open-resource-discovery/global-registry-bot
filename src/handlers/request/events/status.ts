import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';
import { dispatchWebhookHandler } from './webhook-dispatcher.js';

export type StatusEventDependencies<ContextType, RepoInfoType> = {
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  toStringTrim: (value: unknown) => string;
  toRepoInfo: (owner: string, repo: string) => RepoInfoType;
  tryAutoMerge: (context: ContextType, repoInfo: RepoInfoType, headSha: string) => Promise<void>;
};

export function createStatusEventHandler<ContextType extends { payload: unknown }, RepoInfoType>(
  dependencies: StatusEventDependencies<ContextType, RepoInfoType>
): RequestEventHandler<ContextType> {
  return async function handleStatus(context: ContextType): Promise<void> {
    const payload = context.payload;
    const state = dependencies.isPlainObject(payload) ? dependencies.toStringTrim(payload['state']) : '';
    if (state !== 'success') return;

    const repoObj = dependencies.isPlainObject(payload) ? payload['repository'] : undefined;
    const repoName = dependencies.isPlainObject(repoObj) ? dependencies.toStringTrim(repoObj['name']) : '';
    const ownerObj = dependencies.isPlainObject(repoObj) ? repoObj['owner'] : undefined;
    const ownerLogin = dependencies.isPlainObject(ownerObj) ? dependencies.toStringTrim(ownerObj['login']) : '';

    const sha = dependencies.isPlainObject(payload) ? dependencies.toStringTrim(payload['sha']) : '';
    if (!ownerLogin || !repoName || !sha) return;

    await dependencies.tryAutoMerge(context, dependencies.toRepoInfo(ownerLogin, repoName), sha);
  };
}

export function registerStatusEvents<StatusContext>(app: Probot, handler: RequestEventHandler<StatusContext>): void {
  app.on('status', async (context): Promise<void> => {
    await dispatchWebhookHandler(context as StatusContext, handler, { eventFamily: 'status' });
  });
}
