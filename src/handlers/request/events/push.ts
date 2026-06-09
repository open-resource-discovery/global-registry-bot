import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';
import { dispatchWebhookHandler } from './webhook-dispatcher.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type DefaultBranchDirectPrResultBase = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type StaticConfigOptions = {
  forceReload?: boolean;
};

export type PushEventDependencies<
  ContextType extends { payload: unknown },
  RepoInfoType extends RepoInfoBase,
  DefaultBranchDirectPrResultType extends DefaultBranchDirectPrResultBase,
> = {
  readRepoInfoFromPayload: (payload: unknown) => RepoInfoType | null;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  toStringTrim: (value: unknown) => string;
  readDefaultBranchFromPush: (payload: unknown) => string;
  readPushChangedFiles: (payload: unknown) => string[];
  isApprovalConfigChangePath: (filePath: string) => boolean;
  isDefaultBranchPush: (payload: unknown) => boolean;
  getStaticConfig: (context: ContextType, options: StaticConfigOptions) => Promise<unknown>;
  reevaluateOpenDirectPullRequestsAfterDefaultBranchPush: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string,
    reason: string
  ) => Promise<DefaultBranchDirectPrResultType>;
  updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry: (
    context: ContextType,
    repoInfo: RepoInfoType,
    baseBranch: string
  ) => Promise<boolean>;
  log: (context: ContextType, level: 'info' | 'warn', obj: unknown, msg: string) => void;
};

export function createPushEventHandler<
  ContextType extends { payload: unknown },
  RepoInfoType extends RepoInfoBase,
  DefaultBranchDirectPrResultType extends DefaultBranchDirectPrResultBase,
>(
  dependencies: PushEventDependencies<ContextType, RepoInfoType, DefaultBranchDirectPrResultType>
): RequestEventHandler<ContextType> {
  return async function handlePush(context: ContextType): Promise<void> {
    const payload = context.payload;
    const repoInfo = dependencies.readRepoInfoFromPayload(payload);
    const ref = dependencies.isPlainObject(payload) ? dependencies.toStringTrim(payload['ref']) : '';
    const baseBranch = dependencies.readDefaultBranchFromPush(payload);
    const changedFiles = dependencies.readPushChangedFiles(payload);
    const approvalConfigChangedFiles = changedFiles.filter(dependencies.isApprovalConfigChangePath);
    const defaultBranchPush = dependencies.isDefaultBranchPush(payload);

    dependencies.log(
      context,
      'info',
      {
        event: dependencies.toStringTrim((context as unknown as { name?: string }).name),
        ref,
        defaultBranch: baseBranch,
        isDefaultBranchPush: defaultBranchPush,
        owner: repoInfo?.owner,
        repo: repoInfo?.repo,
        changedFilesCount: changedFiles.length,
        approvalConfigChangedFiles,
      },
      'default-branch-push:received'
    );

    if (!defaultBranchPush) return;

    if (!repoInfo) {
      dependencies.log(
        context,
        'warn',
        {
          ref,
          defaultBranch: baseBranch,
        },
        'default-branch-push:missing-repo-info'
      );
      return;
    }

    await dependencies.getStaticConfig(context, { forceReload: true });

    const directPrReevaluationReason = approvalConfigChangedFiles.length
      ? 'default-branch-push:approval-config-change'
      : 'default-branch-push:direct-pr-reevaluation';

    const directResult = await dependencies.reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
      context,
      repoInfo,
      baseBranch,
      directPrReevaluationReason
    );

    if (!directResult.updated && !directResult.processed && !directResult.blockedByActive) {
      await dependencies.updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(
        context,
        repoInfo,
        baseBranch
      );
    }
  };
}

export function registerPushEvents<PushContext>(app: Probot, handler: RequestEventHandler<PushContext>): void {
  app.on('push', async (context): Promise<void> => {
    await dispatchWebhookHandler(context as PushContext, handler, { eventFamily: 'push' });
  });
}
