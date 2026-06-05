import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type IssueUserLikeBase = {
  login?: string | null;
};

type IssueLikeBase = {
  number?: number;
  id?: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: unknown;
  user?: IssueUserLikeBase | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type IssueLifecycleEventDependencies<
  ContextType extends { payload: unknown; issue: () => IssueParamsType },
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  SenderType,
> = {
  getStaticConfig: (context: ContextType) => Promise<unknown>;
  shouldSkipIssueEditedEvent: (context: ContextType) => boolean;
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  toStringTrim: (value: unknown) => string;
  isBotSender: (sender: SenderType) => boolean;
  toLabelNames: (labels: IssueType['labels']) => string[];
  processIssueEvent: (context: ContextType, params: IssueParamsType, issue: IssueType) => Promise<void>;
  log: (context: ContextType, level: 'debug', obj: unknown, msg: string) => void;
  isDebugEnabled: boolean;
};

export function createIssueLifecycleEventHandler<
  ContextType extends { payload: unknown; issue: () => IssueParamsType },
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  SenderType,
>(
  dependencies: IssueLifecycleEventDependencies<ContextType, IssueParamsType, IssueType, SenderType>
): RequestEventHandler<ContextType> {
  return async function handleIssueLifecycle(context: ContextType): Promise<void> {
    await dependencies.getStaticConfig(context);

    if (dependencies.shouldSkipIssueEditedEvent(context)) return;

    const payload = context.payload;
    const payloadObj = dependencies.isPlainObject(payload) ? payload : {};

    const sender = payloadObj['sender'] as SenderType;
    const action = dependencies.toStringTrim(payloadObj['action']).toLowerCase();

    if (action === 'edited' && dependencies.isBotSender(sender)) return;

    const issue = payloadObj['issue'] as IssueType;

    if (dependencies.isDebugEnabled) {
      const safeLabels = dependencies.toLabelNames(issue?.labels);
      let changesKeys: string[] = [];

      const changes = payloadObj['changes'];
      if (dependencies.isPlainObject(changes)) {
        changesKeys = Object.keys(changes);
      }

      dependencies.log(
        context,
        'debug',
        {
          action: payloadObj['action'],
          issueNumber: issue?.number,
          issueId: issue?.id,
          title: issue?.title,
          state: issue?.state,
          user: issue?.user?.login,
          created_at: issue?.created_at,
          updated_at: issue?.updated_at,
          labels: safeLabels,
          bodyLen: String(issue?.body || '').length,
          bodyHead: String(issue?.body || '').slice(0, 300),
          changesKeys,
        },
        'dbg:issues:payload.issue'
      );
    }

    const params = context.issue();
    await dependencies.processIssueEvent(context, params, issue);
  };
}

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
