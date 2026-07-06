import type { Probot } from 'probot';
import type { RequestEventHandler } from './types.js';
import { dispatchWebhookHandler } from './webhook-dispatcher.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueUserLikeBase = {
  login?: string | null;
};

type IssueLikeBase = {
  number: number;
  body?: string | null;
  labels?: unknown;
  user?: IssueUserLikeBase | null;
};

type CommentLikeBase = {
  body?: string | null;
  user: {
    login: string;
  };
};

type SenderLikeBase = {
  type?: string | null;
  login?: string | null;
};

export type IssueCommentEventDependencies<
  ContextType extends {
    payload: unknown;
    name: string;
    issue: () => IssueParamsType;
  },
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  SenderType extends SenderLikeBase,
  PullRequestType,
  TemplateType,
  FormDataType,
> = {
  getStaticConfig: (context: ContextType) => Promise<unknown>;

  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  isBotSender: (sender: SenderType | null | undefined) => boolean;
  hasIssueFormInputs: (issue: IssueType) => boolean;
  isJestWorker: () => boolean;

  stripQuoteAndCode: (body: unknown) => string;
  isApprovalCommentForContext: (context: ContextType, strippedText: string) => boolean;
  isAuthorUpdateComment: (body: unknown) => boolean;

  readFreshPullRequest: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    prNumber: number
  ) => Promise<PullRequestType | null>;

  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo: RepoInfoBase) => number | null;

  handleDirectPrApprovalComment: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    pr: PullRequestType,
    commenter: string
  ) => Promise<void>;

  loadTemplateWithLabelRefresh: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType
  ) => Promise<TemplateType>;

  readIssueBodyForProcessing: (issueBody: unknown) => string;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  isRequestIssue: (context: ContextType, template: TemplateType, parsedFormData: FormDataType) => boolean;

  handleParentOwnerApprovalIfNeeded: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ) => Promise<boolean>;

  handleSystemContactOwnerApprovalIfNeeded: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ) => Promise<boolean>;

  handleApprovalComment: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ) => Promise<void>;

  handleAuthorUpdateComment: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ) => Promise<void>;

  log: (context: ContextType, level: 'debug' | 'error', obj: unknown, msg: string) => void;

  isDebugEnabled: boolean;
};

export function createIssueCommentEventHandler<
  ContextType extends {
    payload: unknown;
    name: string;
    issue: () => IssueParamsType;
  },
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  CommentType extends CommentLikeBase,
  SenderType extends SenderLikeBase,
  PullRequestType,
  TemplateType,
  FormDataType,
>(
  dependencies: IssueCommentEventDependencies<
    ContextType,
    IssueParamsType,
    IssueType,
    SenderType,
    PullRequestType,
    TemplateType,
    FormDataType
  >
): RequestEventHandler<ContextType> {
  return async function handleIssueComment(context: ContextType): Promise<void> {
    await dependencies.getStaticConfig(context);

    const payload = context.payload as Record<string, unknown>;

    const issue = payload['issue'] as IssueType;
    const comment = payload['comment'] as CommentType;
    const sender = payload['sender'] as SenderType | undefined;

    const commenter = String(comment?.user?.login || '');

    if (dependencies.isDebugEnabled) {
      dependencies.log(
        context,
        'debug',
        {
          event: context.name,
          action: payload['action'],
          issue: issue?.number,
          commenter,
        },
        'requestHandler:issue-comment-event'
      );
    }

    if (dependencies.isBotSender(sender)) return;

    const params = context.issue();
    const issueNumber = params.issue_number;
    const repoInfo: RepoInfoBase = { owner: params.owner, repo: params.repo };

    const stripped = dependencies.stripQuoteAndCode(comment.body || '');
    const isApproval = dependencies.isApprovalCommentForContext(context, stripped);

    if (!dependencies.isJestWorker() && !dependencies.hasIssueFormInputs(issue)) {
      const isPullRequestConversation = dependencies.isPlainObject(
        (issue as unknown as Record<string, unknown>)['pull_request']
      );

      if (isPullRequestConversation && isApproval) {
        const pr = await dependencies.readFreshPullRequest(context, repoInfo, issueNumber);

        if (pr && dependencies.parseLinkedIssueNumberFromPr(pr, repoInfo) === null) {
          await dependencies.handleDirectPrApprovalComment(context, repoInfo, pr, commenter);
        }
      }

      return;
    }

    let template: TemplateType;

    try {
      template = await dependencies.loadTemplateWithLabelRefresh(context, params, issue);
    } catch (e: unknown) {
      dependencies.log(
        context,
        'error',
        {
          err: e instanceof Error ? e.message : String(e),
          owner: params.owner,
          repo: params.repo,
          issue: issue?.number,
        },
        'Error loading template in issue_comment handler'
      );
      return;
    }

    const emptyFormData = Object.create(null) as FormDataType;
    const parsedFormData = template
      ? dependencies.parseForm(dependencies.readIssueBodyForProcessing(issue.body), template)
      : emptyFormData;

    if (!dependencies.isRequestIssue(context, template, parsedFormData)) {
      if (dependencies.isDebugEnabled) {
        dependencies.log(
          context,
          'debug',
          {
            issue: issue.number,
            parsedKeys: Object.keys(parsedFormData || {}),
          },
          'requestHandler:issue-comment-event skipped (not a request issue)'
        );
      }

      return;
    }

    if (isApproval) {
      const handled = await dependencies.handleParentOwnerApprovalIfNeeded(
        context,
        params,
        issue,
        template,
        parsedFormData,
        commenter
      );

      if (handled) return;

      const contactHandled = await dependencies.handleSystemContactOwnerApprovalIfNeeded(
        context,
        params,
        issue,
        template,
        parsedFormData,
        commenter
      );

      if (contactHandled) return;

      await dependencies.handleApprovalComment(context, params, issue, template, parsedFormData, commenter);
      return;
    }

    if (comment.user.login === issue.user?.login) {
      const saysUpdated = dependencies.isAuthorUpdateComment(comment.body);
      if (!saysUpdated) return;

      await dependencies.handleAuthorUpdateComment(context, params, issue, template, parsedFormData);
    }
  };
}

export function registerIssueCommentEvents<IssueCommentContext>(
  app: Probot,
  handler: RequestEventHandler<IssueCommentContext>
): void {
  app.on(['issue_comment.created', 'issue_comment.edited'], async (context): Promise<void> => {
    await dispatchWebhookHandler(context as IssueCommentContext, handler, { eventFamily: 'issue_comment' });
  });
}
