import type { Probot } from 'probot';
import { type LogLevel } from '../infrastructure/logger.js';
import { getHttpStatus } from '../infrastructure/errors.js';
import {
  createGitHubIssueUpdateGateway,
  type GitHubIssueUpdateGatewayContext,
} from '../infrastructure/github-gateway.js';
import { readIssueBodyForProcessing } from '../domain/issue-body-processing.js';
import { toStringTrim } from '../domain/login-utils.js';
import { handoverToCpa } from '../application/review-handover.js';
import {
  maybeHandleApprovalDecision as maybeHandleApprovalDecisionApplication,
  type ApprovalHandlingResult,
} from '../application/approval-decision-dispatch.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';
import {
  processAuthorUpdateComment as processAuthorUpdateCommentApplication,
  processRequestIssueLifecycle as processRequestIssueLifecycleApplication,
  type RequestIssueAuthorUpdateCallbacks,
  type RequestIssueLifecycleCallbacks,
} from '../application/request-issue-lifecycle.js';
import {
  closeOutdatedRequestPrs as closeOutdatedRequestPrsApplication,
  type OutdatedRequestPrCleanupCallbacks,
} from '../application/outdated-request-pr-cleanup.js';
import {
  createRequestPrWithRecovery as createRequestPrWithRecoveryApplication,
  type RequestPrCreationRecoveryCallbacks,
} from '../application/request-pr-creation-recovery.js';
import {
  composeRequestIssueAuthorUpdateCallbacks,
  composeRequestIssueLifecycleCallbacks,
  composeRequestPrCreationRecoveryCallbacks,
} from './issue-lifecycle-composition.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: unknown;
  user?: UserLikeBase | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type PullRequestLikeBase = {
  number: number;
  body?: string | null;
  head: { ref: string; sha?: string | null };
};

type EffectiveConstantsBase = {
  reviewRequestedLabels: string[];
  labelOnApproved?: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type ValidationIssueLikeBase = { path?: string | null; message?: string | null };

type ValidateRequestIssueResultBase<TemplateType> = {
  errors?: string[];
  errorsFormattedSingle?: string;
  errorsFormatted?: string;
  validationIssues?: ValidationIssueLikeBase[];
  template?: TemplateType;
  namespace: string;
  nsType: string;
};

type OctokitLikeBase<IssueType> = {
  rest: {
    issues: {
      get: (args: { owner: string; repo: string; issue_number: number }) => Promise<{ data?: IssueType }>;
      removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
    };
    pulls: {
      update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
    };
    repos: {
      getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data?: unknown }>;
    };
    git: {
      deleteRef: (args: { owner: string; repo: string; ref: string }) => Promise<unknown>;
    };
  };
};

type ContextWithOctokitBase<IssueType extends IssueLikeBase> = {
  octokit: OctokitLikeBase<IssueType>;
};

export type RequestLifecycleRuntimeDependencies<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateRequestIssueResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = {
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  isDebugEnabled: boolean;
  hasIssueFormInputs: (issue: IssueType | null | undefined) => boolean;
  loadTemplateWithLabelRefresh: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType
  ) => Promise<TemplateType>;
  buildTemplateLoadErrorMessage: (errMsg: unknown) => string;
  postOnce: (
    context: ContextType,
    params: IssueParamsBase,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  setStateLabel: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  isRequestIssue: (
    context: ContextType,
    template: TemplateType | null | undefined,
    parsedFormData: FormDataType
  ) => boolean;
  toLabelNames: (labels: unknown) => string[];
  detectSingleRoutingLabel: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    labels: string[]
  ) => Promise<string>;
  ensureRoutingLockMarker: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    expectedLabel: string
  ) => Promise<boolean>;
  enforceRoutingLabelLock: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    expectedLabel: string,
    opts?: { changedLabel?: string }
  ) => Promise<boolean>;
  removeRejectedStatusLabel: (context: ContextType, params: IssueParamsType, currentLabels?: string[]) => Promise<void>;
  buildCompatibleRequestSnapshotHashes: (
    issueBody: unknown,
    parsedFormData: FormDataType,
    template: TemplateType
  ) => string[];
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  validateRequestIssue: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    options?: { template?: TemplateType; formData?: FormDataType }
  ) => Promise<ValidateRequestIssueResultType>;
  checkParentChainExistsInFlatStructure: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    template: TemplateType,
    formData: FormDataType,
    explicitResourceName?: string
  ) => Promise<string | null>;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  getApprovedParentOwnerLogin: (issueBody: unknown, target: string) => string;
  isSubContextRequestType: (requestType: unknown) => boolean;
  extractResourceNameFromForm: (formData: FormDataType, template: TemplateType) => string;
  head: (value: unknown) => string;
  renderConfiguredRequestBranchName: (context: ContextType, issue: IssueType, resourceName: string) => string;
  createRequestPr: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    issue: IssueType,
    parsedFormData: FormDataType,
    options: { template: TemplateType }
  ) => Promise<{ number: number }>;
  extractHashFromPrBody: (body: string) => string;
  findOpenIssuePrs: (context: ContextType, repo: RepoInfoBase, issueNumber: number) => Promise<PullRequestType[]>;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  maybeRequireParentOwnerApproval: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    validatedNamespace: string,
    requestType: string
  ) => Promise<boolean>;
  maybeRequireSystemContactOwnerApproval: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    requestType: string,
    validatedNamespace: string
  ) => Promise<boolean>;
  maybeHandleApprovalDecision: typeof maybeHandleApprovalDecisionApplication;
  buildApprovalDecisionDispatchOptions: () => {
    resolveApprovalDecision: (
      dispatchContext: ContextType,
      dispatchParams: IssueParamsType,
      dispatchIssue: IssueType,
      dispatchTemplate: TemplateType,
      dispatchFormData: FormDataType,
      dispatchRequestType: string,
      dispatchNamespace: string
    ) => Promise<ApprovalDecision | boolean>;
    handleApprovedDecision: (
      dispatchContext: ContextType,
      dispatchParams: IssueParamsType,
      dispatchIssue: IssueType,
      dispatchTemplate: TemplateType,
      dispatchFormData: FormDataType,
      decision: ApprovalDecision
    ) => Promise<void>;
    handleRejectedDecision: (
      dispatchContext: ContextType,
      dispatchParams: IssueParamsType,
      dispatchIssue: IssueType,
      decision: ApprovalDecision
    ) => Promise<void>;
  };
  finalizeApprovedRequest: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    options: {
      approvalPrefix?: string;
      approvalComment?: string;
      autoApproved?: boolean;
    }
  ) => Promise<void>;
  resolveManualReviewApproverOverrideFromApprovalHook: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  resolveAdditionalIssueApproversFromApprovalHook: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  buildReviewHandoverOptions: () => Record<string, unknown>;
};

export type RequestLifecycleRuntime<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateRequestIssueResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = {
  buildRequestPrCreationRecoveryCallbacks: () => RequestPrCreationRecoveryCallbacks<
    ContextType,
    RepoInfoBase,
    IssueType,
    TemplateType,
    FormDataType
  >;
  createRequestPrWithRecovery: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    template: TemplateType,
    resourceName: string
  ) => Promise<{ number: number }>;
  buildRequestIssueLifecycleCallbacks: (
    app: Probot
  ) => RequestIssueLifecycleCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateRequestIssueResultType
  >;
  processIssueEvent: (app: Probot, context: ContextType, params: IssueParamsType, issue: IssueType) => Promise<void>;
  buildRequestIssueAuthorUpdateCallbacks: (
    app: Probot
  ) => RequestIssueAuthorUpdateCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateRequestIssueResultType
  >;
  handleAuthorUpdateComment: (
    app: Probot,
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ) => Promise<void>;
  normalizeIssueTitle: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ) => Promise<void>;
  buildOutdatedRequestPrCleanupCallbacks: () => OutdatedRequestPrCleanupCallbacks<
    ContextType,
    PullRequestType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >;
  closeOutdatedRequestPrs: (
    context: ContextType,
    params: IssueParamsType,
    template: TemplateType,
    options?: { parsedFormData?: FormDataType; currentHash?: string; acceptedHashes?: string[] }
  ) => Promise<void>;
};

export function createRequestLifecycleRuntime<
  ContextType extends ContextWithOctokitBase<IssueType>,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateRequestIssueResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  dependencies: RequestLifecycleRuntimeDependencies<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    PullRequestType,
    EffectiveConstantsType,
    ValidateRequestIssueResultType
  >
): RequestLifecycleRuntime<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  PullRequestType,
  EffectiveConstantsType,
  ValidateRequestIssueResultType
> {
  function buildRequestPrCreationRecoveryCallbacks(): RequestPrCreationRecoveryCallbacks<
    ContextType,
    RepoInfoBase,
    IssueType,
    TemplateType,
    FormDataType
  > {
    return composeRequestPrCreationRecoveryCallbacks<ContextType, RepoInfoBase, IssueType, TemplateType, FormDataType>({
      createRequestPr: async (
        context: ContextType,
        repoInfo: RepoInfoBase,
        issue: IssueType,
        parsedFormData: FormDataType,
        options: { template: TemplateType }
      ): Promise<{ number: number }> =>
        await dependencies.createRequestPr(context, repoInfo, issue, parsedFormData, options),
      getHttpStatus,
      renderConfiguredRequestBranchName: dependencies.renderConfiguredRequestBranchName,
    });
  }

  async function createRequestPrWithRecovery(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    template: TemplateType,
    resourceName: string
  ): Promise<{ number: number }> {
    return await createRequestPrWithRecoveryApplication(
      context,
      params,
      issue,
      parsedFormData,
      template,
      resourceName,
      buildRequestPrCreationRecoveryCallbacks()
    );
  }

  function buildRequestIssueLifecycleCallbacks(
    app: Probot
  ): RequestIssueLifecycleCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateRequestIssueResultType
  > {
    return composeRequestIssueLifecycleCallbacks<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      ValidateRequestIssueResultType
    >({
      isJestWorker: Boolean(process.env.JEST_WORKER_ID),
      isDebugEnabled: dependencies.isDebugEnabled,
      hasIssueFormInputs: dependencies.hasIssueFormInputs,
      loadTemplateWithLabelRefresh: dependencies.loadTemplateWithLabelRefresh,
      buildTemplateLoadErrorMessage: dependencies.buildTemplateLoadErrorMessage,
      postOnce: dependencies.postOnce,
      setStateLabel: dependencies.setStateLabel,
      parseForm: dependencies.parseForm,
      isRequestIssue: dependencies.isRequestIssue,
      log: dependencies.log,
      toLabelNames: dependencies.toLabelNames,
      detectSingleRoutingLabel: dependencies.detectSingleRoutingLabel,
      ensureRoutingLockMarker: dependencies.ensureRoutingLockMarker,
      enforceRoutingLabelLock: dependencies.enforceRoutingLabelLock,
      removeRejectedStatusLabel: dependencies.removeRejectedStatusLabel,
      buildCompatibleRequestSnapshotHashes: dependencies.buildCompatibleRequestSnapshotHashes,
      calcSnapshotHash: dependencies.calcSnapshotHash,
      normalizeIssueTitle,
      closeOutdatedRequestPrs,
      validateRequestIssue: dependencies.validateRequestIssue,
      checkParentChainExistsInFlatStructure: dependencies.checkParentChainExistsInFlatStructure,
      resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
      maybeRequireParentOwnerApproval: dependencies.maybeRequireParentOwnerApproval,
      maybeRequireSystemContactOwnerApproval: dependencies.maybeRequireSystemContactOwnerApproval,
      getApprovedParentOwnerLogin: dependencies.getApprovedParentOwnerLogin,
      isSubContextRequestType: dependencies.isSubContextRequestType,
      maybeHandleApprovalDecision: async (
        context,
        params,
        issue,
        template,
        parsedFormData,
        requestType,
        namespace,
        options
      ): Promise<ApprovalHandlingResult> =>
        await (
          dependencies.maybeHandleApprovalDecision as unknown as (
            context: ContextType,
            params: IssueParamsType,
            issue: IssueType,
            template: TemplateType,
            parsedFormData: FormDataType,
            requestType: string,
            namespace: string,
            options: ReturnType<typeof dependencies.buildApprovalDecisionDispatchOptions>
          ) => Promise<ApprovalHandlingResult>
        )(
          context,
          params,
          issue,
          template,
          parsedFormData,
          requestType,
          namespace,
          options as ReturnType<typeof dependencies.buildApprovalDecisionDispatchOptions>
        ),
      buildApprovalDecisionDispatchOptions: dependencies.buildApprovalDecisionDispatchOptions,
      finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
      resolveManualReviewApproverOverrideFromApprovalHook:
        dependencies.resolveManualReviewApproverOverrideFromApprovalHook,
      resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
      handoverToCpa: async (context, params, issue, nsType, namespace, labels, options): Promise<void> =>
        await (
          handoverToCpa as unknown as (
            context: ContextType,
            params: IssueParamsType,
            issue: IssueType,
            nsType: string,
            namespace: string,
            labels: string[],
            options: ReturnType<typeof dependencies.buildReviewHandoverOptions>
          ) => Promise<void>
        )(context, params, issue, nsType, namespace, labels, options),
      buildReviewHandoverOptions: (): Record<string, unknown> => dependencies.buildReviewHandoverOptions(),
      appLog: app.log || console,
    });
  }

  async function processIssueEvent(
    app: Probot,
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType
  ): Promise<void> {
    await processRequestIssueLifecycleApplication(context, params, issue, buildRequestIssueLifecycleCallbacks(app));
  }

  function buildRequestIssueAuthorUpdateCallbacks(
    app: Probot
  ): RequestIssueAuthorUpdateCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateRequestIssueResultType
  > {
    return composeRequestIssueAuthorUpdateCallbacks<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      ValidateRequestIssueResultType
    >({
      validateRequestIssue: dependencies.validateRequestIssue,
      parseForm: dependencies.parseForm,
      calcSnapshotHash: dependencies.calcSnapshotHash,
      checkParentChainExistsInFlatStructure: dependencies.checkParentChainExistsInFlatStructure,
      postOnce: dependencies.postOnce,
      setStateLabel: dependencies.setStateLabel,
      closeOutdatedRequestPrs,
      resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
      maybeRequireParentOwnerApproval: dependencies.maybeRequireParentOwnerApproval,
      log: dependencies.log,
      isDebugEnabled: dependencies.isDebugEnabled,
      maybeRequireSystemContactOwnerApproval: dependencies.maybeRequireSystemContactOwnerApproval,
      getApprovedParentOwnerLogin: dependencies.getApprovedParentOwnerLogin,
      isSubContextRequestType: dependencies.isSubContextRequestType,
      maybeHandleApprovalDecision: async (
        context,
        params,
        issue,
        template,
        parsedFormData,
        requestType,
        namespace,
        options
      ): Promise<ApprovalHandlingResult> =>
        await (
          dependencies.maybeHandleApprovalDecision as unknown as (
            context: ContextType,
            params: IssueParamsType,
            issue: IssueType,
            template: TemplateType,
            parsedFormData: FormDataType,
            requestType: string,
            namespace: string,
            options: ReturnType<typeof dependencies.buildApprovalDecisionDispatchOptions>
          ) => Promise<ApprovalHandlingResult>
        )(
          context,
          params,
          issue,
          template,
          parsedFormData,
          requestType,
          namespace,
          options as ReturnType<typeof dependencies.buildApprovalDecisionDispatchOptions>
        ),
      buildApprovalDecisionDispatchOptions: dependencies.buildApprovalDecisionDispatchOptions,
      finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
      resolveManualReviewApproverOverrideFromApprovalHook:
        dependencies.resolveManualReviewApproverOverrideFromApprovalHook,
      resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
      handoverToCpa: async (context, params, issue, nsType, namespace, labels, options): Promise<void> =>
        await (
          handoverToCpa as unknown as (
            context: ContextType,
            params: IssueParamsType,
            issue: IssueType,
            nsType: string,
            namespace: string,
            labels: string[],
            options: ReturnType<typeof dependencies.buildReviewHandoverOptions>
          ) => Promise<void>
        )(context, params, issue, nsType, namespace, labels, options),
      buildReviewHandoverOptions: (): Record<string, unknown> => dependencies.buildReviewHandoverOptions(),
      appLog: app.log || console,
    });
  }

  async function handleAuthorUpdateComment(
    app: Probot,
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ): Promise<void> {
    await processAuthorUpdateCommentApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      buildRequestIssueAuthorUpdateCallbacks(app)
    );
  }

  async function normalizeIssueTitle(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ): Promise<void> {
    try {
      const resourceName = dependencies.extractResourceNameFromForm(parsedFormData, template);
      const rawPrefix = toStringTrim(template?.title || template?.name || 'Request');
      const prefix = dependencies.head(rawPrefix);

      if (!prefix || !resourceName) return;

      const desiredTitle = `${prefix}: ${resourceName}`;
      if (toStringTrim(issue.title) === desiredTitle) return;

      await createGitHubIssueUpdateGateway(context as unknown as GitHubIssueUpdateGatewayContext).updateIssue({
        owner: params.owner,
        repo: params.repo,
        issue_number: params.issue_number,
        title: desiredTitle,
      });

      issue.title = desiredTitle;
    } catch (err: unknown) {
      dependencies.log(
        context,
        'warn',
        { err: err instanceof Error ? err.message : String(err) },
        'Failed to normalize issue title'
      );
    }
  }

  function buildOutdatedRequestPrCleanupCallbacks(): OutdatedRequestPrCleanupCallbacks<
    ContextType,
    PullRequestType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  > {
    return {
      parseForm: dependencies.parseForm,
      readIssueBodyForProcessing,
      buildCompatibleRequestSnapshotHashes: dependencies.buildCompatibleRequestSnapshotHashes,
      calcSnapshotHash: dependencies.calcSnapshotHash,
      extractHashFromPrBody: dependencies.extractHashFromPrBody,
      findOpenIssuePrs: dependencies.findOpenIssuePrs,
      resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
      postOnce: dependencies.postOnce,
    };
  }

  async function closeOutdatedRequestPrs(
    context: ContextType,
    params: IssueParamsType,
    template: TemplateType,
    options: { parsedFormData?: FormDataType; currentHash?: string; acceptedHashes?: string[] } = {}
  ): Promise<void> {
    await closeOutdatedRequestPrsApplication(
      context,
      params,
      template,
      options,
      buildOutdatedRequestPrCleanupCallbacks()
    );
  }

  return {
    buildRequestPrCreationRecoveryCallbacks,
    createRequestPrWithRecovery,
    buildRequestIssueLifecycleCallbacks,
    processIssueEvent,
    buildRequestIssueAuthorUpdateCallbacks,
    handleAuthorUpdateComment,
    normalizeIssueTitle,
    buildOutdatedRequestPrCleanupCallbacks,
    closeOutdatedRequestPrs,
  };
}

export type {
  ApprovalHandlingResult,
  RequestPrCreationRecoveryCallbacks,
  RequestIssueLifecycleCallbacks,
  RequestIssueAuthorUpdateCallbacks,
  OutdatedRequestPrCleanupCallbacks,
};
