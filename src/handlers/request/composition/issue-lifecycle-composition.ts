import type {
  RequestIssueAuthorUpdateCallbacks,
  RequestIssueLifecycleCallbacks,
} from '../application/request-issue-lifecycle.js';
import type { RequestPrCreationRecoveryCallbacks } from '../application/request-pr-creation-recovery.js';

type AppLogLike = {
  warn?: (...args: unknown[]) => void;
};

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = RepoInfoBase & {
  issue_number: number;
};

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  state?: string | null;
  body?: string | null;
  labels?: unknown;
  user?: UserLikeBase | null;
};

type TemplateShapeBase = {
  _meta?: { requestType?: string | null; root?: string | null };
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type ValidateRequestIssueResultBase<TemplateType> = {
  errors?: string[];
  errorsFormattedSingle?: string;
  errorsFormatted?: string;
  validationIssues?: { message?: string | null; path?: string | null }[];
  template?: TemplateType;
  namespace: string;
  nsType: string;
};

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function warnObject(appLog: AppLogLike | undefined, obj: unknown, msg: string): void {
  const logger = appLog ?? console;
  logger.warn?.(obj, msg);
}

function warnText(appLog: AppLogLike | undefined, msg: string): void {
  const logger = appLog ?? console;
  logger.warn?.(msg);
}

export type RequestPrCreationRecoveryCompositionDependencies<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends { number: number },
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
> = RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType>;

export function composeRequestPrCreationRecoveryCallbacks<
  ContextType,
  RepoType extends RepoInfoBase,
  IssueType extends { number: number },
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
>(
  dependencies: RequestPrCreationRecoveryCompositionDependencies<
    ContextType,
    RepoType,
    IssueType,
    TemplateType,
    FormDataType
  >
): RequestPrCreationRecoveryCallbacks<ContextType, RepoType, IssueType, TemplateType, FormDataType> {
  return {
    createRequestPr: dependencies.createRequestPr,
    getHttpStatus: dependencies.getHttpStatus,
    renderConfiguredRequestBranchName: dependencies.renderConfiguredRequestBranchName,
  };
}

export type RequestIssueLifecycleCompositionDependencies<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = Omit<
  RequestIssueLifecycleCallbacks<ContextType, ParamsType, IssueType, TemplateType, FormDataType, ValidateResultType>,
  'onCloseOutdatedRequestPrsSkipped' | 'onParentChainCheckFailed'
> & {
  appLog?: AppLogLike;
};

export function composeRequestIssueLifecycleCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  dependencies: RequestIssueLifecycleCompositionDependencies<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType
  >
): RequestIssueLifecycleCallbacks<ContextType, ParamsType, IssueType, TemplateType, FormDataType, ValidateResultType> {
  return {
    isJestWorker: dependencies.isJestWorker,
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
    normalizeIssueTitle: dependencies.normalizeIssueTitle,
    closeOutdatedRequestPrs: dependencies.closeOutdatedRequestPrs,
    onCloseOutdatedRequestPrsSkipped: (error: unknown): void => {
      warnObject(dependencies.appLog, { err: errorMessage(error) }, 'closeOutdatedRequestPRs skipped');
    },
    validateRequestIssue: dependencies.validateRequestIssue,
    checkParentChainExistsInFlatStructure: dependencies.checkParentChainExistsInFlatStructure,
    onParentChainCheckFailed: (error: unknown): void => {
      warnObject(dependencies.appLog, { err: errorMessage(error) }, 'parent chain check failed');
    },
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    maybeRequireParentOwnerApproval: dependencies.maybeRequireParentOwnerApproval,
    maybeRequireSystemContactOwnerApproval: dependencies.maybeRequireSystemContactOwnerApproval,
    getApprovedParentOwnerLogin: dependencies.getApprovedParentOwnerLogin,
    isSubContextRequestType: dependencies.isSubContextRequestType,
    maybeHandleApprovalDecision: dependencies.maybeHandleApprovalDecision,
    buildApprovalDecisionDispatchOptions: dependencies.buildApprovalDecisionDispatchOptions,
    finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
    resolveManualReviewApproverOverrideFromApprovalHook:
      dependencies.resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: dependencies.handoverToCpa,
    buildReviewHandoverOptions: dependencies.buildReviewHandoverOptions,
  };
}

export type RequestIssueAuthorUpdateCompositionDependencies<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = Omit<
  RequestIssueAuthorUpdateCallbacks<ContextType, ParamsType, IssueType, TemplateType, FormDataType, ValidateResultType>,
  'onParentChainCheckFailed' | 'onCloseOutdatedRequestPrsSkipped' | 'onRevalidationFailed'
> & {
  appLog?: AppLogLike;
};

export function composeRequestIssueAuthorUpdateCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateShapeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  dependencies: RequestIssueAuthorUpdateCompositionDependencies<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType
  >
): RequestIssueAuthorUpdateCallbacks<
  ContextType,
  ParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  ValidateResultType
> {
  return {
    validateRequestIssue: dependencies.validateRequestIssue,
    parseForm: dependencies.parseForm,
    calcSnapshotHash: dependencies.calcSnapshotHash,
    checkParentChainExistsInFlatStructure: dependencies.checkParentChainExistsInFlatStructure,
    postOnce: dependencies.postOnce,
    setStateLabel: dependencies.setStateLabel,
    closeOutdatedRequestPrs: dependencies.closeOutdatedRequestPrs,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    maybeRequireParentOwnerApproval: dependencies.maybeRequireParentOwnerApproval,
    log: dependencies.log,
    isDebugEnabled: dependencies.isDebugEnabled,
    maybeRequireSystemContactOwnerApproval: dependencies.maybeRequireSystemContactOwnerApproval,
    getApprovedParentOwnerLogin: dependencies.getApprovedParentOwnerLogin,
    isSubContextRequestType: dependencies.isSubContextRequestType,
    maybeHandleApprovalDecision: dependencies.maybeHandleApprovalDecision,
    buildApprovalDecisionDispatchOptions: dependencies.buildApprovalDecisionDispatchOptions,
    finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
    resolveManualReviewApproverOverrideFromApprovalHook:
      dependencies.resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: dependencies.handoverToCpa,
    buildReviewHandoverOptions: dependencies.buildReviewHandoverOptions,
    onParentChainCheckFailed: (error: unknown): void => {
      warnObject(dependencies.appLog, { err: errorMessage(error) }, 'parent chain check failed');
    },
    onCloseOutdatedRequestPrsSkipped: (error: unknown): void => {
      warnObject(dependencies.appLog, { err: errorMessage(error) }, 'closeOutdatedRequestPRs skipped');
    },
    onRevalidationFailed: (error: unknown): void => {
      warnText(dependencies.appLog, `Revalidation failed: ${errorMessage(error)}`);
    },
  };
}
