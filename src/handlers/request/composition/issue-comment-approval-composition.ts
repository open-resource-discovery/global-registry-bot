import type { ApprovalCommentHandlingCallbacks } from '../application/approval-comment-handling.js';
import type { ApprovedRequestFinalizationCallbacks } from '../application/approved-request-finalization.js';
import type { IssueStateReviewerOperationsCallbacks } from '../application/issue-state-reviewer-operations.js';
import type { OwnerApprovalCommentHandlingCallbacks } from '../application/owner-approval-comment-handling.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type PullRequestLikeBase = {
  number: number;
};

type ContactApprovalMetaBase = {
  target?: string | null;
  owners?: string[];
  approvedBy?: string | null;
  approvedAt?: string | null;
};

type ParentApprovalMetaBase = {
  parent?: string | null;
  target?: string | null;
  owners?: string[];
  approvedBy?: string | null;
  approvedAt?: string | null;
};

type EffectiveConstantsCompositeBase = {
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

// Composite: owner-approval needs nsType too
type ValidationResultCompositeBase<TemplateType> = {
  errors?: string[];
  errorsFormattedSingle?: string;
  errorsFormatted?: string;
  validationIssues?: { path?: string | null; message?: string | null }[];
  template?: TemplateType;
  namespace: string;
  nsType: string;
};

type IssueLikeCompositeBase = {
  number: number;
  labels?: unknown;
  body?: string | null;
  user?: { login?: string | null } | null;
};

type TemplateLikeCompositeBase = {
  _meta?: { requestType?: string; root?: string; schema?: string; path?: string };
  [key: string]: unknown;
};

// IssueStateReviewerOperations

export function composeIssueStateReviewerOperationsCallbacks<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeCompositeBase,
  TemplateType extends TemplateLikeCompositeBase,
  FormDataType extends Record<string, string>,
  EffectiveConstantsType extends EffectiveConstantsCompositeBase,
>(
  dependencies: IssueStateReviewerOperationsCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): IssueStateReviewerOperationsCallbacks<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  EffectiveConstantsType
> {
  return {
    toLabelNames: dependencies.toLabelNames,
    normalizeKey: dependencies.normalizeKey,
    resolveWorkflowLabel: dependencies.resolveWorkflowLabel,
    labelsMatching: dependencies.labelsMatching,
    resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
    extractResourceNameFromForm: dependencies.extractResourceNameFromForm,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    runApprovalHook: dependencies.runApprovalHook,
    getHttpStatus: dependencies.getHttpStatus,
    getErrorMessage: dependencies.getErrorMessage,
    log: dependencies.log,
  };
}

// ApprovedRequestFinalization

export function composeApprovedRequestFinalizationCallbacks<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeCompositeBase,
  TemplateType extends TemplateLikeCompositeBase,
  FormDataType extends Record<string, string>,
  EffectiveConstantsType extends EffectiveConstantsCompositeBase,
  PullRequestType extends PullRequestLikeBase,
>(
  dependencies: ApprovedRequestFinalizationCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType,
    PullRequestType
  >
): ApprovedRequestFinalizationCallbacks<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  EffectiveConstantsType,
  PullRequestType
> {
  return {
    resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
    extractResourceNameFromForm: dependencies.extractResourceNameFromForm,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
    findOpenIssuePrs: dependencies.findOpenIssuePrs,
    applyApprovedRequestState: dependencies.applyApprovedRequestState,
    addApprovedLabelToPr: dependencies.addApprovedLabelToPr,
    ensureAssigneesPresent: dependencies.ensureAssigneesPresent,
    createRequestPrWithRecovery: dependencies.createRequestPrWithRecovery,
    postOnce: dependencies.postOnce,
  };
}

// ApprovalCommentHandling

export function composeApprovalCommentHandlingCallbacks<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeCompositeBase,
  TemplateType extends TemplateLikeCompositeBase,
  FormDataType extends Record<string, string>,
  EffectiveConstantsType extends EffectiveConstantsCompositeBase,
  ValidationResultType extends ValidationResultCompositeBase<TemplateType>,
>(
  dependencies: ApprovalCommentHandlingCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType,
    ValidationResultType
  >
): ApprovalCommentHandlingCallbacks<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  EffectiveConstantsType,
  ValidationResultType
> {
  return {
    resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    resolveApproversForRequestType: dependencies.resolveApproversForRequestType,
    ensureReviewLabelsPresentOnIssue: dependencies.ensureReviewLabelsPresentOnIssue,
    postOnce: dependencies.postOnce,
    uniqLogins: dependencies.uniqLogins,
    isAuthorizedApprover: dependencies.isAuthorizedApprover,
    resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
    validateRequestIssue: dependencies.validateRequestIssue,
    setStateLabel: dependencies.setStateLabel,
    checkParentChainExistsInFlatStructure: dependencies.checkParentChainExistsInFlatStructure,
    log: dependencies.log,
    finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
  };
}

// OwnerApprovalCommentHandling

export type OwnerApprovalCommentHandlingCompositionDependencies<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeCompositeBase,
  TemplateType extends TemplateLikeCompositeBase,
  FormDataType extends Record<string, string>,
  ValidationResultType extends ValidationResultCompositeBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
> = Omit<
  OwnerApprovalCommentHandlingCallbacks<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidationResultType,
    ContactApprovalMetaType,
    ParentApprovalMetaType
  >,
  'buildApprovedContactApprovalMeta' | 'buildApprovedParentApprovalMeta'
> & {
  toStringTrim: (value: unknown) => string;
  normalizeLogin: (value: unknown) => string;
  uniqLogins: (values: string[]) => string[];
};

export function composeOwnerApprovalCommentHandlingCallbacks<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeCompositeBase,
  TemplateType extends TemplateLikeCompositeBase,
  FormDataType extends Record<string, string>,
  ValidationResultType extends ValidationResultCompositeBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
>(
  dependencies: OwnerApprovalCommentHandlingCompositionDependencies<
    ContextType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidationResultType,
    ContactApprovalMetaType,
    ParentApprovalMetaType
  >
): OwnerApprovalCommentHandlingCallbacks<
  ContextType,
  IssueParamsType,
  IssueType,
  TemplateType,
  FormDataType,
  ValidationResultType,
  ContactApprovalMetaType,
  ParentApprovalMetaType
> {
  return {
    readContactApprovalMeta: dependencies.readContactApprovalMeta,
    readParentApprovalMeta: dependencies.readParentApprovalMeta,
    normalizeLogin: dependencies.normalizeLogin,
    uniqLogins: dependencies.uniqLogins,
    normalizeKey: dependencies.normalizeKey,
    postOnce: dependencies.postOnce,
    validateRequestIssue: dependencies.validateRequestIssue,
    setStateLabel: dependencies.setStateLabel,
    parseForm: dependencies.parseForm,
    calcSnapshotHash: dependencies.calcSnapshotHash,
    resolveEffectiveRequestType: dependencies.resolveEffectiveRequestType,
    ensureContactApprovalMarker: dependencies.ensureContactApprovalMarker,
    ensureParentApprovalMarker: dependencies.ensureParentApprovalMarker,

    buildApprovedContactApprovalMeta: ({ target, owners, approvedBy, approvedAt }): ContactApprovalMetaType => {
      const meta: ContactApprovalMetaBase = {
        target: dependencies.toStringTrim(target),
        owners: dependencies.uniqLogins(owners || []),
        approvedBy: dependencies.normalizeLogin(approvedBy),
        approvedAt: dependencies.toStringTrim(approvedAt),
      };
      return meta as unknown as ContactApprovalMetaType;
    },

    buildApprovedParentApprovalMeta: ({ parent, target, owners, approvedBy, approvedAt }): ParentApprovalMetaType => {
      const meta: ParentApprovalMetaBase = {
        parent: dependencies.toStringTrim(parent),
        target: dependencies.toStringTrim(target),
        owners: dependencies.uniqLogins(owners || []),
        approvedBy: dependencies.normalizeLogin(approvedBy),
        approvedAt: dependencies.toStringTrim(approvedAt),
      };
      return meta as unknown as ParentApprovalMetaType;
    },

    maybeHandleApprovalDecision: dependencies.maybeHandleApprovalDecision,
    buildApprovalDecisionDispatchOptions: dependencies.buildApprovalDecisionDispatchOptions,
    resolveManualReviewApproverOverrideFromApprovalHook:
      dependencies.resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook: dependencies.resolveAdditionalIssueApproversFromApprovalHook,
    handoverToCpa: dependencies.handoverToCpa,
    buildReviewHandoverOptions: dependencies.buildReviewHandoverOptions,
    setParentOwnerActionState: dependencies.setParentOwnerActionState,
    assignParentOwnersForApproval: dependencies.assignParentOwnersForApproval,
    clearParentOwnerActionState: dependencies.clearParentOwnerActionState,
    isSubContextRequestType: dependencies.isSubContextRequestType,
    finalizeApprovedRequest: dependencies.finalizeApprovedRequest,
  };
}
