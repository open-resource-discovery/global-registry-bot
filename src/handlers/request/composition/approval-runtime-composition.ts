import {
  finalizeApprovedRequest as finalizeApprovedRequestApplication,
  type ApprovedRequestFinalizationCallbacks,
} from '../application/approved-request-finalization.js';
import {
  handleApprovalComment as handleApprovalCommentApplication,
  type ApprovalCommentHandlingCallbacks,
} from '../application/approval-comment-handling.js';
import {
  handleParentOwnerApprovalIfNeeded as handleParentOwnerApprovalIfNeededApplication,
  handleSystemContactOwnerApprovalIfNeeded as handleSystemContactOwnerApprovalIfNeededApplication,
  type OwnerApprovalCommentHandlingCallbacks,
} from '../application/owner-approval-comment-handling.js';
import {
  maybeRequireParentOwnerApproval as maybeRequireParentOwnerApprovalApplication,
  maybeRequireSystemContactOwnerApproval as maybeRequireSystemContactOwnerApprovalApplication,
  type OwnerApprovalRequirementsCallbacks,
} from '../application/owner-approval-requirements.js';
import {
  resolveAdditionalIssueApproversFromApprovalHook as resolveAdditionalIssueApproversFromApprovalHookApplication,
  resolveManualReviewApproverOverrideFromApprovalHook as resolveManualReviewApproverOverrideFromApprovalHookApplication,
  type IssueStateReviewerOperationsCallbacks,
} from '../application/issue-state-reviewer-operations.js';
import { maybeHandleApprovalDecision, type ApprovalHandlingResult } from '../application/approval-decision-dispatch.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';

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
  head: { ref?: string | null; sha?: string | null };
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

export type ApprovalRuntimeDependencies<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateRequestIssueResultType extends ValidateRequestIssueResultBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
> = {
  runApprovalHook: (
    context: ContextType,
    repoInfo: RepoInfoBase,
    args: {
      requestType: string;
      namespace?: string | null;
      resourceName?: string | null;
      formData: FormDataType;
      issue: IssueType;
      requestAuthorId?: string | null;
    }
  ) => Promise<ApprovalDecision | boolean>;
  extractResourceNameFromForm: (formData: FormDataType, template: TemplateType) => string;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
  rejectRequestFromApprovalHook: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    decision: ApprovalDecision,
    options: {
      closeLinkedPrs?: boolean;
      minimizeTag?: string;
      listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
      parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
    }
  ) => Promise<void>;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  resolveApproverRoutingForRequestType: (
    context: ContextType,
    requestType: string | undefined | null,
    fallbackApprovers: string[],
    fallbackApproversPool: string[]
  ) => {
    approvalUsernames: string[];
    autoAssigneePoolUsernames: string[];
  };
  pickAutoAssigneeFromPool: (issue: IssueType, approversPool: string[]) => string[];
  uniqLogins: (values: string[]) => string[];
  toStringTrim: (value: unknown) => string;
  ensureAssigneesPresent: (context: ContextType, params: IssueParamsType, assignees: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: IssueParamsType, labels: string[]) => Promise<void>;
  buildReviewHandoverBody: (
    context: ContextType,
    snapshotHash?: string,
    options?: { target?: 'issue' | 'pull_request' }
  ) => string;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  buildApprovedRequestFinalizationCallbacks: () => Parameters<
    typeof finalizeApprovedRequestApplication<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      EffectiveConstantsType,
      PullRequestType
    >
  >[6];
  buildApprovalCommentHandlingCallbacks: () => Parameters<
    typeof handleApprovalCommentApplication<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      EffectiveConstantsType,
      ValidateRequestIssueResultType
    >
  >[6];
  buildOwnerApprovalCommentHandlingCallbacks: () => Parameters<
    typeof handleParentOwnerApprovalIfNeededApplication<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      ValidateRequestIssueResultType,
      ContactApprovalMetaType,
      ParentApprovalMetaType
    >
  >[6];
  buildOwnerApprovalRequirementsCallbacks: () => Parameters<
    typeof maybeRequireParentOwnerApprovalApplication<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      EffectiveConstantsType
    >
  >[6];
  buildIssueStateReviewerOperationsCallbacks: () => Parameters<
    typeof resolveAdditionalIssueApproversFromApprovalHookApplication<
      ContextType,
      IssueParamsType,
      IssueType,
      TemplateType,
      FormDataType
    >
  >[6];
};

export type ApprovalRuntime<
  ContextType,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
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
  buildReviewHandoverOptions: () => {
    resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
    resolveApproverRoutingForRequestType: (
      context: ContextType,
      requestType: string | undefined,
      approverUsernames: string[],
      approverPoolUsernames: string[]
    ) => {
      approvalUsernames: string[];
      autoAssigneePoolUsernames: string[];
    };
    pickAutoAssigneeFromPool: (issue: IssueType, pool: string[]) => string[];
    uniqLogins: (logins: string[]) => string[];
    toStringTrim: (value: unknown) => string;
    ensureAssigneesPresent: (context: ContextType, params: IssueParamsType, assignees: string[]) => Promise<void>;
    ensureLabelsPresentOnce: (context: ContextType, params: IssueParamsType, labels: string[]) => Promise<void>;
    buildReviewHandoverBody: (context: ContextType, snapshotHash?: string) => string;
  };
  maybeHandleApprovalDecision: typeof maybeHandleApprovalDecision;
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
  handleApprovalComment: (
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ) => Promise<void>;
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
};

export function createApprovalRuntime<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  IssueParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  PullRequestType extends PullRequestLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateRequestIssueResultType extends ValidateRequestIssueResultBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
>(
  dependencies: ApprovalRuntimeDependencies<
    ContextType,
    RepoInfoType,
    IssueParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    PullRequestType,
    EffectiveConstantsType,
    ValidateRequestIssueResultType,
    ContactApprovalMetaType,
    ParentApprovalMetaType
  >
): ApprovalRuntime<ContextType, IssueParamsType, IssueType, TemplateType, FormDataType, EffectiveConstantsType> {
  async function finalizeApprovedRequest(
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
  ): Promise<void> {
    await finalizeApprovedRequestApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      options,
      dependencies.buildApprovedRequestFinalizationCallbacks()
    );
  }

  function buildApprovalDecisionDispatchOptions(): {
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
  } {
    return {
      resolveApprovalDecision: (
        dispatchContext: ContextType,
        dispatchParams: IssueParamsType,
        dispatchIssue: IssueType,
        dispatchTemplate: TemplateType,
        dispatchFormData: FormDataType,
        dispatchRequestType: string,
        dispatchNamespace: string
      ): Promise<ApprovalDecision | boolean> => {
        const repoInfo: RepoInfoBase = { owner: dispatchParams.owner, repo: dispatchParams.repo };
        return dependencies.runApprovalHook(dispatchContext, repoInfo, {
          requestType: dispatchRequestType,
          namespace: dispatchNamespace,
          resourceName: dependencies.extractResourceNameFromForm(dispatchFormData, dispatchTemplate),
          formData: dispatchFormData,
          issue: dispatchIssue,
        });
      },
      handleApprovedDecision: (
        dispatchContext: ContextType,
        dispatchParams: IssueParamsType,
        dispatchIssue: IssueType,
        dispatchTemplate: TemplateType,
        dispatchFormData: FormDataType,
        decision: ApprovalDecision
      ): Promise<void> =>
        finalizeApprovedRequest(dispatchContext, dispatchParams, dispatchIssue, dispatchTemplate, dispatchFormData, {
          approvalPrefix: '',
          approvalComment: decision.comment,
          autoApproved: true,
        }),
      handleRejectedDecision: (
        dispatchContext: ContextType,
        dispatchParams: IssueParamsType,
        dispatchIssue: IssueType,
        decision: ApprovalDecision
      ): Promise<void> =>
        dependencies.rejectRequestFromApprovalHook(dispatchContext, dispatchParams, dispatchIssue, decision, {
          closeLinkedPrs: true,
          minimizeTag: undefined,
          listOpenPullRequests: dependencies.listOpenPullRequests,
          parseLinkedIssueNumberFromPr: dependencies.parseLinkedIssueNumberFromPr,
        }),
    };
  }

  function buildReviewHandoverOptions(): {
    resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
    resolveApproverRoutingForRequestType: (
      context: ContextType,
      requestType: string | undefined,
      approverUsernames: string[],
      approverPoolUsernames: string[]
    ) => {
      approvalUsernames: string[];
      autoAssigneePoolUsernames: string[];
    };
    pickAutoAssigneeFromPool: (issue: IssueType, pool: string[]) => string[];
    uniqLogins: (logins: string[]) => string[];
    toStringTrim: (value: unknown) => string;
    ensureAssigneesPresent: (context: ContextType, params: IssueParamsType, assignees: string[]) => Promise<void>;
    ensureLabelsPresentOnce: (context: ContextType, params: IssueParamsType, labels: string[]) => Promise<void>;
    buildReviewHandoverBody: (context: ContextType, snapshotHash?: string) => string;
  } {
    return {
      resolveEffectiveConstants: dependencies.resolveEffectiveConstants,
      resolveApproverRoutingForRequestType: dependencies.resolveApproverRoutingForRequestType,
      pickAutoAssigneeFromPool: dependencies.pickAutoAssigneeFromPool,
      uniqLogins: dependencies.uniqLogins,
      toStringTrim: dependencies.toStringTrim,
      ensureAssigneesPresent: dependencies.ensureAssigneesPresent,
      ensureLabelsPresentOnce: dependencies.ensureLabelsPresentOnce,
      buildReviewHandoverBody: dependencies.buildReviewHandoverBody,
    };
  }

  async function maybeRequireParentOwnerApproval(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    validatedNamespace: string,
    requestType: string
  ): Promise<boolean> {
    return await maybeRequireParentOwnerApprovalApplication(
      context,
      params,
      issue,
      template,
      validatedNamespace,
      requestType,
      dependencies.buildOwnerApprovalRequirementsCallbacks()
    );
  }

  async function maybeRequireSystemContactOwnerApproval(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    requestType: string,
    validatedNamespace: string
  ): Promise<boolean> {
    return await maybeRequireSystemContactOwnerApprovalApplication(
      context,
      params,
      issue,
      parsedFormData,
      requestType,
      validatedNamespace,
      dependencies.buildOwnerApprovalRequirementsCallbacks()
    );
  }

  async function handleApprovalComment(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ): Promise<void> {
    await handleApprovalCommentApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      commenter,
      dependencies.buildApprovalCommentHandlingCallbacks()
    );
  }

  async function handleParentOwnerApprovalIfNeeded(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ): Promise<boolean> {
    return await handleParentOwnerApprovalIfNeededApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      commenter,
      dependencies.buildOwnerApprovalCommentHandlingCallbacks()
    );
  }

  async function handleSystemContactOwnerApprovalIfNeeded(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    commenter: string
  ): Promise<boolean> {
    return await handleSystemContactOwnerApprovalIfNeededApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      commenter,
      dependencies.buildOwnerApprovalCommentHandlingCallbacks()
    );
  }

  async function resolveAdditionalIssueApproversFromApprovalHook(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ): Promise<string[]> {
    return await resolveAdditionalIssueApproversFromApprovalHookApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType,
      dependencies.buildIssueStateReviewerOperationsCallbacks()
    );
  }

  async function resolveManualReviewApproverOverrideFromApprovalHook(
    context: ContextType,
    params: IssueParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ): Promise<string[]> {
    return await resolveManualReviewApproverOverrideFromApprovalHookApplication(
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType,
      dependencies.buildIssueStateReviewerOperationsCallbacks()
    );
  }

  return {
    buildApprovalDecisionDispatchOptions,
    buildReviewHandoverOptions,
    maybeHandleApprovalDecision,
    finalizeApprovedRequest,
    maybeRequireParentOwnerApproval,
    maybeRequireSystemContactOwnerApproval,
    handleApprovalComment,
    handleParentOwnerApprovalIfNeeded,
    handleSystemContactOwnerApprovalIfNeeded,
    resolveManualReviewApproverOverrideFromApprovalHook,
    resolveAdditionalIssueApproversFromApprovalHook,
  };
}

export type {
  ApprovalHandlingResult,
  ApprovedRequestFinalizationCallbacks,
  ApprovalCommentHandlingCallbacks,
  OwnerApprovalCommentHandlingCallbacks,
  OwnerApprovalRequirementsCallbacks,
  IssueStateReviewerOperationsCallbacks,
};
