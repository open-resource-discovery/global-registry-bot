import {
  buildDetectedIssuesBody,
  normalizeMachineReadableIssues,
  singleMachineReadableIssue,
} from '../domain/machine-readable.js';
import { readIssueBodyForProcessing } from '../domain/issue-body-processing.js';
import { readRoutingLockExpected } from '../domain/routing-lock-marker.js';
import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  state?: string | null;
  body?: string | null;
  labels?: unknown;
  user?: UserLikeBase | null;
};

type TemplateLikeBase = {
  _meta?: { requestType?: string | null } | null;
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type ValidationIssueLike = {
  message?: string | null;
  path?: string | null;
};

type ValidateRequestIssueResultBase<TemplateType> = {
  errors?: string[];
  errorsFormattedSingle?: string;
  errorsFormatted?: string;
  validationIssues?: ValidationIssueLike[];
  template?: TemplateType;
  namespace: string;
  nsType: string;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

type FinalizeApprovedRequestOptions = {
  approvalPrefix?: string;
  approvalComment?: string;
  autoApproved?: boolean;
};

type CloseOutdatedRequestPrsOptions<FormDataType> = {
  parsedFormData?: FormDataType;
  currentHash?: string;
  acceptedHashes?: string[];
};

export type RequestIssueLifecycleCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = {
  isJestWorker: boolean;
  isDebugEnabled: boolean;
  hasIssueFormInputs: (issue: IssueType | null | undefined) => boolean;
  loadTemplateWithLabelRefresh: (context: ContextType, params: ParamsType, issue: IssueType) => Promise<TemplateType>;
  buildTemplateLoadErrorMessage: (errMsg: unknown) => string;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  isRequestIssue: (
    context: ContextType,
    template: TemplateType | null | undefined,
    parsedFormData: FormDataType
  ) => boolean;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  toLabelNames: (labels: unknown) => string[];
  detectSingleRoutingLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    labels: string[]
  ) => Promise<string>;
  ensureRoutingLockMarker: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    expectedLabel: string
  ) => Promise<boolean>;
  enforceRoutingLabelLock: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    expectedLabel: string,
    opts?: { changedLabel?: string }
  ) => Promise<boolean>;
  removeRejectedStatusLabel: (context: ContextType, params: ParamsType, currentLabels?: string[]) => Promise<void>;
  buildCompatibleRequestSnapshotHashes: (
    issueBody: unknown,
    parsedFormData: FormDataType,
    template: TemplateType
  ) => string[];
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  normalizeIssueTitle: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType
  ) => Promise<void>;
  closeOutdatedRequestPrs: (
    context: ContextType,
    params: ParamsType,
    template: TemplateType,
    options: CloseOutdatedRequestPrsOptions<FormDataType>
  ) => Promise<void>;
  onCloseOutdatedRequestPrsSkipped: (error: unknown) => void;
  validateRequestIssue: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    options?: { template?: TemplateType; formData?: FormDataType }
  ) => Promise<ValidateResultType>;
  checkParentChainExistsInFlatStructure: (
    context: ContextType,
    repoInfo: { owner: string; repo: string },
    template: TemplateType,
    formData: FormDataType,
    explicitResourceName?: string
  ) => Promise<string | null>;
  onParentChainCheckFailed: (error: unknown) => void;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  maybeRequireParentOwnerApproval: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    validatedNamespace: string,
    requestType: string
  ) => Promise<boolean>;
  maybeRequireSystemContactOwnerApproval: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    requestType: string,
    validatedNamespace: string
  ) => Promise<boolean>;
  getApprovedParentOwnerLogin: (issueBody: unknown, target: string) => string;
  isSubContextRequestType: (requestType: unknown) => boolean;
  maybeHandleApprovalDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType: string,
    namespace: string,
    options: unknown
  ) => Promise<string>;
  buildApprovalDecisionDispatchOptions: () => unknown;
  finalizeApprovedRequest: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    options: FinalizeApprovedRequestOptions
  ) => Promise<void>;
  resolveManualReviewApproverOverrideFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  resolveAdditionalIssueApproversFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  handoverToCpa: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    nsType: string,
    namespace: string,
    labels: string[],
    options: Record<string, unknown>
  ) => Promise<void>;
  buildReviewHandoverOptions: () => Record<string, unknown>;
};

export type RequestIssueAuthorUpdateCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = {
  validateRequestIssue: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    options?: { template?: TemplateType; formData?: FormDataType }
  ) => Promise<ValidateResultType>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  checkParentChainExistsInFlatStructure: (
    context: ContextType,
    repoInfo: { owner: string; repo: string },
    template: TemplateType,
    formData: FormDataType,
    explicitResourceName?: string
  ) => Promise<string | null>;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  closeOutdatedRequestPrs: (
    context: ContextType,
    params: ParamsType,
    template: TemplateType,
    options?: CloseOutdatedRequestPrsOptions<FormDataType>
  ) => Promise<void>;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  maybeRequireParentOwnerApproval: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    validatedNamespace: string,
    requestType: string
  ) => Promise<boolean>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  isDebugEnabled: boolean;
  maybeRequireSystemContactOwnerApproval: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    requestType: string,
    validatedNamespace: string
  ) => Promise<boolean>;
  getApprovedParentOwnerLogin: (issueBody: unknown, target: string) => string;
  isSubContextRequestType: (requestType: unknown) => boolean;
  maybeHandleApprovalDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType: string,
    namespace: string,
    options: unknown
  ) => Promise<string>;
  buildApprovalDecisionDispatchOptions: () => unknown;
  finalizeApprovedRequest: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    options: FinalizeApprovedRequestOptions
  ) => Promise<void>;
  resolveManualReviewApproverOverrideFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  resolveAdditionalIssueApproversFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  handoverToCpa: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    nsType: string,
    namespace: string,
    labels: string[],
    options: Record<string, unknown>
  ) => Promise<void>;
  buildReviewHandoverOptions: () => Record<string, unknown>;
  onParentChainCheckFailed: (error: unknown) => void;
  onCloseOutdatedRequestPrsSkipped: (error: unknown) => void;
  onRevalidationFailed: (error: unknown) => void;
};

export async function processRequestIssueLifecycle<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  callbacks: RequestIssueLifecycleCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType
  >
): Promise<void> {
  if (!callbacks.isJestWorker) {
    if (!callbacks.hasIssueFormInputs(issue)) return;
  }

  let template: TemplateType;
  try {
    template = await callbacks.loadTemplateWithLabelRefresh(context, params, issue);
  } catch (error: unknown) {
    const message = toStringTrim(error instanceof Error ? error.message : error);
    const messageLower = message.toLowerCase();
    const isRoutingError =
      messageLower.includes('no routing label found') || messageLower.includes('cannot resolve template');

    if (isRoutingError && !callbacks.hasIssueFormInputs(issue)) {
      if (callbacks.isDebugEnabled) {
        callbacks.log(
          context,
          'debug',
          { issue: issue.number, err: message },
          'requestHandler:issues-event skipped (non-form issue)'
        );
      }
      return;
    }

    callbacks.log(context, 'error', { err: message }, 'Error loading template in issues handler');

    await callbacks.postOnce(context, params, callbacks.buildTemplateLoadErrorMessage(message), {
      minimizeTag: 'nsreq:config',
    });
    await callbacks.setStateLabel(context, params, issue, 'author');
    return;
  }

  const processedIssueBody = readIssueBodyForProcessing(issue.body);
  const parsedFormData = callbacks.parseForm(processedIssueBody, template);

  if (!callbacks.isRequestIssue(context, template, parsedFormData)) {
    if (callbacks.isDebugEnabled) {
      callbacks.log(
        context,
        'debug',
        { issue: issue.number, parsedKeys: Object.keys(parsedFormData || {}) },
        'requestHandler:issues-event skipped (not a request issue)'
      );
    }
    return;
  }

  const expectedRouting =
    readRoutingLockExpected(issue.body) ||
    (await callbacks.detectSingleRoutingLabel(context, params, issue, callbacks.toLabelNames(issue.labels)));

  if (expectedRouting) {
    await callbacks.ensureRoutingLockMarker(context, params, issue, expectedRouting);
    await callbacks.enforceRoutingLabelLock(context, params, issue, expectedRouting);
  }

  if (toStringTrim(issue.state).toLowerCase() === 'closed') return;

  await callbacks.removeRejectedStatusLabel(context, params, callbacks.toLabelNames(issue.labels));

  const snapshotHashes = callbacks.buildCompatibleRequestSnapshotHashes(issue.body, parsedFormData, template);
  const currentHash = snapshotHashes[0] || callbacks.calcSnapshotHash(parsedFormData, template, processedIssueBody);

  await callbacks.normalizeIssueTitle(context, params, issue, template, parsedFormData);

  try {
    await callbacks.closeOutdatedRequestPrs(context, params, template, {
      parsedFormData,
      currentHash,
      acceptedHashes: snapshotHashes,
    });
  } catch (error: unknown) {
    callbacks.onCloseOutdatedRequestPrsSkipped(error);
  }

  const result = await callbacks.validateRequestIssue(context, params, issue, {
    template,
    formData: parsedFormData,
  });

  const { errors, errorsFormattedSingle, errorsFormatted, namespace: validatedNamespace, nsType } = result;

  if (errors?.length) {
    const listFallback = (errors || []).map((entry) => `- ${entry}`).join('\n');
    const message =
      errorsFormattedSingle?.trim() || errorsFormatted?.trim() || listFallback || 'Unknown validation error.';

    await callbacks.postOnce(
      context,
      params,
      buildDetectedIssuesBody(message, normalizeMachineReadableIssues(result.validationIssues || [])),
      {
        minimizeTag: 'nsreq:validation',
      }
    );
    await callbacks.setStateLabel(context, params, issue, 'author');
    return;
  }

  try {
    const parentError = await callbacks.checkParentChainExistsInFlatStructure(
      context,
      { owner: params.owner, repo: params.repo },
      template,
      parsedFormData,
      validatedNamespace
    );

    if (parentError) {
      await callbacks.postOnce(
        context,
        params,
        buildDetectedIssuesBody(`- ${parentError}`, singleMachineReadableIssue('name', parentError)),
        {
          minimizeTag: 'nsreq:validation',
        }
      );
      await callbacks.setStateLabel(context, params, issue, 'author');
      return;
    }
  } catch (error: unknown) {
    callbacks.onParentChainCheckFailed(error);
  }

  const resolvedTemplate = result.template || template;
  const effectiveRequestType = callbacks.resolveEffectiveRequestType(resolvedTemplate, parsedFormData);

  const gated = await callbacks.maybeRequireParentOwnerApproval(
    context,
    params,
    issue,
    resolvedTemplate,
    validatedNamespace,
    effectiveRequestType
  );

  if (callbacks.isDebugEnabled) {
    callbacks.log(
      context,
      'debug',
      { issue: issue.number, target: validatedNamespace, requestType: effectiveRequestType, gated },
      'parent-approval:gate-result'
    );
  }

  if (gated) return;

  const contactGated = await callbacks.maybeRequireSystemContactOwnerApproval(
    context,
    params,
    issue,
    parsedFormData,
    effectiveRequestType,
    validatedNamespace
  );

  if (contactGated) return;

  const parentApprovedBy = callbacks.getApprovedParentOwnerLogin(issue.body, validatedNamespace);
  if (callbacks.isSubContextRequestType(effectiveRequestType) && parentApprovedBy) {
    const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
      context,
      params,
      issue,
      resolvedTemplate,
      parsedFormData,
      effectiveRequestType,
      validatedNamespace,
      callbacks.buildApprovalDecisionDispatchOptions()
    );

    if (approvalOutcome !== 'continue') return;

    await callbacks.finalizeApprovedRequest(context, params, issue, resolvedTemplate, parsedFormData, {
      approvalPrefix: `Approved by parent namespace owner @${parentApprovedBy}`,
    });
    return;
  }

  const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
    context,
    params,
    issue,
    resolvedTemplate,
    parsedFormData,
    effectiveRequestType,
    validatedNamespace,
    callbacks.buildApprovalDecisionDispatchOptions()
  );

  if (approvalOutcome !== 'continue') return;

  const manualApproversOverride = await callbacks.resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    resolvedTemplate,
    parsedFormData,
    effectiveRequestType
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await callbacks.resolveAdditionalIssueApproversFromApprovalHook(
        context,
        params,
        issue,
        resolvedTemplate,
        parsedFormData,
        effectiveRequestType
      );

  await callbacks.handoverToCpa(context, params, issue, nsType, validatedNamespace, [], {
    snapshotHash: currentHash,
    requestType: effectiveRequestType,
    extraApprovers: hookApprovers,
    manualApproversOverride,
    ...callbacks.buildReviewHandoverOptions(),
  });
}

export async function processAuthorUpdateComment<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  callbacks: RequestIssueAuthorUpdateCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType
  >
): Promise<void> {
  try {
    const reval = await callbacks.validateRequestIssue(context, params, issue, {
      template,
      formData: parsedFormData,
    });
    const {
      errors: revalErrors,
      errorsFormattedSingle: revalErrorsFormattedSingle,
      errorsFormatted: revalErrorsFormatted,
      namespace,
      nsType,
      template: validatedTemplate,
    } = reval;

    if (Array.isArray(revalErrors) && revalErrors.length === 0 && validatedTemplate) {
      const issueBody = readIssueBodyForProcessing(issue.body);
      const parsedAfterUpdate = callbacks.parseForm(issueBody, validatedTemplate);
      const snapshotHash = callbacks.calcSnapshotHash(parsedAfterUpdate, validatedTemplate, issueBody);

      try {
        const parentError = await callbacks.checkParentChainExistsInFlatStructure(
          context,
          { owner: params.owner, repo: params.repo },
          validatedTemplate,
          parsedAfterUpdate,
          namespace
        );
        if (parentError) {
          await callbacks.postOnce(
            context,
            params,
            buildDetectedIssuesBody(`- ${parentError}`, singleMachineReadableIssue('name', parentError)),
            {
              minimizeTag: 'nsreq:validation',
            }
          );
          await callbacks.setStateLabel(context, params, issue, 'author');
          return;
        }
      } catch (error: unknown) {
        callbacks.onParentChainCheckFailed(error);
      }

      try {
        await callbacks.closeOutdatedRequestPrs(context, params, validatedTemplate);
      } catch (error: unknown) {
        callbacks.onCloseOutdatedRequestPrsSkipped(error);
      }

      const effectiveRequestType = callbacks.resolveEffectiveRequestType(validatedTemplate, parsedAfterUpdate);

      const gated = await callbacks.maybeRequireParentOwnerApproval(
        context,
        params,
        issue,
        validatedTemplate,
        namespace,
        effectiveRequestType
      );

      if (callbacks.isDebugEnabled) {
        callbacks.log(
          context,
          'debug',
          { issue: issue.number, target: namespace, requestType: effectiveRequestType, gated },
          'parent-approval:gate-result(update)'
        );
      }

      if (gated) return;

      const contactGated = await callbacks.maybeRequireSystemContactOwnerApproval(
        context,
        params,
        issue,
        parsedAfterUpdate,
        effectiveRequestType,
        namespace
      );

      if (contactGated) return;

      const parentApprovedBy = callbacks.getApprovedParentOwnerLogin(issue.body, namespace);
      if (callbacks.isSubContextRequestType(effectiveRequestType) && parentApprovedBy) {
        const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
          context,
          params,
          issue,
          validatedTemplate,
          parsedAfterUpdate,
          effectiveRequestType,
          namespace,
          callbacks.buildApprovalDecisionDispatchOptions()
        );

        if (approvalOutcome !== 'continue') return;

        await callbacks.finalizeApprovedRequest(context, params, issue, validatedTemplate, parsedAfterUpdate, {
          approvalPrefix: `Approved by parent namespace owner @${parentApprovedBy}`,
        });
        return;
      }

      const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
        context,
        params,
        issue,
        validatedTemplate,
        parsedAfterUpdate,
        effectiveRequestType,
        namespace,
        callbacks.buildApprovalDecisionDispatchOptions()
      );

      if (approvalOutcome !== 'continue') return;

      const manualApproversOverride = await callbacks.resolveManualReviewApproverOverrideFromApprovalHook(
        context,
        params,
        issue,
        validatedTemplate,
        parsedAfterUpdate,
        effectiveRequestType
      );

      const hookApprovers = manualApproversOverride.length
        ? []
        : await callbacks.resolveAdditionalIssueApproversFromApprovalHook(
            context,
            params,
            issue,
            validatedTemplate,
            parsedAfterUpdate,
            effectiveRequestType
          );

      await callbacks.handoverToCpa(context, params, issue, nsType, namespace, [], {
        snapshotHash,
        requestType: effectiveRequestType,
        extraApprovers: hookApprovers,
        manualApproversOverride,
        ...callbacks.buildReviewHandoverOptions(),
      });
      return;
    }

    const listFallback = (revalErrors || []).map((entry) => `- ${entry}`).join('\n');
    const message =
      revalErrorsFormattedSingle?.trim() || revalErrorsFormatted?.trim() || listFallback || 'Unknown validation error.';
    await callbacks.postOnce(
      context,
      params,
      buildDetectedIssuesBody(
        message,
        normalizeMachineReadableIssues(
          (reval.validationIssues || []).map((validationIssue) => ({
            field: toStringTrim(validationIssue.path) || 'details',
            message: toStringTrim(validationIssue.message),
          }))
        )
      ),
      {
        minimizeTag: 'nsreq:validation',
      }
    );
    await callbacks.setStateLabel(context, params, issue, 'author');
  } catch (error: unknown) {
    callbacks.onRevalidationFailed(error);
  }
}
