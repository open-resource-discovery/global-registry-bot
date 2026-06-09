import {
  buildDetectedIssuesBody,
  normalizeMachineReadableIssues,
  singleMachineReadableIssue,
} from '../domain/machine-readable.js';
import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type UserLikeBase = { login?: string | null };

type IssueLikeBase = {
  number: number;
  user?: UserLikeBase | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type MachineReadableValidationIssue = {
  path?: string | null;
  message?: string | null;
};

type ValidateRequestIssueResultBase<TemplateType> = {
  errors?: string[];
  errorsFormattedSingle?: string;
  errorsFormatted?: string;
  validationIssues?: MachineReadableValidationIssue[];
  template?: TemplateType;
  namespace: string;
};

type EffectiveConstantsBase = {
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

type FinalizeApprovedRequestOptions = {
  approvalPrefix?: string;
  approvalComment?: string;
  autoApproved?: boolean;
};

export type ApprovalCommentHandlingCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  resolveApproversForRequestType: (
    context: ContextType,
    requestType: string | undefined | null,
    fallbackApprovers: string[],
    fallbackApproversPool: string[]
  ) => string[];
  ensureReviewLabelsPresentOnIssue: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    effectiveConstants: EffectiveConstantsType
  ) => Promise<boolean>;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  uniqLogins: (values: string[]) => string[];
  isAuthorizedApprover: (
    commenter: string,
    issueAuthor: string | undefined | null,
    allowedApprovers: string[]
  ) => boolean;
  resolveAdditionalIssueApproversFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  validateRequestIssue: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    options?: { template?: TemplateType; formData?: FormDataType }
  ) => Promise<ValidateResultType>;
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  checkParentChainExistsInFlatStructure: (
    context: ContextType,
    repoInfo: { owner: string; repo: string },
    template: TemplateType,
    formData: FormDataType,
    explicitResourceName?: string
  ) => Promise<string | null>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  finalizeApprovedRequest: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    options: FinalizeApprovedRequestOptions
  ) => Promise<void>;
};

export async function handleApprovalComment<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  commenter: string,
  callbacks: ApprovalCommentHandlingCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType,
    ValidateResultType
  >
): Promise<void> {
  const eff = callbacks.resolveEffectiveConstants(context);
  const requestType = callbacks.resolveEffectiveRequestType(template, parsedFormData);

  const configuredApprovers = callbacks.resolveApproversForRequestType(
    context,
    requestType,
    eff.approverUsernames,
    eff.approverPoolUsernames
  );

  const reviewOk = await callbacks.ensureReviewLabelsPresentOnIssue(context, params, issue, eff);
  if (!reviewOk) {
    await callbacks.postOnce(
      context,
      params,
      'Approval ignored: request is not in review state. Please resolve validation issues and let the bot route it back to review first.',
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  let allowedApprovers = callbacks.uniqLogins([...(configuredApprovers || [])]);
  let okApprover = callbacks.isAuthorizedApprover(commenter, issue.user?.login, allowedApprovers);

  if (!okApprover) {
    const hookApprovers = await callbacks.resolveAdditionalIssueApproversFromApprovalHook(
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType
    );

    allowedApprovers = callbacks.uniqLogins([...(configuredApprovers || []), ...(hookApprovers || [])]);
    okApprover = callbacks.isAuthorizedApprover(commenter, issue.user?.login, allowedApprovers);
  }
  if (!okApprover) {
    const hasConfiguredApprovers = allowedApprovers.length > 0;
    const reason = hasConfiguredApprovers
      ? `Approval ignored: commenter ${commenter} is not an allowed approver for this request type.`
      : `Approval ignored: commenter ${commenter} is not allowed to self-approve this request.`;

    await callbacks.postOnce(context, params, reason, { minimizeTag: 'nsreq:approval-info' });
    return;
  }

  const reval = await callbacks.validateRequestIssue(context, params, issue, {
    template,
    formData: parsedFormData,
  });

  if (reval.errors?.length) {
    const listFallback = (reval.errors || []).map((e) => `- ${e}`).join('\n');
    const message =
      reval.errorsFormattedSingle?.trim() ||
      reval.errorsFormatted?.trim() ||
      listFallback ||
      'Unknown validation error.';

    const normalizedIssues = (reval.validationIssues || []).map((validationIssue) => ({
      field: toStringTrim(validationIssue.path) || 'details',
      message: toStringTrim(validationIssue.message),
    }));

    await callbacks.postOnce(
      context,
      params,
      buildDetectedIssuesBody(message, normalizeMachineReadableIssues(normalizedIssues)),
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
      reval.template || template,
      parsedFormData,
      reval.namespace
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
  } catch (e: unknown) {
    callbacks.log(
      context,
      'warn',
      { err: e instanceof Error ? e.message : String(e) },
      'parent chain check failed during approval'
    );
  }

  await callbacks.finalizeApprovedRequest(context, params, issue, template, parsedFormData, {
    approvalPrefix: `Approved by @${commenter}`,
  });
}
