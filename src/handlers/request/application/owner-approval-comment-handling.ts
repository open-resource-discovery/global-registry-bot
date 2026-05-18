import { buildDetectedIssuesBody, normalizeMachineReadableIssues } from '../domain/machine-readable.js';
import { readIssueBodyForProcessing } from '../domain/issue-body-processing.js';
import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type IssueLikeBase = {
  body?: string | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type ValidationIssueLike = {
  path?: string | null;
  message?: string | null;
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

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type FinalizeApprovedRequestOptions = {
  approvalPrefix?: string;
  approvalComment?: string;
  autoApproved?: boolean;
};

export type OwnerApprovalCommentHandlingCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
> = {
  readContactApprovalMeta: (issueBody: unknown) => ContactApprovalMetaType | null;
  readParentApprovalMeta: (issueBody: unknown) => ParentApprovalMetaType | null;
  normalizeLogin: (value: unknown) => string;
  uniqLogins: (values: string[]) => string[];
  normalizeKey: (value: unknown) => string;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
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
  parseForm: (body: string, template: TemplateType) => FormDataType;
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  ensureContactApprovalMarker: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    meta: ContactApprovalMetaType | null
  ) => Promise<boolean>;
  ensureParentApprovalMarker: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    meta: ParentApprovalMetaType | null
  ) => Promise<boolean>;
  buildApprovedContactApprovalMeta: (args: {
    target: string | null | undefined;
    owners: string[];
    approvedBy: string;
    approvedAt: string;
  }) => ContactApprovalMetaType;
  buildApprovedParentApprovalMeta: (args: {
    parent: string | null | undefined;
    target: string | null | undefined;
    owners: string[];
    approvedBy: string;
    approvedAt: string;
  }) => ParentApprovalMetaType;
  maybeHandleApprovalDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType: string,
    namespace: string,
    options: unknown
  ) => Promise<ApprovalHandlingResult>;
  buildApprovalDecisionDispatchOptions: () => unknown;
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
  setParentOwnerActionState: (context: ContextType, params: ParamsType) => Promise<void>;
  assignParentOwnersForApproval: (context: ContextType, params: ParamsType, owners: string[]) => Promise<void>;
  clearParentOwnerActionState: (context: ContextType, params: ParamsType) => Promise<void>;
  isSubContextRequestType: (requestType: unknown) => boolean;
  finalizeApprovedRequest: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    options: FinalizeApprovedRequestOptions
  ) => Promise<void>;
};

function buildValidationBody(validationIssues: ValidationIssueLike[] | undefined, message: string): string {
  return buildDetectedIssuesBody(
    message,
    normalizeMachineReadableIssues(
      (validationIssues || []).map((validationIssue) => ({
        field: toStringTrim(validationIssue.path) || 'details',
        message: toStringTrim(validationIssue.message),
      }))
    )
  );
}

export async function handleSystemContactOwnerApprovalIfNeeded<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  commenter: string,
  callbacks: OwnerApprovalCommentHandlingCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType,
    ContactApprovalMetaType,
    ParentApprovalMetaType
  >
): Promise<boolean> {
  const meta = callbacks.readContactApprovalMeta(issue.body);
  if (!meta) return false;
  if (callbacks.normalizeLogin(meta.approvedBy)) return false;

  const commenterLogin = callbacks.normalizeLogin(commenter);
  const owners = callbacks.uniqLogins(meta.owners || []);
  const isOwner = owners.some((owner) => owner.toLowerCase() === commenterLogin.toLowerCase());
  const tagBase = `nsreq:contact-approval:${callbacks.normalizeKey(meta.target)}`;

  if (!isOwner) {
    const mentions = owners.map((owner) => `@${owner}`).join(' ');
    await callbacks.postOnce(
      context,
      params,
      `Approval ignored: this request requires contact owner approval for \`${meta.target}\` first.\n\n${mentions}`,
      { minimizeTag: `${tagBase}:pending` }
    );
    return true;
  }

  const reval = await callbacks.validateRequestIssue(context, params, issue, {
    template,
    formData: parsedFormData,
  });

  if (reval.errors?.length) {
    const listFallback = (reval.errors || []).map((error) => `- ${error}`).join('\n');
    const message =
      reval.errorsFormattedSingle?.trim() ||
      reval.errorsFormatted?.trim() ||
      listFallback ||
      'Unknown validation error.';

    await callbacks.postOnce(context, params, buildValidationBody(reval.validationIssues, message), {
      minimizeTag: 'nsreq:validation',
    });
    await callbacks.setStateLabel(context, params, issue, 'author');
    return true;
  }

  const tpl = reval.template || template;
  const bodyStr = readIssueBodyForProcessing(issue.body);
  const parsedNow = callbacks.parseForm(bodyStr, tpl);
  const snapshotHash = callbacks.calcSnapshotHash(parsedNow, tpl, bodyStr);
  const effRt = callbacks.resolveEffectiveRequestType(tpl, parsedNow);
  const approvedAt = new Date().toISOString();

  await callbacks.ensureContactApprovalMarker(
    context,
    params,
    issue,
    callbacks.buildApprovedContactApprovalMeta({
      target: meta.target,
      owners,
      approvedBy: commenterLogin,
      approvedAt,
    })
  );

  const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt,
    reval.namespace,
    callbacks.buildApprovalDecisionDispatchOptions()
  );

  if (approvalOutcome !== 'continue') return true;

  await callbacks.postOnce(
    context,
    params,
    `Contact owner approved by @${commenterLogin}. Continuing with standard review.`,
    {
      minimizeTag: `${tagBase}:approved`,
    }
  );

  const manualApproversOverride = await callbacks.resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await callbacks.resolveAdditionalIssueApproversFromApprovalHook(context, params, issue, tpl, parsedNow, effRt);

  await callbacks.handoverToCpa(context, params, issue, reval.nsType, reval.namespace, [], {
    snapshotHash,
    requestType: effRt,
    extraApprovers: hookApprovers,
    manualApproversOverride,
    ...callbacks.buildReviewHandoverOptions(),
  });

  return true;
}

export async function handleParentOwnerApprovalIfNeeded<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  ValidateResultType extends ValidateRequestIssueResultBase<TemplateType>,
  ContactApprovalMetaType extends ContactApprovalMetaBase,
  ParentApprovalMetaType extends ParentApprovalMetaBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  commenter: string,
  callbacks: OwnerApprovalCommentHandlingCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    ValidateResultType,
    ContactApprovalMetaType,
    ParentApprovalMetaType
  >
): Promise<boolean> {
  const meta = callbacks.readParentApprovalMeta(issue.body);
  if (!meta) return false;
  if (callbacks.normalizeLogin(meta.approvedBy)) return false;

  const commenterLogin = callbacks.normalizeLogin(commenter);
  const owners = callbacks.uniqLogins(meta.owners || []);
  const isOwner = owners.some((owner) => owner.toLowerCase() === commenterLogin.toLowerCase());

  const tagBase = `nsreq:parent-approval:${callbacks.normalizeKey(meta.parent)}:${callbacks.normalizeKey(meta.target)}`;

  if (!isOwner) {
    await callbacks.setParentOwnerActionState(context, params);
    await callbacks.assignParentOwnersForApproval(context, params, owners);

    const mentions = owners.map((owner) => `@${owner}`).join(' ');
    await callbacks.postOnce(
      context,
      params,
      `Approval ignored: this request requires parent owner approval for \`${meta.parent}\` first.\n\n${mentions}`,
      { minimizeTag: `${tagBase}:pending` }
    );
    return true;
  }

  const reval = await callbacks.validateRequestIssue(context, params, issue, { template, formData: parsedFormData });
  if (reval.errors?.length) {
    const listFallback = (reval.errors || []).map((error) => `- ${error}`).join('\n');
    const message =
      reval.errorsFormattedSingle?.trim() ||
      reval.errorsFormatted?.trim() ||
      listFallback ||
      'Unknown validation error.';
    await callbacks.postOnce(context, params, buildValidationBody(reval.validationIssues, message), {
      minimizeTag: 'nsreq:validation',
    });
    await callbacks.clearParentOwnerActionState(context, params);
    await callbacks.setStateLabel(context, params, issue, 'author');
    return true;
  }

  const tpl = reval.template || template;
  const bodyStr = readIssueBodyForProcessing(issue.body);
  const parsedNow = callbacks.parseForm(bodyStr, tpl);
  const snapshotHash = callbacks.calcSnapshotHash(parsedNow, tpl, bodyStr);
  const effRt = callbacks.resolveEffectiveRequestType(tpl, parsedNow);
  const approvedAt = new Date().toISOString();

  await callbacks.ensureParentApprovalMarker(
    context,
    params,
    issue,
    callbacks.buildApprovedParentApprovalMeta({
      parent: meta.parent,
      target: meta.target,
      owners,
      approvedBy: commenterLogin,
      approvedAt,
    })
  );

  await callbacks.clearParentOwnerActionState(context, params);

  const approvalOutcome = await callbacks.maybeHandleApprovalDecision(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt,
    reval.namespace,
    callbacks.buildApprovalDecisionDispatchOptions()
  );

  if (approvalOutcome !== 'continue') return true;

  if (callbacks.isSubContextRequestType(effRt)) {
    await callbacks.finalizeApprovedRequest(context, params, issue, tpl, parsedNow, {
      approvalPrefix: `Approved by parent namespace owner @${commenterLogin}`,
    });
    return true;
  }

  await callbacks.postOnce(
    context,
    params,
    `Parent namespace approved by @${commenterLogin}. Continuing with standard review.`,
    {
      minimizeTag: `${tagBase}:approved`,
    }
  );

  const manualApproversOverride = await callbacks.resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await callbacks.resolveAdditionalIssueApproversFromApprovalHook(context, params, issue, tpl, parsedNow, effRt);

  await callbacks.handoverToCpa(context, params, issue, reval.nsType, reval.namespace, [], {
    snapshotHash,
    requestType: effRt,
    extraApprovers: hookApprovers,
    manualApproversOverride,
    ...callbacks.buildReviewHandoverOptions(),
  });

  return true;
}
