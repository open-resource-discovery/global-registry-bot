import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type IssueLikeBase = { number: number };

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type PullRequestLikeBase = { number: number };

type FinalizeApprovedRequestOptions = {
  approvalPrefix?: string;
  approvalComment?: string;
  autoApproved?: boolean;
};

export type ApprovedRequestFinalizationCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType,
  PullRequestType extends PullRequestLikeBase,
> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  extractResourceNameFromForm: (formData: FormDataType, template: TemplateType) => string;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  resolveAdditionalIssueApproversFromApprovalHook: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType?: string
  ) => Promise<string[]>;
  findOpenIssuePrs: (
    context: ContextType,
    repoInfo: { owner: string; repo: string },
    issueNumber: number
  ) => Promise<PullRequestType[]>;
  applyApprovedRequestState: (
    context: ContextType,
    params: ParamsType,
    effectiveConstants: EffectiveConstantsType
  ) => Promise<void>;
  addApprovedLabelToPr: (
    context: ContextType,
    repoInfo: { owner: string; repo: string },
    prNumber: number
  ) => Promise<void>;
  ensureAssigneesPresent: (context: ContextType, params: IssueParamsBase, assignees: string[]) => Promise<void>;
  createRequestPrWithRecovery: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    parsedFormData: FormDataType,
    template: TemplateType,
    resourceName: string
  ) => Promise<{ number: number }>;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
};

export async function finalizeApprovedRequest<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType,
  PullRequestType extends PullRequestLikeBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  options: FinalizeApprovedRequestOptions,
  callbacks: ApprovedRequestFinalizationCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType,
    PullRequestType
  >
): Promise<void> {
  const eff = callbacks.resolveEffectiveConstants(context);
  const approvalPrefix = toStringTrim(options.approvalPrefix);
  const approvalComment = toStringTrim(options.approvalComment);
  const autoApproved = options.autoApproved === true;

  const resourceName = callbacks.extractResourceNameFromForm(parsedFormData, template).replaceAll('\u00a0', ' ').trim();
  if (!resourceName) {
    await callbacks.postOnce(
      context,
      params,
      'Cannot create PR: missing resource name in the form (expected identifier, product-id or namespace).',
      { minimizeTag: 'nsreq:config' }
    );
    return;
  }

  const requestType = callbacks.resolveEffectiveRequestType(template, parsedFormData);
  const hookApprovers = await callbacks.resolveAdditionalIssueApproversFromApprovalHook(
    context,
    params,
    issue,
    template,
    parsedFormData,
    requestType
  );

  const repoInfo = { owner: params.owner, repo: params.repo };

  let existing: PullRequestType[];
  try {
    existing = await callbacks.findOpenIssuePrs(context, repoInfo, issue.number);
  } catch (e: unknown) {
    const raw = (e instanceof Error ? e.message : String(e)).trim();
    const stripped = raw.replace(/https?:\/\/\S+/gi, '').trim();
    const msg = stripped || 'GitHub could not be checked for an existing pull request.';
    await callbacks.postOnce(context, params, `Failed to create Pull Request: ${msg}`, {
      minimizeTag: 'nsreq:approval-info',
    });
    return;
  }

  if (existing.length) {
    await callbacks.applyApprovedRequestState(context, params, eff);

    if (autoApproved) {
      await callbacks.addApprovedLabelToPr(context, repoInfo, existing[0].number);
    }

    await callbacks.ensureAssigneesPresent(
      context,
      { owner: params.owner, repo: params.repo, issue_number: existing[0].number },
      hookApprovers
    );

    const lead = [toStringTrim(approvalPrefix), toStringTrim(approvalComment)].filter(Boolean).join('. ');
    const body = lead ? `${lead}. PR already open: #${existing[0].number}` : `PR already open: #${existing[0].number}`;

    await callbacks.postOnce(context, params, body, {
      minimizeTag: 'nsreq:approval-info',
    });
    return;
  }

  try {
    const pr = await callbacks.createRequestPrWithRecovery(
      context,
      params,
      issue,
      parsedFormData,
      template,
      resourceName
    );

    await callbacks.applyApprovedRequestState(context, params, eff);

    if (autoApproved) {
      await callbacks.addApprovedLabelToPr(context, repoInfo, pr.number);
    }

    await callbacks.ensureAssigneesPresent(
      context,
      { owner: params.owner, repo: params.repo, issue_number: pr.number },
      hookApprovers
    );

    const lead = [toStringTrim(approvalPrefix), toStringTrim(approvalComment)].filter(Boolean).join('. ');
    const body = lead ? `${lead}. Opened PR: #${pr.number}` : `Opened PR: #${pr.number}`;

    await callbacks.postOnce(context, params, body, {
      minimizeTag: 'nsreq:approval-info',
    });
  } catch (e: unknown) {
    // request-pr-creation-recovery.ts already formats the user-facing message as
    // "Failed to create Pull Request: <stage-aware detail>". Surface it directly
    // to avoid a double "Failed to create Pull Request: Failed to create Pull Request:" prefix.
    const msg = e instanceof Error ? e.message : String(e);
    const prefix = 'Failed to create Pull Request:';
    const body = msg.startsWith(prefix) ? msg : `${prefix} ${msg}`;

    await callbacks.postOnce(context, params, body, {
      minimizeTag: 'nsreq:approval-info',
    });
  }
}
