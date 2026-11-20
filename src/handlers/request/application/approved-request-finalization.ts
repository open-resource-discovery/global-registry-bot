import { toStringTrim } from '../domain/login-utils.js';
import { parseIssueNumberFromPrBody } from '../pr/reconciliation.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type IssueLikeBase = { number: number };

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type PullRequestLikeBase = {
  number: number;
  body?: string | null;
  base?: { ref?: string | null } | null;
};

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
  /** Return the configured or repository-default base branch, or null when not determinable. */
  resolveExpectedBaseBranch: (context: ContextType) => Promise<string | null>;
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

  const resourceName = callbacks.extractResourceNameFromForm(parsedFormData, template).replaceAll(' ', ' ').trim();
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
    // Verify the found PR targets the expected base branch before accepting the shortcut.
    const expectedBase = await callbacks.resolveExpectedBaseBranch(context);
    if (expectedBase === null) {
      await callbacks.postOnce(
        context,
        params,
        'Cannot finalize: the target base branch could not be determined (missing configuration and repository API unavailable). Please check the bot configuration or try again.',
        { minimizeTag: 'nsreq:approval-info' }
      );
      return;
    }
    const pr = existing[0];
    const actualBase = toStringTrim(pr.base?.ref);

    if (!actualBase || actualBase !== expectedBase) {
      await callbacks.postOnce(
        context,
        params,
        actualBase
          ? `Cannot finalize: found PR #${pr.number} targets branch '${actualBase}' but expected '${expectedBase}'. Please check the open PR and reapprove after correcting the base branch.`
          : `Cannot finalize: found PR #${pr.number} has no base branch recorded. Please check the open PR and reapprove.`,
        { minimizeTag: 'nsreq:approval-info' }
      );
      return;
    }

    // Validate PR body independently using the reconciliation-path parser.
    // findOpenIssuePrs uses first-match logic and may accept a PR whose body contains
    // conflicting issue references (e.g. source:#1640 and fix:#999). Parsing with the
    // same function used in reconciliation.ts ensures consistent conflict detection.
    const prBodyIssue = parseIssueNumberFromPrBody(pr.body);
    if (prBodyIssue !== issue.number) {
      await callbacks.postOnce(
        context,
        params,
        prBodyIssue === null
          ? `Cannot finalize: found PR #${pr.number} has conflicting or missing issue references in its body. Please check the open PR and reapprove.`
          : `Cannot finalize: found PR #${pr.number} references issue #${prBodyIssue} but expected #${issue.number}. Please check the open PR and reapprove.`,
        { minimizeTag: 'nsreq:approval-info' }
      );
      return;
    }

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
    const body = lead ? `${lead}. PR already open: #${pr.number}` : `PR already open: #${pr.number}`;

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
