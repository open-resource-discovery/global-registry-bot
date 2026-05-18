import { normalizeLogin, toStringTrim, uniqLogins } from '../domain/login-utils.js';
import { normalizeApprovalDecision, type ApprovalDecision } from '../domain/approval-decision.js';
import { getUnknownManualApprovers } from '../domain/approval-policy.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = RepoInfoBase & {
  issue_number: number;
};

type UserLikeBase = {
  login?: string | null;
};

type IssueLikeBase = {
  labels?: unknown;
  user?: UserLikeBase | null;
};

type TemplateMetaBase = {
  requestType?: string;
};

type TemplateLikeBase = {
  _meta?: TemplateMetaBase;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
};

type ContextWithOctokit = {
  octokit: {
    issues: {
      get: (args: IssueParamsBase) => Promise<{ data?: unknown }>;
      addLabels: (args: IssueParamsBase & { labels: string[] }) => Promise<unknown>;
      removeLabel: (args: IssueParamsBase & { name: string }) => Promise<unknown>;
      addAssignees: (args: IssueParamsBase & { assignees: string[] }) => Promise<unknown>;
    };
  };
};

export type IssueStateReviewerOperationsCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  toLabelNames: (labels: unknown) => string[];
  normalizeKey: (value: unknown) => string;
  resolveWorkflowLabel: (context: ContextType, key: string, fallback: string) => string;
  labelsMatching: (labels: string[], expected: string) => string[];
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  extractResourceNameFromForm: (formData: FormDataType, template: TemplateType) => string;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  runApprovalHook: (
    context: ContextType,
    repoInfo: Pick<ParamsType, 'owner' | 'repo'>,
    args: {
      requestType: string;
      namespace?: string | null;
      resourceName?: string | null;
      formData: FormDataType;
      issue: IssueType;
      requestAuthorId?: string | null;
    }
  ) => Promise<ApprovalDecision | boolean>;
  getHttpStatus: (error: unknown) => number | undefined;
  getErrorMessage: (error: unknown) => string;
  log: (context: ContextType, level: 'warn', obj: unknown, msg: string) => void;
};

const ENSURE_LABELS_INFLIGHT = new Map<string, Promise<void>>();

export function issueScopedKey(params: IssueParamsBase, suffix: string): string {
  return `${params.owner}/${params.repo}#${params.issue_number}:${suffix}`.toLowerCase();
}

export async function fetchIssueLabels<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
>(
  context: ContextType,
  params: ParamsType,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueType,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsBase
    >,
    'toLabelNames'
  >
): Promise<string[]> {
  const { data } = await context.octokit.issues.get(params);
  const issue = data as IssueType;
  return callbacks.toLabelNames(issue.labels);
}

export async function ensureLabelsPresentOnce<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
>(
  context: ContextType,
  params: ParamsType,
  labels: string[],
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueType,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsBase
    >,
    'toLabelNames' | 'normalizeKey' | 'getHttpStatus' | 'getErrorMessage' | 'log'
  >
): Promise<void> {
  const targetLabels = Array.from(new Set((labels || []).map(toStringTrim).filter(Boolean)));
  if (!targetLabels.length) return;

  const key = issueScopedKey(params, `labels:${targetLabels.map(callbacks.normalizeKey).sort().join('|')}`);
  const existing = ENSURE_LABELS_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = (async (): Promise<void> => {
    let currentLabels: string[] = [];

    try {
      currentLabels = await fetchIssueLabels(context, params, callbacks);
    } catch {
      currentLabels = [];
    }

    const currentKeys = new Set(currentLabels.map(callbacks.normalizeKey).filter(Boolean));
    const missing = targetLabels.filter((label) => {
      const normalizedKey = callbacks.normalizeKey(label);
      return normalizedKey && !currentKeys.has(normalizedKey);
    });

    if (!missing.length) return;

    try {
      await context.octokit.issues.addLabels({
        ...params,
        labels: missing,
      });
    } catch (error: unknown) {
      const status = callbacks.getHttpStatus(error);
      if (status !== 404) {
        callbacks.log(
          context,
          'warn',
          {
            err: callbacks.getErrorMessage(error),
            labels: missing,
            issueNumber: params.issue_number,
          },
          'failed to ensure labels'
        );
      }
    }
  })().finally(() => {
    ENSURE_LABELS_INFLIGHT.delete(key);
  });

  ENSURE_LABELS_INFLIGHT.set(key, pending);
  await pending;
}

export async function ensureAssigneesPresent<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
>(
  context: ContextType,
  params: ParamsType,
  assignees: string[],
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsBase
    >,
    'getErrorMessage' | 'log'
  >
): Promise<void> {
  const targetAssignees = uniqLogins((assignees || []).map((value) => toStringTrim(value)).filter(Boolean));
  if (!targetAssignees.length) return;

  try {
    const { data } = await context.octokit.issues.get(params);
    const currentAssignees = uniqLogins(
      (((data as Record<string, unknown>)['assignees'] as (Record<string, unknown> | null | undefined)[]) || [])
        .map((item) => normalizeLogin(toStringTrim(item?.login)))
        .filter(Boolean)
    );

    const missing = targetAssignees.filter(
      (candidate) =>
        !currentAssignees.some(
          (existing) => normalizeLogin(existing).toLowerCase() === normalizeLogin(candidate).toLowerCase()
        )
    );

    if (!missing.length) return;

    await context.octokit.issues.addAssignees({
      ...params,
      assignees: missing,
    });
  } catch (error: unknown) {
    callbacks.log(
      context,
      'warn',
      {
        err: error instanceof Error ? error.message : String(error),
        issueNumber: params.issue_number,
        assignees: targetAssignees,
      },
      'failed to ensure assignees'
    );
  }
}

export async function ensureReviewLabelsPresentOnIssue<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  eff: EffectiveConstantsType,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueType,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsType
    >,
    'toLabelNames' | 'normalizeKey'
  >
): Promise<boolean> {
  const cfgKeys = (eff.reviewRequestedLabels || []).map(callbacks.normalizeKey);
  if (!cfgKeys.length) return true;

  let labels = callbacks.toLabelNames(issue.labels);

  try {
    labels = await fetchIssueLabels(context, params, callbacks);
  } catch {
    // keep payload labels as fallback
  }

  return labels.some((label) => {
    const normalized = callbacks.normalizeKey(label);
    return cfgKeys.some(
      (cfgKey) => normalized === cfgKey || normalized.includes(cfgKey) || cfgKey.includes(normalized)
    );
  });
}

export async function removeReviewPendingLabelsAfterApproval<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  eff: EffectiveConstantsType,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsType
    >,
    'toLabelNames' | 'normalizeKey' | 'getHttpStatus' | 'log'
  >
): Promise<void> {
  const approvedCfg = toStringTrim(eff.labelOnApproved);
  const pendingCfg = (eff.reviewRequestedLabels || []).map(toStringTrim).filter(Boolean);

  if (!approvedCfg || !pendingCfg.length) return;

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params, callbacks);
  } catch {
    return;
  }

  const approvedKey = callbacks.normalizeKey(approvedCfg);
  const hasApproved = labels.some((label) => {
    const normalized = callbacks.normalizeKey(label);
    return normalized === approvedKey || normalized.includes(approvedKey) || approvedKey.includes(normalized);
  });

  if (!hasApproved) return;

  const pendingKeys = pendingCfg.map(callbacks.normalizeKey);

  const toRemove = labels.filter((label) => {
    const normalized = callbacks.normalizeKey(label);
    return pendingKeys.some(
      (pendingKey) => normalized === pendingKey || normalized.includes(pendingKey) || pendingKey.includes(normalized)
    );
  });

  for (const label of toRemove) {
    try {
      await context.octokit.issues.removeLabel({ ...params, name: label });
    } catch (error: unknown) {
      if (callbacks.getHttpStatus(error) !== 404) {
        callbacks.log(
          context,
          'warn',
          { err: error instanceof Error ? error.message : String(error), label },
          'failed to remove review pending label after approval'
        );
      }
    }
  }
}

export async function removeExactLabelsFromIssue<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
>(
  context: ContextType,
  params: ParamsType,
  labelsToRemove: string[],
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsBase
    >,
    'getHttpStatus' | 'log'
  >
): Promise<void> {
  for (const label of labelsToRemove) {
    const name = toStringTrim(label);
    if (!name) continue;

    try {
      await context.octokit.issues.removeLabel({ ...params, name });
    } catch (error: unknown) {
      if (callbacks.getHttpStatus(error) !== 404) {
        callbacks.log(
          context,
          'warn',
          { err: error instanceof Error ? error.message : String(error), label: name },
          'failed to remove label'
        );
      }
    }
  }
}

export async function removeProgressStatusLabels<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  currentLabels: string[] | undefined,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsType
    >,
    'toLabelNames' | 'resolveWorkflowLabel' | 'labelsMatching' | 'getHttpStatus' | 'log'
  >
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params, callbacks);
    } catch {
      return;
    }
  }

  const authorActionLabel = callbacks.resolveWorkflowLabel(context, 'authorAction', 'Requester Action');
  const approverActionLabel = callbacks.resolveWorkflowLabel(context, 'approverAction', 'Review Pending');
  const parentOwnerActionLabel = callbacks.resolveWorkflowLabel(context, 'parentOwnerAction', 'Parent Owner Action');

  const toRemove = new Set<string>([
    ...callbacks.labelsMatching(labels, authorActionLabel),
    ...callbacks.labelsMatching(labels, approverActionLabel),
    ...callbacks.labelsMatching(labels, parentOwnerActionLabel),
  ]);

  if (!toRemove.size) return;
  await removeExactLabelsFromIssue(context, params, Array.from(toRemove), callbacks);
}

export async function removeRejectedStatusLabel<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
>(
  context: ContextType,
  params: ParamsType,
  currentLabels: string[] | undefined,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsBase
    >,
    'toLabelNames' | 'labelsMatching' | 'getHttpStatus' | 'log'
  >
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params, callbacks);
    } catch {
      return;
    }
  }

  const toRemove = callbacks.labelsMatching(labels, 'Rejected');
  if (!toRemove.length) return;
  await removeExactLabelsFromIssue(context, params, toRemove, callbacks);
}

export async function applyApprovedRequestState<
  ContextType extends ContextWithOctokit,
  ParamsType extends IssueParamsBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  eff: EffectiveConstantsType,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsType
    >,
    'toLabelNames' | 'normalizeKey' | 'labelsMatching' | 'resolveWorkflowLabel' | 'getHttpStatus' | 'log'
  >
): Promise<void> {
  try {
    if (eff.labelOnApproved) {
      await context.octokit.issues.addLabels({ ...params, labels: [eff.labelOnApproved] });
    }
  } catch {
    // ignore
  }

  await removeReviewPendingLabelsAfterApproval(context, params, eff, callbacks);

  try {
    const labelsAfter = await fetchIssueLabels(context, params, callbacks);
    const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
    if (callbacks.labelsMatching(labelsAfter, approvedLabel).length) {
      await removeProgressStatusLabels(context, params, labelsAfter, callbacks);
      await removeRejectedStatusLabel(context, params, labelsAfter, callbacks);
    }
  } catch {
    // ignore
  }
}

export async function addApprovedLabelToPr<
  ContextType extends ContextWithOctokit,
  RepoInfoType extends RepoInfoBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  prNumber: number,
  options: { skipStateCleanup?: boolean } | undefined,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      IssueParamsBase,
      IssueLikeBase,
      TemplateLikeBase,
      FormDataBase,
      EffectiveConstantsType
    >,
    | 'resolveEffectiveConstants'
    | 'toLabelNames'
    | 'normalizeKey'
    | 'labelsMatching'
    | 'resolveWorkflowLabel'
    | 'getHttpStatus'
    | 'log'
  >
): Promise<void> {
  const eff = callbacks.resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  if (!approvedLabel) return;

  const params: IssueParamsBase = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    issue_number: prNumber,
  };

  try {
    await context.octokit.issues.addLabels({
      ...params,
      labels: [approvedLabel],
    });
  } catch {
    return;
  }

  if (options?.skipStateCleanup) return;

  await removeReviewPendingLabelsAfterApproval(context, params, eff, callbacks);

  try {
    const labelsAfter = await fetchIssueLabels(context, params, callbacks);

    if (callbacks.labelsMatching(labelsAfter, approvedLabel).length) {
      await removeProgressStatusLabels(context, params, labelsAfter, callbacks);
      await removeRejectedStatusLabel(context, params, labelsAfter, callbacks);
    }
  } catch {
    // best effort cleanup only
  }
}

export async function resolveAdditionalIssueApproversFromApprovalHook<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  requestType: string | undefined,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      EffectiveConstantsBase
    >,
    'resolveEffectiveRequestType' | 'extractResourceNameFromForm' | 'runApprovalHook'
  >
): Promise<string[]> {
  const effectiveRequestType = requestType || callbacks.resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = callbacks.extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const repoInfo = { owner: params.owner, repo: params.repo };
    const decision = normalizeApprovalDecision(
      await callbacks.runApprovalHook(context, repoInfo, {
        requestType: effectiveRequestType,
        namespace: resourceName,
        resourceName,
        formData: parsedFormData,
        issue,
        requestAuthorId: normalizeLogin(issue.user?.login),
      })
    );

    return uniqLogins((decision.approvers || []).filter(Boolean));
  } catch {
    return [];
  }
}

export async function resolveManualReviewApproverOverrideFromApprovalHook<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  requestType: string | undefined,
  callbacks: Pick<
    IssueStateReviewerOperationsCallbacks<
      ContextType,
      ParamsType,
      IssueType,
      TemplateType,
      FormDataType,
      EffectiveConstantsBase
    >,
    'resolveEffectiveRequestType' | 'extractResourceNameFromForm' | 'runApprovalHook'
  >
): Promise<string[]> {
  const effectiveRequestType = requestType || callbacks.resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = callbacks.extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const repoInfo = { owner: params.owner, repo: params.repo };
    const decision = normalizeApprovalDecision(
      await callbacks.runApprovalHook(context, repoInfo, {
        requestType: effectiveRequestType,
        namespace: resourceName,
        resourceName,
        formData: parsedFormData,
        issue,
        requestAuthorId: normalizeLogin(issue.user?.login),
      })
    );

    return getUnknownManualApprovers(decision);
  } catch {
    return [];
  }
}
