import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = { owner: string; repo: string; issue_number: number };

type IssueLikeBase = {
  body?: string | null;
  labels?: unknown;
  state?: string | null;
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  labelOnApproved?: string | null;
  approverUsernames?: string[];
  approverPoolUsernames?: string[];
};

type WorkflowLabelKey = 'authorAction' | 'approverAction' | 'parentOwnerAction';

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export type IssueWorkflowGuardCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  hasIssueFormInputs: (issue: IssueType | null | undefined) => boolean;
  loadTemplateWithLabelRefresh: (context: ContextType, params: ParamsType, issue: IssueType) => Promise<TemplateType>;
  parseForm: (body: string, template: TemplateType) => FormDataType;
  createEmptyFormData: () => FormDataType;
  readIssueBodyForProcessing: (body: unknown) => string;
  isRequestIssue: (
    context: ContextType,
    template: TemplateType | null | undefined,
    parsedFormData: FormDataType
  ) => boolean;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  toLabelNames: (labels: unknown) => string[];
  fetchIssueLabels: (context: ContextType, params: ParamsType) => Promise<string[]>;
  labelsMatching: (labels: string[], expected: string) => string[];
  removeRejectedStatusLabel: (context: ContextType, params: ParamsType, currentLabels?: string[]) => Promise<void>;
  removeProgressStatusLabels: (context: ContextType, params: ParamsType, currentLabels?: string[]) => Promise<void>;
  removeExactLabelsFromIssue: (context: ContextType, params: ParamsType, labelsToRemove: string[]) => Promise<void>;
  addLabels: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  postOnce: (
    context: ContextType,
    params: ParamsType,
    body: string,
    options?: { minimizeTag?: string }
  ) => Promise<void>;
  updateIssueBody: (context: ContextType, params: ParamsType, body: string) => Promise<void>;
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  readRoutingLockExpected: (issueBody: unknown) => string;
  buildRoutingLockBody: (issueBody: unknown, expectedLabel: string) => string;
  normalizeKey: (value: unknown) => string;
  tryLoadTemplateForLabels: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    labels: string[]
  ) => Promise<TemplateType | null>;
  resolveLockedWorkflowLabelKeys: (context: ContextType) => Set<string>;
  resolveEffectiveRequestType: (template: TemplateType, formData: FormDataType) => string;
  resolveApproverRoutingForRequestType: (
    context: ContextType,
    requestType: string | undefined | null,
    fallbackApprovers: string[],
    fallbackApproversPool: string[]
  ) => {
    approvalUsernames: string[];
    autoAssigneePoolUsernames: string[];
  };
  uniqLogins: (values: string[]) => string[];
  isConfiguredApprover: (login: string | undefined | null, allowedApprovers: string[]) => boolean;
  resolveWorkflowLabel: (context: ContextType, key: WorkflowLabelKey, fallback: string) => string;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

const ROUTING_LOCK_NOTICE_INFLIGHT = new Map<string, Promise<void>>();

function routingNoticeKey<ParamsType extends IssueParamsBase>(params: ParamsType): string {
  return `${params.owner}/${params.repo}#${params.issue_number}`;
}

async function postRoutingLockNoticeOnce<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  expected: string,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<void> {
  const key = routingNoticeKey(params);
  const existing = ROUTING_LOCK_NOTICE_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = Promise.resolve()
    .then(async (): Promise<void> => {
      await callbacks.postOnce(
        context,
        params,
        `Routing label is locked to "${expected}". Manual changes were reverted.`,
        { minimizeTag: 'nsreq:routing-label-lock' }
      );
    })
    .finally(() => {
      ROUTING_LOCK_NOTICE_INFLIGHT.delete(key);
    });

  ROUTING_LOCK_NOTICE_INFLIGHT.set(key, pending);
  await pending;
}

async function isRoutingLabelName<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  labelName: unknown,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const name = toStringTrim(labelName);
  if (!name) return false;

  try {
    return Boolean(await callbacks.tryLoadTemplateForLabels(context, params, issue, [name]));
  } catch {
    return false;
  }
}

async function detectRoutingLabels<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  labels: string[],
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<string[]> {
  const uniqueLabels: string[] = [];
  const seen = new Set<string>();

  for (const label of labels) {
    const name = toStringTrim(label);
    const key = callbacks.normalizeKey(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    uniqueLabels.push(name);
  }

  const routing: string[] = [];
  for (const label of uniqueLabels) {
    const template = await callbacks.tryLoadTemplateForLabels(context, params, issue, [label]);
    if (template) routing.push(label);
  }

  return routing;
}

export async function detectSingleRoutingLabel<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  labels: string[],
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<string> {
  const routing = await detectRoutingLabels(context, params, issue, labels, callbacks);
  return routing.length === 1 ? routing[0] : '';
}

export async function ensureRoutingLockMarker<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  expectedLabel: string,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  if (!expected) return false;

  const current = callbacks.readRoutingLockExpected(issue.body);
  if (callbacks.normalizeKey(current) === callbacks.normalizeKey(expected)) return false;

  const nextBody = callbacks.buildRoutingLockBody(issue.body, expected);

  try {
    await callbacks.updateIssueBody(context, params, nextBody);
    return true;
  } catch {
    return false;
  }
}

export async function enforceRoutingLabelLock<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  expectedLabel: string,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >,
  opts?: { changedLabel?: string }
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  const expectedKey = callbacks.normalizeKey(expected);
  if (!expectedKey) return false;

  let labels: string[] = [];
  try {
    labels = await callbacks.fetchIssueLabels(context, params);
  } catch {
    labels = callbacks.toLabelNames(issue.labels);
  }

  const routingLabels = await detectRoutingLabels(context, params, issue, labels, callbacks);
  const toRemove = routingLabels.filter((label) => callbacks.normalizeKey(label) !== expectedKey);
  const hasExpected = labels.some((label) => callbacks.normalizeKey(label) === expectedKey);

  let changed = false;

  if (toRemove.length) {
    await callbacks.removeExactLabelsFromIssue(context, params, toRemove);
    changed = true;
  }

  if (!hasExpected) {
    try {
      await callbacks.addLabels(context, params, [expected]);
      changed = true;
    } catch {
      // ignore label add errors
    }
  }

  if (changed) {
    const touchedLabel = toStringTrim(opts?.changedLabel);
    const shouldNotify =
      !touchedLabel ||
      callbacks.normalizeKey(touchedLabel) === expectedKey ||
      (await isRoutingLabelName(context, params, issue, touchedLabel, callbacks));

    if (shouldNotify) {
      await postRoutingLockNoticeOnce(context, params, expected, callbacks);
    }
  }

  return changed;
}

export async function handleClosedIssueWorkflow<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >,
  rejectedLabel: string
): Promise<void> {
  if (!process.env.JEST_WORKER_ID) {
    if (!callbacks.hasIssueFormInputs(issue)) return;
  }

  let template: TemplateType;
  try {
    template = await callbacks.loadTemplateWithLabelRefresh(context, params, issue);
  } catch {
    return;
  }

  const parsedFormData = template
    ? callbacks.parseForm(callbacks.readIssueBodyForProcessing(issue.body), template)
    : callbacks.createEmptyFormData();
  if (!callbacks.isRequestIssue(context, template, parsedFormData)) return;

  const eff = callbacks.resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

  let labels: string[] = [];
  try {
    labels = await callbacks.fetchIssueLabels(context, params);
  } catch {
    labels = callbacks.toLabelNames(issue.labels);
  }

  const hasApproved = callbacks.labelsMatching(labels, approvedLabel).length > 0;
  if (hasApproved) {
    await callbacks.removeRejectedStatusLabel(context, params, labels);
    await callbacks.removeProgressStatusLabels(context, params, labels);
    return;
  }

  const hasRejected = callbacks.labelsMatching(labels, rejectedLabel).length > 0;
  if (!hasRejected) {
    try {
      await callbacks.addLabels(context, params, [rejectedLabel]);
    } catch (error: unknown) {
      callbacks.log(
        context,
        'warn',
        { err: error instanceof Error ? error.message : String(error), label: rejectedLabel },
        'failed to add rejected status label'
      );
    }
  }

  try {
    labels = await callbacks.fetchIssueLabels(context, params);
  } catch {
    // best effort
  }

  if (callbacks.labelsMatching(labels, rejectedLabel).length) {
    await callbacks.removeProgressStatusLabels(context, params, labels);

    const approvedMatches = callbacks.labelsMatching(labels, approvedLabel);
    if (approvedMatches.length) {
      await callbacks.removeExactLabelsFromIssue(context, params, approvedMatches);
    }
  }
}

export async function handleIssueLabelWorkflow<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  sender: SenderType,
  action: string,
  changedLabel: string,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >,
  labels: {
    requesterAction: string;
    reviewPending: string;
    parentOwnerAction: string;
    rejected: string;
  }
): Promise<void> {
  if (!process.env.JEST_WORKER_ID) {
    if (!callbacks.hasIssueFormInputs(issue)) return;
  }

  let currentLabels = callbacks.toLabelNames(issue.labels);

  const expectedRouting = callbacks.readRoutingLockExpected(issue.body);
  const hasRoutingLock = Boolean(expectedRouting);

  if (expectedRouting) {
    const enforced = await enforceRoutingLabelLock(context, params, issue, expectedRouting, callbacks, {
      changedLabel,
    });
    if (enforced) {
      try {
        currentLabels = await callbacks.fetchIssueLabels(context, params);
      } catch {
        // ignore
      }
    }
  }

  let template: TemplateType | null = null;
  let parsedFormData = callbacks.createEmptyFormData();

  try {
    template = await callbacks.loadTemplateWithLabelRefresh(context, params, issue);
    parsedFormData = template
      ? callbacks.parseForm(callbacks.readIssueBodyForProcessing(issue.body), template)
      : callbacks.createEmptyFormData();
  } catch {
    if (!hasRoutingLock) return;
  }

  if (!hasRoutingLock && !callbacks.isRequestIssue(context, template, parsedFormData)) return;

  const eff = callbacks.resolveEffectiveConstants(context);
  const authorActionLabel = callbacks.resolveWorkflowLabel(context, 'authorAction', labels.requesterAction);
  const approverActionLabel = callbacks.resolveWorkflowLabel(context, 'approverAction', labels.reviewPending);
  const parentOwnerActionLabel = callbacks.resolveWorkflowLabel(context, 'parentOwnerAction', labels.parentOwnerAction);

  const authorActionKey = callbacks.normalizeKey(authorActionLabel);
  const approverActionKey = callbacks.normalizeKey(approverActionLabel);
  const isProgressStateLabel = (key: string): boolean => key === authorActionKey || key === approverActionKey;

  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  const lockedKeys = callbacks.resolveLockedWorkflowLabelKeys(context);
  const changedKey = callbacks.normalizeKey(changedLabel);

  const parentOwnerActionKey = callbacks.normalizeKey(parentOwnerActionLabel);
  if (parentOwnerActionKey) lockedKeys.add(parentOwnerActionKey);

  const effectiveRequestType = template ? callbacks.resolveEffectiveRequestType(template, parsedFormData) : '';
  const approverRouting = effectiveRequestType
    ? callbacks.resolveApproverRoutingForRequestType(
        context,
        effectiveRequestType,
        eff.approverUsernames || [],
        eff.approverPoolUsernames || []
      )
    : {
        approvalUsernames: callbacks.uniqLogins([
          ...(eff.approverUsernames || []),
          ...(eff.approverPoolUsernames || []),
        ]),
        autoAssigneePoolUsernames: callbacks.uniqLogins(eff.approverPoolUsernames || []),
      };

  const senderIsConfiguredApprover = callbacks.isConfiguredApprover(sender?.login, approverRouting.approvalUsernames);

  const managedWorkflowKeys = new Set<string>(Array.from(lockedKeys));
  for (const label of [
    authorActionLabel,
    approverActionLabel,
    parentOwnerActionLabel,
    approvedLabel,
    labels.rejected,
  ]) {
    const key = callbacks.normalizeKey(label);
    if (key) managedWorkflowKeys.add(key);
  }

  if (senderIsConfiguredApprover && changedKey && managedWorkflowKeys.has(changedKey)) {
    return;
  }

  if (changedKey && lockedKeys.has(changedKey) && !isProgressStateLabel(changedKey)) {
    const isManualApprovedAdd =
      action === 'labeled' && callbacks.labelsMatching([changedLabel], approvedLabel).length > 0;

    if (!isManualApprovedAdd) {
      if (action === 'labeled') {
        await callbacks.removeExactLabelsFromIssue(context, params, [changedLabel]);
      } else if (action === 'unlabeled') {
        try {
          await callbacks.addLabels(context, params, [changedLabel]);
        } catch {
          // ignore label add errors
        }
      }

      await callbacks.postOnce(
        context,
        params,
        `Label "${changedLabel}" was reverted. Workflow labels from config are managed by the bot and cannot be changed manually.`,
        { minimizeTag: 'nsreq:workflow-label-lock' }
      );

      return;
    }
  }

  if (action === 'labeled' && callbacks.labelsMatching([changedLabel], approvedLabel).length) {
    const approvedMatches = callbacks.labelsMatching(currentLabels, approvedLabel);
    await callbacks.removeExactLabelsFromIssue(context, params, approvedMatches);

    const hasAuthor = callbacks.labelsMatching(currentLabels, authorActionLabel).length > 0;
    const hasReview = callbacks.labelsMatching(currentLabels, approverActionLabel).length > 0;
    await callbacks.setStateLabel(context, params, issue, hasAuthor ? 'author' : hasReview ? 'review' : 'review');

    await callbacks.postOnce(
      context,
      params,
      'Approved label change reverted. Please comment "Approved" to approve a request.',
      { minimizeTag: 'nsreq:label-guard' }
    );
    return;
  }

  if (
    action === 'labeled' &&
    callbacks.labelsMatching([changedLabel], labels.rejected).length &&
    toStringTrim(issue.state).toLowerCase() !== 'closed'
  ) {
    const rejectedMatches = callbacks.labelsMatching(currentLabels, labels.rejected);
    await callbacks.removeExactLabelsFromIssue(context, params, rejectedMatches);

    await callbacks.postOnce(
      context,
      params,
      'Rejected label change reverted. Rejected is set automatically when a request is closed without approval.',
      { minimizeTag: 'nsreq:label-guard' }
    );
    return;
  }

  if (toStringTrim(issue.state).toLowerCase() === 'closed') {
    let latest = currentLabels;
    try {
      latest = await callbacks.fetchIssueLabels(context, params);
    } catch {
      // ignore
    }

    const hasApproved = callbacks.labelsMatching(latest, approvedLabel).length > 0;
    if (hasApproved) {
      await callbacks.removeRejectedStatusLabel(context, params, latest);
      await callbacks.removeProgressStatusLabels(context, params, latest);
      return;
    }

    const hasRejected = callbacks.labelsMatching(latest, labels.rejected).length > 0;
    if (!hasRejected) {
      try {
        await callbacks.addLabels(context, params, [labels.rejected]);
      } catch {
        // ignore
      }
    }

    try {
      latest = await callbacks.fetchIssueLabels(context, params);
    } catch {
      // ignore
    }

    await callbacks.removeProgressStatusLabels(context, params, latest);

    const approvedMatches = callbacks.labelsMatching(latest, approvedLabel);
    if (approvedMatches.length) {
      await callbacks.removeExactLabelsFromIssue(context, params, approvedMatches);
    }
  }
}
