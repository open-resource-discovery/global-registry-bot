import { buildRoutingLockBody, readRoutingLockExpected } from '../domain/routing-lock-marker.js';
import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

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
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type PostOnceOptionsBase = {
  minimizeTag?: string;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

type WorkflowLabelKey = 'authorAction' | 'approverAction' | 'parentOwnerAction';

const REQUEST_STATUS_LABEL_REQUESTER_ACTION = 'Requester Action';
const REQUEST_STATUS_LABEL_REVIEW_PENDING = 'Review Pending';
const REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION = 'Parent Owner Action';
const REQUEST_STATUS_LABEL_REJECTED = 'Rejected';

const ROUTING_LOCK_NOTICE_INFLIGHT = new Map<string, Promise<void>>();

export type IssueWorkflowGuardCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  tryLoadTemplateForLabels: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    labels: string[]
  ) => Promise<TemplateType | null>;
  normalizeKey: (value: unknown) => string;
  postOnce: (context: ContextType, params: ParamsType, body: string, options?: PostOnceOptionsBase) => Promise<void>;
  updateIssueBody: (context: ContextType, params: ParamsType, body: string) => Promise<void>;
  fetchIssueLabels: (context: ContextType, params: ParamsType) => Promise<string[]>;
  toLabelNames: (labels: unknown) => string[];
  removeExactLabelsFromIssue: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  addLabels: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  labelsMatching: (labels: string[], expected: string) => string[];
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
  resolveLockedWorkflowLabelKeys: (context: ContextType) => Set<string>;
  resolveWorkflowLabel: (context: ContextType, key: WorkflowLabelKey, fallback: string) => string;
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
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  removeRejectedStatusLabel: (context: ContextType, params: ParamsType, currentLabels?: string[]) => Promise<void>;
  removeProgressStatusLabels: (context: ContextType, params: ParamsType, currentLabels?: string[]) => Promise<void>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
  getErrorMessage: (error: unknown) => string;
};

function routingNoticeKey(params: IssueParamsBase): string {
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
  const uniq: string[] = [];
  const seen = new Set<string>();

  for (const label of labels) {
    const name = toStringTrim(label);
    const key = callbacks.normalizeKey(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    uniq.push(name);
  }

  const routing: string[] = [];
  for (const label of uniq) {
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

  const current = readRoutingLockExpected(issue.body);
  if (callbacks.normalizeKey(current) === callbacks.normalizeKey(expected)) return false;

  const nextBody = buildRoutingLockBody(issue.body, expected);

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

export async function handleClosedIssueWorkflowGuard<
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
  >
): Promise<void> {
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

  const effectiveConstants = callbacks.resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(effectiveConstants.labelOnApproved) || 'Approved';

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

  const hasRejected = callbacks.labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length > 0;
  if (!hasRejected) {
    try {
      await callbacks.addLabels(context, params, [REQUEST_STATUS_LABEL_REJECTED]);
    } catch (error: unknown) {
      callbacks.log(
        context,
        'warn',
        { err: callbacks.getErrorMessage(error), label: REQUEST_STATUS_LABEL_REJECTED },
        'failed to add rejected status label'
      );
    }
  }

  try {
    labels = await callbacks.fetchIssueLabels(context, params);
  } catch {
    // best effort
  }

  if (callbacks.labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length) {
    await callbacks.removeProgressStatusLabels(context, params, labels);

    const approvedMatches = callbacks.labelsMatching(labels, approvedLabel);
    if (approvedMatches.length) {
      await callbacks.removeExactLabelsFromIssue(context, params, approvedMatches);
    }
  }
}

export async function handleIssueLabelChangeWorkflowGuard<
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
  action: string,
  changedLabel: string,
  senderLogin: string | undefined | null,
  callbacks: IssueWorkflowGuardCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<void> {
  let labels = callbacks.toLabelNames(issue.labels);

  const expectedRouting = readRoutingLockExpected(issue.body);
  const hasRoutingLock = Boolean(expectedRouting);

  if (expectedRouting) {
    const enforced = await enforceRoutingLabelLock(context, params, issue, expectedRouting, callbacks, {
      changedLabel,
    });
    if (enforced) {
      try {
        labels = await callbacks.fetchIssueLabels(context, params);
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

  const effectiveConstants = callbacks.resolveEffectiveConstants(context);
  const authorActionLabel = callbacks.resolveWorkflowLabel(
    context,
    'authorAction',
    REQUEST_STATUS_LABEL_REQUESTER_ACTION
  );
  const approverActionLabel = callbacks.resolveWorkflowLabel(
    context,
    'approverAction',
    REQUEST_STATUS_LABEL_REVIEW_PENDING
  );
  const parentOwnerActionLabel = callbacks.resolveWorkflowLabel(
    context,
    'parentOwnerAction',
    REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION
  );

  const authorActionKey = callbacks.normalizeKey(authorActionLabel);
  const approverActionKey = callbacks.normalizeKey(approverActionLabel);
  const isProgressStateLabel = (key: string): boolean => key === authorActionKey || key === approverActionKey;

  const approvedLabel = toStringTrim(effectiveConstants.labelOnApproved) || 'Approved';
  const lockedKeys = callbacks.resolveLockedWorkflowLabelKeys(context);
  const changedKey = callbacks.normalizeKey(changedLabel);

  const parentOwnerActionKey = callbacks.normalizeKey(parentOwnerActionLabel);
  if (parentOwnerActionKey) lockedKeys.add(parentOwnerActionKey);

  const effectiveRequestType = template ? callbacks.resolveEffectiveRequestType(template, parsedFormData) : '';
  const approverRouting = effectiveRequestType
    ? callbacks.resolveApproverRoutingForRequestType(
        context,
        effectiveRequestType,
        effectiveConstants.approverUsernames,
        effectiveConstants.approverPoolUsernames
      )
    : {
        approvalUsernames: callbacks.uniqLogins([
          ...(effectiveConstants.approverUsernames || []),
          ...(effectiveConstants.approverPoolUsernames || []),
        ]),
        autoAssigneePoolUsernames: callbacks.uniqLogins(effectiveConstants.approverPoolUsernames || []),
      };

  const senderIsConfiguredApprover = callbacks.isConfiguredApprover(senderLogin, approverRouting.approvalUsernames);

  const managedWorkflowKeys = new Set<string>(Array.from(lockedKeys));
  for (const label of [
    authorActionLabel,
    approverActionLabel,
    parentOwnerActionLabel,
    approvedLabel,
    REQUEST_STATUS_LABEL_REJECTED,
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
    const isManualRejectedAdd =
      action === 'labeled' &&
      callbacks.labelsMatching([changedLabel], REQUEST_STATUS_LABEL_REJECTED).length > 0 &&
      toStringTrim(issue.state).toLowerCase() !== 'closed';

    if (!isManualApprovedAdd && !isManualRejectedAdd) {
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
    const approvedMatches = callbacks.labelsMatching(labels, approvedLabel);
    await callbacks.removeExactLabelsFromIssue(context, params, approvedMatches);

    const hasAuthor = callbacks.labelsMatching(labels, authorActionLabel).length > 0;
    const hasReview = callbacks.labelsMatching(labels, approverActionLabel).length > 0;
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
    callbacks.labelsMatching([changedLabel], REQUEST_STATUS_LABEL_REJECTED).length &&
    toStringTrim(issue.state).toLowerCase() !== 'closed'
  ) {
    const rejectedMatches = callbacks.labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED);
    await callbacks.removeExactLabelsFromIssue(context, params, rejectedMatches);

    await callbacks.postOnce(
      context,
      params,
      'Rejected label change reverted. Rejected state is managed automatically when a request is closed without approval.',
      { minimizeTag: 'nsreq:label-guard' }
    );
    return;
  }

  if (toStringTrim(issue.state).toLowerCase() === 'closed') {
    let latest = labels;
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

    const hasRejected = callbacks.labelsMatching(latest, REQUEST_STATUS_LABEL_REJECTED).length > 0;
    if (!hasRejected) {
      try {
        await callbacks.addLabels(context, params, [REQUEST_STATUS_LABEL_REJECTED]);
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
