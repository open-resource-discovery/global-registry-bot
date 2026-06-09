import {
  buildContactApprovalBody,
  buildParentApprovalBody,
  readContactApprovalMeta,
  readParentApprovalMeta,
  type ContactApprovalMeta,
  type ParentApprovalMeta,
} from '../domain/approval-markers.js';
import { normalizeLogin, toStringTrim, uniqLogins } from '../domain/login-utils.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type UserLikeBase = {
  login?: string | null;
};

type IssueLikeBase = {
  number: number;
  body?: string | null;
  labels?: unknown;
  user?: UserLikeBase | null;
};

type TemplateMetaBase = {
  root?: string;
};

type TemplateLikeBase = {
  _meta?: TemplateMetaBase | null;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  reviewRequestedLabels: string[];
  labelOnApproved?: string | null;
};

type ParentContactCandidates = {
  logins: string[];
  emails: string[];
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

export type OwnerApprovalRequirementsCallbacks<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  normalizeKey: (value: unknown) => string;
  readTemplateRoot?: (template: TemplateType) => string;
  readFormContacts?: (formData: FormDataType) => string;
  labelsMatching: (labels: string[], expected: string) => string[];
  updateIssueBody: (context: ContextType, params: ParamsType, body: string) => Promise<void>;
  readYamlFromRepo: (context: ContextType, repo: RepoInfoBase, path: string) => Promise<unknown | null>;
  extractParentContactCandidates: (value: unknown) => ParentContactCandidates;
  lookupGithubLoginsByEmail: (context: ContextType, email: string) => Promise<string[]>;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  resolveWorkflowLabel: (context: ContextType, key: WorkflowLabelKey, fallback: string) => string;
  fetchIssueLabels: (context: ContextType, params: ParamsType) => Promise<string[]>;
  removeExactLabelsFromIssue: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  ensureAssigneesPresent: (context: ContextType, params: ParamsType, assignees: string[]) => Promise<void>;
  postOnce: (context: ContextType, params: ParamsType, body: string, options?: PostOnceOptionsBase) => Promise<void>;
  isSubContextRequestType: (requestType: unknown) => boolean;
  setStateLabel: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    state: 'author' | 'review'
  ) => Promise<void>;
  log: (context: ContextType, level: LogLevel, obj: unknown, msg: string) => void;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function sameNormalizedLoginSet(a: string[], b: string[]): boolean {
  const normalizedA = uniqLogins(a)
    .map((login) => normalizeLogin(login))
    .filter((login): login is string => !!login)
    .sort((left, right) => left.localeCompare(right));
  const normalizedB = uniqLogins(b)
    .map((login) => normalizeLogin(login))
    .filter((login): login is string => !!login)
    .sort((left, right) => left.localeCompare(right));

  if (normalizedA.length !== normalizedB.length) return false;
  return normalizedA.every((login, index) => login === normalizedB[index]);
}

function resolveParentOwnerActionLabel<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): string {
  return callbacks.resolveWorkflowLabel(context, 'parentOwnerAction', REQUEST_STATUS_LABEL_PARENT_OWNER_ACTION);
}

export async function resolveParentOwnerLoginsForTarget<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  template: TemplateType,
  validatedNamespace: string,
  requestType: string,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<{ parent: string; owners: string[] }> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) return { parent: '', owners: [] };

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length <= 2) return { parent: '', owners: [] };

  const parent = parts.slice(0, -1).join('.');
  if (!parent) return { parent: '', owners: [] };

  const rootRaw = toStringTrim(template?._meta?.root);
  const root = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!root) return { parent, owners: [] };

  const parentPath = `${root}/${parent}.yaml`;
  const doc = await callbacks.readYamlFromRepo(context, { owner: params.owner, repo: params.repo }, parentPath);
  if (!isPlainObject(doc)) return { parent, owners: [] };

  const contacts = doc['contacts'] ?? doc['contact'] ?? doc['owners'] ?? doc['owner'];
  const { logins: directLogins, emails } = callbacks.extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await callbacks.lookupGithubLoginsByEmail(context, email)));
  }

  return { parent, owners: uniqLogins(resolved) };
}

export async function clearParentOwnerActionState<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >,
  currentLabels?: string[]
): Promise<void> {
  const parentOwnerActionLabel = resolveParentOwnerActionLabel(context, callbacks);

  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await callbacks.fetchIssueLabels(context, params);
    } catch {
      labels = [];
    }
  }

  const toRemove = callbacks.labelsMatching(labels, parentOwnerActionLabel);
  if (!toRemove.length) return;

  await callbacks.removeExactLabelsFromIssue(context, params, toRemove);
}

export async function setParentOwnerActionState<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<void> {
  const eff = callbacks.resolveEffectiveConstants(context);

  const parentOwnerActionLabel = resolveParentOwnerActionLabel(context, callbacks);
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
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

  let labels: string[] = [];
  try {
    labels = await callbacks.fetchIssueLabels(context, params);
  } catch {
    labels = [];
  }

  const toRemove = new Set<string>();

  for (const label of [
    authorActionLabel,
    approverActionLabel,
    approvedLabel,
    REQUEST_STATUS_LABEL_REJECTED,
    ...(eff.reviewRequestedLabels || []),
  ]) {
    for (const match of callbacks.labelsMatching(labels, label)) {
      toRemove.add(match);
    }
  }

  if (toRemove.size) {
    await callbacks.removeExactLabelsFromIssue(context, params, Array.from(toRemove));
  }

  await callbacks.ensureLabelsPresentOnce(context, params, [parentOwnerActionLabel]);
}

export async function assignParentOwnersForApproval<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  owners: string[],
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<void> {
  const assignees = uniqLogins((owners || []).map(toStringTrim).filter(Boolean));
  if (!assignees.length) return;

  await callbacks.ensureAssigneesPresent(context, params, assignees);
}

export async function ensureContactApprovalMarker<
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
  meta: ContactApprovalMeta | null,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const current = readContactApprovalMeta(issue.body);

  if (!meta) {
    if (!current) return false;

    try {
      const nextBody = buildContactApprovalBody(issue.body, null);
      await callbacks.updateIssueBody(context, params, nextBody);
      issue.body = nextBody;
      return true;
    } catch {
      return false;
    }
  }

  const next: ContactApprovalMeta = {
    v: 1,
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const approvedBy = normalizeLogin(meta.approvedBy);
  const approvedAt = toStringTrim(meta.approvedAt);

  if (approvedBy) next.approvedBy = approvedBy;
  if (approvedAt) next.approvedAt = approvedAt;

  if (!next.target || !next.owners.length) return false;

  const same =
    current &&
    callbacks.normalizeKey(current.target) === callbacks.normalizeKey(next.target) &&
    sameNormalizedLoginSet(current.owners, next.owners) &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const nextBody = buildContactApprovalBody(issue.body, next);

  try {
    await callbacks.updateIssueBody(context, params, nextBody);
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

export async function resolveRequestContactOwnerLogins<
  ContextType,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  formData: FormDataType,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<string[]> {
  const contacts = formData['contact'] ?? formData['contacts'] ?? '';
  const { logins: directLogins, emails } = callbacks.extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await callbacks.lookupGithubLoginsByEmail(context, email)));
  }

  return uniqLogins(resolved);
}

export async function maybeRequireSystemContactOwnerApproval<
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
  parsedFormData: FormDataType,
  requestType: string,
  validatedNamespace: string,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const normalizedRequestType = toStringTrim(requestType)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
  if (normalizedRequestType !== 'systemnamespace') {
    await ensureContactApprovalMarker(context, params, issue, null, callbacks);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const owners = await resolveRequestContactOwnerLogins(context, parsedFormData, callbacks);
  const requester = normalizeLogin(issue.user?.login);

  if (!target || !owners.length) {
    await ensureContactApprovalMarker(context, params, issue, null, callbacks);
    return false;
  }

  if (requester && owners.some((owner) => owner.toLowerCase() === requester.toLowerCase())) {
    await ensureContactApprovalMarker(context, params, issue, null, callbacks);
    return false;
  }

  const current = readContactApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    callbacks.normalizeKey(current.target) === callbacks.normalizeKey(target) &&
    sameNormalizedLoginSet(current.owners, owners) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) return false;

  await ensureContactApprovalMarker(context, params, issue, { v: 1, target, owners }, callbacks);

  const mentions = owners.map((owner) => `@${owner}`).join(' ');
  const tag = `nsreq:contact-approval:${callbacks.normalizeKey(target)}`;

  await callbacks.postOnce(
    context,
    params,
    `### 🔒 Contact owner approval required

Requester @${requester || 'unknown'} is not listed in the contact owners for \`${target}\`.

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  await callbacks.setStateLabel(context, params, issue, 'author');
  return true;
}

export async function ensureParentApprovalMarker<
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
  meta: ParentApprovalMeta | null,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const current = readParentApprovalMeta(issue.body);

  if (!meta) {
    if (!current) return false;

    try {
      const nextBody = buildParentApprovalBody(issue.body, null);
      await callbacks.updateIssueBody(context, params, nextBody);
      issue.body = nextBody;
      return true;
    } catch {
      return false;
    }
  }

  const next: ParentApprovalMeta = {
    v: 1,
    parent: toStringTrim(meta.parent),
    target: toStringTrim(meta.target),
    owners: uniqLogins(meta.owners || []),
  };

  const approvedBy = normalizeLogin(meta.approvedBy);
  const approvedAt = toStringTrim(meta.approvedAt);

  if (approvedBy) next.approvedBy = approvedBy;
  if (approvedAt) next.approvedAt = approvedAt;

  if (!next.parent || !next.target) return false;

  const same =
    current &&
    callbacks.normalizeKey(current.parent) === callbacks.normalizeKey(next.parent) &&
    callbacks.normalizeKey(current.target) === callbacks.normalizeKey(next.target) &&
    uniqLogins(current.owners).join('|').toLowerCase() === uniqLogins(next.owners).join('|').toLowerCase() &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const nextBody = buildParentApprovalBody(issue.body, next);

  try {
    await callbacks.updateIssueBody(context, params, nextBody);
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

export async function maybeRequireParentOwnerApproval<
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
  template: TemplateType,
  validatedNamespace: string,
  requestType: string,
  callbacks: OwnerApprovalRequirementsCallbacks<
    ContextType,
    ParamsType,
    IssueType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<boolean> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) {
    await ensureParentApprovalMarker(context, params, issue, null, callbacks);
    await clearParentOwnerActionState(context, params, callbacks);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length <= 2) {
    await ensureParentApprovalMarker(context, params, issue, null, callbacks);
    await clearParentOwnerActionState(context, params, callbacks);
    return false;
  }

  const requester = normalizeLogin(issue.user?.login);
  const { parent, owners } = await resolveParentOwnerLoginsForTarget(
    context,
    params,
    template,
    target,
    requestType,
    callbacks
  );

  if (!parent || owners.length === 0) {
    await ensureParentApprovalMarker(context, params, issue, null, callbacks);
    await clearParentOwnerActionState(context, params, callbacks);
    return false;
  }

  if (requester && owners.some((owner) => owner.toLowerCase() === requester.toLowerCase())) {
    callbacks.log(
      context,
      'debug',
      { issue: issue.number, requester, parent, target, owners },
      'parent-approval:skip (requester is parent owner)'
    );

    if (callbacks.isSubContextRequestType(requestType)) {
      await ensureParentApprovalMarker(
        context,
        params,
        issue,
        {
          v: 1,
          parent,
          target,
          owners,
          approvedBy: requester,
          approvedAt: new Date().toISOString(),
        },
        callbacks
      );
    } else {
      await ensureParentApprovalMarker(context, params, issue, null, callbacks);
    }

    await clearParentOwnerActionState(context, params, callbacks);
    return false;
  }

  const current = readParentApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    callbacks.normalizeKey(current.parent) === callbacks.normalizeKey(parent) &&
    callbacks.normalizeKey(current.target) === callbacks.normalizeKey(target) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) {
    await clearParentOwnerActionState(context, params, callbacks);
    return false;
  }

  await ensureParentApprovalMarker(context, params, issue, { v: 1, parent, target, owners }, callbacks);
  await setParentOwnerActionState(context, params, callbacks);
  await assignParentOwnersForApproval(context, params, owners, callbacks);

  const mentions = owners.map((owner) => `@${owner}`).join(' ');
  const tag = `nsreq:parent-approval:${callbacks.normalizeKey(parent)}:${callbacks.normalizeKey(target)}`;

  await callbacks.postOnce(
    context,
    params,
    `### 🔒 Parent owner approval required

Sub-namespace request under \`${parent}\` (target: \`${target}\`).

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  return true;
}
