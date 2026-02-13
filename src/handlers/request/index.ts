import { setStateLabel as setStateLabelRaw, ensureAssigneesOnce as ensureAssigneesOnceRaw } from './state.js';
import { postOnce as postOnceRaw } from './comments.js';
import { loadTemplate as loadTemplateRaw, parseForm as parseFormRaw } from './template.js';
import { validateRequestIssue as validateRequestIssueRaw } from './validation/run.js';
import {
  calcSnapshotHash as calcSnapshotHashRaw,
  extractHashFromPrBody as extractHashFromPRBodyRaw,
  findOpenIssuePrs as findOpenIssuePRsRaw,
} from './pr/snapshot.js';
import { createRequestPr as createRequestPRRaw } from './pr/create.js';
import { tryMergeIfGreen as tryMergeIfGreenRaw } from '../../lib/auto-merge.js';
import { loadStaticConfig, DEFAULT_CONFIG, type NormalizedStaticConfig, type RegistryBotHooks } from '../../config.js';
import { getDocLinksFromConfig } from './constants.js';
import type { Context, Probot } from 'probot';

const DBG = process.env.DEBUG_NS === '1';

type RequestEvents =
  | 'issues.opened'
  | 'issues.edited'
  | 'issues.closed'
  | 'issues.reopened'
  | 'issues.labeled'
  | 'issues.unlabeled'
  | 'issue_comment.created'
  | 'issue_comment.edited'
  | 'check_suite.completed'
  | 'check_run.completed'
  | 'status';

type ResourceBotContextExt = {
  resourceBotConfig?: NormalizedStaticConfig;
  resourceBotHooks?: RegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type BotContext<E extends RequestEvents> = Context<E> & ResourceBotContextExt;

type RepoInfo = { owner: string; repo: string };
type IssueParams = { owner: string; repo: string; issue_number: number };

type LabelLike = string | { name?: string | null };
type UserLike = { login?: string | null };
type SenderLike = { type?: string | null; login?: string | null };

type IssueLike = {
  number: number;
  id?: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: LabelLike[];
  user?: UserLike | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type CommentLike = {
  body?: string | null;
  user: { login: string };
};

type TemplateMeta = {
  requestType?: string;
  root?: string;
  schema?: string;
  path?: string;
};

type TemplateLike = {
  _meta?: TemplateMeta;
  title?: string | null;
  name?: string | null;
  body?: unknown[];
  labels?: unknown[];
  [k: string]: unknown;
};

type FormData = Record<string, string>;

type PostOnceOptions = { minimizeTag?: string };

type PullRequestLike = {
  number: number;
  body?: string | null;
  head: { ref: string; sha: string };
};

type CheckRunPullRequestRef = { number?: number | null };

type CheckRunLike = {
  id?: number | null;
  conclusion?: string | null;
  html_url?: string | null;
  pull_requests?: CheckRunPullRequestRef[] | null;
};

type CheckRunAnnotationLike = {
  path?: string | null;
  message?: string | null;
  title?: string | null;
  annotation_level?: string | null;
  raw_details?: string | null;
};

type ValidateRequestIssueResult = {
  errors: string[];
  errorsGrouped?: unknown;
  errorsFormatted: string;
  errorsFormattedSingle: string;
  formData?: FormData;
  template?: TemplateLike;
  namespace: string;
  nsType: string;
};

type MergeMethod = 'merge' | 'squash' | 'rebase';

type EffectiveConstants = {
  globalLabels: string[];
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  labelAutoMergeCandidate: string | null;
  approverUsernames: string[];
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const status = err['status'];
  return typeof status === 'number' ? status : undefined;
}

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function readCheckRunId(run: CheckRunLike | null): number | null {
  const id = run?.id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

type CheckSuitePullRequestRef = { number?: number | null };

type CheckSuiteLike = {
  id?: number | null;
  conclusion?: string | null;
  head_sha?: string | null;
  pull_requests?: CheckSuitePullRequestRef[] | null;
};

function readCheckSuiteFromPayload(payload: unknown): CheckSuiteLike | null {
  if (!isPlainObject(payload)) return null;
  const suite = payload['check_suite'];
  if (!isPlainObject(suite)) return null;
  return suite as unknown as CheckSuiteLike;
}

function readCheckSuiteId(suite: CheckSuiteLike | null): number | null {
  const id = suite?.id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

function readCheckSuitePrNumbers(suite: CheckSuiteLike | null): number[] {
  const prs = Array.isArray(suite?.pull_requests) ? suite?.pull_requests : [];
  const out: number[] = [];
  for (const pr of prs) {
    const n = pr?.number;
    if (typeof n === 'number' && Number.isFinite(n)) out.push(n);
  }
  return out;
}

async function listAllCheckRunsForSuite(
  context: BotContext<RequestEvents>,
  owner: string,
  repo: string,
  checkSuiteId: number
): Promise<CheckRunLike[]> {
  const all: CheckRunLike[] = [];
  let page = 1;

  while (true) {
    const res = await context.octokit.checks.listForSuite({
      owner,
      repo,
      check_suite_id: checkSuiteId,
      per_page: 100,
      page,
    });

    const data = (res as unknown as { data?: unknown }).data;
    const runs = isPlainObject(data) && Array.isArray(data['check_runs']) ? (data['check_runs'] as unknown[]) : [];

    all.push(...(runs as unknown as CheckRunLike[]));

    if (runs.length < 100) break;
    page += 1;
    if (page > 20) break; // safety cap
  }

  return all;
}

async function listAllCheckRunAnnotations(
  context: BotContext<RequestEvents>,
  owner: string,
  repo: string,
  checkRunId: number
): Promise<CheckRunAnnotationLike[]> {
  const all: CheckRunAnnotationLike[] = [];
  let page = 1;

  while (true) {
    const res = await context.octokit.checks.listAnnotations({
      owner,
      repo,
      check_run_id: checkRunId,
      per_page: 100,
      page,
    });

    const data = (res as unknown as { data?: unknown }).data;
    const items = Array.isArray(data) ? (data as unknown[]) : [];

    all.push(...(items as unknown as CheckRunAnnotationLike[]));

    if (items.length < 100) break;
    page += 1;

    if (page > 20) break; // safety cap
  }

  return all;
}

function isRegistryValidateAnnotation(a: CheckRunAnnotationLike): boolean {
  const t = toStringTrim(a?.title).toLowerCase();
  return t.startsWith('registry-validate');
}

function stripRegistrySuffix(msg: string): string {
  const i = msg.indexOf(' [file=');
  return (i >= 0 ? msg.slice(0, i) : msg).trim();
}

const normalizeKey = (s: unknown): string => {
  const base = toStringTrim(s).toLowerCase();
  return base.replaceAll(/[^\w]+/g, '-').replaceAll(/(?:^-+|-+$)/g, '');
};

function toSectionTitle(field: string): string {
  const raw = toStringTrim(field);
  if (!raw) return 'Details';

  const lc = raw.toLowerCase();
  if (lc === 'contact' || lc === 'contacts') return 'Contacts';

  // Humanize
  const spaced = raw
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim();

  if (!spaced) return 'Details';
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function normalizeMsg(m: string): string {
  const s = toStringTrim(m);

  // Strip leading "/path" token if present
  const firstSpace = s.indexOf(' ');
  const maybePath = firstSpace > 0 ? s.slice(0, firstSpace) : '';
  const rest = firstSpace > 0 ? s.slice(firstSpace + 1).trim() : s;

  const msgOnly = maybePath.startsWith('/') ? rest : s;

  // Normalize "must" -> "MUST"
  return msgOnly.replace(/\bmust\b/gi, 'MUST');
}

function extractFieldFromMsg(m: string): string {
  const s = toStringTrim(m);
  if (!s) return '';

  const ptr = /^\/([^/\s]+)(?:\/|\s|$)/.exec(s);
  if (ptr?.[1]) return ptr[1];

  const reqProp = /\b(?:required property|Property)\s*['"]([^'"]+)['"]/.exec(s);
  if (reqProp?.[1]) return reqProp[1];

  const addProp = /\badditional property\s*['"]([^'"]+)['"]/.exec(s);
  if (addProp?.[1]) return addProp[1];

  const labelReq = /^(.+?)\s+is\s+required\.\s*$/i.exec(s);
  if (labelReq?.[1]) return normalizeKey(labelReq[1]);

  const leadingField = /^([a-z][a-zA-Z0-9_-]*)\s+(?:must|MUST)\b/.exec(s);
  if (leadingField?.[1]) return leadingField[1];

  const dotted = /^([a-z][a-zA-Z0-9_-]*)(?:\[[^\]]*\])?\.[a-zA-Z0-9_-]+\s+is\s+required\./i.exec(s);
  if (dotted?.[1]) return dotted[1];

  return '';
}

function buildRegistryValidationPrCommentBody(filePath: string, messages: string[]): string {
  const lines: string[] = [];

  lines.push(`## Detected issues: ${filePath}`);
  lines.push('');

  // group by field
  const grouped = new Map<string, string[]>();

  for (const raw of messages) {
    const field = extractFieldFromMsg(raw) || 'details';
    const msg = normalizeMsg(raw);
    if (!msg) continue;

    const arr = grouped.get(field) ?? [];
    if (!arr.includes(msg)) arr.push(msg); // dedupe within section
    grouped.set(field, arr);
  }

  const keys = Array.from(grouped.keys()).sort((a, b) => {
    // put "details" last
    if (a === 'details') return 1;
    if (b === 'details') return -1;
    return a.localeCompare(b);
  });

  for (const k of keys) {
    lines.push(`### ${toSectionTitle(k)}`);
    for (const msg of grouped.get(k) ?? []) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}

type LogLevel = 'debug' | 'info' | 'warn' | 'error';
type LoggerFn = (this: unknown, obj: unknown, msg?: string) => void;
type LoggerLike = Partial<Record<LogLevel, LoggerFn>>;

const log = (context: { log?: LoggerLike } | undefined, level: LogLevel, obj: unknown, msg: string): void => {
  const logger = context?.log;
  const fn = logger?.[level];

  if (typeof fn === 'function') {
    fn.call(logger, obj, msg);
  }
};

const labelName = (l: unknown): string => {
  if (typeof l === 'string') return l;
  if (isPlainObject(l) && typeof l.name === 'string') return l.name;
  return '';
};

const toLabelNames = (labels: unknown): string[] =>
  (Array.isArray(labels) ? labels : [])
    .map((l) => labelName(l))
    .map((s) => toStringTrim(s))
    .filter(Boolean);

const stripQuoteAndCode = (text: unknown): string =>
  toStringTrim(text)
    .replaceAll(/```[\s\S]*?```/g, '')
    .replaceAll(/^>.*$/gm, '')
    .trim();

const ISSUE_FORM_FIELD_HEADING_RE = /^###\s+\S+/m;

function hasIssueFormInputs(issue: IssueLike | null | undefined): boolean {
  const body = stripQuoteAndCode(issue?.body);
  return ISSUE_FORM_FIELD_HEADING_RE.test(body);
}

const isBotSender = (sender: SenderLike | undefined | null): boolean =>
  sender?.type === 'Bot' || /(\[bot\]|-bot)$/i.test(sender?.login || '');

const head = (s: unknown): string => toStringTrim(s).split(':')[0].trim();

const resolveEffectiveConstants = (context: BotContext<RequestEvents>): EffectiveConstants => {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const wf = cfg?.workflow ?? {};
  let labels: Record<string, unknown> = {};
  if (isPlainObject(wf)) {
    const raw = (wf as Record<string, unknown>)['labels'];
    if (isPlainObject(raw)) labels = raw;
  }

  const toStringArray = (raw: unknown): string[] => {
    if (Array.isArray(raw)) return raw.map((x) => toStringTrim(x)).filter(Boolean);
    if (raw !== undefined && raw !== null) return [toStringTrim(raw)].filter(Boolean);
    return [];
  };

  const globalLabels = toStringArray(labels['global']);
  const reviewRequestedLabels = toStringArray(labels['approvalRequested']);
  const approvalSuccessfulArr = toStringArray(labels['approvalSuccessful']);
  const labelOnApproved = approvalSuccessfulArr.length ? approvalSuccessfulArr[0] : null;
  const autoMergeCandidateArr = toStringArray(labels['autoMergeCandidate']);
  const labelAutoMergeCandidate = autoMergeCandidateArr.length ? autoMergeCandidateArr[0] : null;

  let approverUsernames: string[] = [];
  if (isPlainObject(wf)) {
    const raw = (wf as Record<string, unknown>)['approvers'];
    if (Array.isArray(raw)) approverUsernames = raw.map((x) => toStringTrim(x)).filter(Boolean);
  }

  return {
    globalLabels: globalLabels.map((x) => x.trim()).filter(Boolean),
    reviewRequestedLabels: reviewRequestedLabels.map((x) => x.trim()).filter(Boolean),
    labelOnApproved: labelOnApproved ? String(labelOnApproved).trim() : null,
    labelAutoMergeCandidate: labelAutoMergeCandidate ? String(labelAutoMergeCandidate).trim() : null,
    approverUsernames: approverUsernames.map((x) => x.trim()).filter(Boolean),
  };
};

function resolveLockedWorkflowLabelKeys(context: BotContext<RequestEvents>): Set<string> {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const wf = cfg?.workflow ?? {};

  let labelsCfg: Record<string, unknown> = {};
  if (isPlainObject(wf)) {
    const raw = (wf as Record<string, unknown>)['labels'];
    if (isPlainObject(raw)) labelsCfg = raw;
  }

  const labels: string[] = [];
  for (const v of Object.values(labelsCfg)) {
    if (Array.isArray(v)) labels.push(...v.map((x) => toStringTrim(x)).filter(Boolean));
    else labels.push(toStringTrim(v));
  }

  return new Set(labels.map(normalizeKey).filter(Boolean));
}

function resolveApproversForRequestType(
  context: BotContext<RequestEvents>,
  requestType: string | undefined | null,
  fallbackApprovers: string[]
): string[] {
  const rt = toStringTrim(requestType);
  if (!rt) return fallbackApprovers;

  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = cfg?.requests;

  if (!reqs || typeof reqs !== 'object') return fallbackApprovers;

  const asRec = reqs as unknown as Record<string, unknown>;
  const direct = asRec[rt];

  let entry: Record<string, unknown> | null = null;

  if (isPlainObject(direct)) {
    entry = direct;
  } else {
    const rtKey = normalizeKey(rt);
    for (const [k, v] of Object.entries(asRec)) {
      if (normalizeKey(k) === rtKey && isPlainObject(v)) {
        entry = v;
        break;
      }
    }
  }

  if (!entry) return fallbackApprovers;

  const raw = entry['approvers'];
  if (!Array.isArray(raw) || raw.length === 0) return fallbackApprovers;

  return raw.map((x) => toStringTrim(x)).filter(Boolean);
}

const buildTemplateLoadErrorMessage = (errMsg: unknown): string => {
  const msg = toStringTrim(errMsg);
  const isRouting = msg.includes('no routing label found') || msg.includes('Cannot resolve template');

  if (!isRouting) {
    return `## Configuration error: unable to load request template\n\n**Details**\n- ${msg || 'Unknown error'}`;
  }

  return `## Cannot process this issue: no routing label detected

This bot routes request issues by a **unique label** that is auto-assigned by the selected Issue Form template.

**Fix**
- Ensure the Issue Form template includes a unique routing label
- Ensure this label exists in the repo (Settings → Issues → Labels)
- Re-open or edit the issue to retrigger

**Details**
- ${msg || 'No routing label found'}`;
};

function isRequestIssue(
  context: BotContext<RequestEvents>,
  template: TemplateLike | null | undefined,
  parsedFormData: FormData
): boolean {
  const parsedKeys = Object.keys(parsedFormData || {}).filter(Boolean);
  const meta = template?._meta || {};
  const requestType = String(meta.requestType || '').trim();
  const root = String(meta.root || '').trim();
  const schema = String(meta.schema || '').trim();

  const hasTemplateMeta = Boolean(requestType && root && schema);
  const hasFormData = parsedKeys.length > 0;

  const isReq = Boolean(template) && hasTemplateMeta && hasFormData;

  if (DBG) {
    log(
      context,
      'debug',
      {
        tplPath: String(meta.path || '').trim(),
        requestType,
        root,
        schema,
        parsedKeys,
        isReq,
      },
      'isRequestIssue(new-requests-only)'
    );
  }

  return isReq;
}

// Typed wrappers around JS modules
type SetStateLabelFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  state: 'author' | 'review'
) => Promise<void>;

type EnsureAssigneesOnceFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  assignees: string[]
) => Promise<void>;

type PostOnceFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  body: string,
  options?: PostOnceOptions
) => Promise<void>;

type LoadTemplateFn = (
  context: BotContext<RequestEvents>,
  opts: {
    owner: string;
    repo: string;
    templatePath?: string;
    issueLabels?: unknown;
    issueTitle?: string;
  }
) => Promise<TemplateLike>;

type ParseFormFn = (body: string, template: TemplateLike) => FormData;

type ValidateRequestIssueFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  options?: { template?: TemplateLike; formData?: FormData }
) => Promise<ValidateRequestIssueResult>;

type CalcSnapshotHashFn = (formData: FormData, template: TemplateLike, rawBody: string) => string;

type ExtractHashFromPrBodyFn = (body: string) => string;

type FindOpenIssuePrsFn = (
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  issueNumber: number
) => Promise<PullRequestLike[]>;

type CreateRequestPrFn = (
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  issue: IssueLike,
  formData: FormData,
  options?: { template?: TemplateLike }
) => Promise<{ number: number }>;

type TryMergeIfGreenFn = (
  context: BotContext<RequestEvents>,
  args: {
    owner: string;
    repo: string;
    prNumber: number;
    mergeMethod: MergeMethod;
    prData: PullRequestLike;
  }
) => Promise<void>;

const setStateLabel = setStateLabelRaw as unknown as SetStateLabelFn;
const ensureAssigneesOnce = ensureAssigneesOnceRaw as unknown as EnsureAssigneesOnceFn;
const postOnce = postOnceRaw as unknown as PostOnceFn;
const loadTemplate = loadTemplateRaw as unknown as LoadTemplateFn;
const parseForm = parseFormRaw as unknown as ParseFormFn;
const validateRequestIssue = validateRequestIssueRaw as unknown as ValidateRequestIssueFn;
const calcSnapshotHash = calcSnapshotHashRaw as unknown as CalcSnapshotHashFn;
const extractHashFromPrBody = extractHashFromPRBodyRaw as unknown as ExtractHashFromPrBodyFn;
const findOpenIssuePrs = findOpenIssuePRsRaw as unknown as FindOpenIssuePrsFn;
const createRequestPr = createRequestPRRaw as unknown as CreateRequestPrFn;
const tryMergeIfGreen = tryMergeIfGreenRaw as unknown as TryMergeIfGreenFn;

// Helpers moved to outer scope to satisfy linting
function extractResourceNameFromForm(formData: FormData, template: TemplateLike): string {
  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isProduct = rt === 'product';

  const val = isProduct
    ? (formData['product-id'] ?? formData['productId'] ?? formData['identifier'] ?? '')
    : (formData['identifier'] ?? formData['namespace'] ?? formData['name'] ?? formData['vendor'] ?? '');

  return toStringTrim(val);
}

function resolveEffectiveRequestType(template: TemplateLike, formData: FormData): string {
  const rt = toStringTrim(template?._meta?.requestType);

  if (rt && rt.toLowerCase() === 'partnernamespace') {
    const selected = toStringTrim((formData as Record<string, unknown>)['requestType']);
    const norm = selected.replace(/[\s_-]/g, '').toLowerCase();

    if (norm === 'authority') return 'authorityNamespace';
    if (norm === 'system') return 'systemNamespace';
    if (norm === 'subcontext') return 'subContextNamespace';
  }

  return rt;
}

async function fetchIssueLabels(
  context: BotContext<RequestEvents>,
  { owner, repo, issue_number }: IssueParams
): Promise<string[]> {
  const { data } = await context.octokit.issues.get({ owner, repo, issue_number });
  const issue = data as unknown as IssueLike;
  return toLabelNames(issue.labels);
}

async function handoverToCpa(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  _nsType: string,
  _namespace: string,
  _links: string[] = [],
  options: { snapshotHash?: string; requestType?: string } = {}
): Promise<void> {
  const eff = resolveEffectiveConstants(context);

  await setStateLabel(context, params, issue, 'review');
  const approversForType = resolveApproversForRequestType(context, options.requestType, eff.approverUsernames);
  await ensureAssigneesOnce(context, params, issue, approversForType);

  const labelsToAdd = [...(eff.globalLabels || []), ...(eff.reviewRequestedLabels || [])].filter(Boolean);

  if (labelsToAdd.length) {
    try {
      await context.octokit.issues.addLabels({
        ...params,
        labels: labelsToAdd,
      });
    } catch {
      // ignore label add errors
    }
  }

  if (eff.labelOnApproved) {
    try {
      await context.octokit.issues.removeLabel({
        ...params,
        name: eff.labelOnApproved,
      });
    } catch {
      // ignore if not present
    }
  }

  const docsLinks = getDocLinksFromConfig(context.resourceBotConfig ?? DEFAULT_CONFIG);
  const docsSection = docsLinks ? `\n\n${docsLinks.trim()}` : '';
  const snapshotMarker = options.snapshotHash ? `\n\n<!-- nsreq:snapshot:${options.snapshotHash} -->` : '';

  const handoverMsg = `### ✅ No issues detected
### ➡️ Routing to an approver for review

---

Once reviewed, please comment \`Approved\` to create an automatic Pull Request.${docsSection}${snapshotMarker}`;

  await postOnce(context, params, handoverMsg, { minimizeTag: 'nsreq:handover' });
}

async function ensureReviewLabelsPresentOnIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  eff: EffectiveConstants
): Promise<boolean> {
  const cfgKeys = (eff.reviewRequestedLabels || []).map(normalizeKey);
  if (!cfgKeys.length) return true;

  const issueLabels = Array.isArray(issue.labels) ? issue.labels : [];
  const hasReviewLabel = issueLabels.some((l) => {
    const k = normalizeKey(labelName(l));
    return cfgKeys.some((ck) => k === ck || k.includes(ck) || ck.includes(k));
  });

  if (hasReviewLabel) return true;

  const reviewLabels = eff.reviewRequestedLabels || [];
  if (reviewLabels.length) {
    try {
      await context.octokit.issues.addLabels({
        ...params,
        labels: reviewLabels,
      });
    } catch (e: unknown) {
      log(
        context,
        'warn',
        { err: e instanceof Error ? e.message : String(e) },
        'failed to auto-add review labels on approval'
      );
    }
  }

  const refreshed = await context.octokit.issues.get({
    owner: params.owner,
    repo: params.repo,
    issue_number: params.issue_number,
  });

  const refreshedIssue = refreshed.data as unknown as IssueLike;
  const refreshedLabels = Array.isArray(refreshedIssue.labels) ? refreshedIssue.labels : [];

  return refreshedLabels.some((l) => {
    const k = normalizeKey(labelName(l));
    return cfgKeys.some((ck) => k === ck || k.includes(ck) || ck.includes(k));
  });
}

async function removeReviewPendingLabelsAfterApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  eff: EffectiveConstants
): Promise<void> {
  const approvedCfg = toStringTrim(eff.labelOnApproved);
  const pendingCfg = (eff.reviewRequestedLabels || []).map(toStringTrim).filter(Boolean);

  if (!approvedCfg || !pendingCfg.length) return;

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    return;
  }

  const approvedKey = normalizeKey(approvedCfg);
  const hasApproved = labels.some((l) => {
    const k = normalizeKey(l);
    return k === approvedKey || k.includes(approvedKey) || approvedKey.includes(k);
  });

  if (!hasApproved) return;

  const pendingKeys = pendingCfg.map(normalizeKey);

  const toRemove = labels.filter((l) => {
    const k = normalizeKey(l);
    return pendingKeys.some((pk) => k === pk || k.includes(pk) || pk.includes(k));
  });

  for (const label of toRemove) {
    try {
      await context.octokit.issues.removeLabel({ ...params, name: label });
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label },
          'failed to remove review pending label after approval'
        );
      }
    }
  }
}

// Request lifecycle status labels
const REQUEST_STATUS_LABEL_REQUESTER_ACTION = 'Requester Action';
const REQUEST_STATUS_LABEL_REVIEW_PENDING = 'Review Pending';
const REQUEST_STATUS_LABEL_REJECTED = 'Rejected';

const labelsMatching = (labels: string[], expected: string): string[] => {
  const expectedKey = normalizeKey(expected);
  if (!expectedKey) return [];

  return (labels || []).filter((l) => {
    const k = normalizeKey(l);
    return k === expectedKey || k.includes(expectedKey) || expectedKey.includes(k);
  });
};

async function removeExactLabelsFromIssue(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labelsToRemove: string[]
): Promise<void> {
  for (const label of labelsToRemove) {
    const name = toStringTrim(label);
    if (!name) continue;

    try {
      await context.octokit.issues.removeLabel({ ...params, name });
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label: name },
          'failed to remove label'
        );
      }
    }
  }
}

async function removeProgressStatusLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      return;
    }
  }

  const toRemove = new Set<string>([
    ...labelsMatching(labels, REQUEST_STATUS_LABEL_REQUESTER_ACTION),
    ...labelsMatching(labels, REQUEST_STATUS_LABEL_REVIEW_PENDING),
  ]);

  if (!toRemove.size) return;
  await removeExactLabelsFromIssue(context, params, Array.from(toRemove));
}

async function removeRejectedStatusLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  currentLabels?: string[]
): Promise<void> {
  let labels = (currentLabels || []).slice();
  if (!labels.length) {
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      return;
    }
  }

  const toRemove = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED);
  if (!toRemove.length) return;
  await removeExactLabelsFromIssue(context, params, toRemove);
}

// Higher-level orchestration helpers to reduce handler complexity
function isAuthorizedApprover(
  commenter: string,
  issueAuthor: string | undefined | null,
  allowedApprovers: string[]
): boolean {
  const commenterLc = String(commenter || '').toLowerCase();
  const hasConfiguredApprovers = Array.isArray(allowedApprovers) && allowedApprovers.length > 0;

  if (hasConfiguredApprovers) {
    return allowedApprovers.some((u) => String(u || '').toLowerCase() === commenterLc);
  }

  const issueAuthorLc = String(issueAuthor || '').toLowerCase();
  return Boolean(commenterLc && commenterLc !== issueAuthorLc);
}

async function processIssueEvent(
  app: Probot,
  context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>,
  params: IssueParams,
  issue: IssueLike
): Promise<void> {
  if (!process.env.JEST_WORKER_ID) {
    if (!hasIssueFormInputs(issue)) return;
  }
  let template: TemplateLike;
  try {
    template = await loadTemplateWithLabelRefresh(context, params, issue);
  } catch (e: unknown) {
    const msg = toStringTrim(e instanceof Error ? e.message : e);

    const msgLc = msg.toLowerCase();
    const isRoutingErr = msgLc.includes('no routing label found') || msgLc.includes('cannot resolve template');

    // Blanket / freeform issues
    if (isRoutingErr && !hasIssueFormInputs(issue)) {
      if (DBG) {
        log(
          context,
          'debug',
          { issue: issue.number, err: msg },
          'requestHandler:issues-event skipped (non-form issue)'
        );
      }
      return;
    }

    log(context, 'error', { err: msg }, 'Error loading template in issues handler');

    const userMsg = buildTemplateLoadErrorMessage(msg);
    await postOnce(context, params, userMsg, { minimizeTag: 'nsreq:config' });
    await setStateLabel(context, params, issue, 'author');
    return;
  }

  const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
  if (!isRequestIssue(context, template, parsedFormData)) {
    if (DBG) {
      log(
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
    (await detectSingleRoutingLabel(context, params, issue, toLabelNames(issue.labels)));

  if (expectedRouting) {
    await ensureRoutingLockMarker(context, params, issue, expectedRouting);
    await enforceRoutingLabelLock(context, params, issue, expectedRouting);
  }

  // Closed issues are terminal (Approved/Rejected). Do not re-run the request workflow on them.
  if (toStringTrim(issue.state).toLowerCase() === 'closed') return;

  // If the issue was previously closed as rejected and later reopened, clear that terminal status.
  await removeRejectedStatusLabel(context, params, toLabelNames(issue.labels));

  const currentHash = calcSnapshotHash(parsedFormData, template, readIssueBodyForProcessing(issue.body));

  await normalizeIssueTitle(context, params, issue, template, parsedFormData);

  try {
    await closeOutdatedRequestPrs(context, params, template, { parsedFormData, currentHash });
  } catch (e: unknown) {
    (app.log || console).warn?.({ err: e instanceof Error ? e.message : String(e) }, 'closeOutdatedRequestPRs skipped');
  }

  const result = await validateRequestIssue(context, params, issue, {
    template,
    formData: parsedFormData,
  });

  const { errors, errorsFormattedSingle, errorsFormatted, namespace: validatedNamespace, nsType } = result;

  if (errors?.length) {
    const listFallback = (errors || []).map((e) => `- ${e}`).join('\n');
    const message =
      errorsFormattedSingle?.trim() || errorsFormatted?.trim() || listFallback || 'Unknown validation error.';

    await postOnce(context, params, `## Detected issues\n\n${message}`, {
      minimizeTag: 'nsreq:validation',
    });
    await setStateLabel(context, params, issue, 'author');
    return;
  }

  try {
    const parentError = await checkParentChainExistsInFlatStructure(
      context,
      { owner: params.owner, repo: params.repo },
      template,
      parsedFormData
    );

    if (parentError) {
      await postOnce(context, params, `## Detected issues\n\n- ${parentError}`, {
        minimizeTag: 'nsreq:validation',
      });
      await setStateLabel(context, params, issue, 'author');
      return;
    }
  } catch (e: unknown) {
    (app.log || console).warn?.({ err: e instanceof Error ? e.message : String(e) }, 'parent chain check failed');
  }

  await handoverToCpa(context, params, issue, nsType, validatedNamespace, [], {
    snapshotHash: currentHash,
    requestType: resolveEffectiveRequestType(result.template || template, parsedFormData),
  });
}

async function handleApprovalComment(
  context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<void> {
  const eff = resolveEffectiveConstants(context);

  const allowedApprovers = resolveApproversForRequestType(
    context,
    resolveEffectiveRequestType(template, parsedFormData),
    eff.approverUsernames
  );

  const reviewOk = await ensureReviewLabelsPresentOnIssue(context, params, issue, eff);
  if (!reviewOk) {
    await postOnce(
      context,
      params,
      'Approval ignored: review label missing on the issue. Please ensure the request is marked for review.',
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  const okApprover = isAuthorizedApprover(commenter, issue.user?.login, allowedApprovers);
  if (!okApprover) {
    const hasConfiguredApprovers = allowedApprovers.length > 0;
    const reason = hasConfiguredApprovers
      ? `Approval ignored: commenter ${commenter} is not an allowed approver for this request type.`
      : `Approval ignored: commenter ${commenter} is not allowed to self-approve this request.`;

    await postOnce(context, params, reason, { minimizeTag: 'nsreq:approval-info' });
    return;
  }

  const resourceName = extractResourceNameFromForm(parsedFormData, template).replaceAll('\u00a0', ' ').trim();
  if (!resourceName) {
    await postOnce(
      context,
      params,
      'Cannot create PR: missing resource name in the form (expected identifier, product-id or namespace).',
      { minimizeTag: 'nsreq:config' }
    );
    return;
  }

  const existing = await findOpenIssuePrs(context, { owner: params.owner, repo: params.repo }, issue.number);
  if (existing.length) {
    await postOnce(context, params, `Approved by @${commenter}. PR already open: #${existing[0].number}`, {
      minimizeTag: 'nsreq:approval-info',
    });
    return;
  }

  try {
    const pr = await createRequestPr(context, { owner: params.owner, repo: params.repo }, issue, parsedFormData, {
      template,
    });
    try {
      if (eff.labelOnApproved) {
        await context.octokit.issues.addLabels({ ...params, labels: [eff.labelOnApproved] });
      }
    } catch {
      // ignore
    }
    await removeReviewPendingLabelsAfterApproval(context, params, eff);

    // If the issue is now in a terminal "Approved" state, remove the in-progress status labels.
    try {
      const labelsAfter = await fetchIssueLabels(context, params);
      const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
      if (labelsMatching(labelsAfter, approvedLabel).length) {
        await removeProgressStatusLabels(context, params, labelsAfter);
        await removeRejectedStatusLabel(context, params, labelsAfter);
      }
    } catch {
      // ignore
    }

    await postOnce(context, params, `Approved by @${commenter}. Opened PR: #${pr.number}`, {
      minimizeTag: 'nsreq:approval-info',
    });
  } catch (e: unknown) {
    await postOnce(
      context,
      params,
      `Failed to create PR automatically: ${e instanceof Error ? e.message : String(e)}`,
      { minimizeTag: 'nsreq:approval-info' }
    );
  }
}

async function handleAuthorUpdateComment(
  app: Probot,
  context: BotContext<'issue_comment.created' | 'issue_comment.edited'>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData
): Promise<void> {
  try {
    const reval = await validateRequestIssue(context, params, issue, {
      template,
      formData: parsedFormData,
    });
    const {
      errors: revalErrors,
      errorsFormattedSingle: revalErrorsFormattedSingle,
      errorsFormatted: revalErrorsFormatted,
      namespace,
      nsType,
      template: tpl,
    } = reval;

    if (Array.isArray(revalErrors) && revalErrors.length === 0 && tpl) {
      const parsedAfterUpdate = parseForm(readIssueBodyForProcessing(issue.body), tpl);
      const snapshotHash = calcSnapshotHash(parsedAfterUpdate, tpl, readIssueBodyForProcessing(issue.body));

      try {
        const parentError = await checkParentChainExistsInFlatStructure(
          context,
          { owner: params.owner, repo: params.repo },
          tpl,
          parsedAfterUpdate
        );
        if (parentError) {
          await postOnce(context, params, `## Detected issues\n\n- ${parentError}`, {
            minimizeTag: 'nsreq:validation',
          });
          await setStateLabel(context, params, issue, 'author');
          return;
        }
      } catch (e: unknown) {
        (app.log || console).warn?.({ err: e instanceof Error ? e.message : String(e) }, 'parent chain check failed');
      }

      try {
        await closeOutdatedRequestPrs(context, params, tpl);
      } catch (e: unknown) {
        (app.log || console).warn?.(
          { err: e instanceof Error ? e.message : String(e) },
          'closeOutdatedRequestPRs skipped'
        );
      }

      await handoverToCpa(context, params, issue, nsType, namespace, [], {
        snapshotHash,
        requestType: toStringTrim(tpl?._meta?.requestType),
      });
      return;
    }

    const listFallback = (revalErrors || []).map((e) => `- ${e}`).join('\n');
    const message =
      revalErrorsFormattedSingle?.trim() || revalErrorsFormatted?.trim() || listFallback || 'Unknown validation error.';
    await postOnce(context, params, `## Detected issues\n\n${message}`, {
      minimizeTag: 'nsreq:validation',
    });
    await setStateLabel(context, params, issue, 'author');
  } catch (e: unknown) {
    (app.log || console).warn?.(`Revalidation failed: ${e instanceof Error ? e.message : String(e)}`);
  }
}

async function checkParentChainExistsInFlatStructure(
  context: BotContext<RequestEvents>,
  { owner, repo }: RepoInfo,
  template: TemplateLike,
  formData: FormData
): Promise<string | null> {
  const rootRaw = toStringTrim(template?._meta?.root);
  const STRUCT_ROOT = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!STRUCT_ROOT) return null;

  const resourceName = extractResourceNameFromForm(formData, template);
  const parts = toStringTrim(resourceName).split('.').filter(Boolean);

  // Needs at least 2 segments like "sap.cds"
  if (parts.length < 2) return null;

  // Check parents from closest to last top-level:
  // sap.cds.foo.bar.test -> sap.cds.foo.bar -> sap.cds.foo -> sap.cds
  for (let i = parts.length - 1; i >= 2; i -= 1) {
    const parentName = parts.slice(0, i).join('.');
    const parentFilePath = `${STRUCT_ROOT}/${parentName}.yaml`;

    try {
      await context.octokit.repos.getContent({ owner, repo, path: parentFilePath });
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) {
        return `Parent resource '${parentName}' is not present. Please register the parent first.`;
      }
      throw e;
    }
  }

  return null;
}

async function loadTemplateWithLabelRefresh(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike
): Promise<TemplateLike> {
  let labels = toLabelNames(issue?.labels);

  try {
    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? toStringTrim(e.message) : toStringTrim(e);
    if (!msg.includes('no routing label found')) throw e;

    labels = await fetchIssueLabels(context, params);

    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  }
}

function readPayloadLabelName(payload: unknown): string {
  if (!isPlainObject(payload)) return '';
  const l = payload['label'];
  if (typeof l === 'string') return toStringTrim(l);
  if (isPlainObject(l)) return toStringTrim(l['name']);
  return '';
}

async function tryLoadTemplateForLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<TemplateLike | null> {
  try {
    return await loadTemplate(context, {
      owner: params.owner,
      repo: params.repo,
      issueTitle: toStringTrim(issue?.title || ''),
      issueLabels: labels,
    });
  } catch {
    return null;
  }
}

const ROUTING_LOCK_READ_RE = /<!--\s*nsreq:routing-lock\s*=\s*({[\s\S]*?})\s*-->/i;
const ROUTING_LOCK_STRIP_RE = /<!--\s*nsreq:routing-lock\s*=\s*{[\s\S]*?}\s*-->\s*/gi;

type RoutingLockMeta = { v: 1; expected: string };

function readRoutingLockExpected(issueBody: unknown): string {
  const body = String(issueBody || '');
  const m = body.match(ROUTING_LOCK_READ_RE);
  if (!m) return '';
  try {
    const meta = JSON.parse(String(m[1] || ''));
    return toStringTrim((meta as Record<string, unknown>)?.['expected']);
  } catch {
    return '';
  }
}

function stripRoutingLockFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(ROUTING_LOCK_STRIP_RE, '').trimEnd();
}

function readIssueBodyForProcessing(issueBody: unknown): string {
  return toStringTrim(stripRoutingLockFromBody(issueBody));
}

async function detectRoutingLabels(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<string[]> {
  const uniq: string[] = [];
  const seen = new Set<string>();
  for (const l of labels) {
    const name = toStringTrim(l);
    const key = normalizeKey(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    uniq.push(name);
  }

  const routing: string[] = [];
  for (const l of uniq) {
    const tpl = await tryLoadTemplateForLabels(context, params, issue, [l]);
    if (tpl) routing.push(l);
  }
  return routing;
}

async function detectSingleRoutingLabel(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labels: string[]
): Promise<string> {
  const routing = await detectRoutingLabels(context, params, issue, labels);
  return routing.length === 1 ? routing[0] : '';
}

async function ensureRoutingLockMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  if (!expected) return false;

  const current = readRoutingLockExpected(issue.body);
  if (normalizeKey(current) === normalizeKey(expected)) return false;

  const cleaned = stripRoutingLockFromBody(issue.body);
  const meta: RoutingLockMeta = { v: 1, expected };
  const metaStr = JSON.stringify(meta);
  const nextBody = `${cleaned}\n\n<!-- nsreq:routing-lock = ${metaStr} -->\n`;

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    return true;
  } catch {
    return false;
  }
}

async function enforceRoutingLabelLock(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  expectedLabel: string
): Promise<boolean> {
  const expected = toStringTrim(expectedLabel);
  const expectedKey = normalizeKey(expected);
  if (!expectedKey) return false;

  let labels: string[] = [];
  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    labels = toLabelNames(issue.labels);
  }

  const routingLabels = await detectRoutingLabels(context, params, issue, labels);
  const toRemove = routingLabels.filter((l) => normalizeKey(l) !== expectedKey);

  const hasExpected = labels.some((l) => normalizeKey(l) === expectedKey);

  let changed = false;

  if (toRemove.length) {
    await removeExactLabelsFromIssue(context, params, toRemove);
    changed = true;
  }

  if (!hasExpected) {
    try {
      await context.octokit.issues.addLabels({ ...params, labels: [expected] });
      changed = true;
    } catch {
      // ignore label add errors
    }
  }

  if (changed) {
    await postOnce(context, params, `Routing label is locked to "${expected}". Manual changes were reverted.`, {
      minimizeTag: 'nsreq:routing-label-lock',
    });
  }

  return changed;
}

async function normalizeIssueTitle(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData
): Promise<void> {
  try {
    const resourceName = extractResourceNameFromForm(parsedFormData, template);
    const rawPrefix = toStringTrim(template?.title || template?.name || 'Request');
    const prefix = head(rawPrefix);

    if (!prefix || !resourceName) return;

    const desiredTitle = `${prefix}: ${resourceName}`;
    if (toStringTrim(issue.title) === desiredTitle) return;

    await context.octokit.issues.update({
      owner: params.owner,
      repo: params.repo,
      issue_number: params.issue_number,
      title: desiredTitle,
    });

    issue.title = desiredTitle;
  } catch (err: unknown) {
    log(context, 'warn', { err: err instanceof Error ? err.message : String(err) }, 'Failed to normalize issue title');
  }
}

async function closeOutdatedRequestPrs(
  context: BotContext<RequestEvents>,
  { owner, repo, issue_number }: IssueParams,
  template: TemplateLike,
  options: { parsedFormData?: FormData; currentHash?: string } = {}
): Promise<void> {
  const ensureFormAndHash = async (): Promise<{
    parsedFormData: FormData;
    currentHash: string;
  }> => {
    const { parsedFormData: givenForm, currentHash: givenHash } = options;
    if (givenForm && givenHash) return { parsedFormData: givenForm, currentHash: givenHash };

    const { data } = await context.octokit.issues.get({ owner, repo, issue_number });
    const issue = data as unknown as IssueLike;
    const bodyStr = readIssueBodyForProcessing(issue.body);
    const form = parseForm(bodyStr, template);
    const hash = calcSnapshotHash(form, template, readIssueBodyForProcessing(issue.body));
    return { parsedFormData: form, currentHash: hash };
  };

  const closePr = async (prNum: number, ref: string): Promise<void> => {
    try {
      await context.octokit.pulls.update({ owner, repo, pull_number: prNum, state: 'closed' });
    } catch {
      // ignore
    }
    try {
      await context.octokit.git.deleteRef({ owner, repo, ref: `heads/${ref}` });
    } catch {
      // ignore
    }
  };

  const { currentHash } = await ensureFormAndHash();
  const prs = await findOpenIssuePrs(context, { owner, repo }, issue_number);
  if (!prs.length) return;

  const eff = resolveEffectiveConstants(context);
  const onApproved = eff.labelOnApproved;
  const closed: number[] = [];

  for (const pr of prs) {
    const prHash = extractHashFromPrBody(toStringTrim(pr.body));
    if (prHash && prHash === currentHash) continue;
    await closePr(pr.number, pr.head.ref);
    closed.push(pr.number);
  }

  if (!closed.length) return;

  const list = closed.map((n) => `#${n}`).join(', ');
  await postOnce(
    context,
    { owner, repo, issue_number },
    `Form updated → closing outdated PR(s): ${list}. Please re-approve to open a new PR.`,
    { minimizeTag: 'nsreq:pr-outdated' }
  );

  if (!onApproved) return;
  try {
    await context.octokit.issues.removeLabel({ owner, repo, issue_number, name: onApproved });
  } catch {
    // ignore
  }
}

export default function requestHandler(app: Probot): void {
  const getStaticConfig = async (context: BotContext<RequestEvents>): Promise<NormalizedStaticConfig> => {
    if (context.resourceBotConfig && context.resourceBotHooks !== undefined) return context.resourceBotConfig;

    try {
      const { config, hooks, hooksSource } = await loadStaticConfig(context, {
        validate: false,
        updateIssue: false,
      });

      context.resourceBotConfig = config;
      context.resourceBotHooks = hooks;
      context.resourceBotHooksSource = hooksSource || null;

      return context.resourceBotConfig;
    } catch (err: unknown) {
      (app.log || console).warn?.(
        { err: err instanceof Error ? err.message : String(err) },
        'failed to load resource-bot static config, using defaults'
      );
      context.resourceBotConfig = DEFAULT_CONFIG;
      context.resourceBotHooks = null;
      context.resourceBotHooksSource = null;
      return context.resourceBotConfig;
    }
  };

  const isRequestIssue = (
    context: BotContext<RequestEvents>,
    template: TemplateLike | null | undefined,
    parsedFormData: FormData
  ): boolean => {
    const parsedKeys = Object.keys(parsedFormData || {}).filter(Boolean);

    const meta = template?._meta || {};
    const requestType = String(meta.requestType || '').trim();
    const root = String(meta.root || '').trim();
    const schema = String(meta.schema || '').trim();

    const hasTemplateMeta = Boolean(requestType && root && schema);
    const hasFormData = parsedKeys.length > 0;

    const isReq = Boolean(template) && hasTemplateMeta && hasFormData;

    if (DBG) {
      log(
        context,
        'debug',
        {
          tplPath: String(meta.path || '').trim(),
          requestType,
          root,
          schema,
          parsedKeys,
          isReq,
        },
        'isRequestIssue(new-requests-only)'
      );
    }

    return isReq;
  };

  const shouldSkipIssueEditedEvent = (
    context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>
  ): boolean => {
    const payload = context.payload as unknown;

    const action = isPlainObject(payload) ? toStringTrim(payload['action']) : '';
    if (action !== 'edited') return false;

    const changes = isPlainObject(payload) && 'changes' in payload ? payload['changes'] : undefined;
    const chObj = isPlainObject(changes) ? changes : {};

    const bodyOrLabelChanged = Boolean(chObj['body']) || Boolean(chObj['labels']);
    return !bodyOrLabelChanged;
  };

  // normalizeIssueTitle moved to outer scope
  const isApprovalComment = (context: BotContext<RequestEvents>, strippedText: string): boolean => {
    const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
    const wf = cfg?.workflow ?? {};
    let labelsCfg: Record<string, unknown> = {};
    if (isPlainObject(wf)) {
      const raw = (wf as Record<string, unknown>)['labels'];
      if (isPlainObject(raw)) labelsCfg = raw;
    }

    const approvalSuccessful = labelsCfg['approvalSuccessful'];
    let approvalKeyword = '';
    if (Array.isArray(approvalSuccessful)) approvalKeyword = toStringTrim(approvalSuccessful[0]);
    else if (approvalSuccessful !== undefined && approvalSuccessful !== null)
      approvalKeyword = toStringTrim(approvalSuccessful);

    const lowered = toStringTrim(strippedText).toLowerCase();

    if (approvalKeyword) {
      const kw = String(approvalKeyword).toLowerCase();
      if (kw && lowered.includes(kw)) return true;
    }

    return /\b(approved|approve[ds]?|lgtm)\b/i.test(strippedText || '');
  };

  // moved to outer scope
  app.on(
    ['issues.opened', 'issues.edited', 'issues.reopened'],
    async (context: BotContext<'issues.opened' | 'issues.edited' | 'issues.reopened'>): Promise<void> => {
      await getStaticConfig(context);

      if (shouldSkipIssueEditedEvent(context)) return;

      const sender = context.payload.sender as unknown as SenderLike;
      const action = toStringTrim((context.payload as unknown as Record<string, unknown>)['action']).toLowerCase();
      if (action === 'edited' && isBotSender(sender)) return; // prevent loops

      const issue = context.payload.issue as unknown as IssueLike;

      if (DBG) {
        const safeLabels = toLabelNames(issue?.labels);
        const payload = context.payload as unknown;

        let changesKeys: string[] = [];
        if (isPlainObject(payload) && 'changes' in payload) {
          const c = payload['changes'];
          if (isPlainObject(c)) changesKeys = Object.keys(c);
        }

        log(
          context,
          'debug',
          {
            action: (context.payload as unknown as Record<string, unknown>)?.action,
            issueNumber: issue?.number,
            issueId: issue?.id,
            title: issue?.title,
            state: issue?.state,
            user: issue?.user?.login,
            created_at: issue?.created_at,
            updated_at: issue?.updated_at,
            labels: safeLabels,
            bodyLen: String(issue?.body || '').length,
            bodyHead: String(issue?.body || '').slice(0, 300),
            changesKeys,
          },
          'dbg:issues:payload.issue'
        );
      }

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };
      await processIssueEvent(app, context, params, issue);
    }
  );

  app.on('issues.closed', async (context: BotContext<'issues.closed'>): Promise<void> => {
    await getStaticConfig(context);

    const issue = context.payload.issue as unknown as IssueLike;

    if (!process.env.JEST_WORKER_ID) {
      if (!hasIssueFormInputs(issue)) return;
    }

    const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
    const params: IssueParams = { owner, repo, issue_number: issueNumber };

    let template: TemplateLike;
    try {
      template = await loadTemplateWithLabelRefresh(context, params, issue);
    } catch {
      // Not a request issue
      return;
    }

    const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
    if (!isRequestIssue(context, template, parsedFormData)) return;

    const eff = resolveEffectiveConstants(context);
    const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

    let labels: string[] = [];
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      labels = toLabelNames(issue.labels);
    }

    const hasApproved = labelsMatching(labels, approvedLabel).length > 0;

    // If approved, keep it clean
    if (hasApproved) {
      await removeRejectedStatusLabel(context, params, labels);
      await removeProgressStatusLabels(context, params, labels);
      return;
    }

    // Closed but not approved -> mark as rejected
    const hasRejected = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length > 0;
    if (!hasRejected) {
      try {
        await context.octokit.issues.addLabels({
          ...params,
          labels: [REQUEST_STATUS_LABEL_REJECTED],
        });
      } catch (e: unknown) {
        log(
          context,
          'warn',
          { err: e instanceof Error ? e.message : String(e), label: REQUEST_STATUS_LABEL_REJECTED },
          'failed to add rejected status label'
        );
      }
    }

    // Clean up progress status labels once Rejected is present.
    try {
      labels = await fetchIssueLabels(context, params);
    } catch {
      // best effort
    }

    if (labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED).length) {
      await removeProgressStatusLabels(context, params, labels);

      // enforce mutual exclusivity
      const approvedMatches = labelsMatching(labels, approvedLabel);
      if (approvedMatches.length) {
        await removeExactLabelsFromIssue(context, params, approvedMatches);
      }
    }
  });

  app.on(
    ['issues.labeled', 'issues.unlabeled'],
    async (context: BotContext<'issues.labeled' | 'issues.unlabeled'>): Promise<void> => {
      await getStaticConfig(context);

      const sender = context.payload.sender as unknown as SenderLike;
      if (isBotSender(sender)) return; // prevent loops

      const issue = context.payload.issue as unknown as IssueLike;

      if (!process.env.JEST_WORKER_ID) {
        if (!hasIssueFormInputs(issue)) return;
      }
      const action = toStringTrim((context.payload as unknown as Record<string, unknown>)['action']).toLowerCase();

      const changedLabel = readPayloadLabelName(context.payload as unknown);
      if (!changedLabel) return;

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };

      let labels = toLabelNames(issue.labels);

      const expectedRouting = readRoutingLockExpected(issue.body);
      const hasRoutingLock = Boolean(expectedRouting);

      // Enforce routing label lock. This closes swap/multi-label bypasses.
      if (expectedRouting) {
        const enforced = await enforceRoutingLabelLock(context, params, issue, expectedRouting);
        if (enforced) {
          try {
            labels = await fetchIssueLabels(context, params);
          } catch {
            // ignore
          }
        }
      }

      // 2) Load template
      if (!hasRoutingLock) {
        let template: TemplateLike | null = null;
        try {
          template = await loadTemplateWithLabelRefresh(context, params, issue);
        } catch {
          return;
        }

        const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
        if (!isRequestIssue(context, template, parsedFormData)) return;
      }

      const eff = resolveEffectiveConstants(context);

      // Allow manual switching of progress-state labels (authorAction / approverAction).
      const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
      const wf = cfg?.workflow ?? {};
      const labelsCfg =
        isPlainObject(wf) && isPlainObject((wf as Record<string, unknown>)['labels'])
          ? ((wf as Record<string, unknown>)['labels'] as Record<string, unknown>)
          : {};

      const authorActionLabel = toStringTrim(labelsCfg['authorAction']) || REQUEST_STATUS_LABEL_REQUESTER_ACTION;
      const approverActionLabel = toStringTrim(labelsCfg['approverAction']) || REQUEST_STATUS_LABEL_REVIEW_PENDING;

      const authorActionKey = normalizeKey(authorActionLabel);
      const approverActionKey = normalizeKey(approverActionLabel);
      const isProgressStateLabel = (k: string): boolean => k === authorActionKey || k === approverActionKey;

      const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';

      const lockedKeys = resolveLockedWorkflowLabelKeys(context);
      const changedKey = normalizeKey(changedLabel);

      if (changedKey && lockedKeys.has(changedKey) && !isProgressStateLabel(changedKey)) {
        // Let the existing "Approved label" guard handle manual approval attempts.
        const isManualApprovedAdd = action === 'labeled' && labelsMatching([changedLabel], approvedLabel).length > 0;

        if (!isManualApprovedAdd) {
          if (action === 'labeled') {
            await removeExactLabelsFromIssue(context, params, [changedLabel]);
          } else if (action === 'unlabeled') {
            try {
              await context.octokit.issues.addLabels({ ...params, labels: [changedLabel] });
            } catch {
              // ignore label add errors
            }
          }

          await postOnce(
            context,
            params,
            `Label "${changedLabel}" was reverted. Workflow labels from config are managed by the bot and cannot be changed manually.`,
            { minimizeTag: 'nsreq:workflow-label-lock' }
          );

          return;
        }
      }

      // 3) Manual "Approved" label => rollback
      if (action === 'labeled' && labelsMatching([changedLabel], approvedLabel).length) {
        const approvedMatches = labelsMatching(labels, approvedLabel);
        await removeExactLabelsFromIssue(context, params, approvedMatches);

        // Best effort: keep existing progress label
        const hasAuthor = labelsMatching(labels, authorActionLabel).length > 0;
        const hasReview = labelsMatching(labels, approverActionLabel).length > 0;
        await setStateLabel(context, params, issue, hasAuthor ? 'author' : hasReview ? 'review' : 'review');

        await postOnce(
          context,
          params,
          'Approved label change reverted. Please comment "Approved" to approve a request.',
          { minimizeTag: 'nsreq:label-guard' }
        );
        return;
      }

      // 4) Manual "Rejected" on open issues => rollback
      if (
        action === 'labeled' &&
        labelsMatching([changedLabel], REQUEST_STATUS_LABEL_REJECTED).length &&
        toStringTrim(issue.state).toLowerCase() !== 'closed'
      ) {
        const rejectedMatches = labelsMatching(labels, REQUEST_STATUS_LABEL_REJECTED);
        await removeExactLabelsFromIssue(context, params, rejectedMatches);

        await postOnce(
          context,
          params,
          'Rejected label change reverted. Rejected is set automatically when a request is closed without approval.',
          { minimizeTag: 'nsreq:label-guard' }
        );
        return;
      }

      // 5) Closed issues: enforce terminal state (Approved vs Rejected) + cleanup
      if (toStringTrim(issue.state).toLowerCase() === 'closed') {
        let latest = labels;
        try {
          latest = await fetchIssueLabels(context, params);
        } catch {
          // ignore
        }

        const hasApproved = labelsMatching(latest, approvedLabel).length > 0;

        if (hasApproved) {
          await removeRejectedStatusLabel(context, params, latest);
          await removeProgressStatusLabels(context, params, latest);
          return;
        }

        const hasRejected = labelsMatching(latest, REQUEST_STATUS_LABEL_REJECTED).length > 0;
        if (!hasRejected) {
          try {
            await context.octokit.issues.addLabels({
              ...params,
              labels: [REQUEST_STATUS_LABEL_REJECTED],
            });
          } catch {
            // ignore
          }
        }

        try {
          latest = await fetchIssueLabels(context, params);
        } catch {
          // ignore
        }

        await removeProgressStatusLabels(context, params, latest);

        // mutual exclusivity
        const approvedMatches = labelsMatching(latest, approvedLabel);
        if (approvedMatches.length) {
          await removeExactLabelsFromIssue(context, params, approvedMatches);
        }
        return;
      }
    }
  );

  app.on(
    ['issue_comment.created', 'issue_comment.edited'],
    async (context: BotContext<'issue_comment.created' | 'issue_comment.edited'>): Promise<void> => {
      await getStaticConfig(context);

      const issue = context.payload.issue as unknown as IssueLike;
      if (!process.env.JEST_WORKER_ID) {
        if (!hasIssueFormInputs(issue)) return;
      }
      const comment = context.payload.comment as unknown as CommentLike;
      const sender = context.payload.sender as unknown as SenderLike;

      const commenter = String(comment?.user?.login || '');

      if (DBG) {
        log(
          context,
          'debug',
          {
            event: context.name,
            action: (context.payload as unknown as Record<string, unknown>)?.action,
            issue: issue?.number,
            commenter,
          },
          'requestHandler:issue-comment-event'
        );
      }

      if (isBotSender(sender)) return;

      const { owner, repo, issue_number: issueNumber } = context.issue() as IssueParams;
      const params: IssueParams = { owner, repo, issue_number: issueNumber };

      let template: TemplateLike;
      try {
        template = await loadTemplateWithLabelRefresh(context, params, issue);
      } catch (e: unknown) {
        log(
          context,
          'error',
          { err: e instanceof Error ? e.message : String(e), owner, repo, issue: issue?.number },
          'Error loading template in issue_comment handler'
        );
        return;
      }

      const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
      if (!isRequestIssue(context, template, parsedFormData)) {
        if (DBG) {
          log(
            context,
            'debug',
            { issue: issue.number, parsedKeys: Object.keys(parsedFormData || {}) },
            'requestHandler:issue-comment-event skipped (not a request issue)'
          );
        }
        return;
      }

      const stripped = stripQuoteAndCode(comment.body || '');
      const isApproval = isApprovalComment(context, stripped);
      if (isApproval) {
        await handleApprovalComment(context, params, issue, template, parsedFormData, commenter);
        return;
      }

      if (comment.user.login === issue.user?.login) {
        const saysUpdated = /\b(updated|update|fixed|fix(ed)?|addressed|done)\b/i.test(toStringTrim(comment.body));
        if (!saysUpdated) return;
        await handleAuthorUpdateComment(app, context, params, issue, template, parsedFormData);
      }
    }
  );

  const tryAutoMerge = async (
    context: BotContext<'check_suite.completed' | 'check_run.completed' | 'status'>,
    repoInfo: RepoInfo,
    headSha: string
  ): Promise<void> => {
    const { owner, repo } = repoInfo;
    await getStaticConfig(context);

    let page = 1;
    const candidates: PullRequestLike[] = [];

    while (true) {
      const { data } = await context.octokit.pulls.list({
        owner,
        repo,
        state: 'open',
        per_page: 100,
        page,
      });

      const prs = (data || []) as unknown as PullRequestLike[];
      if (!prs.length) break;

      candidates.push(...prs.filter((pr) => pr.head?.sha === headSha));
      if (prs.length < 100) break;

      page += 1;
    }

    for (const pr of candidates) {
      const body = String(pr.body || '');
      const m = /source:\s*#(\d+)/i.exec(body) ?? /issue\s*#(\d+)/i.exec(body);
      if (!m) continue;

      const issueNumber = Number.parseInt(m[1], 10);
      const params: IssueParams = { owner, repo, issue_number: issueNumber };

      let issue: IssueLike;
      try {
        const res = await context.octokit.issues.get({ owner, repo, issue_number: issueNumber });
        issue = res.data as unknown as IssueLike;
      } catch {
        continue;
      }

      if (!process.env.JEST_WORKER_ID) {
        if (!hasIssueFormInputs(issue)) continue;
      }

      let template: TemplateLike;
      try {
        template = await loadTemplateWithLabelRefresh(context, params, issue);
      } catch {
        continue;
      }

      const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
      if (!isRequestIssue(context, template, parsedFormData)) continue;

      const currentHash = calcSnapshotHash(parsedFormData, template, readIssueBodyForProcessing(issue.body));
      const prHash = extractHashFromPrBody(body);

      if (!prHash || prHash !== currentHash) {
        await closeOutdatedRequestPrs(context, params, template, { parsedFormData, currentHash });
        continue;
      }

      await tryMergeIfGreen(context, {
        owner,
        repo,
        prNumber: pr.number,
        mergeMethod: 'squash',
        prData: pr,
      });
    }
  };

  app.on(
    ['check_suite.completed', 'check_run.completed'],
    async (context: BotContext<'check_suite.completed' | 'check_run.completed'>): Promise<void> => {
      const payload = context.payload as unknown;

      // Avoid duplicate posting (both events fire). Handle only check_suite.
      if (context.name === 'check_run.completed') return;

      if (DBG) {
        log(
          context,
          'debug',
          { event: context.name, action: isPlainObject(payload) ? payload['action'] : undefined },
          'dbg:checks:event received'
        );
      }

      const suite = isPlainObject(payload) && 'check_suite' in payload ? payload['check_suite'] : undefined;

      let conclusionRaw: unknown;
      if (isPlainObject(suite)) conclusionRaw = suite['conclusion'];

      let headShaRaw: unknown;
      if (isPlainObject(suite)) headShaRaw = suite['head_sha'];

      const conclusion = toStringTrim(conclusionRaw).toLowerCase();
      const headShaStr = toStringTrim(headShaRaw);

      const repoObj = isPlainObject(payload) ? payload['repository'] : undefined;
      const repoName = isPlainObject(repoObj) ? toStringTrim(repoObj['name']) : '';
      const ownerObj = isPlainObject(repoObj) ? repoObj['owner'] : undefined;
      const ownerLogin = isPlainObject(ownerObj) ? toStringTrim(ownerObj['login']) : '';

      if (!ownerLogin || !repoName) return;

      if (DBG) {
        log(context, 'debug', { ownerLogin, repoName, conclusion, headShaStr }, 'dbg:checks:context resolved');
      }

      // success -> keep existing auto-merge behavior
      if (conclusion === 'success') {
        if (!headShaStr) return;
        await tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, headShaStr);
        return;
      }

      // failure -> comment on PR if registry-validate annotations exist
      const checkSuite = readCheckSuiteFromPayload(context.payload as unknown);
      const suiteId = readCheckSuiteId(checkSuite);
      if (!suiteId) return;

      const prNumbers = readCheckSuitePrNumbers(checkSuite);
      if (!prNumbers.length) return;

      if (DBG) {
        log(context, 'debug', { suiteId, prNumbers }, 'dbg:checks:failure suite');
      }

      let runsForSuite: CheckRunLike[] = [];
      try {
        runsForSuite = await listAllCheckRunsForSuite(context, ownerLogin, repoName, suiteId);
        if (DBG) {
          log(
            context,
            'debug',
            {
              suiteId,
              runsForSuite: runsForSuite.map((r) => ({
                id: readCheckRunId(r),
                conclusion: toStringTrim(r.conclusion),
                url: toStringTrim(r.html_url),
              })),
            },
            'dbg:checks:runs listed for suite'
          );
        }
      } catch {
        return;
      }

      // Build PR "files changed" URLs once (best-effort).
      const prFilesUrlByNumber = new Map<number, string>();
      for (const prNumber of prNumbers) {
        try {
          const pr = await context.octokit.pulls.get({
            owner: ownerLogin,
            repo: repoName,
            pull_number: prNumber,
          });
          // pr.data is expected to be PullRequestLike, but may have extra fields
          const html = toStringTrim((pr.data as { html_url?: string })?.html_url);
          if (html) prFilesUrlByNumber.set(prNumber, `${html}/files`);
        } catch {
          // ignore
        }
      }

      // Find the first run that contains registry-validate annotations and post from it.
      for (const r of runsForSuite) {
        const runId = readCheckRunId(r);
        if (!runId) continue;

        let annotations: CheckRunAnnotationLike[] = [];
        try {
          annotations = await listAllCheckRunAnnotations(context, ownerLogin, repoName, runId);
        } catch {
          continue;
        }

        const relevant = annotations.filter(isRegistryValidateAnnotation);
        if (DBG) {
          log(
            context,
            'debug',
            { checkRunId: runId, annotationsTotal: annotations.length, relevant: relevant.length },
            'dbg:checks:annotations loaded (suite run)'
          );
        }
        if (!relevant.length) continue;

        const byFile = new Map<string, string[]>();
        for (const a of relevant) {
          const file = toStringTrim(a.path) || 'unknown file';
          const rawMsg = toStringTrim(a.message) || toStringTrim(a.raw_details);
          const msg = stripRegistrySuffix(rawMsg);
          if (!msg) continue;
          const arr = byFile.get(file) ?? [];
          arr.push(msg);
          byFile.set(file, arr);
        }

        for (const [file, msgs] of byFile.entries()) {
          for (const prNumber of prNumbers) {
            const body = buildRegistryValidationPrCommentBody(file, msgs);
            if (!body) continue;

            if (DBG) {
              log(context, 'debug', { prNumber, file, bodyLen: body.length }, 'dbg:checks:posting PR comment');
            }

            // Use per-file tag to avoid overwriting when multiple files fail.
            await postOnce(context, { owner: ownerLogin, repo: repoName, issue_number: prNumber }, body, {
              minimizeTag: `nsreq:ci-validation:${normalizeKey(file)}`,
            });
          }
        }

        break; // avoid spamming multiple runs/suite events
      }
    }
  );

  app.on('status', async (context: BotContext<'status'>): Promise<void> => {
    const payload = context.payload as unknown;
    const state = isPlainObject(payload) ? toStringTrim(payload['state']) : '';
    if (state !== 'success') return;

    const repoObj = isPlainObject(payload) ? payload['repository'] : undefined;
    const repoName = isPlainObject(repoObj) ? toStringTrim(repoObj['name']) : '';
    const ownerObj = isPlainObject(repoObj) ? repoObj['owner'] : undefined;
    const ownerLogin = isPlainObject(ownerObj) ? toStringTrim(ownerObj['login']) : '';

    const sha = isPlainObject(payload) ? toStringTrim(payload['sha']) : '';
    if (!ownerLogin || !repoName || !sha) return;

    await tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, sha);
  });
}
