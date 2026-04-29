import { setStateLabel as setStateLabelRaw, ensureAssigneesOnce as ensureAssigneesOnceRaw } from './state.js';
import { postOnce as postOnceRaw, collapseBotCommentsByPrefix as collapseBotCommentsByPrefixRaw } from './comments.js';
import { loadTemplate as loadTemplateRaw, parseForm as parseFormRaw } from './template.js';
import {
  validateRequestIssue as validateRequestIssueRaw,
  runApprovalHook as runApprovalHookRaw,
} from './validation/run.js';
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
import YAML from 'yaml';
import { createHash } from 'node:crypto';

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
  | 'status'
  | 'push';

type ResourceBotContextExt = {
  resourceBotConfig?: NormalizedStaticConfig;
  resourceBotHooks?: RegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type StaticConfigLoadOptions = {
  forceReload?: boolean;
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

type CollapseBotCommentsByPrefixOptions = {
  perPage?: number;
  tagPrefix: string;
  keepTags?: string[];
  collapseBody?: string;
  classifier?: 'OUTDATED' | 'RESOLVED' | 'DUPLICATE' | 'OFF_TOPIC' | 'SPAM' | 'ABUSE';
};

type PullRequestRepoLike = {
  name?: string | null;
  full_name?: string | null;
  owner?: UserLike | null;
};

type PullRequestBranchLike = {
  ref: string;
  sha: string;
  repo?: PullRequestRepoLike | null;
};

type PullRequestLike = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: UserLike | null;
  head: PullRequestBranchLike;
  base?: PullRequestBranchLike;

  mergeable?: boolean | null;
  mergeable_state?: string | null;
  draft?: boolean | null;
};

type PullRequestFileLike = {
  filename?: string | null;
  status?: string | null;
};

type PullRequestCommitLike = {
  author?: UserLike | null;
  committer?: UserLike | null;
};

type PullRequestReviewLike = {
  id?: number | null;
  state?: string | null;
  body?: string | null;
  submitted_at?: string | null;
  user?: UserLike | null;
};

type RefCheckRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
};

type CheckRunPullRequestRef = { number?: number | null };

type CheckRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
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

type GitTreeEntryLike = {
  path?: string | null;
  type?: string | null;
  sha?: string | null;
};

type GitTreeLike = {
  tree?: GitTreeEntryLike[];
};

type DirectPrApprovalOptions = {
  baseBranch?: string;
};

type AutomatedApprovalReviewOptions = {
  skipApprovedLabelStateCleanup?: boolean;
};

type HeadGreenRunSummary = {
  id?: number;
  name: string;
  status: string;
  conclusion: string;
};

type HeadGreenEvaluation = {
  green: boolean;
  reason: string;
  latestRuns: HeadGreenRunSummary[];
  blockingRuns: HeadGreenRunSummary[];
  statusState?: string;
};

type ValidateRequestIssueResult = {
  errors: string[];
  errorsGrouped?: unknown;
  errorsFormatted: string;
  errorsFormattedSingle: string;
  validationIssues?: { message: string; path: string }[];
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
  approverPoolUsernames: string[];
};

type MachineReadableIssue = Readonly<{
  field: string;
  message: string;
  filePath?: string;
}>;

type RegistryValidationMachineReadableSource = Readonly<{
  filePath: string;
  message: string;
  schemaPath?: string;
}>;

type SchemaFieldAliasLookup = Map<string, string>;

function normalizeMachineReadableIssues(value: unknown): MachineReadableIssue[] {
  const items = Array.isArray(value) ? value : [];
  const out: MachineReadableIssue[] = [];
  const seen = new Set<string>();

  for (const item of items) {
    if (!isPlainObject(item)) continue;

    const message = toStringTrim(item['message']);
    const field = toStringTrim(item['field'] ?? item['path']) || 'details';
    const filePath = toStringTrim(item['filePath']);

    if (!message) continue;

    const key = `${field}\u0000${filePath}\u0000${message}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      field,
      message,
      ...(filePath ? { filePath } : {}),
    });
  }

  return out;
}

function buildMachineReadableMetadataBlock(issues: MachineReadableIssue[]): string {
  const normalized = normalizeMachineReadableIssues(issues);
  if (!normalized.length) return '';

  return `
##
<details>
<summary>Show as JSON (Robots Friendly)</summary>

\`\`\`json
${JSON.stringify(normalized, null, 2)}
\`\`\`
</details>`;
}

function buildDetectedIssuesBody(message: string, issues: MachineReadableIssue[] = []): string {
  return `## Detected issues

${message}${buildMachineReadableMetadataBlock(issues)}`;
}

function singleMachineReadableIssue(field: string, message: string, filePath = ''): MachineReadableIssue[] {
  const normalizedMessage = toStringTrim(message);
  const normalizedField = toStringTrim(field) || 'details';
  const normalizedFilePath = toStringTrim(filePath);

  return normalizedMessage
    ? [
        {
          field: normalizedField,
          message: normalizedMessage,
          ...(normalizedFilePath ? { filePath: normalizedFilePath } : {}),
        },
      ]
    : [];
}

const SCHEMA_FIELD_ALIAS_CACHE = new Map<string, Promise<SchemaFieldAliasLookup>>();
const MERGE_INFLIGHT = new Map<string, Promise<void>>();
const AUTO_APPROVED_PR_HEADS = new Set<string>();

const AUTO_APPROVAL_REVIEW_INFLIGHT = new Map<string, Promise<boolean>>();
const AUTO_MERGE_EVALUATION_INFLIGHT = new Map<string, Promise<void>>();

const AUTO_MERGE_EVALUATION_RECENT_UNTIL = new Map<string, number>();
const AUTO_MERGE_EVALUATION_RECENT_TTL_MS = 30_000;

function normalizeSchemaFieldAlias(value: unknown): string {
  const raw = toStringTrim(value);
  if (!raw) return '';

  return raw
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function addSchemaFieldAlias(lookup: SchemaFieldAliasLookup, aliasValue: unknown, propertyName: string): void {
  const alias = normalizeSchemaFieldAlias(aliasValue);
  if (!alias || lookup.has(alias)) return;

  lookup.set(alias, propertyName);

  if (alias.endsWith('s') && alias.length > 1) {
    const singular = alias.slice(0, -1);
    if (singular && !lookup.has(singular)) lookup.set(singular, propertyName);
  } else {
    const plural = `${alias}s`;
    if (!lookup.has(plural)) lookup.set(plural, propertyName);
  }
}

function collectSchemaFieldAliasesForProperty(
  propertyName: string,
  propertyDef: unknown,
  lookup: SchemaFieldAliasLookup
): void {
  addSchemaFieldAlias(lookup, propertyName, propertyName);
  if (!isPlainObject(propertyDef)) return;

  addSchemaFieldAlias(lookup, propertyDef['title'], propertyName);
  addSchemaFieldAlias(lookup, propertyDef['x-form-field'], propertyName);
  collectSchemaFieldAliases(propertyDef, lookup);
}

function collectSchemaFieldAliasesFromProperties(props: Record<string, unknown>, lookup: SchemaFieldAliasLookup): void {
  for (const [propertyName, propertyDef] of Object.entries(props)) {
    collectSchemaFieldAliasesForProperty(propertyName, propertyDef, lookup);
  }
}

function collectSchemaFieldAliasesFromArray(items: unknown[], lookup: SchemaFieldAliasLookup): void {
  for (const item of items) {
    collectSchemaFieldAliases(item, lookup);
  }
}

function collectSchemaFieldAliases(schemaObj: unknown, lookup: SchemaFieldAliasLookup): void {
  if (!isPlainObject(schemaObj)) return;

  const props = isPlainObject(schemaObj['properties']) ? schemaObj['properties'] : null;
  if (props) collectSchemaFieldAliasesFromProperties(props, lookup);

  for (const key of ['allOf', 'anyOf', 'oneOf'] as const) {
    const items = schemaObj[key];
    if (!Array.isArray(items)) continue;

    collectSchemaFieldAliasesFromArray(items, lookup);
  }

  const defs = isPlainObject(schemaObj['$defs']) ? schemaObj['$defs'] : null;
  if (defs) {
    for (const value of Object.values(defs)) {
      collectSchemaFieldAliases(value, lookup);
    }
  }
}

async function loadSchemaFieldAliasLookup(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  schemaPath: string
): Promise<SchemaFieldAliasLookup> {
  const rawPath = toStringTrim(schemaPath);
  if (!rawPath) return new Map<string, string>();

  const cleaned = rawPath.replace(/^\.?\//, '');
  const candidates = rawPath.startsWith('/')
    ? [rawPath.replace(/^\/+/, '')]
    : [cleaned.startsWith('.github/') ? cleaned : `.github/registry-bot/${cleaned}`, cleaned];

  const cacheKey = `${repoInfo.owner}/${repoInfo.repo}:${JSON.stringify(candidates)}`;
  const cached = SCHEMA_FIELD_ALIAS_CACHE.get(cacheKey);
  if (cached) return await cached;

  const pending = (async (): Promise<SchemaFieldAliasLookup> => {
    for (const candidate of candidates) {
      const raw = await readRepoFileText(context, repoInfo, candidate);
      if (!raw) continue;

      try {
        const parsed = JSON.parse(raw) as unknown;
        const lookup = new Map<string, string>();
        collectSchemaFieldAliases(parsed, lookup);
        return lookup;
      } catch {
        continue;
      }
    }

    return new Map<string, string>();
  })();

  SCHEMA_FIELD_ALIAS_CACHE.set(cacheKey, pending);
  return await pending;
}

async function resolveMachineReadableRegistryField(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  fieldHint: string,
  schemaPath?: string
): Promise<string> {
  const fallback = toStringTrim(fieldHint) || 'details';
  const normalizedSchemaPath = toStringTrim(schemaPath);

  if (!normalizedSchemaPath || fallback === 'details') return fallback;

  const lookup = await loadSchemaFieldAliasLookup(context, repoInfo, normalizedSchemaPath);
  if (!lookup.size) return fallback;

  return lookup.get(normalizeSchemaFieldAlias(fallback)) || fallback;
}

async function buildRegistryValidationMachineReadableIssues(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  items: RegistryValidationMachineReadableSource[]
): Promise<MachineReadableIssue[]> {
  const out: MachineReadableIssue[] = [];

  for (const item of items || []) {
    const message = normalizeMsg(item.message);
    if (!message) continue;

    const fieldHint = extractFieldFromMsg(item.message) || 'details';
    const field = await resolveMachineReadableRegistryField(context, repoInfo, fieldHint, item.schemaPath);
    const normalizedFilePath = toStringTrim(item.filePath);

    out.push({
      field,
      message,
      ...(normalizedFilePath ? { filePath: normalizedFilePath } : {}),
    });
  }

  return normalizeMachineReadableIssues(out);
}

function normalizeApprovalHookErrorsForComment(decision: ApprovalDecision): MachineReadableIssue[] {
  const raw = Array.isArray(decision.errors) ? decision.errors : [];
  const mapped = raw.map((entry) => ({
    field: toStringTrim(entry?.field) || 'details',
    message: toStringTrim(entry?.message),
  }));

  const normalized = normalizeMachineReadableIssues(mapped);
  if (normalized.length) return normalized;

  const fallbackMessage =
    toStringTrim(decision.message) || toStringTrim(decision.reason) || toStringTrim(decision.comment);
  const fallbackField = toStringTrim(decision.path) || 'details';

  return fallbackMessage ? [{ field: fallbackField, message: fallbackMessage }] : [];
}

function buildApprovalHookIssueList(issues: MachineReadableIssue[]): string {
  const normalized = normalizeMachineReadableIssues(issues);
  if (!normalized.length) return '';

  const grouped = new Map<string, string[]>();

  for (const issue of normalized) {
    const key = toStringTrim(issue.field) || 'details';
    const arr = grouped.get(key) ?? [];
    if (!arr.includes(issue.message)) arr.push(issue.message);
    grouped.set(key, arr);
  }

  const keys = Array.from(grouped.keys()).sort((a, b) => {
    if (a === 'details') return 1;
    if (b === 'details') return -1;
    return a.localeCompare(b);
  });

  const lines: string[] = [];
  for (const key of keys) {
    lines.push(`### ${toSectionTitle(key)}`);
    for (const msg of grouped.get(key) ?? []) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }

  return lines.join('\n').trim();
}

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

function normalizeLogin(value: unknown): string {
  return toStringTrim(value).replace(/^@+/, '').trim();
}

function uniqLogins(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const v of values || []) {
    const s = normalizeLogin(v);
    if (!s) continue;
    const k = s.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out;
}

type RepoContentFile = { content?: string; encoding?: string };

function isRepoContentFile(v: unknown): v is RepoContentFile {
  return isPlainObject(v) && typeof v['content'] === 'string';
}

async function readRepoFileText(
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  path: string
): Promise<string | null> {
  const p = toStringTrim(path).replace(/^\/+/, '');
  if (!p) return null;

  try {
    const res = await context.octokit.repos.getContent({ owner: repo.owner, repo: repo.repo, path: p });
    const data = (res as unknown as { data?: unknown }).data;

    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    const enc = typeof data.encoding === 'string' ? data.encoding : 'base64';
    return Buffer.from(String(data.content || ''), enc as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

async function readYamlFromRepo(
  context: BotContext<RequestEvents>,
  repo: RepoInfo,
  path: string
): Promise<unknown | null> {
  const txt = await readRepoFileText(context, repo, path);
  if (!txt) return null;

  try {
    return YAML.parse(txt);
  } catch {
    return null;
  }
}

const LOGIN_RE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function extractParentContactCandidates(value: unknown): { logins: string[]; emails: string[] } {
  const logins: string[] = [];
  const emails: string[] = [];

  const pushLogin = (v: unknown): void => {
    const s = normalizeLogin(v);
    if (!s) return;
    if (!LOGIN_RE.test(s)) return;
    logins.push(s);
  };

  const pushEmail = (v: unknown): void => {
    const s = toStringTrim(v);
    if (!s) return;
    const t = s.replace(/^<|>$/g, '').trim();
    if (!EMAIL_RE.test(t)) return;
    emails.push(t);
  };

  const fromString = (raw: string, strongLoginHint: boolean): void => {
    const s = toStringTrim(raw);
    if (!s) return;

    const urlM =
      /(?:https?:\/\/)?(?:www\.)?(?:github\.com|github\.tools\.sap)\/([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)/gi;
    for (const m of s.matchAll(urlM)) {
      if (m?.[1]) pushLogin(m[1]);
    }

    const tokens = s
      .split(/[,\s;]+/)
      .map((t) => t.trim())
      .filter(Boolean);

    for (let t of tokens) {
      t = t.replace(/^[<([{"']+|[>)\]},"']+$/g, '').trim();
      if (!t) continue;

      if (t.includes('@') && EMAIL_RE.test(t)) {
        pushEmail(t);
        continue;
      }

      if (t.startsWith('@')) {
        const u = t.slice(1);
        if (u && !u.includes('.') && LOGIN_RE.test(u)) pushLogin(u);
        continue;
      }

      if ((strongLoginHint || tokens.length === 1) && !t.includes('.') && LOGIN_RE.test(t)) {
        pushLogin(t);
      }
    }
  };

  const walk = (v: unknown, keyHint?: string): void => {
    if (v === null || v === undefined) return;

    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
      const k = String(keyHint || '').toLowerCase();
      const strong = ['github', 'login', 'username', 'user', 'owner', 'id', 'uid', 'account', 'gh'].some((x) =>
        k.includes(x)
      );
      fromString(String(v), strong);
      return;
    }

    if (Array.isArray(v)) {
      for (const el of v) walk(el, keyHint);
      return;
    }

    if (isPlainObject(v)) {
      for (const [k, vv] of Object.entries(v)) walk(vv, k);
      return;
    }
  };

  walk(value);

  return { logins: uniqLogins(logins), emails: Array.from(new Set(emails.map((e) => e.toLowerCase()))) };
}

const EMAIL_TO_LOGINS_CACHE = new Map<string, Promise<string[]>>();

async function lookupGithubLoginsByEmail(context: BotContext<RequestEvents>, email: string): Promise<string[]> {
  const e = toStringTrim(email).toLowerCase();
  if (!e || !e.includes('@')) return [];

  const cached = EMAIL_TO_LOGINS_CACHE.get(e);
  if (cached) return await cached;

  const p = (async (): Promise<string[]> => {
    const found: string[] = [];
    const q = `${e} in:email`;

    try {
      const res = await context.octokit.search.users({ q, per_page: 5 });
      const items = (res as unknown as { data?: { items?: { login?: string }[] } })?.data?.items ?? [];
      for (const it of items) {
        const login = normalizeLogin(it?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    if (found.length) return uniqLogins(found);

    try {
      const gql = `
        query($q: String!) {
          search(type: USER, query: $q, first: 5) {
            nodes { ... on User { login } }
          }
        }
      `;
      const r = await (
        context.octokit as unknown as {
          graphql: (q: string, v: unknown) => Promise<{ search?: { nodes?: { login?: string }[] } }>;
        }
      ).graphql(gql, { q });

      const nodes = r?.search?.nodes ?? [];
      for (const n of nodes) {
        const login = normalizeLogin(n?.login);
        if (login) found.push(login);
      }
    } catch {
      /* empty */
    }

    return uniqLogins(found);
  })();

  EMAIL_TO_LOGINS_CACHE.set(e, p);
  return await p;
}

async function resolveParentOwnerLoginsForTarget(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  template: TemplateLike,
  validatedNamespace: string,
  requestType: string
): Promise<{ parent: string; owners: string[] }> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) return { parent: '', owners: [] };

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean);

  if (parts.length <= 2) return { parent: '', owners: [] };

  const parent = parts.slice(0, -1).join('.');
  if (!parent) return { parent: '', owners: [] };

  const rootRaw = toStringTrim(template?._meta?.root);
  const root = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!root) return { parent, owners: [] };

  const parentPath = `${root}/${parent}.yaml`;
  const doc = await readYamlFromRepo(context, { owner: params.owner, repo: params.repo }, parentPath);
  if (!isPlainObject(doc)) return { parent, owners: [] };

  const rec = doc;
  const contacts = rec['contacts'] ?? rec['contact'] ?? rec['owners'] ?? rec['owner'];

  const { logins: directLogins, emails } = extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await lookupGithubLoginsByEmail(context, email)));
  }

  return { parent, owners: uniqLogins(resolved) };
}

function readCheckRunId(run: CheckRunLike | null): number | null {
  const id = run?.id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

type CheckSuitePullRequestRef = { number?: number | null };

type CheckSuiteLike = {
  id?: number | null;
  status?: string | null;
  conclusion?: string | null;
  head_sha?: string | null;
  head_branch?: string | null;
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

async function resolveCheckSuitePrNumbers(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  suite: CheckSuiteLike | null,
  headSha: string
): Promise<number[]> {
  const direct = readCheckSuitePrNumbers(suite);
  if (direct.length) return Array.from(new Set(direct));

  const sha = toStringTrim(headSha);
  if (!sha) return [];

  try {
    const res = await context.octokit.repos.listPullRequestsAssociatedWithCommit({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      commit_sha: sha,
      per_page: 100,
    });

    const data = (res as unknown as { data?: unknown }).data;
    const items = Array.isArray(data) ? data : [];

    const fromCommit = items
      .map((pr) => {
        if (!isPlainObject(pr)) return null;

        const state = toStringTrim(pr['state']).toLowerCase();
        const number = pr['number'];

        if (state !== 'open') return null;
        if (typeof number !== 'number' || !Number.isFinite(number)) return null;

        return number;
      })
      .filter((n): n is number => typeof n === 'number');

    if (fromCommit.length) return Array.from(new Set(fromCommit));
  } catch {
    // ignore and fall through to the repo scan fallback
  }

  const matches: number[] = [];
  let page = 1;

  while (true) {
    const { data } = await context.octokit.pulls.list({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      state: 'open',
      per_page: 100,
      page,
    });

    const prs = (data || []) as unknown as PullRequestLike[];
    if (!prs.length) break;

    for (const pr of prs) {
      if (toStringTrim(pr.head?.sha) !== sha) continue;
      if (typeof pr.number !== 'number' || !Number.isFinite(pr.number)) continue;
      matches.push(pr.number);
    }

    if (prs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return Array.from(new Set(matches));
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

function groupRegistryValidationMessages(messages: string[]): Map<string, string[]> {
  const grouped = new Map<string, string[]>();

  for (const raw of messages) {
    const field = extractFieldFromMsg(raw) || 'details';
    const msg = normalizeMsg(raw);
    if (!msg) continue;

    const arr = grouped.get(field) ?? [];
    if (!arr.includes(msg)) arr.push(msg);
    grouped.set(field, arr);
  }

  return grouped;
}

function sortRegistryValidationGroupKeys(grouped: Map<string, string[]>): string[] {
  return Array.from(grouped.keys()).sort((a, b) => {
    if (a === 'details') return 1;
    if (b === 'details') return -1;
    return a.localeCompare(b);
  });
}

function appendRegistryValidationSections(lines: string[], grouped: Map<string, string[]>, headingLevel: string): void {
  for (const key of sortRegistryValidationGroupKeys(grouped)) {
    lines.push(`${headingLevel} ${toSectionTitle(key)}`);
    for (const msg of grouped.get(key) ?? []) {
      lines.push(`- ${msg}`);
    }
    lines.push('');
  }
}

function appendRegistryValidationFileSection(lines: string[], filePath: string, messages: string[]): void {
  lines.push(`### File: \`${filePath}\``, '');
  appendRegistryValidationSections(lines, groupRegistryValidationMessages(messages), '####');
}

async function buildRegistryValidationPrCommentBody(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  filePath: string,
  messages: string[],
  machineReadableSources: RegistryValidationMachineReadableSource[]
): Promise<string> {
  const lines: string[] = ['## Detected issues', '', `### File: \`${filePath}\``, ''];

  appendRegistryValidationSections(lines, groupRegistryValidationMessages(messages), '###');

  const body = lines.join('\n').trimEnd();
  const machineReadable = await buildRegistryValidationMachineReadableIssues(context, repoInfo, machineReadableSources);

  return `${body}

${buildMachineReadableMetadataBlock(machineReadable)}`;
}

async function buildRegistryValidationAggregatePrCommentBody(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  byFile: Map<string, string[]>,
  machineReadableSources: RegistryValidationMachineReadableSource[]
): Promise<string> {
  const entries = Array.from(byFile.entries())
    .filter(([, messages]) => Array.isArray(messages) && messages.length > 0)
    .sort(([a], [b]) => a.localeCompare(b));

  if (!entries.length) return '';
  if (entries.length === 1) {
    const [filePath, messages] = entries[0];
    return await buildRegistryValidationPrCommentBody(
      context,
      repoInfo,
      filePath,
      messages,
      machineReadableSources.filter((item) => toStringTrim(item.filePath) === toStringTrim(filePath))
    );
  }

  const lines: string[] = ['## Detected issues', ''];

  for (const [filePath, messages] of entries) {
    appendRegistryValidationFileSection(lines, filePath, messages);
  }

  const machineReadable = await buildRegistryValidationMachineReadableIssues(context, repoInfo, machineReadableSources);

  return `${lines.join('\n').trimEnd()}

${buildMachineReadableMetadataBlock(machineReadable)}`;
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

function normalizeApprovalCommandToken(value: unknown): string {
  let s = toStringTrim(value).replace(/^\/+/, '').trim().toLowerCase();

  const leadingTrimChars = new Set(['"', "'", '`', '(', '[', '{', '<']);
  const trailingTrimChars = new Set(['"', "'", '`', ')', ']', '}', '>', '.', ',', '!', '?', ';', ':']);

  while (s && leadingTrimChars.has(s[0])) s = s.slice(1).trim();
  while (s && trailingTrimChars.has(s.at(-1) || '')) s = s.slice(0, -1).trim();

  return s;
}

function isExplicitApprovalCommand(text: unknown, configuredKeyword?: string): boolean {
  const lines = toStringTrim(text)
    .split(/\r?\n/)
    .map((line) => toStringTrim(line))
    .filter(Boolean);

  if (!lines.length) return false;

  const allowed = new Set<string>(['approved', 'approve', 'lgtm']);
  const cfg = normalizeApprovalCommandToken(configuredKeyword);
  if (cfg) allowed.add(cfg);

  return lines.some((line) => {
    const normalized = normalizeApprovalCommandToken(line);
    return Boolean(normalized) && allowed.has(normalized);
  });
}

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
  let approverPoolUsernames: string[] = [];

  if (isPlainObject(wf)) {
    const rawApprovers = (wf as Record<string, unknown>)['approvers'];
    if (Array.isArray(rawApprovers)) approverUsernames = rawApprovers.map((x) => toStringTrim(x)).filter(Boolean);

    const rawApproversPool = (wf as Record<string, unknown>)['approversPool'];
    if (Array.isArray(rawApproversPool)) {
      approverPoolUsernames = rawApproversPool.map((x) => toStringTrim(x)).filter(Boolean);
    }
  }

  return {
    globalLabels: globalLabels.map((x) => x.trim()).filter(Boolean),
    reviewRequestedLabels: reviewRequestedLabels.map((x) => x.trim()).filter(Boolean),
    labelOnApproved: labelOnApproved ? String(labelOnApproved).trim() : null,
    labelAutoMergeCandidate: labelAutoMergeCandidate ? String(labelAutoMergeCandidate).trim() : null,
    approverUsernames: uniqLogins(approverUsernames.map((x) => x.trim()).filter(Boolean)),
    approverPoolUsernames: uniqLogins(approverPoolUsernames.map((x) => x.trim()).filter(Boolean)),
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

function resolveApproverRoutingForRequestType(
  context: BotContext<RequestEvents>,
  requestType: string | undefined | null,
  fallbackApprovers: string[],
  fallbackApproversPool: string[]
): {
  approvalUsernames: string[];
  autoAssigneePoolUsernames: string[];
} {
  const fallbackApprovalUsernames = uniqLogins([...(fallbackApprovers || []), ...(fallbackApproversPool || [])]);
  const fallbackPoolUsernames = uniqLogins(fallbackApproversPool || []);

  const rt = toStringTrim(requestType);
  if (!rt) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = cfg?.requests;

  if (!reqs || typeof reqs !== 'object') {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

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

  if (!entry) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const hasOwnApprovers = Array.isArray(entry['approvers']);
  const hasOwnApproversPool = Array.isArray(entry['approversPool']);

  if (!hasOwnApprovers && !hasOwnApproversPool) {
    return {
      approvalUsernames: fallbackApprovalUsernames,
      autoAssigneePoolUsernames: fallbackPoolUsernames,
    };
  }

  const ownApprovers = hasOwnApprovers
    ? (entry['approvers'] as unknown[]).map((x) => toStringTrim(x)).filter(Boolean)
    : [];

  const ownApproversPool = hasOwnApproversPool
    ? (entry['approversPool'] as unknown[]).map((x) => toStringTrim(x)).filter(Boolean)
    : [];

  return {
    approvalUsernames: uniqLogins([...ownApprovers, ...ownApproversPool]),
    autoAssigneePoolUsernames: uniqLogins(ownApproversPool),
  };
}

function resolveApproversForRequestType(
  context: BotContext<RequestEvents>,
  requestType: string | undefined | null,
  fallbackApprovers: string[],
  fallbackApproversPool: string[] = []
): string[] {
  return resolveApproverRoutingForRequestType(context, requestType, fallbackApprovers, fallbackApproversPool)
    .approvalUsernames;
}

function pickAutoAssigneeFromPool(issue: IssueLike, approversPool: string[]): string[] {
  const users = uniqLogins(approversPool || []).sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  if (!users.length) return [];

  const issueNumber = typeof issue?.number === 'number' && Number.isFinite(issue.number) ? issue.number : 1;
  const idx = (Math.max(issueNumber, 1) - 1) % users.length;

  return [users[idx]];
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

type CollapseBotCommentsByPrefixFn = (
  context: BotContext<RequestEvents>,
  params: IssueParams,
  options: CollapseBotCommentsByPrefixOptions
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

type ApprovalDecision = {
  status?: 'approved' | 'rejected' | 'unknown';
  path?: string;
  reason?: string;
  comment?: string;
  message?: string;
  approvers?: string[];
  errors?: {
    field?: string;
    message?: string;
  }[];
};

type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type RunApprovalHookFn = (
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  }
) => Promise<ApprovalDecision | boolean>;

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
) => Promise<boolean | void>;

const setStateLabel = setStateLabelRaw as unknown as SetStateLabelFn;
const ensureAssigneesOnce = ensureAssigneesOnceRaw as unknown as EnsureAssigneesOnceFn;
const postOnce = postOnceRaw as unknown as PostOnceFn;
const collapseBotCommentsByPrefix = collapseBotCommentsByPrefixRaw as unknown as CollapseBotCommentsByPrefixFn;
const loadTemplate = loadTemplateRaw as unknown as LoadTemplateFn;
const parseForm = parseFormRaw as unknown as ParseFormFn;
const validateRequestIssue = validateRequestIssueRaw as unknown as ValidateRequestIssueFn;
const runApprovalHook = runApprovalHookRaw as unknown as RunApprovalHookFn;
const calcSnapshotHash = calcSnapshotHashRaw as unknown as CalcSnapshotHashFn;
const extractHashFromPrBody = extractHashFromPRBodyRaw as unknown as ExtractHashFromPrBodyFn;
const findOpenIssuePrs = findOpenIssuePRsRaw as unknown as FindOpenIssuePrsFn;
const createRequestPr = createRequestPRRaw as unknown as CreateRequestPrFn;
const tryMergeIfGreen = tryMergeIfGreenRaw as unknown as TryMergeIfGreenFn;

function readCheckRunFromPayload(payload: unknown): CheckRunLike | null {
  if (!isPlainObject(payload)) return null;

  const run = payload['check_run'];
  if (!isPlainObject(run)) return null;

  return run as unknown as CheckRunLike;
}

function readCheckRunPrNumbers(run: CheckRunLike | null): number[] {
  const prs = Array.isArray(run?.pull_requests) ? run.pull_requests : [];
  const out: number[] = [];

  for (const pr of prs) {
    const n = pr?.number;
    if (typeof n === 'number' && Number.isFinite(n)) out.push(n);
  }

  return Array.from(new Set(out));
}

function extractResourceNameFromForm(formData: FormData, template: TemplateLike): string {
  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isProduct = rt === 'product';

  const val = isProduct
    ? (formData['product-id'] ?? formData['productId'] ?? formData['identifier'] ?? formData['id'] ?? '')
    : (formData['identifier'] ??
      formData['namespace'] ??
      formData['id'] ??
      formData['name'] ??
      formData['vendor'] ??
      '');

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

const ENSURE_LABELS_INFLIGHT = new Map<string, Promise<void>>();
const ON_APPROVAL_UNKNOWN_POST_INFLIGHT = new Map<string, Promise<void>>();

function isAutoMergeEvaluationRecentlyCompleted(key: string): boolean {
  const until = AUTO_MERGE_EVALUATION_RECENT_UNTIL.get(key);

  if (!until) return false;

  if (until <= Date.now()) {
    AUTO_MERGE_EVALUATION_RECENT_UNTIL.delete(key);
    return false;
  }

  return true;
}

function markAutoMergeEvaluationRecentlyCompleted(key: string): void {
  AUTO_MERGE_EVALUATION_RECENT_UNTIL.set(key, Date.now() + AUTO_MERGE_EVALUATION_RECENT_TTL_MS);
}

function issueScopedKey(params: IssueParams, suffix: string): string {
  return `${params.owner}/${params.repo}#${params.issue_number}:${suffix}`.toLowerCase();
}

async function ensureLabelsPresentOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  labels: string[]
): Promise<void> {
  const targetLabels = Array.from(new Set((labels || []).map(toStringTrim).filter(Boolean)));
  if (!targetLabels.length) return;

  const key = issueScopedKey(params, `labels:${targetLabels.map(normalizeKey).sort().join('|')}`);
  const existing = ENSURE_LABELS_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = (async (): Promise<void> => {
    let currentLabels: string[] = [];

    try {
      currentLabels = await fetchIssueLabels(context, params);
    } catch {
      currentLabels = [];
    }

    const currentKeys = new Set(currentLabels.map(normalizeKey).filter(Boolean));
    const missing = targetLabels.filter((label) => {
      const key = normalizeKey(label);
      return key && !currentKeys.has(key);
    });

    if (!missing.length) return;

    try {
      await context.octokit.issues.addLabels({
        ...params,
        labels: missing,
      });
    } catch (error: unknown) {
      const status = getHttpStatus(error);
      if (status !== 404) {
        log(
          context,
          'warn',
          {
            err: getErrorMessage(error),
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

async function postApprovalUnknownOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  decision: ApprovalDecision
): Promise<void> {
  const key = issueScopedKey(params, 'on-approval-unknown');
  const existing = ON_APPROVAL_UNKNOWN_POST_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = (async (): Promise<void> => {
    await postOnce(context, params, buildApprovalUnknownBody(decision), {
      minimizeTag: 'nsreq:on-approval:unknown',
    });
  })().finally(() => {
    ON_APPROVAL_UNKNOWN_POST_INFLIGHT.delete(key);
  });

  ON_APPROVAL_UNKNOWN_POST_INFLIGHT.set(key, pending);
  await pending;
}

async function ensureAssigneesPresent(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  assignees: string[]
): Promise<void> {
  const targetAssignees = uniqLogins((assignees || []).map((x) => toStringTrim(x)).filter(Boolean));
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
  } catch (e: unknown) {
    log(
      context,
      'warn',
      {
        err: e instanceof Error ? e.message : String(e),
        issueNumber: params.issue_number,
        assignees: targetAssignees,
      },
      'failed to ensure assignees'
    );
  }
}

function buildReviewHandoverBody(
  context: BotContext<RequestEvents>,
  snapshotHash?: string,
  options: { target?: 'issue' | 'pull_request' } = {}
): string {
  const docsLinks = getDocLinksFromConfig(context.resourceBotConfig ?? DEFAULT_CONFIG);
  const docsSection = docsLinks ? `\n\n${docsLinks.trim()}` : '';
  const snapshotMarker = snapshotHash ? `\n\n<!-- nsreq:snapshot:${snapshotHash} -->` : '';

  const target = options.target || 'issue';
  const instruction =
    target === 'pull_request'
      ? 'Once reviewed, please comment `Approved` to approve this PR for merge.'
      : 'Once reviewed, please comment `Approved` to create an automatic Pull Request.';

  return `### ✅ No issues detected

### ➡️ Routing to an approver for review

---

${instruction}${docsSection}${snapshotMarker}

<!-- nsreq:handover -->`;
}

async function handoverToCpa(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  _nsType: string,
  _namespace: string,
  _links: string[] = [],
  options: {
    snapshotHash?: string;
    requestType?: string;
    extraApprovers?: string[];
    manualApproversOverride?: string[];
  } = {}
): Promise<void> {
  const eff = resolveEffectiveConstants(context);

  await setStateLabel(context, params, issue, 'review');

  const approverRouting = resolveApproverRoutingForRequestType(
    context,
    options.requestType,
    eff.approverUsernames,
    eff.approverPoolUsernames
  );

  const assigneesForType = approverRouting.autoAssigneePoolUsernames.length
    ? pickAutoAssigneeFromPool(issue, approverRouting.autoAssigneePoolUsernames)
    : approverRouting.approvalUsernames;

  const manualApproversOverride = uniqLogins(
    (options.manualApproversOverride || []).map((value) => toStringTrim(value)).filter(Boolean)
  );

  const mergedAssignees = manualApproversOverride.length
    ? manualApproversOverride
    : uniqLogins([...(assigneesForType || []), ...(options.extraApprovers || []).filter(Boolean)]);

  await ensureAssigneesOnce(context, params, issue, mergedAssignees);
  await ensureAssigneesPresent(context, params, mergedAssignees);

  const labelsToAdd = [...(eff.globalLabels || []), ...(eff.reviewRequestedLabels || [])].filter(Boolean);

  await ensureLabelsPresentOnce(context, params, labelsToAdd);

  if (eff.labelOnApproved) {
    try {
      await context.octokit.issues.removeLabel({
        ...params,
        name: eff.labelOnApproved,
      });
    } catch {
      /* empty */
    }
  }

  const handoverMsg = buildReviewHandoverBody(context, options.snapshotHash);

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

  let labels = toLabelNames(issue.labels);

  try {
    labels = await fetchIssueLabels(context, params);
  } catch {
    // keep payload labels as fallback
  }

  return labels.some((l) => {
    const k = normalizeKey(l);
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

function buildApprovalDecisionJson(decision: ApprovalDecision): string {
  const payload: Record<string, unknown> = {};
  if (decision.status) payload.status = decision.status;
  if (decision.path) payload.path = decision.path;
  if (decision.reason) payload.reason = decision.reason;
  if (decision.comment) payload.comment = decision.comment;
  if (decision.message) payload.message = decision.message;
  if (Array.isArray(decision.approvers) && decision.approvers.length) payload.approvers = decision.approvers;
  if (Array.isArray(decision.errors) && decision.errors.length) payload.errors = decision.errors;
  return JSON.stringify(payload, null, 2);
}

function normalizeApprovalDecision(decision: ApprovalDecision | boolean): ApprovalDecision {
  if (decision === true) return { status: 'approved' };
  if (decision === false) return {};
  if (!decision) return {};

  const normalized = decision || {};
  const approvers = uniqLogins(
    Array.isArray(normalized.approvers) ? normalized.approvers.map((x) => toStringTrim(x)).filter(Boolean) : []
  );
  const { approvers: _approvers, ...normalizedWithoutApprovers } = normalized;

  return {
    ...normalizedWithoutApprovers,
    ...(approvers.length ? { approvers } : {}),
  };
}

function getUnknownManualApprovers(decision: ApprovalDecision): string[] {
  const normalized = normalizeApprovalDecision(decision);

  if (normalized.status !== 'unknown') return [];

  return uniqLogins((normalized.approvers || []).map((value) => toStringTrim(value)).filter(Boolean));
}

function isManualApprovalRequiredText(value: unknown): boolean {
  return /\bmanual approval required\b/i.test(toStringTrim(value));
}

function getVisibleApprovalText(decision: ApprovalDecision): string {
  const comment = toStringTrim(decision.comment);
  if (comment && !isManualApprovalRequiredText(comment)) return comment;

  const message = toStringTrim(decision.message);
  if (message && !isManualApprovalRequiredText(message)) return message;

  const reason = toStringTrim(decision.reason);
  if (reason && !isManualApprovalRequiredText(reason)) return reason;

  return '';
}

function buildAutoApprovalReviewBody(decision: ApprovalDecision, headSha: string): string {
  const visible = getVisibleApprovalText(decision);
  const marker = buildAutoApprovalReviewMarker(headSha);

  return visible ? `${visible}\n\n${marker}` : marker;
}

function isApprovalDecisionAuthorizedByHookApprovers(
  decision: ApprovalDecision,
  requesterId: string | undefined | null
): boolean {
  const requester = normalizeLogin(requesterId).toLowerCase();
  if (!requester) return false;

  const approvers = uniqLogins((decision.approvers || []).map((value) => toStringTrim(value)).filter(Boolean));
  return approvers.some((approver) => normalizeLogin(approver).toLowerCase() === requester);
}

function promoteUnknownApprovalDecisionForDirectPrRequester(
  decision: ApprovalDecision,
  requesterId: string | undefined | null
): ApprovalDecision {
  const normalized = normalizeApprovalDecision(decision);

  if (normalized.status !== 'unknown') return normalized;
  if (!isApprovalDecisionAuthorizedByHookApprovers(normalized, requesterId)) return normalized;

  return {
    ...normalized,
    status: 'approved',
    comment: getVisibleApprovalText(normalized),
    message: '',
    reason: '',
  };
}

function buildApprovalUnknownBody(decision: ApprovalDecision): string {
  const lead = toStringTrim(decision.message) || toStringTrim(decision.comment) || toStringTrim(decision.reason);
  const leadBlock = lead ? `${lead}\n\n` : '';

  return `${leadBlock}<details>
<summary>Decision details</summary>

\`\`\`json
${buildApprovalDecisionJson({ status: 'unknown', ...decision })}
\`\`\`
</details>

Continuing with the standard review flow.`;
}

function buildApprovalRejectedBody(decision: ApprovalDecision): string {
  const issues = normalizeApprovalHookErrorsForComment(decision);
  const groupedIssues = buildApprovalHookIssueList(issues);
  const detectedIssuesBlock = groupedIssues ? buildDetectedIssuesBody(groupedIssues, issues) : '';

  const lead = toStringTrim(decision.message) || toStringTrim(decision.comment) || toStringTrim(decision.reason);
  const leadBlock = lead && !detectedIssuesBlock ? `${lead}\n\n` : '';
  const issuesBlock = detectedIssuesBlock ? `${detectedIssuesBlock}\n\n` : '';

  return `## onApproval rejected this request

${leadBlock}${issuesBlock}Closing this request automatically.`;
}

async function applyApprovedRequestState(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  eff: EffectiveConstants
): Promise<void> {
  try {
    if (eff.labelOnApproved) {
      await context.octokit.issues.addLabels({ ...params, labels: [eff.labelOnApproved] });
    }
  } catch {
    // ignore
  }

  await removeReviewPendingLabelsAfterApproval(context, params, eff);

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
}

function prAsIssueLike(pr: PullRequestLike): IssueLike {
  return {
    number: pr.number,
    title: pr.title,
    body: pr.body,
    state: pr.state,
    user: pr.user,
    labels: [],
  };
}

function resolveReviewAssigneesForRequestTypes(
  context: BotContext<RequestEvents>,
  reviewTarget: IssueLike,
  requestTypes: string[]
): string[] {
  const eff = resolveEffectiveConstants(context);
  const types = Array.from(new Set((requestTypes || []).map(toStringTrim).filter(Boolean)));

  const pickForRouting = (routing: { approvalUsernames: string[]; autoAssigneePoolUsernames: string[] }): string[] =>
    routing.autoAssigneePoolUsernames.length
      ? pickAutoAssigneeFromPool(reviewTarget, routing.autoAssigneePoolUsernames)
      : routing.approvalUsernames;

  if (!types.length) {
    const fallbackRouting = resolveApproverRoutingForRequestType(
      context,
      '',
      eff.approverUsernames,
      eff.approverPoolUsernames
    );

    return uniqLogins(pickForRouting(fallbackRouting));
  }

  const assignees: string[] = [];

  for (const requestType of types) {
    const routing = resolveApproverRoutingForRequestType(
      context,
      requestType,
      eff.approverUsernames,
      eff.approverPoolUsernames
    );

    assignees.push(...pickForRouting(routing));
  }

  return uniqLogins(assignees);
}

function resolveAllowedApproversForRequestTypes(context: BotContext<RequestEvents>, requestTypes: string[]): string[] {
  const eff = resolveEffectiveConstants(context);
  const types = Array.from(new Set((requestTypes || []).map(toStringTrim).filter(Boolean)));

  if (!types.length) {
    return resolveApproverRoutingForRequestType(context, '', eff.approverUsernames, eff.approverPoolUsernames)
      .approvalUsernames;
  }

  return uniqLogins(
    types.flatMap(
      (requestType) =>
        resolveApproverRoutingForRequestType(context, requestType, eff.approverUsernames, eff.approverPoolUsernames)
          .approvalUsernames
    )
  );
}

function calcStandaloneDirectPrSnapshotHash(pr: PullRequestLike, changedFiles: string[]): string {
  const payload = {
    headSha: toStringTrim(pr.head?.sha),
    files: Array.from(new Set((changedFiles || []).map(normalizeRepoPath).filter(Boolean))).sort(),
  };

  return createHash('sha1').update(JSON.stringify(payload)).digest('hex');
}

async function createAutomatedApprovalReview(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
  const reviewBody = buildAutoApprovalReviewBody(decision, headSha);
  const failureText = getVisibleApprovalText(decision) || 'The onApproval hook matched this PR.';

  try {
    await (
      context.octokit.pulls as unknown as {
        createReview: (args: {
          owner: string;
          repo: string;
          pull_number: number;
          event: 'APPROVE';
          body: string;
        }) => Promise<unknown>;
      }
    ).createReview({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: pr.number,
      event: 'APPROVE',
      body: reviewBody,
    });

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
      },
      'automated PR approval review created'
    );

    return true;
  } catch (e: unknown) {
    const errObj = isPlainObject(e) ? e : {};
    const status = typeof errObj['status'] === 'number' ? errObj['status'] : undefined;

    const response = isPlainObject(errObj['response']) ? errObj['response'] : {};
    const responseData = response['data'];

    const message = e instanceof Error ? e.message : String(e);

    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        status,
        message,
        responseData,
      },
      'failed to create automated PR approval review'
    );

    await postOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      `## onApproval matched, but automatic PR approval failed

${failureText}

Approval API error: ${message}${status ? ` (HTTP ${status})` : ''}

The PR could not be approved automatically, so merge remains blocked until a review is added manually.`,
      { minimizeTag: 'nsreq:on-approval:approve-failed' }
    );

    return false;
  }
}

async function resolvePullRequestRequestAuthorId(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<string> {
  let page = 1;

  const blockedServiceUsers = new Set<string>([
    'web-flow-serviceuser',
    'global-registry-bot',
    'global-registry-bot[bot]',
    'my-registry-bot',
    'my-registry-bot[bot]',
    'github-actions[bot]',
    'github-actions',
  ]);

  const isUsableRequesterLogin = (value: unknown): string => {
    const login = normalizeLogin(value);
    if (!login) return '';
    if (blockedServiceUsers.has(login.toLowerCase())) return '';
    return login;
  };

  let lastAuthorLogin = '';
  let lastCommitterLogin = '';
  let firstAuthorLogin = '';
  let firstCommitterLogin = '';

  try {
    while (true) {
      const res = await (
        context.octokit.pulls as unknown as {
          listCommits: (args: {
            owner: string;
            repo: string;
            pull_number: number;
            per_page?: number;
            page?: number;
          }) => Promise<{ data?: PullRequestCommitLike[] }>;
        }
      ).listCommits({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        per_page: 100,
        page,
      });

      const commits = Array.isArray(res?.data) ? res.data : [];
      if (!commits.length) break;

      for (const commit of commits) {
        const authorLogin = isUsableRequesterLogin(commit?.author?.login);
        const committerLogin = isUsableRequesterLogin(commit?.committer?.login);

        if (!firstAuthorLogin && authorLogin) firstAuthorLogin = authorLogin;
        if (!firstCommitterLogin && committerLogin) firstCommitterLogin = committerLogin;

        if (authorLogin) lastAuthorLogin = authorLogin;
        if (committerLogin) lastCommitterLogin = committerLogin;
      }

      if (commits.length < 100) break;
      page += 1;
      if (page > 20) break;
    }
  } catch {
    // Fall through to PR author fallback below
  }

  return (
    lastAuthorLogin ||
    lastCommitterLogin ||
    firstAuthorLogin ||
    firstCommitterLogin ||
    isUsableRequesterLogin(pr.user?.login) ||
    ''
  );
}

async function addApprovedLabelToPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  options: { skipStateCleanup?: boolean } = {}
): Promise<void> {
  const eff = resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  if (!approvedLabel) return;

  const params: IssueParams = {
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

  // Standalone cross-repo direct PRs must stay PR-only here.
  // Reading the PR as an issue would break the no-linked-issue guarantee.
  if (options.skipStateCleanup) return;

  await removeReviewPendingLabelsAfterApproval(context, params, eff);

  try {
    const labelsAfter = await fetchIssueLabels(context, params);

    if (labelsMatching(labelsAfter, approvedLabel).length) {
      await removeProgressStatusLabels(context, params, labelsAfter);
      await removeRejectedStatusLabel(context, params, labelsAfter);
    }
  } catch {
    // best effort cleanup only
  }
}

const AUTO_APPROVAL_REVIEW_MARKER_PREFIX = 'nsreq:auto-approval:';

function buildAutoApprovalReviewMarker(headSha: string): string {
  return `<!-- ${AUTO_APPROVAL_REVIEW_MARKER_PREFIX}${toStringTrim(headSha)} -->`;
}

async function runEnsureAutomatedApprovalReviewForCurrentHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  headSha: string,
  options: AutomatedApprovalReviewOptions = {}
): Promise<boolean> {
  if (hasAutoApprovedPrHead(repoInfo, pr.number, headSha)) {
    return true;
  }

  if (await hasAutoApprovalReviewForHead(context, repoInfo, pr.number, headSha)) {
    markAutoApprovedPrHead(repoInfo, pr.number, headSha);
    return true;
  }

  const approved = await createAutomatedApprovalReview(context, repoInfo, pr, decision);
  if (!approved) return false;

  markAutoApprovedPrHead(repoInfo, pr.number, headSha);

  await addApprovedLabelToPr(context, repoInfo, pr.number, {
    skipStateCleanup: options.skipApprovedLabelStateCleanup === true,
  });

  return true;
}

async function ensureAutomatedApprovalReviewForCurrentHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  options: AutomatedApprovalReviewOptions = {}
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = autoApprovedPrHeadKey(repoInfo, pr.number, headSha);

  const existing = AUTO_APPROVAL_REVIEW_INFLIGHT.get(key);
  if (existing) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
      },
      'automated PR approval review deduped: already in flight'
    );

    return await existing;
  }

  const pending = runEnsureAutomatedApprovalReviewForCurrentHead(
    context,
    repoInfo,
    pr,
    decision,
    headSha,
    options
  ).finally(() => {
    AUTO_APPROVAL_REVIEW_INFLIGHT.delete(key);
  });

  AUTO_APPROVAL_REVIEW_INFLIGHT.set(key, pending);
  return await pending;
}

async function listPullRequestReviews(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<PullRequestReviewLike[]> {
  const out: PullRequestReviewLike[] = [];
  let page = 1;

  while (true) {
    const res = await (
      context.octokit.pulls as unknown as {
        listReviews: (args: {
          owner: string;
          repo: string;
          pull_number: number;
          per_page?: number;
          page?: number;
        }) => Promise<{ data?: PullRequestReviewLike[] }>;
      }
    ).listReviews({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: prNumber,
      per_page: 100,
      page,
    });

    const reviews = Array.isArray(res?.data) ? res.data : [];
    if (!reviews.length) break;

    out.push(...reviews);

    if (reviews.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return out;
}

const ACTIONABLE_REVIEW_STATES = new Set<string>(['APPROVED', 'CHANGES_REQUESTED', 'DISMISSED']);

function sortPullRequestReviewsChronologically(reviews: PullRequestReviewLike[]): PullRequestReviewLike[] {
  return reviews.slice().sort((a, b) => {
    const at = Date.parse(toStringTrim(a.submitted_at));
    const bt = Date.parse(toStringTrim(b.submitted_at));

    if (Number.isFinite(at) && Number.isFinite(bt) && at !== bt) return at - bt;

    const aid = typeof a.id === 'number' ? a.id : 0;
    const bid = typeof b.id === 'number' ? b.id : 0;
    return aid - bid;
  });
}

function getLatestActionableReviewStates(reviews: PullRequestReviewLike[]): Map<string, string> {
  const latestByReviewer = new Map<string, string>();

  for (const review of sortPullRequestReviewsChronologically(reviews)) {
    const reviewer = normalizeLogin(review?.user?.login).toLowerCase();
    const state = toStringTrim(review?.state).toUpperCase();

    if (!reviewer || !ACTIONABLE_REVIEW_STATES.has(state)) continue;

    latestByReviewer.set(reviewer, state);
  }

  return latestByReviewer;
}

async function hasApprovedLabelOnPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<boolean> {
  const eff = resolveEffectiveConstants(context);
  const approvedLabel = toStringTrim(eff.labelOnApproved) || 'Approved';
  if (!approvedLabel) return false;

  try {
    const labels = await fetchIssueLabels(context, {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      issue_number: prNumber,
    });

    return labelsMatching(labels, approvedLabel).length > 0;
  } catch {
    return false;
  }
}

async function isPullRequestApprovedForBranchMaintenance(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: { allowLabelFallback?: boolean } = {}
): Promise<boolean> {
  let reviews: PullRequestReviewLike[];
  try {
    reviews = await listPullRequestReviews(context, repoInfo, pr.number);
  } catch {
    reviews = [];
  }

  const latestStates = getLatestActionableReviewStates(reviews);
  const latestStateValues = new Set(latestStates.values());

  if (latestStateValues.has('CHANGES_REQUESTED')) {
    return false;
  }

  if (isSnapshotManagedRequestPr(pr)) return true;

  const headSha = toStringTrim(pr.head?.sha);
  const marker = headSha ? buildAutoApprovalReviewMarker(headSha) : null;

  if (
    marker &&
    reviews.some(
      (review) =>
        toStringTrim(review?.state).toUpperCase() === 'APPROVED' && toStringTrim(review?.body).includes(marker)
    )
  ) {
    return true;
  }

  if (latestStateValues.has('APPROVED')) {
    return true;
  }

  if (headSha && AUTO_APPROVED_PR_HEADS.has(autoApprovedPrHeadKey(repoInfo, pr.number, headSha))) {
    return true;
  }

  if (options.allowLabelFallback !== false && (await hasApprovedLabelOnPr(context, repoInfo, pr.number))) {
    return true;
  }

  return false;
}

async function hasAutoApprovalReviewForHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  headSha: string
): Promise<boolean> {
  const marker = buildAutoApprovalReviewMarker(headSha);

  try {
    const reviews = await listPullRequestReviews(context, repoInfo, prNumber);

    return reviews.some(
      (review) =>
        toStringTrim(review?.state).toUpperCase() === 'APPROVED' && toStringTrim(review?.body).includes(marker)
    );
  } catch {
    return false;
  }
}

function isGreenCheckConclusion(conclusion: string): boolean {
  const value = toStringTrim(conclusion).toLowerCase();
  return value === 'success' || value === 'neutral' || value === 'skipped';
}

function isBlockingCheckConclusion(conclusion: string): boolean {
  const value = toStringTrim(conclusion).toLowerCase();
  return (
    value === 'failure' ||
    value === 'cancelled' ||
    value === 'timed_out' ||
    value === 'action_required' ||
    value === 'startup_failure' ||
    value === 'stale'
  );
}

function summarizeHeadGreenRun(run: RefCheckRunLike): HeadGreenRunSummary {
  const id = typeof run?.id === 'number' && Number.isFinite(run.id) ? run.id : undefined;

  return {
    ...(id !== undefined ? { id } : {}),
    name: toStringTrim(run?.name) || '__unnamed__',
    status: toStringTrim(run?.status).toLowerCase(),
    conclusion: toStringTrim(run?.conclusion).toLowerCase(),
  };
}

async function evaluateHeadGreenForApprovalReevaluation(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  headSha: string
): Promise<HeadGreenEvaluation> {
  const ref = toStringTrim(headSha);
  if (!ref) {
    return {
      green: false,
      reason: 'missing-head-sha',
      latestRuns: [],
      blockingRuns: [],
    };
  }

  try {
    const all: RefCheckRunLike[] = [];
    let page = 1;

    while (true) {
      const res = await (
        context.octokit.checks as unknown as {
          listForRef: (args: {
            owner: string;
            repo: string;
            ref: string;
            per_page?: number;
            page?: number;
          }) => Promise<{ data?: unknown }>;
        }
      ).listForRef({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        ref,
        per_page: 100,
        page,
      });

      const data = (res as { data?: unknown }).data;
      const runs =
        isPlainObject(data) && Array.isArray(data['check_runs'])
          ? (data['check_runs'] as unknown as RefCheckRunLike[])
          : [];

      all.push(...runs);

      if (runs.length < 100) break;
      page += 1;
      if (page > 20) break;
    }

    const latestByName = new Map<string, RefCheckRunLike>();

    for (const run of all) {
      const name = toStringTrim(run?.name) || '__unnamed__';
      const currentId = typeof run?.id === 'number' ? run.id : -1;
      const prev = latestByName.get(name);
      const prevId = typeof prev?.id === 'number' ? prev.id : -1;

      if (!prev || currentId > prevId) {
        latestByName.set(name, run);
      }
    }

    if (latestByName.size > 0) {
      const latestRuns = Array.from(latestByName.values()).map(summarizeHeadGreenRun);

      const incompleteRuns = latestRuns.filter((run) => run.status !== 'completed');
      if (incompleteRuns.length) {
        return {
          green: false,
          reason: 'check-runs-not-completed',
          latestRuns,
          blockingRuns: incompleteRuns,
        };
      }

      const blockingRuns = latestRuns.filter(
        (run) => isBlockingCheckConclusion(run.conclusion) || !isGreenCheckConclusion(run.conclusion)
      );

      if (blockingRuns.length) {
        return {
          green: false,
          reason: 'check-runs-blocking-or-not-green',
          latestRuns,
          blockingRuns,
        };
      }

      const sawSuccess = latestRuns.some((run) => run.conclusion === 'success');
      if (!sawSuccess) {
        return {
          green: false,
          reason: 'no-success-check-run',
          latestRuns,
          blockingRuns: [],
        };
      }

      return {
        green: true,
        reason: 'check-runs-green',
        latestRuns,
        blockingRuns: [],
      };
    }
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: ref,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'head-green:check-runs-fetch-failed'
    );
  }

  try {
    const res = await (
      context.octokit.repos as unknown as {
        getCombinedStatusForRef: (args: {
          owner: string;
          repo: string;
          ref: string;
        }) => Promise<{ data?: { state?: string | null } }>;
      }
    ).getCombinedStatusForRef({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      ref,
    });

    const statusState = toStringTrim(res?.data?.state).toLowerCase();

    return {
      green: statusState === 'success',
      reason: statusState === 'success' ? 'combined-status-success' : 'combined-status-not-success',
      latestRuns: [],
      blockingRuns: [],
      statusState,
    };
  } catch (_error: unknown) {
    return {
      green: false,
      reason: 'combined-status-fetch-failed',
      latestRuns: [],
      blockingRuns: [],
    };
  }
}

function mergeInflightKey(repoInfo: RepoInfo, pr: PullRequestLike): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${pr.number}:${toStringTrim(pr.head?.sha)}`;
}

function autoApprovedPrHeadKey(repoInfo: RepoInfo, prNumber: number, headSha: string): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${prNumber}:${toStringTrim(headSha)}`.toLowerCase();
}

function markAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): void {
  const key = autoApprovedPrHeadKey(repoInfo, prNumber, headSha);
  if (key) AUTO_APPROVED_PR_HEADS.add(key);
}

function hasAutoApprovedPrHead(repoInfo: RepoInfo, prNumber: number, headSha: string): boolean {
  const key = autoApprovedPrHeadKey(repoInfo, prNumber, headSha);
  return Boolean(key && AUTO_APPROVED_PR_HEADS.has(key));
}

class CooldownUntilMap extends Map<string, number> {
  public override get(key: string): number | undefined {
    const until = super.get(key);
    if (until !== undefined && until <= Date.now()) {
      super.delete(key);
      return undefined;
    }
    return until;
  }

  public override has(key: string): boolean {
    return this.get(key) !== undefined;
  }
}

const UPDATE_BRANCH_INFLIGHT = new Map<string, Promise<boolean>>();
const UPDATE_BRANCH_COOLDOWN_UNTIL = new CooldownUntilMap();

const DEFAULT_BRANCH_UPDATE_RETRY_DELAY_MS = 5000;
const UPDATE_BRANCH_RETRY_DELAY_MS = 2000;
const UPDATE_BRANCH_COOLDOWN_MS = 15000;

type SequentialRegistryPrResult = {
  updated: boolean;
  processed: boolean;
  blockedByActive: boolean;
};

type SequentialRegistryPrCandidate = {
  pr: PullRequestLike;
  freshPr: PullRequestLike;
  changedRegistryFiles: string[];
  mustUpdate: boolean;
  approvedForUpdate: boolean;
};

type SequentialRegistryPrActive = {
  prNumber: number;
  startedHeadSha: string;
  startedAt: number;
  expiresAt: number;
  reason: string;
};

const SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT = new Map<string, Promise<SequentialRegistryPrResult>>();
const SEQUENTIAL_REGISTRY_PR_ACTIVE = new Map<string, SequentialRegistryPrActive>();
const SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS = new Map<string, number>();

const SEQUENTIAL_REGISTRY_PR_ACTIVE_TTL_MS = 30 * 60 * 1000;
const SEQUENTIAL_REGISTRY_PR_SKIP_TTL_MS = 6 * 60 * 60 * 1000;

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function updateBranchInflightKey(repoInfo: RepoInfo, pr: PullRequestLike): string {
  return `${repoInfo.owner}/${repoInfo.repo}#${pr.number}`;
}

function isUpdateBranchCooldownActive(key: string): boolean {
  const until = UPDATE_BRANCH_COOLDOWN_UNTIL.get(key);
  // eslint-disable-next-line eqeqeq
  if (until == null) return false;

  if (until <= Date.now()) {
    UPDATE_BRANCH_COOLDOWN_UNTIL.delete(key);
    return false;
  }

  return true;
}

function markUpdateBranchCooldown(key: string): void {
  UPDATE_BRANCH_COOLDOWN_UNTIL.set(key, Date.now() + UPDATE_BRANCH_COOLDOWN_MS);
}

function isBenignUpdateBranchFailure(error: unknown): boolean {
  const status = getHttpStatus(error);
  const msg = getErrorMessage(error).toLowerCase();

  if (status !== 422) return false;

  return (
    msg.includes('expected_head_sha') ||
    msg.includes('head sha') ||
    msg.includes('head branch was modified') ||
    msg.includes('not behind') ||
    msg.includes('up to date') ||
    msg.includes('up-to-date') ||
    msg.includes('already up') ||
    msg.includes('already up-to-date') ||
    msg.includes('already up to date')
  );
}

function isManualUpdateBranchFailure(error: unknown): boolean {
  const status = getHttpStatus(error);
  const msg = getErrorMessage(error).toLowerCase();

  return (
    status === 403 ||
    status === 404 ||
    msg.includes('conflict') ||
    msg.includes('merge conflict') ||
    msg.includes('protected branch') ||
    msg.includes('permission') ||
    msg.includes('forbidden')
  );
}

async function callPullRequestBranchUpdate(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  expectedHeadSha?: string
): Promise<void> {
  const args: {
    owner: string;
    repo: string;
    pull_number: number;
    expected_head_sha?: string;
  } = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    pull_number: prNumber,
  };

  const normalizedExpectedHeadSha = toStringTrim(expectedHeadSha);
  if (normalizedExpectedHeadSha) {
    args.expected_head_sha = normalizedExpectedHeadSha;
  }

  await (
    context.octokit.pulls as unknown as {
      updateBranch: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        expected_head_sha?: string;
      }) => Promise<unknown>;
    }
  ).updateBranch(args);
}

async function requestPullRequestBranchUpdate(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = updateBranchInflightKey(repoInfo, pr);

  if (isUpdateBranchCooldownActive(key)) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha,
        reason,
      },
      'pull-request branch update skipped: cooldown active'
    );

    return false;
  }

  const existing = UPDATE_BRANCH_INFLIGHT.get(key);
  if (existing) return await existing;

  const pending = (async (): Promise<boolean> => {
    try {
      await callPullRequestBranchUpdate(context, repoInfo, pr.number, headSha);

      markUpdateBranchCooldown(key);

      log(
        context,
        'info',
        {
          prNumber: pr.number,
          headSha,
          reason,
        },
        'pull-request branch update requested'
      );

      return true;
    } catch (error: unknown) {
      const msg = getErrorMessage(error);
      const status = getHttpStatus(error);

      if (isBenignUpdateBranchFailure(error)) {
        const fresh = await readFreshPullRequest(context, repoInfo, pr.number);
        const freshHeadSha = toStringTrim(fresh?.head?.sha);
        const freshMergeableState = readMergeableState(fresh);
        const stillBehind = Boolean(fresh && isPullRequestBehindBase(fresh));

        if (freshHeadSha && freshHeadSha !== headSha) {
          log(
            context,
            'info',
            {
              prNumber: pr.number,
              oldHeadSha: headSha,
              freshHeadSha,
              status,
              err: msg,
              reason,
              freshMergeableState,
            },
            'pull-request branch update skipped: head already changed'
          );

          return false;
        }

        if (stillBehind) {
          await delayMs(UPDATE_BRANCH_RETRY_DELAY_MS);

          try {
            await callPullRequestBranchUpdate(context, repoInfo, pr.number);

            markUpdateBranchCooldown(key);

            log(
              context,
              'info',
              {
                prNumber: pr.number,
                headSha,
                freshHeadSha,
                reason,
              },
              'pull-request branch update requested after expected-head retry'
            );

            return true;
          } catch (retryError: unknown) {
            markUpdateBranchCooldown(key);

            log(
              context,
              'warn',
              {
                prNumber: pr.number,
                headSha,
                freshHeadSha,
                status: getHttpStatus(retryError),
                err: getErrorMessage(retryError),
                originalStatus: status,
                originalErr: msg,
                reason,
                freshMergeableState,
              },
              'pull-request branch update retry failed'
            );

            return false;
          }
        }

        log(
          context,
          'info',
          {
            prNumber: pr.number,
            oldHeadSha: headSha,
            freshHeadSha,
            status,
            err: msg,
            reason,
            freshMergeableState,
          },
          'pull-request branch update skipped after benign failure'
        );

        return false;
      }

      log(
        context,
        'warn',
        {
          prNumber: pr.number,
          headSha,
          status,
          err: msg,
          reason,
        },
        'pull-request branch update failed'
      );

      if (isManualUpdateBranchFailure(error)) {
        await postOnce(
          context,
          { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
          `## Could not update PR branch automatically

The PR is approved, but the bot could not update the branch with the latest base branch.

Reason:
\`${msg}\`

Please update the branch manually.`,
          { minimizeTag: 'nsreq:update-branch-failed' }
        );
      }

      return false;
    }
  })().finally(() => {
    UPDATE_BRANCH_INFLIGHT.delete(key);
  });

  UPDATE_BRANCH_INFLIGHT.set(key, pending);
  return await pending;
}

function shouldTryBranchUpdateAfterMergeFailure(error: unknown): boolean {
  const msg = getErrorMessage(error).toLowerCase();

  return (
    msg.includes('branch is out-of-date') ||
    msg.includes('branch is out of date') ||
    msg.includes('update branch') ||
    msg.includes('must be up to date') ||
    msg.includes('must be up-to-date') ||
    msg.includes('behind the base branch')
  );
}

function isMergeBlockedByBranchProtection(error: unknown): boolean {
  const msg = getErrorMessage(error).toLowerCase();

  return (
    msg.includes('at least 1 approving review is required') ||
    msg.includes('approving review is required') ||
    msg.includes('required status check') ||
    msg.includes('is expected') ||
    msg.includes('protected branch') ||
    msg.includes('pull request is not mergeable')
  );
}

const MERGEABILITY_POLL_ATTEMPTS = 6;
const MERGEABILITY_POLL_DELAY_MS = 1500;

function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function readFreshPullRequest(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<PullRequestLike | null> {
  try {
    const res = await (
      context.octokit.pulls as unknown as {
        get: (args: { owner: string; repo: string; pull_number: number }) => Promise<{ data?: PullRequestLike }>;
      }
    ).get({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      pull_number: prNumber,
    });

    return res.data || null;
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'failed to refresh pull request'
    );

    return null;
  }
}

function readMergeableState(pr: PullRequestLike | null | undefined): string {
  return toStringTrim(pr?.mergeable_state).toLowerCase();
}

function isPullRequestOpen(pr: PullRequestLike | null | undefined): boolean {
  return toStringTrim(pr?.state).toLowerCase() === 'open';
}

function isMergeabilityPending(pr: PullRequestLike | null | undefined): boolean {
  const state = readMergeableState(pr);

  return pr?.mergeable === null || state === 'unknown' || state === 'checking';
}

function isPullRequestBehindBase(pr: PullRequestLike | null | undefined): boolean {
  return readMergeableState(pr) === 'behind';
}

function isPullRequestDirty(pr: PullRequestLike | null | undefined): boolean {
  const state = readMergeableState(pr);

  return state === 'dirty' || state === 'conflicting';
}

async function waitForPullRequestMergeability(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<PullRequestLike> {
  let current = pr;

  for (let attempt = 1; attempt <= MERGEABILITY_POLL_ATTEMPTS; attempt += 1) {
    const fresh = await readFreshPullRequest(context, repoInfo, pr.number);
    if (fresh) current = fresh;

    const mergeable = current.mergeable;
    const mergeableState = readMergeableState(current);

    log(
      context,
      DBG ? 'debug' : 'info',
      {
        prNumber: current.number,
        attempt,
        headSha: toStringTrim(current.head?.sha),
        mergeable,
        mergeableState,
        reason,
      },
      'pull-request mergeability state'
    );

    if (!isPullRequestOpen(current)) return current;
    if (!isMergeabilityPending(current)) return current;

    await delayMs(MERGEABILITY_POLL_DELAY_MS);
  }

  return current;
}

async function tryMergeApprovedPrOrUpdateBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<void> {
  const key = mergeInflightKey(repoInfo, pr);
  const existing = MERGE_INFLIGHT.get(key);

  if (existing) {
    await existing;
    return;
  }

  const pending = runMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, reason).finally(() => {
    MERGE_INFLIGHT.delete(key);
  });

  MERGE_INFLIGHT.set(key, pending);
  await pending;
}

async function runMergeApprovedPrOrUpdateBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<void> {
  const originalHeadSha = toStringTrim(pr.head?.sha);
  const baseBranch = toStringTrim(pr.base?.ref);

  let currentPr = await waitForPullRequestMergeability(context, repoInfo, pr, `${reason}:before-merge`);

  if (!isPullRequestOpen(currentPr)) return;

  const currentHeadSha = toStringTrim(currentPr.head?.sha);

  if (originalHeadSha && currentHeadSha && originalHeadSha !== currentHeadSha) {
    log(
      context,
      'info',
      {
        prNumber: currentPr.number,
        originalHeadSha,
        currentHeadSha,
        reason,
      },
      'pull-request head changed before merge, waiting for new CI'
    );

    return;
  }

  if (isPullRequestDirty(currentPr)) {
    log(
      context,
      'warn',
      {
        prNumber: currentPr.number,
        mergeableState: readMergeableState(currentPr),
        reason,
      },
      'pull-request has merge conflicts, auto-merge skipped'
    );

    return;
  }

  if (await shouldUpdatePullRequestBranch(context, repoInfo, currentPr, baseBranch)) {
    await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
      context,
      repoInfo,
      currentPr,
      baseBranch,
      `${reason}:behind-before-merge`
    );
    return;
  }

  const hasCurrentHeadAutoApproval = currentHeadSha
    ? hasAutoApprovedPrHead(repoInfo, currentPr.number, currentHeadSha)
    : false;

  const hasMergeApproval =
    hasCurrentHeadAutoApproval ||
    (await isPullRequestApprovedForBranchMaintenance(context, repoInfo, currentPr, {
      allowLabelFallback: !isCrossRepositoryPullRequest(currentPr, repoInfo),
    }));

  if (!hasMergeApproval) {
    log(
      context,
      'info',
      {
        prNumber: currentPr.number,
        headSha: currentHeadSha,
        reason,
      },
      'pull-request merge skipped: no qualifying approval'
    );

    return;
  }

  if (currentHeadSha) {
    const greenResult = await evaluateHeadGreenForApprovalReevaluation(context, repoInfo, currentHeadSha);

    if (!greenResult.green) {
      log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          headSha: currentHeadSha,
          greenReason: greenResult.reason,
          blockingRuns: greenResult.blockingRuns,
          latestRuns: greenResult.latestRuns.slice(0, 30),
          reason,
        },
        'pull-request merge skipped: current head checks are not green'
      );

      return;
    }

    const pendingRuns = greenResult.latestRuns.filter((run) => toStringTrim(run.status).toLowerCase() !== 'completed');

    if (pendingRuns.length) {
      log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          headSha: currentHeadSha,
          pendingRuns,
          reason,
        },
        'pull-request merge skipped: current head checks are still pending'
      );

      return;
    }
  }

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const beforeHeadSha = toStringTrim(currentPr.head?.sha);

    try {
      const merged = await tryMergeIfGreen(context, {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        prNumber: currentPr.number,
        mergeMethod: 'squash',
        prData: currentPr,
      });

      const afterMergeAttempt = await readFreshPullRequest(context, repoInfo, currentPr.number);
      if (!afterMergeAttempt) return;

      if (!isPullRequestOpen(afterMergeAttempt)) return;

      const afterHeadSha = toStringTrim(afterMergeAttempt.head?.sha);

      if (beforeHeadSha && afterHeadSha && beforeHeadSha !== afterHeadSha) {
        log(
          context,
          'info',
          {
            prNumber: currentPr.number,
            beforeHeadSha,
            afterHeadSha,
            reason,
          },
          'pull-request head changed after merge attempt'
        );

        return;
      }

      if (merged === true) return;

      if (merged === false) {
        log(
          context,
          'info',
          {
            prNumber: afterMergeAttempt.number,
            headSha: toStringTrim(afterMergeAttempt.head?.sha),
            mergeable: afterMergeAttempt.mergeable,
            mergeableState: readMergeableState(afterMergeAttempt),
            reason,
          },
          'pull-request merge returned false, branch update not requested'
        );

        return;
      }

      currentPr = await waitForPullRequestMergeability(
        context,
        repoInfo,
        afterMergeAttempt,
        `${reason}:after-merge-attempt-${attempt}`
      );

      if (!isPullRequestOpen(currentPr)) return;

      if (isPullRequestDirty(currentPr)) {
        log(
          context,
          'warn',
          {
            prNumber: currentPr.number,
            mergeableState: readMergeableState(currentPr),
            reason,
          },
          'pull-request has merge conflicts after mergeability refresh'
        );

        return;
      }

      if (await shouldUpdatePullRequestBranch(context, repoInfo, currentPr, baseBranch)) {
        await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
          context,
          repoInfo,
          currentPr,
          baseBranch,
          `${reason}:behind-after-merge-attempt`
        );
        return;
      }

      if (attempt < 2 && isMergeabilityPending(currentPr)) {
        continue;
      }

      log(
        context,
        'info',
        {
          prNumber: currentPr.number,
          mergeable: currentPr.mergeable,
          mergeableState: readMergeableState(currentPr),
          reason,
        },
        'pull-request not merged after green check'
      );

      return;
    } catch (error: unknown) {
      if (isMergeBlockedByBranchProtection(error)) {
        log(
          context,
          'info',
          {
            prNumber: currentPr.number,
            headSha: toStringTrim(currentPr.head?.sha),
            err: getErrorMessage(error),
            status: getHttpStatus(error),
            reason,
          },
          'pull-request merge blocked by branch protection'
        );

        return;
      }

      if (shouldTryBranchUpdateAfterMergeFailure(error)) {
        const freshPr = (await readFreshPullRequest(context, repoInfo, currentPr.number)) || currentPr;

        if (await shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch)) {
          await requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
            context,
            repoInfo,
            freshPr,
            baseBranch,
            `${reason}:merge-failed-outdated`
          );
        } else {
          log(
            context,
            'info',
            {
              prNumber: freshPr.number,
              headSha: toStringTrim(freshPr.head?.sha),
              mergeable: freshPr.mergeable,
              mergeableState: readMergeableState(freshPr),
              err: getErrorMessage(error),
              status: getHttpStatus(error),
              reason,
            },
            'pull-request merge failed, branch update not requested'
          );
        }

        return;
      }

      throw error;
    }
  }
}

function parsePositiveIssueNumber(value: string | undefined): number | null {
  if (!value) return null;

  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function parseIssueNumberFromText(value: unknown, patterns: RegExp[]): number | null {
  const raw = toStringTrim(value);
  if (!raw) return null;

  for (const pattern of patterns) {
    const match = pattern.exec(raw);
    const parsed = parsePositiveIssueNumber(match?.[1]);
    if (parsed !== null) return parsed;
  }

  return null;
}

function parseLinkedIssueNumberFromPrBody(body: unknown): number | null {
  return parseIssueNumberFromText(body, [
    /<!--\s*nsreq:issue:(\d+)\s*-->/i,
    /\bsource\s*:\s*#(\d+)\b/i,
    /\bissue\s*#\s*(\d+)\b/i,
    /\bissue\s+(\d+)\b/i,
    /\b(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s*:?\s*#(\d+)\b/i,
  ]);
}

function parseLinkedIssueNumberFromPr(pr: PullRequestLike, repoInfo?: RepoInfo): number | null {
  const fromBody = parseLinkedIssueNumberFromPrBody(pr.body);
  if (fromBody !== null) return fromBody;

  const fromTitle = parseIssueNumberFromText(pr.title, [
    /\bissue\s*#?\s*(\d+)\b/i,
    /\b(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s*:?\s*#(\d+)\b/i,
  ]);

  if (fromTitle !== null) return fromTitle;

  if (repoInfo && isCrossRepositoryPullRequest(pr, repoInfo)) return null;

  return parseIssueNumberFromText(pr.head?.ref, [/(?:^|[-_/])issue[-_/]?(\d+)(?:$|[-_/])/i]);
}

function isSnapshotManagedRequestPr(pr: PullRequestLike): boolean {
  return Boolean(extractHashFromPrBody(toStringTrim(pr.body)));
}

async function listOpenPullRequests(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo
): Promise<PullRequestLike[]> {
  const out: PullRequestLike[] = [];
  let page = 1;

  while (true) {
    const { data } = await context.octokit.pulls.list({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      state: 'open',
      per_page: 100,
      page,
    });

    const prs = (data || []) as unknown as PullRequestLike[];
    if (!prs.length) break;

    out.push(...prs);

    if (prs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return out;
}

async function processPullRequestForAutoMerge(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<void> {
  const prBaseBranch = toStringTrim(pr.base?.ref);

  if (await isSequentialDirectRegistryPr(context, repoInfo, pr, prBaseBranch)) {
    if (await shouldDeferSequentialDirectRegistryPrProcessing(context, repoInfo, pr)) {
      return;
    }
  }

  const issueNumber = parseLinkedIssueNumberFromPr(pr, repoInfo);

  if (issueNumber === null) {
    const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  const params: IssueParams = {
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    issue_number: issueNumber,
  };

  let issue: IssueLike;
  try {
    const res = await context.octokit.issues.get(params);
    issue = res.data as unknown as IssueLike;
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'direct-pr:linked-issue-read-failed-fallback-standalone'
    );

    const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, freshPr, {
      baseBranch: toStringTrim(freshPr.base?.ref),
    });

    if (standaloneOutcome !== 'approved') return;

    const approvedPr = (await readFreshPullRequest(context, repoInfo, freshPr.number)) || freshPr;
    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, approvedPr, 'auto-merge');
    return;
  }

  if (!process.env.JEST_WORKER_ID && !hasIssueFormInputs(issue)) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
      },
      'direct-pr:linked-issue-not-request-form-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  let template: TemplateLike;
  try {
    template = await loadTemplateWithLabelRefresh(context, params, issue);
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        issueNumber,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'direct-pr:linked-issue-template-load-failed-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
  if (!isRequestIssue(context, template, parsedFormData)) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        issueNumber,
        parsedKeys: Object.keys(parsedFormData || {}),
      },
      'direct-pr:linked-issue-not-request-issue-fallback-standalone'
    );

    const standaloneOutcome = await maybeHandleStandaloneDirectPrApproval(context, repoInfo, pr);
    if (standaloneOutcome !== 'approved') return;

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const body = toStringTrim(pr.body);
  const snapshotHashes = buildCompatibleRequestSnapshotHashes(issue.body, parsedFormData, template);
  const currentHash =
    snapshotHashes[0] || calcSnapshotHash(parsedFormData, template, readIssueBodyForProcessing(issue.body));
  const prHash = extractHashFromPrBody(body);

  if (prHash) {
    if (!snapshotHashes.includes(prHash)) {
      await closeOutdatedRequestPrs(context, params, template, {
        parsedFormData,
        currentHash,
        acceptedHashes: snapshotHashes,
      });
      return;
    }

    await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
    return;
  }

  const directPrOutcome = await maybeHandleDirectPrApprovalForMerge(
    context,
    repoInfo,
    params,
    issue,
    template,
    parsedFormData,
    pr
  );

  if (directPrOutcome !== 'approved') return;

  await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'auto-merge');
}

function isApprovalConfigChangePath(filePath: string): boolean {
  return /^\.github\/registry-bot\/config\.(?:[cm]?js|ts|ya?ml)$/i.test(normalizeRepoPath(filePath));
}

function readPushChangedFiles(payload: unknown): string[] {
  if (!isPlainObject(payload)) return [];

  const commits = Array.isArray(payload['commits']) ? payload['commits'] : [];
  const out: string[] = [];
  const seen = new Set<string>();

  for (const commit of commits) {
    if (!isPlainObject(commit)) continue;

    for (const key of ['added', 'modified', 'removed'] as const) {
      const files = Array.isArray(commit[key]) ? commit[key] : [];
      for (const file of files) {
        const normalized = normalizeRepoPath(file);
        if (!normalized || seen.has(normalized)) continue;
        seen.add(normalized);
        out.push(normalized);
      }
    }
  }

  return out;
}

async function reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string,
  reason = 'default-branch-push:direct-pr-reevaluation'
): Promise<SequentialRegistryPrResult> {
  log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      baseBranch,
      reason,
      hooksSource: context.resourceBotHooksSource,
    },
    'direct-pr-reeval:start'
  );

  const result = await runOneSequentialDirectRegistryPrMaintenance(context, repoInfo, baseBranch, reason);

  log(
    context,
    'info',
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      baseBranch,
      reason,
      ...result,
    },
    'direct-pr-reeval:done'
  );

  return result;
}

function isDefaultBranchPush(payload: unknown): boolean {
  if (!isPlainObject(payload)) return false;

  const ref = toStringTrim(payload['ref']);
  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  const defaultBranch = repoObj ? toStringTrim(repoObj['default_branch']) : '';

  return Boolean(ref && defaultBranch && ref === `refs/heads/${defaultBranch}`);
}

function readDefaultBranchFromPayload(payload: unknown): string {
  if (!isPlainObject(payload)) return '';

  const repoObj = isPlainObject(payload['repository']) ? payload['repository'] : null;
  return repoObj ? toStringTrim(repoObj['default_branch']) : '';
}

function readDefaultBranchFromPush(payload: unknown): string {
  return readDefaultBranchFromPayload(payload);
}

function pullRequestTargetsBranch(pr: PullRequestLike, branchName: string): boolean {
  const target = toStringTrim(branchName);
  if (!target) return true;

  const prBase = toStringTrim(pr.base?.ref);
  return !prBase || prBase === target;
}

async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string,
  reason = 'default-branch-push'
): Promise<boolean> {
  if (await isSequentialRegistryPrActiveBlocking(context, repoInfo)) {
    return false;
  }

  const openPrs = await listOpenPullRequests(context, repoInfo);

  for (const pr of openPrs.sort((a, b) => b.number - a.number)) {
    const headSha = toStringTrim(pr.head?.sha);

    if (!headSha) continue;
    if (!pullRequestTargetsBranch(pr, baseBranch)) continue;
    if (isSequentialRegistryPrHeadSkipped(repoInfo, pr)) continue;

    try {
      const changedRegistryFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, baseBranch);

      if (!changedRegistryFiles.length) {
        log(context, 'info', { prNumber: pr.number, reason }, 'skip branch update: no registry yaml files changed');
        continue;
      }

      if (!isSnapshotManagedRequestPr(pr)) {
        log(
          context,
          'info',
          {
            prNumber: pr.number,
            reason,
          },
          'skip branch update: direct registry PR handled by sequential queue'
        );
        continue;
      }

      const approved = await isPullRequestApprovedForBranchMaintenance(context, repoInfo, pr);
      if (!approved) {
        log(context, 'info', { prNumber: pr.number, reason }, 'skip branch update: PR is not approved');
        continue;
      }

      const freshPr = await waitForPullRequestMergeability(context, repoInfo, pr, `${reason}:before-update-branch`);

      if (!isPullRequestOpen(freshPr)) continue;

      if (isPullRequestDirty(freshPr)) {
        log(
          context,
          'warn',
          {
            prNumber: freshPr.number,
            mergeableState: readMergeableState(freshPr),
            reason,
          },
          'skip branch update: PR has merge conflicts'
        );
        continue;
      }

      const mustUpdate = await shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch);

      if (!mustUpdate) {
        log(
          context,
          'info',
          {
            prNumber: freshPr.number,
            mergeable: freshPr.mergeable,
            mergeableState: readMergeableState(freshPr),
            reason,
          },
          'skip branch update: PR is not behind current base'
        );
        continue;
      }

      const requested = await requestPullRequestBranchUpdate(context, repoInfo, freshPr, reason);

      if (requested) {
        return true;
      }

      markSequentialRegistryPrHeadSkipped(context, repoInfo, freshPr, 'approved-branch-update-request-failed');
    } catch (error: unknown) {
      log(
        context,
        'warn',
        {
          err: getErrorMessage(error),
          prNumber: pr.number,
          reason,
        },
        'failed to update approved pull request branch after default branch push'
      );

      markSequentialRegistryPrHeadSkipped(context, repoInfo, pr, 'approved-branch-update-exception');
    }
  }

  return false;
}

async function updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string
): Promise<boolean> {
  const requested = await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
    context,
    repoInfo,
    baseBranch,
    'default-branch-push'
  );

  if (requested) return true;

  const retryTimer = setTimeout(() => {
    void updateApprovedOpenPullRequestBranchesAfterDefaultBranchPush(
      context,
      repoInfo,
      baseBranch,
      'default-branch-push:delayed-retry'
    ).catch((error: unknown) => {
      log(
        context,
        'warn',
        {
          err: getErrorMessage(error),
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          baseBranch,
        },
        'failed to run delayed approved pull request branch update retry'
      );
    });
  }, DEFAULT_BRANCH_UPDATE_RETRY_DELAY_MS);

  if (retryTimer && typeof (retryTimer as { unref?: () => void }).unref === 'function') {
    retryTimer.unref();
  }

  return false;
}

function normalizeRepoPath(path: unknown): string {
  return toStringTrim(path)
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/');
}

function isYamlPath(path: string): boolean {
  const p = path.toLowerCase();
  return p.endsWith('.yaml') || p.endsWith('.yml');
}

function normalizeTypeToken(value: unknown): string {
  return toStringTrim(value)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
}

function mapRegistryDocTypeToRequestType(value: unknown): string {
  const type = normalizeTypeToken(value);

  if (type === 'system') return 'systemNamespace';
  if (type === 'authority') return 'authorityNamespace';
  if (type === 'subcontext') return 'subContextNamespace';
  if (type === 'product') return 'product';
  if (type === 'vendor') return 'vendor';

  return '';
}

function matchRequestTypesForFile(context: BotContext<RequestEvents>, filePath: string): string[] {
  const fp = normalizeRepoPath(filePath);
  const cfg = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = isPlainObject(cfg.requests) ? cfg.requests : {};
  const matches: string[] = [];

  for (const [requestType, entry] of Object.entries(reqs)) {
    if (!isPlainObject(entry)) continue;

    const folder = normalizeRepoPath(entry['folderName']);
    if (!folder) continue;

    if (fp === folder || fp.startsWith(`${folder}/`)) {
      matches.push(requestType);
    }
  }

  return matches;
}

function pickRequestTypeForChangedResource(
  context: BotContext<RequestEvents>,
  filePath: string,
  doc: Record<string, unknown>
): string {
  const candidates = matchRequestTypesForFile(context, filePath);
  if (candidates.length === 0) return '';
  if (candidates.length === 1) return candidates[0];

  const byDocType = mapRegistryDocTypeToRequestType(doc['type']);
  if (byDocType && candidates.includes(byDocType)) return byDocType;

  return '';
}

function resolveRegistryDocResourceName(doc: Record<string, unknown>): string {
  const directKeys = ['identifier', 'namespace', 'product-id', 'productId', 'id', 'name', 'vendor'];

  for (const key of directKeys) {
    const value = toStringTrim(doc[key]).replaceAll('\u00a0', ' ').trim();
    if (value) return value;
  }

  return '';
}

function stringifyRegistryDocFormValue(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);

  if (Array.isArray(value)) {
    const scalarItems = value
      .map((item) =>
        typeof item === 'string' || typeof item === 'number' || typeof item === 'boolean' ? String(item) : ''
      )
      .map((item) => item.trim())
      .filter(Boolean);

    if (scalarItems.length === value.length) return scalarItems.join('\n');
  }

  return YAML.stringify(value).trim();
}

function buildFormDataFromRegistryDoc(doc: Record<string, unknown>): FormData {
  const out: FormData = {};

  for (const [key, value] of Object.entries(doc)) {
    const serialized = stringifyRegistryDocFormValue(value);
    if (serialized) out[key] = serialized;
  }

  const resourceName = resolveRegistryDocResourceName(doc);
  if (resourceName) {
    out.identifier = out.identifier || resourceName;
    out.namespace = out.namespace || resourceName;
  }

  const name = toStringTrim(doc['name']);
  if (name && !out.name) out.name = name;

  const description = toStringTrim(doc['description']);
  if (description && !out.description) out.description = description;

  const title = toStringTrim(doc['title']);
  if (title && !out.title) out.title = title;

  const vendor = toStringTrim(doc['vendor']);
  if (vendor && !out.vendor) out.vendor = vendor;

  const contacts = Array.isArray(doc['contact'])
    ? doc['contact']
        .map((v: unknown) => toStringTrim(v))
        .filter(Boolean)
        .join('\n')
    : toStringTrim(doc['contact']);

  if (contacts && !out.contact) out.contact = contacts;

  return out;
}

function isRegistryEntryPath(context: BotContext<RequestEvents>, filePath: string): boolean {
  return matchRequestTypesForFile(context, filePath).length > 0;
}
function isChangedYamlCandidate(file: PullRequestFileLike): string {
  const filename = normalizeRepoPath(file?.filename);
  const status = toStringTrim(file?.status).toLowerCase();

  if (!filename || !isYamlPath(filename) || status === 'removed') return '';
  return filename;
}

async function listChangedYamlFilesPage(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number,
  page: number
): Promise<PullRequestFileLike[]> {
  const res = await (
    context.octokit.pulls as unknown as {
      listFiles: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: PullRequestFileLike[] }>;
    }
  ).listFiles({
    owner: repoInfo.owner,
    repo: repoInfo.repo,
    pull_number: prNumber,
    per_page: 100,
    page,
  });

  return Array.isArray(res?.data) ? res.data : [];
}

async function listChangedYamlFilesForPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  prNumber: number
): Promise<string[]> {
  const out: string[] = [];
  let page = 1;

  while (true) {
    const files = await listChangedYamlFilesPage(context, repoInfo, prNumber, page);
    if (!files.length) break;

    for (const file of files) {
      const filename = isChangedYamlCandidate(file);
      if (filename && isRegistryEntryPath(context, filename)) out.push(filename);
    }

    if (files.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return Array.from(new Set(out));
}

async function readBranchHeadSha(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string
): Promise<string> {
  const branch = toStringTrim(branchName);
  if (!branch) return '';

  try {
    const res = await context.octokit.repos.getBranch({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      branch,
    });

    return toStringTrim((res as unknown as { data?: { commit?: { sha?: string | null } } })?.data?.commit?.sha);
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        branch,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'branch-head-sha:read-failed'
    );

    return '';
  }
}

async function readRecursiveGitTreeEntries(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  ref: string
): Promise<GitTreeEntryLike[]> {
  const treeSha = toStringTrim(ref);
  if (!treeSha) return [];

  try {
    const res = await (
      context.octokit.git as unknown as {
        getTree: (args: {
          owner: string;
          repo: string;
          tree_sha: string;
          recursive?: 'true';
        }) => Promise<{ data?: GitTreeLike }>;
      }
    ).getTree({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      tree_sha: treeSha,
      recursive: 'true',
    });

    return Array.isArray(res?.data?.tree) ? res.data.tree : [];
  } catch (error: unknown) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        ref: treeSha,
        err: getErrorMessage(error),
        status: getHttpStatus(error),
      },
      'git-tree:read-failed'
    );

    return [];
  }
}

function registryYamlTreeEntryPath(context: BotContext<RequestEvents>, entry: GitTreeEntryLike): string {
  const path = normalizeRepoPath(entry?.path);
  const type = toStringTrim(entry?.type).toLowerCase();

  if (type !== 'blob') return '';
  if (!path || !isYamlPath(path)) return '';
  if (!isRegistryEntryPath(context, path)) return '';

  return path;
}

type PullRequestHeadReadCandidate = {
  repoInfo: RepoInfo;
  ref: string;
  source: string;
};

function sameRepoInfo(a: RepoInfo, b: RepoInfo): boolean {
  return a.owner.toLowerCase() === b.owner.toLowerCase() && a.repo.toLowerCase() === b.repo.toLowerCase();
}

function resolveRepoInfoFromRepoLike(repoLike: PullRequestRepoLike | null | undefined): RepoInfo | null {
  const fullName = toStringTrim(repoLike?.full_name);
  if (fullName) {
    const parts = fullName
      .split('/')
      .map((part) => toStringTrim(part))
      .filter(Boolean);

    if (parts.length === 2) {
      return { owner: parts[0], repo: parts[1] };
    }
  }

  const owner = normalizeLogin(repoLike?.owner?.login);
  const repo = toStringTrim(repoLike?.name);

  return owner && repo ? { owner, repo } : null;
}

function resolvePullRequestHeadRepoInfo(pr: PullRequestLike, fallbackRepoInfo: RepoInfo): RepoInfo {
  return resolveRepoInfoFromRepoLike(pr.head?.repo) || fallbackRepoInfo;
}

function isCrossRepositoryPullRequest(pr: PullRequestLike, baseRepoInfo: RepoInfo): boolean {
  return !sameRepoInfo(resolvePullRequestHeadRepoInfo(pr, baseRepoInfo), baseRepoInfo);
}

function buildPullRequestHeadReadCandidates(repoInfo: RepoInfo, pr: PullRequestLike): PullRequestHeadReadCandidate[] {
  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const headSha = toStringTrim(pr.head?.sha);
  const headRef = toStringTrim(pr.head?.ref);
  const isCrossRepo = !sameRepoInfo(headRepoInfo, repoInfo);

  const out: PullRequestHeadReadCandidate[] = [];
  const seen = new Set<string>();

  const add = (candidateRepoInfo: RepoInfo, ref: string, source: string): void => {
    const normalizedRef = toStringTrim(ref);
    if (!normalizedRef) return;

    const key = `${candidateRepoInfo.owner}/${candidateRepoInfo.repo}:${normalizedRef}`;
    if (seen.has(key)) return;
    seen.add(key);

    out.push({
      repoInfo: candidateRepoInfo,
      ref: normalizedRef,
      source,
    });
  };

  add(repoInfo, headSha, 'base-repo:head-sha');
  add(repoInfo, `refs/pull/${pr.number}/head`, 'base-repo:pull-ref-full');
  add(repoInfo, `pull/${pr.number}/head`, 'base-repo:pull-ref-short');

  if (!isCrossRepo) {
    add(repoInfo, headRef, 'base-repo:head-ref');
  }

  add(headRepoInfo, headSha, 'head-repo:head-sha');
  add(headRepoInfo, headRef, 'head-repo:head-ref');

  return out;
}

async function readPullRequestHeadFileText(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  path: string
): Promise<string | null> {
  const candidates = buildPullRequestHeadReadCandidates(repoInfo, pr);
  const normalizedPath = normalizeRepoPath(path);

  for (const candidate of candidates) {
    const raw = await readRepoFileTextAtRef(context, candidate.repoInfo, normalizedPath, candidate.ref);
    if (raw === null) continue;

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        path: normalizedPath,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'pull-request head file resolved'
    );

    return raw;
  }

  log(
    context,
    'warn',
    {
      prNumber: pr.number,
      path: normalizedPath,
      baseOwner: repoInfo.owner,
      baseRepo: repoInfo.repo,
      headOwner: resolvePullRequestHeadRepoInfo(pr, repoInfo).owner,
      headRepo: resolvePullRequestHeadRepoInfo(pr, repoInfo).repo,
      headRef: toStringTrim(pr.head?.ref),
      headSha: toStringTrim(pr.head?.sha),
      crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      candidates: candidates.map((candidate) => ({
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
      })),
    },
    'pull-request head file read failed'
  );

  return null;
}

async function readPullRequestHeadTreeEntries(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<GitTreeEntryLike[]> {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return [];

  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const candidates: PullRequestHeadReadCandidate[] = [
    {
      repoInfo,
      ref: headSha,
      source: 'base-repo:head-sha',
    },
  ];

  if (!sameRepoInfo(headRepoInfo, repoInfo)) {
    candidates.push({
      repoInfo: headRepoInfo,
      ref: headSha,
      source: 'head-repo:head-sha',
    });
  }

  for (const candidate of candidates) {
    const entries = await readRecursiveGitTreeEntries(context, candidate.repoInfo, candidate.ref);
    if (!entries.length) continue;

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        source: candidate.source,
        owner: candidate.repoInfo.owner,
        repo: candidate.repoInfo.repo,
        ref: candidate.ref,
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        entries: entries.length,
      },
      'pull-request head tree resolved'
    );

    return entries;
  }

  return [];
}

async function listChangedYamlFilesForPrAgainstCurrentBase(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<string[]> {
  const baseRef = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!baseRef) return [];

  const baseSha = await readBranchHeadSha(context, repoInfo, baseRef);
  if (!baseSha) return [];

  const [baseEntries, headEntries] = await Promise.all([
    readRecursiveGitTreeEntries(context, repoInfo, baseSha),
    readPullRequestHeadTreeEntries(context, repoInfo, pr),
  ]);

  const baseByPath = new Map<string, string>();

  for (const entry of baseEntries) {
    const path = registryYamlTreeEntryPath(context, entry);
    if (!path) continue;

    const sha = toStringTrim(entry.sha);
    if (sha) baseByPath.set(path, sha);
  }

  const changed: string[] = [];
  const seen = new Set<string>();

  for (const entry of headEntries) {
    const path = registryYamlTreeEntryPath(context, entry);
    if (!path || seen.has(path)) continue;

    const headEntrySha = toStringTrim(entry.sha);
    const baseEntrySha = baseByPath.get(path) || '';

    if (!headEntrySha) continue;
    if (baseEntrySha && baseEntrySha === headEntrySha) continue;

    seen.add(path);
    changed.push(path);
  }

  return changed;
}

async function listChangedYamlFilesForPrWithFallback(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch?: string
): Promise<string[]> {
  const fromPullFiles = await listChangedYamlFilesForPr(context, repoInfo, pr.number);
  if (fromPullFiles.length) return fromPullFiles;

  const fallbackBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!fallbackBaseBranch) return [];

  const fromTreeDiff = await listChangedYamlFilesForPrAgainstCurrentBase(context, repoInfo, pr, fallbackBaseBranch);

  if (fromTreeDiff.length) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        baseBranch: fallbackBaseBranch,
        changedRegistryFiles: fromTreeDiff,
      },
      'changed-registry-files:fallback-tree-diff'
    );
  }

  return fromTreeDiff;
}

async function isPullRequestBehindCurrentBase(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<boolean> {
  const headSha = toStringTrim(pr.head?.sha);
  const headRef = toStringTrim(pr.head?.ref);
  const baseRef = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);

  if (!headSha || !baseRef) return false;

  const baseHeadSha = await readBranchHeadSha(context, repoInfo, baseRef);
  if (!baseHeadSha || baseHeadSha === headSha) return false;

  const headRepoInfo = resolvePullRequestHeadRepoInfo(pr, repoInfo);
  const candidates: string[] = [`${headSha}...${baseHeadSha}`];

  if (!sameRepoInfo(headRepoInfo, repoInfo) && headRef) {
    candidates.push(`${headRepoInfo.owner}:${headRef}...${repoInfo.owner}:${baseRef}`);
  }

  for (const basehead of candidates) {
    try {
      const res = await (
        context.octokit.repos as unknown as {
          compareCommitsWithBasehead: (args: {
            owner: string;
            repo: string;
            basehead: string;
          }) => Promise<{ data?: { status?: string | null; ahead_by?: number | null } }>;
        }
      ).compareCommitsWithBasehead({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        basehead,
      });

      const status = toStringTrim(res?.data?.status).toLowerCase();
      const aheadBy = typeof res?.data?.ahead_by === 'number' ? res.data.ahead_by : 0;

      log(
        context,
        'info',
        {
          prNumber: pr.number,
          basehead,
          status,
          aheadBy,
          headSha,
          baseHeadSha,
          crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        },
        'pull-request behind-current-base compare'
      );

      if (status === 'ahead' || status === 'diverged' || aheadBy > 0) return true;
      if (status === 'identical') return false;
    } catch (error: unknown) {
      log(
        context,
        'warn',
        {
          prNumber: pr.number,
          basehead,
          headSha,
          baseBranch: baseRef,
          baseHeadSha,
          err: getErrorMessage(error),
          status: getHttpStatus(error),
          crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
        },
        'pull-request behind-current-base compare failed'
      );
    }
  }

  return isPullRequestBehindBase(pr);
}

async function shouldUpdatePullRequestBranch(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string
): Promise<boolean> {
  if (isPullRequestBehindBase(pr)) return true;
  return await isPullRequestBehindCurrentBase(context, repoInfo, pr, baseBranch);
}

function sequentialRegistryPrRepoKey(repoInfo: RepoInfo): string {
  return `${repoInfo.owner}/${repoInfo.repo}`.toLowerCase();
}

function sequentialRegistryPrHeadKey(repoInfo: RepoInfo, prNumber: number, headSha: string): string {
  return `${sequentialRegistryPrRepoKey(repoInfo)}#${prNumber}:${toStringTrim(headSha)}`;
}

function pruneSequentialRegistryPrSkipState(): void {
  const now = Date.now();

  for (const [key, until] of SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.entries()) {
    if (until <= now) SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.delete(key);
  }
}

function isSequentialRegistryPrHeadSkipped(repoInfo: RepoInfo, pr: PullRequestLike): boolean {
  pruneSequentialRegistryPrSkipState();

  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return false;

  const key = sequentialRegistryPrHeadKey(repoInfo, pr.number, headSha);
  return (SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.get(key) || 0) > Date.now();
}

function markSequentialRegistryPrHeadSkipped(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): void {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return;

  const key = sequentialRegistryPrHeadKey(repoInfo, pr.number, headSha);
  SEQUENTIAL_REGISTRY_PR_SKIPPED_HEADS.set(key, Date.now() + SEQUENTIAL_REGISTRY_PR_SKIP_TTL_MS);

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha,
      reason,
    },
    'sequential-registry-pr:head-skipped'
  );
}

function getSequentialRegistryPrActive(repoInfo: RepoInfo): SequentialRegistryPrActive | null {
  return SEQUENTIAL_REGISTRY_PR_ACTIVE.get(sequentialRegistryPrRepoKey(repoInfo)) || null;
}

function clearSequentialRegistryPrActive(repoInfo: RepoInfo): void {
  SEQUENTIAL_REGISTRY_PR_ACTIVE.delete(sequentialRegistryPrRepoKey(repoInfo));
}

function markSequentialRegistryPrActive(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): void {
  const headSha = toStringTrim(pr.head?.sha);
  if (!headSha) return;

  const startedAt = Date.now();
  const active: SequentialRegistryPrActive = {
    prNumber: pr.number,
    startedHeadSha: headSha,
    startedAt,
    expiresAt: startedAt + SEQUENTIAL_REGISTRY_PR_ACTIVE_TTL_MS,
    reason,
  };

  SEQUENTIAL_REGISTRY_PR_ACTIVE.set(sequentialRegistryPrRepoKey(repoInfo), active);

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha,
      expiresAt: active.expiresAt,
      reason,
    },
    'sequential-registry-pr:active-set'
  );
}

async function isSequentialRegistryPrActiveBlocking(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo
): Promise<boolean> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active) return false;

  if (active.expiresAt <= Date.now()) {
    log(
      context,
      'warn',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        reason: active.reason,
      },
      'sequential-registry-pr:active-expired'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  const freshPr = await readFreshPullRequest(context, repoInfo, active.prNumber);

  if (!freshPr || !isPullRequestOpen(freshPr)) {
    log(
      context,
      'info',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        freshHeadSha: toStringTrim(freshPr?.head?.sha),
        reason: active.reason,
      },
      'sequential-registry-pr:active-cleared-closed'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  const baseBranch = toStringTrim(freshPr.base?.ref);
  const isDirectRegistryPr = await isSequentialDirectRegistryPr(context, repoInfo, freshPr, baseBranch);

  if (!isDirectRegistryPr) {
    log(
      context,
      'info',
      {
        prNumber: active.prNumber,
        startedHeadSha: active.startedHeadSha,
        currentHeadSha: toStringTrim(freshPr.head?.sha),
        reason: active.reason,
      },
      'sequential-registry-pr:active-cleared-non-direct'
    );

    clearSequentialRegistryPrActive(repoInfo);
    return false;
  }

  log(
    context,
    'info',
    {
      prNumber: active.prNumber,
      startedHeadSha: active.startedHeadSha,
      currentHeadSha: toStringTrim(freshPr.head?.sha),
      reason: active.reason,
    },
    'sequential-registry-pr:active-blocking'
  );

  return true;
}

async function isSequentialDirectRegistryPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch?: string
): Promise<boolean> {
  const targetBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);
  if (!targetBaseBranch) return false;
  if (isSnapshotManagedRequestPr(pr)) return false;
  if (!pullRequestTargetsBranch(pr, targetBaseBranch)) return false;

  try {
    const changedRegistryFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, targetBaseBranch);
    return changedRegistryFiles.length > 0;
  } catch (error) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        baseBranch: targetBaseBranch,
        error: error instanceof Error ? error.message : String(error),
      },
      'sequential-registry-pr:changed-files-lookup-failed'
    );

    return false;
  }
}

async function shouldDeferSequentialDirectRegistryPrProcessing(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<boolean> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber === pr.number) return false;

  if (!(await isSequentialRegistryPrActiveBlocking(context, repoInfo))) {
    return false;
  }

  const currentActive = getSequentialRegistryPrActive(repoInfo);
  if (!currentActive || currentActive.prNumber === pr.number) {
    return false;
  }

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      activePrNumber: currentActive.prNumber,
      activeHeadSha: currentActive.startedHeadSha,
    },
    'sequential-registry-pr:auto-merge-deferred'
  );

  return true;
}

async function requestPullRequestBranchUpdateRespectingSequentialRegistryQueue(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  baseBranch: string,
  reason: string
): Promise<boolean> {
  const targetBaseBranch = toStringTrim(baseBranch) || toStringTrim(pr.base?.ref);

  if (!(await isSequentialDirectRegistryPr(context, repoInfo, pr, targetBaseBranch))) {
    return await requestPullRequestBranchUpdate(context, repoInfo, pr, reason);
  }

  const active = getSequentialRegistryPrActive(repoInfo);

  if (active && active.prNumber === pr.number) {
    const requested = await requestPullRequestBranchUpdate(context, repoInfo, pr, reason);

    if (requested) {
      markSequentialRegistryPrActive(context, repoInfo, pr, reason);
    }

    return requested;
  }

  const result = await runOneSequentialDirectRegistryPrMaintenance(context, repoInfo, targetBaseBranch, reason);
  return result.updated;
}

async function advanceSequentialRegistryPrQueueAfterTerminalState(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  reason: string
): Promise<void> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber !== pr.number) return;

  const freshPr = await readFreshPullRequest(context, repoInfo, pr.number);

  if (freshPr && isPullRequestOpen(freshPr)) {
    return;
  }

  clearSequentialRegistryPrActive(repoInfo);

  const baseBranch = toStringTrim(freshPr?.base?.ref) || toStringTrim(pr.base?.ref);
  if (!baseBranch) return;

  await runOneSequentialDirectRegistryPrMaintenance(context, repoInfo, baseBranch, reason);
}

async function collectSequentialDirectRegistryPrCandidates(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string,
  reason: string
): Promise<SequentialRegistryPrCandidate[]> {
  const openPrs = await listOpenPullRequests(context, repoInfo);
  const candidates: SequentialRegistryPrCandidate[] = [];

  for (const pr of openPrs.sort((a, b) => b.number - a.number)) {
    const headSha = toStringTrim(pr.head?.sha);
    const linkedIssueNumber = parseLinkedIssueNumberFromPr(pr, repoInfo);
    const snapshotManaged = isSnapshotManagedRequestPr(pr);

    const baseLog = {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      prNumber: pr.number,
      title: toStringTrim(pr.title),
      headSha,
      headRef: toStringTrim(pr.head?.ref),
      prBase: toStringTrim(pr.base?.ref),
      baseBranch,
      linkedIssueNumber,
      snapshotManaged,
      reason,
    };

    if (snapshotManaged) {
      log(context, 'info', { ...baseLog, skipReason: 'snapshot-managed-request-pr' }, 'direct-pr-reeval:skip');
      continue;
    }

    if (!headSha) {
      log(context, 'info', { ...baseLog, skipReason: 'missing-head-sha' }, 'direct-pr-reeval:skip');
      continue;
    }

    if (!pullRequestTargetsBranch(pr, baseBranch)) {
      log(context, 'info', { ...baseLog, skipReason: 'different-base-branch' }, 'direct-pr-reeval:skip');
      continue;
    }

    if (isSequentialRegistryPrHeadSkipped(repoInfo, pr)) {
      log(context, 'info', { ...baseLog, skipReason: 'head-temporarily-skipped' }, 'direct-pr-reeval:skip');
      continue;
    }

    const changedRegistryFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, baseBranch);

    if (!changedRegistryFiles.length) {
      log(
        context,
        'info',
        {
          ...baseLog,
          changedRegistryFiles,
          skipReason: 'no-registry-yaml-files-changed',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
    const freshHeadSha = toStringTrim(freshPr.head?.sha);

    if (!isPullRequestOpen(freshPr)) {
      log(
        context,
        'info',
        {
          ...baseLog,
          freshHeadSha,
          changedRegistryFiles,
          skipReason: 'pr-not-open',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    if (isPullRequestDirty(freshPr)) {
      log(
        context,
        'warn',
        {
          ...baseLog,
          freshHeadSha,
          changedRegistryFiles,
          mergeableState: readMergeableState(freshPr),
          skipReason: 'pr-has-merge-conflicts',
        },
        'direct-pr-reeval:skip'
      );
      continue;
    }

    const mustUpdate = await shouldUpdatePullRequestBranch(context, repoInfo, freshPr, baseBranch);
    const approvedForUpdate = mustUpdate
      ? await isPullRequestApprovedForBranchMaintenance(context, repoInfo, freshPr)
      : false;

    log(
      context,
      'info',
      {
        ...baseLog,
        freshHeadSha,
        changedRegistryFiles,
        mergeable: freshPr.mergeable,
        mergeableState: readMergeableState(freshPr),
        mustUpdate,
        approvedForUpdate,
      },
      'direct-pr-reeval:update-check'
    );

    candidates.push({
      pr,
      freshPr,
      changedRegistryFiles,
      mustUpdate,
      approvedForUpdate,
    });
  }

  return candidates;
}

async function runOneSequentialDirectRegistryPrMaintenance(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  baseBranch: string,
  reason: string
): Promise<SequentialRegistryPrResult> {
  const key = sequentialRegistryPrRepoKey(repoInfo);
  const existing = SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.get(key);

  if (existing) return await existing;

  const pending = (async (): Promise<SequentialRegistryPrResult> => {
    if (await isSequentialRegistryPrActiveBlocking(context, repoInfo)) {
      return { updated: false, processed: false, blockedByActive: true };
    }

    const candidates = await collectSequentialDirectRegistryPrCandidates(context, repoInfo, baseBranch, reason);

    for (const candidate of candidates.filter((item) => item.mustUpdate)) {
      const requested = await requestPullRequestBranchUpdate(
        context,
        repoInfo,
        candidate.freshPr,
        candidate.approvedForUpdate
          ? `${reason}:sequential-direct-pr-update-approved`
          : `${reason}:sequential-direct-pr-refresh-stale`
      );

      log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          prNumber: candidate.freshPr.number,
          title: toStringTrim(candidate.freshPr.title),
          headSha: toStringTrim(candidate.freshPr.head?.sha),
          headRef: toStringTrim(candidate.freshPr.head?.ref),
          baseBranch,
          changedRegistryFiles: candidate.changedRegistryFiles,
          requested,
          reason,
        },
        'direct-pr-reeval:update-before-approval-result'
      );

      if (requested) {
        markSequentialRegistryPrActive(context, repoInfo, candidate.freshPr, reason);
        return { updated: true, processed: true, blockedByActive: false };
      }

      markSequentialRegistryPrHeadSkipped(context, repoInfo, candidate.freshPr, 'branch-update-request-failed');
    }

    for (const candidate of candidates.filter((item) => !item.mustUpdate)) {
      const headSha = toStringTrim(candidate.freshPr.head?.sha);
      const greenResult = headSha
        ? await evaluateHeadGreenForApprovalReevaluation(context, repoInfo, headSha)
        : {
            green: false,
            reason: 'missing-head-sha',
            latestRuns: [],
            blockingRuns: [],
          };

      log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          prNumber: candidate.freshPr.number,
          headSha,
          baseBranch,
          changedRegistryFiles: candidate.changedRegistryFiles,
          green: greenResult.green,
          greenReason: greenResult.reason,
          blockingRuns: greenResult.blockingRuns,
          latestRuns: greenResult.latestRuns.slice(0, 30),
          reason,
        },
        'direct-pr-reeval:head-green'
      );

      if (!greenResult.green) {
        continue;
      }

      await processPullRequestForAutoMerge(context, repoInfo, candidate.freshPr);
      return { updated: false, processed: true, blockedByActive: false };
    }

    return { updated: false, processed: false, blockedByActive: false };
  })().finally(() => {
    SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.delete(key);
  });

  SEQUENTIAL_REGISTRY_PR_QUEUE_INFLIGHT.set(key, pending);
  return await pending;
}

async function markFailedRegistryPrHeadsForSha(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  headSha: string,
  baseBranch: string,
  reason: string
): Promise<boolean> {
  const sha = toStringTrim(headSha);
  if (!sha) return false;

  const openPrs = await listOpenPullRequests(context, repoInfo);
  const matching = openPrs.filter((pr) => toStringTrim(pr.head?.sha) === sha);

  let marked = false;

  for (const pr of matching) {
    if (!pullRequestTargetsBranch(pr, baseBranch)) continue;

    const changedRegistryFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, baseBranch);
    if (!changedRegistryFiles.length) continue;

    markSequentialRegistryPrHeadSkipped(context, repoInfo, pr, reason);

    const active = getSequentialRegistryPrActive(repoInfo);
    if (active?.prNumber === pr.number) {
      clearSequentialRegistryPrActive(repoInfo);
    }

    marked = true;

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: sha,
        changedRegistryFiles,
        reason,
      },
      'sequential-registry-pr:failed-head-marked'
    );
  }

  return marked;
}

async function resolveSequentialRegistryQueueBaseBranchForHead(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  headSha: string,
  fallbackBaseBranch: string
): Promise<string> {
  const fallback = toStringTrim(fallbackBaseBranch);
  if (fallback) return fallback;

  const sha = toStringTrim(headSha);
  if (!sha) return '';

  try {
    const openPrs = await listOpenPullRequests(context, repoInfo);
    const matchingPr = openPrs.find((pr) => toStringTrim(pr.head?.sha) === sha);
    const baseBranch = toStringTrim(matchingPr?.base?.ref);

    if (baseBranch) return baseBranch;
  } catch {
    return '';
  }

  return '';
}

async function handleBlockingRegistryHeadConclusion(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  headSha: string,
  baseBranch: string,
  reason: string
): Promise<boolean> {
  const sha = toStringTrim(headSha);
  if (!sha) return false;

  const marked = await markFailedRegistryPrHeadsForSha(context, repoInfo, sha, baseBranch, reason);
  if (!marked) {
    log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: sha,
        baseBranch: toStringTrim(baseBranch),
        reason,
      },
      'sequential-registry-pr:blocking-head-not-marked'
    );

    return false;
  }

  const advanceBaseBranch = await resolveSequentialRegistryQueueBaseBranchForHead(context, repoInfo, sha, baseBranch);

  if (!advanceBaseBranch) {
    log(
      context,
      'warn',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: sha,
        reason,
      },
      'sequential-registry-pr:advance-skipped-missing-base-branch'
    );

    return true;
  }

  await runOneSequentialDirectRegistryPrMaintenance(
    context,
    repoInfo,
    advanceBaseBranch,
    `${reason}:advance-next-registry-pr`
  );

  return true;
}

async function releaseSequentialRegistryPrIfNotApprovedAfterGreen(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike
): Promise<void> {
  const active = getSequentialRegistryPrActive(repoInfo);
  if (!active || active.prNumber !== pr.number) return;

  const freshPr = await readFreshPullRequest(context, repoInfo, pr.number);
  if (!freshPr || !isPullRequestOpen(freshPr)) {
    clearSequentialRegistryPrActive(repoInfo);
    return;
  }

  const headSha = toStringTrim(freshPr.head?.sha);
  if (!headSha) return;

  const approvedForMaintenance = await isPullRequestApprovedForBranchMaintenance(context, repoInfo, freshPr);
  if (approvedForMaintenance) {
    return;
  }

  const greenResult = await evaluateHeadGreenForApprovalReevaluation(context, repoInfo, headSha);
  if (!greenResult.green) return;

  markSequentialRegistryPrHeadSkipped(context, repoInfo, freshPr, 'green-head-did-not-qualify-for-approval');
  clearSequentialRegistryPrActive(repoInfo);

  await runOneSequentialDirectRegistryPrMaintenance(
    context,
    repoInfo,
    toStringTrim(freshPr.base?.ref),
    'sequential-direct-pr:advance-after-not-approved'
  );
}

async function readRepoFileTextAtRef(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  path: string,
  ref: string
): Promise<string | null> {
  const p = normalizeRepoPath(path);
  const branchRef = toStringTrim(ref);
  if (!p || !branchRef) return null;

  try {
    const res = await (
      context.octokit.repos as unknown as {
        getContent: (args: { owner: string; repo: string; path: string; ref?: string }) => Promise<{ data?: unknown }>;
      }
    ).getContent({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      path: p,
      ref: branchRef,
    });

    const data = (res as { data?: unknown }).data;
    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    const enc = typeof data.encoding === 'string' ? data.encoding : 'base64';
    return Buffer.from(String(data.content || ''), enc as BufferEncoding).toString('utf8');
  } catch {
    return null;
  }
}

async function readRegistryDocForApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  filePath: string
): Promise<Record<string, unknown> | null> {
  const raw = await readPullRequestHeadFileText(context, repoInfo, pr, filePath);
  if (!raw) return null;

  try {
    const parsed = YAML.parse(raw) as unknown;
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function evaluateChangedResourceApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  filePath: string,
  requestAuthorId?: string
): Promise<ApprovalDecision> {
  const parsed = await readRegistryDocForApproval(context, repoInfo, pr, filePath);
  if (!parsed) {
    log(
      context,
      'warn',
      {
        prNumber: pr.number,
        filePath,
        baseOwner: repoInfo.owner,
        baseRepo: repoInfo.repo,
        headOwner: resolvePullRequestHeadRepoInfo(pr, repoInfo).owner,
        headRepo: resolvePullRequestHeadRepoInfo(pr, repoInfo).repo,
        headRef: toStringTrim(pr.head?.ref),
        headSha: toStringTrim(pr.head?.sha),
        crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      },
      'direct-pr:on-approval:registry-doc-read-failed'
    );

    return { status: 'unknown' };
  }

  const requestType = pickRequestTypeForChangedResource(context, filePath, parsed);
  if (!requestType) return { status: 'unknown' };

  const resourceName = resolveRegistryDocResourceName(parsed);
  if (!resourceName) return { status: 'unknown' };

  const decision = normalizeApprovalDecision(
    await runApprovalHook(context, repoInfo, {
      requestType,
      namespace: resourceName,
      resourceName,
      formData: buildFormDataFromRegistryDoc(parsed),
      requestAuthorId,
      issue: {
        number: pr.number,
        title: pr.title,
        body: pr.body,
        state: pr.state,
        user: pr.user,
        labels: [],
      },
    })
  );

  return promoteUnknownApprovalDecisionForDirectPrRequester(decision, requestAuthorId);
}

async function evaluateDirectPrOnApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  requestAuthorIdOverride?: string,
  options: DirectPrApprovalOptions = {}
): Promise<ApprovalDecision> {
  const changedFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, options.baseBranch);

  const fallbackRequestAuthorId = requestAuthorIdOverride
    ? ''
    : await resolvePullRequestRequestAuthorId(context, repoInfo, pr);

  const requestAuthorId = toStringTrim(requestAuthorIdOverride) || fallbackRequestAuthorId;

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      headSha: toStringTrim(pr.head?.sha),
      headRef: toStringTrim(pr.head?.ref),
      requestAuthorId,
      changedFiles,
      linkedIssueNumber: parseLinkedIssueNumberFromPr(pr, repoInfo),
      crossRepo: isCrossRepositoryPullRequest(pr, repoInfo),
      headOwner: resolvePullRequestHeadRepoInfo(pr, repoInfo).owner,
      headRepo: resolvePullRequestHeadRepoInfo(pr, repoInfo).repo,
      hooksSource: context.resourceBotHooksSource,
    },
    'direct-pr:on-approval:start'
  );

  if (!changedFiles.length) {
    log(
      context,
      'info',
      {
        prNumber: pr.number,
        headSha: toStringTrim(pr.head?.sha),
        headRef: toStringTrim(pr.head?.ref),
      },
      'direct-pr:on-approval:skip-no-registry-files'
    );

    return {};
  }

  let sawApproved = false;
  let sawNonApproved = false;
  let approvedComment = '';
  let firstUnknownDecision: ApprovalDecision | null = null;

  for (const filePath of changedFiles) {
    const decision = await evaluateChangedResourceApproval(context, repoInfo, pr, filePath, requestAuthorId);

    log(
      context,
      'info',
      {
        prNumber: pr.number,
        filePath,
        requestAuthorId,
        status: toStringTrim(decision.status) || 'none',
        reason: toStringTrim(decision.reason),
        message: toStringTrim(decision.message),
        path: toStringTrim(decision.path),
      },
      'direct-pr:on-approval:file-decision'
    );

    if (decision.status === 'rejected') {
      return decision;
    }

    if (decision.status === 'approved') {
      sawApproved = true;
      if (!approvedComment) approvedComment = toStringTrim(decision.comment);
      continue;
    }

    sawNonApproved = true;
    if (decision.status === 'unknown' && !firstUnknownDecision) {
      firstUnknownDecision = decision;
    }
  }

  if (sawApproved && !sawNonApproved) {
    return {
      status: 'approved',
      ...(approvedComment ? { comment: approvedComment } : {}),
    };
  }

  if (firstUnknownDecision) {
    return firstUnknownDecision;
  }

  if (sawNonApproved) {
    return {
      status: 'unknown',
      reason: 'Manual review required because onApproval did not approve all changed registry files.',
    };
  }

  return {};
}

async function resolveDirectPrRequestTypes(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: DirectPrApprovalOptions = {}
): Promise<string[]> {
  const changedFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, options.baseBranch);
  const requestTypes: string[] = [];

  for (const filePath of changedFiles) {
    const parsed = await readRegistryDocForApproval(context, repoInfo, pr, filePath);
    if (!parsed) continue;

    const requestType = pickRequestTypeForChangedResource(context, filePath, parsed);
    if (!requestType) continue;

    requestTypes.push(requestType);
  }

  return Array.from(new Set(requestTypes));
}

async function handoverStandaloneDirectPrToReview(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  decision: ApprovalDecision,
  options: DirectPrApprovalOptions = {}
): Promise<void> {
  const eff = resolveEffectiveConstants(context);
  const params: IssueParams = { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number };
  const prIssue = prAsIssueLike(pr);

  await setStateLabel(context, params, prIssue, 'review');

  const changedFiles = await listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, options.baseBranch);
  const requestTypes = await resolveDirectPrRequestTypes(context, repoInfo, pr, options);

  const manualApproversOverride = getUnknownManualApprovers(decision);

  const assignees = manualApproversOverride.length
    ? manualApproversOverride
    : resolveReviewAssigneesForRequestTypes(context, prIssue, requestTypes);

  if (assignees.length) {
    await ensureAssigneesOnce(context, params, prIssue, assignees);
    await ensureAssigneesPresent(context, params, assignees);
  }

  const labelsToAdd = [...(eff.globalLabels || []), ...(eff.reviewRequestedLabels || [])].filter(Boolean);
  await ensureLabelsPresentOnce(context, params, labelsToAdd);

  if (eff.labelOnApproved) {
    try {
      await context.octokit.issues.removeLabel({
        ...params,
        name: eff.labelOnApproved,
      });
    } catch {
      // ignore
    }
  }

  const snapshotHash = calcStandaloneDirectPrSnapshotHash(pr, changedFiles);

  await postOnce(context, params, buildReviewHandoverBody(context, snapshotHash, { target: 'pull_request' }), {
    minimizeTag: 'nsreq:handover',
  });

  await postApprovalUnknownOnce(context, params, decision);

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      requestTypes,
      changedFiles,
      assignees,
      snapshotHash,
      decisionStatus: toStringTrim(decision.status) || 'none',
    },
    'direct-pr:handover-to-review'
  );
}

async function handleDirectPrApprovalComment(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  commenter: string
): Promise<void> {
  const eff = resolveEffectiveConstants(context);
  const params: IssueParams = { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number };
  const prIssue = prAsIssueLike(pr);

  const reviewOk = await ensureReviewLabelsPresentOnIssue(context, params, prIssue, eff);
  if (!reviewOk) {
    await postOnce(
      context,
      params,
      'Approval ignored: direct PR is not in review state. Please wait until validation has routed it to review.',
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  const requestTypes = await resolveDirectPrRequestTypes(context, repoInfo, pr, {
    baseBranch: toStringTrim(pr.base?.ref),
  });

  const configuredApprovers = resolveAllowedApproversForRequestTypes(context, requestTypes);

  const approvalDecision = await evaluateDirectPrOnApproval(context, repoInfo, pr, undefined, {
    baseBranch: toStringTrim(pr.base?.ref),
  });

  if (approvalDecision.status === 'rejected') {
    await postOnce(context, params, buildApprovalRejectedBody(approvalDecision), {
      minimizeTag: 'nsreq:on-approval:rejected',
    });
    return;
  }

  const allowedApprovers = uniqLogins([...(configuredApprovers || []), ...(approvalDecision.approvers || [])]);

  const okApprover = isAuthorizedApprover(commenter, pr.user?.login, allowedApprovers);

  if (!okApprover) {
    await postOnce(
      context,
      params,
      `Approval ignored: commenter ${commenter} is not an allowed approver for this direct PR.`,
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  const approved = await ensureAutomatedApprovalReviewForCurrentHead(
    context,
    repoInfo,
    pr,
    {
      status: 'approved',
      comment: `Approved by @${commenter}`,
    },
    {
      skipApprovedLabelStateCleanup: isCrossRepositoryPullRequest(pr, repoInfo),
    }
  );

  if (!approved) return;

  await tryMergeApprovedPrOrUpdateBranch(context, repoInfo, pr, 'direct-pr-manual-approval');
}

async function maybeHandleStandaloneDirectPrApproval(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  pr: PullRequestLike,
  options: DirectPrApprovalOptions = {}
): Promise<ApprovalHandlingResult> {
  const decision = await evaluateDirectPrOnApproval(context, repoInfo, pr, undefined, options);

  if (decision.status === 'approved') {
    const approved = await ensureAutomatedApprovalReviewForCurrentHead(context, repoInfo, pr, decision, {
      skipApprovedLabelStateCleanup: isCrossRepositoryPullRequest(pr, repoInfo),
    });

    if (!approved) return 'continue';

    return 'approved';
  }

  if (decision.status === 'rejected') {
    await postOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      buildApprovalRejectedBody(decision),
      { minimizeTag: 'nsreq:on-approval:rejected' }
    );

    try {
      await context.octokit.pulls.update({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        state: 'closed',
      });
    } catch {
      // ignore
    }

    return 'rejected';
  }

  if (decision.status === 'unknown') {
    await handoverStandaloneDirectPrToReview(context, repoInfo, pr, decision, options);
    return 'continue';
  }

  return 'continue';
}

async function closeLinkedIssuePrs(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  issueNumber: number
): Promise<number[]> {
  let prs: PullRequestLike[] = [];

  try {
    prs = (await findOpenIssuePRsRaw(context, repoInfo, issueNumber)) as unknown as PullRequestLike[];
  } catch {
    prs = [];
  }

  if (prs.length === 0) {
    prs = (await listOpenPullRequests(context, repoInfo)).filter(
      (pr) => parseLinkedIssueNumberFromPr(pr, repoInfo) === issueNumber
    );
  }
  const closed: number[] = [];

  for (const pr of prs) {
    try {
      await context.octokit.pulls.update({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        state: 'closed',
      });
      closed.push(pr.number);
    } catch {
      // ignore
    }
  }

  return closed;
}

async function rejectRequestFromApprovalHook(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  decision: ApprovalDecision,
  options: { closeLinkedPrs?: boolean; minimizeTag?: string } = {}
): Promise<void> {
  const repoInfo = { owner: params.owner, repo: params.repo };

  let closedPrs: number[] = [];
  if (options.closeLinkedPrs) {
    try {
      closedPrs = await closeLinkedIssuePrs(context, repoInfo, issue.number);
    } catch {
      // ignore
    }
  }

  const closedPrRefs = closedPrs.map((n) => `#${n}`).join(', ');
  const closedPrSection = closedPrs.length ? `\n\nClosed linked PR(s): ${closedPrRefs}.` : '';

  await postOnce(context, params, `${buildApprovalRejectedBody(decision)}${closedPrSection}`, {
    minimizeTag: options.minimizeTag || 'nsreq:on-approval:rejected',
  });

  try {
    await context.octokit.issues.update({ ...params, state: 'closed' });
    issue.state = 'closed';
  } catch {
    // ignore
  }
}

async function finalizeApprovedRequest(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  options: {
    approvalPrefix?: string;
    approvalComment?: string;
    autoApproved?: boolean;
  }
): Promise<void> {
  const eff = resolveEffectiveConstants(context);
  const approvalPrefix = toStringTrim(options.approvalPrefix);
  const approvalComment = toStringTrim(options.approvalComment);
  const autoApproved = options.autoApproved === true;

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

  const requestType = resolveEffectiveRequestType(template, parsedFormData);
  const hookApprovers = await resolveAdditionalIssueApproversFromApprovalHook(
    context,
    params,
    issue,
    template,
    parsedFormData,
    requestType
  );

  const existing = await findOpenIssuePrs(context, { owner: params.owner, repo: params.repo }, issue.number);
  if (existing.length) {
    await applyApprovedRequestState(context, params, eff);

    if (autoApproved) {
      await addApprovedLabelToPr(context, { owner: params.owner, repo: params.repo }, existing[0].number);
    }

    await ensureAssigneesPresent(
      context,
      { owner: params.owner, repo: params.repo, issue_number: existing[0].number },
      hookApprovers
    );

    const lead = [toStringTrim(approvalPrefix), toStringTrim(approvalComment)].filter(Boolean).join('. ');
    const body = lead ? `${lead}. PR already open: #${existing[0].number}` : `PR already open: #${existing[0].number}`;

    await postOnce(context, params, body, {
      minimizeTag: 'nsreq:approval-info',
    });
    return;
  }

  try {
    const pr = await createRequestPrWithRecovery(context, params, issue, parsedFormData, template, resourceName);

    await applyApprovedRequestState(context, params, eff);

    if (autoApproved) {
      await addApprovedLabelToPr(context, { owner: params.owner, repo: params.repo }, pr.number);
    }

    await ensureAssigneesPresent(
      context,
      { owner: params.owner, repo: params.repo, issue_number: pr.number },
      hookApprovers
    );

    const lead = [toStringTrim(approvalPrefix), toStringTrim(approvalComment)].filter(Boolean).join('. ');
    const body = lead ? `${lead}. Opened PR: #${pr.number}` : `Opened PR: #${pr.number}`;

    await postOnce(context, params, body, {
      minimizeTag: 'nsreq:approval-info',
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);

    await postOnce(context, params, `Failed to create Pull Request: ${msg}`, { minimizeTag: 'nsreq:approval-info' });
  }
}

async function maybeHandleApprovalDecision(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  requestType: string,
  namespace: string
): Promise<ApprovalHandlingResult> {
  const decision = normalizeApprovalDecision(
    await runApprovalHook(
      context,
      { owner: params.owner, repo: params.repo },
      {
        requestType,
        namespace,
        resourceName: extractResourceNameFromForm(parsedFormData, template),
        formData: parsedFormData,
        issue,
      }
    )
  );

  if (decision.status === 'approved') {
    await finalizeApprovedRequest(context, params, issue, template, parsedFormData, {
      approvalPrefix: '',
      approvalComment: decision.comment,
      autoApproved: true,
    });
    return 'approved';
  }

  if (decision.status === 'rejected') {
    await rejectRequestFromApprovalHook(context, params, issue, decision, {
      closeLinkedPrs: true,
    });
    return 'rejected';
  }

  if (decision.status === 'unknown') {
    await postApprovalUnknownOnce(context, params, decision);
  }

  return 'continue';
}

async function maybeHandleDirectPrApprovalForMerge(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  issueParams: IssueParams,
  issue: IssueLike,
  _template: TemplateLike,
  _parsedFormData: FormData,
  pr: PullRequestLike
): Promise<ApprovalHandlingResult> {
  const issueAuthorId = normalizeLogin(issue.user?.login);
  const prRequesterId = await resolvePullRequestRequestAuthorId(context, repoInfo, pr);
  const requestAuthorId = issueAuthorId || prRequesterId;

  log(
    context,
    'info',
    {
      prNumber: pr.number,
      linkedIssueNumber: issue.number,
      issueAuthorId,
      prRequesterId,
      requestAuthorId,
    },
    'direct-pr:linked-issue-requester-resolved'
  );

  const decision = normalizeApprovalDecision(
    await evaluateDirectPrOnApproval(context, repoInfo, pr, requestAuthorId || undefined)
  );

  if (decision.status === 'approved') {
    const approved = await ensureAutomatedApprovalReviewForCurrentHead(context, repoInfo, pr, decision);
    if (!approved) return 'continue';

    await applyApprovedRequestState(context, issueParams, resolveEffectiveConstants(context));
    return 'approved';
  }

  if (decision.status === 'rejected') {
    await postOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      buildApprovalRejectedBody(decision),
      { minimizeTag: 'nsreq:on-approval:rejected' }
    );

    try {
      await context.octokit.pulls.update({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        state: 'closed',
      });
    } catch {
      // ignore
    }

    await rejectRequestFromApprovalHook(context, issueParams, issue, decision, {
      closeLinkedPrs: true,
      minimizeTag: 'nsreq:on-approval:issue-rejected',
    });

    return 'rejected';
  }

  if (decision.status === 'unknown') {
    await postApprovalUnknownOnce(
      context,
      { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number },
      decision
    );
  }

  return 'continue';
}

function buildSafeResourceSlug(resourceName: unknown): string {
  return toStringTrim(resourceName)
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, '-')
    .replace(/-+/g, '-');
}

function resolveStructuredRootForTemplate(template: TemplateLike): string {
  return toStringTrim(template?._meta?.root).replace(/^\/+/, '').replace(/\/+$/, '');
}

function renderConfiguredRequestBranchName(
  context: BotContext<RequestEvents>,
  issue: IssueLike,
  resourceName: string
): string {
  const cfg = (context.resourceBotConfig ?? DEFAULT_CONFIG) as unknown as {
    pr?: { branchNameTemplate?: unknown } | null;
  };

  const branchTemplate = toStringTrim(cfg?.pr?.branchNameTemplate) || 'feat/resource-{resource}-issue-{issue}';

  return String(branchTemplate)
    .replace('{resource}', buildSafeResourceSlug(resourceName))
    .replace('{issue}', String(issue.number || ''));
}

function extractCreatePrFailureMessage(error: unknown): string {
  const raw = (error instanceof Error ? error.message : String(error)).trim();
  const withoutUrl = raw.replace(/\s*-\s*https?:\/\/\S+$/i, '').trim();

  const marker = 'Validation Failed:';
  const idx = withoutUrl.indexOf(marker);

  if (idx >= 0) {
    const tail = withoutUrl.slice(idx + marker.length).trim();

    try {
      const parsed = JSON.parse(tail) as Record<string, unknown>;
      const msg = toStringTrim(parsed['message']);
      if (msg) return msg;
    } catch {
      // ignore
    }

    return tail || withoutUrl;
  }

  return withoutUrl;
}

function parseNoCommitsHeadBranchFromCreatePrError(error: unknown): string {
  const raw = extractCreatePrFailureMessage(error);
  const m = /No commits between [^ ]+ and ([^"\s]+)/i.exec(raw);
  return m?.[1] ? toStringTrim(m[1]).replace(/^refs\/heads\//, '') : '';
}

function isResourceAlreadyExistsDuringPrCreation(error: unknown): boolean {
  const msg = extractCreatePrFailureMessage(error);
  return /Resource ['"`][^'"`]+['"`] already exists at /i.test(msg);
}

async function registryResourceExistsOnDefaultBranch(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  template: TemplateLike,
  resourceName: string
): Promise<boolean> {
  const structRoot = resolveStructuredRootForTemplate(template);
  if (!structRoot || !resourceName) return false;

  for (const ext of ['yaml', 'yml']) {
    try {
      await context.octokit.repos.getContent({
        owner: params.owner,
        repo: params.repo,
        path: `${structRoot}/${resourceName}.${ext}`,
      });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) continue;
      throw e;
    }
  }

  return false;
}

async function deleteBranchRefIfPresent(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string
): Promise<void> {
  const branch = toStringTrim(branchName).replace(/^refs\/heads\//, '');
  if (!branch) return;

  try {
    await context.octokit.git.deleteRef({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      ref: `heads/${branch}`,
    });
  } catch (e: unknown) {
    if (getHttpStatus(e) !== 404) throw e;
  }
}

function formatCreateRequestFailureForUser(error: unknown, branchName = '', resourceName = ''): string {
  const msg = extractCreatePrFailureMessage(error);
  const parsedBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || toStringTrim(branchName);

  if (/^No commits between\b/i.test(msg)) {
    const suffix = parsedBranch ? ` '${parsedBranch}'` : '';
    return `Failed to create PR automatically: stale request branch${suffix} blocked PR creation. Please retry approval.`;
  }

  if (isResourceAlreadyExistsDuringPrCreation(error)) {
    const suffix = resourceName ? ` '${resourceName}'` : '';
    return `Failed to create PR automatically: a stale request branch already contains${suffix}. Please retry approval.`;
  }

  return `Failed to create PR automatically: ${msg}`;
}

async function runCreateRequestPr(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike
): Promise<{ number: number }> {
  return await createRequestPr(context, repoInfo, issue, parsedFormData, { template });
}

async function retryCreatePrAfterBranchCleanup(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike
): Promise<{ number: number }> {
  await deleteBranchRefIfPresent(context, repoInfo, branchName);
  return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template);
}

async function handleNoCommitsCreatePrFailure(
  context: BotContext<RequestEvents>,
  repoInfo: RepoInfo,
  branchName: string,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike,
  resourceName: string
): Promise<{ number: number }> {
  try {
    return await retryCreatePrAfterBranchCleanup(context, repoInfo, branchName, issue, parsedFormData, template);
  } catch (retryError: unknown) {
    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName));
  }
}

async function handleAlreadyExistsCreatePrFailure(
  context: BotContext<RequestEvents>,
  args: {
    params: IssueParams;
    repoInfo: RepoInfo;
    issue: IssueLike;
    parsedFormData: FormData;
    template: TemplateLike;
    resourceName: string;
    branchName: string;
  }
): Promise<{ number: number }> {
  const { params, repoInfo, issue, parsedFormData, template, resourceName, branchName } = args;

  try {
    const existsOnDefaultBranch = await registryResourceExistsOnDefaultBranch(context, params, template, resourceName);

    if (existsOnDefaultBranch) {
      throw new Error(`Failed to create PR automatically: Resource '${resourceName}' already exists in the registry.`);
    }

    return await retryCreatePrAfterBranchCleanup(context, repoInfo, branchName, issue, parsedFormData, template);
  } catch (retryError: unknown) {
    if (retryError instanceof Error && retryError.message.startsWith('Failed to create PR automatically:')) {
      throw retryError;
    }

    throw new Error(formatCreateRequestFailureForUser(retryError, branchName, resourceName));
  }
}

async function createRequestPrWithRecovery(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  parsedFormData: FormData,
  template: TemplateLike,
  resourceName: string
): Promise<{ number: number }> {
  const repoInfo: RepoInfo = { owner: params.owner, repo: params.repo };
  const fallbackBranchName = renderConfiguredRequestBranchName(context, issue, resourceName);

  try {
    return await runCreateRequestPr(context, repoInfo, issue, parsedFormData, template);
  } catch (error: unknown) {
    const staleNoCommitsBranch = parseNoCommitsHeadBranchFromCreatePrError(error) || fallbackBranchName;
    const failureMessage = extractCreatePrFailureMessage(error);

    if (/^No commits between\b/i.test(failureMessage)) {
      return await handleNoCommitsCreatePrFailure(
        context,
        repoInfo,
        staleNoCommitsBranch,
        issue,
        parsedFormData,
        template,
        resourceName
      );
    }

    if (isResourceAlreadyExistsDuringPrCreation(error)) {
      return await handleAlreadyExistsCreatePrFailure(context, {
        params,
        repoInfo,
        issue,
        parsedFormData,
        template,
        resourceName,
        branchName: fallbackBranchName,
      });
    }

    throw new Error(formatCreateRequestFailureForUser(error, staleNoCommitsBranch, resourceName));
  }
}

function isConfiguredApprover(login: string | undefined | null, allowedApprovers: string[]): boolean {
  const who = normalizeLogin(login).toLowerCase();
  if (!who) return false;

  return (allowedApprovers || []).some((u) => normalizeLogin(u).toLowerCase() === who);
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

  const snapshotHashes = buildCompatibleRequestSnapshotHashes(issue.body, parsedFormData, template);
  const currentHash =
    snapshotHashes[0] || calcSnapshotHash(parsedFormData, template, readIssueBodyForProcessing(issue.body));

  await normalizeIssueTitle(context, params, issue, template, parsedFormData);

  try {
    await closeOutdatedRequestPrs(context, params, template, {
      parsedFormData,
      currentHash,
      acceptedHashes: snapshotHashes,
    });
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

    await postOnce(
      context,
      params,
      buildDetectedIssuesBody(message, normalizeMachineReadableIssues(result.validationIssues || [])),
      {
        minimizeTag: 'nsreq:validation',
      }
    );
    await setStateLabel(context, params, issue, 'author');
    return;
  }

  try {
    const parentError = await checkParentChainExistsInFlatStructure(
      context,
      { owner: params.owner, repo: params.repo },
      template,
      parsedFormData,
      validatedNamespace
    );

    if (parentError) {
      await postOnce(
        context,
        params,
        buildDetectedIssuesBody(`- ${parentError}`, singleMachineReadableIssue('name', parentError)),
        {
          minimizeTag: 'nsreq:validation',
        }
      );
      await setStateLabel(context, params, issue, 'author');
      return;
    }
  } catch (e: unknown) {
    (app.log || console).warn?.({ err: e instanceof Error ? e.message : String(e) }, 'parent chain check failed');
  }

  const effectiveRequestType = resolveEffectiveRequestType(result.template || template, parsedFormData);

  const gated = await maybeRequireParentOwnerApproval(
    context,
    params,
    issue,
    result.template || template,
    validatedNamespace,
    effectiveRequestType
  );

  if (DBG) {
    log(
      context,
      'debug',
      { issue: issue.number, target: validatedNamespace, requestType: effectiveRequestType, gated },
      'parent-approval:gate-result'
    );
  }

  if (gated) return;

  const contactGated = await maybeRequireSystemContactOwnerApproval(
    context,
    params,
    issue,
    parsedFormData,
    effectiveRequestType,
    validatedNamespace
  );

  if (contactGated) return;

  const parentApprovedBy = getApprovedParentOwnerLogin(issue.body, validatedNamespace);
  if (isSubContextRequestType(effectiveRequestType) && parentApprovedBy) {
    const approvalOutcome = await maybeHandleApprovalDecision(
      context,
      params,
      issue,
      result.template || template,
      parsedFormData,
      effectiveRequestType,
      validatedNamespace
    );

    if (approvalOutcome !== 'continue') return;

    await finalizeApprovedRequest(context, params, issue, result.template || template, parsedFormData, {
      approvalPrefix: `Approved by parent namespace owner @${parentApprovedBy}`,
    });
    return;
  }

  const approvalOutcome = await maybeHandleApprovalDecision(
    context,
    params,
    issue,
    result.template || template,
    parsedFormData,
    effectiveRequestType,
    validatedNamespace
  );

  if (approvalOutcome !== 'continue') return;

  const manualApproversOverride = await resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    result.template || template,
    parsedFormData,
    effectiveRequestType
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await resolveAdditionalIssueApproversFromApprovalHook(
        context,
        params,
        issue,
        result.template || template,
        parsedFormData,
        effectiveRequestType
      );

  await handoverToCpa(context, params, issue, nsType, validatedNamespace, [], {
    snapshotHash: currentHash,
    requestType: effectiveRequestType,
    extraApprovers: hookApprovers,
    manualApproversOverride,
  });
}

async function resolveAdditionalIssueApproversFromApprovalHook(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  requestType?: string
): Promise<string[]> {
  const effectiveRequestType = requestType || resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const decision = normalizeApprovalDecision(
      await runApprovalHook(
        context,
        { owner: params.owner, repo: params.repo },
        {
          requestType: effectiveRequestType,
          namespace: resourceName,
          resourceName,
          formData: parsedFormData,
          issue,
          requestAuthorId: normalizeLogin(issue.user?.login),
        }
      )
    );

    return uniqLogins((decision.approvers || []).filter(Boolean));
  } catch {
    return [];
  }
}

async function resolveManualReviewApproverOverrideFromApprovalHook(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  requestType?: string
): Promise<string[]> {
  const effectiveRequestType = requestType || resolveEffectiveRequestType(template, parsedFormData);
  const resourceName = extractResourceNameFromForm(parsedFormData, template);

  if (!effectiveRequestType) return [];

  try {
    const decision = normalizeApprovalDecision(
      await runApprovalHook(
        context,
        { owner: params.owner, repo: params.repo },
        {
          requestType: effectiveRequestType,
          namespace: resourceName,
          resourceName,
          formData: parsedFormData,
          issue,
          requestAuthorId: normalizeLogin(issue.user?.login),
        }
      )
    );

    return getUnknownManualApprovers(decision);
  } catch {
    return [];
  }
}

async function handleApprovalComment(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<void> {
  const eff = resolveEffectiveConstants(context);
  const requestType = resolveEffectiveRequestType(template, parsedFormData);

  const configuredApprovers = resolveApproversForRequestType(
    context,
    requestType,
    eff.approverUsernames,
    eff.approverPoolUsernames
  );

  const reviewOk = await ensureReviewLabelsPresentOnIssue(context, params, issue, eff);
  if (!reviewOk) {
    await postOnce(
      context,
      params,
      'Approval ignored: request is not in review state. Please resolve validation issues and let the bot route it back to review first.',
      { minimizeTag: 'nsreq:approval-info' }
    );
    return;
  }

  let allowedApprovers = uniqLogins([...(configuredApprovers || [])]);
  let okApprover = isAuthorizedApprover(commenter, issue.user?.login, allowedApprovers);

  if (!okApprover) {
    const hookApprovers = await resolveAdditionalIssueApproversFromApprovalHook(
      context,
      params,
      issue,
      template,
      parsedFormData,
      requestType
    );

    allowedApprovers = uniqLogins([...(configuredApprovers || []), ...(hookApprovers || [])]);
    okApprover = isAuthorizedApprover(commenter, issue.user?.login, allowedApprovers);
  }
  if (!okApprover) {
    const hasConfiguredApprovers = allowedApprovers.length > 0;
    const reason = hasConfiguredApprovers
      ? `Approval ignored: commenter ${commenter} is not an allowed approver for this request type.`
      : `Approval ignored: commenter ${commenter} is not allowed to self-approve this request.`;

    await postOnce(context, params, reason, { minimizeTag: 'nsreq:approval-info' });
    return;
  }

  const reval = await validateRequestIssue(context, params, issue, {
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

    const normalizedIssues = (reval.validationIssues || []).map((issue) => ({
      field: toStringTrim(issue.path) || 'details',
      message: toStringTrim(issue.message),
    }));

    await postOnce(
      context,
      params,
      buildDetectedIssuesBody(message, normalizeMachineReadableIssues(normalizedIssues)),
      {
        minimizeTag: 'nsreq:validation',
      }
    );
    await setStateLabel(context, params, issue, 'author');
    return;
  }

  try {
    const parentError = await checkParentChainExistsInFlatStructure(
      context,
      { owner: params.owner, repo: params.repo },
      reval.template || template,
      parsedFormData,
      reval.namespace
    );

    if (parentError) {
      await postOnce(
        context,
        params,
        buildDetectedIssuesBody(`- ${parentError}`, singleMachineReadableIssue('name', parentError)),
        {
          minimizeTag: 'nsreq:validation',
        }
      );
      await setStateLabel(context, params, issue, 'author');
      return;
    }
  } catch (e: unknown) {
    log(
      context,
      'warn',
      { err: e instanceof Error ? e.message : String(e) },
      'parent chain check failed during approval'
    );
  }

  await finalizeApprovedRequest(context, params, issue, template, parsedFormData, {
    approvalPrefix: `Approved by @${commenter}`,
  });
}

async function handleAuthorUpdateComment(
  app: Probot,
  context: BotContext<RequestEvents>,
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
          parsedAfterUpdate,
          namespace
        );
        if (parentError) {
          await postOnce(
            context,
            params,
            buildDetectedIssuesBody(`- ${parentError}`, singleMachineReadableIssue('name', parentError)),
            {
              minimizeTag: 'nsreq:validation',
            }
          );
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

      const effectiveRequestType = resolveEffectiveRequestType(tpl, parsedAfterUpdate);

      const gated = await maybeRequireParentOwnerApproval(context, params, issue, tpl, namespace, effectiveRequestType);

      if (DBG) {
        log(
          context,
          'debug',
          { issue: issue.number, target: namespace, requestType: effectiveRequestType, gated },
          'parent-approval:gate-result(update)'
        );
      }

      if (gated) return;

      const contactGated = await maybeRequireSystemContactOwnerApproval(
        context,
        params,
        issue,
        parsedAfterUpdate,
        effectiveRequestType,
        namespace
      );

      if (contactGated) return;

      const parentApprovedBy = getApprovedParentOwnerLogin(issue.body, namespace);
      if (isSubContextRequestType(effectiveRequestType) && parentApprovedBy) {
        const approvalOutcome = await maybeHandleApprovalDecision(
          context,
          params,
          issue,
          tpl,
          parsedAfterUpdate,
          effectiveRequestType,
          namespace
        );

        if (approvalOutcome !== 'continue') return;

        await finalizeApprovedRequest(context, params, issue, tpl, parsedAfterUpdate, {
          approvalPrefix: `Approved by parent namespace owner @${parentApprovedBy}`,
        });
        return;
      }

      const approvalOutcome = await maybeHandleApprovalDecision(
        context,
        params,
        issue,
        tpl,
        parsedAfterUpdate,
        effectiveRequestType,
        namespace
      );

      if (approvalOutcome !== 'continue') return;

      const manualApproversOverride = await resolveManualReviewApproverOverrideFromApprovalHook(
        context,
        params,
        issue,
        tpl,
        parsedAfterUpdate,
        effectiveRequestType
      );

      const hookApprovers = manualApproversOverride.length
        ? []
        : await resolveAdditionalIssueApproversFromApprovalHook(
            context,
            params,
            issue,
            tpl,
            parsedAfterUpdate,
            effectiveRequestType
          );

      await handoverToCpa(context, params, issue, nsType, namespace, [], {
        snapshotHash,
        requestType: effectiveRequestType,
        extraApprovers: hookApprovers,
        manualApproversOverride,
      });
      return;
    }

    const listFallback = (revalErrors || []).map((e) => `- ${e}`).join('\n');
    const message =
      revalErrorsFormattedSingle?.trim() || revalErrorsFormatted?.trim() || listFallback || 'Unknown validation error.';
    await postOnce(
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
    await setStateLabel(context, params, issue, 'author');
  } catch (e: unknown) {
    (app.log || console).warn?.(`Revalidation failed: ${e instanceof Error ? e.message : String(e)}`);
  }
}

function resolveVendorRegistryRootForRequestHandler(context: BotContext<RequestEvents>): string {
  const cfg: NormalizedStaticConfig = context.resourceBotConfig ?? DEFAULT_CONFIG;
  const reqs = isPlainObject(cfg.requests) ? cfg.requests : {};
  const vendorEntry = isPlainObject(reqs['vendor']) ? reqs['vendor'] : null;
  const vendorRoot = normalizeRepoPath(vendorEntry ? vendorEntry['folderName'] : '').replace(/\/+$/, '');
  return vendorRoot || 'data/vendors';
}

async function repoYamlExists(context: BotContext<RequestEvents>, repo: RepoInfo, basePath: string): Promise<boolean> {
  for (const ext of ['yaml', 'yml']) {
    try {
      await context.octokit.repos.getContent({
        owner: repo.owner,
        repo: repo.repo,
        path: `${basePath}.${ext}`,
      });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) !== 404) throw e;
    }
  }

  return false;
}

async function checkParentChainExistsInFlatStructure(
  context: BotContext<RequestEvents>,
  { owner, repo }: RepoInfo,
  template: TemplateLike,
  formData: FormData,
  explicitResourceName?: string
): Promise<string | null> {
  const rootRaw = toStringTrim(template?._meta?.root);
  const structRoot = rootRaw.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!structRoot) return null;

  const rt = toStringTrim(template?._meta?.requestType).toLowerCase();
  const isNamespaceLike = rt.includes('namespace') || rt === 'subcontext' || rt === 'system' || rt === 'authority';
  if (!isNamespaceLike) return null;

  const resourceName = toStringTrim(explicitResourceName) || extractResourceNameFromForm(formData, template);
  const parts = toStringTrim(resourceName).split('.').filter(Boolean);
  if (parts.length < 2) return null;

  const repoInfo: RepoInfo = { owner, repo };
  const vendorRoot = resolveVendorRegistryRootForRequestHandler(context);

  for (let i = parts.length - 1; i >= 1; i -= 1) {
    const parentName = parts.slice(0, i).join('.');
    if (!parentName) continue;

    const exists =
      i === 1
        ? await repoYamlExists(context, repoInfo, `${vendorRoot}/${parentName}`)
        : await repoYamlExists(context, repoInfo, `${structRoot}/${parentName}`);

    if (exists) continue;

    return i === 1
      ? `Vendor '${parentName}' is not present. Please register the vendor first.`
      : `Parent resource '${parentName}' is not present. Please register the parent first.`;
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

const PARENT_APPROVAL_READ_RE = /<!--\s*nsreq:parent-approval\s*=\s*({[\s\S]*?})\s*-->/i;
const PARENT_APPROVAL_STRIP_RE = /<!--\s*nsreq:parent-approval\s*=\s*{[\s\S]*?}\s*-->\s*/gi;

type ParentApprovalMeta = {
  v: 1;
  parent: string;
  target: string;
  owners: string[];
  approvedBy?: string;
  approvedAt?: string;
};

type ContactApprovalMeta = {
  v: 1;
  target: string;
  owners: string[];
  approvedBy?: string;
  approvedAt?: string;
};

const CONTACT_APPROVAL_READ_RE = /<!--\s*nsreq:contact-approval\s*=\s*({[\s\S]*?})\s*-->/i;
const CONTACT_APPROVAL_STRIP_RE = /<!--\s*nsreq:contact-approval\s*=\s*{[\s\S]*?}\s*-->\s*/gi;

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

function isSubContextRequestType(requestType: unknown): boolean {
  const rt = normalizeTypeToken(requestType);
  return rt === 'subcontextnamespace' || rt === 'subcontext';
}

function getApprovedParentOwnerLogin(issueBody: unknown, target: string): string {
  const meta = readParentApprovalMeta(issueBody);
  if (!meta) return '';

  const approvedBy = normalizeLogin(meta.approvedBy);
  if (!approvedBy) return '';

  return normalizeKey(meta.target) === normalizeKey(target) ? approvedBy : '';
}

function stripContactApprovalFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(CONTACT_APPROVAL_STRIP_RE, '').trimEnd();
}

function readContactApprovalMeta(issueBody: unknown): ContactApprovalMeta | null {
  const body = String(issueBody || '');
  const m = body.match(CONTACT_APPROVAL_READ_RE);
  if (!m) return null;

  try {
    const raw = JSON.parse(String(m[1] || ''));
    if (!isPlainObject(raw)) return null;
    if (raw['v'] !== 1) return null;

    const target = toStringTrim(raw['target']);
    const ownersRaw = raw['owners'];
    const owners = Array.isArray(ownersRaw) ? uniqLogins(ownersRaw.map(toStringTrim).filter(Boolean)) : [];
    const approvedBy = normalizeLogin(raw['approvedBy']);
    const approvedAt = toStringTrim(raw['approvedAt']);

    if (!target || !owners.length) return null;

    const out: ContactApprovalMeta = { v: 1, target, owners };
    if (approvedBy) out.approvedBy = approvedBy;
    if (approvedAt) out.approvedAt = approvedAt;
    return out;
  } catch {
    return null;
  }
}

async function ensureContactApprovalMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  meta: ContactApprovalMeta | null
): Promise<boolean> {
  const current = readContactApprovalMeta(issue.body);
  const cleaned = stripContactApprovalFromBody(issue.body);

  if (!meta) {
    if (!current) return false;

    try {
      const nextBody = `${cleaned}\n`;
      await context.octokit.issues.update({ ...params, body: nextBody });
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
    normalizeKey(current.target) === normalizeKey(next.target) &&
    sameNormalizedLoginSet(current.owners, next.owners) &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const metaStr = JSON.stringify(next);
  const nextBody = `${cleaned}\n\n<!-- nsreq:contact-approval = ${metaStr} -->\n`;

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

async function resolveRequestContactOwnerLogins(
  context: BotContext<RequestEvents>,
  formData: FormData
): Promise<string[]> {
  const contacts = formData['contact'] ?? formData['contacts'] ?? '';
  const { logins: directLogins, emails } = extractParentContactCandidates(contacts);

  const resolved: string[] = [...directLogins];
  for (const email of emails.slice(0, 10)) {
    resolved.push(...(await lookupGithubLoginsByEmail(context, email)));
  }

  return uniqLogins(resolved);
}

async function maybeRequireSystemContactOwnerApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  parsedFormData: FormData,
  requestType: string,
  validatedNamespace: string
): Promise<boolean> {
  if (normalizeTypeToken(requestType) !== 'systemnamespace') {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const owners = await resolveRequestContactOwnerLogins(context, parsedFormData);
  const requester = normalizeLogin(issue.user?.login);

  if (!target || !owners.length) {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  if (requester && owners.some((owner) => owner.toLowerCase() === requester.toLowerCase())) {
    await ensureContactApprovalMarker(context, params, issue, null);
    return false;
  }

  const current = readContactApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    normalizeKey(current.target) === normalizeKey(target) &&
    sameNormalizedLoginSet(current.owners, owners) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) return false;

  await ensureContactApprovalMarker(context, params, issue, { v: 1, target, owners });

  const mentions = owners.map((owner) => `@${owner}`).join(' ');
  const tag = `nsreq:contact-approval:${normalizeKey(target)}`;

  await postOnce(
    context,
    params,
    `### 🔒 Contact owner approval required

Requester @${requester || 'unknown'} is not listed in the contact owners for \`${target}\`.

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  await setStateLabel(context, params, issue, 'author');
  return true;
}

async function handleSystemContactOwnerApprovalIfNeeded(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<boolean> {
  const meta = readContactApprovalMeta(issue.body);
  if (!meta) return false;
  if (normalizeLogin(meta.approvedBy)) return false;

  const commenterLogin = normalizeLogin(commenter);
  const owners = uniqLogins(meta.owners || []);
  const isOwner = owners.some((owner) => owner.toLowerCase() === commenterLogin.toLowerCase());
  const tagBase = `nsreq:contact-approval:${normalizeKey(meta.target)}`;

  if (!isOwner) {
    const mentions = owners.map((owner) => `@${owner}`).join(' ');
    await postOnce(
      context,
      params,
      `Approval ignored: this request requires contact owner approval for \`${meta.target}\` first.

${mentions}`,
      { minimizeTag: `${tagBase}:pending` }
    );
    return true;
  }

  const reval = await validateRequestIssue(context, params, issue, {
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

    await postOnce(
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
      { minimizeTag: 'nsreq:validation' }
    );
    await setStateLabel(context, params, issue, 'author');
    return true;
  }

  const tpl = reval.template || template;
  const bodyStr = readIssueBodyForProcessing(issue.body);
  const parsedNow = parseForm(bodyStr, tpl);
  const snapshotHash = calcSnapshotHash(parsedNow, tpl, bodyStr);
  const effRt = resolveEffectiveRequestType(tpl, parsedNow);

  await ensureContactApprovalMarker(context, params, issue, {
    v: 1,
    target: meta.target,
    owners,
    approvedBy: commenterLogin,
    approvedAt: new Date().toISOString(),
  });

  const approvalOutcome = await maybeHandleApprovalDecision(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt,
    reval.namespace
  );

  if (approvalOutcome !== 'continue') return true;

  await postOnce(context, params, `Contact owner approved by @${commenterLogin}. Continuing with standard review.`, {
    minimizeTag: `${tagBase}:approved`,
  });

  const manualApproversOverride = await resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await resolveAdditionalIssueApproversFromApprovalHook(context, params, issue, tpl, parsedNow, effRt);

  await handoverToCpa(context, params, issue, reval.nsType, reval.namespace, [], {
    snapshotHash,
    requestType: effRt,
    extraApprovers: hookApprovers,
    manualApproversOverride,
  });

  return true;
}

function stripParentApprovalFromBody(issueBody: unknown): string {
  const body = String(issueBody || '');
  return body.replace(PARENT_APPROVAL_STRIP_RE, '').trimEnd();
}

function readParentApprovalMeta(issueBody: unknown): ParentApprovalMeta | null {
  const body = String(issueBody || '');
  const m = body.match(PARENT_APPROVAL_READ_RE);
  if (!m) return null;

  try {
    const raw = JSON.parse(String(m[1] || ''));
    if (!isPlainObject(raw)) return null;
    if (raw['v'] !== 1) return null;

    const parent = toStringTrim(raw['parent']);
    const target = toStringTrim(raw['target']);
    const ownersRaw = raw['owners'];
    const owners = Array.isArray(ownersRaw) ? uniqLogins(ownersRaw.map(toStringTrim).filter(Boolean)) : [];

    const approvedBy = normalizeLogin(raw['approvedBy']);
    const approvedAt = toStringTrim(raw['approvedAt']);

    if (!parent || !target) return null;

    const out: ParentApprovalMeta = { v: 1, parent, target, owners };
    if (approvedBy) out.approvedBy = approvedBy;
    if (approvedAt) out.approvedAt = approvedAt;
    return out;
  } catch {
    return null;
  }
}

async function ensureParentApprovalMarker(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  meta: ParentApprovalMeta | null
): Promise<boolean> {
  const current = readParentApprovalMeta(issue.body);
  const cleaned = stripParentApprovalFromBody(issue.body);

  if (!meta) {
    if (!current) return false;
    try {
      const nextBody = `${cleaned}\n`;
      await context.octokit.issues.update({ ...params, body: nextBody });
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

  const ab = normalizeLogin(meta.approvedBy);
  const at = toStringTrim(meta.approvedAt);
  if (ab) next.approvedBy = ab;
  if (at) next.approvedAt = at;

  if (!next.parent || !next.target) return false;

  const same =
    current &&
    normalizeKey(current.parent) === normalizeKey(next.parent) &&
    normalizeKey(current.target) === normalizeKey(next.target) &&
    uniqLogins(current.owners).join('|').toLowerCase() === uniqLogins(next.owners).join('|').toLowerCase() &&
    normalizeLogin(current.approvedBy) === normalizeLogin(next.approvedBy) &&
    toStringTrim(current.approvedAt) === toStringTrim(next.approvedAt);

  if (same) return false;

  const metaStr = JSON.stringify(next);
  const nextBody = `${cleaned}\n\n<!-- nsreq:parent-approval = ${metaStr} -->\n`;

  try {
    await context.octokit.issues.update({ ...params, body: nextBody });
    issue.body = nextBody;
    return true;
  } catch {
    return false;
  }
}

async function maybeRequireParentOwnerApproval(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  validatedNamespace: string,
  requestType: string
): Promise<boolean> {
  const rt = toStringTrim(requestType).toLowerCase();
  if (!rt.includes('namespace')) {
    await ensureParentApprovalMarker(context, params, issue, null);
    return false;
  }

  const target = toStringTrim(validatedNamespace);
  const parts = target
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean);

  if (parts.length <= 2) {
    await ensureParentApprovalMarker(context, params, issue, null);
    return false;
  }

  const requester = normalizeLogin(issue.user?.login);

  const { parent, owners } = await resolveParentOwnerLoginsForTarget(context, params, template, target, requestType);

  if (!parent || owners.length === 0) {
    await ensureParentApprovalMarker(context, params, issue, null);
    return false;
  }

  if (requester && owners.some((o) => o.toLowerCase() === requester.toLowerCase())) {
    if (DBG) {
      log(
        context,
        'debug',
        { issue: issue.number, requester, parent, target, owners },
        'parent-approval:skip (requester is parent owner)'
      );
    }
    await ensureParentApprovalMarker(context, params, issue, null);
    return false;
  }

  const current = readParentApprovalMeta(issue.body);
  const alreadyApproved =
    current &&
    normalizeKey(current.parent) === normalizeKey(parent) &&
    normalizeKey(current.target) === normalizeKey(target) &&
    Boolean(normalizeLogin(current.approvedBy));

  if (alreadyApproved) return false;

  await ensureParentApprovalMarker(context, params, issue, { v: 1, parent, target, owners });

  const mentions = owners.map((o) => `@${o}`).join(' ');
  const tag = `nsreq:parent-approval:${normalizeKey(parent)}:${normalizeKey(target)}`;

  await postOnce(
    context,
    params,
    `### 🔒 Parent owner approval required

Sub-namespace request under \`${parent}\` (target: \`${target}\`).

${mentions}

Please confirm by commenting \`Approved\`. After that, the bot will continue with the standard review workflow.`,
    { minimizeTag: tag }
  );

  await setStateLabel(context, params, issue, 'author');
  return true;
}

async function handleParentOwnerApprovalIfNeeded(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  template: TemplateLike,
  parsedFormData: FormData,
  commenter: string
): Promise<boolean> {
  const meta = readParentApprovalMeta(issue.body);
  if (!meta) return false;
  if (normalizeLogin(meta.approvedBy)) return false;

  const commenterLogin = normalizeLogin(commenter);
  const owners = uniqLogins(meta.owners || []);
  const isOwner = owners.some((o) => o.toLowerCase() === commenterLogin.toLowerCase());

  const tagBase = `nsreq:parent-approval:${normalizeKey(meta.parent)}:${normalizeKey(meta.target)}`;

  if (!isOwner) {
    const mentions = owners.map((o) => `@${o}`).join(' ');
    await postOnce(
      context,
      params,
      `Approval ignored: this request requires parent owner approval for \`${meta.parent}\` first.

${mentions}`,
      { minimizeTag: `${tagBase}:pending` }
    );
    return true;
  }

  const reval = await validateRequestIssue(context, params, issue, { template, formData: parsedFormData });
  if (reval.errors?.length) {
    const listFallback = (reval.errors || []).map((e) => `- ${e}`).join('\n');
    const message =
      reval.errorsFormattedSingle?.trim() ||
      reval.errorsFormatted?.trim() ||
      listFallback ||
      'Unknown validation error.';
    await postOnce(
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
    await setStateLabel(context, params, issue, 'author');
    return true;
  }

  const tpl = reval.template || template;
  const bodyStr = readIssueBodyForProcessing(issue.body);
  const parsedNow = parseForm(bodyStr, tpl);
  const snapshotHash = calcSnapshotHash(parsedNow, tpl, bodyStr);
  const effRt = resolveEffectiveRequestType(tpl, parsedNow);

  await ensureParentApprovalMarker(context, params, issue, {
    v: 1,
    parent: meta.parent,
    target: meta.target,
    owners,
    approvedBy: commenterLogin,
    approvedAt: new Date().toISOString(),
  });

  const approvalOutcome = await maybeHandleApprovalDecision(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt,
    reval.namespace
  );

  if (approvalOutcome !== 'continue') return true;

  if (isSubContextRequestType(effRt)) {
    await finalizeApprovedRequest(context, params, issue, tpl, parsedNow, {
      approvalPrefix: `Approved by parent namespace owner @${commenterLogin}`,
    });
    return true;
  }

  await postOnce(context, params, `Parent namespace approved by @${commenterLogin}. Continuing with standard review.`, {
    minimizeTag: `${tagBase}:approved`,
  });

  const manualApproversOverride = await resolveManualReviewApproverOverrideFromApprovalHook(
    context,
    params,
    issue,
    tpl,
    parsedNow,
    effRt
  );

  const hookApprovers = manualApproversOverride.length
    ? []
    : await resolveAdditionalIssueApproversFromApprovalHook(context, params, issue, tpl, parsedNow, effRt);

  await handoverToCpa(context, params, issue, reval.nsType, reval.namespace, [], {
    snapshotHash,
    requestType: effRt,
    extraApprovers: hookApprovers,
    manualApproversOverride,
  });

  return true;
}

const ROUTING_LOCK_NOTICE_INFLIGHT = new Map<string, Promise<void>>();

function routingNoticeKey(params: IssueParams): string {
  return `${params.owner}/${params.repo}#${params.issue_number}`;
}

async function postRoutingLockNoticeOnce(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  expected: string
): Promise<void> {
  const key = routingNoticeKey(params);
  const existing = ROUTING_LOCK_NOTICE_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const p = Promise.resolve()
    .then(async (): Promise<void> => {
      await postOnce(context, params, `Routing label is locked to "${expected}". Manual changes were reverted.`, {
        minimizeTag: 'nsreq:routing-label-lock',
      });
    })
    .finally(() => {
      ROUTING_LOCK_NOTICE_INFLIGHT.delete(key);
    });

  ROUTING_LOCK_NOTICE_INFLIGHT.set(key, p);
  await p;
}

async function isRoutingLabelName(
  context: BotContext<RequestEvents>,
  params: IssueParams,
  issue: IssueLike,
  labelName: unknown
): Promise<boolean> {
  const name = toStringTrim(labelName);
  if (!name) return false;
  try {
    return Boolean(await tryLoadTemplateForLabels(context, params, issue, [name]));
  } catch {
    return false;
  }
}

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
  return toStringTrim(stripContactApprovalFromBody(stripParentApprovalFromBody(stripRoutingLockFromBody(issueBody))));
}

function buildCompatibleRequestSnapshotHashes(
  issueBody: unknown,
  parsedFormData: FormData,
  template: TemplateLike
): string[] {
  const processedBody = readIssueBodyForProcessing(issueBody);
  const rawBody = String(issueBody || '');

  return Array.from(
    new Set(
      [calcSnapshotHash(parsedFormData, template, processedBody), calcSnapshotHash(parsedFormData, template, rawBody)]
        .map((value) => toStringTrim(value))
        .filter(Boolean)
    )
  );
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
  expectedLabel: string,
  opts?: { changedLabel?: string }
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
    const touchedLabel = toStringTrim(opts?.changedLabel);

    // Only notify on routing-label events - avoid spamming on unrelated label changes.
    const shouldNotify =
      !touchedLabel ||
      normalizeKey(touchedLabel) === expectedKey ||
      (await isRoutingLabelName(context, params, issue, touchedLabel));

    if (shouldNotify) {
      await postRoutingLockNoticeOnce(context, params, expected);
    }
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
  options: { parsedFormData?: FormData; currentHash?: string; acceptedHashes?: string[] } = {}
): Promise<void> {
  const ensureFormAndHash = async (): Promise<{
    parsedFormData: FormData;
    currentHash: string;
    acceptedHashes: string[];
  }> => {
    const { parsedFormData: givenForm, currentHash: givenHash, acceptedHashes: givenAcceptedHashes } = options;

    if (givenForm && givenHash) {
      const acceptedHashes = Array.from(
        new Set((givenAcceptedHashes || [givenHash]).map((value) => toStringTrim(value)).filter(Boolean))
      );

      return {
        parsedFormData: givenForm,
        currentHash: givenHash,
        acceptedHashes: acceptedHashes.length ? acceptedHashes : [givenHash],
      };
    }

    const { data } = await context.octokit.issues.get({ owner, repo, issue_number });
    const issue = data as unknown as IssueLike;
    const bodyStr = readIssueBodyForProcessing(issue.body);
    const form = parseForm(bodyStr, template);
    const acceptedHashes = buildCompatibleRequestSnapshotHashes(issue.body, form, template);
    const currentHash = acceptedHashes[0] || calcSnapshotHash(form, template, bodyStr);

    return {
      parsedFormData: form,
      currentHash,
      acceptedHashes,
    };
  };

  const closePr = async (prNum: number, ref: string): Promise<void> => {
    try {
      await context.octokit.pulls.update({ owner, repo, pull_number: prNum, state: 'closed' });
    } catch {
      /* empty */
    }
    try {
      await context.octokit.git.deleteRef({ owner, repo, ref: `heads/${ref}` });
    } catch {
      /* empty */
    }
  };

  const { currentHash, acceptedHashes } = await ensureFormAndHash();
  const acceptedHashSet = new Set((acceptedHashes || []).map((value) => toStringTrim(value)).filter(Boolean));

  if (currentHash) acceptedHashSet.add(currentHash);

  const prs = await findOpenIssuePrs(context, { owner, repo }, issue_number);
  if (!prs.length) return;

  const eff = resolveEffectiveConstants(context);
  const onApproved = eff.labelOnApproved;
  const closed: number[] = [];

  for (const pr of prs) {
    const prHash = extractHashFromPrBody(toStringTrim(pr.body));

    if (!prHash) continue;
    if (acceptedHashSet.has(prHash)) continue;

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
    /* empty */
  }
}

function readRepoInfoFromPayload(payload: unknown): RepoInfo | null {
  if (!isPlainObject(payload)) return null;

  const repoObj = payload['repository'];
  if (!isPlainObject(repoObj)) return null;

  const repoName = toStringTrim(repoObj['name']);
  const ownerObj = isPlainObject(repoObj['owner']) ? repoObj['owner'] : null;
  const ownerLogin = ownerObj ? toStringTrim(ownerObj['login']) : '';

  if (!ownerLogin || !repoName) return null;

  return { owner: ownerLogin, repo: repoName };
}

export default function requestHandler(app: Probot): void {
  const getStaticConfig = async (
    context: BotContext<RequestEvents>,
    options: StaticConfigLoadOptions = {}
  ): Promise<NormalizedStaticConfig> => {
    const forceReload = options.forceReload === true;

    if (!forceReload && context.resourceBotConfig && context.resourceBotHooks !== undefined) {
      return context.resourceBotConfig;
    }

    try {
      const { config, hooks, hooksSource } = await loadStaticConfig(context, {
        validate: false,
        updateIssue: false,
        forceReload,
      });

      context.resourceBotConfig = config;
      context.resourceBotHooks = hooks;
      context.resourceBotHooksSource = hooksSource || null;

      log(
        context,
        'info',
        {
          forceReload,
          hooksSource: context.resourceBotHooksSource,
        },
        'static-config:context-loaded'
      );

      return context.resourceBotConfig;
    } catch (err: unknown) {
      (app.log || console).warn?.(
        {
          err: err instanceof Error ? err.message : String(err),
          forceReload,
        },
        'failed to load resource-bot static config, using defaults'
      );

      context.resourceBotConfig = DEFAULT_CONFIG;
      context.resourceBotHooks = null;
      context.resourceBotHooksSource = null;

      return context.resourceBotConfig;
    }
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
    else if (approvalSuccessful !== undefined && approvalSuccessful !== null) {
      approvalKeyword = toStringTrim(approvalSuccessful);
    }

    return isExplicitApprovalCommand(strippedText, approvalKeyword);
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
        const enforced = await enforceRoutingLabelLock(context, params, issue, expectedRouting, { changedLabel });
        if (enforced) {
          try {
            labels = await fetchIssueLabels(context, params);
          } catch {
            // ignore
          }
        }
      }

      // 2) Load template
      let template: TemplateLike | null = null;
      let parsedFormData: FormData = {};

      try {
        template = await loadTemplateWithLabelRefresh(context, params, issue);
        parsedFormData = template ? parseForm(readIssueBodyForProcessing(issue.body), template) : {};
      } catch {
        if (!hasRoutingLock) return;
      }

      if (!hasRoutingLock && !isRequestIssue(context, template, parsedFormData)) return;

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

      const effectiveRequestType = template ? resolveEffectiveRequestType(template, parsedFormData) : '';
      const approverRouting = effectiveRequestType
        ? resolveApproverRoutingForRequestType(
            context,
            effectiveRequestType,
            eff.approverUsernames,
            eff.approverPoolUsernames
          )
        : {
            approvalUsernames: uniqLogins([...(eff.approverUsernames || []), ...(eff.approverPoolUsernames || [])]),
            autoAssigneePoolUsernames: uniqLogins(eff.approverPoolUsernames || []),
          };

      const senderIsConfiguredApprover = isConfiguredApprover(sender?.login, approverRouting.approvalUsernames);

      const managedWorkflowKeys = new Set<string>(Array.from(lockedKeys));
      for (const label of [authorActionLabel, approverActionLabel, approvedLabel, REQUEST_STATUS_LABEL_REJECTED]) {
        const key = normalizeKey(label);
        if (key) managedWorkflowKeys.add(key);
      }

      // Configured approvers may manage workflow labels manually
      // Keep routing-label lock logic above intact
      if (senderIsConfiguredApprover && changedKey && managedWorkflowKeys.has(changedKey)) {
        return;
      }

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
      const repoInfo: RepoInfo = { owner, repo };

      const stripped = stripQuoteAndCode(comment.body || '');
      const isApproval = isApprovalComment(context, stripped);

      if (!process.env.JEST_WORKER_ID && !hasIssueFormInputs(issue)) {
        const isPullRequestConversation = isPlainObject((issue as Record<string, unknown>)['pull_request']);

        if (isPullRequestConversation && isApproval) {
          const pr = await readFreshPullRequest(context, repoInfo, issueNumber);
          if (pr && parseLinkedIssueNumberFromPr(pr, repoInfo) === null) {
            await handleDirectPrApprovalComment(context, repoInfo, pr, commenter);
          }
        }

        return;
      }

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

      if (isApproval) {
        const handled = await handleParentOwnerApprovalIfNeeded(
          context,
          params,
          issue,
          template,
          parsedFormData,
          commenter
        );
        if (handled) return;

        const contactHandled = await handleSystemContactOwnerApprovalIfNeeded(
          context,
          params,
          issue,
          template,
          parsedFormData,
          commenter
        );
        if (contactHandled) return;

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

  const runAutoMergeEvaluation = async (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    normalizedHeadSha: string
  ): Promise<void> => {
    await getStaticConfig(context);

    const greenResult = await evaluateHeadGreenForApprovalReevaluation(context, repoInfo, normalizedHeadSha);

    log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: normalizedHeadSha,
        green: greenResult.green,
        greenReason: greenResult.reason,
        statusState: greenResult.statusState,
        blockingRuns: greenResult.blockingRuns,
        latestRuns: greenResult.latestRuns.slice(0, 30),
      },
      'auto-merge:head-green'
    );

    if (!greenResult.green) return;

    const candidates = (await listOpenPullRequests(context, repoInfo)).filter(
      (pr) => toStringTrim(pr.head?.sha) === normalizedHeadSha
    );

    log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        headSha: normalizedHeadSha,
        candidatePrNumbers: candidates.map((pr) => pr.number),
      },
      'auto-merge:candidates'
    );

    for (const pr of candidates) {
      try {
        await processPullRequestForAutoMerge(context, repoInfo, pr);
        await releaseSequentialRegistryPrIfNotApprovedAfterGreen(context, repoInfo, pr);
        await advanceSequentialRegistryPrQueueAfterTerminalState(
          context,
          repoInfo,
          pr,
          'sequential-direct-pr:advance-after-terminal-state'
        );
      } catch (e: unknown) {
        log(
          context,
          'warn',
          {
            err: e instanceof Error ? e.message : String(e),
            prNumber: pr.number,
          },
          'auto-merge candidate processing failed'
        );

        const freshPr = (await readFreshPullRequest(context, repoInfo, pr.number)) || pr;
        const baseBranch = toStringTrim(freshPr.base?.ref) || toStringTrim(pr.base?.ref);
        const isSequentialDirectRegistry = baseBranch
          ? await isSequentialDirectRegistryPr(context, repoInfo, freshPr, baseBranch)
          : false;

        if (!isSequentialDirectRegistry) {
          continue;
        }

        const active = getSequentialRegistryPrActive(repoInfo);
        const wasActiveSequentialPr = active?.prNumber === freshPr.number || active?.prNumber === pr.number;

        markSequentialRegistryPrHeadSkipped(context, repoInfo, freshPr, 'auto-merge-candidate-processing-failed');

        if (wasActiveSequentialPr) {
          clearSequentialRegistryPrActive(repoInfo);

          if (baseBranch) {
            await runOneSequentialDirectRegistryPrMaintenance(
              context,
              repoInfo,
              baseBranch,
              'sequential-direct-pr:advance-after-processing-failure'
            );
          }
        }
      }
    }
  };

  const tryAutoMerge = async (
    context: BotContext<RequestEvents>,
    repoInfo: RepoInfo,
    headSha: string
  ): Promise<void> => {
    const normalizedHeadSha = toStringTrim(headSha);
    if (!normalizedHeadSha) {
      log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
        },
        'auto-merge:skip-missing-head-sha'
      );
      return;
    }

    const key = `${repoInfo.owner}/${repoInfo.repo}:${normalizedHeadSha}:auto-merge-evaluation`.toLowerCase();

    const existing = AUTO_MERGE_EVALUATION_INFLIGHT.get(key);
    if (existing) {
      log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          headSha: normalizedHeadSha,
        },
        'auto-merge:evaluation deduped: already in flight'
      );

      await existing;
      return;
    }

    if (isAutoMergeEvaluationRecentlyCompleted(key)) {
      log(
        context,
        'info',
        {
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          headSha: normalizedHeadSha,
        },
        'auto-merge:evaluation skipped: recently completed'
      );

      return;
    }

    const pending = runAutoMergeEvaluation(context, repoInfo, normalizedHeadSha).finally(() => {
      AUTO_MERGE_EVALUATION_INFLIGHT.delete(key);
      markAutoMergeEvaluationRecentlyCompleted(key);
    });

    AUTO_MERGE_EVALUATION_INFLIGHT.set(key, pending);
    await pending;
  };

  const maybeHandleDefaultBranchCheckSuiteSuccess = async (
    context: BotContext<RequestEvents>,
    payload: unknown,
    checkSuite: CheckSuiteLike | null,
    repoInfo: RepoInfo
  ): Promise<void> => {
    const defaultBranch = readDefaultBranchFromPayload(payload);
    const headBranch = toStringTrim(checkSuite?.head_branch);
    const headSha = toStringTrim(checkSuite?.head_sha);
    const conclusion = toStringTrim(checkSuite?.conclusion).toLowerCase();
    const status = toStringTrim(checkSuite?.status).toLowerCase();

    let isDefaultBranchSuite = Boolean(defaultBranch && headBranch && headBranch === defaultBranch);
    let defaultBranchHeadSha = '';

    if (!isDefaultBranchSuite && defaultBranch && headSha) {
      try {
        const branch = await context.octokit.repos.getBranch({
          owner: repoInfo.owner,
          repo: repoInfo.repo,
          branch: defaultBranch,
        });

        defaultBranchHeadSha = toStringTrim(
          (branch as unknown as { data?: { commit?: { sha?: string | null } } })?.data?.commit?.sha
        );

        isDefaultBranchSuite = Boolean(defaultBranchHeadSha && defaultBranchHeadSha === headSha);
      } catch (error: unknown) {
        log(
          context,
          'warn',
          {
            owner: repoInfo.owner,
            repo: repoInfo.repo,
            defaultBranch,
            headSha,
            err: getErrorMessage(error),
            status: getHttpStatus(error),
          },
          'default-branch-check-suite:branch-head-read-failed'
        );
      }
    }

    log(
      context,
      'info',
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        defaultBranch,
        headBranch,
        headSha,
        defaultBranchHeadSha,
        conclusion,
        status,
        isDefaultBranchSuite,
      },
      'default-branch-check-suite:evaluated'
    );

    if (!isDefaultBranchSuite) return;
    if (status && status !== 'completed') return;
    if (conclusion !== 'success') return;

    await getStaticConfig(context, { forceReload: true });

    const directResult = await reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
      context,
      repoInfo,
      defaultBranch,
      'default-branch-check-suite:direct-pr-reevaluation'
    );

    if (!directResult.updated && !directResult.processed && !directResult.blockedByActive) {
      await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(context, repoInfo, defaultBranch);
    }
  };

  app.on('push', async (context: BotContext<'push'>): Promise<void> => {
    const payload = context.payload as unknown;
    const repoInfo = readRepoInfoFromPayload(payload);
    const ref = isPlainObject(payload) ? toStringTrim(payload['ref']) : '';
    const baseBranch = readDefaultBranchFromPush(payload);
    const changedFiles = readPushChangedFiles(payload);
    const approvalConfigChangedFiles = changedFiles.filter(isApprovalConfigChangePath);
    const defaultBranchPush = isDefaultBranchPush(payload);

    log(
      context,
      'info',
      {
        event: toStringTrim((context as unknown as { name?: string }).name),
        ref,
        defaultBranch: baseBranch,
        isDefaultBranchPush: defaultBranchPush,
        owner: repoInfo?.owner,
        repo: repoInfo?.repo,
        changedFilesCount: changedFiles.length,
        approvalConfigChangedFiles,
      },
      'default-branch-push:received'
    );

    if (!defaultBranchPush) return;

    if (!repoInfo) {
      log(
        context,
        'warn',
        {
          ref,
          defaultBranch: baseBranch,
        },
        'default-branch-push:missing-repo-info'
      );
      return;
    }

    await getStaticConfig(context, { forceReload: true });

    const directPrReevaluationReason = approvalConfigChangedFiles.length
      ? 'default-branch-push:approval-config-change'
      : 'default-branch-push:direct-pr-reevaluation';

    const directResult = await reevaluateOpenDirectPullRequestsAfterDefaultBranchPush(
      context,
      repoInfo,
      baseBranch,
      directPrReevaluationReason
    );

    if (!directResult.updated && !directResult.processed && !directResult.blockedByActive) {
      await updateApprovedOpenPullRequestBranchesAfterDefaultBranchPushWithRetry(context, repoInfo, baseBranch);
    }
  });

  app.on(
    ['check_suite.completed', 'check_run.completed'],
    async (context: BotContext<'check_suite.completed' | 'check_run.completed'>): Promise<void> => {
      const payload = context.payload as unknown;
      const action = isPlainObject(payload) ? toStringTrim(payload['action']).toLowerCase() : '';
      const eventName = toStringTrim((context as unknown as { name?: string }).name);
      const run = readCheckRunFromPayload(payload);
      const checkSuite = readCheckSuiteFromPayload(payload);
      const repoInfo = readRepoInfoFromPayload(payload);

      log(
        context,
        'info',
        {
          event: eventName,
          action,
          hasCheckRun: Boolean(run),
          hasCheckSuite: Boolean(checkSuite),
          checkRunHeadSha: toStringTrim(run?.head_sha),
          checkRunStatus: toStringTrim(run?.status).toLowerCase(),
          checkRunConclusion: toStringTrim(run?.conclusion).toLowerCase(),
          checkSuiteHeadSha: toStringTrim(checkSuite?.head_sha),
          checkSuiteHeadBranch: toStringTrim(checkSuite?.head_branch),
          checkSuiteStatus: toStringTrim(checkSuite?.status).toLowerCase(),
          checkSuiteConclusion: toStringTrim(checkSuite?.conclusion).toLowerCase(),
          owner: repoInfo?.owner,
          repo: repoInfo?.repo,
        },
        'checks:event-classification'
      );

      if (run) {
        const conclusion = toStringTrim(run?.conclusion).toLowerCase();
        const status = toStringTrim(run?.status).toLowerCase();
        const headShaStr = toStringTrim(run?.head_sha);

        if (!repoInfo) {
          log(
            context,
            'warn',
            {
              event: eventName,
              action,
              conclusion,
              status,
              headShaStr,
            },
            'checks:check-run-missing-repo-info'
          );
          return;
        }

        const prNumbers = readCheckRunPrNumbers(run);

        log(
          context,
          'info',
          {
            owner: repoInfo.owner,
            repo: repoInfo.repo,
            conclusion,
            status,
            headShaStr,
            prNumbers,
          },
          'checks:check-run resolved'
        );

        if (status !== 'completed') return;
        if (conclusion && conclusion !== 'success') {
          if (isBlockingCheckConclusion(conclusion)) {
            await getStaticConfig(context);

            await handleBlockingRegistryHeadConclusion(
              context,
              repoInfo,
              headShaStr,
              readDefaultBranchFromPayload(payload),
              `check-run:${conclusion}`
            );
          }

          return;
        }
        if (conclusion !== 'success') return;
        if (!headShaStr) return;

        for (const prNumber of prNumbers) {
          await collapseBotCommentsByPrefix(
            context,
            { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: prNumber },
            {
              tagPrefix: 'nsreq:ci-validation',
              collapseBody: 'Validation issues resolved.',
              classifier: 'RESOLVED',
            }
          );
        }

        await tryAutoMerge(context, repoInfo, headShaStr);
        return;
      }

      if (!checkSuite) return;
      if (!repoInfo) return;

      const conclusion = toStringTrim(checkSuite.conclusion).toLowerCase();
      const headShaStr = toStringTrim(checkSuite.head_sha);
      const ownerLogin = repoInfo.owner;
      const repoName = repoInfo.repo;

      const prNumbers = await resolveCheckSuitePrNumbers(context, repoInfo, checkSuite, headShaStr);

      log(
        context,
        'info',
        {
          ownerLogin,
          repoName,
          conclusion,
          headShaStr,
          checkSuiteHeadBranch: toStringTrim(checkSuite.head_branch),
          checkSuiteStatus: toStringTrim(checkSuite.status).toLowerCase(),
          prNumbers,
        },
        'checks:context resolved'
      );

      // success -> collapse old CI validation comments + keep existing auto-merge behavior
      if (conclusion === 'success') {
        for (const prNumber of prNumbers) {
          await collapseBotCommentsByPrefix(
            context,
            { owner: ownerLogin, repo: repoName, issue_number: prNumber },
            {
              tagPrefix: 'nsreq:ci-validation',
              collapseBody: 'Validation issues resolved.',
              classifier: 'RESOLVED',
            }
          );
        }

        await maybeHandleDefaultBranchCheckSuiteSuccess(context, payload, checkSuite, {
          owner: ownerLogin,
          repo: repoName,
        });

        if (!headShaStr) return;
        await tryAutoMerge(context, { owner: ownerLogin, repo: repoName }, headShaStr);
        return;
      }

      if (isBlockingCheckConclusion(conclusion)) {
        await getStaticConfig(context);

        await handleBlockingRegistryHeadConclusion(
          context,
          { owner: ownerLogin, repo: repoName },
          headShaStr,
          readDefaultBranchFromPayload(payload),
          `check-suite:${conclusion}`
        );
      }

      // failure -> comment on PR if registry-validate annotations exist
      const suiteId = readCheckSuiteId(checkSuite);
      if (!suiteId) return;
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
        const machineReadableSources: RegistryValidationMachineReadableSource[] = [];
        for (const a of relevant) {
          const file = toStringTrim(a.path) || 'unknown file';
          const rawMsg = toStringTrim(a.message) || toStringTrim(a.raw_details);
          const msg = stripRegistrySuffix(rawMsg);
          if (!msg) continue;
          const schemaMeta = /\bschema=([^\s\]]+)/.exec(rawMsg) ?? /\[schema=([^\]]+)\]/.exec(rawMsg);
          const arr = byFile.get(file) ?? [];
          arr.push(msg);
          byFile.set(file, arr);

          machineReadableSources.push({
            filePath: file,
            message: msg,
            schemaPath: schemaMeta?.[1] ? toStringTrim(schemaMeta[1]) : '',
          });
        }

        const currentCiTags = ['nsreq:ci-validation'];

        for (const prNumber of prNumbers) {
          await collapseBotCommentsByPrefix(
            context,
            { owner: ownerLogin, repo: repoName, issue_number: prNumber },
            {
              tagPrefix: 'nsreq:ci-validation',
              keepTags: currentCiTags,
              collapseBody: 'Validation issues resolved.',
              classifier: 'RESOLVED',
            }
          );
        }

        const body = await buildRegistryValidationAggregatePrCommentBody(
          context,
          { owner: ownerLogin, repo: repoName },
          byFile,
          machineReadableSources
        );
        if (!body) break;

        for (const prNumber of prNumbers) {
          if (DBG) {
            log(
              context,
              'debug',
              { prNumber, files: Array.from(byFile.keys()), bodyLen: body.length },
              'dbg:checks:posting PR comment'
            );
          }

          await postOnce(context, { owner: ownerLogin, repo: repoName, issue_number: prNumber }, body, {
            minimizeTag: 'nsreq:ci-validation',
          });
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
