import { parseForm as parseFormRaw, loadTemplate as loadTemplateRaw } from '../template.js';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadStaticConfig, type RegistryBotHooks as StaticRegistryBotHooks } from '../../../config.js';
import { loadSecrets } from '../../../utils/secrets.js';
import { createHookApi as createHookApiRaw } from './hook-api.js';
import Ajv2020Module from 'ajv/dist/2020.js';
import type { ValidateFunction } from 'ajv';
import { runHookInWorker } from './hook-pool.js';
import addFormatsModule from 'ajv-formats';
import ajvErrorsModule from 'ajv-errors';

const moduleFileName = fileURLToPath(import.meta.url);
const dirName = dirname(moduleFileName);

const DBG = process.env.DEBUG_NS === '1';

const CONFIG_BASE_DIR = '.github/registry-bot';

type HookSecrets = Readonly<{
  CLD_API_BASE_URL?: string;
  CLD_API_KEY?: string;

  STC_API_BASE_URL?: string;
  STC_API_KEY?: string;

  PPMS_API_BASE_URL?: string;
  PPMS_API_KEY?: string;

  [k: string]: string | undefined;
}>;

type CoreSecrets = Readonly<{
  APP_ID?: string;
  WEBHOOK_SECRET?: string;
  PRIVATE_KEY?: string;
  DEBUG_NS?: string;
  HOOK_SECRETS: HookSecrets;
}>;

const coreSecrets = loadSecrets() as unknown as CoreSecrets;

type LoggerLike = {
  debug?: (obj: unknown, msg?: string) => void;
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
};

type HookLogger = {
  debug: (obj: unknown, msg?: string) => void;
  info: (obj: unknown, msg?: string) => void;
  warn: (obj: unknown, msg?: string) => void;
  error: (obj: unknown, msg?: string) => void;
};

function getHookLogger(log?: LoggerLike): HookLogger {
  const noop = (_obj: unknown, _msg?: string): void => {};
  return {
    debug: typeof log?.debug === 'function' ? log.debug : noop,
    info: typeof log?.info === 'function' ? log.info : noop,
    warn: typeof log?.warn === 'function' ? log.warn : noop,
    error: typeof log?.error === 'function' ? log.error : noop,
  };
}

type RepoRef = { owner: string; repo: string };
type IssueRef = { owner: string; repo: string; issue_number: number };

type RepoContentFile = { content: string; encoding?: string };
type RepoContentResponse = RepoContentFile | RepoContentFile[];

export type IssueListItem = { title: string; number: number };

export type OctokitLike = {
  repos: {
    getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data: RepoContentResponse }>;
  };
  issues: {
    get: (args: { owner: string; repo: string; issue_number: number }) => Promise<{ data: unknown }>;
    listForRepo: (args: {
      owner: string;
      repo: string;
      state: 'open' | 'closed' | 'all';
      per_page?: number;
    }) => Promise<{ data: IssueListItem[] }>;
    update: (args: {
      owner: string;
      repo: string;
      issue_number: number;
      body?: string;
      state?: 'open' | 'closed';
      title?: string;
    }) => Promise<unknown>;
    create: (args: { owner: string; repo: string; title: string; body: string; labels?: string[] }) => Promise<unknown>;
    createComment: (args: { owner: string; repo: string; issue_number: number; body: string }) => Promise<unknown>;
    addLabels: (args: { owner: string; repo: string; issue_number: number; labels: string[] }) => Promise<unknown>;
    removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
  };
};

type RequestConfigEntry = {
  folderName?: string;
  schema?: string;
  issueTemplate?: string;
  [k: string]: unknown;
};

type ResourceBotConfig = {
  requests?: Record<string, RequestConfigEntry>;
  hooks?: { allowedHosts?: string[] };
  [k: string]: unknown;
};

// Removed redundant HookApi alias per lint suggestion
type BeforeValidateArgs = Readonly<{
  requestType: string;
  form: FormData;
  api: unknown;
  config: Readonly<Record<string, string>>;
  log?: LoggerLike | undefined;
}>;

type CustomValidateArgs = Readonly<{
  requestType: string;
  resourceName: string;
  candidate: Record<string, unknown>;
  form: FormData;
  api: unknown;
  log?: LoggerLike | undefined;
}>;

export type ApprovalHookStatus = 'approved' | 'rejected' | 'unknown';

export type ApprovalHookDecision = Readonly<{
  status?: ApprovalHookStatus;
  path?: string;
  reason?: string;
  comment?: string;
  message?: string;
  approvers?: readonly string[];
  error?: readonly {
    field?: string;
    message?: string;
  }[];
  errors?: readonly {
    field?: string;
    message?: string;
  }[];
}>;

type ApprovalHookResult =
  | ApprovalHookStatus
  | boolean
  | {
      status?: unknown;
      path?: unknown;
      reason?: unknown;
      comment?: unknown;
      message?: unknown;
      approvers?: unknown;
      error?: unknown;
      errors?: unknown;
      approved?: unknown;
    }
  | undefined
  | void;

type OnApprovalArgs = Readonly<{
  requestType: string;
  namespace: string;
  resourceName: string;
  form: FormData;
  data: Readonly<Record<string, unknown>>;
  requestAuthor: Readonly<{
    id: string;
    email: string;
  }>;
  config: Readonly<{
    raw: Readonly<Record<string, unknown>>;
    approvers: string[];
  }>;
  issue: Readonly<{
    number: number;
    title: string;
    body: string;
    state: string;
    author: string;
    labels: string[];
  }>;
  log?: LoggerLike | undefined;
}>;

type AjvPluginsArgs = Readonly<{
  ajv: unknown;
  context: ValidationContext;
}>;

type HookValidationItem =
  | string
  | {
      field?: unknown;
      message?: unknown;
    };

type HookValidationResult = HookValidationItem[] | undefined | void;

type ResourceBotHooks = {
  ajvPlugins?: (args: AjvPluginsArgs) => void;
  beforeValidate?: (args: BeforeValidateArgs) => void | Promise<void>;

  customValidate?: (args: CustomValidateArgs) => HookValidationResult | Promise<HookValidationResult>;

  onValidate?: (args: CustomValidateArgs) => HookValidationResult | Promise<HookValidationResult>;

  onApproval?: (args: OnApprovalArgs) => ApprovalHookResult | Promise<ApprovalHookResult>;

  [k: string]: unknown;
};

type HookDescriptor = Readonly<{
  __type: string;
  __path: string;
  __hash: string;
  __code: string;
}>;

function isHookDescriptor(v: unknown): v is HookDescriptor {
  return (
    isPlainObject(v) &&
    typeof v.__type === 'string' &&
    typeof v.__path === 'string' &&
    typeof v.__hash === 'string' &&
    typeof v.__code === 'string'
  );
}

type ValidationContext = {
  octokit: OctokitLike;
  log?: LoggerLike;
  repo: () => RepoRef;
  issue: () => IssueRef;

  resourceBotConfig?: ResourceBotConfig;
  resourceBotHooks?: ResourceBotHooks | StaticRegistryBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type IssueLike = {
  number?: number;
  body?: string | null;
  title?: string | null;
  state?: string | null;
  labels?: (string | { name?: string | null })[] | null;
  user?: { login?: string | null } | null;
};

type TemplateField = {
  id?: string;
  attributes?: { label?: string };
  validations?: { required?: boolean };
  [k: string]: unknown;
};

type TemplateMeta = {
  requestType?: string;
  schema?: string;
  root?: string;
  path?: string;
  [k: string]: unknown;
};

type TemplateLike = {
  body?: TemplateField[];
  title?: string;
  name?: string;
  _meta?: TemplateMeta;
  [k: string]: unknown;
};

type FormData = Record<string, string>;

type CandidateData = Record<string, unknown>;

type ValidationBuckets = {
  registry: string[];
  form: string[];
  rules: string[];
  schema: string[];
};

export type ValidationIssue = Readonly<{
  message: string;
  path: string;
}>;

type ValidateRequestIssueOptions = Readonly<{
  mode?: 'request' | 'modify';
  template?: TemplateLike;
  formData?: FormData;
}>;

type AjvInstance = {
  addSchema: (schema: unknown, key?: string) => void;
  compile: (schema: unknown) => ValidateFunction<unknown>;
  getSchema: (key: string) => unknown;
  addMetaSchema: (schema: unknown) => void;
  defaultMeta?: string;
};

type AjvConstructor = new (opts?: { strict?: boolean; allErrors?: boolean }) => AjvInstance;

// eslint-disable-next-line @typescript-eslint/naming-convention
const Ajv2020: AjvConstructor =
  (Ajv2020Module as unknown as { default?: AjvConstructor }).default ?? (Ajv2020Module as unknown as AjvConstructor);

type AjvPlugin = (ajv: AjvInstance) => void;

const addFormats: AjvPlugin =
  (addFormatsModule as unknown as { default?: AjvPlugin }).default ?? (addFormatsModule as unknown as AjvPlugin);

const ajvErrors: AjvPlugin =
  (ajvErrorsModule as unknown as { default?: AjvPlugin }).default ?? (ajvErrorsModule as unknown as AjvPlugin);

export type ValidateRequestIssueResult = Readonly<{
  errors: string[];
  errorsGrouped: ValidationBuckets;
  errorsFormatted: string;
  errorsFormattedSingle: string;
  validationIssues: ValidationIssue[];
  formData: FormData;
  template: TemplateLike | null;
  namespace: string;
  nsType: string;
}>;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringSafe(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v).trim();
  return '';
}

function isRepoContentFile(value: unknown): value is RepoContentFile {
  return isPlainObject(value) && typeof value.content === 'string';
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const status = err['status'];
  return typeof status === 'number' ? status : undefined;
}

function pickHookPublicConfig(secrets: HookSecrets): Readonly<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(secrets || {})) {
    if (!v) continue;
    if (/_API_BASE_URL$/i.test(k)) out[k] = String(v).trim();
  }
  return out;
}

function pickHookSecretsForWorker(secrets: HookSecrets): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(secrets || {})) {
    if (typeof v !== 'string') continue;
    const s = v.trim();
    if (!s) continue;
    out[k] = s;
  }
  return out;
}

type AjvErrorLike = {
  keyword?: string;
  instancePath?: string;
  schemaPath?: string;
  message?: string;
  params?: Record<string, unknown>;
};

type AjvErrorMessageWrapper = AjvErrorLike & {
  params?: Record<string, unknown> & { errors?: AjvErrorLike[] };
};

// Lightweight helpers to access unknown objects safely without any assertions
function getRecordProp(obj: unknown, key: string): unknown {
  if (!isPlainObject(obj)) return undefined;
  return obj[key];
}

function getObjectProp(obj: unknown, key: string): Record<string, unknown> | null {
  const v = getRecordProp(obj, key);
  return isPlainObject(v) ? v : null;
}

function getStringProp(obj: unknown, key: string): string | undefined {
  const v = getRecordProp(obj, key);
  return typeof v === 'string' ? v : undefined;
}

type CreateHookApiFn = (context: ValidationContext, args: { secrets: HookSecrets; allowedHosts: string[] }) => unknown;
const createHookApi = createHookApiRaw as unknown as CreateHookApiFn;

// Hook API is treated as unknown to keep implementation flexible

type LoadTemplateFn = (
  context: ValidationContext,
  args: {
    owner: string;
    repo: string;
    issueTitle?: string;
    issueLabels?: unknown;
    templatePath?: string;
    issueBody?: string;
  }
) => Promise<TemplateLike>;

type ParseFormFn = (body: string, template: TemplateLike) => FormData;

const loadTemplate = loadTemplateRaw as unknown as LoadTemplateFn;
const parseForm = parseFormRaw as unknown as ParseFormFn;

// Error buckets + formatting
function newBuckets(): ValidationBuckets {
  return { registry: [], form: [], rules: [], schema: [] };
}

function dedupe(arr: unknown): string[] {
  const a = Array.isArray(arr) ? arr : [];
  return Array.from(new Set(a.map((s) => String(s).trim()).filter(Boolean)));
}

function normalizeHookErrors(value: unknown): string[] {
  if (!Array.isArray(value)) return [];

  const out: string[] = [];
  for (const it of value) {
    if (typeof it === 'string') {
      const s = it.trim();
      if (s) out.push(s);
      continue;
    }

    if (isPlainObject(it)) {
      const msg = toStringSafe(it.message);
      if (!msg) continue;

      const field = toStringSafe(it.field);
      out.push(field ? `${field}: ${msg}` : msg);
      continue;
    }

    const s = toStringSafe(it);
    if (s) out.push(s);
  }

  return out;
}

function normalizeApprovalHookResult(value: unknown): ApprovalHookDecision {
  if (value === true) return { status: 'approved' };
  if (value === false || value === undefined || value === null) return {};

  const token = toStringSafe(value).toLowerCase();
  if (token === 'approved' || token === 'rejected' || token === 'unknown') {
    return { status: token as ApprovalHookStatus };
  }

  if (!isPlainObject(value)) return {};

  const approvers = toLoginArray(value['approvers']);

  if (value['approved'] === true) {
    const comment = toStringSafe(value['comment']);
    const message = toStringSafe(value['message']);

    return {
      status: 'approved',
      ...(comment ? { comment } : {}),
      ...(message ? { message } : {}),
      ...(approvers.length ? { approvers } : {}),
    };
  }

  const status = toStringSafe(value['status']).toLowerCase();
  const path = toStringSafe(value['path']);
  const reason = toStringSafe(value['reason']);
  const comment = toStringSafe(value['comment']);
  const message = toStringSafe(value['message']);
  const errors = normalizeApprovalHookErrors(value['errors'] ?? value['error']);

  if (status === 'approved' || status === 'rejected' || status === 'unknown') {
    return {
      status: status as ApprovalHookStatus,
      ...(path ? { path } : {}),
      ...(reason ? { reason } : {}),
      ...(comment ? { comment } : {}),
      ...(message ? { message } : {}),
      ...(approvers.length ? { approvers } : {}),
      ...(errors.length ? { errors } : {}),
    };
  }

  return {};
}

function approvalIssueLabelName(value: unknown): string {
  if (typeof value === 'string') return toStringSafe(value);
  if (isPlainObject(value)) return toStringSafe(value['name']);
  return '';
}

function toApprovalIssueLabelNames(labels: IssueLike['labels']): string[] {
  const items = Array.isArray(labels) ? labels : [];
  return items.map((label) => approvalIssueLabelName(label)).filter(Boolean);
}

function normalizeApprovalHookErrors(value: unknown): readonly {
  field?: string;
  message?: string;
}[] {
  if (!Array.isArray(value)) return [];

  const out: { field?: string; message?: string }[] = [];
  const seen = new Set<string>();

  for (const item of value) {
    if (!isPlainObject(item)) continue;

    const field = toStringSafe(item['field']);
    const message = toStringSafe(item['message']);
    if (!message) continue;

    const key = `${field}\u0000${message}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      ...(field ? { field } : {}),
      message,
    });
  }

  return out;
}

function normalizeLoginValue(value: unknown): string {
  return toStringSafe(value).replace(/^@+/, '').trim();
}

function uniqLogins(values: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  for (const value of values || []) {
    const login = normalizeLoginValue(value);
    if (!login) continue;

    const key = login.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(login);
  }

  return out;
}

function toLoginArray(value: unknown): string[] {
  return Array.isArray(value) ? uniqLogins(value.map((item) => normalizeLoginValue(item)).filter(Boolean)) : [];
}

function getApprovalHookApprovers(context: ValidationContext, requestType: string): string[] {
  const cfg = context.resourceBotConfig ?? {};
  const workflow = isPlainObject(cfg['workflow']) ? cfg['workflow'] : {};

  const fallbackApprovers = uniqLogins([
    ...toLoginArray(workflow['approvers']),
    ...toLoginArray(workflow['approversPool']),
  ]);

  const reqs = isPlainObject(cfg.requests) ? cfg.requests : {};
  const entry = isPlainObject(reqs[requestType]) ? reqs[requestType] : null;

  if (!entry) return fallbackApprovers;

  const hasOwnApprovers = Array.isArray(entry['approvers']);
  const hasOwnApproversPool = Array.isArray(entry['approversPool']);

  if (!hasOwnApprovers && !hasOwnApproversPool) return fallbackApprovers;

  return uniqLogins([...toLoginArray(entry['approvers']), ...toLoginArray(entry['approversPool'])]);
}

function buildApprovalHookData(
  args: {
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
  },
  namespace: string,
  resourceName: string
): Readonly<Record<string, unknown>> {
  return {
    ...args.formData,
    name: resourceName || namespace,
    identifier: toStringSafe(args.formData['identifier']) || resourceName || namespace,
    namespace: toStringSafe(args.formData['namespace']) || namespace || resourceName,
  };
}

function toMachineReadablePath(value: unknown, fallback = 'general'): string {
  const path = toStringSafe(value);
  return path || fallback;
}

function makeValidationIssue(path: unknown, message: unknown, fallbackPath = 'general'): ValidationIssue | null {
  const msg = toStringSafe(message);
  if (!msg) return null;

  return {
    path: toMachineReadablePath(path, fallbackPath),
    message: msg,
  };
}

function dedupeValidationIssues(issues: ValidationIssue[]): ValidationIssue[] {
  const out: ValidationIssue[] = [];
  const seen = new Set<string>();

  for (const issue of issues) {
    const key = `${issue.path}\u0000${issue.message}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(issue);
  }

  return out;
}

function buildLabelToFieldIdMap(template: TemplateLike): Map<string, string> {
  const out = new Map<string, string>();
  const fields = Array.isArray(template?.body) ? template.body : [];

  for (const field of fields) {
    const id = toStringSafe(field?.id);
    if (!id) continue;

    const label = toStringSafe(field?.attributes?.label);
    if (label) out.set(label.toLowerCase(), id);

    const humanized = humanizeKey(id);
    if (humanized) out.set(humanized.toLowerCase(), id);
  }

  return out;
}

function getTemplateFieldLabel(template: TemplateLike, fieldId: unknown): string {
  const id = toStringSafe(fieldId);
  if (!id) return '';

  const fields = Array.isArray(template?.body) ? template.body : [];
  for (const field of fields) {
    if (toStringSafe(field?.id) !== id) continue;

    return toStringSafe(field?.attributes?.label);
  }

  return '';
}

function getSchemaMappedFieldName(schemaObj: unknown, fieldId: unknown): string {
  const id = toStringSafe(fieldId);
  if (!id) return '';

  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (!schemaProps) return '';

  if (Object.hasOwn(schemaProps, id)) return id;

  for (const [propName, propDef] of Object.entries(schemaProps)) {
    if (!isPlainObject(propDef)) continue;
    if (toStringSafe(propDef['x-form-field']) !== id) continue;
    return propName;
  }

  return '';
}

function resolveMachineReadableFieldName(
  template: TemplateLike,
  schemaObj: unknown,
  fieldId: unknown,
  fallback = 'details'
): string {
  const id = toStringSafe(fieldId);
  if (!id) return fallback;

  const schemaFieldName = getSchemaMappedFieldName(schemaObj, id);
  if (schemaFieldName) return schemaFieldName;

  const templateLabel = getTemplateFieldLabel(template, id);
  if (templateLabel) return templateLabel;

  return id || fallback;
}

function parseRuleValidationIssue(raw: string): ValidationIssue | null {
  const m = /^([A-Za-z0-9_.-]+)\s*:\s*(.+)$/.exec(raw);
  if (!m?.[1] || !m?.[2]) return null;

  return makeValidationIssue(m[1], m[2], 'rules');
}

function buildMachineReadableValidationIssues(
  buckets: ValidationBuckets,
  template: TemplateLike,
  schemaObj: unknown,
  ajvErrors: AjvErrorLike[] = []
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const labelToFieldId = buildLabelToFieldIdMap(template);
  const { idToLabel } = buildTemplateLabelMaps(template);
  const primaryFieldId = guessPrimaryFieldId(template) || 'identifier';
  const primaryLabel = resolveMachineReadableFieldName(template, schemaObj, primaryFieldId, 'details');
  const ajvMessages = new Set<string>();

  for (const err of Array.isArray(ajvErrors) ? ajvErrors : []) {
    const msg = normalizeAjvMessage(err?.message);
    if (!msg) continue;

    ajvMessages.add(msg);

    const fieldIds = fieldIdsFromAjvError(err);
    if (fieldIds.length) {
      for (const fieldId of fieldIds) {
        const issue = makeValidationIssue(
          resolveMachineReadableFieldName(template, schemaObj, fieldId, 'details'),
          msg,
          'details'
        );
        if (issue) issues.push(issue);
      }
      continue;
    }

    const instancePath = toStringSafe(err?.instancePath).replace(/^\/+/, '');
    const instanceField = instancePath.split('/').find(Boolean) || '';
    const issue = makeValidationIssue(
      resolveMachineReadableFieldName(template, schemaObj, instanceField, 'details'),
      msg,
      'details'
    );
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.form || [])) {
    const requiredMatch = /^Required field is missing in form:\s*(.+)$/i.exec(raw);
    if (requiredMatch?.[1]) {
      const label = toStringSafe(requiredMatch[1]).toLowerCase();
      const fieldName = resolveMachineReadableFieldName(
        template,
        schemaObj,
        labelToFieldId.get(label) || '',
        requiredMatch[1]
      );
      const issue = makeValidationIssue(fieldName, 'Required field is missing.', requiredMatch[1]);
      if (issue) issues.push(issue);
      continue;
    }

    const issue = makeValidationIssue('details', raw, 'details');
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.rules || [])) {
    const structured = parseRuleValidationIssue(raw);
    if (structured) {
      issues.push({
        ...structured,
        path: resolveMachineReadableFieldName(template, schemaObj, structured.path, structured.path || 'details'),
      });
      continue;
    }

    const inferredFieldId = inferFieldLabelFromRuleMsg(raw, primaryFieldId, idToLabel);
    const inferredFieldName = inferredFieldId
      ? resolveMachineReadableFieldName(template, schemaObj, inferredFieldId, inferredFieldId)
      : 'details';

    const issue = makeValidationIssue(inferredFieldName, raw, inferredFieldName);
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.registry || [])) {
    const issue = makeValidationIssue(primaryLabel, raw, primaryLabel);
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.schema || [])) {
    const normalized = normalizeAjvMessage(raw);
    if (ajvMessages.has(normalized)) continue;

    const issue = makeValidationIssue('details', raw, 'details');
    if (issue) issues.push(issue);
  }

  return dedupeValidationIssues(issues);
}

function formatBuckets(b: ValidationBuckets): string {
  const sections: string[] = [];
  if (b.registry.length)
    sections.push(['## Registry', ...dedupe(b.registry)].join('\n- ').replace('## Registry\n- ', '## Registry\n- '));
  if (b.form.length) sections.push(['## Form', ...dedupe(b.form)].join('\n- ').replace('## Form\n- ', '## Form\n- '));
  if (b.rules.length)
    sections.push(['## Rules', ...dedupe(b.rules)].join('\n- ').replace('## Rules\n- ', '## Rules\n- '));
  if (b.schema.length)
    sections.push(['## Schema', ...dedupe(b.schema)].join('\n- ').replace('## Schema\n- ', '## Schema\n- '));

  return sections
    .map((sec) => {
      const lines = sec.split('\n- ');
      const head = lines.shift() || '';
      const items = lines;
      return `${head}\n- ${items.join('\n- ')}`;
    })
    .join('\n\n');
}

function formatFirstBucket(b: ValidationBuckets): string {
  const order: (keyof ValidationBuckets)[] = ['schema', 'registry', 'form', 'rules'];
  const head: Record<keyof ValidationBuckets, string> = {
    schema: '### Schema',
    registry: '### Registry',
    form: '### Form',
    rules: '### Rules',
  };

  for (const k of order) {
    const items = dedupe(b[k] || []);
    if (items.length) return `${head[k]}\n- ${items.join('\n- ')}`;
  }
  return '';
}

// Unified field-grouped formatting
function humanizeKey(s: unknown): string {
  const v = toStringSafe(s);
  if (!v) return 'General';
  const spaced = v
    .replaceAll(/[_-]+/g, ' ')
    .replaceAll(/([a-z0-9])([A-Z])/g, '$1 $2')
    .trim();
  return spaced ? spaced[0].toUpperCase() + spaced.slice(1) : 'General';
}

function buildTemplateLabelMaps(template: TemplateLike): {
  idToLabel: Map<string, string>;
  labelOrder: string[];
} {
  const idToLabel = new Map<string, string>();
  const labelOrder: string[] = [];

  const fields = Array.isArray(template?.body) ? template.body : [];
  for (const f of fields) {
    const id = String(f?.id || '').trim();
    if (!id) continue;

    const label = String(f?.attributes?.label || '').trim() || humanizeKey(id);
    idToLabel.set(id, label);
    labelOrder.push(label);
  }

  return { idToLabel, labelOrder };
}

function addGrouped(grouped: Map<string, string[]>, key: unknown, msg: unknown): void {
  const k = toStringSafe(key) || 'General';
  const m = toStringSafe(msg);
  if (!m) return;

  let items = grouped.get(k);
  if (!items) {
    items = [];
    grouped.set(k, items);
  }
  items.push(m);
}

function fieldIdFromAjvError(e: unknown): string {
  if (!isPlainObject(e)) return '';
  const err = e as AjvErrorLike;

  if (err.keyword === 'required') {
    const mp = err.params?.['missingProperty'];
    if (typeof mp === 'string') return mp.trim();
  }

  if (err.keyword === 'additionalProperties') {
    const ap = err.params?.['additionalProperty'];
    if (typeof ap === 'string') return ap.trim();
  }

  const p = String(err.instancePath || '').trim();
  if (p.startsWith('/')) {
    const first = p.split('/').find(Boolean) || '';
    if (first) return String(first).trim();
  }

  return '';
}

function fieldIdsFromAjvError(e: unknown): string[] {
  if (!isPlainObject(e)) return [];
  const err = e as AjvErrorMessageWrapper;

  if (err.keyword === 'errorMessage' && Array.isArray(err.params?.errors)) {
    const ids: string[] = [];
    for (const inner of err.params.errors) {
      const id = fieldIdFromAjvError(inner);
      if (id) ids.push(id);
    }
    return Array.from(new Set(ids));
  }

  const single = fieldIdFromAjvError(err);
  return single ? [single] : [];
}

function getValueAtInstancePath(obj: unknown, instancePath: unknown): unknown {
  const p = toStringSafe(instancePath);
  if (!p || p === '/') return obj;

  const parts = p.split('/').filter(Boolean);
  let cur: unknown = obj;

  for (const part of parts) {
    if (cur === null || cur === undefined) return undefined;

    if (Array.isArray(cur) && /^\d+$/.test(part)) {
      cur = cur[Number(part)];
      continue;
    }

    if (isPlainObject(cur)) {
      cur = cur[part];
      continue;
    }

    return undefined;
  }

  return cur;
}

function filterNoisyOneOfTypeErrors(ajvErrs: unknown, candidate: unknown): AjvErrorLike[] {
  const errs = Array.isArray(ajvErrs) ? (ajvErrs as unknown[]) : [];

  const hasSpecificErrorAtPath = new Set(
    errs
      .filter((e) => isPlainObject(e))
      .map((e) => e as AjvErrorLike)
      .filter(
        (e) =>
          String(e.instancePath || '') &&
          ['pattern', 'format', 'minItems', 'uniqueItems', 'errorMessage', 'oneOf', 'anyOf'].includes(
            String(e.keyword || '')
          )
      )
      .map((e) => String(e.instancePath || ''))
  );

  const sane: AjvErrorLike[] = errs.filter(isPlainObject).map((e) => e as AjvErrorLike);
  return sane.filter((e) => {
    if (e.keyword === 'type' && toStringSafe(e.params?.['type']) === 'string') {
      const path = String(e.instancePath || '');
      const val = getValueAtInstancePath(candidate, path);

      if (Array.isArray(val) && hasSpecificErrorAtPath.has(path)) {
        return false;
      }
    }

    return true;
  });
}

function normalizeAjvMessage(msg: unknown): string {
  const raw = toStringSafe(msg);
  if (!raw) return '';

  let out = raw
    .replaceAll(/\bmust\s+not\b/gi, 'MUST NOT')
    .replaceAll(/\bshall\s+not\b/gi, 'SHALL NOT')
    .replaceAll(/\bshould\s+not\b/gi, 'SHOULD NOT')
    .replaceAll(/\bmust\b/gi, 'MUST')
    .replaceAll(/\brequired\b/gi, 'REQUIRED')
    .replaceAll(/\bshall\b/gi, 'SHALL')
    .replaceAll(/\bshould\b/gi, 'SHOULD')
    .replaceAll(/\brecommended\b/gi, 'RECOMMENDED')
    .replaceAll(/\bmay\b/gi, 'MAY')
    .replaceAll(/\boptional\b/gi, 'OPTIONAL');

  out = out.charAt(0).toUpperCase() + out.slice(1);
  return out;
}

function extractFieldLabelFromFormMsg(msg: unknown): string {
  const s = toStringSafe(msg);
  const m1 = /^Required field is missing in form:\s*(.+)$/i.exec(s);
  if (m1?.[1]) return toStringSafe(m1[1]);
  return '';
}

function guessPrimaryFieldId(template: TemplateLike): string {
  const fields = Array.isArray(template?.body) ? template.body : [];
  const ids = new Set(fields.map((f) => String(f?.id || '').trim()).filter(Boolean));

  if (ids.has('identifier')) return 'identifier';
  if (ids.has('namespace')) return 'namespace';
  if (ids.has('product-id')) return 'product-id';
  if (ids.has('productId')) return 'productId';

  for (const f of fields) {
    const id = String(f?.id || '').trim();
    if (!id) continue;
    if (f?.validations?.required !== true) continue;

    const label = String(f?.attributes?.label || '').toLowerCase();
    const looksLikeId =
      id.toLowerCase().includes('id') ||
      id.toLowerCase().includes('identifier') ||
      id.toLowerCase().includes('namespace') ||
      label.includes('id') ||
      label.includes('identifier') ||
      label.includes('namespace') ||
      label.includes('product id');

    if (looksLikeId) return id;
  }

  return fields.length ? String(fields[0]?.id || '').trim() : '';
}

function inferFieldLabelFromRuleMsg(msg: unknown, primary: string, idMap: Map<string, string>): string {
  const s = toStringSafe(msg).toLowerCase();

  if (s.includes('identifier') || s.includes('namespace') || s.includes('product id')) {
    return primary;
  }

  if (s.includes('title') && idMap.get('title')) {
    return 'title';
  }

  return '';
}

function groupAjvErrors(
  grouped: Map<string, string[]>,
  idToLabel: Map<string, string>,
  ajvErrors: AjvErrorLike[]
): void {
  if (!Array.isArray(ajvErrors) || !ajvErrors.length) return;
  for (const e of ajvErrors) {
    const ids = fieldIdsFromAjvError(e);
    if (ids.length) {
      for (const fieldId of ids) {
        const label = idToLabel.get(fieldId) || humanizeKey(fieldId);
        addGrouped(grouped, label, normalizeAjvMessage(e?.message));
      }
      continue;
    }
    addGrouped(grouped, 'General', normalizeAjvMessage(e?.message));
  }
}

function processBucketItem(
  grouped: Map<string, string[]>,
  idToLabel: Map<string, string>,
  ajvMsgLcSet: Set<string>,
  it: unknown,
  kind: 'form' | 'rules',
  primaryLabel: string
): void {
  const fieldLabel = extractFieldLabelFromFormMsg(it);
  if (fieldLabel) {
    const stripped = toStringSafe(it).replace(/^Required field is missing in form:\s*/i, '');
    if (stripped && stripped.toLowerCase() === fieldLabel.toLowerCase()) {
      const ajvRequiredMsg = `${fieldLabel} is required.`;
      if (!ajvMsgLcSet.has(ajvRequiredMsg.toLowerCase())) addGrouped(grouped, fieldLabel, 'Required field is missing.');
      return;
    }
    addGrouped(grouped, fieldLabel, stripped);
    return;
  }

  // Use fieldId only for grouping into the correct form section, but do not render it.
  const s = toStringSafe(it);
  const m = /^([A-Za-z0-9_-]+)\s*:\s*(.+)$/.exec(s);
  if (m?.[1] && m?.[2]) {
    const fieldId = m[1].trim();
    const msgOnly = m[2].trim();
    const label = idToLabel.get(fieldId);

    if (label && msgOnly) {
      addGrouped(grouped, label, msgOnly);
      return;
    }
  }

  if (kind === 'rules') {
    const inferred = inferFieldLabelFromRuleMsg(it, primaryLabel, idToLabel);
    if (inferred) {
      addGrouped(grouped, inferred, it);
      return;
    }
  }

  addGrouped(grouped, 'General', it || '');
}

function addBucketMsgs(
  grouped: Map<string, string[]>,
  idToLabel: Map<string, string>,
  ajvMsgLcSet: Set<string>,
  arr: unknown,
  kind: 'form' | 'rules',
  primaryLabel: string
): void {
  const items = dedupe(arr || []);
  for (const it of items) processBucketItem(grouped, idToLabel, ajvMsgLcSet, it, kind, primaryLabel);
}

function orderGroupedKeys(labelOrder: string[], grouped: Map<string, string[]>): string[] {
  const keys = Array.from(grouped.keys());
  const ordered: string[] = [];
  for (const lbl of labelOrder) if (grouped.has(lbl)) ordered.push(lbl);
  for (const k of keys) if (k !== 'General' && !ordered.includes(k)) ordered.push(k);
  if (grouped.has('General')) ordered.push('General');
  return ordered;
}

function formatUnifiedIssues(
  buckets: ValidationBuckets,
  template: TemplateLike,
  ajvErrors: AjvErrorLike[] = []
): string {
  const grouped = new Map<string, string[]>();
  const { idToLabel, labelOrder } = buildTemplateLabelMaps(template);

  const primaryFieldId = guessPrimaryFieldId(template);
  const primaryLabel = primaryFieldId ? idToLabel.get(primaryFieldId) || humanizeKey(primaryFieldId) : 'General';

  const ajvMsgSet = new Set(
    (Array.isArray(ajvErrors) ? ajvErrors : []).map((e) => normalizeAjvMessage(e?.message)).filter(Boolean)
  );
  const ajvMsgLcSet = new Set(Array.from(ajvMsgSet).map((s) => String(s).toLowerCase()));

  groupAjvErrors(grouped, idToLabel, ajvErrors);
  addBucketMsgs(grouped, idToLabel, ajvMsgLcSet, buckets?.form, 'form', primaryLabel);
  addBucketMsgs(grouped, idToLabel, ajvMsgLcSet, buckets?.rules, 'rules', primaryLabel);

  for (const it of dedupe(buckets?.registry || [])) addGrouped(grouped, primaryLabel, it);
  for (const it of dedupe(buckets?.schema || [])) {
    const msg = toStringSafe(it);
    if (!msg) continue;
    if (ajvMsgSet.has(msg)) continue;
    addGrouped(grouped, 'General', `[schema] ${msg}`);
  }
  const ordered = orderGroupedKeys(labelOrder, grouped);
  return ordered
    .map((k) => {
      const lines = dedupe(grouped.get(k) || []);
      return `### ${k}\n- ${lines.join('\n- ')}`;
    })
    .join('\n\n');
}

// Config access
const getRequestsConfig = (context: ValidationContext): Record<string, RequestConfigEntry> => {
  const req = context?.resourceBotConfig?.requests;
  const isRequestConfigMap = (v: unknown): v is Record<string, RequestConfigEntry> => isPlainObject(v);
  return isRequestConfigMap(req) ? req : {};
};

const getRequestConfig = (context: ValidationContext, requestType: unknown): RequestConfigEntry | null => {
  const rt = toStringSafe(requestType);
  if (!rt) return null;

  const req = getRequestsConfig(context);
  if (Object.hasOwn(req, rt)) return req[rt];

  const rtLc = rt.toLowerCase();
  for (const [k, v] of Object.entries(req)) {
    if (String(k).toLowerCase() === rtLc) return v;
  }
  return null;
};

const getResourceBotHooks = (context: ValidationContext): ResourceBotHooks | null => {
  const hooks = context?.resourceBotHooks;
  const isHooksConfig = (v: unknown): v is ResourceBotHooks => isPlainObject(v);
  return isHooksConfig(hooks) ? hooks : null;
};

// Primary ID resolution
function pickIdentifierFromFields(template: TemplateLike, formData: FormData): string {
  const asTrimmed = (v: unknown): string => (typeof v === 'string' ? v.trim() : '');
  const fields = Array.isArray(template.body) ? template.body : [];

  for (const field of fields) {
    const id = String(field.id).trim();
    const required = field?.validations?.required === true;
    const label = toStringSafe(field?.attributes?.label).toLowerCase();
    if (!required) continue;
    const looksLikeId =
      id.includes('id') ||
      id.includes('identifier') ||
      id.includes('namespace') ||
      label.includes('id') ||
      label.includes('identifier') ||
      label.includes('namespace');
    if (looksLikeId) {
      const raw = asTrimmed((formData as Record<string, unknown>)[id]);
      if (raw) return raw;
    }
  }

  for (const field of fields) {
    if (!field?.id) continue;
    if (field?.validations?.required !== true) continue;
    const raw = asTrimmed((formData as Record<string, unknown>)[String(field.id)]);
    if (raw) return raw;
  }

  for (const field of fields) {
    if (!field?.id) continue;
    const raw = asTrimmed((formData as Record<string, unknown>)[String(field.id)]);
    if (raw) return raw;
  }
  return '';
}

function normalizePrimaryResourceToken(v: unknown): string {
  return toStringSafe(v)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
}

const PRIMARY_RESOURCE_FIELDS = new Set(['identifier', 'namespace', 'productid', 'id', 'name', 'vendor']);

function isPrimaryResourceField(v: unknown): boolean {
  return PRIMARY_RESOURCE_FIELDS.has(normalizePrimaryResourceToken(v));
}

function readFirstPrimaryValue(record: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = toStringSafe(record[key]).replaceAll('\u00a0', ' ').trim();
    if (value) return value;
  }

  return '';
}

function readPrimaryValueFromSchemaFields(
  schemaProps: Record<string, unknown>,
  record: Record<string, unknown>
): string {
  for (const [propName, propDef] of Object.entries(schemaProps)) {
    if (!isPlainObject(propDef)) continue;

    const formField = toStringSafe(propDef['x-form-field']);
    if (!isPrimaryResourceField(formField)) continue;

    const match = readFirstPrimaryValue(record, [formField, propName]);
    if (match) return match;
  }

  return '';
}

function readPrimaryValueFromSchemaPropertyNames(
  schemaProps: Record<string, unknown>,
  record: Record<string, unknown>
): string {
  for (const propName of Object.keys(schemaProps)) {
    if (!isPrimaryResourceField(propName)) continue;

    const value = readFirstPrimaryValue(record, [propName]);
    if (value) return value;
  }

  return '';
}

function resolvePrimaryIdFromRecord(schemaObj: unknown, record: Record<string, unknown>): string {
  const directKeys = ['identifier', 'namespace', 'product-id', 'productId', 'id', 'name', 'vendor'];
  const directValue = readFirstPrimaryValue(record, directKeys);
  if (directValue) return directValue;

  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (!schemaProps) return '';

  const schemaFieldValue = readPrimaryValueFromSchemaFields(schemaProps, record);
  if (schemaFieldValue) return schemaFieldValue;

  return readPrimaryValueFromSchemaPropertyNames(schemaProps, record);
}

export function resolvePrimaryIdFromCandidate(candidate: Record<string, unknown>, schemaObj: unknown): string {
  return resolvePrimaryIdFromRecord(schemaObj, candidate);
}

function readDirectPrimaryIdFromForm(formData: FormData): string {
  return readFirstPrimaryValue(formData as Record<string, unknown>, [
    'identifier',
    'namespace',
    'product-id',
    'productId',
  ]);
}

function readIdentifierMappedSchemaValue(formData: FormData, schemaObj: unknown): string {
  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (!schemaProps) return '';

  for (const [propName, propDef] of Object.entries(schemaProps)) {
    if (!isPlainObject(propDef) || propDef['x-form-field'] !== 'identifier') continue;
    return readFirstPrimaryValue(formData as Record<string, unknown>, ['identifier', propName]);
  }

  return '';
}

export function resolvePrimaryIdFromTemplate(template: TemplateLike, formData: FormData, schemaObj: unknown): string {
  if (!template) return '';

  return (
    readDirectPrimaryIdFromForm(formData) ||
    readIdentifierMappedSchemaValue(formData, schemaObj) ||
    resolvePrimaryIdFromRecord(schemaObj, formData as Record<string, unknown>) ||
    pickIdentifierFromFields(template, formData)
  );
}

function resolvePrimaryIdFromSchemaAndForm(formData: FormData, schemaObj: unknown): string {
  const asTrimmed = (v: unknown): string => toStringSafe(v).replaceAll('\u00a0', ' ').trim();

  const viaGeneric = resolvePrimaryIdFromRecord(schemaObj, formData as Record<string, unknown>);
  if (viaGeneric) return viaGeneric;

  return asTrimmed(formData.title || '');
}

function normalizeFormDataForHookValidation(
  requestType: string,
  formData: FormData,
  schemaObj: unknown,
  template?: TemplateLike | null
): FormData {
  const rawResolved = template
    ? resolvePrimaryIdFromTemplate(template, formData, schemaObj)
    : resolvePrimaryIdFromSchemaAndForm(formData, schemaObj);

  const rawIdOrNs = rawResolved.replaceAll('\u00a0', ' ').trim();

  const description = String(
    formData.description ||
      (formData as Record<string, string>)['system-description'] ||
      (formData as Record<string, string>)['sub-context-description'] ||
      ''
  )
    .replaceAll('\u00a0', ' ')
    .trim();

  return {
    ...formData,
    requestType,
    identifier: rawIdOrNs,
    namespace: rawIdOrNs,
    description,
    contact: linesToSafeString(
      (formData as Record<string, unknown>)['contact'] ?? (formData as Record<string, unknown>)['contacts']
    ),
    correlationIds: linesToSafeString(
      (formData as Record<string, unknown>)['correlationIds'] ??
        (formData as Record<string, unknown>)['correlation-ids']
    ),
  };
}

async function stringifyCandidateValueForForm(v: unknown): Promise<string> {
  if (v === undefined || v === null) return '';

  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);

  if (Array.isArray(v)) {
    const allScalar = v.every(
      (it) => typeof it === 'string' || typeof it === 'number' || typeof it === 'boolean' || it === null
    );
    if (allScalar) return linesToSafeString(v);
  }

  try {
    const yamlMod = (await import('yaml')) as unknown as { stringify: (val: unknown) => string };
    return yamlMod.stringify(v).trim();
  } catch {
    return String(v);
  }
}

async function writeSerializedFormValue(target: FormData, key: string, value: unknown): Promise<string> {
  if (value === undefined || value === null || target[key]) return '';

  const serialized = await stringifyCandidateValueForForm(value);
  if (serialized) target[key] = serialized;
  return serialized;
}

async function mapSchemaCandidatePropsToForm(
  out: FormData,
  props: Record<string, unknown>,
  candidateRec: Record<string, unknown>
): Promise<void> {
  for (const [propName, propDef] of Object.entries(props)) {
    const serialized = await writeSerializedFormValue(out, propName, getRecordProp(candidateRec, propName));
    if (!serialized) continue;

    const ff = isPlainObject(propDef) ? toStringSafe(propDef['x-form-field']).trim() : '';
    if (ff && !out[ff]) out[ff] = serialized;
  }
}

async function mapRemainingCandidatePropsToForm(out: FormData, candidateRec: Record<string, unknown>): Promise<void> {
  for (const [k, v] of Object.entries(candidateRec)) {
    await writeSerializedFormValue(out, k, v);
  }
}

async function buildFormDataForHookValidationFromCandidate(
  requestType: string,
  schemaObj: unknown,
  candidate: CandidateData
): Promise<FormData> {
  const out: FormData = {};
  const props = getObjectProp(schemaObj, 'properties') || {};
  const candidateRec = isPlainObject(candidate) ? candidate : {};

  await mapSchemaCandidatePropsToForm(out, props, candidateRec);
  await mapRemainingCandidatePropsToForm(out, candidateRec);

  return normalizeFormDataForHookValidation(requestType, out, schemaObj, null);
}

// AJV init + caches
const SCHEMA_CACHE = new Map<string, ValidateFunction<unknown>>();
const AJV_CACHE = new Map<string, AjvInstance>();
const REPO_SCHEMA_CACHE = new Map<string, unknown>();

function initAjvInstance(ajv: AjvInstance, context: ValidationContext): void {
  // ajv-formats + ajv-errors are applied to the instance
  addFormats(ajv);
  ajvErrors(ajv);

  const hooks = getResourceBotHooks(context);
  if (hooks && typeof hooks.ajvPlugins === 'function') {
    try {
      hooks.ajvPlugins({ ajv, context });
    } catch (err: unknown) {
      context.log?.warn?.(
        { err: err instanceof Error ? err.message : String(err) },
        'resource-bot hooks.ajvPlugins failed'
      );
    }
  }
}

function buildValidateRequestIssueResult(
  errors: string[],
  buckets: ValidationBuckets,
  template: TemplateLike,
  options: {
    schemaObj: unknown;
    ajvErrorsForUnifiedFormat: AjvErrorLike[];
    formData: FormData;
    namespace: string;
    nsType: string;
  }
): ValidateRequestIssueResult {
  const unified = formatUnifiedIssues(buckets, template, options.ajvErrorsForUnifiedFormat);
  const validationIssues = buildMachineReadableValidationIssues(
    buckets,
    template,
    options.schemaObj,
    options.ajvErrorsForUnifiedFormat
  );

  return {
    errors,
    errorsGrouped: buckets,
    errorsFormatted: unified || formatBuckets(buckets),
    errorsFormattedSingle: unified || formatFirstBucket(buckets),
    validationIssues,
    formData: options.formData,
    template,
    namespace: options.namespace,
    nsType: options.nsType,
  };
}

function buildValidateRequestIssueErrorResult(
  errors: string[],
  buckets: ValidationBuckets,
  template: TemplateLike,
  schemaObj: unknown,
  message: string,
  targetBucket: string[]
): ValidateRequestIssueResult {
  targetBucket.push(message);
  errors.push(message);

  return buildValidateRequestIssueResult(errors, buckets, template, {
    schemaObj,
    ajvErrorsForUnifiedFormat: [],
    formData: {},
    namespace: '',
    nsType: '',
  });
}

function resolveTemplateAndRequestType(
  context: ValidationContext,
  template: TemplateLike,
  formData: FormData,
  errors: string[],
  buckets: ValidationBuckets
):
  | { template: TemplateLike; requestType: string; requestCfg: RequestConfigEntry }
  | { result: ValidateRequestIssueResult } {
  let requestType = String(template?._meta?.requestType || '').trim();

  if (requestType && requestType.toLowerCase() === 'partnernamespace') {
    const selected =
      (formData as Record<string, unknown>)['requestType'] ?? (formData as Record<string, unknown>)['request-type'];

    const mapped = mapPartnerNamespaceRequestTypeToConfigKey(selected);
    if (!mapped) {
      return {
        result: buildValidateRequestIssueErrorResult(
          errors,
          buckets,
          template,
          null,
          `Invalid Partner Namespace 'Request Type' selection '${toStringSafe(selected) || ''}'. Expected one of: authority, system, subContext.`,
          buckets.form
        ),
      };
    }

    const mappedCfg = getRequestConfig(context, mapped);
    if (!mappedCfg) {
      return {
        result: buildValidateRequestIssueErrorResult(
          errors,
          buckets,
          template,
          null,
          `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests has no such entry.`,
          buckets.schema
        ),
      };
    }

    const mappedSchema = toStringSafe(mappedCfg.schema);
    if (!mappedSchema) {
      return {
        result: buildValidateRequestIssueErrorResult(
          errors,
          buckets,
          template,
          null,
          `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests['${mapped}'].schema is empty.`,
          buckets.schema
        ),
      };
    }

    const nextMeta = template._meta
      ? {
          ...template._meta,
          requestType: mapped,
          schema: mappedSchema,
          root: toStringSafe(mappedCfg.folderName),
        }
      : {
          requestType: mapped,
          schema: mappedSchema,
          root: toStringSafe(mappedCfg.folderName),
        };

    template = { ...template };
    template._meta = nextMeta;

    requestType = mapped;
  }

  if (!requestType) {
    return {
      result: buildValidateRequestIssueErrorResult(
        errors,
        buckets,
        template,
        null,
        'Configuration error: template missing _meta.requestType (expected cfg.requests mapping via loadTemplate).',
        buckets.schema
      ),
    };
  }

  const requestCfg = getRequestConfig(context, requestType);
  if (!requestCfg) {
    return {
      result: buildValidateRequestIssueErrorResult(
        errors,
        buckets,
        template,
        null,
        `Configuration error: unknown requestType '${requestType}' (missing cfg.requests entry).`,
        buckets.schema
      ),
    };
  }

  return { template, requestType, requestCfg };
}

async function checkRegistryDuplicate(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  template: TemplateLike,
  requestCfg: RequestConfigEntry,
  normalizedFormData: FormData,
  buckets: ValidationBuckets,
  errors: string[]
): Promise<void> {
  const namespace = String(normalizedFormData.namespace || '').trim();
  if (!namespace) return;

  const resourceName = String(normalizedFormData.identifier || normalizedFormData.namespace || '').trim();
  if (!resourceName) return;

  try {
    const structRoot = resolveRegistryRootForTemplate(context, template, requestCfg);
    const filePath = `${structRoot}/${resourceName}.yaml`;

    await context.octokit.repos.getContent({ owner: repoInfo.owner, repo: repoInfo.repo, path: filePath });

    const msg = `Resource '${resourceName}' already exists in registry`;
    buckets.registry.push(msg);
    errors.push(msg);
  } catch (e: unknown) {
    if (getHttpStatus(e) !== 404) {
      context.log?.warn?.({ err: e instanceof Error ? e.message : String(e) }, 'registry existence check failed');
    }
  }
}

function getAjvKey(context: ValidationContext, schemaPath: string): string {
  const hs = context.resourceBotHooksSource;
  return hs ? `${schemaPath}::${hs}` : schemaPath;
}

function getValidateCacheKey(ajvKey: string, schemaPath: string): string {
  return `${ajvKey}#${schemaPath}#root`;
}

function getOrCreateValidator(
  context: ValidationContext,
  schemaObj: unknown,
  schemaPath: string
): { validate: ValidateFunction<unknown>; ajvKey: string } {
  const ajvKey = getAjvKey(context, schemaPath);

  let ajv = AJV_CACHE.get(ajvKey);
  if (!ajv) {
    ajv = new Ajv2020({ strict: false, allErrors: true });
    initAjvInstance(ajv, context);
    ajv.addSchema(schemaObj, schemaPath);
    AJV_CACHE.set(ajvKey, ajv);
  }

  const cacheKey = getValidateCacheKey(ajvKey, schemaPath);

  let validate = SCHEMA_CACHE.get(cacheKey);
  if (!validate) {
    validate = ajv.compile(schemaObj);
    SCHEMA_CACHE.set(cacheKey, validate);
  }

  return { validate, ajvKey };
}

// Schema loading
async function loadSchemaLocal(schemaPath: unknown): Promise<unknown> {
  const want = toStringSafe(schemaPath) || 'namespace.schema.json';
  const cleanedWant = want.replace(/^\.?\//, '');

  const srcDir = resolve(dirName, '../../..');
  const projectRoot = resolve(srcDir, '..');

  const candidates: string[] = [
    cleanedWant,
    `./${cleanedWant}`,
    `../${cleanedWant}`,
    `../../${cleanedWant}`,
    `../../../${cleanedWant}`,
    resolve(srcDir, 'schemas', cleanedWant),
    resolve(projectRoot, 'src', 'schemas', cleanedWant),
    resolve(process.cwd(), 'src', 'schemas', cleanedWant),
    resolve(process.cwd(), cleanedWant),
  ];

  const errors: string[] = [];
  for (const cand of candidates) {
    const abs = resolve(dirName, cand);
    try {
      const buf = await readFile(abs, 'utf8');
      return JSON.parse(buf);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`'${cand}': ${msg}`);
    }
  }

  throw new Error(`Failed to load schema (tried: ${candidates.join(', ')}). Errors: ${errors.join(' | ')}`);
}

async function loadSchemaFromRepoOrLocal(
  context: ValidationContext,
  owner: string,
  repo: string,
  schemaPath: unknown
): Promise<unknown> {
  const raw = toStringSafe(schemaPath);
  if (!raw) return null;

  const octokit = context?.octokit;
  const searchPaths = ['.github/registry-bot/request-schemas', 'schema', '.'];

  const addCandidate = (set: Set<string>, p: unknown): void => {
    const cleaned = toStringSafe(p).replace(/^\/+/, '');
    if (cleaned) set.add(cleaned);
  };

  const cacheRepoKeyBase = octokit && owner && repo ? `${String(owner)}/${String(repo)}` : '';

  if (octokit && owner && repo) {
    const candidates = new Set<string>();

    if (raw.startsWith('/')) {
      // Explicit repo-absolute path
      addCandidate(candidates, raw);
    } else {
      const cleaned = raw.replace(/^\.?\//, '');

      const isRepoRelativeConfigPath = cleaned.startsWith(`${CONFIG_BASE_DIR}/`) || cleaned.startsWith('.github/');

      if (isRepoRelativeConfigPath) {
        // Already a repo-relative path -> use as-is only
        addCandidate(candidates, cleaned);
      } else {
        // Relative short path -> search through known schema locations
        addCandidate(candidates, `${CONFIG_BASE_DIR}/${cleaned}`);
        for (const base of searchPaths) {
          addCandidate(candidates, `${base.replace(/^\.?\//, '')}/${cleaned}`);
        }
        addCandidate(candidates, cleaned);
      }
    }

    for (const p of candidates) {
      const cacheKey = cacheRepoKeyBase ? `${cacheRepoKeyBase}:${p}` : '';
      if (cacheKey && REPO_SCHEMA_CACHE.has(cacheKey)) return REPO_SCHEMA_CACHE.get(cacheKey);

      try {
        const res = await octokit.repos.getContent({ owner, repo, path: p });
        const data = res.data;

        if (!Array.isArray(data) && isRepoContentFile(data)) {
          const text = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
          const obj = JSON.parse(text);
          if (cacheKey) REPO_SCHEMA_CACHE.set(cacheKey, obj);
          return obj;
        }
      } catch (e: unknown) {
        if (getHttpStatus(e) === 404) continue;
        break;
      }
    }
  }

  return loadSchemaLocal(raw);
}

// Value normalization helpers
function linesToSafeString(v: unknown): string {
  if (Array.isArray(v))
    return v
      .map((x) => String(x || '').trim())
      .filter(Boolean)
      .join('\n');

  if (v === null || v === undefined) return '';

  const s = toStringSafe(v);
  return s
    .split(/\r?\n/)
    .map((x) => x.trim())
    .filter(Boolean)
    .join('\n');
}

function mapOpenSystemToVisibility(v: unknown): string {
  const s = toStringSafe(v).trim().toLowerCase();
  if (s === 'yes') return 'public';
  if (s === 'no') return 'internal';
  return '';
}

async function parseMaybeYamlJson(v: unknown): Promise<unknown> {
  if (v === null) return undefined;
  if (Array.isArray(v) || isPlainObject(v)) return v;

  const s = toStringSafe(v);
  if (!s) return undefined;

  try {
    return JSON.parse(s);
  } catch {
    // ignore
  }

  try {
    const mod = (await import('yaml')) as unknown as {
      default: { parse: (src: string) => unknown };
    };
    return mod.default.parse(s);
  } catch {
    // ignore
  }

  return undefined;
}

// projectForSchema
export async function projectForSchema(
  category: string,
  form: FormData,
  schemaObj: unknown
): Promise<Record<string, unknown>> {
  const props: Record<string, unknown> | null = getObjectProp(schemaObj, 'properties');

  if (!props) {
    throw new Error('Configuration error: schema is missing or malformed (no properties).');
  }

  const toStringTrim = (v: unknown): string => toStringSafe(v).replaceAll('\u00a0', ' ').trim();

  const toUniqueStringArray = (v: unknown): string[] => {
    const raw = Array.isArray(v) ? v : toStringTrim(v).split(/\r?\n|,/);
    const arr = raw.map((x) => toStringTrim(x)).filter((x) => x && x.toLowerCase() !== 'undefined');
    return Array.from(new Set(arr));
  };

  const coerceBySchema = async (propDef: unknown, raw: unknown): Promise<unknown> => {
    if (raw === null || raw === undefined) return undefined;

    const def = isPlainObject(propDef) ? propDef : {};
    const typeVal = getRecordProp(def, 'type');
    const type = typeof typeVal === 'string' ? typeVal : undefined;

    if (type === 'array') {
      if (Array.isArray(raw)) return raw;

      const s = toStringTrim(raw);
      if (!s) return [];

      const itemsCandidate = getRecordProp(def, 'items');
      const itemsTypeVal = getRecordProp(itemsCandidate, 'type');
      const itemsType = typeof itemsTypeVal === 'string' ? itemsTypeVal : undefined;

      if (itemsType === 'object') {
        const parsed = await parseMaybeYamlJson(s);
        if (Array.isArray(parsed)) return parsed;
      }

      return toUniqueStringArray(s);
    }

    if (type === 'object') {
      if (isPlainObject(raw)) return raw;
      const parsed = await parseMaybeYamlJson(raw);
      return isPlainObject(parsed) ? parsed : undefined;
    }

    if (type === 'boolean') {
      if (typeof raw === 'boolean') return raw;
      const s = toStringTrim(raw).toLowerCase();
      if (['true', 'yes', 'y', '1'].includes(s)) return true;
      if (['false', 'no', 'n', '0'].includes(s)) return false;
      return undefined;
    }

    if (type === 'integer' || type === 'number') {
      if (typeof raw === 'number') return raw;
      const s = toStringTrim(raw);
      if (!s) return undefined;
      const n = Number(s);
      return Number.isFinite(n) ? n : undefined;
    }

    if (Array.isArray(raw)) {
      const joined = raw
        .map((x) => toStringTrim(x))
        .filter(Boolean)
        .join('\n');
      return joined || undefined;
    }

    const s = toStringTrim(raw);
    return s || undefined;
  };

  const pickPropName = (candidates: string[]): string => candidates.find((k) => Object.hasOwn(props, k)) || '';

  const contactProp = pickPropName(['contacts', 'contact']);
  const corrIdsProp = pickPropName(['correlationIds', 'correlation-ids']);
  const corrTypesProp = pickPropName(['correlationIdTypes', 'correlation-id-types']);

  const nsForSchema = toStringTrim(
    form.identifier ||
      form.namespace ||
      (form as Record<string, string>)['name'] ||
      (form as Record<string, string>)['vendor'] ||
      ''
  );
  const visibility =
    mapOpenSystemToVisibility((form as Record<string, string>)['open-system']) ||
    toStringTrim((form as Record<string, string>)['visibility'] || '');

  const candidate: Record<string, unknown> = {};

  // 1) Schema-driven x-form-field mapping
  for (const [propName, propDef] of Object.entries(props)) {
    const ff = isPlainObject(propDef) ? propDef['x-form-field'] : null;
    if (!ff) continue;

    const key = toStringSafe(ff);
    if (!key) continue;
    const raw = (form as Record<string, unknown>)?.[key];
    const coerced = await coerceBySchema(propDef, raw);

    if (coerced === undefined) continue;
    if (Array.isArray(coerced) && coerced.length === 0) continue;
    if (isPlainObject(coerced) && Object.keys(coerced).length === 0) continue;

    candidate[propName] = coerced;
  }

  // 2) Generic mapping
  if (Object.hasOwn(props, 'type')) {
    const tDef = getRecordProp(props, 'type');
    const constStr = getStringProp(tDef, 'const');
    const expectedConst = constStr ? toStringTrim(constStr) : '';

    if (expectedConst) candidate.type = expectedConst;
    else if (category) candidate.type = toStringTrim(category);
  }

  if (Object.hasOwn(props, 'name') && nsForSchema) candidate['name'] = nsForSchema;

  if (Object.hasOwn(props, 'identifier') && toStringTrim(form.identifier)) {
    candidate['identifier'] = toStringTrim(form.identifier);
  }

  if (Object.hasOwn(props, 'description')) {
    const d = toStringTrim((form as Record<string, string>)['description']);
    if (d) candidate['description'] = d;
  }

  if (contactProp) {
    const arr = toUniqueStringArray((form as Record<string, unknown>)['contact']);
    if (arr.length) {
      const coerced = await coerceBySchema(props[contactProp], arr);
      if (coerced !== undefined && (!Array.isArray(coerced) || coerced.length)) candidate[contactProp] = coerced;
    }
  }

  if (corrIdsProp) {
    const arr = toUniqueStringArray(
      (form as Record<string, unknown>)['correlationIds'] ?? (form as Record<string, unknown>)['correlation-ids']
    );
    if (arr.length) {
      const coerced = await coerceBySchema(props[corrIdsProp], arr);
      if (coerced !== undefined && (!Array.isArray(coerced) || coerced.length)) candidate[corrIdsProp] = coerced;
    }
  }

  if (corrTypesProp) {
    const citRaw =
      (form as Record<string, unknown>)['correlationIdTypes'] ??
      (form as Record<string, unknown>)['correlation-id-types'];
    let parsed: unknown = citRaw;

    if (!Array.isArray(parsed) && !isPlainObject(parsed)) {
      const s = toStringTrim(parsed);
      if (s) parsed = (await parseMaybeYamlJson(s)) ?? parsed;
    }

    const coerced = await coerceBySchema(props[corrTypesProp], parsed);
    if (coerced !== undefined && (!Array.isArray(coerced) || coerced.length)) candidate[corrTypesProp] = coerced;
  }

  if (Object.hasOwn(props, 'visibility') && visibility) candidate['visibility'] = visibility;

  if (Object.hasOwn(props, 'title')) {
    const t = toStringTrim((form as Record<string, unknown>)['title']);
    if (t) candidate['title'] = t;
  }

  if (Object.hasOwn(props, 'shortDescription')) {
    const sd = toStringTrim(
      (form as Record<string, unknown>)['shortDescription'] ??
        (form as Record<string, unknown>)['short-description'] ??
        ''
    );
    if (sd) candidate['shortDescription'] = sd;
  }

  if (Object.hasOwn(props, 'summary')) {
    const s = toStringTrim((form as Record<string, unknown>)['summary']);
    if (s) candidate['summary'] = s;
  }

  if (Object.hasOwn(props, 'details')) {
    const d = toStringTrim((form as Record<string, unknown>)['details']);
    if (d) candidate['details'] = d;
  }

  if (Object.hasOwn(props, 'parentId')) {
    const p = toStringTrim((form as Record<string, unknown>)['parentId']);
    if (p) candidate['parentId'] = p;
  }

  // 3) Fallback mapping:
  // If a schema property has no x-form-field and the form has the same key, map it automatically.
  const formRec = form as Record<string, unknown>;

  for (const [propName, propDef] of Object.entries(props)) {
    // don't override anything already set by x-form-field or generic mapping
    if (Object.hasOwn(candidate, propName)) continue;

    // opt-in mapping wins: if schema explicitly declares x-form-field, do not fallback-map
    const ff = isPlainObject(propDef) ? propDef['x-form-field'] : null;
    if (ff) continue;

    // only if the form actually contains the same key (even if the value is falsy)
    if (!Object.hasOwn(formRec, propName)) continue;

    const raw = formRec[propName];
    const coerced = await coerceBySchema(propDef, raw);

    if (coerced === undefined) continue;
    if (Array.isArray(coerced) && coerced.length === 0) continue;
    if (isPlainObject(coerced) && Object.keys(coerced).length === 0) continue;

    candidate[propName] = coerced;
  }

  return candidate;
}

// Registry root resolution
function resolveRegistryRootForTemplate(
  _context: ValidationContext,
  template: TemplateLike,
  requestCfg: RequestConfigEntry
): string {
  const folderName = String(template?._meta?.root || '').trim() || String(requestCfg?.folderName || '').trim();
  return folderName.replace(/^\/+/, '').replace(/\/+$/, '') || 'data';
}

function isNamespaceLikeRequestType(requestType: unknown): boolean {
  const rt = toStringSafe(requestType)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
  if (!rt || rt === 'vendor') return false;

  return rt.includes('namespace') || rt === 'system' || rt === 'subcontext' || rt === 'authority';
}

function isSystemNamespaceRequestType(requestType: unknown): boolean {
  const rt = toStringSafe(requestType)
    .replace(/[\s_-]/g, '')
    .toLowerCase();
  return rt === 'systemnamespace' || rt === 'system';
}

function extractVendorRootFromResourceName(resourceName: unknown): string {
  const raw = toStringSafe(resourceName).replaceAll('\u00a0', ' ').trim();
  if (!raw) return '';

  const first = raw
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean)[0];

  return toStringSafe(first).toLowerCase();
}

function normalizeStringArray(value: unknown): string[] {
  const raw = Array.isArray(value) ? value : value !== undefined && value !== null ? [value] : [];

  return Array.from(new Set(raw.map((v) => toStringSafe(v).replace(/^@+/, '').trim().toLowerCase()).filter(Boolean)));
}

function resolveVendorRegistryRoot(context: ValidationContext): string {
  const vendorCfg = getRequestConfig(context, 'vendor');
  const folder = toStringSafe(vendorCfg?.folderName).replace(/^\/+/, '').replace(/\/+$/, '');
  return folder || 'data/vendors';
}

function resolveAllowedSystemNamespaceVendors(requestCfg: RequestConfigEntry | null): string[] {
  const configured = normalizeStringArray(requestCfg?.['allowedVendorRoots']);
  if (configured.length) return configured;

  const legacy = normalizeStringArray(requestCfg?.['allowedVendors']);
  if (legacy.length) return legacy;

  // preserve current behavior unless explicitly configured otherwise
  return ['sap'];
}

async function repoPathExists(
  context: ValidationContext,
  owner: string,
  repo: string,
  repoPath: string
): Promise<boolean> {
  try {
    await context.octokit.repos.getContent({ owner, repo, path: repoPath });
    return true;
  } catch (e: unknown) {
    if (getHttpStatus(e) === 404) return false;
    throw e;
  }
}

async function collectVendorGovernanceErrors(
  context: ValidationContext,
  owner: string,
  repo: string,
  requestType: string,
  requestCfg: RequestConfigEntry | null,
  resourceName: string
): Promise<string[]> {
  if (!isNamespaceLikeRequestType(requestType)) return [];

  const vendorRoot = extractVendorRootFromResourceName(resourceName);
  if (!vendorRoot) return [];

  const vendorRegistryRoot = resolveVendorRegistryRoot(context);
  const vendorYamlPath = `${vendorRegistryRoot}/${vendorRoot}.yaml`;
  const vendorYmlPath = `${vendorRegistryRoot}/${vendorRoot}.yml`;

  const hasVendorEntry =
    (await repoPathExists(context, owner, repo, vendorYamlPath)) ||
    (await repoPathExists(context, owner, repo, vendorYmlPath));

  const errors: string[] = [];

  if (!hasVendorEntry) {
    errors.push(
      `Vendor '${vendorRoot}' is not registered. Please register '${vendorRoot}' first before requesting '${resourceName}'.`
    );
  }

  if (isSystemNamespaceRequestType(requestType)) {
    const allowedVendorRoots = resolveAllowedSystemNamespaceVendors(requestCfg);

    if (!allowedVendorRoots.includes(vendorRoot)) {
      errors.push(
        `System namespaces are only allowed for vendor roots: ${allowedVendorRoots.join(', ')}. Requested vendor root: '${vendorRoot}'.`
      );
    }
  }

  return errors;
}

// Core validate function
async function ensureStaticConfigLoaded(context: ValidationContext): Promise<void> {
  if (context.resourceBotConfig) return;

  try {
    const { config, hooks, hooksSource } = await loadStaticConfig(
      context as unknown as Parameters<typeof loadStaticConfig>[0],
      {
        validate: false,
        updateIssue: false,
      }
    );

    context.resourceBotConfig = (config || {}) as unknown as ResourceBotConfig;
    context.resourceBotHooks = (hooks || null) as unknown as ResourceBotHooks | null;
    context.resourceBotHooksSource = hooksSource || null;
  } catch (err: unknown) {
    context.log?.warn?.({ err: err instanceof Error ? err.message : String(err) }, 'static-config:load-failed');
    context.resourceBotConfig = {};
    context.resourceBotHooks = null;
    context.resourceBotHooksSource = null;
  }
}

function buildMissingTemplateResult(msg: unknown): ValidateRequestIssueResult {
  const m = msg
    ? `Configuration error: Missing form template (${toStringSafe(msg)})`
    : 'Configuration error: Missing form template';

  const buckets = newBuckets();
  buckets.form.push(m);

  return {
    errors: [m],
    errorsGrouped: buckets,
    errorsFormatted: `### Form\n- ${m}`,
    errorsFormattedSingle: `### Form\n- ${m}`,
    validationIssues: [{ path: 'template', message: m }],
    formData: {},
    template: null,
    namespace: '',
    nsType: '',
  };
}

function isEmpty(v: unknown): boolean {
  if (v === null || v === undefined) return true;
  if (Array.isArray(v)) return v.length === 0;
  const s = toStringSafe(v);
  return !s || s.toLowerCase() === 'undefined';
}

function inferNsType(requestType: unknown): string {
  const requestTypeLc = toStringSafe(requestType).toLowerCase();

  if (requestTypeLc.includes('subcontext')) return 'subcontext';
  if (requestTypeLc.includes('authority')) return 'authority';
  if (requestTypeLc.includes('system')) return 'system';
  if (requestTypeLc === 'product') return 'product';
  return requestTypeLc;
}

function mapPartnerNamespaceRequestTypeToConfigKey(v: unknown): string {
  const raw = toStringSafe(v).trim();
  const norm = raw.replace(/[\s_-]/g, '').toLowerCase();

  if (norm === 'authority') return 'authorityNamespace';
  if (norm === 'system') return 'systemNamespace';
  if (norm === 'subcontext') return 'subContextNamespace';

  return '';
}

/**
 * Validate a namespace issue.
 * params.mode: 'request' | 'modify' (default: 'request')
 */
export async function validateRequestIssue(
  context: ValidationContext,
  params: { owner: string; repo: string },
  issue: IssueLike,
  options: ValidateRequestIssueOptions = {}
): Promise<ValidateRequestIssueResult> {
  await ensureStaticConfigLoaded(context);

  const { owner, repo } = params;

  const errors: string[] = [];
  const buckets = newBuckets();
  let ajvErrorsForUnifiedFormat: AjvErrorLike[] = [];

  const givenTemplate = options?.template;
  const givenFormData = options?.formData;

  // 1) template
  let template = givenTemplate;
  if (!template) {
    try {
      template = await loadTemplate(context, { owner, repo, issueBody: String(issue.body || '') });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : toStringSafe(e);
      return buildMissingTemplateResult(msg);
    }
  }

  // 2) hook api
  const ah = context.resourceBotConfig?.hooks?.allowedHosts;
  const allowedHosts = Array.isArray(ah) ? ah : [];

  const workerSecrets = pickHookSecretsForWorker(coreSecrets.HOOK_SECRETS || {});

  const hookApi = createHookApi(context, {
    secrets: coreSecrets.HOOK_SECRETS || {},
    allowedHosts,
  });

  // 3) form
  const formData = givenFormData || parseForm(String(issue.body || ''), template);

  const resolvedTemplate = resolveTemplateAndRequestType(context, template, formData, errors, buckets);
  if ('result' in resolvedTemplate) return resolvedTemplate.result;

  template = resolvedTemplate.template;
  const { requestType, requestCfg } = resolvedTemplate;

  if (DBG && context.log?.info) context.log.info({ formData }, 'ns:parsedFormData');

  // hooks.beforeValidate
  const hooks = getResourceBotHooks(context);
  if (hooks) {
    // Worker path: hooks is a descriptor (raw code + hash)
    if (isHookDescriptor(hooks)) {
      const beforeValidateArgs: BeforeValidateArgs = {
        requestType,
        form: formData,
        api: null,
        config: pickHookPublicConfig(coreSecrets.HOOK_SECRETS || {}),
        log: undefined,
      };
      const res = await runHookInWorker(
        {
          owner,
          repo,
          path: hooks.__path,
          hash: hooks.__hash,
          code: hooks.__code,
          fn: 'beforeValidate',
          args: beforeValidateArgs,
          allowedHosts,
          secrets: workerSecrets,
        },
        { timeoutMs: 8000 }
      );

      // forward worker logs into main logger
      if (res.logs.length) {
        for (const l of res.logs) {
          const msg = l.msg || 'hook:beforeValidate';
          if (l.level === 'error') context.log?.error?.(l.obj, msg);
          else if (l.level === 'warn') context.log?.warn?.(l.obj, msg);
          else if (l.level === 'debug') context.log?.debug?.(l.obj, msg);
          else context.log?.info?.(l.obj, msg);
        }
      }

      // if worker returned a hook error, only warn
      const hookErr = getStringProp(res.value, '__hookError');
      if (hookErr) {
        context.log?.warn?.({ err: hookErr }, 'resource-bot hooks.beforeValidate failed');
      }

      // Apply in-worker mutations back onto the main-thread formData
      const workerForm = getObjectProp(res.value, 'form');
      if (workerForm) {
        const dst = formData as Record<string, string>;

        // clear existing keys
        for (const k of Object.keys(dst)) delete dst[k];

        // repopulate
        for (const [k, v] of Object.entries(workerForm)) {
          if (v === null || v === undefined) continue;

          if (typeof v === 'string') dst[k] = v;
          else if (typeof v === 'number' || typeof v === 'boolean') dst[k] = String(v);
          else dst[k] = String(v);
        }
      }
    } else if (typeof hooks.beforeValidate === 'function') {
      // Legacy in-process path
      try {
        await hooks.beforeValidate({
          requestType,
          form: formData,
          api: hookApi,
          config: pickHookPublicConfig(coreSecrets.HOOK_SECRETS || {}),
          log: getHookLogger(context.log),
        });
      } catch (err: unknown) {
        context.log?.warn?.(
          { err: err instanceof Error ? err.message : String(err) },
          'resource-bot hooks.beforeValidate failed'
        );
      }
    }
  }

  // 5) Required field check from template
  const requiredFields = (template?.body || []).filter((f) => f?.id && f.validations?.required);

  const missingRequired = requiredFields
    .filter((f) => isEmpty((formData as Record<string, unknown>)?.[String(f.id)]))
    .map((f) => String(f?.attributes?.label || f.id));

  for (const label of missingRequired) {
    const msg = `Required field is missing in form: ${label}`;
    buckets.form.push(msg);
    errors.push(msg);
  }

  // 6) Resolve primary identifier
  const schemaPathForId = String(template?._meta?.schema || '').trim();
  const schemaObjForId = await loadSchemaFromRepoOrLocal(context, owner, repo, schemaPathForId);
  let schemaObjForValidation: unknown = schemaObjForId;

  const rawResolved = resolvePrimaryIdFromTemplate(template, formData, schemaObjForId) || '';
  const rawIdOrNs = rawResolved.replaceAll('\u00a0', ' ').trim();

  if (!rawIdOrNs) {
    const msg = 'Cannot resolve primary identifier from template';
    buckets.form.push(msg);
    errors.push(msg);

    return buildValidateRequestIssueResult(errors, buckets, template, {
      schemaObj: schemaObjForValidation,
      ajvErrorsForUnifiedFormat: [],
      formData: {},
      namespace: '',
      nsType: '',
    });
  }

  // 7) Normalize formData
  const normalizedFormData: FormData = normalizeFormDataForHookValidation(
    requestType,
    formData,
    schemaObjForId,
    template
  );

  if (DBG && context.log?.info) {
    context.log.info(
      {
        description: normalizedFormData.description,
        correlationIds: normalizedFormData.correlationIds,
      },
      'ns:normalizedFormData'
    );
  }

  // 7.1) vendor governance
  try {
    const vendorErrors = await collectVendorGovernanceErrors(context, owner, repo, requestType, requestCfg, rawIdOrNs);

    if (vendorErrors.length) {
      buckets.registry.push(...vendorErrors);
      errors.push(...vendorErrors);
    }
  } catch (e: unknown) {
    const msg = `Configuration error: vendor governance validation failed: ${e instanceof Error ? e.message : String(e)}`;
    buckets.registry.push(msg);
    errors.push(msg);
  }

  // 8) schema validation + hooks.customValidate
  const schemaPath = String(template?._meta?.schema || '').trim();
  if (schemaPath) {
    try {
      if (DBG && context.log?.info) {
        context.log.info(
          {
            requestType,
            schemaPath,
            via: template?._meta?.schema ? 'template-meta' : 'cfg.requests',
          },
          'ns:schema-path'
        );
      }

      const schemaObj = await loadSchemaFromRepoOrLocal(context, owner, repo, schemaPath);
      if (!schemaObj) throw new Error(`Schema not found for path: ${schemaPath}`);
      schemaObjForValidation = schemaObj;

      // enforce identifier mapping consistency
      const schemaProps = getObjectProp(schemaObj, 'properties') || {};

      const idPropEntry = Object.entries(schemaProps).find(
        ([, def]) => isPlainObject(def) && def['x-form-field'] === 'identifier'
      );

      const hasIdentifierFieldInTemplate = Array.isArray(template?.body)
        ? template.body.some((f) => f?.id === 'identifier')
        : false;

      if (idPropEntry && !hasIdentifierFieldInTemplate) {
        const msg =
          'Configuration error: schema marks a primary identifier with x-form-field="identifier", but the form template has no field with id "identifier".';
        buckets.schema.push(msg);
        errors.push(msg);
      }

      const candidate = await projectForSchema(requestType, normalizedFormData, schemaObj);

      if (DBG && context.log?.info) {
        context.log.info({ category: requestType, keys: Object.keys(candidate), candidate }, 'schema-input');
      }

      const { validate } = getOrCreateValidator(context, schemaObj, schemaPath);

      const runCustomValidate = async (): Promise<void> => {
        if (!hooks) return;

        // Worker path: hooks is just a descriptor (raw code + hash)
        if (isHookDescriptor(hooks)) {
          const nameVal = candidate['name'];

          const args: CustomValidateArgs = {
            requestType,
            resourceName: rawIdOrNs || (typeof nameVal === 'string' ? nameVal : ''),
            candidate,
            form: normalizedFormData,
            api: null,
            log: undefined,
          };

          // Prefer the new entrypoint name first
          const fnsToTry = ['onValidate', 'customValidate'] as const;

          for (const fn of fnsToTry) {
            const res = await runHookInWorker(
              {
                owner,
                repo,
                path: hooks.__path,
                hash: hooks.__hash,
                code: hooks.__code,
                fn,
                args,
                allowedHosts,
                secrets: workerSecrets,
              },
              { timeoutMs: 8000 }
            );

            // Optional: forward worker logs into main logger
            if (res.logs?.length && context.log?.info) {
              for (const l of res.logs) {
                const msg = l.msg || `hook:${fn}`;
                if (l.level === 'error') context.log.error?.(l.obj, msg);
                else if (l.level === 'warn') context.log.warn?.(l.obj, msg);
                else if (l.level === 'debug') context.log.debug?.(l.obj, msg);
                else context.log.info?.(l.obj, msg);
              }
            }

            const hookErr = getStringProp(res.value, '__hookError');
            if (hookErr) {
              context.log?.warn?.({ err: hookErr, fn }, 'resource-bot hook validation failed');
              // if the function existed, do not fall back
              if (res.found) break;
              continue;
            }

            const msgs = normalizeHookErrors(res.value);
            if (msgs.length) {
              buckets.rules.push(...msgs);
              errors.push(...msgs);
            }

            // IMPORTANT: stop if the function existed
            if (res.found) break;
          }

          return;
        }

        // Legacy path
        const validateHook =
          typeof hooks.onValidate === 'function'
            ? hooks.onValidate
            : typeof hooks.customValidate === 'function'
              ? hooks.customValidate
              : null;

        if (!validateHook) return;

        try {
          const nameVal = candidate['name'];

          const extra = await validateHook({
            requestType,
            resourceName: rawIdOrNs || (typeof nameVal === 'string' ? nameVal : ''),
            candidate,
            form: normalizedFormData,
            api: hookApi,
            log: getHookLogger(context.log),
          });

          const msgs = normalizeHookErrors(extra);
          if (msgs.length) {
            buckets.rules.push(...msgs);
            errors.push(...msgs);
          }
        } catch (err: unknown) {
          context.log?.warn?.(
            { err: err instanceof Error ? err.message : String(err) },
            'resource-bot hooks custom validation failed'
          );
        }
      };

      const valid = validate(candidate);
      if (valid) {
        await runCustomValidate();
      } else {
        if (DBG && context.log?.info) {
          const summarized = (validate.errors || []).map((e) => ({
            path: (e as AjvErrorLike).instancePath || '/',
            keyword: (e as AjvErrorLike).keyword,
            schema: (e as AjvErrorLike).schemaPath,
            msg: (e as AjvErrorLike).message,
            params: (e as AjvErrorLike).params,
          }));
          context.log.info({ errors: summarized }, 'ns:ajv-errors');

          const extras = (validate.errors || [])
            .filter((e) => {
              const err = e as AjvErrorLike;
              return (
                err.keyword === 'additionalProperties' &&
                isPlainObject(err.params) &&
                typeof err.params?.['additionalProperty'] === 'string'
              );
            })
            .map((e) => (e as AjvErrorLike).params?.['additionalProperty'] as string);

          if (extras.length) {
            context.log.info({ extras, candidateKeys: Object.keys(candidate) }, 'ns:additional-properties');
          }
        }

        const ajvErrsRaw = Array.isArray(validate.errors) ? (validate.errors as unknown[]) : [];
        const ajvErrs = filterNoisyOneOfTypeErrors(ajvErrsRaw, candidate);

        ajvErrorsForUnifiedFormat = ajvErrs;

        const primary = dedupe(ajvErrs.map((e) => normalizeAjvMessage(e?.message)).filter(Boolean));
        if (primary.length) {
          buckets.schema.push(...primary);
          errors.push(...primary);
        }

        await runCustomValidate();
      }
    } catch (e: unknown) {
      const msg = `Configuration error: schema validation failed: ${e instanceof Error ? e.message : String(e)}`;
      buckets.schema.push(msg);
      errors.push(msg);
    }
  } else {
    const msg = `No schema configured for requestType '${requestType}' (expected cfg.requests[requestType].schema -> template._meta.schema).`;
    buckets.schema.push(msg);
    errors.push(msg);
  }

  const namespace = String(normalizedFormData.namespace || '').trim();
  await checkRegistryDuplicate(context, { owner, repo }, template, requestCfg, normalizedFormData, buckets, errors);

  const nsType = inferNsType(requestType);

  return buildValidateRequestIssueResult(errors, buckets, template, {
    schemaObj: schemaObjForValidation,
    ajvErrorsForUnifiedFormat,
    formData: normalizedFormData,
    namespace,
    nsType,
  });
}

function logApprovalHookMessages(
  context: ValidationContext,
  logs: { level: 'debug' | 'info' | 'warn' | 'error'; obj: unknown; msg?: string }[] | undefined
): void {
  if (!logs?.length) return;

  for (const entry of logs) {
    const msg = entry.msg || 'hook:onApproval';
    if (entry.level === 'error') context.log?.error?.(entry.obj, msg);
    else if (entry.level === 'warn') context.log?.warn?.(entry.obj, msg);
    else if (entry.level === 'debug') context.log?.debug?.(entry.obj, msg);
    else context.log?.info?.(entry.obj, msg);
  }
}

function getApprovalAllowedHosts(context: ValidationContext): string[] {
  return Array.isArray(context.resourceBotConfig?.hooks?.allowedHosts)
    ? context.resourceBotConfig.hooks.allowedHosts
    : [];
}

function resolveApprovalNamespace(args: {
  namespace?: string | null;
  resourceName?: string | null;
  formData: FormData;
}): string {
  return (
    toStringSafe(args.namespace) ||
    toStringSafe(args.formData['namespace']) ||
    toStringSafe(args.formData['identifier']) ||
    toStringSafe(args.resourceName)
  );
}

function resolveApprovalResourceName(
  args: {
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
  },
  namespace: string
): string {
  return (
    toStringSafe(args.resourceName) ||
    toStringSafe(args.formData['identifier']) ||
    toStringSafe(args.formData['namespace']) ||
    namespace
  );
}

function buildApprovalHookArgs(
  context: ValidationContext,
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  }
): OnApprovalArgs {
  const namespace = resolveApprovalNamespace(args);
  const resourceName = resolveApprovalResourceName(args, namespace);

  const hasExplicitRequestAuthorId = args.requestAuthorId !== undefined && args.requestAuthorId !== null;
  const requesterId = hasExplicitRequestAuthorId
    ? toStringSafe(args.requestAuthorId)
    : toStringSafe(args.issue?.user?.login);

  return {
    requestType: toStringSafe(args.requestType),
    namespace,
    resourceName,
    form: args.formData,
    data: buildApprovalHookData(args, namespace, resourceName),
    requestAuthor: {
      id: requesterId,
      email: '',
    },
    config: {
      raw: isPlainObject(context.resourceBotConfig) ? context.resourceBotConfig : {},
      approvers: getApprovalHookApprovers(context, toStringSafe(args.requestType)),
    },
    issue: {
      number: typeof args.issue?.number === 'number' ? args.issue.number : 0,
      title: toStringSafe(args.issue?.title),
      body: toStringSafe(args.issue?.body),
      state: toStringSafe(args.issue?.state),
      author: requesterId,
      labels: toApprovalIssueLabelNames(args.issue?.labels),
    },
    log: undefined,
  };
}

async function runApprovalHookDescriptor(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  hooks: HookDescriptor,
  hookArgs: OnApprovalArgs,
  allowedHosts: string[]
): Promise<ApprovalHookDecision> {
  const workerSecrets = pickHookSecretsForWorker(coreSecrets.HOOK_SECRETS || {});
  const res = await runHookInWorker(
    {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      path: hooks.__path,
      hash: hooks.__hash,
      code: hooks.__code,
      fn: 'onApproval',
      args: hookArgs,
      allowedHosts,
      secrets: workerSecrets,
    },
    { timeoutMs: 8000 }
  );

  logApprovalHookMessages(context, res.logs);

  const hookErr = getStringProp(res.value, '__hookError');
  if (hookErr) {
    context.log?.warn?.({ err: hookErr }, 'resource-bot hooks.onApproval failed');
    return {};
  }

  if (!res.found) return {};
  return normalizeApprovalHookResult(res.value);
}

async function runApprovalHookInProcess(
  context: ValidationContext,
  hooks: ResourceBotHooks,
  hookArgs: OnApprovalArgs
): Promise<ApprovalHookDecision> {
  const onApprovalHook = hooks.onApproval;
  if (typeof onApprovalHook !== 'function') return {};

  try {
    const ret = await onApprovalHook({
      ...hookArgs,
      log: getHookLogger(context.log),
    });

    return normalizeApprovalHookResult(ret);
  } catch (err: unknown) {
    context.log?.warn?.(
      { err: err instanceof Error ? err.message : String(err) },
      'resource-bot hooks.onApproval failed'
    );
    return {};
  }
}

export async function runApprovalHook(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  args: {
    requestType: string;
    namespace?: string | null;
    resourceName?: string | null;
    formData: FormData;
    issue: IssueLike;
    requestAuthorId?: string | null;
  }
): Promise<ApprovalHookDecision> {
  await ensureStaticConfigLoaded(context);

  const hooks = getResourceBotHooks(context);
  if (!hooks) return {};

  const allowedHosts = getApprovalAllowedHosts(context);
  const hookArgs = buildApprovalHookArgs(context, args);

  if (isHookDescriptor(hooks)) {
    return runApprovalHookDescriptor(context, repoInfo, hooks, hookArgs, allowedHosts);
  }

  return runApprovalHookInProcess(context, hooks, hookArgs);
}

// CI helper: run the same onValidate hook pipeline the bot uses,
// but against an already materialized registry entry
export async function runCustomValidateForRegistryCandidate(
  context: ValidationContext,
  repoInfo: { owner: string; repo: string },
  args: {
    requestType: string;
    schema: unknown;
    candidate: CandidateData;
    resourceName?: string | null;
    formData?: FormData | null;
  }
): Promise<string[]> {
  const hooks = getResourceBotHooks(context);

  if (!hooks) return [];

  const allowedHosts = Array.isArray(context.resourceBotConfig?.hooks?.allowedHosts)
    ? context.resourceBotConfig.hooks.allowedHosts
    : [];

  const form =
    args.formData && isPlainObject(args.formData)
      ? normalizeFormDataForHookValidation(args.requestType, args.formData, args.schema, null)
      : await buildFormDataForHookValidationFromCandidate(args.requestType, args.schema, args.candidate);

  const normalizedResourceName = toStringSafe(form.identifier) || toStringSafe(form.namespace);
  const inferredResourceName = resolvePrimaryIdFromCandidate(args.candidate, args.schema);

  const resourceName =
    normalizedResourceName ||
    inferredResourceName ||
    toStringSafe(args.resourceName) ||
    toStringSafe(getRecordProp(args.candidate, 'product-id')) ||
    toStringSafe(getRecordProp(args.candidate, 'productId')) ||
    toStringSafe(getRecordProp(args.candidate, 'id')) ||
    toStringSafe(getRecordProp(args.candidate, 'name')) ||
    toStringSafe(getRecordProp(args.candidate, 'identifier')) ||
    toStringSafe(getRecordProp(args.candidate, 'namespace')) ||
    toStringSafe(getRecordProp(args.candidate, 'vendor'));

  const onValidateHook = hooks.onValidate;
  const isLegacyHook = typeof onValidateHook === 'function';

  if (isLegacyHook) {
    // Legacy hooks object
    try {
      const extra = await onValidateHook({
        requestType: args.requestType,
        resourceName,
        candidate: args.candidate,
        form,
        api: createHookApi(context, {
          secrets: coreSecrets.HOOK_SECRETS || {},
          allowedHosts,
        }),
        log: getHookLogger(context.log),
      });

      return normalizeHookErrors(extra);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return [`Hook onValidate failed: ${msg}`];
    }
  }

  // Descriptor-based hooks
  if (isHookDescriptor(hooks)) {
    const workerSecrets = pickHookSecretsForWorker(coreSecrets.HOOK_SECRETS || {});
    const hookArgs: CustomValidateArgs = {
      requestType: args.requestType,
      resourceName,
      candidate: args.candidate,
      form,
      api: null,
      log: undefined,
    };

    const res = await runHookInWorker(
      {
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        path: hooks.__path,
        hash: hooks.__hash,
        code: hooks.__code,
        fn: 'onValidate',
        args: hookArgs,
        allowedHosts,
        secrets: workerSecrets,
      },
      { timeoutMs: 8000 }
    );

    if (res.logs?.length && context.log?.info) {
      for (const l of res.logs) {
        const msg = l.msg || 'hook:onValidate';
        if (l.level === 'error') context.log.error?.(l.obj, msg);
        else if (l.level === 'warn') context.log.warn?.(l.obj, msg);
        else if (l.level === 'debug') context.log.debug?.(l.obj, msg);
        else context.log.info?.(l.obj, msg);
      }
    }

    const msgs = normalizeHookErrors(res.value);
    return msgs.length ? msgs : [];
  }

  return [];
}
