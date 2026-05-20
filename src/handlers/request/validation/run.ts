import { parseForm as parseFormRaw, loadTemplate as loadTemplateRaw } from '../template.js';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadStaticConfig, type RegistryBotHooks as StaticRegistryBotHooks } from '../../../config.js';
import { loadSecrets } from '../../../utils/secrets.js';
import { createHookApi as createHookApiRaw } from './hook-api.js';
import Ajv2020Module from 'ajv/dist/2020.js';
import type { ValidateFunction } from 'ajv';
import {
  buildFormDataForHookValidationFromCandidate,
  normalizeFormDataForHookValidation,
  projectForSchema,
  resolvePrimaryIdFromCandidate,
  resolvePrimaryIdFromTemplate,
} from './form-schema-projection.js';
import { runApprovalHookRuntime } from './hook-approval-runtime.js';
import { runBeforeValidateHookRuntime } from './hook-before-validate-runtime.js';
import { runRegistryCustomValidateRuntime } from './hook-registry-custom-validate-runtime.js';
import { runValidationHookRuntime } from './hook-validation-runtime.js';
import { buildMissingTemplateResult, buildValidateRequestIssueResult } from './validation-result-formatting.js';
import addFormatsModule from 'ajv-formats';
import ajvErrorsModule from 'ajv-errors';

export {
  projectForSchema,
  resolvePrimaryIdFromCandidate,
  resolvePrimaryIdFromTemplate,
} from './form-schema-projection.js';

const moduleFileName = fileURLToPath(import.meta.url);
const dirName = dirname(moduleFileName);

const DBG = process.env.DEBUG_NS === '1';

const CONFIG_BASE_DIR = '.github/registry-bot';

type HookSecrets = Readonly<Record<string, string | undefined>>;

type HookWorkerConfig = Readonly<Record<string, string>>;

type HookRuntimeConfig = Readonly<
  Record<string, string | ((key: string) => string)> & {
    getSecret: (key: string) => string;
  }
>;

type HookConfig = HookWorkerConfig | HookRuntimeConfig;

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
  config: HookConfig;
  log?: LoggerLike | undefined;
}>;

type CustomValidateArgs = Readonly<{
  requestType: string;
  resourceName: string;
  candidate: Record<string, unknown>;
  form: FormData;
  api: unknown;
  config: HookConfig;
  requestAuthor: Readonly<{
    id: string;
    email?: string;
  }>;
  issue: Readonly<{
    number: number;
    title: string;
    body: string;
    state: string;
    author: string;
    labels: string[];
  }>;
  parentResourceName?: string;
  parentCandidate?: Readonly<Record<string, unknown>> | null;
  parentOwners?: readonly string[];
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

function normalizeHookSecretName(value: unknown): string {
  return toStringSafe(value)
    .replace(/^HOOK_SECRET_/i, '')
    .trim();
}

function buildHookWorkerConfig(secrets: HookSecrets): HookWorkerConfig {
  const out: Record<string, string> = {};

  for (const [key, value] of Object.entries(secrets || {})) {
    if (typeof value !== 'string') continue;

    const secretName = normalizeHookSecretName(key);
    const secretValue = value.trim();

    if (!secretName || !secretValue) continue;

    out[secretName] = secretValue;
  }

  return Object.freeze(out);
}

function buildHookRuntimeConfig(secrets: HookSecrets): HookRuntimeConfig {
  const values = buildHookWorkerConfig(secrets);

  return Object.freeze({
    ...values,
    getSecret: (key: string): string => values[normalizeHookSecretName(key)] || '',
  }) as HookRuntimeConfig;
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

function approvalIssueLabelName(value: unknown): string {
  if (typeof value === 'string') return toStringSafe(value);
  if (isPlainObject(value)) return toStringSafe(value['name']);
  return '';
}

function toApprovalIssueLabelNames(labels: IssueLike['labels']): string[] {
  const items = Array.isArray(labels) ? labels : [];
  return items.map((label) => approvalIssueLabelName(label)).filter(Boolean);
}

function buildValidationHookIssue(issue: IssueLike): CustomValidateArgs['issue'] {
  const author = toStringSafe(issue?.user?.login);

  return {
    number: typeof issue?.number === 'number' ? issue.number : 0,
    title: toStringSafe(issue?.title),
    body: toStringSafe(issue?.body),
    state: toStringSafe(issue?.state),
    author,
    labels: toApprovalIssueLabelNames(issue?.labels),
  };
}

function buildValidationHookRequestAuthor(issue: IssueLike): CustomValidateArgs['requestAuthor'] {
  return {
    id: toStringSafe(issue?.user?.login),
  };
}

function resolveUpperNamespaceName(resourceName: unknown): string {
  const parts = toStringSafe(resourceName)
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean);

  if (parts.length <= 2) return '';

  return parts.slice(0, -1).join('.');
}

async function parseYamlObject(raw: string): Promise<Record<string, unknown> | null> {
  try {
    const yamlMod = (await import('yaml')) as unknown as {
      parse?: (src: string) => unknown;
      default?: { parse?: (src: string) => unknown };
    };

    const parse = yamlMod.parse || yamlMod.default?.parse;
    if (typeof parse !== 'function') return null;

    const parsed = parse(raw);
    return isPlainObject(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function readRepoYamlObject(
  context: ValidationContext,
  owner: string,
  repo: string,
  basePath: string
): Promise<Record<string, unknown> | null> {
  for (const ext of ['yaml', 'yml']) {
    try {
      const res = await context.octokit.repos.getContent({
        owner,
        repo,
        path: `${basePath}.${ext}`,
      });

      const data = res.data;
      if (Array.isArray(data) || !isRepoContentFile(data)) continue;

      const text = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
      const parsed = await parseYamlObject(text);
      if (parsed) return parsed;
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) continue;
      throw e;
    }
  }

  return null;
}

async function resolveParentCandidateForValidationHook(
  context: ValidationContext,
  owner: string,
  repo: string,
  template: TemplateLike,
  requestCfg: RequestConfigEntry,
  resourceName: string
): Promise<{ parentResourceName: string; parentCandidate: Record<string, unknown> | null }> {
  const parentResourceName = resolveUpperNamespaceName(resourceName);
  if (!parentResourceName) {
    return { parentResourceName: '', parentCandidate: null };
  }

  const root = resolveRegistryRootForTemplate(context, template, requestCfg);
  const parentCandidate = await readRepoYamlObject(context, owner, repo, `${root}/${parentResourceName}`);

  return {
    parentResourceName,
    parentCandidate,
  };
}

function addNormalizedOwnerReference(out: Set<string>, value: unknown): void {
  const raw = toStringSafe(value);
  if (!raw) return;

  const githubUrlMatch = /(?:github\.com|github\.tools\.sap)\/([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?)/i.exec(
    raw
  );

  if (githubUrlMatch?.[1]) {
    out.add(normalizeLoginValue(githubUrlMatch[1]).toLowerCase());
  }

  for (const part of raw.split(/[,\s;]+/)) {
    const cleaned = toStringSafe(part).replace(/^@+/, '').toLowerCase();
    if (!cleaned) continue;

    if (cleaned.includes('@')) {
      out.add(cleaned);
      continue;
    }

    if (/^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/i.test(cleaned)) {
      out.add(cleaned);
    }
  }
}

function collectNormalizedOwnerReferences(out: Set<string>, value: unknown): void {
  if (value === null || value === undefined) return;

  if (Array.isArray(value)) {
    for (const item of value) collectNormalizedOwnerReferences(out, item);
    return;
  }

  if (isPlainObject(value)) {
    for (const item of Object.values(value)) collectNormalizedOwnerReferences(out, item);
    return;
  }

  addNormalizedOwnerReference(out, value);
}

function resolveParentOwnersForValidationHook(parentCandidate: Record<string, unknown> | null): string[] {
  if (!parentCandidate) return [];

  const out = new Set<string>();

  collectNormalizedOwnerReferences(
    out,
    parentCandidate['contacts'] ?? parentCandidate['contact'] ?? parentCandidate['owners'] ?? parentCandidate['owner']
  );

  return Array.from(out).filter(Boolean).sort();
}

async function buildCustomValidateContextArgs(
  context: ValidationContext,
  owner: string,
  repo: string,
  issue: IssueLike,
  template: TemplateLike,
  requestCfg: RequestConfigEntry,
  resourceName: string
): Promise<
  Pick<CustomValidateArgs, 'requestAuthor' | 'issue' | 'parentResourceName' | 'parentCandidate' | 'parentOwners'>
> {
  const parent = await resolveParentCandidateForValidationHook(
    context,
    owner,
    repo,
    template,
    requestCfg,
    resourceName
  );

  return {
    requestAuthor: buildValidationHookRequestAuthor(issue),
    issue: buildValidationHookIssue(issue),
    parentResourceName: parent.parentResourceName,
    parentCandidate: parent.parentCandidate,
    parentOwners: resolveParentOwnersForValidationHook(parent.parentCandidate),
  };
}

function normalizeLoginValue(value: unknown): string {
  return toStringSafe(value).replace(/^@+/, '').trim();
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
  const hookWorkerConfig = buildHookWorkerConfig(coreSecrets.HOOK_SECRETS || {});
  const hookRuntimeConfig = buildHookRuntimeConfig(coreSecrets.HOOK_SECRETS || {});

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
    await runBeforeValidateHookRuntime({
      hooks,
      owner,
      repo,
      requestType,
      formData,
      allowedHosts,
      workerSecrets,
      hookWorkerConfig,
      hookRuntimeConfig,
      hookApi,
      log: context.log,
      isHookDescriptor,
      getHookLogger,
      getStringProp,
      getObjectProp,
    });
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

        const customValidateContextArgs = await buildCustomValidateContextArgs(
          context,
          owner,
          repo,
          issue,
          template,
          requestCfg,
          rawIdOrNs
        );

        await runValidationHookRuntime({
          hooks,
          owner,
          repo,
          requestType,
          rawIdOrNs,
          candidate,
          normalizedFormData,
          customValidateContextArgs,
          hookApi,
          hookWorkerConfig,
          hookRuntimeConfig,
          allowedHosts,
          workerSecrets,
          log: context.log,
          isHookDescriptor,
          getStringProp,
          normalizeHookErrors,
          getHookLogger,
          rulesBucket: buckets.rules,
          errors,
        });
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

  return runApprovalHookRuntime(context, repoInfo, args, {
    hookSecrets: coreSecrets.HOOK_SECRETS || {},
  });
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
  await ensureStaticConfigLoaded(context);

  const hooks = getResourceBotHooks(context);

  if (!hooks) return [];

  const allowedHosts = Array.isArray(context.resourceBotConfig?.hooks?.allowedHosts)
    ? context.resourceBotConfig.hooks.allowedHosts
    : [];

  const hookWorkerConfig = buildHookWorkerConfig(coreSecrets.HOOK_SECRETS || {});
  const hookRuntimeConfig = buildHookRuntimeConfig(coreSecrets.HOOK_SECRETS || {});

  return runRegistryCustomValidateRuntime({
    context,
    hooks,
    repoInfo,
    requestType: args.requestType,
    schema: args.schema,
    candidate: args.candidate,
    resourceName: args.resourceName,
    formData: args.formData,
    allowedHosts,
    hookSecrets: coreSecrets.HOOK_SECRETS || {},
    hookWorkerConfig,
    hookRuntimeConfig,
    isHookDescriptor,
    getHookLogger,
    normalizeHookErrors,
    buildFormDataForHookValidationFromCandidate,
    normalizeFormDataForHookValidation,
    resolvePrimaryIdFromCandidate,
    getRecordProp,
    toStringSafe,
    pickHookSecretsForWorker,
    createHookApi,
  });
}
