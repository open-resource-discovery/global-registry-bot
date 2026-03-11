import { parseForm as parseFormRaw, loadTemplate as loadTemplateRaw } from '../template.js';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { loadStaticConfig } from '../../../config.js';
import { loadSecrets } from '../../../utils/secrets.js';
import { createHookApi as createHookApiRaw } from './hook-api.js';
import Ajv2020Module from 'ajv/dist/2020.js';
import type { ValidateFunction } from 'ajv';
import { runHookInWorker } from './hook-pool.js';
import addFormatsModule from 'ajv-formats';
import ajvErrorsModule from 'ajv-errors';

import meta2020_12 from 'ajv/dist/refs/json-schema-2020-12/schema.json' with { type: 'json' };

const META_2020_12_ID = 'https://json-schema.org/draft/2020-12/schema';

const fileName = fileURLToPath(import.meta.url);
const dirName = dirname(fileName);

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
  resourceBotHooks?: ResourceBotHooks | null;
  resourceBotHooksSource?: string | null;
};

type IssueLike = {
  body?: string | null;
  title?: string | null;
  labels?: (string | { name?: string | null })[] | null;
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

  if (!grouped.has(k)) grouped.set(k, []);
  grouped.get(k)!.push(m);
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
  if (s.includes('identifier') || s.includes('namespace') || s.includes('product id')) return primary;
  if (s.includes('title') && idMap.get('title')) return idMap.get('title') || '';
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

  if (!grouped.size) return '';
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
    if (!field?.id) continue;
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

function isPrimaryResourceField(v: unknown): boolean {
  const t = normalizePrimaryResourceToken(v);
  return t === 'identifier' || t === 'namespace' || t === 'productid' || t === 'id' || t === 'name' || t === 'vendor';
}

function resolvePrimaryIdFromRecord(schemaObj: unknown, record: Record<string, unknown>): string {
  const asTrimmed = (v: unknown): string => toStringSafe(v).replaceAll('\u00a0', ' ').trim();

  const directKeys = ['identifier', 'namespace', 'product-id', 'productId', 'id', 'name', 'vendor'];
  for (const key of directKeys) {
    const value = asTrimmed(record[key]);
    if (value) return value;
  }

  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (!schemaProps) return '';

  for (const [propName, propDef] of Object.entries(schemaProps)) {
    if (!isPlainObject(propDef)) continue;

    const ff = toStringSafe(propDef['x-form-field']);
    if (!isPrimaryResourceField(ff)) continue;

    const viaField = asTrimmed(record[ff]);
    if (viaField) return viaField;

    const viaProp = asTrimmed(record[propName]);
    if (viaProp) return viaProp;
  }

  for (const propName of Object.keys(schemaProps)) {
    if (!isPrimaryResourceField(propName)) continue;

    const viaProp = asTrimmed(record[propName]);
    if (viaProp) return viaProp;
  }

  return '';
}

export function resolvePrimaryIdFromCandidate(candidate: Record<string, unknown>, schemaObj: unknown): string {
  return resolvePrimaryIdFromRecord(schemaObj, candidate);
}

export function resolvePrimaryIdFromTemplate(template: TemplateLike, formData: FormData, schemaObj: unknown): string {
  if (!template) return '';
  const asTrimmed = (v: unknown): string => (typeof v === 'string' ? v.trim() : '');

  const directIdentifier = asTrimmed(formData.identifier);
  if (directIdentifier) return directIdentifier;

  const directNamespace = asTrimmed(formData.namespace);
  if (directNamespace) return directNamespace;

  const directProductId = asTrimmed(formData['product-id'] || formData.productId);
  if (directProductId) return directProductId;

  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (schemaProps) {
    for (const [propName, propDef] of Object.entries(schemaProps)) {
      if (isPlainObject(propDef) && propDef['x-form-field'] === 'identifier') {
        const viaIdentifier = asTrimmed(formData.identifier);
        if (viaIdentifier) return viaIdentifier;
        const viaProp = asTrimmed((formData as Record<string, unknown>)[propName]);
        if (viaProp) return viaProp;
        break;
      }
    }
  }

  const viaGeneric = resolvePrimaryIdFromRecord(schemaObj, formData as Record<string, unknown>);
  if (viaGeneric) return viaGeneric;

  return pickIdentifierFromFields(template, formData);
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

async function buildFormDataForHookValidationFromCandidate(
  requestType: string,
  schemaObj: unknown,
  candidate: CandidateData
): Promise<FormData> {
  const out: FormData = {};
  const props = getObjectProp(schemaObj, 'properties') || {};
  const candidateRec = isPlainObject(candidate) ? candidate : {};

  for (const [propName, propDef] of Object.entries(props)) {
    const raw = getRecordProp(candidateRec, propName);
    if (raw === undefined || raw === null) continue;

    const serialized = await stringifyCandidateValueForForm(raw);
    if (!serialized) continue;

    const ff = isPlainObject(propDef) ? toStringSafe(propDef['x-form-field']).trim() : '';

    if (ff && !out[ff]) out[ff] = serialized;
    if (!out[propName]) out[propName] = serialized;
  }

  for (const [k, v] of Object.entries(candidateRec)) {
    if (v === undefined || v === null) continue;
    if (out[k]) continue;

    const serialized = await stringifyCandidateValueForForm(v);
    if (!serialized) continue;
    out[k] = serialized;
  }

  return normalizeFormDataForHookValidation(requestType, out, schemaObj, null);
}

// AJV init + caches
const SCHEMA_CACHE = new Map<string, ValidateFunction<unknown>>();
const AJV_CACHE = new Map<string, AjvInstance>();
const REPO_SCHEMA_CACHE = new Map<string, unknown>();

function initAjvInstance(ajv: AjvInstance, context: ValidationContext): void {
  // Ensure draft 2020-12 meta-schema is present so "$schema: .../2020-12/schema" works.
  try {
    if (!ajv.getSchema(META_2020_12_ID)) {
      ajv.addMetaSchema(meta2020_12);
      ajv.defaultMeta = META_2020_12_ID;
    }
  } catch {
    // ignore
  }

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
      addCandidate(candidates, raw);
    } else {
      const cleaned = raw.replace(/^\.?\//, '');
      addCandidate(candidates, `${CONFIG_BASE_DIR}/${cleaned}`);
      for (const base of searchPaths) addCandidate(candidates, `${base.replace(/^\.?\//, '')}/${cleaned}`);
      addCandidate(candidates, cleaned);
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

  // 4) requestType & cfg are authoritative from template._meta (injected via cfg.requests)
  let requestType = String(template?._meta?.requestType || '').trim();

  // Partner Namespace template can target multiple request types via the form enum `requestType`.
  if (requestType && requestType.toLowerCase() === 'partnernamespace') {
    const selected =
      (formData as Record<string, unknown>)['requestType'] ?? (formData as Record<string, unknown>)['request-type'];

    const mapped = mapPartnerNamespaceRequestTypeToConfigKey(selected);
    if (!mapped) {
      const msg = `Invalid Partner Namespace 'Request Type' selection '${toStringSafe(selected) || ''}'. Expected one of: authority, system, subContext.`;
      buckets.form.push(msg);
      errors.push(msg);

      const unified = formatUnifiedIssues(buckets, template, []);
      return {
        errors,
        errorsGrouped: buckets,
        errorsFormatted: unified || formatBuckets(buckets),
        errorsFormattedSingle: unified || formatFirstBucket(buckets),
        formData: {},
        template,
        namespace: '',
        nsType: '',
      };
    }

    const mappedCfg = getRequestConfig(context, mapped);
    if (!mappedCfg) {
      const msg = `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests has no such entry.`;
      buckets.schema.push(msg);
      errors.push(msg);

      const unified = formatUnifiedIssues(buckets, template, []);
      return {
        errors,
        errorsGrouped: buckets,
        errorsFormatted: unified || formatBuckets(buckets),
        errorsFormattedSingle: unified || formatFirstBucket(buckets),
        formData: {},
        template,
        namespace: '',
        nsType: '',
      };
    }

    const mappedSchema = toStringSafe(mappedCfg.schema);
    if (!mappedSchema) {
      const msg = `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests['${mapped}'].schema is empty.`;
      buckets.schema.push(msg);
      errors.push(msg);

      const unified = formatUnifiedIssues(buckets, template, []);
      return {
        errors,
        errorsGrouped: buckets,
        errorsFormatted: unified || formatBuckets(buckets),
        errorsFormattedSingle: unified || formatFirstBucket(buckets),
        formData: {},
        template,
        namespace: '',
        nsType: '',
      };
    }

    // Clone template meta to avoid mutating cached templates across issues.
    template = {
      ...template,
      _meta: {
        ...(template._meta || {}),
        requestType: mapped,
        schema: mappedSchema,
        root: toStringSafe(mappedCfg.folderName),
      },
    };

    requestType = mapped;
  }
  const requestCfg = getRequestConfig(context, requestType);

  if (!requestType) {
    const msg =
      'Configuration error: template missing _meta.requestType (expected cfg.requests mapping via loadTemplate).';
    buckets.schema.push(msg);
    errors.push(msg);

    const unified = formatUnifiedIssues(buckets, template, []);
    return {
      errors,
      errorsGrouped: buckets,
      errorsFormatted: unified || formatBuckets(buckets),
      errorsFormattedSingle: unified || formatFirstBucket(buckets),
      formData: {},
      template,
      namespace: '',
      nsType: '',
    };
  }

  if (!requestCfg) {
    const msg = `Configuration error: unknown requestType '${requestType}' (missing cfg.requests entry).`;
    buckets.schema.push(msg);
    errors.push(msg);

    const unified = formatUnifiedIssues(buckets, template, []);
    return {
      errors,
      errorsGrouped: buckets,
      errorsFormatted: unified || formatBuckets(buckets),
      errorsFormattedSingle: unified || formatFirstBucket(buckets),
      formData: {},
      template,
      namespace: '',
      nsType: '',
    };
  }

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

  const rawResolved = resolvePrimaryIdFromTemplate(template, formData, schemaObjForId) || '';
  const rawIdOrNs = rawResolved.replaceAll('\u00a0', ' ').trim();

  if (!rawIdOrNs) {
    const msg = 'Cannot resolve primary identifier from template';
    buckets.form.push(msg);
    const unified = formatUnifiedIssues(buckets, template, []);
    return {
      errors: [msg],
      errorsGrouped: buckets,
      errorsFormatted: unified || formatBuckets(buckets),
      errorsFormattedSingle: unified || formatFirstBucket(buckets),
      formData: {},
      template,
      namespace: '',
      nsType: '',
    };
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

  // 9) registry existence check
  const namespace = String(normalizedFormData.namespace || '').trim();

  if (namespace) {
    const resourceName = String(normalizedFormData.identifier || normalizedFormData.namespace || '').trim();

    if (resourceName) {
      try {
        const structRoot = resolveRegistryRootForTemplate(context, template, requestCfg);
        const filePath = `${structRoot}/${resourceName}.yaml`;

        await context.octokit.repos.getContent({ owner, repo, path: filePath });

        const msg = `Resource '${resourceName}' already exists in registry`;
        buckets.registry.push(msg);
        errors.push(msg);
      } catch (e: unknown) {
        if (getHttpStatus(e) !== 404) {
          context.log?.warn?.({ err: e instanceof Error ? e.message : String(e) }, 'registry existence check failed');
        }
      }
    }
  }

  const nsType = inferNsType(requestType);

  // 10) output formatting
  const unified = formatUnifiedIssues(buckets, template, ajvErrorsForUnifiedFormat);
  const errorsFormatted = unified || formatBuckets(buckets);
  const errorsFormattedSingle = unified || formatFirstBucket(buckets);

  return {
    errors,
    errorsGrouped: buckets,
    errorsFormatted,
    errorsFormattedSingle,
    formData: normalizedFormData,
    template,
    namespace,
    nsType,
  };
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
