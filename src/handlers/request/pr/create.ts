import jsYamlModule from 'js-yaml';
import { calcSnapshotHash as calcSnapshotHashRaw, SNAPSHOT_HASH_MARKER_KEY } from './snapshot.js';
import {
  tryEnableAutoMerge as tryEnableAutoMergeRaw,
  tryMergeIfGreen as tryMergeIfGreenRaw,
} from '../../../lib/auto-merge.js';
import { loadTemplate as loadTemplateRaw } from '../template.js';
import { loadStaticConfig as loadStaticConfigRaw } from '../../../config.js';
import {
  resolvePrimaryIdFromTemplate as resolvePrimaryIdFromTemplateRaw,
  projectForSchema as projectForSchemaRaw,
} from '../validation/run.js';

// All root/type/title/labels must come from user config or template meta.

const CONFIG_BASE_DIR = '.github/registry-bot';

type JsonObject = Record<string, unknown>;
type JsonValue = unknown;

type MergeMethodRest = 'merge' | 'squash' | 'rebase';
type MergeMethodGraphql = 'MERGE' | 'SQUASH' | 'REBASE';

type GitHubLabel = string | { name?: string | null };

type IssueLike = {
  number: number;
  title?: string | null;
  body?: string | null;
  labels?: GitHubLabel[] | null;
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
  title?: string;
  name?: string;
  body?: TemplateField[];
  _meta?: TemplateMeta;
  [k: string]: unknown;
};

type PullRequestLike = {
  number: number;
  node_id: string;
  body?: string | null;
  draft?: boolean;
  state?: string;
  head: { ref: string; sha: string };
};

type RepoGetResponse = { default_branch: string };
type BranchGetResponse = { commit?: { sha?: string } };

type RepoContentFile = { content: string; encoding?: string; sha?: string };
type RepoContentResponse = RepoContentFile | RepoContentFile[] | unknown;

type LoggerLike = {
  info?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  error?: (obj: unknown, msg?: string) => void;
  debug?: (obj: unknown, msg?: string) => void;
};

type OctokitLike = {
  repos: {
    get: (args: { owner: string; repo: string }) => Promise<{ data: RepoGetResponse }>;
    getBranch: (args: { owner: string; repo: string; branch: string }) => Promise<{ data: BranchGetResponse }>;
    getContent: (args: {
      owner: string;
      repo: string;
      path: string;
      ref?: string;
    }) => Promise<{ data: RepoContentResponse }>;
    createOrUpdateFileContents: (args: {
      owner: string;
      repo: string;
      path: string;
      message: string;
      content: string; // base64
      branch: string;
      sha?: string;
    }) => Promise<unknown>;
  };
  git: {
    createRef: (args: { owner: string; repo: string; ref: string; sha: string }) => Promise<unknown>;
  };
  pulls: {
    list: (args: {
      owner: string;
      repo: string;
      state?: 'open' | 'closed' | 'all';
      head?: string;
    }) => Promise<{ data: PullRequestLike[] }>;
    create: (args: {
      owner: string;
      repo: string;
      title: string;
      head: string;
      base: string;
      body?: string;
      maintainer_can_modify?: boolean;
    }) => Promise<{ data: PullRequestLike }>;
  };
  issues: {
    addLabels: (args: { owner: string; repo: string; issue_number: number; labels: string[] }) => Promise<unknown>;
  };
};

type ResourceBotConfig = {
  pr?: {
    branchNameTemplate?: string;
    titleTemplate?: string;
    commitMessageTemplate?: string;
    bodyFooter?: string;
    baseBranch?: string;
    autoMerge?: { enabled?: boolean | null; method?: string | null };
  };
  workflow?: { labels?: { autoMergeCandidate?: string | null } };
  schema?: { searchPaths?: string[] };
  [k: string]: unknown;
};

type ContextLike = {
  octokit: OctokitLike;
  log: LoggerLike;
  resourceBotConfig?: ResourceBotConfig;
};

type CreateRequestPrOptions = {
  template?: TemplateLike;
};

type EffectivePrOptions = {
  branchTemplate: string;
  prTitleTemplate: string;
  commitMessageTemplate: string;
  bodyFooter: string;
  autoMergeEnabled: boolean;
  autoMergeMethod: string;
  autoMergeLabel: string;
  schemaSearchPaths: string[];
  baseBranch: string;
};

// imported functions
const calcSnapshotHash = calcSnapshotHashRaw as unknown as (
  formData: Record<string, unknown>,
  template: TemplateLike,
  issueBody: string
) => string;

const loadTemplate = loadTemplateRaw as unknown as (
  context: ContextLike,
  args: { owner: string; repo: string; issueTitle?: string; issueLabels?: string[] }
) => Promise<TemplateLike>;

const loadStaticConfig = loadStaticConfigRaw as unknown as (
  context: unknown,
  options: { validate: boolean; updateIssue: boolean }
) => Promise<{ config: ResourceBotConfig }>;

const resolvePrimaryIdFromTemplate = resolvePrimaryIdFromTemplateRaw as unknown as (
  template: TemplateLike,
  formData: Record<string, unknown>,
  schemaObj: JsonValue
) => string;

const projectForSchema = projectForSchemaRaw as unknown as (
  category: string,
  form: Record<string, unknown>,
  schemaObj: JsonValue
) => Promise<JsonObject>;

const tryEnableAutoMerge = tryEnableAutoMergeRaw as unknown as (
  context: unknown,
  pr: Pick<PullRequestLike, 'number' | 'node_id'>,
  opts?: { mergeMethod?: MergeMethodGraphql }
) => Promise<boolean>;

const tryMergeIfGreen = tryMergeIfGreenRaw as unknown as (
  context: unknown,
  args: {
    owner: string;
    repo: string;
    prNumber: number;
    mergeMethod?: MergeMethodRest;
    requireApproval?: boolean;
    prData?: PullRequestLike;
  }
) => Promise<boolean>;

// helpers
function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const s = err['status'];
  return typeof s === 'number' ? s : undefined;
}

function isRepoContentFile(data: unknown): data is RepoContentFile {
  return isPlainObject(data) && typeof data.content === 'string';
}

type JsYamlApi = {
  dump: (obj: unknown, opts?: Record<string, unknown>) => string;
  load: (src: string, opts?: Record<string, unknown>) => unknown;
  JSON_SCHEMA?: unknown;
};

const jsYaml = jsYamlModule as unknown as JsYamlApi;

function sanitizeForYaml(value: unknown): unknown {
  if (value === undefined) return undefined;
  if (value === null) return null;

  const t = typeof value;
  if (t === 'string' || t === 'boolean') return value;
  if (t === 'number') return Number.isFinite(value as number) ? value : String(value);
  if (t === 'bigint') return (value as bigint).toString();

  if (value instanceof Date) return value.toISOString();

  if (Array.isArray(value)) {
    const out = value.map(sanitizeForYaml).filter((v) => v !== undefined) as unknown[];
    return out;
  }

  if (isPlainObject(value)) {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value)) {
      const v = sanitizeForYaml(value[k]);
      if (v === undefined) continue;
      out[k] = v;
    }
    return out;
  }

  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
    return value.toString('base64');
  }

  return undefined;
}

function dumpYamlDoc(value: unknown): string {
  const sanitized = sanitizeForYaml(value);
  const doc = sanitized === undefined ? {} : sanitized;

  return jsYaml.dump(doc, {
    indent: 2,
    lineWidth: -1,
    noRefs: true,
    sortKeys: false,
    skipInvalid: true,
  });
}

function loadYamlDoc(src: string): unknown {
  const schema = jsYaml.JSON_SCHEMA;
  if (schema) {
    const opts: { schema: unknown } = { schema };
    return jsYaml.load(src, opts);
  }
  return jsYaml.load(src);
}

// simple repo schema JSON cache
const REPO_SCHEMA_CACHE = new Map<string, JsonValue>();

// PR options from config.yaml (pr + workflow.labels)
const getEffectivePrOptions = (context: ContextLike): EffectivePrOptions => {
  const cfg = context?.resourceBotConfig || {};
  const pr = cfg.pr || {};
  const wf = cfg.workflow || {};
  const labels = wf.labels || {};
  const autoMergeCfg = pr.autoMerge || {};
  const schemaCfg = cfg.schema || {};

  const autoMergeMethod = String(autoMergeCfg.method || 'squash').toLowerCase();

  // Optional extra searchPaths for schemas
  const schemaSearchPaths =
    Array.isArray(schemaCfg.searchPaths) && schemaCfg.searchPaths.length
      ? schemaCfg.searchPaths
      : ['schema', '.github/registry-bot/request-schemas', '.'];

  // Commit message template - fall back to a sensible default
  const commitMessageTemplate =
    typeof pr.commitMessageTemplate === 'string' && pr.commitMessageTemplate.trim()
      ? pr.commitMessageTemplate
      : 'chore({root}): register {resource} (#${issue})';

  // Body footer: own key preferred, fallback to commit template
  const bodyFooter =
    typeof pr.bodyFooter === 'string'
      ? pr.bodyFooter
      : typeof pr.commitMessageTemplate === 'string'
        ? pr.commitMessageTemplate
        : '';

  return {
    branchTemplate: pr.branchNameTemplate || 'feat/resource-{resource}-issue-{issue}',
    prTitleTemplate: pr.titleTemplate || 'Register {type} {resource}',
    commitMessageTemplate,
    bodyFooter,
    autoMergeEnabled: typeof autoMergeCfg.enabled === 'boolean' ? autoMergeCfg.enabled : true,
    autoMergeMethod,
    autoMergeLabel: labels.autoMergeCandidate || 'auto-merge-candidate',
    schemaSearchPaths,
    baseBranch: typeof pr.baseBranch === 'string' ? pr.baseBranch.trim() : '',
  };
};

const buildSafeResourceSlug = (resourceName: unknown): string =>
  String(resourceName || '')
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, '-')
    .replace(/-+/g, '-');

const applyBranchTemplate = (template: unknown, resourceName: string, issueNumber: number): string => {
  const safe = buildSafeResourceSlug(resourceName);
  return String(template || '')
    .replace('{resource}', safe)
    .replace('{issue}', String(issueNumber || ''));
};

const buildCommitMessage = (template: unknown, root: string, resourceName: string, issueNumber: number): string => {
  const t = String(template || '');
  if (!t) return `chore(${root}): register ${resourceName} (#${issueNumber})`;
  return t
    .replace('{root}', root || '')
    .replace('{resource}', resourceName || '')
    .replace('{issue}', String(issueNumber || ''));
};

const buildPrTitle = (template: unknown, type: string, resourceName: string, root: string): string => {
  const t = String(template || '');
  if (!t) return `Register ${type || root} ${resourceName}`;
  return t
    .replace('{type}', type || '')
    .replace('{resource}', resourceName || '')
    .replace('{root}', root || '');
};

function orderCandidateForYaml(candidate: JsonValue): JsonValue {
  if (!isPlainObject(candidate)) return candidate;

  const preferred = [
    'type',
    'name',
    'id',
    'title',
    'description',
    'shortDescription',
    'summary',
    'details',
    'contact',
    'contacts',
    'cldSystemRole',
    'stcServiceId',
    'ppms',
    'parentId',
    'correlationIds',
    'correlationIdTypes',
    'visibility',
    'parent',
  ];

  const out: JsonObject = {};
  for (const k of preferred) {
    if (Object.prototype.hasOwnProperty.call(candidate, k)) out[k] = candidate[k];
  }
  for (const k of Object.keys(candidate)) {
    if (!Object.prototype.hasOwnProperty.call(out, k)) out[k] = candidate[k];
  }
  return out;
}

// Resolve schema path ONLY via template meta (injected from cfg.requests in template.js).
async function loadSchemaForTemplate(
  context: ContextLike,
  repoRef: { owner: string; repo: string },
  template: TemplateLike
): Promise<JsonValue | null> {
  const { schemaSearchPaths } = getEffectivePrOptions(context);

  const schemaPath = String(template?._meta?.schema || '').trim();
  if (!schemaPath || !context?.octokit) return null;

  const raw = schemaPath.replace(/^\/+/, '').trim();
  const candidates = new Set<string>();

  // If schema is already a repo-relative path, try it first
  candidates.add(raw);

  // Also try resolving against known bases
  const cleaned = raw.replace(/^\.?\//, '');
  for (const base of schemaSearchPaths) {
    const b = String(base || '').trim();
    if (!b) continue;
    const baseClean = b.replace(/^\.?\//, '').replace(/\/+$/, '');
    candidates.add(`${baseClean}/${cleaned}`);
  }

  // Default base
  candidates.add(`${CONFIG_BASE_DIR}/${cleaned}`);

  const repoKey = `${repoRef.owner}/${repoRef.repo}`;

  for (const p of candidates) {
    const cacheKey = `${repoKey}:${p}`;
    if (REPO_SCHEMA_CACHE.has(cacheKey)) return REPO_SCHEMA_CACHE.get(cacheKey) ?? null;

    try {
      const res = await context.octokit.repos.getContent({
        owner: repoRef.owner,
        repo: repoRef.repo,
        path: p,
      });
      const data = res.data;

      if (!Array.isArray(data) && isRepoContentFile(data)) {
        // GitHub contents API returns Base64 encoded content for files
        const txt = Buffer.from(data.content, (data.encoding || 'base64') as BufferEncoding).toString('utf8');
        const obj = JSON.parse(txt);

        REPO_SCHEMA_CACHE.set(cacheKey, obj);
        return obj;
      }
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) continue;
      throw e;
    }
  }

  return null;
}

function getTypeSubschema(schema: JsonValue, type: string): JsonValue | null {
  if (!isPlainObject(schema)) return null;

  const defs = schema['$defs'];
  if (isPlainObject(defs) && Object.prototype.hasOwnProperty.call(defs, type)) {
    return defs[type] ?? null;
  }

  const oneOf = ((schema as JsonObject)['oneOf'] as JsonValue[]) || [];
  const anyOf = ((schema as JsonObject)['anyOf'] as JsonValue[]) || [];
  const allOf = ((schema as JsonObject)['allOf'] as JsonValue[]) || [];
  const candidates: JsonValue[] = [oneOf, anyOf, allOf].flat();

  for (const c of candidates) {
    if (!isPlainObject(c)) continue;
    const props = c['properties'];
    if (!isPlainObject(props)) continue;

    const t = props['type'];
    if (isPlainObject(t) && t['const'] === type) return c;
  }

  return schema;
}

function stripDefaultsBySchema(node: JsonValue, subschema: JsonValue): void {
  if (!isPlainObject(node) || !isPlainObject(subschema)) return;

  const props = isPlainObject(subschema['properties']) ? (subschema['properties'] as JsonObject) : {};

  for (const [k, defSchemaRaw] of Object.entries(props)) {
    if (!Object.prototype.hasOwnProperty.call(node, k)) continue;

    const defSchema = isPlainObject(defSchemaRaw) ? defSchemaRaw : null;
    if (defSchema && Object.prototype.hasOwnProperty.call(defSchema, 'default')) {
      if (node[k] === defSchema['default']) delete node[k];
    }

    if (defSchema && defSchema['type'] === 'object' && isPlainObject(node[k])) {
      stripDefaultsBySchema(node[k], defSchema);
    }

    if (defSchema && defSchema['type'] === 'array' && Array.isArray(node[k])) {
      const itemsSchema = isPlainObject(defSchema['items']) ? defSchema['items'] : null;
      if (itemsSchema) {
        for (const el of node[k] as unknown[]) {
          if (isPlainObject(el)) stripDefaultsBySchema(el, itemsSchema);
        }
      }
    }
  }
}

function resolveTypeConstFromSchema(schemaObj: JsonValue, fallback = ''): string {
  const props =
    isPlainObject(schemaObj) && isPlainObject(schemaObj['properties']) ? (schemaObj['properties'] as JsonObject) : null;
  if (!props) return String(fallback || '').trim();

  const def = props['type'];
  const c = isPlainObject(def) && typeof def['const'] === 'string' ? String(def['const']).trim() : '';
  return c || String(fallback || '').trim();
}

function pickContactProp(schemaObj: JsonValue): 'contacts' | 'contact' | '' {
  const props =
    isPlainObject(schemaObj) && isPlainObject(schemaObj['properties']) ? (schemaObj['properties'] as JsonObject) : null;
  if (!props) return 'contact';

  if (Object.prototype.hasOwnProperty.call(props, 'contacts') && props['contacts'] !== false) return 'contacts';
  if (Object.prototype.hasOwnProperty.call(props, 'contact') && props['contact'] !== false) return 'contact';
  return ''; // schema forbids both
}

const normalizeStringArray = (v: unknown): string[] => {
  if (v === null || typeof v === 'undefined') return [];
  if (Array.isArray(v)) return Array.from(new Set(v.map((x) => String(x).trim()).filter(Boolean)));

  return Array.from(
    new Set(
      String(v)
        .split(/\r?\n|,/)
        .map((s) => s.trim())
        .filter(Boolean)
    )
  );
};

const buildCorrIds = (form: Record<string, unknown>): string[] => {
  const list: string[] = [];

  // manual correlationIds field (multiline / comma)
  list.push(...normalizeStringArray(form?.correlationIds ?? form?.['correlation-ids'] ?? ''));

  const cld = String(form?.['cld-system-role'] || '').trim();
  if (cld) list.push(`sap.cld:systemRole:${cld}`);

  const stc = String(form?.['stc-service-id'] || '').trim();
  if (stc) list.push(`sap.stc:service:${stc}`);

  const ppms = String(form?.['ppms-product-object-number'] || form?.ppms || '').trim();
  if (ppms) list.push(`sap.ppms:product:${ppms}`);

  return Array.from(new Set(list));
};

const parseMaybeYamlJson = (v: unknown): unknown => {
  if (v === null || typeof v === 'undefined') return undefined;
  if (Array.isArray(v) || isPlainObject(v)) return v;

  const s = String(v).trim();
  if (!s) return undefined;

  try {
    return JSON.parse(s);
  } catch {
    // ignore
  }

  try {
    return loadYamlDoc(s);
  } catch {
    // ignore
  }

  return undefined;
};

function mapPartnerNamespaceRequestTypeToConfigKey(v: unknown): string {
  const raw = String(v ?? '').trim();
  const norm = raw.replace(/[\s_-]/g, '').toLowerCase();

  if (norm === 'authority') return 'authorityNamespace';
  if (norm === 'system') return 'systemNamespace';
  if (norm === 'subcontext') return 'subContextNamespace';

  return '';
}

function getRequestEntryFromConfig(context: ContextLike, requestType: string): Record<string, unknown> | null {
  const cfg = context.resourceBotConfig as unknown as Record<string, unknown> | undefined;
  const reqs = cfg && isPlainObject(cfg['requests']) ? cfg['requests'] : null;
  if (!reqs) return null;

  if (isPlainObject(reqs[requestType])) return reqs[requestType];

  const want = String(requestType || '').toLowerCase();
  for (const [k, v] of Object.entries(reqs)) {
    if (String(k).toLowerCase() === want && isPlainObject(v)) return v;
  }

  return null;
}

export async function createRequestPr(
  context: ContextLike,
  repoRef: { owner: string; repo: string },
  issue: IssueLike,
  formData: Record<string, unknown>,
  options: CreateRequestPrOptions = {}
): Promise<PullRequestLike> {
  const { owner, repo } = repoRef;

  if (!context.resourceBotConfig) {
    try {
      const { config } = await loadStaticConfig(context, { validate: false, updateIssue: false });
      context.resourceBotConfig = config || {};
    } catch {
      context.resourceBotConfig = {};
    }
  }

  const prOpts = getEffectivePrOptions(context);
  const givenTemplate = options?.template;

  const issueLabels = (issue?.labels || [])
    .map((l) => (typeof l === 'string' ? l : l?.name))
    .filter((x): x is string => Boolean(x));

  let template =
    givenTemplate ||
    (await loadTemplate(context, {
      owner,
      repo,
      issueTitle: String(issue.title || ''),
      issueLabels,
    }).catch(() => null));

  if (!template) {
    throw new Error('Configuration error: Missing form template (could not resolve template via labels).');
  }

  const { data: repoData } = await context.octokit.repos.get({ owner, repo });
  const defaultBranch = repoData.default_branch;
  const baseBranch = prOpts.baseBranch || defaultBranch;

  const { data: baseBranchData } = await context.octokit.repos.getBranch({
    owner,
    repo,
    branch: baseBranch,
  });
  const baseSha = String(baseBranchData?.commit?.sha || '').trim();
  if (!baseSha) throw new Error(`Cannot resolve base SHA for branch '${baseBranch}'.`);

  let requestType = String(template?._meta?.requestType || '').trim();
  if (!requestType) {
    throw new Error('Configuration error: template missing _meta.requestType (cfg.requests injection).');
  }

  // Partner Namespace template: derive effective requestType from the form enum `requestType`.
  if (requestType && requestType.toLowerCase() === 'partnernamespace') {
    const selected = (formData as JsonObject)['requestType'] ?? (formData as JsonObject)['request-type'];
    const mapped = mapPartnerNamespaceRequestTypeToConfigKey(selected);

    if (!mapped) {
      throw new Error(
        `Invalid Partner Namespace 'Request Type' selection '${String(selected ?? '').trim()}'. Expected one of: authority, system, subContext.`
      );
    }

    const entry = getRequestEntryFromConfig(context, mapped);
    const schema = entry ? String(entry['schema'] || '').trim() : '';
    const root = entry ? String(entry['folderName'] || '').trim() : '';

    if (!schema) {
      throw new Error(
        `Configuration error: Partner Namespace selection maps to '${mapped}', but cfg.requests has no schema for it.`
      );
    }

    // Clone template meta to avoid mutating cached templates across issues.
    template = {
      ...template,
      _meta: {
        ...(template._meta || {}),
        requestType: mapped,
        schema,
        root: root || String(template?._meta?.root || '').trim(),
      },
    };

    requestType = mapped;
  }

  const isProductReq = requestType.toLowerCase() === 'product';

  // cfg.requests-derived meta (set by template.js)
  const folderName = String(template?._meta?.root || '').trim();
  if (!folderName) {
    throw new Error(
      `Configuration error: template missing _meta.root (expected cfg.requests[requestType].folderName injection). tpl='${String(
        template?._meta?.path || ''
      )}' requestType='${requestType}'`
    );
  }

  const STRUCT_ROOT = folderName.replace(/^\/+/, '').replace(/\/+$/, '');

  // schema needed for template-driven ID resolution
  const prSchemaObj = await loadSchemaForTemplate(context, { owner, repo }, template);
  if (!prSchemaObj) {
    throw new Error(
      `Configuration error: schema could not be loaded for tpl='${String(
        template?._meta?.path || ''
      )}' schema='${String(template?._meta?.schema || '')}'`
    );
  }

  const resourceName = resolvePrimaryIdFromTemplate(template, formData, prSchemaObj)
    .replace(/\u00a0/g, ' ')
    .trim();

  if (!resourceName) {
    throw new Error('Could not resolve primary identifier from template. Please fill the required ID field.');
  }

  if (process.env.DEBUG_NS === '1' && context.log?.info) {
    context.log.info(
      {
        metaSchema: template?._meta?.schema,
        STRUCT_ROOT,
        isProductReq,
        resourceName,
        formKeys: Object.keys(formData || {}),
      },
      'pr:root-and-name'
    );
  }

  const branch = applyBranchTemplate(prOpts.branchTemplate, resourceName, issue.number);

  try {
    // Git refs are created under refs/heads/<branch>
    await context.octokit.git.createRef({ owner, repo, ref: `refs/heads/${branch}`, sha: baseSha });
  } catch (e: unknown) {
    if (getHttpStatus(e) !== 422) throw e;
  }

  const existsAt = async (p: string, refName: string): Promise<boolean> => {
    try {
      await context.octokit.repos.getContent({ owner, repo, path: p, ref: refName });
      return true;
    } catch (e: unknown) {
      if (getHttpStatus(e) === 404) return false;
      throw e;
    }
  };

  const writeFileAt = async (p: string, contentText: string, sha?: string | null): Promise<void> => {
    const message = buildCommitMessage(prOpts.commitMessageTemplate, STRUCT_ROOT, resourceName, issue.number);

    // Contents API expects Base64 encoded content for create/update
    const params: {
      owner: string;
      repo: string;
      path: string;
      message: string;
      content: string;
      branch: string;
      sha?: string;
    } = {
      owner,
      repo,
      path: p,
      message,
      content: Buffer.from(contentText, 'utf8').toString('base64'),
      branch,
    };

    if (sha) params.sha = sha;
    await context.octokit.repos.createOrUpdateFileContents(params);
  };

  const buildFlatPaths = (n: string): { file: string } => {
    const safe = String(n || '').trim();
    if (!safe) throw new Error(`Invalid resource name '${n}'`);
    return { file: `${STRUCT_ROOT}/${safe}.yaml` };
  };

  const { file: resourceFile } = buildFlatPaths(resourceName);

  // Namespace schemas should define `type.const` (e.g. "System", "Authority", "SubContext")
  let type = isProductReq ? 'Product' : resolveTypeConstFromSchema(prSchemaObj, requestType);
  if (!type) type = isProductReq ? 'Product' : requestType;

  // PRODUCT BRANCH
  if (isProductReq) {
    const { file: productFile } = buildFlatPaths(resourceName);

    const parentId = String((formData as JsonObject).parentId || '').trim();

    const normalized: JsonObject = {
      ...formData,
      requestType,
      // force id as primary key, do NOT propagate identifier/parentId
      id: String((formData as JsonObject).id || (formData as JsonObject).identifier || resourceName).trim(),
      description: String((formData as JsonObject).description || '')
        .replace(/\u00a0/g, ' ')
        .trim(),
      contact: normalizeStringArray((formData as JsonObject).contact),
      ...(parentId ? { parent: `sap:product:${parentId}:` } : {}),
    };

    // defensive cleanup (prevents projection/yaml from carrying duplicates)
    delete normalized.identifier;
    delete normalized.parentId;
    delete normalized.namespace;

    let candidate = await projectForSchema(requestType, normalized, prSchemaObj);

    if (!isPlainObject(candidate)) {
      throw new Error('Schema projection failed for Product candidate.');
    }

    // enforce: id only, drop duplicates
    candidate.id = String(candidate.id || normalized.id || resourceName).trim();
    delete candidate.identifier;
    delete candidate.parentId;

    // Product schemas often use "id" instead of "identifier"
    if (!candidate.id && (normalized.id || normalized.identifier)) {
      candidate.id = String(normalized.id || normalized.identifier).trim();
    }

    // parentId -> parent URI
    if (parentId && !candidate.parent) {
      const productSub = getTypeSubschema(prSchemaObj, 'Product') || prSchemaObj;
      const props =
        isPlainObject(productSub) && isPlainObject(productSub.properties)
          ? (productSub.properties as JsonObject)
          : null;

      if (!props || (Object.prototype.hasOwnProperty.call(props, 'parent') && props.parent !== false)) {
        candidate.parent = `sap:product:${parentId}:`;
      }
    }

    if (parentId) {
      (candidate as Record<string, unknown>).parentId = parentId;
    } else {
      delete (candidate as Record<string, unknown>).parentId;
    }

    delete (candidate as Record<string, unknown>).parent;

    candidate = orderCandidateForYaml(candidate) as JsonObject;

    const productSub = getTypeSubschema(prSchemaObj, 'Product') || prSchemaObj;
    stripDefaultsBySchema(candidate, productSub);

    const exists = (await existsAt(productFile, defaultBranch)) || (await existsAt(productFile, branch));
    if (exists) throw new Error(`Resource '${resourceName}' already exists at ${STRUCT_ROOT}`);

    const yamlText = dumpYamlDoc(candidate);
    await writeFileAt(productFile, yamlText, null);

    let pr: PullRequestLike | undefined;
    try {
      const { data: prs } = await context.octokit.pulls.list({
        owner,
        repo,
        state: 'open',
        head: `${owner}:${branch}`,
      });
      pr = prs[0];
    } catch {
      // ignore listing errors
    }

    if (!pr) {
      const hash = calcSnapshotHash(formData, template, String(issue.body || ''));
      const hashComment = `<!-- ${SNAPSHOT_HASH_MARKER_KEY}:${hash} -->`;
      const issueMarker = `<!-- nsreq:issue:${issue.number} -->`;

      const prTitle = buildPrTitle(prOpts.prTitleTemplate, 'Product', resourceName, STRUCT_ROOT);
      const bodyHeader = `This PR registers **${resourceName}**.`;

      const body = `${bodyHeader}

  fix: #${issue.number}

  Type: Product
  ${prOpts.bodyFooter}

  ${issueMarker}
  ${hashComment}`;

      const res = await context.octokit.pulls.create({
        owner,
        repo,
        title: prTitle,
        head: branch,
        base: baseBranch,
        body,
        maintainer_can_modify: true,
      });
      pr = res.data;
    }

    let enabled = false;
    if (prOpts.autoMergeEnabled) {
      enabled = await tryEnableAutoMerge(context, pr, {
        mergeMethod: prOpts.autoMergeMethod.toUpperCase() as MergeMethodGraphql,
      });
    }

    if (!enabled && prOpts.autoMergeLabel) {
      try {
        await context.octokit.issues.addLabels({
          owner,
          repo,
          issue_number: pr.number,
          labels: [prOpts.autoMergeLabel],
        });
      } catch {
        // ignore
      }
    }

    if (prOpts.autoMergeEnabled) {
      await tryMergeIfGreen(context, {
        owner,
        repo,
        prNumber: pr.number,
        mergeMethod: prOpts.autoMergeMethod as MergeMethodRest,
        prData: pr,
      });
    }

    return pr;
  }

  // NAMESPACE / AUTHORITY / SUBCONTEXT / VENDOR BRANCH
  // Build YAML strictly schema-driven via projectForSchema(), then apply minimal policy constraints.

  const tplIds = new Set((template?.body || []).map((f) => f?.id).filter(Boolean) as string[]);
  const hasContactField = tplIds.has('contact') || tplIds.has('contacts');
  const hasVisibilityField = tplIds.has('visibility') || tplIds.has('open-system');

  const normalized: JsonObject = {
    ...formData,
    requestType,
    identifier: resourceName,
    namespace: resourceName,
    description: String((formData as JsonObject).description || '')
      .replace(/\u00a0/g, ' ')
      .trim(),
  };

  // Only for System-ish requests
  if (requestType.toLowerCase() === 'systemnamespace') {
    const corrIds = buildCorrIds(formData);
    if (corrIds.length) normalized.correlationIds = corrIds;

    const corrTypes = Array.isArray((formData as JsonObject).correlationIdTypes)
      ? (formData as JsonObject).correlationIdTypes
      : parseMaybeYamlJson((formData as JsonObject).correlationIdTypes);

    if (Array.isArray(corrTypes) && corrTypes.length) {
      normalized.correlationIdTypes = corrTypes;
    }
  }

  let candidate = await projectForSchema(requestType, normalized, prSchemaObj);

  if (!isPlainObject(candidate)) {
    throw new Error('Schema projection failed for namespace candidate.');
  }

  const hasDirectSystemIds = tplIds.has('cld-system-role') || tplIds.has('stc-service-id') || tplIds.has('ppms');

  if (hasDirectSystemIds) {
    delete (candidate as Record<string, unknown>).correlationIds;
  }

  // Ensure stable YAML order: type -> name -> description -> ...
  candidate = orderCandidateForYaml(candidate) as JsonObject;

  // Enforce/derive visibility only if the template exposes it (prevents leaking defaults into YAML)
  if (!hasVisibilityField) {
    delete candidate.visibility;
  }

  // If the template doesn't have contact, never emit it
  if (!hasContactField) {
    delete candidate.contact;
    delete candidate.contacts;
  }

  // Enforce minItems for contact/contacts only when applicable
  const contactProp = pickContactProp(prSchemaObj); // 'contacts' | 'contact' | ''
  if (contactProp) {
    const typeSub = getTypeSubschema(prSchemaObj, String(candidate.type || type)) || prSchemaObj;

    const subProps =
      isPlainObject(typeSub) && isPlainObject((typeSub as JsonObject).properties)
        ? ((typeSub as JsonObject).properties as JsonObject)
        : null;

    const rootProps =
      isPlainObject(prSchemaObj) && isPlainObject((prSchemaObj as JsonObject).properties)
        ? ((prSchemaObj as JsonObject).properties as JsonObject)
        : null;

    const def = (subProps && subProps[contactProp]) || (rootProps && rootProps[contactProp]) || null;

    const minItems =
      isPlainObject(def) && typeof (def as JsonObject).minItems === 'number' ? Number((def as JsonObject).minItems) : 0;

    // Only enforce when:
    // - template exposes contact/contacts (user can provide it), OR
    // - schema requires it, OR
    // - candidate explicitly contains the property
    const reqRoot =
      isPlainObject(prSchemaObj) && Array.isArray((prSchemaObj as JsonObject).required)
        ? ((prSchemaObj as JsonObject).required as unknown[]).map((x) => String(x).trim())
        : [];

    const reqType =
      isPlainObject(typeSub) && Array.isArray((typeSub as JsonObject).required)
        ? ((typeSub as JsonObject).required as unknown[]).map((x) => String(x).trim())
        : [];

    const isRequired = new Set([...reqRoot, ...reqType]).has(contactProp);
    const hasProp = Object.prototype.hasOwnProperty.call(candidate, contactProp);

    if (minItems && (hasContactField || isRequired || hasProp)) {
      const arr = (candidate as Record<string, unknown>)[contactProp];
      if (!Array.isArray(arr) || arr.length < minItems) {
        throw new Error(`Schema violation: '${contactProp}' requires at least ${minItems} entries.`);
      }
    }
  } else {
    delete (candidate as Record<string, unknown>).contact;
    delete (candidate as Record<string, unknown>).contacts;
  }

  // Apply type-specific policy
  if (String(candidate.type || '').trim() === 'SubContext') {
    delete candidate.correlationIdTypes;
    delete candidate.visibility;
    delete candidate.deprecated;
    delete candidate.expiryDate;
  }

  // Strip defaults
  {
    const typeSub = getTypeSubschema(prSchemaObj, String(candidate.type || type)) || prSchemaObj;
    stripDefaultsBySchema(candidate, typeSub);
  }

  const exists = (await existsAt(resourceFile, defaultBranch)) || (await existsAt(resourceFile, branch));
  if (exists) throw new Error(`Resource '${resourceName}' already exists at ${STRUCT_ROOT}`);

  const yamlText = dumpYamlDoc(candidate);
  await writeFileAt(resourceFile, yamlText, null);

  let pr: PullRequestLike | undefined;
  try {
    const { data: prs } = await context.octokit.pulls.list({
      owner,
      repo,
      state: 'open',
      head: `${owner}:${branch}`,
    });
    pr = prs[0];
  } catch {
    // ignore listing errors
  }

  if (!pr) {
    const hash = calcSnapshotHash(formData, template, String(issue.body || ''));
    const hashComment = `<!-- ${SNAPSHOT_HASH_MARKER_KEY}:${hash} -->`;

    const prTitle = buildPrTitle(prOpts.prTitleTemplate, String(candidate.type || type), resourceName, STRUCT_ROOT);

    const bodyHeader = `This PR registers **${resourceName}**.`;
    const body = `${bodyHeader}\n\nfix: #${issue.number}\n\nType: ${String(
      candidate.type || type
    )}\n${prOpts.bodyFooter}\n\n${hashComment}`;

    const res = await context.octokit.pulls.create({
      owner,
      repo,
      head: branch,
      base: baseBranch,
      title: prTitle,
      body,
    });
    pr = res.data;
  }

  let enabled = false;
  if (prOpts.autoMergeEnabled) {
    enabled = await tryEnableAutoMerge(context, pr, {
      mergeMethod: prOpts.autoMergeMethod.toUpperCase() as MergeMethodGraphql,
    });
  }

  if (!enabled && prOpts.autoMergeLabel) {
    try {
      await context.octokit.issues.addLabels({
        owner,
        repo,
        issue_number: pr.number,
        labels: [prOpts.autoMergeLabel],
      });
    } catch {
      // ignore
    }
  }

  if (prOpts.autoMergeEnabled) {
    await tryMergeIfGreen(context, {
      owner,
      repo,
      prNumber: pr.number,
      mergeMethod: prOpts.autoMergeMethod as MergeMethodRest,
      prData: pr,
    });
  }

  context.log?.info?.(
    { requestType, type: candidate?.type, schemaId: (prSchemaObj as JsonObject)?.$id },
    'dbg:type-mapping'
  );

  context.log?.info?.(
    {
      requestType,
      schemaId: (prSchemaObj as JsonObject)?.$id,
      type: candidate?.type,
      tpl: template?._meta?.path,
    },
    'dbg:validation-routing'
  );

  return pr;
}
