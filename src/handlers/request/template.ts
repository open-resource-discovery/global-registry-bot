import YAML from 'yaml';
import { buildAllowedFieldIdsFromSchema, parseForm as parseFormRaw } from '../../utils/parser.js';

const DBG = process.env.DEBUG_NS === '1';

const CONFIG_BASE_DIR = '.github/registry-bot';
const TTL_MS = 5 * 60 * 1000;

type CacheEntry<T> = { ts: number; data: T };

type RequestConfigEntry = {
  issueTemplate?: string;
  schema?: string;
  folderName?: string;
  [k: string]: unknown;
};

type WorkflowLabelsConfig = {
  routingLabelPrefixes?: string | string[];
  routingLabelPrefix?: string | string[];
  [k: string]: unknown;
};

type ResourceBotConfig = {
  requests?: unknown;
  workflow?: { labels?: WorkflowLabelsConfig };
  [k: string]: unknown;
};

type LogLike = {
  debug?: (obj: unknown, msg?: string) => void;
};

type OctokitLike = {
  repos: {
    getContent: (params: { owner: string; repo: string; path: string }) => Promise<{ data: unknown }>;
  };
};

export type ContextLike = {
  octokit?: OctokitLike;
  log?: LogLike;
  resourceBotConfig?: ResourceBotConfig;
};

export type TemplateMeta = {
  path: string;
  name: string;
  schema?: string;
  root?: string;
  requestType?: string;
  [k: string]: unknown;
};

export type Template = Record<string, unknown> & {
  title?: string;
  labels: string[];
  body: unknown[];
  _meta?: TemplateMeta;
};

type LabelIndex = {
  labelToType: Map<string, { requestType: string; label: string }>;
  expectedLabels: string[];
};

type ParseAllowedIdsRepoInfo = {
  owner: string;
  repo: string;
};

type ParseAllowedIdsContext = {
  octokit: {
    repos: {
      getContent: (args: { owner: string; repo: string; path: string }) => Promise<{ data?: unknown }>;
    };
  };
};

type ParseAllowedIdsTemplateField = {
  id?: string;
};

type ParseAllowedIdsTemplate = {
  body?: ParseAllowedIdsTemplateField[];
  _meta?: {
    schema?: string;
    parseAllowedFieldIds?: string[];
    [k: string]: unknown;
  };
  [k: string]: unknown;
};

const TEMPLATE_PARSE_ALLOWED_FIELD_IDS_CACHE = new Map<string, Promise<string[]>>();

function parseAllowedToString(value: unknown): string {
  return String(value ?? '').trim();
}

function parseAllowedIsPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseAllowedNormalizeRepoPath(value: unknown): string {
  return parseAllowedToString(value)
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/');
}

function buildTemplateFieldIdSetForParseFilter(template: ParseAllowedIdsTemplate): Set<string> {
  const out = new Set<string>();

  for (const field of template?.body || []) {
    const id = parseAllowedToString(field?.id);
    if (id) out.add(id);
  }

  return out;
}

function buildParseAllowedFieldIds(template: ParseAllowedIdsTemplate, schemaObj: unknown): string[] {
  const templateFieldIds = Array.from(buildTemplateFieldIdSetForParseFilter(template));
  if (!templateFieldIds.length) return [];

  return buildAllowedFieldIdsFromSchema(schemaObj, null, templateFieldIds);
}

async function readSchemaTextForParseFilter(
  context: ParseAllowedIdsContext,
  repoInfo: ParseAllowedIdsRepoInfo,
  schemaPath: string
): Promise<string | null> {
  const rawPath = parseAllowedToString(schemaPath);
  if (!rawPath) return null;

  const cleaned = rawPath.replace(/^\.?\//, '');
  const candidates = rawPath.startsWith('/')
    ? [rawPath.replace(/^\/+/, '')]
    : cleaned.startsWith('.github/')
      ? [cleaned]
      : [`.github/registry-bot/${cleaned}`, cleaned];

  for (const candidate of Array.from(new Set(candidates.map(parseAllowedNormalizeRepoPath).filter(Boolean)))) {
    try {
      const res = await context.octokit.repos.getContent({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        path: candidate,
      });

      const data = (res as { data?: unknown }).data;
      if (Array.isArray(data)) continue;
      if (!parseAllowedIsPlainObject(data)) continue;
      if (typeof data['content'] !== 'string') continue;

      const encoding = typeof data['encoding'] === 'string' ? data['encoding'] : 'base64';
      return Buffer.from(String(data['content'] || ''), encoding as BufferEncoding).toString('utf8');
    } catch {
      continue;
    }
  }

  return null;
}

async function resolveTemplateParseAllowedFieldIds(
  context: ParseAllowedIdsContext,
  repoInfo: ParseAllowedIdsRepoInfo,
  template: ParseAllowedIdsTemplate
): Promise<string[]> {
  const schemaPath = parseAllowedToString(template?._meta?.schema);
  const templateFieldIds = Array.from(buildTemplateFieldIdSetForParseFilter(template)).sort();

  if (!schemaPath || !templateFieldIds.length) return [];

  const cacheKey = `${repoInfo.owner}/${repoInfo.repo}:${schemaPath}:${templateFieldIds.join(',')}`;
  const cached = TEMPLATE_PARSE_ALLOWED_FIELD_IDS_CACHE.get(cacheKey);
  if (cached) return await cached;

  const pending = (async (): Promise<string[]> => {
    const raw = await readSchemaTextForParseFilter(context, repoInfo, schemaPath);
    if (!raw) return [];

    try {
      const schemaObj = JSON.parse(raw) as unknown;
      return buildParseAllowedFieldIds(template, schemaObj);
    } catch {
      return [];
    }
  })();

  TEMPLATE_PARSE_ALLOWED_FIELD_IDS_CACHE.set(cacheKey, pending);
  return await pending;
}

async function attachParseAllowedFieldIds(
  context: ParseAllowedIdsContext,
  repoInfo: ParseAllowedIdsRepoInfo,
  template: ParseAllowedIdsTemplate
): Promise<void> {
  const parseAllowedFieldIds = await resolveTemplateParseAllowedFieldIds(context, repoInfo, template);
  if (!parseAllowedFieldIds.length) return;

  template._meta = {
    ...(template._meta || {}),
    parseAllowedFieldIds,
  };
}

const TEMPLATE_CACHE = new Map<string, CacheEntry<unknown>>();
const TEMPLATE_FILE_CACHE = new Map<string, CacheEntry<Template>>();

const now = (): number => Date.now();

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((x) => String(x ?? '').trim()).filter(Boolean);
}

const normalizePosixPath = (p: unknown): string => {
  const raw = String(p ?? '')
    .trim()
    .replace(/\\/g, '/');

  const parts = raw.split('/').filter((x) => x && x !== '.');
  const out: string[] = [];
  for (const part of parts) {
    if (part === '..') out.pop();
    else out.push(part);
  }
  return out.join('/');
};

const resolveRepoPathFromConfig = (rawPath: unknown): string => {
  const p = String(rawPath ?? '').trim();
  if (!p) return '';

  const cleaned = p.replace(/\\/g, '/');

  if (cleaned.startsWith('.github/')) return normalizePosixPath(cleaned);
  if (cleaned.startsWith('/')) return normalizePosixPath(cleaned.replace(/^\/+/, ''));

  return normalizePosixPath(`${CONFIG_BASE_DIR}/${cleaned}`);
};

const getRequestsConfig = (context: ContextLike): Record<string, RequestConfigEntry> => {
  const cfg = context.resourceBotConfig ?? {};
  const req = cfg.requests;
  return isPlainObject(req) ? (req as Record<string, RequestConfigEntry>) : {};
};

const getRoutingLabelPrefixes = (context: ContextLike): string[] => {
  const cfg = context.resourceBotConfig ?? {};
  const labelsCfg = cfg.workflow?.labels && isPlainObject(cfg.workflow.labels) ? cfg.workflow.labels : {};

  const raw = labelsCfg.routingLabelPrefixes ?? labelsCfg.routingLabelPrefix ?? ['registry-bot:'];

  const arr = Array.isArray(raw) ? raw : [raw];

  return arr
    .map((x) => String(x ?? '').trim())
    .filter(Boolean)
    .map((x) => x.toLowerCase());
};

const normalizeLabel = (l: unknown): string =>
  String(l ?? '')
    .trim()
    .toLowerCase();

type IssueLabelLike = string | { name?: unknown } | null | undefined;

const toLabelStrings = (issueLabels: unknown): string[] => {
  if (!Array.isArray(issueLabels)) return [];
  return issueLabels
    .map((l: IssueLabelLike) =>
      typeof l === 'string' ? l : l && typeof l === 'object' && 'name' in l ? (l as { name?: unknown }).name : undefined
    )
    .map((x) => String(x ?? '').trim())
    .filter(Boolean);
};

const findRequestByTemplatePath = (
  context: ContextLike,
  templatePath: unknown
): { requestType: string; rc: RequestConfigEntry } | null => {
  const tplPath = normalizePosixPath(String(templatePath ?? '').trim());
  if (!tplPath) return null;

  const req = getRequestsConfig(context);
  for (const [requestType, rc] of Object.entries(req)) {
    const cfgTpl = resolveRepoPathFromConfig(rc?.issueTemplate);
    if (cfgTpl && cfgTpl === tplPath) return { requestType, rc };
  }
  return null;
};

export function categoryFromTemplate(template: Template | null | undefined): string {
  return String(template?._meta?.requestType ?? '').trim();
}

// Extract YAML front-matter from .md
function parseFrontMatterMd(text: string): { fm: Record<string, unknown> | null; body: string } {
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/m.exec(text);
  if (!m) return { fm: null, body: text };
  try {
    const parsed = YAML.parse(m[1]) as unknown;
    const fm = isPlainObject(parsed) ? parsed : null;
    return { fm, body: m[2] ?? '' };
  } catch {
    return { fm: null, body: text };
  }
}

const applyRequestMeta = (template: Template, requestType: string, rc: RequestConfigEntry): Template => {
  const schemaPath = rc?.schema ? resolveRepoPathFromConfig(rc.schema) : undefined;

  const prev = template._meta ?? { path: '', name: '' };
  const meta: TemplateMeta = {
    path: String(prev.path ?? '').trim(),
    name: String(prev.name ?? '').trim(),
    requestType: requestType || prev.requestType,
    schema: schemaPath || prev.schema,
    root: rc?.folderName || prev.root,
  };

  template._meta = meta;

  return template;
};

async function applyRequestMetaAndAttachParseAllowedFieldIds(
  context: ContextLike,
  owner: string,
  repo: string,
  template: Template,
  requestType: string,
  rc: RequestConfigEntry
): Promise<Template> {
  applyRequestMeta(template, requestType, rc);

  if (context.octokit) {
    await attachParseAllowedFieldIds(
      { octokit: context.octokit },
      { owner, repo },
      template as ParseAllowedIdsTemplate
    );
  }

  return template;
}

const buildLabelIndexFromTemplates = async (
  context: ContextLike,
  args: { owner: string; repo: string; fetchFile: (path: string) => Promise<Template> }
): Promise<LabelIndex> => {
  const { owner, repo, fetchFile } = args;
  const idxCacheKey = `${owner}/${repo}:label-index`;

  const cached = TEMPLATE_CACHE.get(idxCacheKey) as CacheEntry<LabelIndex> | undefined;
  if (cached && now() - cached.ts < TTL_MS) {
    if (DBG && context.log?.debug) {
      context.log.debug({ idxCacheKey, ageMs: now() - cached.ts }, 'tpl:label-index:cache-hit');
    }
    return cached.data;
  }

  const req = getRequestsConfig(context);
  const templateInfos: {
    requestType: string;
    rc: RequestConfigEntry;
    tplPath: string;
    tplLabels: string[];
  }[] = [];

  for (const [requestType, rc] of Object.entries(req)) {
    const tplPath = resolveRepoPathFromConfig(rc?.issueTemplate);
    if (!tplPath) {
      throw new Error(`Configuration error: cfg.requests.${requestType}.issueTemplate is missing.`);
    }

    const tpl = await fetchFile(tplPath);
    const tplLabels = asStringArray(tpl?.labels);
    templateInfos.push({ requestType, rc, tplPath, tplLabels });
  }

  const routingPrefixes = getRoutingLabelPrefixes(context);

  const counts = new Map<string, number>();
  for (const info of templateInfos) {
    for (const lab of info.tplLabels) {
      const key = normalizeLabel(lab);
      if (!key) continue;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
  }

  const labelToType = new Map<string, { requestType: string; label: string }>();
  const expectedLabels: string[] = [];

  for (const info of templateInfos) {
    const normalizedTplLabels = info.tplLabels
      .map((l) => ({ raw: String(l ?? '').trim(), norm: normalizeLabel(l) }))
      .filter((x) => x.norm);

    const prefixed = normalizedTplLabels.filter((x) => routingPrefixes.some((p) => x.norm.startsWith(p)));

    let routing: { raw: string; norm: string }[] = [];

    if (prefixed.length > 0) {
      routing = prefixed;
      if (routing.length !== 1) {
        throw new Error(
          `Configuration error: issue template '${info.tplPath}' for requestType '${info.requestType}' ` +
            `must define exactly ONE routing label with prefix [${routingPrefixes.join(', ')}], ` +
            `but found: ${routing.map((x) => `'${x.raw}'`).join(', ')}`
        );
      }
    } else {
      const unique = normalizedTplLabels.filter((x) => (counts.get(x.norm) ?? 0) === 1);

      if (unique.length === 0) {
        const suggested = `${routingPrefixes[0]}${info.requestType}`;
        throw new Error(
          `Cannot resolve routing label for '${info.requestType}': issue template '${info.tplPath}' has no routing label. ` +
            `Add exactly one routing label (recommended: '${suggested}').`
        );
      }

      if (unique.length !== 1) {
        const suggested = `${routingPrefixes[0]}${info.requestType}`;
        throw new Error(
          `Configuration error: issue template '${info.tplPath}' for requestType '${info.requestType}' ` +
            `has multiple unique labels and routing would be ambiguous: ${unique
              .map((x) => `'${x.raw}'`)
              .join(', ')}. ` +
            `Fix: keep only one unique routing label OR add exactly one prefixed routing label (recommended: '${suggested}').`
        );
      }

      routing = unique;
    }

    const chosen = routing[0];
    const existing = labelToType.get(chosen.norm);

    if (existing && existing.requestType !== info.requestType) {
      throw new Error(
        `Configuration error: routing label '${chosen.raw}' is mapped to multiple requestTypes ` +
          `('${existing.requestType}' and '${info.requestType}').`
      );
    }

    labelToType.set(chosen.norm, { requestType: info.requestType, label: chosen.raw });
    expectedLabels.push(chosen.raw);
  }

  const out: LabelIndex = {
    labelToType,
    expectedLabels: Array.from(new Set(expectedLabels)).sort(),
  };

  if (DBG && context.log?.debug) {
    context.log.debug({ owner, repo, routingLabels: out.expectedLabels }, 'tpl:label-index:built');
  }

  TEMPLATE_CACHE.set(idxCacheKey, { ts: now(), data: out });
  return out;
};

const detectRequestTypeFromIssueLabels = (issueLabels: unknown, labelIndex: LabelIndex): string => {
  const labels = toLabelStrings(issueLabels);
  if (labels.length === 0) return '';

  const hits = new Set<string>();
  const matchedLabels: string[] = [];

  for (const lab of labels) {
    const hit = labelIndex.labelToType.get(normalizeLabel(lab));
    if (hit?.requestType) {
      hits.add(hit.requestType);
      matchedLabels.push(hit.label || lab);
    }
  }

  if (hits.size === 0) return '';
  if (hits.size > 1) {
    throw new Error(
      `Cannot resolve template: issue has multiple routing labels for different requestTypes (${Array.from(hits).join(
        ', '
      )}). Matched labels: ${matchedLabels.join(', ')}`
    );
  }

  return Array.from(hits)[0];
};

// Raw structures parsed from YAML/Markdown front-matter
type RawAttributes = {
  name?: unknown;
  title?: unknown;
  labels?: unknown;
  body?: unknown;
};

type RawTemplateFile = {
  name?: unknown;
  title?: unknown;
  labels?: unknown;
  body?: unknown;
  schema?: unknown;
  root?: unknown;
  attributes?: RawAttributes | null | undefined;
  _meta?: Record<string, unknown> | null | undefined;
};

export async function loadTemplate(
  context: ContextLike,
  opts: {
    owner: string;
    repo: string;
    templatePath?: string;
    issueLabels?: unknown;
    issueTitle?: string;
  }
): Promise<Template> {
  const owner = opts?.owner ?? '';
  const repo = opts?.repo ?? '';
  const templatePath = opts?.templatePath;
  const issueLabels = opts?.issueLabels ?? [];
  const issueTitle = opts?.issueTitle ?? '';

  if (!context?.octokit) {
    throw new Error('Configuration error: octokit is not available in context.');
  }

  const octokit = context.octokit;
  if (!owner || !repo) {
    throw new Error('Configuration error: owner/repo are required to load templates.');
  }

  const labels = toLabelStrings(issueLabels);

  if (DBG && context.log?.debug) {
    context.log.debug(
      {
        owner,
        repo,
        templatePath: String(templatePath ?? ''),
        issueTitle: String(issueTitle ?? '').slice(0, 120),
        issueLabels: labels,
      },
      'tpl:loadTemplate:input'
    );
  }

  const fetchFile = async (path: string): Promise<Template> => {
    const pathKey = `${owner}/${repo}:${path}`;
    const fh = TEMPLATE_FILE_CACHE.get(pathKey);
    if (fh && now() - fh.ts < TTL_MS) {
      if (DBG && context.log?.debug) {
        context.log.debug({ path, ageMs: now() - fh.ts }, 'tpl:file-cache-hit');
      }
      return fh.data;
    }

    const { data } = await octokit.repos.getContent({ owner, repo, path });

    type RepoFile = { content?: unknown; encoding?: unknown } & Record<string, unknown>;
    const file = data as RepoFile;

    if (Array.isArray(data) || !isPlainObject(data) || typeof file.content !== 'string') {
      throw new Error(`Template path '${path}' is not a file.`);
    }

    const encoding = typeof file.encoding === 'string' ? file.encoding : 'base64';
    const text = Buffer.from(file.content, encoding as BufferEncoding).toString('utf8');

    let parsed: Record<string, unknown> = {};
    let body: unknown[] = [];

    if (/\.md$/i.test(path)) {
      const { fm } = parseFrontMatterMd(text);
      parsed = fm ?? {};
      const bodyField = (parsed as { body?: unknown }).body;
      body = Array.isArray(bodyField) ? (bodyField as unknown[]) : [];
    } else {
      try {
        const y = YAML.parse(text) as unknown;
        parsed = isPlainObject(y) ? y : {};
      } catch {
        parsed = {};
      }

      const rawParsed = parsed as RawTemplateFile;
      if (Array.isArray(rawParsed.body)) {
        body = rawParsed.body as unknown[];
      } else if (rawParsed.attributes && Array.isArray(rawParsed.attributes.body)) {
        body = rawParsed.attributes.body as unknown[];
      } else {
        body = [];
      }
    }
    const raw = parsed as RawTemplateFile;

    const nameRaw = raw.name ?? raw.attributes?.name ?? '';
    const titleRaw = raw.title ?? raw.attributes?.title ?? '';

    const labelsFromTop = asStringArray(raw.labels);
    const labelsFromAttr = asStringArray(raw.attributes?.labels);
    const tplLabels = labelsFromTop.length > 0 ? labelsFromTop : labelsFromAttr;

    const rawMeta = raw._meta;
    const parsedMeta = isPlainObject(rawMeta) ? rawMeta : {};

    const metaNameFallback = typeof parsedMeta.name === 'string' ? parsedMeta.name : '';
    const metaSchemaFallback = typeof parsedMeta.schema === 'string' ? parsedMeta.schema : undefined;
    const metaRootFallback = typeof parsedMeta.root === 'string' ? parsedMeta.root : undefined;

    const schemaTop = typeof raw.schema === 'string' ? raw.schema : undefined;
    const rootTop = typeof raw.root === 'string' ? raw.root : undefined;

    const tpl: Template = {
      ...raw,
      title: typeof titleRaw === 'string' ? titleRaw : String(titleRaw ?? ''),
      labels: tplLabels,
      body,
      _meta: {
        ...parsedMeta,
        path,
        name: String(nameRaw ?? metaNameFallback ?? '').trim(),
        schema: metaSchemaFallback || schemaTop || undefined,
        root: metaRootFallback || rootTop || undefined,
      },
    };

    if (DBG && context.log?.debug) {
      context.log.debug(
        {
          path,
          metaName: tpl._meta?.name || '',
          title: tpl.title || '',
          labels: tplLabels,
          bodyFields: Array.isArray(tpl.body) ? tpl.body.length : 0,
        },
        'tpl:fetched'
      );
    }

    TEMPLATE_FILE_CACHE.set(pathKey, { ts: now(), data: tpl });
    return tpl;
  };

  if (templatePath && String(templatePath).trim()) {
    const resolvedPath = resolveRepoPathFromConfig(templatePath);
    const tpl = await fetchFile(resolvedPath);

    const byTpl = findRequestByTemplatePath(context, resolvedPath);
    if (byTpl) {
      await applyRequestMetaAndAttachParseAllowedFieldIds(context, owner, repo, tpl, byTpl.requestType, byTpl.rc);
    }

    const cacheKey = `${owner}/${repo}:path:${resolvedPath}`;
    TEMPLATE_CACHE.set(cacheKey, { ts: now(), data: tpl });
    return tpl;
  }

  const labelIndex = await buildLabelIndexFromTemplates(context, { owner, repo, fetchFile });
  const requestType = detectRequestTypeFromIssueLabels(labels, labelIndex);

  if (!requestType) {
    const prefixes = getRoutingLabelPrefixes(context);
    throw new Error(
      `Cannot resolve template: no routing label found on issue. Expected exactly one routing label (e.g. '${prefixes[0]}<requestType>') from: ${labelIndex.expectedLabels.join(
        ', '
      )}`
    );
  }

  const req = getRequestsConfig(context);
  const rc = req?.[requestType];
  if (!rc?.issueTemplate) {
    throw new Error(`Cannot resolve template: requestType '${requestType}' not mapped in cfg.requests.`);
  }

  const resolvedTplPath = resolveRepoPathFromConfig(rc.issueTemplate);

  const cacheKey = `${owner}/${repo}:requestType:${requestType}`;
  const cached = TEMPLATE_CACHE.get(cacheKey) as CacheEntry<Template> | undefined;
  if (cached && now() - cached.ts < TTL_MS) {
    if (DBG && context.log?.debug) {
      context.log.debug({ cacheKey, ageMs: now() - cached.ts }, 'tpl:cache-hit');
    }
    return cached.data;
  }

  const tpl = await fetchFile(resolvedTplPath);
  await applyRequestMetaAndAttachParseAllowedFieldIds(context, owner, repo, tpl, requestType, rc);

  if (!tpl._meta?.requestType) {
    if (!tpl._meta) {
      tpl._meta = { path: '', name: '', requestType };
    } else {
      const meta: TemplateMeta = { ...tpl._meta, requestType };
      tpl._meta = meta;
    }
  }

  TEMPLATE_CACHE.set(cacheKey, { ts: now(), data: tpl });
  return tpl;
}

type TemplateFieldMinimal = {
  id?: string;
  attributes?: { label?: string };
};

function isTemplateField(value: unknown): value is TemplateFieldMinimal {
  if (!value || typeof value !== 'object') return false;
  const v = value as Record<string, unknown>;
  const idOk = v.id === undefined || typeof v.id === 'string';
  const attrs = v.attributes;
  const attrsOk = true;
  if (attrs !== undefined) {
    if (!isPlainObject(attrs)) return false;
    const lbl = attrs.label;
    if (!(lbl === undefined || typeof lbl === 'string')) return false;
  }
  return idOk && attrsOk;
}

type ParseFormOptions = {
  allowedFieldIds?: Iterable<string> | null;
};

export function parseForm(body: string, template: Template, options: ParseFormOptions = {}): Record<string, string> {
  const bodyFields: TemplateFieldMinimal[] = Array.isArray(template.body) ? template.body.filter(isTemplateField) : [];

  const hasAllowedFieldOverride = Object.hasOwn(options, 'allowedFieldIds');

  const allowedFieldIds = hasAllowedFieldOverride
    ? (options.allowedFieldIds ?? [])
    : Array.isArray(template._meta?.parseAllowedFieldIds)
      ? template._meta.parseAllowedFieldIds
      : [];

  return parseFormRaw(
    body || '',
    {
      body: bodyFields,
      _meta: {
        parseAllowedFieldIds: Array.from(allowedFieldIds || []),
      },
    },
    {
      allowedFieldIds,
    }
  );
}
