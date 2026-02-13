const DBG = process.env.DEBUG_NS === '1';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function getHttpStatus(err: unknown): number | undefined {
  if (err && typeof err === 'object' && 'status' in (err as Record<string, unknown>)) {
    const s = (err as { status?: unknown }).status;
    return typeof s === 'number' ? s : undefined;
  }
  return undefined;
}

function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((x) => String(x ?? '').trim()).filter(Boolean);
}
function coerceOptionalString(obj: Record<string, unknown>, key: string): void {
  const v = obj[key];
  if (v === undefined || v === null) return;
  if (typeof v === 'string') obj[key] = v.trim();
  else if (typeof v === 'number' || typeof v === 'boolean') obj[key] = String(v).trim();
}

function normalizeEnabled(value: unknown): boolean | null | undefined {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (v === 'true') return true;
    if (v === 'false') return false;
    return null;
  }
  if (typeof value === 'boolean') return value;
  return null;
}

function normalizeMethod(value: unknown): string | null | undefined {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return null;
}
import YAML from 'yaml';
import Ajv2020 from 'ajv/dist/2020.js';
import ajvErrors from 'ajv-errors';
import { createHash } from 'node:crypto';
import type { ErrorObject, ValidateFunction } from 'ajv';
import { DEFAULT_CONFIG as DEFAULT_CONFIG_RAW, STATIC_CONFIG_SCHEMA } from './handlers/request/constants.js';

type WorkflowLabelsConfig = {
  routingLabelPrefixes?: string | string[];
  routingLabelPrefix?: string | string[];
  global?: string | string[];
  authorAction?: string | number | boolean | null;
  approverAction?: string | number | boolean | null;
  autoMergeCandidate?: string | number | boolean | null;
  approvalRequested?: string | string[] | null;
  approvalSuccessful?: string | string[] | null;
  [k: string]: unknown;
};

type NormalizedRequestConfig = {
  issueTemplate?: string;
  schema?: string;
  folderName?: string;
  approvers?: string[] | null;
  [k: string]: unknown;
};

type PrConfig = {
  branchNameTemplate?: string;
  titleTemplate?: string;
  autoMerge?: {
    enabled?: boolean | null;
    method?: string | null;
    [k: string]: unknown;
  };
  [k: string]: unknown;
};

type WorkflowConfig = {
  labels?: WorkflowLabelsConfig;
  approvers?: string[] | null;
  [k: string]: unknown;
};

export type NormalizedStaticConfig = {
  requests?: Record<string, NormalizedRequestConfig>;
  pr?: PrConfig;
  workflow?: WorkflowConfig;
  validation?: Record<string, unknown>;
  registry?: Record<string, unknown>;
  schema?: Record<string, unknown>;
  [k: string]: unknown;
};

type RepoRef = { owner: string; repo: string };

type IssueLike = { number: number; title: string };

type OctokitLike = {
  repos: {
    getContent: (params: { owner: string; repo: string; path: string }) => Promise<{ data: unknown }>;
  };
  issues: {
    listForRepo: (params: {
      owner: string;
      repo: string;
      state: 'open' | 'closed' | 'all';
      per_page?: number;
    }) => Promise<{ data: IssueLike[] }>;
    update: (params: {
      owner: string;
      repo: string;
      issue_number: number;
      body?: string;
      state?: 'open' | 'closed';
    }) => Promise<unknown>;
    create: (params: {
      owner: string;
      repo: string;
      title: string;
      body?: string;
      labels?: string[];
    }) => Promise<unknown>;
    createComment: (params: { owner: string; repo: string; issue_number: number; body: string }) => Promise<unknown>;
  };
};

type LogLike = {
  debug?: (obj: unknown, msg?: string) => void;
  warn?: (obj: unknown, msg?: string) => void;
  info?: (obj: unknown, msg?: string) => void;
};

export type RegistryBotHooks = Record<string, unknown>;

type RegistryBotContextLike = {
  octokit: OctokitLike;
  log?: LogLike;
  repo: () => { owner: string; repo: string };
};

type LoadStaticConfigOptions = {
  validate?: boolean;
  updateIssue?: boolean;
  forceReload?: boolean;
};

type LoadStaticConfigResult = {
  config: NormalizedStaticConfig;
  source: string;
  hooks: RegistryBotHooks | null;
  hooksSource: string | null;
};

const CONFIG_LOCATIONS = ['.github/registry-bot/config.yaml', '.github/registry-bot/config.yml'] as const;
const JS_CONFIG_LOCATIONS = ['.github/registry-bot/config.js'] as const;

const CONFIG_TTL_MS = 5 * 60 * 1000; // 5 minutes
const CONFIG_CACHE = new Map<string, { ts: number; value: LoadStaticConfigResult }>();

const MINIMAL_CONFIG_EXAMPLE_YAML = `
requests:
  sample:
    folderName: resources
    schema: .github/registry-bot/schemas/sample.json
    issueTemplate: .github/ISSUE_TEMPLATE/sample.md
pr:
  branchNameTemplate: req/\${type}-\${id}
  titleTemplate: Request: \${title}
  autoMerge:
    enabled: false
    method: squash
workflow:
  labels:
    routingLabelPrefix: request:
`;

export class StaticConfigValidationError extends Error {
  public readonly errors: string[];
  public readonly rawErrors: ErrorObject[] | null | undefined;

  public constructor(message: string, errors: string[], rawErrors: ErrorObject[] | null | undefined) {
    super(message);
    this.name = 'StaticConfigValidationError';
    this.errors = errors;
    this.rawErrors = rawErrors;
  }
}

export class StaticConfigMissingError extends Error {
  public readonly errors: string[];

  public constructor(message: string, errors: string[]) {
    super(message);
    this.name = 'StaticConfigMissingError';
    this.errors = errors;
  }
}

const CONFIG_VALIDATOR: ValidateFunction<unknown> = ((): ValidateFunction<unknown> => {
  const ajvCtor = Ajv2020 as unknown as new (opts?: Record<string, unknown>) => {
    compile: (schema: unknown) => ValidateFunction<unknown>;
  };
  const ajv = new ajvCtor({ allErrors: true, strict: false });
  try {
    const addErrors = ajvErrors as unknown as (a: unknown) => void;
    addErrors(ajv);
  } catch {
    // ignore optional errors plugin failures
  }
  return ajv.compile(STATIC_CONFIG_SCHEMA as unknown);
})();

function validateStaticConfigShape(config: unknown, context: RegistryBotContextLike, source: string | null): void {
  const valid = CONFIG_VALIDATOR(config);
  if (valid) return;

  const rawErrors = CONFIG_VALIDATOR.errors;
  const errors = (rawErrors ?? []).map((e) => `${e.instancePath || '/'} ${e.message || ''}`.trim());
  const msg = `Invalid registry-bot config at ${source || 'unknown'}: ${errors.join('; ')}`;

  context.log?.warn?.({ errors, source }, msg);
  throw new StaticConfigValidationError(msg, errors, rawErrors);
}

type RepoContentFile = { content: string; encoding?: string };

function isRepoContentFile(data: unknown): data is RepoContentFile {
  return isPlainObject(data) && typeof data.content === 'string';
}

async function readRepoFileIfExists(octokit: OctokitLike, ref: RepoRef, filePath: string): Promise<string | null> {
  try {
    const res = await octokit.repos.getContent({
      owner: ref.owner,
      repo: ref.repo,
      path: filePath,
    });
    const data = res.data;

    if (Array.isArray(data) || !isRepoContentFile(data)) return null;

    // GitHub contents API returns base64 in practice for file content.
    return Buffer.from(data.content, 'base64').toString('utf8');
  } catch (err: unknown) {
    if (getHttpStatus(err) === 404) return null;
    throw err;
  }
}

function parseConfigString(raw: string, filePath: string): unknown {
  const lower = filePath.toLowerCase();
  if (lower.endsWith('.yml') || lower.endsWith('.yaml')) return (YAML.parse(raw) ?? {}) as unknown;
  if (lower.endsWith('.json')) return JSON.parse(raw) as unknown;
  throw new Error(`Unsupported config extension for ${filePath}`);
}

function deepMerge(base: Record<string, unknown>, override: unknown): Record<string, unknown> {
  if (!isPlainObject(override)) return base;

  const result: Record<string, unknown> = { ...base };

  for (const [key, value] of Object.entries(override)) {
    if (value === undefined || value === null) continue;

    const existing = result[key];

    if (isPlainObject(value) && isPlainObject(existing)) {
      result[key] = deepMerge(existing, value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

function buildRequests(top: Record<string, unknown>): Record<string, NormalizedRequestConfig> {
  const rawRequests = top['requests'];
  const requestsObj = isPlainObject(rawRequests) ? rawRequests : {};
  const requests: Record<string, NormalizedRequestConfig> = {};

  for (const [k, rc0] of Object.entries(requestsObj)) {
    const rc = isPlainObject(rc0) ? { ...rc0 } : {};

    coerceOptionalString(rc as Record<string, unknown>, 'folderName');
    coerceOptionalString(rc as Record<string, unknown>, 'schema');
    coerceOptionalString(rc as Record<string, unknown>, 'issueTemplate');

    const approversRaw = (rc as Record<string, unknown>)['approvers'];
    if (approversRaw === null) {
      (rc as Record<string, unknown>)['approvers'] = null;
    } else if (approversRaw !== undefined) {
      (rc as Record<string, unknown>)['approvers'] = normalizeStringArray(approversRaw);
    }
    requests[k] = rc as NormalizedRequestConfig;
  }

  return requests;
}

function buildPr(top: Record<string, unknown>): PrConfig {
  const prRaw = top['pr'];
  const prObj = isPlainObject(prRaw) ? { ...prRaw } : {};

  coerceOptionalString(prObj as Record<string, unknown>, 'branchNameTemplate');
  coerceOptionalString(prObj as Record<string, unknown>, 'titleTemplate');

  const amRaw = prObj['autoMerge'];
  const amObj = isPlainObject(amRaw) ? { ...amRaw } : {};

  const enabled: boolean | null | undefined = normalizeEnabled(amObj['enabled']);

  const method: string | null | undefined = normalizeMethod(amObj['method']);

  const pr: PrConfig = {
    ...(prObj as Record<string, unknown>),
    autoMerge: {
      ...(amObj as Record<string, unknown>),
      enabled,
      method,
    },
  };
  return pr;
}

function buildWorkflow(top: Record<string, unknown>): WorkflowConfig {
  const wfRaw = top['workflow'];
  const wfObj = isPlainObject(wfRaw) ? { ...wfRaw } : {};

  const labelsRaw = wfObj['labels'];
  const labelsObj = isPlainObject(labelsRaw) ? { ...labelsRaw } : {};

  if (labelsObj['global'] !== undefined && labelsObj['global'] !== null) {
    labelsObj['global'] = normalizeStringArray(labelsObj['global']);
  }

  for (const k of ['authorAction', 'approverAction', 'autoMergeCandidate'] as const) {
    coerceOptionalString(labelsObj as Record<string, unknown>, k);
  }

  for (const k of ['approvalRequested', 'approvalSuccessful'] as const) {
    if (labelsObj[k] !== undefined && labelsObj[k] !== null) {
      labelsObj[k] = normalizeStringArray(labelsObj[k]);
    }
  }

  let approvers: string[] | null | undefined;
  const approversRaw = wfObj['approvers'];
  if (approversRaw === undefined) approvers = undefined;
  else if (approversRaw === null) approvers = null;
  else approvers = normalizeStringArray(approversRaw);

  const workflow: WorkflowConfig = {
    ...(wfObj as Record<string, unknown>),
    labels: labelsObj as WorkflowLabelsConfig,
    approvers,
  };
  return workflow;
}

function normalizeStaticConfig(rawConfig: unknown): NormalizedStaticConfig {
  const top = isPlainObject(rawConfig) ? { ...rawConfig } : {};
  if (Object.hasOwn(top, 'issueTemplate')) delete top['issueTemplate'];

  const requests = buildRequests(top);
  const pr = buildPr(top);
  const workflow = buildWorkflow(top);

  const out: NormalizedStaticConfig = {
    ...(top as Record<string, unknown>),
    requests,
    pr,
    workflow,
  };

  const validationRaw = top['validation'];
  const registryRaw = top['registry'];
  const schemaRaw = top['schema'];

  if (validationRaw !== undefined) out.validation = isPlainObject(validationRaw) ? validationRaw : {};
  if (registryRaw !== undefined) out.registry = isPlainObject(registryRaw) ? registryRaw : {};
  if (schemaRaw !== undefined) out.schema = isPlainObject(schemaRaw) ? schemaRaw : {};

  return out;
}

async function createOrUpdateStaticConfigIssue(
  context: RegistryBotContextLike,
  args: {
    source: string | null;
    err: StaticConfigValidationError | StaticConfigMissingError | Error;
  }
): Promise<void> {
  const { owner, repo } = context.repo();

  const err = args.err;
  const source = args.source;

  const errors =
    err instanceof StaticConfigValidationError || err instanceof StaticConfigMissingError ? err.errors : [err.message];

  const isMissing = err.name === 'StaticConfigMissingError';
  const title = 'registry-bot: invalid static config.yaml';

  let existing: IssueLike | null = null;

  try {
    const { data: issues } = await context.octokit.issues.listForRepo({
      owner,
      repo,
      state: 'open',
      per_page: 50,
    });

    existing = issues.find((i) => i.title === title) ?? null;
  } catch (error_: unknown) {
    context.log?.warn?.({ err: asError(error_).message }, 'failed to list issues while reporting static config error');
  }

  const sourceDisplay = source || '.github/registry-bot/config.yaml';
  const errorList = errors.map((e) => `- ${e}`).join('\n');

  let bodyBlock: string;

  if (isMissing) {
    const header =
      `No static registry-bot configuration file was found in this repository.\n\n` +
      `The bot requires a config file at one of the following locations:\n` +
      `- \`.github/registry-bot/config.yaml\`\n` +
      `- \`.github/registry-bot/config.yml\`\n\n`;

    const details =
      `Detected problem:\n${errorList}\n\n` +
      `Below is a minimal example that passes the built-in schema. ` +
      `You should at least configure one requests entry (requestType -> folderName/schema/issueTemplate):\n\n` +
      '```yaml\n' +
      MINIMAL_CONFIG_EXAMPLE_YAML +
      '\n```\n\n' +
      `_Last check: ${new Date().toISOString()}_`;

    bodyBlock = header + details;
  } else {
    const header = `The static configuration file \`${sourceDisplay}\` is invalid according to the built-in registry-bot schema.\n\n`;
    bodyBlock =
      `${header}Detected schema errors:\n${errorList}\n\n` +
      `Please fix requests mapping and push an update. ` +
      `_Last check: ${new Date().toISOString()}_`;
  }

  if (existing) {
    if (DBG && context.log?.debug) {
      context.log.debug(
        { owner, repo, issue_number: existing.number, source, errorsCount: errors.length },
        'static-config:issue:update-existing'
      );
    }
    try {
      await context.octokit.issues.update({
        owner,
        repo,
        issue_number: existing.number,
        body: bodyBlock,
      });
    } catch (error_: unknown) {
      context.log?.warn?.(
        { err: asError(error_).message, issue_number: existing.number },
        'failed to update static config error issue'
      );
    }
  } else {
    try {
      await context.octokit.issues.create({
        owner,
        repo,
        title,
        body: bodyBlock,
        labels: ['registry-bot', 'config-error'],
      });
    } catch (error_: unknown) {
      context.log?.warn?.({ err: asError(error_).message }, 'failed to create static config error issue');
    }
  }
}

async function closeStaticConfigIssueIfResolved(
  context: RegistryBotContextLike,
  args: { source: string | null }
): Promise<void> {
  const { owner, repo } = context.repo();
  const title = 'registry-bot: invalid static config.yaml';

  try {
    const { data: issues } = await context.octokit.issues.listForRepo({
      owner,
      repo,
      state: 'open',
      per_page: 50,
    });

    const existing = issues.find((i) => i.title === title);
    if (!existing) {
      if (DBG && context.log?.debug) {
        context.log.debug({ owner, repo, source: args.source }, 'static-config:issue:close-skip-no-existing');
      }
      return;
    }

    if (DBG && context.log?.debug) {
      context.log.debug(
        { owner, repo, issue_number: existing.number, source: args.source },
        'static-config:issue:close-existing'
      );
    }

    const sourceDisplay = args.source || '.github/registry-bot/config.yaml';
    const body =
      `Static configuration is now valid.\n\n` +
      `The file \`${sourceDisplay}\` currently passes schema validation.\n\n` +
      `_Checked at ${new Date().toISOString()}_`;

    await context.octokit.issues.createComment({
      owner,
      repo,
      issue_number: existing.number,
      body,
    });

    await context.octokit.issues.update({
      owner,
      repo,
      issue_number: existing.number,
      state: 'closed',
    });
  } catch (err: unknown) {
    context.log?.warn?.(
      { err: asError(err).message },
      'failed to close static config error issue after successful validation'
    );
  }
}

async function loadJsConfigFromRepo(
  context: RegistryBotContextLike,
  ref: RepoRef
): Promise<{ hooks: RegistryBotHooks | null; source: string | null }> {
  for (const filePath of JS_CONFIG_LOCATIONS) {
    const raw = await readRepoFileIfExists(context.octokit, ref, filePath);
    if (!raw) continue;

    try {
      const hash = createHash('sha256').update(raw).digest('hex').slice(0, 16);

      // Only return the raw ESM source + metadata
      const hooksDescriptor: RegistryBotHooks = {
        __type: 'registry-bot-hooks:esm',
        __path: filePath,
        __hash: hash,
        __code: raw,
      };

      // Include hash so cache keys change when hooks change
      return { hooks: hooksDescriptor, source: `repo:${filePath}#${hash}` };
    } catch (err: unknown) {
      context.log?.warn?.(
        { err: asError(err).message, repo: ref.repo, path: filePath },
        'failed to prepare ESM registry-bot hooks descriptor'
      );
    }
  }

  return { hooks: null, source: null };
}

async function loadConfigFromRepo(
  context: RegistryBotContextLike,
  owner: string,
  repo: string,
  locations: readonly string[]
): Promise<{ merged: Record<string, unknown>; source: string } | null> {
  let merged: Record<string, unknown> = {
    ...(DEFAULT_CONFIG_RAW as unknown as Record<string, unknown>),
  };
  for (const filePath of locations) {
    const raw = await readRepoFileIfExists(context.octokit, { owner, repo }, filePath);
    if (!raw) continue;

    try {
      const parsed = parseConfigString(raw, filePath);
      merged = deepMerge(merged, parsed);
      const source = `repo:${filePath}`;
      return { merged, source };
    } catch (err: unknown) {
      context.log?.warn?.({ err: asError(err).message }, 'failed to parse static config');
    }
  }
  return null;
}

async function getInitialConfig(
  context: RegistryBotContextLike,
  owner: string,
  repo: string
): Promise<{ config: Record<string, unknown>; source: string }> {
  let config: Record<string, unknown> = {
    ...(DEFAULT_CONFIG_RAW as unknown as Record<string, unknown>),
  };
  let source = 'default';

  const repoCfg = await loadConfigFromRepo(context, owner, repo, CONFIG_LOCATIONS);
  if (repoCfg) {
    config = repoCfg.merged;
    source = repoCfg.source;
    return { config, source };
  }

  const orgRepo = '.github';
  const orgCfg = await loadConfigFromRepo(context, owner, orgRepo, CONFIG_LOCATIONS);
  if (orgCfg) {
    config = orgCfg.merged;
    source = orgCfg.source.replace('repo:', 'org:');
  }

  return { config, source };
}

async function validateOrFallbackAndReport(
  context: RegistryBotContextLike,
  config: Record<string, unknown>,
  source: string,
  options: { validate: boolean; updateIssue: boolean }
): Promise<{ config: Record<string, unknown>; source: string }> {
  const { validate, updateIssue } = options;
  const hasUserConfig = source !== 'default';

  if (DBG && context.log?.debug) {
    context.log.debug(
      { ...context.repo(), source, hasUserConfig, validate, updateIssue },
      'static-config:load:validation-decision'
    );
  }

  if (hasUserConfig && validate) {
    try {
      validateStaticConfigShape(config, context, source);
      return { config, source };
    } catch (err: unknown) {
      const e = err instanceof Error ? err : new Error(String(err));

      context.log?.warn?.(
        { err: e.message, source },
        'static config validation failed - falling back to DEFAULT_CONFIG'
      );

      if (updateIssue) {
        await reportValidationError(context, source, e);
      }

      return {
        config: { ...(DEFAULT_CONFIG_RAW as unknown as Record<string, unknown>) },
        source: 'default-invalid-config',
      };
    }
  }

  return { config, source };
}

async function reportMissingIfNeeded(
  context: RegistryBotContextLike,
  source: string,
  options: { validate: boolean; updateIssue: boolean }
): Promise<void> {
  const { validate, updateIssue } = options;
  const hasUserConfig = source !== 'default';

  if (!hasUserConfig && validate && updateIssue) {
    if (DBG && context.log?.debug) {
      context.log.debug({ source }, 'no static registry-bot config found; reporting missing config issue');
    }

    const err = new StaticConfigMissingError(
      'Missing static registry-bot config file: expected .github/registry-bot/config.yaml or .github/registry-bot/config.yml',
      ['No static config file found at .github/registry-bot/config.yaml or .github/registry-bot/config.yml']
    );

    try {
      await createOrUpdateStaticConfigIssue(context, {
        source: '.github/registry-bot/config.yaml',
        err,
      });
    } catch (error_: unknown) {
      context.log?.warn?.({ err: asError(error_).message }, 'failed to report missing static config via issue');
    }
  } else if (!hasUserConfig) {
    context.log?.debug?.({ source }, 'no static registry-bot config found; using DEFAULT_CONFIG without validation');
  } else if (hasUserConfig && !validate && DBG && context.log?.debug) {
    context.log.debug({ ...context.repo(), source }, 'static-config:validation-skipped');
  }
}

async function reportValidationError(context: RegistryBotContextLike, source: string, err: Error): Promise<void> {
  try {
    await createOrUpdateStaticConfigIssue(context, { source, err });
  } catch (error_: unknown) {
    context.log?.warn?.({ err: asError(error_).message }, 'failed to report static config validation error via issue');
  }
}

function getCachedResult(
  context: RegistryBotContextLike,
  cacheKey: string,
  forceReload: boolean
): LoadStaticConfigResult | null {
  if (forceReload) return null;
  const cached = CONFIG_CACHE.get(cacheKey);
  if (!cached) return null;
  if (Date.now() - cached.ts < CONFIG_TTL_MS) {
    if (DBG && context.log?.debug) {
      const { owner, repo } = context.repo();
      context.log.debug({ owner, repo, source: cached.value.source }, 'static-config:cache-hit');
    }
    return cached.value;
  }
  return null;
}

async function loadHooks(
  context: RegistryBotContextLike,
  owner: string,
  repo: string,
  source: string
): Promise<{ hooks: RegistryBotHooks | null; hooksSource: string | null }> {
  try {
    const jsRes = await loadJsConfigFromRepo(context, { owner, repo });
    let hooks = jsRes.hooks;
    let hooksSource = jsRes.source;

    if (!hooks && source === 'default') {
      const orgRes = await loadJsConfigFromRepo(context, { owner, repo: '.github' });
      hooks = orgRes.hooks;
      hooksSource = orgRes.source;
    }
    return { hooks, hooksSource };
  } catch (err: unknown) {
    context.log?.warn?.(
      { err: asError(err).message },
      'failed to load JS registry-bot config; continuing without hooks'
    );
    return { hooks: null, hooksSource: null };
  }
}

async function maybeCloseIssueAfterValidation(
  context: RegistryBotContextLike,
  source: string,
  options: { validate: boolean; updateIssue: boolean }
): Promise<void> {
  const { validate, updateIssue } = options;
  if (validate && updateIssue && source !== 'default' && source !== 'default-invalid-config') {
    try {
      await closeStaticConfigIssueIfResolved(context, { source });
    } catch (err: unknown) {
      context.log?.warn?.(
        { err: asError(err).message },
        'failed to check and close static config error issue after successful validation'
      );
    }
  }
}

export async function loadStaticConfig(
  context: RegistryBotContextLike,
  options: LoadStaticConfigOptions = {}
): Promise<LoadStaticConfigResult> {
  const validate = options.validate ?? true;
  const updateIssue = options.updateIssue ?? true;
  const forceReload = options.forceReload ?? false;

  const { owner, repo } = context.repo();

  const cacheKey = `${owner}/${repo}:${validate ? 'v1' : 'v0'}:${updateIssue ? 'i1' : 'i0'}`;
  const cached = getCachedResult(context, cacheKey, forceReload);
  if (cached) return cached;

  if (DBG && context.log?.debug) {
    context.log.debug({ owner, repo, configLocations: CONFIG_LOCATIONS }, 'static-config:load:start');
  }

  const init = await getInitialConfig(context, owner, repo);
  let config = init.config;
  let source = init.source;
  let hooks: RegistryBotHooks | null = null;
  let hooksSource: string | null = null;

  if (DBG && context.log?.debug) {
    let origin = 'default';
    if (source.startsWith('org:')) origin = 'org';
    else if (source.startsWith('repo:')) origin = 'repo';
    context.log.debug({ owner, repo, source }, `static-config:load:${origin}-config-detected`);
  }

  // note: hasUserConfig can be derived from source when needed

  const validated = await validateOrFallbackAndReport(context, config, source, {
    validate,
    updateIssue,
  });
  config = validated.config;
  source = validated.source;

  const normalized = normalizeStaticConfig(config);

  await maybeCloseIssueAfterValidation(context, source, { validate, updateIssue });
  await reportMissingIfNeeded(context, source, { validate, updateIssue });

  const hooksRes = await loadHooks(context, owner, repo, source);
  hooks = hooksRes.hooks;
  hooksSource = hooksRes.hooksSource;

  const result: LoadStaticConfigResult = { config: normalized, source, hooks, hooksSource };

  CONFIG_CACHE.set(cacheKey, { ts: Date.now(), value: result });

  context.log?.debug?.({ source, hooksSource }, 'static config loaded');
  return result;
}

export const DEFAULT_CONFIG: NormalizedStaticConfig = normalizeStaticConfig(DEFAULT_CONFIG_RAW);
