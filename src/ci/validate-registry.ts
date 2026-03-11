/* eslint-disable no-console */
import YAML from 'yaml';
import { readFile, access, readdir, stat } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import Ajv2020Module from 'ajv/dist/2020.js';
import addFormatsModule from 'ajv-formats';
import ajvErrorsModule from 'ajv-errors';
import type { ErrorObject, ValidateFunction } from 'ajv';
import path from 'node:path';
import { createHash } from 'node:crypto';
import type { RegistryBotHooks, NormalizedStaticConfig } from '../config.js';
import {
  runCustomValidateForRegistryCandidate,
  resolvePrimaryIdFromCandidate,
  type OctokitLike,
  type IssueListItem,
} from '../handlers/request/validation/run.js';

const CONFIG_BASE_DIR = '.github/registry-bot';
const MAIN_DISABLE_VALIDATION_KEY = 'x-sap-main-disable-validation';

type RequestsConfig = Record<string, { folderName: string; schema: string }>;

type LoadedValidationConfig = {
  requests: RequestsConfig;
  hooksAllowedHosts: string[];
};

type Mode = 'pr' | 'main';

type SchemaCandidate = {
  requestType: string;
  schemaPath: string;
};

type FileTarget = {
  filePath: string;
  candidates: SchemaCandidate[];
};

type SchemaCacheEntry = {
  validate: ValidateFunction<unknown>;
  typeConst: string;
  schemaObj: unknown;
};

type FileValidationTry = {
  requestType: string;
  schemaPath: string;
  ok: boolean;
  errors: string[];
  reason?: string;
};

type FileValidationResult = {
  filePath: string;
  ok: boolean;

  // Selected schema
  requestType: string;
  schemaPath: string;

  // All candidate attempts
  tries: FileValidationTry[];

  // For ok=false: errors from the selected/best candidate
  errors: string[];
};

type AjvInstance = {
  compile: (schema: unknown) => ValidateFunction<unknown>;
};

type AjvConstructor = new (opts?: { strict?: boolean; allErrors?: boolean }) => AjvInstance;

const ajv2020Ctor: AjvConstructor =
  (Ajv2020Module as unknown as { default?: AjvConstructor }).default ?? (Ajv2020Module as unknown as AjvConstructor);

type AjvPlugin = (ajv: unknown) => void;

const addFormats: AjvPlugin =
  (addFormatsModule as unknown as { default?: AjvPlugin }).default ?? (addFormatsModule as unknown as AjvPlugin);

const ajvErrors: AjvPlugin =
  (ajvErrorsModule as unknown as { default?: AjvPlugin }).default ?? (ajvErrorsModule as unknown as AjvPlugin);

const execFileAsync = promisify(execFile);

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function normalizeTypeToken(v: unknown): string {
  if (typeof v !== 'string') return '';
  return v.trim().toLowerCase();
}

function normalizeRepoPath(p: string): string {
  const s = String(p ?? '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/\/{2,}/g, '/'); // collapse repeated slashes

  return s
    .replace(/^(\.\/)+/, '')
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');
}

function isYamlPath(p: string): boolean {
  const s = p.toLowerCase();
  return s.endsWith('.yaml') || s.endsWith('.yml');
}

function pickMode(): Mode {
  const forced = String(process.env.REGISTRY_VALIDATE_MODE ?? '')
    .trim()
    .toLowerCase();
  if (forced === 'pr' || forced === 'main') return forced;

  const eventName = String(process.env.GITHUB_EVENT_NAME ?? '').trim();
  return eventName === 'pull_request' ? 'pr' : 'main';
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var ${name}`);
  return v;
}

async function readTextFromGitRevision(revision: string, repoPath: string): Promise<string | null> {
  const rev = String(revision ?? '').trim();
  const rel = normalizeRepoPath(repoPath);
  if (!rev || !rel) return null;

  try {
    const { stdout } = await execFileAsync('git', ['show', `${rev}:${rel}`], {
      maxBuffer: 5 * 1024 * 1024,
    });
    return String(stdout ?? '');
  } catch {
    return null;
  }
}

async function readTrustedRepoFileText(repoPath: string, trustedRef?: string | null): Promise<string> {
  const rel = normalizeRepoPath(repoPath);
  if (!rel) throw new Error(`Invalid repository path '${repoPath}'`);

  const trusted = String(trustedRef ?? '').trim();
  if (trusted) {
    const txt = await readTextFromGitRevision(trusted, rel);
    if (txt === null) throw new Error(`Missing trusted file '${rel}' at revision '${trusted}'`);
    return txt;
  }

  return await readFile(rel, 'utf8');
}

async function loadValidationConfig(trustedRef?: string | null): Promise<LoadedValidationConfig> {
  const paths = ['.github/registry-bot/config.yaml', '.github/registry-bot/config.yml'];

  let raw: string | null = null;
  let usedPath: string | null = null;

  for (const p of paths) {
    try {
      raw = await readTrustedRepoFileText(p, trustedRef);
      usedPath = p;
      break;
    } catch {
      // ignore
    }
  }

  if (!raw || !usedPath) {
    throw new Error('Missing registry-bot config: expected .github/registry-bot/config.yaml or .yml');
  }

  const parsed: unknown = YAML.parse(raw);
  if (!isPlainObject(parsed)) throw new Error(`Invalid YAML in ${usedPath}`);

  const requestsRaw = parsed['requests'];
  if (!isPlainObject(requestsRaw)) {
    throw new Error(`Invalid config: missing "requests" mapping in ${usedPath}`);
  }

  const out: RequestsConfig = {};

  for (const [requestType, rc0] of Object.entries(requestsRaw)) {
    if (!isPlainObject(rc0)) continue;

    const folderName = rc0['folderName'];
    const schema = rc0['schema'];

    if (typeof folderName !== 'string' || typeof schema !== 'string') continue;

    const folder = normalizeRepoPath(folderName);
    let schemaPath = normalizeRepoPath(schema);

    if (schemaPath && !schemaPath.startsWith(`${CONFIG_BASE_DIR}/`) && !schemaPath.startsWith('.github/')) {
      schemaPath = `${CONFIG_BASE_DIR}/${schemaPath}`;
    }

    out[requestType] = { folderName: folder, schema: schemaPath };
  }

  if (Object.keys(out).length === 0) {
    throw new Error(`Invalid config: no usable requests.*.folderName + requests.*.schema found in ${usedPath}`);
  }

  const hooksRaw = parsed['hooks'];
  const hooksAllowedHosts =
    isPlainObject(hooksRaw) && Array.isArray(hooksRaw['allowedHosts'])
      ? (hooksRaw['allowedHosts'] as unknown[]).map((x) => String(x ?? '').trim()).filter(Boolean)
      : [];

  return { requests: out, hooksAllowedHosts };
}

function matchRequestTypesForFile(filePath: string, requests: RequestsConfig): FileTarget | null {
  const fp = normalizeRepoPath(filePath);
  const candidates: SchemaCandidate[] = [];

  for (const [requestType, cfg] of Object.entries(requests)) {
    const folder = normalizeRepoPath(cfg.folderName);
    if (!folder) continue;

    if (fp === folder || fp.startsWith(`${folder}/`)) {
      candidates.push({ requestType, schemaPath: normalizeRepoPath(cfg.schema) });
    }
  }

  if (candidates.length === 0) return null;
  return { filePath: fp, candidates };
}

async function getChangedFiles(baseSha: string, headRef = 'HEAD'): Promise<string[]> {
  const run = async (lhs: string, rhs: string): Promise<string[]> => {
    const { stdout } = await execFileAsync('git', ['diff', '--name-only', '--diff-filter=AMR', lhs, rhs], {
      maxBuffer: 5 * 1024 * 1024,
    });

    return String(stdout ?? '')
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => s.replace(/\\/g, '/'));
  };

  try {
    return await run(baseSha, headRef);
  } catch {
    if (headRef !== 'HEAD') return await run(baseSha, 'HEAD');
    throw new Error(`Failed to diff changed files for base '${baseSha}' and head '${headRef}'`);
  }
}

async function getAllTrackedFilesUnder(folder: string): Promise<string[]> {
  const f = normalizeRepoPath(folder);
  if (!f) return [];

  const { stdout } = await execFileAsync('git', ['ls-files', '--', f], {
    maxBuffer: 10 * 1024 * 1024,
  });

  return String(stdout ?? '')
    .split('\n')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => s.replace(/\\/g, '/'));
}

function buildAjv(): AjvInstance {
  const ajv = new ajv2020Ctor({ strict: false, allErrors: true });

  try {
    addFormats(ajv);
  } catch {
    // ignore
  }

  try {
    ajvErrors(ajv);
  } catch {
    // ignore
  }

  return ajv;
}

function formatAjvErrors(errors: ErrorObject[] | null | undefined): string[] {
  return (errors ?? []).map((e) => {
    const whereRaw = String(e.instancePath ?? '').trim();
    const where = whereRaw && whereRaw !== '/' ? whereRaw : '';
    const msg = String(e.message ?? '').trim();
    return where ? `${where} ${msg}`.trim() : msg;
  });
}

function cloneJson<T>(v: T): T {
  // schema is JSON
  return JSON.parse(JSON.stringify(v)) as T;
}

/**
 * In "main" mode we want to keep legacy files valid.
 * For any object schema level:
 * - if a property schema has x-sap-main-disable-validation: true
 * - remove that property name from "required"
 *
 * This only relaxes "required". If the field exists, it is still validated normally.
 */
function stripMainDisabledRequired(schema: unknown): void {
  if (Array.isArray(schema)) {
    for (const v of schema) stripMainDisabledRequired(v);
    return;
  }
  if (!isPlainObject(schema)) return;

  const props = schema['properties'];
  const req = schema['required'];

  if (isPlainObject(props) && Array.isArray(req)) {
    const disabled = new Set<string>();
    for (const [k, v] of Object.entries(props)) {
      if (isPlainObject(v) && v[MAIN_DISABLE_VALIDATION_KEY] === true) {
        disabled.add(k);
      }
    }

    if (disabled.size > 0) {
      const nextReq = req.filter((r) => typeof r !== 'string' || !disabled.has(r));
      if (nextReq.length > 0) schema['required'] = nextReq;
      else delete schema['required'];

      // Also relax validation for these fields in main mode
      for (const key of disabled) {
        // Replace the property's schema with an empty schema => always valid
        props[key] = {};
      }
    }
  }

  for (const v of Object.values(schema)) {
    stripMainDisabledRequired(v);
  }
}

function maybePatchSchemaForMain(schemaObj: unknown, mode: Mode): unknown {
  if (mode !== 'main') return schemaObj;
  const cloned = cloneJson(schemaObj);
  stripMainDisabledRequired(cloned);
  return cloned;
}

async function loadJsonFile(path: string, trustedRef?: string | null): Promise<unknown> {
  const raw = await readTrustedRepoFileText(path, trustedRef);
  return JSON.parse(raw) as unknown;
}

async function loadYamlFile(path: string): Promise<unknown> {
  const raw = await readFile(path, 'utf8');
  return (YAML.parse(raw) ?? {}) as unknown;
}

function readDocType(doc: unknown): string {
  if (!isPlainObject(doc)) return '';
  return normalizeTypeToken(doc['type']);
}

function extractSchemaTypeConst(schemaObj: unknown): string {
  if (!isPlainObject(schemaObj)) return '';
  const props = schemaObj['properties'];
  if (!isPlainObject(props)) return '';
  const typeDef = props['type'];
  if (!isPlainObject(typeDef)) return '';

  const c = typeDef['const'];
  if (typeof c === 'string') return normalizeTypeToken(c);

  const en = typeDef['enum'];
  if (Array.isArray(en) && en.length === 1 && typeof en[0] === 'string') {
    return normalizeTypeToken(en[0]);
  }

  return '';
}

async function getSchemaEntry(
  schemaFsPath: string,
  ajv: AjvInstance,
  schemaCache: Map<string, SchemaCacheEntry>,
  mode: Mode = pickMode(),
  trustedRef?: string | null
): Promise<SchemaCacheEntry> {
  const refKey = String(trustedRef ?? '').trim() || 'workspace';
  const cacheKey = `${mode}:${refKey}:${schemaFsPath}`;
  const cached = schemaCache.get(cacheKey);
  if (cached) return cached;

  const schemaObjRaw = await loadJsonFile(schemaFsPath, trustedRef);
  const schemaObj = maybePatchSchemaForMain(schemaObjRaw, mode);
  const validate = ajv.compile(schemaObj);

  const entry: SchemaCacheEntry = {
    validate,
    typeConst: extractSchemaTypeConst(schemaObj),
    schemaObj,
  };

  schemaCache.set(cacheKey, entry);
  return entry;
}

function scoreErrors(errors: string[]): number {
  return errors.length > 0 ? errors.length : 9999;
}

function pickBestTry(tries: FileValidationTry[]): FileValidationTry {
  let best = tries[0];
  let bestScore = scoreErrors(best.errors);

  for (const t of tries) {
    const s = scoreErrors(t.errors);
    if (s < bestScore) {
      best = t;
      bestScore = s;
    }
  }
  return best;
}

type ValidationContextShape = {
  repo: () => RepoInfo;
  issue: () => { owner: string; repo: string; issue_number: number };
};

type BotValidationContext = ValidationContextShape & {
  octokit: OctokitLike;
  log: Console;
  resourceBotConfig: NormalizedStaticConfig;
  resourceBotHooks: RegistryBotHooks | null;
  resourceBotHooksSource: string | null;
};

async function validateOneFile(
  target: FileTarget,
  ajv: AjvInstance,
  schemaCache: Map<string, SchemaCacheEntry>,
  botValidationContext: BotValidationContext,
  repoInfo: RepoInfo,
  mode: Mode = pickMode(),
  trustedRef?: string | null
): Promise<FileValidationResult> {
  const fileFsPath = normalizeRepoPath(target.filePath);
  const doc = await loadYamlFile(fileFsPath);
  const docType = readDocType(doc);

  type LoadedCandidate =
    | { candidate: SchemaCandidate; schemaFsPath: string; entry: SchemaCacheEntry }
    | { candidate: SchemaCandidate; schemaFsPath: string; loadError: string };

  const loaded: LoadedCandidate[] = [];

  for (const c of target.candidates) {
    const schemaFsPath = normalizeRepoPath(c.schemaPath);
    try {
      const entry = await getSchemaEntry(schemaFsPath, ajv, schemaCache, mode, trustedRef);
      loaded.push({ candidate: c, schemaFsPath, entry });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      loaded.push({ candidate: c, schemaFsPath, loadError: msg });
    }
  }

  // If doc.type exists, prefer schema where properties.type.const matches it
  let ordered = loaded;

  if (docType) {
    const matching: LoadedCandidate[] = [];
    const rest: LoadedCandidate[] = [];

    for (const x of loaded) {
      if ('entry' in x && x.entry.typeConst === docType) matching.push(x);
      else rest.push(x);
    }

    if (matching.length > 0) {
      ordered = [...matching, ...rest];
    }
  }

  const tries: FileValidationTry[] = [];

  for (const x of ordered) {
    const schemaPath = x.schemaFsPath;

    if ('loadError' in x) {
      tries.push({
        requestType: x.candidate.requestType,
        schemaPath,
        ok: false,
        errors: [`schema load failed: ${x.loadError}`],
        reason: 'schema-load-failed',
      });
      continue;
    }

    const ok = Boolean(x.entry.validate(doc));

    if (ok) {
      const successTry: FileValidationTry = {
        requestType: x.candidate.requestType,
        schemaPath,
        ok: true,
        errors: [],
        reason: docType && x.entry.typeConst === docType ? 'type-match' : 'first-valid',
      };

      const extraErrors: string[] = [];

      const fileBase = path
        .basename(target.filePath)
        .replace(/\.ya?ml$/i, '')
        .trim();

      const docObj = isPlainObject(doc) ? doc : {};
      const resourceIdentifier = resolvePrimaryIdFromCandidate(docObj, x.entry.schemaObj)
        .replaceAll('\u00a0', ' ')
        .trim();

      if (resourceIdentifier) {
        const norm = (s: string): string =>
          s
            .replaceAll('\u00a0', ' ')
            .trim()
            .toLowerCase()
            .replace(/\s+/g, '')
            .replace(/[^a-z0-9._-]/g, '');

        if (norm(resourceIdentifier) !== norm(fileBase)) {
          extraErrors.push(
            `File name '${fileBase}' must match the resource identifier '${resourceIdentifier}'. Please rename the file or update the identifier field.`
          );
        }
      }

      // Parent chain check
      try {
        extraErrors.push(...(await checkFlatParentChain(target.filePath)));
      } catch {
        // ignore
      }

      // Bot hook validation parity (PR only)
      if (mode === 'pr') {
        try {
          const resourceName = resourceIdentifier || path.basename(target.filePath).replace(/\.ya?ml$/i, '');

          const hookErrors = await runCustomValidateForRegistryCandidate(botValidationContext, repoInfo, {
            requestType: x.candidate.requestType,
            schema: x.entry.schemaObj,
            candidate: isPlainObject(doc) ? doc : {},
            resourceName,
          });

          if (hookErrors.length) extraErrors.push(...hookErrors);
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e);
          extraErrors.push(`Hook onValidate failed: ${msg}`);
        }
      }

      if (extraErrors.length === 0) {
        return {
          filePath: target.filePath,
          ok: true,
          requestType: x.candidate.requestType,
          schemaPath,
          tries: [...tries, successTry],
          errors: [],
        };
      }

      return {
        filePath: target.filePath,
        ok: false,
        requestType: x.candidate.requestType,
        schemaPath,
        tries: [...tries, successTry],
        errors: extraErrors,
      };
    }

    const errors = formatAjvErrors(x.entry.validate.errors);

    tries.push({
      requestType: x.candidate.requestType,
      schemaPath,
      ok: false,
      errors,
      reason: docType && x.entry.typeConst === docType ? 'type-match-failed' : 'failed',
    });
  }

  // None matched: prefer the schema that matches doc.type, otherwise pick best by score
  const bestByScore = tries.length > 0 ? pickBestTry(tries) : null;

  const bestByType =
    docType && tries.length > 0
      ? (tries.find((t) => t.reason === 'type-match-failed') ?? tries.find((t) => t.reason === 'type-match') ?? null)
      : null;

  const selected = bestByType ?? bestByScore;

  const fallbackCandidate = target.candidates[0];
  const requestType = selected?.requestType ?? fallbackCandidate?.requestType ?? 'unknown';
  const schemaPath = selected?.schemaPath ?? normalizeRepoPath(fallbackCandidate?.schemaPath ?? 'unknown');

  return {
    filePath: target.filePath,
    ok: false,
    requestType,
    schemaPath,
    tries,
    errors: selected?.errors ?? ['No candidate schema available for this file'],
  };
}

function escapeCommandData(v: string): string {
  return String(v).replace(/%/g, '%25').replace(/\r/g, '%0D').replace(/\n/g, '%0A');
}

function escapeCommandProperty(v: string): string {
  return escapeCommandData(v).replace(/:/g, '%3A').replace(/,/g, '%2C');
}

function ghAnnotateError(file: string, message: string, title?: string): void {
  const safeFile = escapeCommandProperty(file);
  const safeTitle = title ? escapeCommandProperty(title) : '';
  const safeMsg = escapeCommandData(String(message).replace(/\r?\n/g, ' ').trim());

  const props: string[] = [`file=${safeFile}`];
  if (safeTitle) props.push(`title=${safeTitle}`);

  console.error(`::error ${props.join(',')}::${safeMsg}`);
}

type RepoInfo = { owner: string; repo: string };

type RepoContentFile = {
  type: 'file';
  name: string;
  path: string;
  content: string;
  encoding: 'base64';
};

type RepoContentResponse = RepoContentFile | RepoContentFile[];

function readRepoInfoFromEnv(): RepoInfo {
  const raw = String(process.env.GITHUB_REPOSITORY ?? '').trim();
  const [owner, repo] = raw.split('/');
  return { owner: owner || 'local', repo: repo || 'repo' };
}

type LocalGetContentArgs = { owner: string; repo: string; path: string };

function notFoundError(): Error & { status: number } {
  const e = new Error('Not Found') as Error & { status: number };
  e.status = 404;
  return e;
}

function createLocalOctokit(): OctokitLike {
  const repos = {
    getContent: async (args: LocalGetContentArgs): Promise<{ data: RepoContentResponse }> => {
      const rel = normalizeRepoPath(args.path);
      const full = path.join(process.cwd(), rel);

      try {
        const st = await stat(full);

        if (st.isDirectory()) {
          const names = await readdir(full);
          const items: RepoContentFile[] = names.map((name) => ({
            type: 'file',
            name,
            path: normalizeRepoPath(path.join(rel, name)),
            content: Buffer.from('').toString('base64'),
            encoding: 'base64',
          }));

          return { data: items };
        }

        const buf = await readFile(full);
        const file: RepoContentFile = {
          type: 'file',
          name: path.basename(rel),
          path: rel,
          content: Buffer.from(buf).toString('base64'),
          encoding: 'base64',
        };

        return { data: file };
      } catch {
        throw notFoundError();
      }
    },
  };

  const issues = {
    get(_args: { owner: string; repo: string; issue_number: number }): Promise<{ data: unknown }> {
      return Promise.resolve({ data: {} });
    },
    listForRepo(_args: {
      owner: string;
      repo: string;
      state: 'open' | 'closed' | 'all';
      per_page?: number;
    }): Promise<{ data: IssueListItem[] }> {
      return Promise.resolve({ data: [] });
    },
    update(_args: { owner: string; repo: string; issue_number: number }): Promise<unknown> {
      return Promise.resolve({});
    },
    create(_args: { owner: string; repo: string; title: string; body: string }): Promise<unknown> {
      return Promise.resolve({});
    },
    createComment(_args: { owner: string; repo: string; issue_number: number; body: string }): Promise<unknown> {
      return Promise.resolve({});
    },
    addLabels(_args: { owner: string; repo: string; issue_number: number; labels: string[] }): Promise<unknown> {
      return Promise.resolve({});
    },
    removeLabel(_args: { owner: string; repo: string; issue_number: number; name: string }): Promise<unknown> {
      return Promise.resolve({});
    },
  };

  return { repos, issues };
}

type HooksDescriptor = {
  hooks: RegistryBotHooks | null;
  hooksSource: string | null;
};

async function loadLocalHooksDescriptor(trustedRef?: string | null): Promise<HooksDescriptor> {
  const relPath = '.github/registry-bot/config.js';

  try {
    const code = await readTrustedRepoFileText(relPath, trustedRef);
    const hash = createHash('sha256').update(code).digest('hex');

    const hooks = {
      __type: 'registry-bot-hooks:esm',
      __path: relPath,
      __hash: hash,
      __code: code,
    } as unknown as RegistryBotHooks;

    return { hooks, hooksSource: `repo:${relPath}#${hash}` };
  } catch {
    return { hooks: null, hooksSource: null };
  }
}

async function checkFlatParentChain(filePath: string): Promise<string[]> {
  const fp = normalizeRepoPath(filePath);

  if (!/(^|\/)namespaces\//i.test(fp)) return [];

  const base = path.basename(fp).replace(/\.ya?ml$/i, '');
  const parts = base.split('.').filter(Boolean);

  if (parts.length < 2) return [];

  const dir = path.dirname(fp);
  const errors: string[] = [];

  for (let i = parts.length - 1; i >= 2; i -= 1) {
    const parent = parts.slice(0, i).join('.');
    const yamlPath = normalizeRepoPath(path.join(dir, `${parent}.yaml`));
    const ymlPath = normalizeRepoPath(path.join(dir, `${parent}.yml`));

    try {
      await access(yamlPath);
      continue;
    } catch {
      // try .yml
    }

    try {
      await access(ymlPath);
      continue;
    } catch {
      errors.push(`Parent resource '${parent}' is not present in flat structure. Please register the parent first.`);
    }
  }

  return errors;
}

async function main(): Promise<void> {
  const mode = pickMode();

  const isForkPr =
    mode === 'pr' &&
    String(process.env.PR_IS_FORK ?? '')
      .trim()
      .toLowerCase() === 'true';
  const trustedConfigRef = isForkPr ? requireEnv('PR_BASE_SHA') : '';

  if (isForkPr) {
    console.log(`Fork PR detected -> using trusted config/hooks/schemas from base ${trustedConfigRef}`);
  }

  const validationConfig = await loadValidationConfig(trustedConfigRef);
  const requests = validationConfig.requests;
  const repoInfo = readRepoInfoFromEnv();
  const hooksDesc = await loadLocalHooksDescriptor(trustedConfigRef);

  const botConfig = {
    requests,
    hooks: { allowedHosts: validationConfig.hooksAllowedHosts },
  } as unknown as NormalizedStaticConfig;

  const botValidationContext: BotValidationContext = {
    octokit: createLocalOctokit(),
    log: console,
    resourceBotConfig: botConfig,
    resourceBotHooks: hooksDesc.hooks,
    resourceBotHooksSource: hooksDesc.hooksSource,
    repo: (): RepoInfo => repoInfo,
    issue: (): { owner: string; repo: string; issue_number: number } => ({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      issue_number: 0,
    }),
  };

  let targets: FileTarget[] = [];

  if (mode === 'pr') {
    const baseSha = requireEnv('PR_BASE_SHA');
    const headRef = isForkPr ? 'HEAD' : requireEnv('PR_HEAD_SHA');

    const changed = await getChangedFiles(baseSha, headRef);

    targets = changed
      .filter(isYamlPath)
      .map((p) => matchRequestTypesForFile(p, requests))
      .filter((x): x is FileTarget => Boolean(x));
  } else {
    // Validate all files in all configured folders
    const uniqueFolders = Array.from(
      new Set(
        Object.values(requests)
          .map((c) => normalizeRepoPath(c.folderName))
          .filter(Boolean)
      )
    );

    const all: string[] = [];
    for (const folder of uniqueFolders) {
      const files = await getAllTrackedFilesUnder(folder);
      all.push(...files);
    }

    targets = all
      .filter(isYamlPath)
      .map((p) => matchRequestTypesForFile(p, requests))
      .filter((x): x is FileTarget => Boolean(x));
  }

  // de-dup
  const seen = new Set<string>();
  targets = targets.filter((t) => {
    const k = t.filePath;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  if (targets.length === 0) {
    console.log(`No registry files to validate in mode=${mode}`);
    return;
  }

  const ajv = buildAjv();
  const schemaCache = new Map<string, SchemaCacheEntry>();

  const results: FileValidationResult[] = [];

  for (const t of targets) {
    try {
      const r = await validateOneFile(t, ajv, schemaCache, botValidationContext, repoInfo, mode, trustedConfigRef);
      results.push(r);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      const fallback = t.candidates[0];

      results.push({
        filePath: t.filePath,
        ok: false,
        requestType: fallback?.requestType ?? 'unknown',
        schemaPath: normalizeRepoPath(fallback?.schemaPath ?? 'unknown'),
        tries: [],
        errors: [`validator crash: ${msg}`],
      });
    }
  }

  const failed = results.filter((r) => !r.ok);

  console.log(`Validated ${results.length} file(s) in mode=${mode}, failed=${failed.length}`);

  for (const f of failed) {
    const suffix = ` [file=${f.filePath} schema=${f.schemaPath} requestType=${f.requestType}]`;
    const title = `registry-validate ${f.requestType}`;

    for (const err of f.errors) {
      ghAnnotateError(f.filePath, `${err}${suffix}`, title);
    }
  }

  if (failed.length > 0) {
    throw new Error(`Registry validation failed for ${failed.length} file(s)`);
  }
}

export const TEST_UTILS = {
  normalizeRepoPath,
  extractSchemaTypeConst,
  matchRequestTypesForFile,
  validateOneFile,
  buildAjv,
};

export { main };

// Prevent auto-run when imported by Jest
if (!process.env.JEST_WORKER_ID) {
  main().catch((e: unknown) => {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(msg);
    process.exitCode = 1;
  });
}
