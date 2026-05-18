import { toStringTrim } from '../domain/login-utils.js';
import {
  buildMachineReadableMetadataBlock,
  normalizeMachineReadableIssues,
  type MachineReadableIssue,
} from '../domain/machine-readable.js';
import {
  buildRegistryValidationAggregateBody,
  buildRegistryValidationCommentHeading,
  extractFieldFromMsg,
  filterMachineReadableSourcesForFile,
  filterRegistryValidationEntries,
  normalizeMsg,
  type RegistryValidationMachineReadableSource,
} from '../domain/registry-validation-annotations.js';

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type SchemaFieldAliasLookup = Map<string, string>;

export type RequestValidationPostingCallbacks<ContextType, RepoInfoType extends RepoInfoBase> = {
  readRepoFileText: (context: ContextType, repoInfo: RepoInfoType, path: string) => Promise<string | null>;
};

const SCHEMA_FIELD_ALIAS_CACHE = new Map<string, Promise<SchemaFieldAliasLookup>>();

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

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

export async function loadSchemaFieldAliasLookup<ContextType, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repoInfo: RepoInfoType,
  schemaPath: string,
  callbacks: RequestValidationPostingCallbacks<ContextType, RepoInfoType>
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
      const raw = await callbacks.readRepoFileText(context, repoInfo, candidate);
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

export async function resolveMachineReadableRegistryField<ContextType, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repoInfo: RepoInfoType,
  fieldHint: string,
  schemaPath: string | undefined,
  callbacks: RequestValidationPostingCallbacks<ContextType, RepoInfoType>
): Promise<string> {
  const fallback = toStringTrim(fieldHint) || 'details';
  const normalizedSchemaPath = toStringTrim(schemaPath);

  if (!normalizedSchemaPath || fallback === 'details') return fallback;

  const lookup = await loadSchemaFieldAliasLookup(context, repoInfo, normalizedSchemaPath, callbacks);
  if (!lookup.size) return fallback;

  return lookup.get(normalizeSchemaFieldAlias(fallback)) || fallback;
}

export async function buildRegistryValidationMachineReadableIssues<ContextType, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repoInfo: RepoInfoType,
  items: RegistryValidationMachineReadableSource[],
  callbacks: RequestValidationPostingCallbacks<ContextType, RepoInfoType>
): Promise<MachineReadableIssue[]> {
  const out: MachineReadableIssue[] = [];

  for (const item of items || []) {
    const message = normalizeMsg(item.message);
    if (!message) continue;

    const fieldHint = extractFieldFromMsg(item.message) || 'details';
    const field = await resolveMachineReadableRegistryField(context, repoInfo, fieldHint, item.schemaPath, callbacks);
    const normalizedFilePath = toStringTrim(item.filePath);

    out.push({
      field,
      message,
      ...(normalizedFilePath ? { filePath: normalizedFilePath } : {}),
    });
  }

  return normalizeMachineReadableIssues(out);
}

export async function buildRegistryValidationPrCommentBody<ContextType, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repoInfo: RepoInfoType,
  filePath: string,
  messages: string[],
  machineReadableSources: RegistryValidationMachineReadableSource[],
  callbacks: RequestValidationPostingCallbacks<ContextType, RepoInfoType>
): Promise<string> {
  const lines: string[] = [
    '## Detected issues',
    '',
    ...buildRegistryValidationCommentHeading(filePath, messages, '###'),
  ];

  const body = lines.join('\n').trimEnd();
  const machineReadable = await buildRegistryValidationMachineReadableIssues(
    context,
    repoInfo,
    machineReadableSources,
    callbacks
  );

  return `${body}

${buildMachineReadableMetadataBlock(machineReadable)}`;
}

export async function buildRegistryValidationAggregatePrCommentBody<ContextType, RepoInfoType extends RepoInfoBase>(
  context: ContextType,
  repoInfo: RepoInfoType,
  byFile: Map<string, string[]>,
  machineReadableSources: RegistryValidationMachineReadableSource[],
  callbacks: RequestValidationPostingCallbacks<ContextType, RepoInfoType>
): Promise<string> {
  const entries = filterRegistryValidationEntries(byFile);

  if (!entries.length) return '';
  if (entries.length === 1) {
    const [filePath, messages] = entries[0];
    return await buildRegistryValidationPrCommentBody(
      context,
      repoInfo,
      filePath,
      messages,
      filterMachineReadableSourcesForFile(machineReadableSources, filePath),
      callbacks
    );
  }

  const body = buildRegistryValidationAggregateBody(byFile);

  const machineReadable = await buildRegistryValidationMachineReadableIssues(
    context,
    repoInfo,
    machineReadableSources,
    callbacks
  );

  return `${body}

${buildMachineReadableMetadataBlock(machineReadable)}`;
}
