import YAML from 'yaml';
import type { NormalizedStaticConfig } from '../../../config.js';

type RequestConfigEntry = Record<string, unknown>;
type RequestConfigMap = Record<string, RequestConfigEntry>;

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeRepoPath(path: unknown): string {
  return toStringTrim(path)
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/');
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

function getRequestConfigEntries(
  config: Pick<NormalizedStaticConfig, 'requests'> | null | undefined
): RequestConfigMap {
  return isPlainObject(config?.requests) ? config.requests : {};
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

export function matchRequestTypesForFile(
  config: Pick<NormalizedStaticConfig, 'requests'> | null | undefined,
  filePath: string
): string[] {
  const fp = normalizeRepoPath(filePath);
  const reqs = getRequestConfigEntries(config);
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

export function pickRequestTypeForChangedResource(
  config: Pick<NormalizedStaticConfig, 'requests'> | null | undefined,
  filePath: string,
  doc: Record<string, unknown>
): string {
  const candidates = matchRequestTypesForFile(config, filePath);
  if (candidates.length === 0) return '';
  if (candidates.length === 1) return candidates[0];

  const byDocType = mapRegistryDocTypeToRequestType(doc['type']);
  if (byDocType && candidates.includes(byDocType)) return byDocType;

  return '';
}

export function resolveRegistryDocResourceName(doc: Record<string, unknown>): string {
  const directKeys = ['identifier', 'namespace', 'product-id', 'productId', 'id', 'name', 'vendor'];

  for (const key of directKeys) {
    const value = toStringTrim(doc[key]).replaceAll('\u00a0', ' ').trim();
    if (value) return value;
  }

  return '';
}

export function buildFormDataFromRegistryDoc(doc: Record<string, unknown>): Record<string, string> {
  const out: Record<string, string> = {};

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
