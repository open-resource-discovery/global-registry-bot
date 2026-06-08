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

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringSafe(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v).trim();
  return '';
}

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

export function normalizeFormDataForHookValidation(
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

export async function buildFormDataForHookValidationFromCandidate(
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

  const formRec = form as Record<string, unknown>;

  for (const [propName, propDef] of Object.entries(props)) {
    if (Object.hasOwn(candidate, propName)) continue;

    const ff = isPlainObject(propDef) ? propDef['x-form-field'] : null;
    if (ff) continue;

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
