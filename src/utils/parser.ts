type TemplateField = {
  id?: string;
  attributes?: {
    label?: string;
  };
};

const NO_RESPONSE_RE = /^_?\s*no\s*response\s*(?:provided)?\s*_?\.?$/i;

function normLabel(s: unknown): string {
  return String(s || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^\w]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function sanitizeScalar(v: unknown): string {
  const s = String(v ?? '').trim();
  if (!s) return '';
  if (NO_RESPONSE_RE.test(s)) return '';
  return s;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const INTERNAL_ALLOWED_FIELD_IDS = [
  'requestType',
  'request-type',
  'open-system',
  'system-description',
  'sub-context-description',
  'shortDescription',
  'short-description',
  'correlationIds',
  'correlation-ids',
  'correlationIdTypes',
  'correlation-id-types',
];

const SCHEMA_PARSE_COMPAT_FIELD_ALIASES: Record<string, string[]> = {
  contact: ['contacts'],
  contacts: ['contact'],
  description: ['system-description', 'sub-context-description'],
  shortDescription: ['short-description'],
  correlationIds: ['correlation-ids'],
  correlationIdTypes: ['correlation-id-types'],
};

function addAllowedFieldId(out: Set<string>, value: unknown): void {
  const id = String(value ?? '').trim();
  if (id) out.add(id);
}

function toKebabCase(value: string): string {
  return String(value ?? '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1-$2')
    .replace(/[_\s]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function toSnakeCase(value: string): string {
  return String(value ?? '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[-\s]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function addAllowedCandidate(out: Set<string>, templateFieldIds: Set<string> | null, value: unknown): void {
  const raw = String(value ?? '').trim();
  if (!raw) return;

  const variants = [raw, toKebabCase(raw), toSnakeCase(raw)];

  for (const variant of variants) {
    if (!variant) continue;
    if (templateFieldIds && !templateFieldIds.has(variant)) continue;

    out.add(variant);
  }
}

function collectAllowedFieldIdsFromSchema(
  schema: unknown,
  out: Set<string>,
  templateFieldIds: Set<string> | null
): void {
  if (!isPlainObject(schema)) return;

  const props = isPlainObject(schema['properties']) ? schema['properties'] : null;

  if (props) {
    for (const [propertyName, propertyDef] of Object.entries(props)) {
      const prop = isPlainObject(propertyDef) ? propertyDef : {};
      const mappedFieldId = String(prop['x-form-field'] ?? '').trim();

      if (mappedFieldId) {
        addAllowedCandidate(out, templateFieldIds, mappedFieldId);
      } else {
        const isConstOnly = Object.hasOwn(prop, 'const');

        // Schema constants such as "type" are generated data, not issue-form input.
        if (!isConstOnly) {
          addAllowedCandidate(out, templateFieldIds, propertyName);
        }
      }

      for (const alias of SCHEMA_PARSE_COMPAT_FIELD_ALIASES[propertyName] || []) {
        addAllowedCandidate(out, templateFieldIds, alias);
      }
    }
  }

  // Only schema composition can contribute top-level input fields.
  // Do not blindly collect from $defs, otherwise nested object properties can become fake form fields.
  for (const key of ['allOf', 'anyOf', 'oneOf'] as const) {
    const entries = schema[key];
    if (!Array.isArray(entries)) continue;

    for (const entry of entries) {
      collectAllowedFieldIdsFromSchema(entry, out, templateFieldIds);
    }
  }
}

export function buildAllowedFieldIdsFromSchema(
  schema: unknown,
  extraAllowedFieldIds: Iterable<string> | null = null,
  templateFieldIds: Iterable<string> | null = null
): string[] {
  const out = new Set<string>();

  const templateFieldIdSet = templateFieldIds
    ? new Set(
        Array.from(templateFieldIds)
          .map((id) => String(id ?? '').trim())
          .filter(Boolean)
      )
    : null;

  for (const id of INTERNAL_ALLOWED_FIELD_IDS) {
    addAllowedFieldId(out, id);
  }

  if (extraAllowedFieldIds) {
    for (const id of extraAllowedFieldIds) {
      addAllowedFieldId(out, id);
    }
  }

  collectAllowedFieldIdsFromSchema(schema, out, templateFieldIdSet);

  return Array.from(out);
}

export function filterParsedFormData(
  parsed: Record<string, string>,
  allowedFieldIds: Iterable<string> | null = null
): Record<string, string> {
  if (!allowedFieldIds) return { ...parsed };

  const allowed = new Set<string>();
  for (const id of allowedFieldIds) addAllowedFieldId(allowed, id);

  if (!allowed.size) return { ...parsed };

  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed || {})) {
    if (!allowed.has(key)) continue;
    out[key] = value;
  }

  return out;
}

function extractValue(raw: string): string {
  const m = raw.match(/```[\w-]*\r?\n([\s\S]*?)\r?\n?```/);
  let v = m ? m[1] : raw;

  v = v.replace(/^\s+|\s+$/g, '');
  if (!m) v = v.replace(/\n{3,}/g, '\n\n');

  if (!v.trim()) return '';
  if (NO_RESPONSE_RE.test(v)) return '';

  v = v
    .replace(/^\s*>\s?/gm, '')
    .replace(/^\s*[-*]\s+\[[ xX]\]\s*/gm, '')
    .replace(/^\s*[-*]\s+/gm, '');

  v = v.replace(/^\s*[:-]\s*$/gm, '').trimEnd();

  return sanitizeScalar(v);
}

function buildFieldMap(template: TemplateLike): Record<string, string> {
  const map: Record<string, string> = {};

  for (const f of template?.body || []) {
    if (!f?.id) continue;

    const lbl = f?.attributes?.label || f.id;
    const idKey = normLabel(f.id);
    const lblKey = normLabel(lbl);

    map[idKey] = f.id;
    map[lblKey] = f.id;

    if (lblKey !== idKey) {
      map[`${lblKey}-${idKey}`] = f.id;
      map[`${idKey}-${lblKey}`] = f.id;
    }
  }

  return map;
}

function getMultilineValue(lines: string[], startIdx: number): { value: string; newIndex: number } {
  const buf: string[] = [];
  let j = startIdx;
  while (j < lines.length && lines[j] && !/^\s*[^:]+:\s*/.test(lines[j])) {
    buf.push(lines[j]);
    j++;
  }
  return { value: buf.join('\n'), newIndex: j - 1 };
}

function fallbackKvScan(text: string, fieldMap: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  const keys = Object.keys(fieldMap);
  if (!keys.length) return out;

  const lines = text.split(/\r?\n/);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]?.trim();
    if (!line) continue;

    const m = /^([^:]+):\s*(.*)$/.exec(line);
    if (!m) continue;

    const keyNorm = normLabel(m[1]);
    const fieldId = fieldMap[keyNorm];
    if (!fieldId) continue;

    let value = m[2] || '';

    if (!value && i + 1 < lines.length && lines[i + 1] && !/^\s*[^:]+:\s*/.test(lines[i + 1])) {
      const { value: multiValue, newIndex } = getMultilineValue(lines, i + 1);
      i = newIndex;
      value = multiValue;
    }

    value = extractValue(value);
    if (value !== '') out[fieldId] = value;
  }

  return out;
}

type TemplateMeta = {
  parseAllowedFieldIds?: string[];
};

type TemplateLike = {
  body?: TemplateField[];
  _meta?: TemplateMeta;
};

function normalizeFieldId(value: unknown): string {
  return String(value ?? '').trim();
}

function toAllowedFieldSet(value: Iterable<string> | undefined): Set<string> {
  const out = new Set<string>();
  if (!value) return out;

  for (const item of value) {
    const id = normalizeFieldId(item);
    if (id) out.add(id);
  }

  return out;
}

function filterParsedValues(
  values: Record<string, string>,
  template: TemplateLike,
  allowedFieldIds?: Iterable<string> | null
): Record<string, string> {
  const allowed = toAllowedFieldSet(allowedFieldIds ?? template?._meta?.parseAllowedFieldIds);
  if (!allowed.size) return values;

  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(values)) {
    if (!allowed.has(key)) continue;
    out[key] = value;
  }

  return out;
}

export function parseForm(
  body: unknown,
  template: TemplateLike,
  options: { allowedFieldIds?: Iterable<string> | null } = {}
): Record<string, string> {
  const result: Record<string, string> = {};
  if (!body || !template) return result;

  const fieldMap = buildFieldMap(template);

  const text = String(body)
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/\r\n/g, '\n');

  const headingRe = /^#{2,6}[ \t]+([^\n]+?)\s*$/gm;

  const sections: { labelRaw: string; content: string }[] = [];
  const heads: { labelRaw: string; start: number; endOfLine: number }[] = [];

  let m: RegExpExecArray | null;
  while ((m = headingRe.exec(text)) !== null) {
    heads.push({ labelRaw: m[1], start: m.index, endOfLine: headingRe.lastIndex });
  }

  for (let i = 0; i < heads.length; i++) {
    const h = heads[i];
    const nextStart = i + 1 < heads.length ? heads[i + 1].start : text.length;
    const content = text.slice(h.endOfLine, nextStart);
    sections.push({ labelRaw: h.labelRaw, content });
  }

  for (const sec of sections) {
    const labelKey = normLabel(sec.labelRaw.replace(/:$/, ''));
    const fieldId = fieldMap[labelKey] || null;
    if (!fieldId) continue;

    const value = extractValue(sec.content);
    if (value === '') continue;

    result[fieldId] = value;
  }

  if (!Object.keys(result).length) {
    const kv = fallbackKvScan(text, fieldMap);
    for (const [k, v] of Object.entries(kv)) result[k] = v;
  }

  return filterParsedValues(result, template, options.allowedFieldIds);
}
