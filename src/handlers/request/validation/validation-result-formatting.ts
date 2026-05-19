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

type ValidationBuckets = {
  registry: string[];
  form: string[];
  rules: string[];
  schema: string[];
};

type ValidationIssue = Readonly<{
  message: string;
  path: string;
}>;

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

type ValidateRequestIssueResult = Readonly<{
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

function getObjectProp(obj: unknown, key: string): Record<string, unknown> | null {
  if (!isPlainObject(obj)) return null;
  const value = obj[key];
  return isPlainObject(value) ? value : null;
}

function dedupe(arr: unknown): string[] {
  const a = Array.isArray(arr) ? arr : [];
  return Array.from(new Set(a.map((s) => String(s).trim()).filter(Boolean)));
}

function toMachineReadablePath(value: unknown, fallback = 'general'): string {
  const path = toStringSafe(value);
  return path || fallback;
}

function makeValidationIssue(path: unknown, message: unknown, fallbackPath = 'general'): ValidationIssue | null {
  const msg = toStringSafe(message);
  if (!msg) return null;

  return {
    path: toMachineReadablePath(path, fallbackPath),
    message: msg,
  };
}

function dedupeValidationIssues(issues: ValidationIssue[]): ValidationIssue[] {
  const out: ValidationIssue[] = [];
  const seen = new Set<string>();

  for (const issue of issues) {
    const key = `${issue.path}\u0000${issue.message}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(issue);
  }

  return out;
}

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

function buildLabelToFieldIdMap(template: TemplateLike): Map<string, string> {
  const out = new Map<string, string>();
  const fields = Array.isArray(template?.body) ? template.body : [];

  for (const field of fields) {
    const id = toStringSafe(field?.id);
    if (!id) continue;

    const label = toStringSafe(field?.attributes?.label);
    if (label) out.set(label.toLowerCase(), id);

    const humanized = humanizeKey(id);
    if (humanized) out.set(humanized.toLowerCase(), id);
  }

  return out;
}

function getTemplateFieldLabel(template: TemplateLike, fieldId: unknown): string {
  const id = toStringSafe(fieldId);
  if (!id) return '';

  const fields = Array.isArray(template?.body) ? template.body : [];
  for (const field of fields) {
    if (toStringSafe(field?.id) !== id) continue;

    return toStringSafe(field?.attributes?.label);
  }

  return '';
}

function getSchemaMappedFieldName(schemaObj: unknown, fieldId: unknown): string {
  const id = toStringSafe(fieldId);
  if (!id) return '';

  const schemaProps = getObjectProp(schemaObj, 'properties');
  if (!schemaProps) return '';

  if (Object.hasOwn(schemaProps, id)) return id;

  for (const [propName, propDef] of Object.entries(schemaProps)) {
    if (!isPlainObject(propDef)) continue;
    if (toStringSafe(propDef['x-form-field']) !== id) continue;
    return propName;
  }

  return '';
}

function resolveMachineReadableFieldName(
  template: TemplateLike,
  schemaObj: unknown,
  fieldId: unknown,
  fallback = 'details'
): string {
  const id = toStringSafe(fieldId);
  if (!id) return fallback;

  const schemaFieldName = getSchemaMappedFieldName(schemaObj, id);
  if (schemaFieldName) return schemaFieldName;

  const templateLabel = getTemplateFieldLabel(template, id);
  if (templateLabel) return templateLabel;

  return id || fallback;
}

function parseRuleValidationIssue(raw: string): ValidationIssue | null {
  const m = /^([A-Za-z0-9_.-]+)\s*:\s*(.+)$/.exec(raw);
  if (!m?.[1] || !m?.[2]) return null;

  return makeValidationIssue(m[1], m[2], 'rules');
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

  if (s.includes('identifier') || s.includes('namespace') || s.includes('product id')) {
    return primary;
  }

  if (s.includes('title') && idMap.get('title')) {
    return 'title';
  }

  return '';
}

function addGrouped(grouped: Map<string, string[]>, key: unknown, msg: unknown): void {
  const k = toStringSafe(key) || 'General';
  const m = toStringSafe(msg);
  if (!m) return;

  let items = grouped.get(k);
  if (!items) {
    items = [];
    grouped.set(k, items);
  }
  items.push(m);
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

function buildMachineReadableValidationIssues(
  buckets: ValidationBuckets,
  template: TemplateLike,
  schemaObj: unknown,
  ajvErrors: AjvErrorLike[] = []
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const labelToFieldId = buildLabelToFieldIdMap(template);
  const { idToLabel } = buildTemplateLabelMaps(template);
  const primaryFieldId = guessPrimaryFieldId(template) || 'identifier';
  const primaryLabel = resolveMachineReadableFieldName(template, schemaObj, primaryFieldId, 'details');
  const ajvMessages = new Set<string>();

  for (const err of Array.isArray(ajvErrors) ? ajvErrors : []) {
    const msg = normalizeAjvMessage(err?.message);
    if (!msg) continue;

    ajvMessages.add(msg);

    const fieldIds = fieldIdsFromAjvError(err);
    if (fieldIds.length) {
      for (const fieldId of fieldIds) {
        const issue = makeValidationIssue(
          resolveMachineReadableFieldName(template, schemaObj, fieldId, 'details'),
          msg,
          'details'
        );
        if (issue) issues.push(issue);
      }
      continue;
    }

    const instancePath = toStringSafe(err?.instancePath).replace(/^\/+/, '');
    const instanceField = instancePath.split('/').find(Boolean) || '';
    const issue = makeValidationIssue(
      resolveMachineReadableFieldName(template, schemaObj, instanceField, 'details'),
      msg,
      'details'
    );
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.form || [])) {
    const requiredMatch = /^Required field is missing in form:\s*(.+)$/i.exec(raw);
    if (requiredMatch?.[1]) {
      const label = toStringSafe(requiredMatch[1]).toLowerCase();
      const fieldName = resolveMachineReadableFieldName(
        template,
        schemaObj,
        labelToFieldId.get(label) || '',
        requiredMatch[1]
      );
      const issue = makeValidationIssue(fieldName, 'Required field is missing.', requiredMatch[1]);
      if (issue) issues.push(issue);
      continue;
    }

    const issue = makeValidationIssue('details', raw, 'details');
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.rules || [])) {
    const structured = parseRuleValidationIssue(raw);
    if (structured) {
      issues.push({
        ...structured,
        path: resolveMachineReadableFieldName(template, schemaObj, structured.path, structured.path || 'details'),
      });
      continue;
    }

    const inferredFieldId = inferFieldLabelFromRuleMsg(raw, primaryFieldId, idToLabel);
    const inferredFieldName = inferredFieldId
      ? resolveMachineReadableFieldName(template, schemaObj, inferredFieldId, inferredFieldId)
      : 'details';

    const issue = makeValidationIssue(inferredFieldName, raw, inferredFieldName);
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.registry || [])) {
    const issue = makeValidationIssue(primaryLabel, raw, primaryLabel);
    if (issue) issues.push(issue);
  }

  for (const raw of dedupe(buckets.schema || [])) {
    const normalized = normalizeAjvMessage(raw);
    if (ajvMessages.has(normalized)) continue;

    const issue = makeValidationIssue('details', raw, 'details');
    if (issue) issues.push(issue);
  }

  return dedupeValidationIssues(issues);
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
  const ordered = orderGroupedKeys(labelOrder, grouped);
  return ordered
    .map((k) => {
      const lines = dedupe(grouped.get(k) || []);
      return `### ${k}\n- ${lines.join('\n- ')}`;
    })
    .join('\n\n');
}

export function buildValidateRequestIssueResult(
  errors: string[],
  buckets: ValidationBuckets,
  template: TemplateLike,
  options: {
    schemaObj: unknown;
    ajvErrorsForUnifiedFormat: AjvErrorLike[];
    formData: FormData;
    namespace: string;
    nsType: string;
  }
): ValidateRequestIssueResult {
  const unified = formatUnifiedIssues(buckets, template, options.ajvErrorsForUnifiedFormat);
  const validationIssues = buildMachineReadableValidationIssues(
    buckets,
    template,
    options.schemaObj,
    options.ajvErrorsForUnifiedFormat
  );

  return {
    errors,
    errorsGrouped: buckets,
    errorsFormatted: unified || formatBuckets(buckets),
    errorsFormattedSingle: unified || formatFirstBucket(buckets),
    validationIssues,
    formData: options.formData,
    template,
    namespace: options.namespace,
    nsType: options.nsType,
  };
}

export function buildMissingTemplateResult(msg: unknown): ValidateRequestIssueResult {
  const m = msg
    ? `Configuration error: Missing form template (${toStringSafe(msg)})`
    : 'Configuration error: Missing form template';

  const buckets: ValidationBuckets = { registry: [], form: [], rules: [], schema: [] };
  buckets.form.push(m);

  return {
    errors: [m],
    errorsGrouped: buckets,
    errorsFormatted: `### Form\n- ${m}`,
    errorsFormattedSingle: `### Form\n- ${m}`,
    validationIssues: [{ path: 'template', message: m }],
    formData: {},
    template: null,
    namespace: '',
    nsType: '',
  };
}
