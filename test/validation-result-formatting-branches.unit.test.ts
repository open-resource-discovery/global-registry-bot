/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/consistent-type-assertions */
/**
 * Branch-coverage tests targeting src/handlers/request/validation/validation-result-formatting.ts
 * (the existing validation-result-formatting.unit.test.ts tests run.ts, not this file)
 */
import { describe, it, expect } from '@jest/globals';
import {
  buildValidateRequestIssueResult,
  buildMissingTemplateResult,
} from '../src/handlers/request/validation/validation-result-formatting.js';

// ---------------------------------------------------------------------------
// Type helpers (mirroring the source types without importing them)
// ---------------------------------------------------------------------------
type ValidationBuckets = { registry: string[]; form: string[]; rules: string[]; schema: string[] };
type AjvError = {
  keyword?: string;
  instancePath?: string;
  schemaPath?: string;
  message?: string;
  params?: Record<string, unknown>;
};
type TemplateField = {
  id?: string;
  attributes?: { label?: string };
  validations?: { required?: boolean };
  [k: string]: unknown;
};
type Template = {
  body?: TemplateField[];
  _meta?: Record<string, unknown>;
  [k: string]: unknown;
};

const emptyBuckets = (): ValidationBuckets => ({ registry: [], form: [], rules: [], schema: [] });

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function baseOpts(overrides: Record<string, unknown> = {}) {
  return {
    schemaObj: null as unknown,
    ajvErrorsForUnifiedFormat: [] as AjvError[],
    formData: {} as Record<string, string>,
    namespace: 'test.ns',
    nsType: 'dataProduct',
    ...overrides,
  };
}

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function invoke(
  template: Template,
  buckets: ValidationBuckets = emptyBuckets(),
  optOverrides: Record<string, unknown> = {},
  errors: string[] = []
) {
  return buildValidateRequestIssueResult(errors, buckets, template, baseOpts(optOverrides) as any);
}

// ---------------------------------------------------------------------------
// buildMissingTemplateResult — L628 cond-expr + toStringSafe branches
// ---------------------------------------------------------------------------
describe('buildMissingTemplateResult', () => {
  it('uses generic message when msg is null (L628 arm 1 - no msg)', () => {
    const result = buildMissingTemplateResult(null);
    expect(result.errors[0]).toBe('Configuration error: Missing form template');
    expect(result.template).toBeNull();
    expect(result.namespace).toBe('');
  });

  it('uses generic message when msg is undefined', () => {
    const result = buildMissingTemplateResult(undefined);
    expect(result.errors[0]).toBe('Configuration error: Missing form template');
  });

  it('embeds string msg (L628 arm 0 - has msg; toStringSafe L68 arm 1 + L69 arm 0)', () => {
    const result = buildMissingTemplateResult('bad-template-path');
    expect(result.errors[0]).toBe('Configuration error: Missing form template (bad-template-path)');
  });

  it('embeds number msg (toStringSafe L69 arm 1 not-string, binary-expr arm 0 number)', () => {
    const result = buildMissingTemplateResult(42 as unknown);
    expect(result.errors[0]).toContain('(42)');
  });

  it('embeds boolean msg (toStringSafe L69 binary-expr arm 1 boolean)', () => {
    const result = buildMissingTemplateResult(true as unknown);
    expect(result.errors[0]).toContain('(true)');
  });

  it('embeds object msg as empty string (toStringSafe else arm — returns empty)', () => {
    const result = buildMissingTemplateResult({} as unknown);
    expect(result.errors[0]).toBe('Configuration error: Missing form template ()');
  });

  it('returns standard shaped result', () => {
    const result = buildMissingTemplateResult('x');
    expect(result.validationIssues).toHaveLength(1);
    expect(result.validationIssues[0].path).toBe('template');
    expect(result.formData).toEqual({});
    expect(result.nsType).toBe('');
  });
});

// ---------------------------------------------------------------------------
// buildValidateRequestIssueResult — empty / minimal inputs
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — empty inputs', () => {
  it('handles empty template without body (L130, L145 false arms — no array body)', () => {
    const result = invoke({});
    expect(result.errors).toEqual([]);
    expect(result.validationIssues).toEqual([]);
  });

  it('handles template with non-array body (L130 false arm)', () => {
    const result = invoke({ body: 'not-an-array' as any });
    expect(result.validationIssues).toEqual([]);
  });

  it('falls back to formatBuckets when unified is empty (L617 binary-expr arm 1)', () => {
    const result = invoke({}, emptyBuckets());
    expect(result.errorsFormatted).toBe('');
    expect(result.errorsFormattedSingle).toBe('');
  });

  it('returns namespace and nsType from options', () => {
    const result = invoke({}, emptyBuckets(), { namespace: 'org.unit', nsType: 'service' });
    expect(result.namespace).toBe('org.unit');
    expect(result.nsType).toBe('service');
  });

  it('passes formData through unchanged', () => {
    const fd = { field1: 'val1', field2: 'val2' };
    const result = invoke({}, emptyBuckets(), { formData: fd });
    expect(result.formData).toBe(fd);
  });
});

// ---------------------------------------------------------------------------
// Template body field shapes
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — template body field shapes', () => {
  it('handles body field without id (L132 binary-expr, L133/L149 if !id → continue)', () => {
    const template: Template = { body: [{ attributes: { label: 'No Id Field' } }] };
    const result = invoke(template);
    expect(result.validationIssues).toEqual([]);
  });

  it('handles body field with empty-string id (treated as missing)', () => {
    const template: Template = { body: [{ id: '', attributes: { label: 'Empty Id' } }] };
    const result = invoke(template);
    expect(result.validationIssues).toEqual([]);
  });

  it('handles field with id but without label — humanizeKey fallback (L135)', () => {
    const template: Template = { body: [{ id: 'myFieldId', validations: { required: true } }] };
    const result = invoke(template, { ...emptyBuckets(), form: ['myFieldId: some error'] });
    expect(result.errorsFormatted).toContain('My Field Id');
  });

  it('guessPrimaryFieldId returns identifier when present in body (L289 true)', () => {
    const template: Template = {
      body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['already exists'] });
    expect(result.errorsFormatted).toContain('Identifier');
  });

  it('guessPrimaryFieldId falls back to namespace (L291 true)', () => {
    const template: Template = {
      body: [{ id: 'namespace', attributes: { label: 'Namespace' } }],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['duplicate ns'] });
    expect(result.errorsFormatted).toContain('Namespace');
  });

  it('guessPrimaryFieldId falls back to product-id (L292 true)', () => {
    const template: Template = {
      body: [{ id: 'product-id', attributes: { label: 'Product ID' } }],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['conflict'] });
    expect(result.errorsFormatted).toContain('Product ID');
  });

  it('guessPrimaryFieldId falls back to productId (L295 true)', () => {
    const template: Template = {
      body: [{ id: 'productId', attributes: { label: 'Product Identifier' } }],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['conflict'] });
    expect(result.errorsFormatted).toContain('Product Identifier');
  });

  it('guessPrimaryFieldId: uses required field whose id looks like identifier (L296-309)', () => {
    const template: Template = {
      body: [
        { id: 'title', attributes: { label: 'Title' }, validations: { required: true } },
        {
          id: 'product-identifier',
          attributes: { label: 'Product Identifier' },
          validations: { required: true },
        },
      ],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['conflict'] });
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('guessPrimaryFieldId: returns first field id when no id-like field (L312 arm 0)', () => {
    const template: Template = {
      body: [
        { id: 'title', attributes: { label: 'Title' } },
        { id: 'description', attributes: { label: 'Description' } },
      ],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['msg'] });
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('guessPrimaryFieldId returns empty string for empty body (L312 arm 1)', () => {
    const template: Template = { body: [] };
    const result = invoke(template, { ...emptyBuckets(), registry: ['msg'] });
    expect(result.errorsFormatted).toContain('msg');
  });

  it('required field with id NOT containing id/identifier/namespace → loop continues (L309 false)', () => {
    const template: Template = {
      body: [{ id: 'title', attributes: { label: 'Title' }, validations: { required: true } }],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['conflict'] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });

  it('required field with required: false → skipped (L297 if required !== true → continue)', () => {
    const template: Template = {
      body: [
        {
          id: 'some-identifier',
          attributes: { label: 'Some Identifier' },
          validations: { required: false },
        },
      ],
    };
    const result = invoke(template, { ...emptyBuckets(), registry: ['msg'] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// Bucket processing — form
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — form bucket processing', () => {
  const template: Template = {
    body: [
      { id: 'namespace', attributes: { label: 'Namespace' } },
      { id: 'title', attributes: { label: 'Title' } },
    ],
  };

  it('processes Required field is missing pattern (L470 true arm)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Namespace'],
    };
    const result = invoke(template, buckets);
    expect(result.validationIssues.some((i) => i.message.includes('Required'))).toBe(true);
  });

  it('processes form item matching fieldId: message pattern (L382-390)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['namespace: must be a valid ORD ID'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('Namespace');
    expect(result.errorsFormatted).toContain('must be a valid ORD ID');
  });

  it('falls through to General when form item has unknown fieldId (kind=form, no rules infer)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['unknown-field: some error'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('General');
  });

  it('deduplicates validation issues with same path+message (L105 seen.has → continue)', () => {
    // Two identical AJV errors produce two ValidationIssues with the same path+message.
    // dedupeValidationIssues keeps only one.
    const err: AjvError = {
      keyword: 'required',
      message: 'namespace is missing',
      params: { missingProperty: 'namespace' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err, { ...err }] });
    // normalizeAjvMessage('namespace is missing') → 'Namespace is missing' (only first char upped)
    const dupes = result.validationIssues.filter((i) => i.path === 'Namespace' && i.message === 'Namespace is missing');
    expect(dupes.length).toBe(1);
  });

  it('Required missing in form + ajv already has it → suppressed duplicate (L374)', () => {
    const ajvError: AjvError = {
      keyword: 'required',
      message: 'namespace is required',
      params: { missingProperty: 'namespace' },
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Namespace'],
    };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [ajvError] });
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('handles extractFieldLabelFromFormMsg matching regex (L281 true arm)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Title'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('Title');
  });
});

// ---------------------------------------------------------------------------
// Bucket processing — rules
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — rules bucket processing', () => {
  const template: Template = {
    body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
  };

  it('processes structured rule item (parseRuleValidationIssue match, L213 true)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      rules: ['identifier: must be unique across registry'],
    };
    const result = invoke(template, buckets);
    const issue = result.validationIssues.find((i) => i.message.includes('must be unique'));
    expect(issue).toBeTruthy();
  });

  it('infers field from rule containing identifier text (inferFieldLabelFromRuleMsg L318)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      rules: ['This identifier has already been registered'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('infers field from rule containing namespace text (L318 namespace match)', () => {
    const templateNs: Template = {
      body: [{ id: 'namespace', attributes: { label: 'Namespace' } }],
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      rules: ['The namespace must be registered first'],
    };
    const result = invoke(templateNs, buckets);
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('infers title field from rule (inferFieldLabelFromRuleMsg L322 true)', () => {
    const templateT: Template = {
      body: [
        { id: 'identifier', attributes: { label: 'Identifier' } },
        { id: 'title', attributes: { label: 'Title' } },
      ],
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      rules: ['The title must not be empty'],
    };
    const result = invoke(templateT, buckets);
    expect(result.errorsFormatted).toContain('title');
  });

  it('rule with no matching field falls to General (L402)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      rules: ['Some generic rule violation'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('General');
  });
});

// ---------------------------------------------------------------------------
// Bucket processing — registry and schema + formatBuckets/formatFirstBucket
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — registry/schema/formatBuckets', () => {
  const template: Template = {
    body: [{ id: 'namespace', attributes: { label: 'Namespace' } }],
  };

  it('routes registry messages to primary label group (L506-508)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      registry: ['Package already registered'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('Namespace');
    expect(result.errorsFormatted).toContain('Package already registered');
  });

  it('routes schema messages to General group (L511)', () => {
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      schema: ['must have required property "title"'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('General');
  });

  it('skips schema msg that duplicates normalised AJV message (L516 if ajvMessages.has → continue)', () => {
    const ajvError: AjvError = {
      keyword: 'required',
      message: 'must have required property "title"',
      params: { missingProperty: 'title' },
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      schema: ['Must Have Required Property "Title"'],
    };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [ajvError] });
    expect(result).toBeTruthy();
  });

  it('formatBuckets all 4 sections true (L524/526/527/529): non-array values bypass unified', () => {
    // When bucket values are non-arrays, dedupe() returns [] so unified=''.
    // formatBuckets then checks .length on the raw value (string.length > 0) → each if-arm fires.
    const buckets = {
      registry: 'r1' as any as string[],
      form: 'f1' as any as string[],
      rules: 'rl1' as any as string[],
      schema: 's1' as any as string[],
    };
    const result = invoke({}, buckets);
    expect(result.errorsFormatted).toContain('## Registry');
    expect(result.errorsFormatted).toContain('## Form');
    expect(result.errorsFormatted).toContain('## Rules');
    expect(result.errorsFormatted).toContain('## Schema');
  });

  it('formatBuckets only registry (L524 true, others false) — non-array forces unified=empty', () => {
    const buckets = {
      registry: 'registry-error' as any as string[],
      form: [] as string[],
      rules: [] as string[],
      schema: [] as string[],
    };
    const result = invoke({}, buckets);
    expect(result.errorsFormatted).toContain('## Registry');
    expect(result.errorsFormatted).not.toContain('## Form');
  });

  it('formatBuckets only schema (L529 true) — non-array forces unified=empty', () => {
    const buckets = {
      registry: [] as string[],
      form: [] as string[],
      rules: [] as string[],
      schema: 'schema-error' as any as string[],
    };
    const result = invoke({}, buckets);
    expect(result.errorsFormatted).toContain('## Schema');
  });

  it('dedupe ignores non-array bucket value (L80 arm 1 — Array.isArray false → returns [])', () => {
    // Non-array form value: dedupe returns [], no items in grouped, unified='',
    // then formatBuckets uses string.length to detect non-empty bucket.
    const buckets = {
      ...emptyBuckets(),
      form: 'not-an-array' as any as string[],
    };
    const result = invoke({}, buckets);
    expect(result.errorsFormatted).toContain('## Form');
  });

  it('formatFirstBucket returns empty string when all buckets empty (L554 return empty)', () => {
    const result = invoke({}, emptyBuckets());
    expect(result.errorsFormattedSingle).toBe('');
  });

  it('errorsFormattedSingle uses unified output (### format) when buckets are properly filled', () => {
    const buckets: ValidationBuckets = { ...emptyBuckets(), form: ['form-error'] };
    const result = invoke({}, buckets);
    expect(result.errorsFormattedSingle).toContain('### General');
    expect(result.errorsFormattedSingle).toContain('form-error');
  });
});

// ---------------------------------------------------------------------------
// AJV error processing
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — AJV error processing', () => {
  const template: Template = {
    body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
  };

  it('handles non-array ajvErrors gracefully (L347 early return from groupAjvErrors)', () => {
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: null as any });
    expect(result.validationIssues).toEqual([]);
  });

  it('handles non-plain-object AJV error (L239/L262 !isPlainObject → return empty/[])', () => {
    const result = invoke(template, emptyBuckets(), {
      ajvErrorsForUnifiedFormat: ['not-an-object' as any, 42 as any],
    });
    expect(result.validationIssues).toEqual([]);
  });

  it('handles AJV error with empty message (L220/L441 empty msg → skip)', () => {
    const err: AjvError = { keyword: 'required', message: '', params: { missingProperty: 'id' } };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues).toEqual([]);
  });

  it('processes keyword=required with string missingProperty (L242/L244 true → return mp)', () => {
    const err: AjvError = {
      keyword: 'required',
      message: 'must have required property',
      params: { missingProperty: 'identifier' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    // path resolves via getTemplateFieldLabel → label 'Identifier' from template
    const issue = result.validationIssues.find((i) => i.path === 'Identifier');
    expect(issue).toBeTruthy();
  });

  it('processes keyword=required with non-string missingProperty (L244 false → continues)', () => {
    const err: AjvError = {
      keyword: 'required',
      message: 'must have required property',
      params: { missingProperty: 42 as any },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });

  it('processes keyword=additionalProperties with string additionalProperty (L247/L249 true)', () => {
    const err: AjvError = {
      keyword: 'additionalProperties',
      message: 'must NOT have additional properties',
      params: { additionalProperty: 'unknownField' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    const issue = result.validationIssues.find((i) => i.path === 'unknownField');
    expect(issue).toBeTruthy();
  });

  it('processes keyword=additionalProperties with non-string additionalProperty (L249 false)', () => {
    const err: AjvError = {
      keyword: 'additionalProperties',
      message: 'must NOT have additional properties',
      params: { additionalProperty: 99 as any },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });

  it('processes instancePath starting with / (L252/L253/L255 true)', () => {
    const err: AjvError = {
      keyword: 'type',
      instancePath: '/identifier',
      message: 'must be string',
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    const issue = result.validationIssues.find((i) => i.message.includes('string'));
    expect(issue).toBeTruthy();
  });

  it('processes instancePath not starting with / (L253 false, uses path as-is)', () => {
    const err: AjvError = {
      keyword: 'type',
      instancePath: 'identifier',
      message: 'must be string',
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });

  it('processes keyword=errorMessage with params.errors array (L265 true)', () => {
    const inner: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'identifier' },
    };
    const err = {
      keyword: 'errorMessage',
      message: 'Custom error: identifier is required',
      params: { errors: [inner] },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    // path resolves to template label 'Identifier'
    const issue = result.validationIssues.find((i) => i.path === 'Identifier');
    expect(issue).toBeTruthy();
  });

  it('processes keyword=errorMessage without params.errors (L265 false → single fieldId)', () => {
    const err = {
      keyword: 'errorMessage',
      instancePath: '/identifier',
      message: 'Custom validation failed',
      params: {},
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });

  it('errorMessage inner error with no extractable fieldId (L269 if id → false, skip push)', () => {
    const inner: AjvError = { keyword: 'type', message: 'must be string', params: {} };
    const err = {
      keyword: 'errorMessage',
      message: 'Custom error',
      params: { errors: [inner] },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });

  it('normalizeAjvMessage uppercases rfc2119 keywords (must → MUST, should → SHOULD)', () => {
    const err: AjvError = {
      keyword: 'required',
      message: 'must be a valid string that should follow the naming convention',
      params: { missingProperty: 'identifier' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    // path resolves to template label 'Identifier'
    const issue = result.validationIssues.find((i) => i.path === 'Identifier');
    expect(issue?.message).toContain('MUST');
    expect(issue?.message).toContain('SHOULD');
  });

  it('deduplicates AJV validation issues with same path+message (L105 seen.has → continue)', () => {
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'identifier' },
    };
    const result = invoke(template, emptyBuckets(), {
      ajvErrorsForUnifiedFormat: [err, { ...err }],
    });
    // path resolves to label 'Identifier'; normalizeAjvMessage('is required') → 'Is REQUIRED'
    const dupes = result.validationIssues.filter((i) => i.path === 'Identifier' && i.message === 'Is REQUIRED');
    expect(dupes.length).toBe(1);
  });

  it('processes instancePath "/" with only root separator (L255 if first is empty → return empty)', () => {
    const err: AjvError = {
      keyword: 'type',
      instancePath: '/',
      message: 'invalid value',
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// getSchemaMappedFieldName / schemaObj field resolution
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — schema object field resolution', () => {
  it('uses schemaObj.properties to resolve fieldId directly (L182 Object.hasOwn true)', () => {
    const schemaObj = {
      properties: { identifier: { type: 'string' }, title: { type: 'string' } },
    };
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'identifier' },
    };
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'ID' } }] };
    const result = invoke(template, emptyBuckets(), { schemaObj, ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.some((i) => i.path === 'identifier')).toBe(true);
  });

  it('uses x-form-field to map schema property to form field (L184-188)', () => {
    const schemaObj = {
      properties: { productId: { 'type': 'string', 'x-form-field': 'identifier' } },
    };
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'identifier' },
    };
    const template: Template = {
      body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
    };
    const result = invoke(template, emptyBuckets(), { schemaObj, ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });

  it('handles schemaObj with non-plain-object properties value (L180 false → getSchemaMapped returns empty, L76 arm 1)', () => {
    const schemaObj = { properties: 'not-an-object' };
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'identifier' },
    };
    const template: Template = {
      body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
    };
    const result = invoke(template, emptyBuckets(), { schemaObj, ajvErrorsForUnifiedFormat: [err] });
    // getSchemaMapped returns '' → falls back to template label 'Identifier'
    expect(result.validationIssues.some((i) => i.path === 'Identifier')).toBe(true);
  });

  it('handles propDef that is not plain object (L185 !isPlainObject → continue)', () => {
    const schemaObj = { properties: { myField: 'not-a-plain-object' } };
    const err: AjvError = {
      keyword: 'required',
      message: 'identifier is required',
      params: { missingProperty: 'identifier' },
    };
    const template: Template = {
      body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
    };
    const result = invoke(template, emptyBuckets(), { schemaObj, ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });

  it('handles propDef with x-form-field that does not match (L187 false → continue → return empty)', () => {
    const schemaObj = {
      properties: { productId: { 'type': 'string', 'x-form-field': 'otherId' } },
    };
    const err: AjvError = {
      keyword: 'required',
      message: 'identifier is required',
      params: { missingProperty: 'identifier' },
    };
    const template: Template = {
      body: [{ id: 'identifier', attributes: { label: 'Identifier' } }],
    };
    const result = invoke(template, emptyBuckets(), { schemaObj, ajvErrorsForUnifiedFormat: [err] });
    expect(result).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// resolveMachineReadableFieldName variations
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — resolveMachineReadableFieldName', () => {
  it('returns template field label when schema has no matching property (L204-206)', () => {
    const template: Template = {
      body: [{ id: 'myId', attributes: { label: 'My Label' } }],
    };
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'myId' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    const issue = result.validationIssues.find((i) => i.path === 'My Label');
    expect(issue).toBeTruthy();
  });

  it('returns fieldId when no schema/template label resolves it (L208 binary-expr id || fallback)', () => {
    const template: Template = { body: [{ id: 'customField', attributes: { label: '' } }] };
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: 'customField' },
    };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    const issue = result.validationIssues.find((i) => i.path === 'customField');
    expect(issue).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// humanizeKey edge cases
// ---------------------------------------------------------------------------
describe('humanizeKey via template field processing', () => {
  it('humanizes camelCase key from template field id (L116-119 replaceAll camel split)', () => {
    const template: Template = {
      body: [{ id: 'myProductId', validations: { required: true } }],
    };
    const result = invoke(template, { ...emptyBuckets(), form: ['myProductId: error'] });
    expect(result.errorsFormatted).toContain('My Product Id');
  });

  it('humanizes kebab-case key (replaceAll dashes → spaces, capitalises first letter only)', () => {
    const template: Template = { body: [{ id: 'my-field-id' }] };
    const result = invoke(template, { ...emptyBuckets(), form: ['my-field-id: validation error'] });
    // humanizeKey only capitalises the very first character → 'My field id'
    expect(result.errorsFormatted).toContain('My field id');
  });
});

// ---------------------------------------------------------------------------
// makeValidationIssue — L91 (empty message → return null)
// ---------------------------------------------------------------------------
describe('makeValidationIssue empty message suppression', () => {
  it('produces no issues when form item is whitespace only (L91 true → null)', () => {
    const buckets: ValidationBuckets = { ...emptyBuckets(), form: ['   '] };
    const result = invoke({ body: [] }, buckets);
    expect(result.validationIssues).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// orderGroupedKeys — label ordering
// ---------------------------------------------------------------------------
describe('orderGroupedKeys — label order from template body', () => {
  it('orders grouped keys by label order then extra keys then General', () => {
    const template: Template = {
      body: [
        { id: 'namespace', attributes: { label: 'Namespace' } },
        { id: 'title', attributes: { label: 'Title' } },
      ],
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['title: title error', 'namespace: namespace error'],
      rules: ['Some generic rule'],
    };
    const result = invoke(template, buckets);
    const formatted = result.errorsFormatted;
    const nsIdx = formatted.indexOf('### Namespace');
    const titleIdx = formatted.indexOf('### Title');
    const genIdx = formatted.indexOf('### General');
    expect(nsIdx).toBeLessThan(titleIdx);
    expect(titleIdx).toBeLessThan(genIdx);
  });
});

// ---------------------------------------------------------------------------
// humanizeKey — dash/underscore-only id → empty spaced → 'General' (L120 arm=1)
// ---------------------------------------------------------------------------
describe('humanizeKey dash-only field id → General', () => {
  it('AJV missingProperty "-" causes humanizeKey to return General (L120 cond-expr false)', () => {
    const err: AjvError = {
      keyword: 'required',
      message: 'is required',
      params: { missingProperty: '-' },
    };
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'Identifier' } }] };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
    expect(result.errorsFormatted).toContain('General');
  });
});

// ---------------------------------------------------------------------------
// fieldIdFromAjvError — non-plain-object inner error (L239 arm=0)
// ---------------------------------------------------------------------------
describe('fieldIdFromAjvError non-plain-object inner', () => {
  it('errorMessage AJV error with string inner → fieldIdFromAjvError returns empty (L239 arm=0)', () => {
    const err = {
      keyword: 'errorMessage',
      message: 'validation failed',
      params: { errors: ['not-an-object', 42] },
    };
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'Identifier' } }] };
    const result = invoke(template, emptyBuckets(), { ajvErrorsForUnifiedFormat: [err as any] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// processBucketItem — L372 arm=1 (stripped ≠ fieldLabel due to trailing space)
//                   — L374 arm=1 (AJV message already in ajvMsgLcSet → suppress)
// ---------------------------------------------------------------------------
describe('processBucketItem edge cases', () => {
  it('form message with trailing spaces: stripped ≠ fieldLabel → addGrouped directly (L372 arm=1)', () => {
    const template: Template = { body: [{ id: 'namespace', attributes: { label: 'Namespace' } }] };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Namespace  '],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('Namespace');
  });

  it('AJV error with period in message suppresses duplicate required form item (L374 arm=1)', () => {
    const ajvError: AjvError = {
      keyword: 'required',
      message: 'Namespace is required.',
      params: { missingProperty: 'namespace' },
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Namespace'],
    };
    const template: Template = { body: [{ id: 'namespace', attributes: { label: 'Namespace' } }] };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [ajvError] });
    expect(result.errorsFormatted).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// labelToFieldId.get miss — L475 arm=1 (label not in map → || '')
// ---------------------------------------------------------------------------
describe('buildMachineReadableValidationIssues — required field unknown label', () => {
  it('Required form item with label not in template → labelToFieldId.get returns undefined (L475 arm=1)', () => {
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'Identifier' } }] };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: UnknownLabel'],
    };
    const result = invoke(template, buckets);
    expect(result.validationIssues.some((i) => i.message === 'Required field is missing.')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Partial buckets — missing form/rules/schema → || [] fallbacks
// (L413/L468/L487/L511/L579 arm=1)
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — partial buckets (missing properties)', () => {
  it('bucket with only registry: missing form/rules/schema hit || [] fallbacks (L413/L468/L487/L511/L579 arm=1)', () => {
    const partialBuckets = { registry: ['registry error'] } as unknown as ValidationBuckets;
    const result = invoke({} as Template, partialBuckets);
    expect(result.errorsFormatted).toBeTruthy();
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(1);
  });

  it('bucket with only form: missing registry/schema hit || [] fallbacks (L506/L578 arm=1)', () => {
    const partialBuckets = { form: ['form error'] } as unknown as ValidationBuckets;
    const result = invoke({} as Template, partialBuckets);
    expect(result.errorsFormatted).toContain('General');
  });
});

// ---------------------------------------------------------------------------
// formatUnifiedIssues: schema msg already-normalized matches ajvMsgSet (L582 arm=0)
// buildMachineReadableValidationIssues: schema msg normalizes to ajvMessages entry (L513 arm=0)
// ---------------------------------------------------------------------------
describe('schema bucket deduplication against AJV messages', () => {
  it('schema item in normalized form matches ajvMsgSet → suppressed in formatUnifiedIssues (L582 arm=0)', () => {
    const ajvError: AjvError = {
      keyword: 'required',
      message: 'must be a valid ORD ID',
      params: { missingProperty: 'identifier' },
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      schema: ['MUST be a valid ORD ID'],
    };
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'Identifier' } }] };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [ajvError] });
    expect(result.errorsFormatted).toBeTruthy();
  });

  it('schema item normalizes to same string as AJV msg → skipped in buildMachineReadable (L513 arm=0)', () => {
    const ajvError: AjvError = {
      keyword: 'required',
      message: 'must have required property',
      params: { missingProperty: 'identifier' },
    };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      schema: ['must have required property'],
    };
    const template: Template = { body: [{ id: 'identifier', attributes: { label: 'Identifier' } }] };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [ajvError] });
    const schemaOnlyIssues = result.validationIssues.filter(
      (i) => i.path === 'details' && i.message.includes('REQUIRED')
    );
    expect(schemaOnlyIssues.length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// L372 arm1: stripped !== fieldLabel (form item has embedded newline)
// ---------------------------------------------------------------------------
describe('processBucketItem L372 arm1: stripped differs from fieldLabel', () => {
  it('form item with newline after label → stripped ≠ fieldLabel → addGrouped directly', () => {
    const template: Template = { body: [{ id: 'namespace', attributes: { label: 'Namespace' } }] };
    const buckets: ValidationBuckets = {
      ...emptyBuckets(),
      form: ['Required field is missing in form: Namespace\nExtra continuation text'],
    };
    const result = invoke(template, buckets);
    expect(result.errorsFormatted).toContain('Namespace');
  });
});

// ---------------------------------------------------------------------------
// Full round-trip
// ---------------------------------------------------------------------------
describe('buildValidateRequestIssueResult — round-trip', () => {
  it('returns errors array unchanged', () => {
    const errs = ['err1', 'err2'];
    const result = buildValidateRequestIssueResult(errs, emptyBuckets(), {}, baseOpts() as any);
    expect(result.errors).toEqual(errs);
    expect(result.errorsGrouped).toEqual(emptyBuckets());
    expect(result.template).toEqual({});
  });

  it('returns combined machine-readable issues from multiple buckets', () => {
    const template: Template = {
      body: [
        { id: 'namespace', attributes: { label: 'Namespace' } },
        { id: 'title', attributes: { label: 'Title' } },
      ],
    };
    const err: AjvError = {
      keyword: 'required',
      message: 'must be present',
      params: { missingProperty: 'namespace' },
    };
    const buckets: ValidationBuckets = {
      registry: ['registry conflict'],
      form: ['Required field is missing in form: Title'],
      rules: ['namespace: duplicate detected'],
      schema: [],
    };
    const result = invoke(template, buckets, { ajvErrorsForUnifiedFormat: [err] });
    expect(result.validationIssues.length).toBeGreaterThanOrEqual(3);
    expect(result.errorsFormatted).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// formatUnifiedIssues: primaryLabel humanizeKey fallback (L567 binary-expr arm1)
// fires when guessPrimaryFieldId returns an id but idToLabel has no entry for it
// (template field has id but no attributes.label)
// ---------------------------------------------------------------------------
describe('formatUnifiedIssues — primaryLabel humanizeKey fallback (L567 arm1)', () => {
  it('uses humanizeKey when primary field has no label attribute (L567 binary-expr arm1)', () => {
    const template: Template = { body: [{ id: 'identifier' }] };
    const result = invoke(template, { ...emptyBuckets(), registry: ['already registered'] });
    expect(result.errorsFormatted).toContain('Identifier');
  });
});
