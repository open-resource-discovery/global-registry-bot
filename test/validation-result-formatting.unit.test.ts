/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Branch-coverage tests for the pure/exported helper logic in
 * src/handlers/request/validation/run.ts
 *
 * These tests exercise formatting, resolution, and projection functions directly
 * through the exported surface without mocking any external modules.
 */
import { describe, it, expect } from '@jest/globals';
import {
  resolvePrimaryIdFromCandidate,
  resolvePrimaryIdFromTemplate,
  projectForSchema,
} from '../src/handlers/request/validation/run.js';

// Helpers
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeTemplate(fields: { id: string; label?: string; required?: boolean }[]) {
  return {
    body: fields.map((f) => ({
      id: f.id,
      attributes: { label: f.label ?? f.id },
      validations: f.required === true ? { required: true } : undefined,
    })),
    _meta: {},
  };
}

// resolvePrimaryIdFromCandidate
describe('resolvePrimaryIdFromCandidate', () => {
  it('returns identifier when present', () => {
    const candidate = { identifier: 'sap.foo', name: 'other' };
    expect(resolvePrimaryIdFromCandidate(candidate, {})).toBe('sap.foo');
  });

  it('returns namespace when identifier is absent', () => {
    const candidate = { namespace: 'sap.ns' };
    expect(resolvePrimaryIdFromCandidate(candidate, {})).toBe('sap.ns');
  });

  it('returns name when identifier and namespace are absent', () => {
    const candidate = { name: 'my-name' };
    expect(resolvePrimaryIdFromCandidate(candidate, {})).toBe('my-name');
  });

  it('returns vendor when only vendor is present', () => {
    const candidate = { vendor: 'acme' };
    expect(resolvePrimaryIdFromCandidate(candidate, {})).toBe('acme');
  });

  it('returns empty string when candidate has no primary fields', () => {
    expect(resolvePrimaryIdFromCandidate({ foo: 'bar' }, {})).toBe('');
  });

  it('prefers schema x-form-field that maps to a primary field', () => {
    const candidate = { resourceName: 'mapped-id' };
    const schemaObj = {
      properties: {
        resourceName: { 'x-form-field': 'identifier' },
      },
    };
    // 'identifier' is a primary resource field — returns via schema mapping
    expect(resolvePrimaryIdFromCandidate(candidate, schemaObj)).toBe('mapped-id');
  });

  it('falls back to property name when it is a primary field name in schema', () => {
    const candidate = { identifier: 'direct-from-prop' };
    const schemaObj = {
      properties: {
        identifier: { type: 'string' },
      },
    };
    expect(resolvePrimaryIdFromCandidate(candidate, schemaObj)).toBe('direct-from-prop');
  });

  it('handles null/undefined candidate gracefully — returns empty string', () => {
    // resolvePrimaryIdFromCandidate always receives a Record but schemaObj can be anything
    expect(resolvePrimaryIdFromCandidate({}, null)).toBe('');
    expect(resolvePrimaryIdFromCandidate({}, undefined)).toBe('');
  });

  it('trims non-breaking spaces from values', () => {
    const candidate = { identifier: 'sap foo' };
    expect(resolvePrimaryIdFromCandidate(candidate, {})).toBe('sap foo'.replaceAll(' ', ' ').trim());
  });

  it('returns empty string when schemaObj has no properties key', () => {
    const candidate = { customProp: 'val' };
    const schemaObj = { type: 'object' }; // no `properties`
    expect(resolvePrimaryIdFromCandidate(candidate, schemaObj)).toBe('');
  });
});

// resolvePrimaryIdFromTemplate
describe('resolvePrimaryIdFromTemplate', () => {
  it('returns empty string when template is null/undefined', () => {
    expect(resolvePrimaryIdFromTemplate(null as any, {}, {})).toBe('');
    expect(resolvePrimaryIdFromTemplate(undefined as any, {}, {})).toBe('');
  });

  it('picks identifier field directly from formData', () => {
    const template = makeTemplate([{ id: 'identifier', required: true }]);
    const form = { identifier: '  sap.one  ' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('sap.one');
  });

  it('picks namespace field directly from formData', () => {
    const template = makeTemplate([{ id: 'namespace', required: true }]);
    const form = { namespace: 'sap.ns' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('sap.ns');
  });

  it('picks product-id field directly from formData', () => {
    const template = makeTemplate([{ id: 'product-id', required: true }]);
    const form = { 'product-id': 'PROD-1' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('PROD-1');
  });

  it('uses schema x-form-field="identifier" mapping when direct fields are missing', () => {
    const template = makeTemplate([{ id: 'resourceName', label: 'Resource Name', required: true }]);
    const form = { resourceName: 'schema-driven-id' };
    const schemaObj = {
      properties: {
        resourceName: { 'x-form-field': 'identifier' },
      },
    };
    expect(resolvePrimaryIdFromTemplate(template as any, form, schemaObj)).toBe('schema-driven-id');
  });

  it('falls back to pickIdentifierFromFields when direct and schema paths fail', () => {
    const template = makeTemplate([{ id: 'myCustomId', label: 'My Custom Identifier', required: true }]);
    const form = { myCustomId: 'fallback-val' };
    // schemaObj has no relevant x-form-field and myCustomId is not a primary field name
    // but the template field id contains "id" -> looksLikeId = true
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('fallback-val');
  });

  it('returns empty string when all resolution paths fail', () => {
    const template = makeTemplate([{ id: 'unrelated', label: 'Notes' }]);
    const form = { unrelated: '' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('');
  });

  it('handles formData with non-breaking spaces in identifier', () => {
    const template = makeTemplate([{ id: 'identifier', required: true }]);
    const form = { identifier: 'sap foo' };
    // readFirstPrimaryValue trims non-breaking spaces
    const result = resolvePrimaryIdFromTemplate(template as any, form, {});
    expect(result).toBe('sap foo');
  });

  it('picks from required field with id-like label when no standard keys present', () => {
    const template = makeTemplate([{ id: 'customRef', label: 'Resource Identifier', required: true }]);
    const form = { customRef: 'by-label' };
    // label includes 'identifier' -> looksLikeId
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('by-label');
  });

  it('falls back to first available required field value', () => {
    const template = makeTemplate([
      { id: 'alpha', label: 'Alpha', required: true },
      { id: 'beta', label: 'Beta' },
    ]);
    const form = { alpha: 'first-required' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('first-required');
  });

  it('handles empty body array in template', () => {
    const template = { body: [], _meta: {} };
    const form = { identifier: '' };
    expect(resolvePrimaryIdFromTemplate(template as any, form, {})).toBe('');
  });
});

// projectForSchema — branch coverage for coerceBySchema + generic mapping
describe('projectForSchema', () => {
  it('throws when schema has no properties', async () => {
    await expect(projectForSchema('system', {}, { type: 'object' })).rejects.toThrow(
      'Configuration error: schema is missing or malformed'
    );
  });

  it('throws when schemaObj is null', async () => {
    await expect(projectForSchema('system', {}, null)).rejects.toThrow();
  });

  it('maps type from const in schema', async () => {
    const schema = { properties: { type: { const: 'system' } } };
    const result = await projectForSchema('system', {}, schema);
    expect(result.type).toBe('system');
  });

  it('uses category for type when schema type has no const', async () => {
    const schema = { properties: { type: { type: 'string' } } };
    const result = await projectForSchema('myCategory', {}, schema);
    expect(result.type).toBe('myCategory');
  });

  it('maps name from identifier in form', async () => {
    const schema = { properties: { type: { const: 'x' }, name: { type: 'string' } } };
    const result = await projectForSchema('x', { identifier: 'acme.sys' }, schema);
    expect(result.name).toBe('acme.sys');
  });

  it('maps name from namespace when identifier is absent', async () => {
    const schema = { properties: { type: { const: 'x' }, name: { type: 'string' } } };
    const result = await projectForSchema('x', { namespace: 'acme.ns' } as any, schema);
    expect(result.name).toBe('acme.ns');
  });

  it('maps description from form', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        description: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', description: '  hello  ' } as any, schema);
    expect(result.description).toBe('hello');
  });

  it('does NOT set description when blank', async () => {
    const schema = {
      properties: { type: { const: 'x' }, description: { type: 'string' } },
    };
    const result = await projectForSchema('x', { description: '   ' } as any, schema);
    expect(result).not.toHaveProperty('description');
  });

  it('maps visibility from open-system=yes -> public', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        visibility: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', { 'identifier': 'a', 'open-system': 'yes' } as any, schema);
    expect(result.visibility).toBe('public');
  });

  it('maps visibility from open-system=no -> internal', async () => {
    const schema = {
      properties: { type: { const: 'x' }, visibility: { type: 'string' } },
    };
    const result = await projectForSchema('x', { 'open-system': 'no' } as any, schema);
    expect(result.visibility).toBe('internal');
  });

  it('does NOT set visibility when open-system is empty and no visibility key', async () => {
    const schema = { properties: { type: { const: 'x' }, visibility: { type: 'string' } } };
    const result = await projectForSchema('x', {} as any, schema);
    expect(result).not.toHaveProperty('visibility');
  });

  it('maps contacts array from contact string', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        contacts: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', contact: 'a@x\nb@y' } as any, schema);
    expect(result.contacts).toEqual(['a@x', 'b@y']);
  });

  it('maps contact (singular) when contacts is not in schema', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        contact: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', contact: 'x@y' } as any, schema);
    expect(result.contact).toEqual(['x@y']);
  });

  it('skips contacts when contact field is empty', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        contacts: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { contact: '' } as any, schema);
    expect(result).not.toHaveProperty('contacts');
  });

  it('maps correlationIds deduplicated', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        correlationIds: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', correlationIds: 'c1\nc1\nc2' } as any, schema);
    expect(result.correlationIds).toEqual(['c1', 'c2']);
  });

  it('maps correlation-ids via alternate key', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        correlationIds: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { 'identifier': 'a', 'correlation-ids': 'x1,x2' } as any, schema);
    expect(result.correlationIds).toEqual(['x1', 'x2']);
  });

  it('maps correlationIdTypes from JSON string', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        correlationIdTypes: { type: 'array', items: { type: 'object' } },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', correlationIdTypes: '[{"k":"v"}]' } as any, schema);
    expect(result.correlationIdTypes).toEqual([{ k: 'v' }]);
  });

  it('skips correlationIdTypes when raw value is empty string', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        correlationIdTypes: { type: 'array', items: { type: 'object' } },
      },
    };
    // Empty string -> parseMaybeYamlJson returns undefined -> coerceBySchema returns []
    const result = await projectForSchema('x', { correlationIdTypes: '' } as any, schema);
    expect(result).not.toHaveProperty('correlationIdTypes');
  });

  it('maps boolean field true via x-form-field', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        enabled: { 'type': 'boolean', 'x-form-field': 'enabled' },
      },
    };
    const result = await projectForSchema('x', { enabled: 'yes' } as any, schema);
    expect(result.enabled).toBe(true);
  });

  it('maps boolean field false via x-form-field', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        enabled: { 'type': 'boolean', 'x-form-field': 'enabled' },
      },
    };
    const result = await projectForSchema('x', { enabled: 'no' } as any, schema);
    expect(result.enabled).toBe(false);
  });

  it('returns undefined (skips) for unrecognized boolean string', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        enabled: { 'type': 'boolean', 'x-form-field': 'enabled' },
      },
    };
    const result = await projectForSchema('x', { enabled: 'maybe' } as any, schema);
    // coerceBySchema returns undefined -> not set
    expect(result).not.toHaveProperty('enabled');
  });

  it('maps integer field via x-form-field', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        count: { 'type': 'integer', 'x-form-field': 'count' },
      },
    };
    const result = await projectForSchema('x', { count: '42' } as any, schema);
    expect(result.count).toBe(42);
  });

  it('skips integer field when value is not a finite number', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        count: { 'type': 'integer', 'x-form-field': 'count' },
      },
    };
    const result = await projectForSchema('x', { count: 'not-a-number' } as any, schema);
    expect(result).not.toHaveProperty('count');
  });

  it('passes through existing number values unchanged', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        score: { 'type': 'number', 'x-form-field': 'score' },
      },
    };
    const result = await projectForSchema('x', { score: 3.14 } as any, schema);
    expect(result.score).toBe(3.14);
  });

  it('skips integer field when value is empty string', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        count: { 'type': 'integer', 'x-form-field': 'count' },
      },
    };
    const result = await projectForSchema('x', { count: '   ' } as any, schema);
    expect(result).not.toHaveProperty('count');
  });

  it('maps string x-form-field from array of strings (joins with newline)', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        notes: { 'type': 'string', 'x-form-field': 'notes' },
      },
    };
    const result = await projectForSchema('x', { notes: ['line1', 'line2'] } as any, schema);
    expect(result.notes).toBe('line1\nline2');
  });

  it('returns undefined for string x-form-field with empty array', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        notes: { 'type': 'string', 'x-form-field': 'notes' },
      },
    };
    const result = await projectForSchema('x', { notes: [] } as any, schema);
    expect(result).not.toHaveProperty('notes');
  });

  it('maps object field from JSON string via x-form-field', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        meta: { 'type': 'object', 'x-form-field': 'meta' },
      },
    };
    const result = await projectForSchema('x', { meta: '{"a":1}' } as any, schema);
    expect(result.meta).toEqual({ a: 1 });
  });

  it('skips object field when value is not parseable as object', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        meta: { 'type': 'object', 'x-form-field': 'meta' },
      },
    };
    // Array is not an object
    const result = await projectForSchema('x', { meta: [] } as any, schema);
    expect(result).not.toHaveProperty('meta');
  });

  it('skips empty object from fallback mapping', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        emptyObj: { type: 'object' },
      },
    };
    const result = await projectForSchema('x', { emptyObj: {} } as any, schema);
    expect(result).not.toHaveProperty('emptyObj');
  });

  it('maps array from x-form-field already being an array (passed through)', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        tags: { 'type': 'array', 'items': { type: 'string' }, 'x-form-field': 'tags' },
      },
    };
    const result = await projectForSchema('x', { tags: ['a', 'b'] } as any, schema);
    expect(result.tags).toEqual(['a', 'b']);
  });

  it('skips array field when coerced result is empty', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        tags: { 'type': 'array', 'items': { type: 'string' }, 'x-form-field': 'tags' },
      },
    };
    const result = await projectForSchema('x', { tags: '' } as any, schema);
    expect(result).not.toHaveProperty('tags');
  });

  it('skips x-form-field when raw value is null', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        meta: { 'type': 'object', 'x-form-field': 'meta' },
      },
    };
    const result = await projectForSchema('x', { meta: null } as any, schema);
    expect(result).not.toHaveProperty('meta');
  });

  it('fallback mapping: maps same-named prop when no x-form-field and form has the key', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        extraFlag: { type: 'boolean' },
      },
    };
    const result = await projectForSchema('x', { extraFlag: 'true' } as any, schema);
    expect(result.extraFlag).toBe(true);
  });

  it('fallback mapping: skips prop when form does not have the same key', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        missingProp: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', {} as any, schema);
    expect(result).not.toHaveProperty('missingProp');
  });

  it('maps shortDescription from short-description key', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        shortDescription: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', { 'short-description': '  brief  ' } as any, schema);
    expect(result.shortDescription).toBe('brief');
  });

  it('does NOT set shortDescription when blank', async () => {
    const schema = {
      properties: { type: { const: 'x' }, shortDescription: { type: 'string' } },
    };
    const result = await projectForSchema('x', { 'short-description': '' } as any, schema);
    expect(result).not.toHaveProperty('shortDescription');
  });

  it('maps summary from form', async () => {
    const schema = {
      properties: { type: { const: 'x' }, summary: { type: 'string' } },
    };
    const result = await projectForSchema('x', { summary: ' sum ' } as any, schema);
    expect(result.summary).toBe('sum');
  });

  it('maps details from form', async () => {
    const schema = {
      properties: { type: { const: 'x' }, details: { type: 'string' } },
    };
    const result = await projectForSchema('x', { details: ' det ' } as any, schema);
    expect(result.details).toBe('det');
  });

  it('maps parentId from form', async () => {
    const schema = {
      properties: { type: { const: 'x' }, parentId: { type: 'string' } },
    };
    const result = await projectForSchema('x', { parentId: ' p1 ' } as any, schema);
    expect(result.parentId).toBe('p1');
  });

  it('maps title from form', async () => {
    const schema = {
      properties: { type: { const: 'x' }, title: { type: 'string' } },
    };
    const result = await projectForSchema('x', { title: '  My Title  ' } as any, schema);
    expect(result.title).toBe('My Title');
  });

  it('does NOT set title when blank', async () => {
    const schema = {
      properties: { type: { const: 'x' }, title: { type: 'string' } },
    };
    const result = await projectForSchema('x', { title: '' } as any, schema);
    expect(result).not.toHaveProperty('title');
  });

  it('maps identifier prop when schema has explicit identifier property', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        identifier: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', { identifier: 'explicit-id' } as any, schema);
    expect(result.identifier).toBe('explicit-id');
  });

  it('skips identifier prop when form identifier is blank', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        identifier: { type: 'string' },
      },
    };
    const result = await projectForSchema('x', { identifier: '' } as any, schema);
    expect(result).not.toHaveProperty('identifier');
  });

  it('coerceBySchema: passes through existing boolean true directly', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        flag: { 'type': 'boolean', 'x-form-field': 'flag' },
      },
    };
    const result = await projectForSchema('x', { flag: true } as any, schema);
    expect(result.flag).toBe(true);
  });

  it('coerceBySchema: passes through existing boolean false directly', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        flag: { 'type': 'boolean', 'x-form-field': 'flag' },
      },
    };
    const result = await projectForSchema('x', { flag: false } as any, schema);
    expect(result.flag).toBe(false);
  });

  it('coerceBySchema: passes through existing number directly for integer type', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        count: { 'type': 'integer', 'x-form-field': 'count' },
      },
    };
    const result = await projectForSchema('x', { count: 7 } as any, schema);
    expect(result.count).toBe(7);
  });

  it('coerceBySchema: handles plain object raw for object type (passes through)', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        meta: { 'type': 'object', 'x-form-field': 'meta' },
      },
    };
    const result = await projectForSchema('x', { meta: { a: 1 } } as any, schema);
    expect(result.meta).toEqual({ a: 1 });
  });

  it('coerceBySchema: string field value is trimmed', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        label: { 'type': 'string', 'x-form-field': 'label' },
      },
    };
    const result = await projectForSchema('x', { label: '  trimmed  ' } as any, schema);
    expect(result.label).toBe('trimmed');
  });

  it('coerceBySchema: returns undefined (skips) for empty trimmed string field', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        label: { 'type': 'string', 'x-form-field': 'label' },
      },
    };
    const result = await projectForSchema('x', { label: '   ' } as any, schema);
    expect(result).not.toHaveProperty('label');
  });

  it('x-form-field: empty string x-form-field is skipped in schema-driven loop but fallback still maps by name', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        // propDef with x-form-field = '' — skipped in schema-driven loop (key is falsy)
        // fallback: ff = '' is falsy so fallback does NOT skip, maps propA by same name
        propA: { 'type': 'string', 'x-form-field': '' },
      },
    };
    const result = await projectForSchema('x', { propA: 'val' } as any, schema);
    // fallback mapping applies since ff (empty string) is falsy — propA IS mapped
    expect(result.propA).toBe('val');
  });

  it('deduplicates comma-separated correlation IDs', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        name: { type: 'string' },
        correlationIds: { type: 'array', items: { type: 'string' } },
      },
    };
    const result = await projectForSchema('x', { identifier: 'a', correlationIds: 'a, b, a, c' } as any, schema);
    expect(result.correlationIds).toEqual(['a', 'b', 'c']);
  });

  it('handles boolean true/false string variants: y, 1, n, 0', async () => {
    const schema = {
      properties: {
        type: { const: 'x' },
        f1: { 'type': 'boolean', 'x-form-field': 'f1' },
        f2: { 'type': 'boolean', 'x-form-field': 'f2' },
        f3: { 'type': 'boolean', 'x-form-field': 'f3' },
        f4: { 'type': 'boolean', 'x-form-field': 'f4' },
      },
    };
    const result = await projectForSchema('x', { f1: 'y', f2: '1', f3: 'n', f4: '0' } as any, schema);
    expect(result.f1).toBe(true);
    expect(result.f2).toBe(true);
    expect(result.f3).toBe(false);
    expect(result.f4).toBe(false);
  });

  it('generic mapping: no-op when props has no type, name, description etc keys', async () => {
    const schema = {
      properties: {
        customField: { type: 'string' },
      },
    };
    const result = await projectForSchema('cat', {} as any, schema);
    // none of the generic keys present in schema -> no output
    expect(Object.keys(result)).toEqual([]);
  });
});
