import { describe, test, expect } from '@jest/globals';
import {
  resolvePrimaryIdFromCandidate,
  resolvePrimaryIdFromTemplate,
  normalizeFormDataForHookValidation,
  buildFormDataForHookValidationFromCandidate,
  projectForSchema,
} from '../src/handlers/request/validation/form-schema-projection.js';

// ---- resolvePrimaryIdFromCandidate -----------------------------------------------

describe('resolvePrimaryIdFromCandidate', () => {
  test('picks identifier directly', () => {
    expect(resolvePrimaryIdFromCandidate({ identifier: 'foo' }, {})).toBe('foo');
  });
  test('picks namespace when identifier absent', () => {
    expect(resolvePrimaryIdFromCandidate({ namespace: 'ns' }, {})).toBe('ns');
  });
  test('picks id when identifier and namespace absent', () => {
    expect(resolvePrimaryIdFromCandidate({ id: 'the-id' }, {})).toBe('the-id');
  });
  test('returns empty string when no primary fields', () => {
    expect(resolvePrimaryIdFromCandidate({ other: 'x' }, {})).toBe('');
  });
  test('uses schema x-form-field when no direct match', () => {
    const schema = { properties: { myId: { 'x-form-field': 'identifier' } } };
    expect(resolvePrimaryIdFromCandidate({ myId: 'mapped' }, schema)).toBe('mapped');
  });
  test('uses schema property name when it is a primary field', () => {
    const schema = { properties: { namespace: {} } };
    expect(resolvePrimaryIdFromCandidate({ namespace: 'ns-val' }, schema)).toBe('ns-val');
  });
  test('null schema returns empty when candidate has no direct keys', () => {
    expect(resolvePrimaryIdFromCandidate({ other: 'x' }, null)).toBe('');
  });
  test('non-plain-object schema returns empty when no direct match', () => {
    expect(resolvePrimaryIdFromCandidate({ other: 'x' }, 'string' as never)).toBe('');
  });
  test('trims whitespace from identifier', () => {
    expect(resolvePrimaryIdFromCandidate({ identifier: '  foo  ' }, {})).toBe('foo');
  });
});

// ---- resolvePrimaryIdFromTemplate -----------------------------------------------

describe('resolvePrimaryIdFromTemplate', () => {
  test('picks identifier from formData directly', () => {
    expect(resolvePrimaryIdFromTemplate({}, { identifier: 'abc' }, {})).toBe('abc');
  });
  test('picks namespace from formData', () => {
    expect(resolvePrimaryIdFromTemplate({}, { namespace: 'ns' }, {})).toBe('ns');
  });
  test('picks product-id from formData', () => {
    expect(resolvePrimaryIdFromTemplate({}, { 'product-id': 'pid' }, {})).toBe('pid');
  });
  test('uses schema x-form-field=identifier mapping when form has no primary key', () => {
    const schema = { properties: { myProp: { 'x-form-field': 'identifier' } } };
    const form = { myProp: 'fallback-id' };
    expect(resolvePrimaryIdFromTemplate({}, form, schema)).toBe('fallback-id');
  });
  test('falls back to pickIdentifierFromFields from template body', () => {
    const template = {
      body: [{ id: 'identifier', validations: { required: true }, attributes: { label: 'ID' } }],
    };
    const form = { identifier: 'from-body' };
    expect(resolvePrimaryIdFromTemplate(template, form, null)).toBe('from-body');
  });
  test('pickIdentifierFromFields: non-array body uses empty fields', () => {
    const template = { body: 'not-array' as unknown as never };
    expect(resolvePrimaryIdFromTemplate(template, {}, null)).toBe('');
  });
  test('pickIdentifierFromFields: field with required=false skipped in id pass', () => {
    const template = {
      body: [
        { id: 'identifier', validations: { required: false }, attributes: { label: 'ID' } },
        { id: 'title', validations: { required: true }, attributes: { label: 'Title' } },
      ],
    };
    expect(resolvePrimaryIdFromTemplate(template, { title: 'T', identifier: 'I' }, null)).toBe('I');
  });
  test('pickIdentifierFromFields: label containing namespace triggers match', () => {
    const template = {
      body: [{ id: 'ns', validations: { required: true }, attributes: { label: 'namespace label' } }],
    };
    expect(resolvePrimaryIdFromTemplate(template, { ns: 'ns-val' }, null)).toBe('ns-val');
  });
  test('pickIdentifierFromFields: id-like match with empty raw falls to second pass', () => {
    const template = {
      body: [
        { id: 'identifier', validations: { required: true }, attributes: { label: '' } },
        { id: 'other', validations: { required: true }, attributes: { label: 'other' } },
      ],
    };
    expect(resolvePrimaryIdFromTemplate(template, { identifier: '', other: 'fallback' }, null)).toBe('fallback');
  });
  test('pickIdentifierFromFields: field with no id skipped in second pass', () => {
    const template = {
      body: [
        { validations: { required: true } },
        { id: 'ok', validations: { required: true }, attributes: { label: '' } },
      ],
    };
    expect(resolvePrimaryIdFromTemplate(template, { ok: 'ok-val' }, null)).toBe('ok-val');
  });
  test('pickIdentifierFromFields: third fallback picks first non-empty required=false field', () => {
    const template = {
      body: [{ id: 'anyprop', validations: { required: false }, attributes: { label: '' } }],
    };
    expect(resolvePrimaryIdFromTemplate(template, { anyprop: 'last-resort' }, null)).toBe('last-resort');
  });
  test('returns empty string when all resolution paths fail', () => {
    expect(resolvePrimaryIdFromTemplate({ body: [] }, {}, null)).toBe('');
  });
  test('null template returns empty string', () => {
    expect(resolvePrimaryIdFromTemplate(null as never, { identifier: 'x' }, {})).toBe('');
  });
});

// ---- normalizeFormDataForHookValidation -----------------------------------------

describe('normalizeFormDataForHookValidation', () => {
  test('with template: uses template-based identifier resolution', () => {
    const template = { body: [] };
    const result = normalizeFormDataForHookValidation('product', { identifier: 'myId' }, {}, template);
    expect(result.identifier).toBe('myId');
    expect(result.namespace).toBe('myId');
    expect(result.requestType).toBe('product');
  });
  test('without template: uses schema+form resolution', () => {
    const result = normalizeFormDataForHookValidation('product', { identifier: 'sid' }, {});
    expect(result.identifier).toBe('sid');
  });
  test('without template: falls back to title when no primary id', () => {
    const result = normalizeFormDataForHookValidation('product', { title: 'My Title' }, {});
    expect(result.identifier).toBe('My Title');
  });
  test('description from system-description key', () => {
    const result = normalizeFormDataForHookValidation('product', { 'system-description': 'sys desc' }, {});
    expect(result.description).toBe('sys desc');
  });
  test('description from sub-context-description key', () => {
    const result = normalizeFormDataForHookValidation('product', { 'sub-context-description': 'sub desc' }, {});
    expect(result.description).toBe('sub desc');
  });
  test('contact from contacts key (fallback)', () => {
    const result = normalizeFormDataForHookValidation('product', { contacts: 'alice\nbob' }, {});
    expect(result.contact).toBe('alice\nbob');
  });
  test('correlationIds from correlation-ids key (fallback)', () => {
    const result = normalizeFormDataForHookValidation('product', { 'correlation-ids': 'a\nb' }, {});
    expect(result.correlationIds).toBe('a\nb');
  });
});

// ---- projectForSchema — throws -------------------------------------------------

describe('projectForSchema — throws when no properties', () => {
  test('throws for null schemaObj', async () => {
    await expect(projectForSchema('cat', {}, null)).rejects.toThrow();
  });
  test('throws for schema without properties', async () => {
    await expect(projectForSchema('cat', {}, { type: 'object' })).rejects.toThrow();
  });
});

// ---- projectForSchema — type mapping ------------------------------------------

describe('projectForSchema — type mapping', () => {
  test('uses const from type schema property', async () => {
    const schema = { properties: { type: { const: 'fixed-type' } } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result.type).toBe('fixed-type');
  });
  test('uses category when type property has no const', async () => {
    const schema = { properties: { type: {} } };
    const result = await projectForSchema('mycat', {}, schema);
    expect(result.type).toBe('mycat');
  });
  test('no type property in schema — no type in result', async () => {
    const schema = { properties: { name: {} } };
    const result = await projectForSchema('cat', { name: 'n' }, schema);
    expect(result).not.toHaveProperty('type');
  });
});

// ---- projectForSchema — name and identifier ------------------------------------

describe('projectForSchema — name and identifier', () => {
  test('name from identifier in form', async () => {
    const schema = { properties: { name: {} } };
    const result = await projectForSchema('cat', { identifier: 'my-id' }, schema);
    expect(result.name).toBe('my-id');
  });
  test('name from namespace when identifier absent', async () => {
    const schema = { properties: { name: {} } };
    const result = await projectForSchema('cat', { namespace: 'my-ns' }, schema);
    expect(result.name).toBe('my-ns');
  });
  test('identifier property mapped when form.identifier set', async () => {
    const schema = { properties: { identifier: {} } };
    const result = await projectForSchema('cat', { identifier: 'the-id' }, schema);
    expect(result.identifier).toBe('the-id');
  });
  test('identifier skipped when form.identifier empty', async () => {
    const schema = { properties: { identifier: {} } };
    const result = await projectForSchema('cat', { identifier: '' }, schema);
    expect(result).not.toHaveProperty('identifier');
  });
});

// ---- projectForSchema — description -------------------------------------------

describe('projectForSchema — description', () => {
  test('description mapped when present', async () => {
    const schema = { properties: { description: {} } };
    const result = await projectForSchema('cat', { description: 'desc text' }, schema);
    expect(result.description).toBe('desc text');
  });
  test('description skipped when empty', async () => {
    const schema = { properties: { description: {} } };
    const result = await projectForSchema('cat', { description: '  ' }, schema);
    expect(result).not.toHaveProperty('description');
  });
});

// ---- projectForSchema — visibility --------------------------------------------

describe('projectForSchema — visibility', () => {
  test('open-system=yes maps to public', async () => {
    const schema = { properties: { visibility: {} } };
    const result = await projectForSchema('cat', { 'open-system': 'yes' }, schema);
    expect(result.visibility).toBe('public');
  });
  test('open-system=no maps to internal', async () => {
    const schema = { properties: { visibility: {} } };
    const result = await projectForSchema('cat', { 'open-system': 'no' }, schema);
    expect(result.visibility).toBe('internal');
  });
  test('visibility from visibility field directly', async () => {
    const schema = { properties: { visibility: {} } };
    const result = await projectForSchema('cat', { visibility: 'private' }, schema);
    expect(result.visibility).toBe('private');
  });
  test('visibility skipped when no open-system and no visibility field', async () => {
    const schema = { properties: { visibility: {} } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('visibility');
  });
});

// ---- projectForSchema — contacts/correlationIds --------------------------------

describe('projectForSchema — contacts', () => {
  test('contacts array coerced from newline-separated string', async () => {
    const schema = { properties: { contacts: { type: 'array' } } };
    const result = await projectForSchema('cat', { contact: 'alice\nbob' }, schema);
    expect(result.contacts).toEqual(['alice', 'bob']);
  });
  test('contact (singular) coerced from string', async () => {
    const schema = { properties: { contact: { type: 'array' } } };
    const result = await projectForSchema('cat', { contact: 'alice' }, schema);
    expect(result.contact).toEqual(['alice']);
  });
  test('contacts omitted when contact field is empty', async () => {
    const schema = { properties: { contacts: { type: 'array' } } };
    const result = await projectForSchema('cat', { contact: '' }, schema);
    expect(result).not.toHaveProperty('contacts');
  });
});

describe('projectForSchema — correlationIds', () => {
  test('correlationIds coerced from newline string', async () => {
    const schema = { properties: { correlationIds: { type: 'array' } } };
    const result = await projectForSchema('cat', { 'correlation-ids': 'id1\nid2' }, schema);
    expect(result.correlationIds).toEqual(['id1', 'id2']);
  });
  test('correlationIds omitted when empty', async () => {
    const schema = { properties: { correlationIds: { type: 'array' } } };
    const result = await projectForSchema('cat', { 'correlation-ids': '' }, schema);
    expect(result).not.toHaveProperty('correlationIds');
  });
});

// ---- projectForSchema — boolean coercion ----------------------------------------

describe('projectForSchema — boolean coercion via x-form-field', () => {
  const boolSchema = { properties: { isPublic: { 'type': 'boolean', 'x-form-field': 'open-system' } } };
  test('yes → true', async () => {
    expect((await projectForSchema('cat', { 'open-system': 'yes' }, boolSchema)).isPublic).toBe(true);
  });
  test('no → false', async () => {
    expect((await projectForSchema('cat', { 'open-system': 'no' }, boolSchema)).isPublic).toBe(false);
  });
  test('y → true', async () => {
    expect((await projectForSchema('cat', { 'open-system': 'y' }, boolSchema)).isPublic).toBe(true);
  });
  test('n → false', async () => {
    expect((await projectForSchema('cat', { 'open-system': 'n' }, boolSchema)).isPublic).toBe(false);
  });
  test('1 → true', async () => {
    expect((await projectForSchema('cat', { 'open-system': '1' }, boolSchema)).isPublic).toBe(true);
  });
  test('0 → false', async () => {
    expect((await projectForSchema('cat', { 'open-system': '0' }, boolSchema)).isPublic).toBe(false);
  });
  test('unrecognized string → omitted', async () => {
    const result = await projectForSchema('cat', { 'open-system': 'maybe' }, boolSchema);
    expect(result).not.toHaveProperty('isPublic');
  });
  test('native boolean true passes through', async () => {
    expect((await projectForSchema('cat', { 'open-system': true as unknown as string }, boolSchema)).isPublic).toBe(
      true
    );
  });
});

// ---- projectForSchema — integer/number coercion ---------------------------------

describe('projectForSchema — integer/number coercion via x-form-field', () => {
  const intSchema = { properties: { count: { 'type': 'integer', 'x-form-field': 'count-field' } } };
  test('numeric string → number', async () => {
    expect((await projectForSchema('cat', { 'count-field': '42' }, intSchema)).count).toBe(42);
  });
  test('empty string → omitted', async () => {
    const result = await projectForSchema('cat', { 'count-field': '' }, intSchema);
    expect(result).not.toHaveProperty('count');
  });
  test('non-numeric string → omitted', async () => {
    const result = await projectForSchema('cat', { 'count-field': 'abc' }, intSchema);
    expect(result).not.toHaveProperty('count');
  });
  test('native number passes through', async () => {
    expect((await projectForSchema('cat', { 'count-field': 5 as unknown as string }, intSchema)).count).toBe(5);
  });
});

// ---- projectForSchema — array coercion ------------------------------------------

describe('projectForSchema — array coercion via x-form-field', () => {
  const arrSchema = { properties: { tags: { 'type': 'array', 'x-form-field': 'tags' } } };
  test('array input passes through', async () => {
    expect((await projectForSchema('cat', { tags: ['a', 'b'] as unknown as string }, arrSchema)).tags).toEqual([
      'a',
      'b',
    ]);
  });
  test('newline-separated string coerced to array', async () => {
    expect((await projectForSchema('cat', { tags: 'x\ny' }, arrSchema)).tags).toEqual(['x', 'y']);
  });
  test('empty string → omitted', async () => {
    const result = await projectForSchema('cat', { tags: '' }, arrSchema);
    expect(result).not.toHaveProperty('tags');
  });
  test('array with mixed non-scalar items stays as array', async () => {
    const mixed = [{ a: 1 }, 'b'] as unknown as string;
    expect((await projectForSchema('cat', { tags: mixed }, arrSchema)).tags).toEqual([{ a: 1 }, 'b']);
  });
});

// ---- projectForSchema — object coercion -----------------------------------------

describe('projectForSchema — object coercion via x-form-field', () => {
  const objSchema = { properties: { meta: { 'type': 'object', 'x-form-field': 'meta' } } };
  test('plain object passes through', async () => {
    const obj = { key: 'val' };
    expect((await projectForSchema('cat', { meta: obj as unknown as string }, objSchema)).meta).toEqual(obj);
  });
  test('JSON string coerced to object', async () => {
    expect((await projectForSchema('cat', { meta: '{"k":"v"}' }, objSchema)).meta).toEqual({ k: 'v' });
  });
  test('non-object string → omitted', async () => {
    const result = await projectForSchema('cat', { meta: 'plain text' }, objSchema);
    expect(result).not.toHaveProperty('meta');
  });
});

// ---- projectForSchema — title, shortDescription, summary, details, parentId -----

describe('projectForSchema — title and text fields', () => {
  test('title mapped when present', async () => {
    const schema = { properties: { title: {} } };
    const result = await projectForSchema('cat', { title: 'My Title' }, schema);
    expect(result.title).toBe('My Title');
  });
  test('title skipped when absent', async () => {
    const schema = { properties: { title: {} } };
    expect(await projectForSchema('cat', {}, schema)).not.toHaveProperty('title');
  });
  test('shortDescription from short-description key', async () => {
    const schema = { properties: { shortDescription: {} } };
    const result = await projectForSchema('cat', { 'short-description': 'brief' }, schema);
    expect(result.shortDescription).toBe('brief');
  });
  test('summary mapped', async () => {
    const schema = { properties: { summary: {} } };
    const result = await projectForSchema('cat', { summary: 'sum' }, schema);
    expect(result.summary).toBe('sum');
  });
  test('details mapped', async () => {
    const schema = { properties: { details: {} } };
    const result = await projectForSchema('cat', { details: 'det' }, schema);
    expect(result.details).toBe('det');
  });
  test('parentId mapped', async () => {
    const schema = { properties: { parentId: {} } };
    const result = await projectForSchema('cat', { parentId: 'parent.ns' }, schema);
    expect(result.parentId).toBe('parent.ns');
  });
});

// ---- projectForSchema — fallback mapping ----------------------------------------

describe('projectForSchema — fallback mapping (no x-form-field)', () => {
  test('maps form key matching schema prop when no x-form-field', async () => {
    const schema = { properties: { customField: {} } };
    const result = await projectForSchema('cat', { customField: 'cval' }, schema);
    expect(result.customField).toBe('cval');
  });
  test('skips when form lacks the key', async () => {
    const schema = { properties: { customField: {} } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('customField');
  });
  test('non-plain-object propDef still allows fallback mapping', async () => {
    const schema = { properties: { customField: 'not-an-object' } };
    const result = await projectForSchema('cat', { customField: 'val' }, schema);
    expect(result.customField).toBe('val');
  });
});

// ---- buildFormDataForHookValidationFromCandidate ---------------------------------

describe('buildFormDataForHookValidationFromCandidate', () => {
  test('null candidate uses empty record', async () => {
    const result = await buildFormDataForHookValidationFromCandidate('product', {}, null as never);
    expect(result.requestType).toBe('product');
    expect(result.identifier).toBe('');
  });
  test('maps candidate props via schema properties', async () => {
    const schema = { properties: { identifier: {} } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { identifier: 'cand-id' });
    expect(result.identifier).toBe('cand-id');
  });
  test('maps remaining candidate props not in schema', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { extra: 'val' });
    expect(result.extra).toBe('val');
  });
  test('boolean candidate value stringified to true/false string', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { flag: true });
    expect(result.flag).toBe('true');
  });
  test('number candidate value stringified', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { count: 42 });
    expect(result.count).toBe('42');
  });
  test('null candidate value skipped', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { nullField: null });
    expect(result).not.toHaveProperty('nullField');
  });
  test('array of scalars joined as newline-separated lines', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { tags: ['a', 'b', 'c'] });
    expect(result.tags).toBe('a\nb\nc');
  });
  test('non-plain-object candidate uses empty record', async () => {
    const result = await buildFormDataForHookValidationFromCandidate('product', null, 'not-an-object' as never);
    expect(result.requestType).toBe('product');
  });
  test('target key already written by schema-map not overwritten by remaining-props pass', async () => {
    const schema = { properties: { identifier: {} } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { identifier: 'from-schema' });
    expect(result.identifier).toBe('from-schema');
  });
  test('array with null element: null coerced to empty string', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { tags: [null, 'a'] });
    expect(result.tags).toBe('a');
  });
});

// ---- toStringSafe object fallback (hits return '' branch) ----------------------

describe('resolvePrimaryIdFromCandidate — object-valued identifier', () => {
  test('object identifier returns empty string (toStringSafe object fallback)', () => {
    expect(resolvePrimaryIdFromCandidate({ identifier: {} as unknown as string }, {})).toBe('');
  });
  test('array identifier returns empty string', () => {
    expect(resolvePrimaryIdFromCandidate({ identifier: [] as unknown as string }, {})).toBe('');
  });
});

// ---- parseMaybeYamlJson via projectForSchema -----------------------------------

describe('projectForSchema — parseMaybeYamlJson paths', () => {
  test('object-type coercion with empty string input → omitted', async () => {
    const schema = { properties: { meta: { 'type': 'object', 'x-form-field': 'meta' } } };
    const result = await projectForSchema('cat', { meta: '' }, schema);
    expect(result).not.toHaveProperty('meta');
  });
  test('array type with items.type=object: YAML-parsed array result', async () => {
    const schema = {
      properties: {
        items: {
          'type': 'array',
          'x-form-field': 'items',
          'items': { type: 'object' },
        },
      },
    };
    const result = await projectForSchema('cat', { items: '[{"id":1}]' }, schema);
    expect(Array.isArray(result.items)).toBe(true);
  });
  test('empty category and no type const omits type field', async () => {
    const schema = { properties: { type: {} } };
    const result = await projectForSchema('', {}, schema);
    expect(result).not.toHaveProperty('type');
  });
});

// ---- projectForSchema — coerced-undefined and empty-array skips ----------------

describe('projectForSchema — skip conditions', () => {
  test('coerced-undefined value omits property', async () => {
    const schema = { properties: { flag: { 'type': 'boolean', 'x-form-field': 'flag' } } };
    const result = await projectForSchema('cat', { flag: 'unknown-bool' }, schema);
    expect(result).not.toHaveProperty('flag');
  });
  test('empty coerced array omits property', async () => {
    const schema = { properties: { tags: { 'type': 'array', 'x-form-field': 'tags' } } };
    const result = await projectForSchema('cat', { tags: '' }, schema);
    expect(result).not.toHaveProperty('tags');
  });
  test('contacts empty array omitted from result', async () => {
    const schema = { properties: { contacts: { type: 'array' } } };
    const result = await projectForSchema('cat', { contact: '' }, schema);
    expect(result).not.toHaveProperty('contacts');
  });
  test('correlationIds empty array omitted', async () => {
    const schema = { properties: { correlationIds: { type: 'array' } } };
    const result = await projectForSchema('cat', { 'correlation-ids': '' }, schema);
    expect(result).not.toHaveProperty('correlationIds');
  });
  test('array with non-scalar elements stringified via YAML', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, {
      nested: [{ a: 1 }, { b: 2 }],
    });
    expect(result).toHaveProperty('nested');
  });
});

// ---- pickIdentifierFromFields: third pass no-id field (L84 arm0) ----------------

describe('pickIdentifierFromFields third pass — field with no id (L84 arm0)', () => {
  test('no-id required=false field skipped in third pass; next field found', () => {
    const template = {
      body: [{ validations: { required: false } }, { id: 'anyprop', validations: { required: false } }],
    };
    expect(resolvePrimaryIdFromTemplate(template, { anyprop: 'val' }, null)).toBe('val');
  });
});

// ---- readPrimaryValueFromSchemaFields branches (L117, L123) ---------------------

describe('resolvePrimaryIdFromCandidate — schema field branches', () => {
  test('non-plain-object propDef in schema is skipped (L117 arm0)', () => {
    const schema = { properties: { myField: 'not-an-object' as never } };
    expect(resolvePrimaryIdFromCandidate({ other: 'x' }, schema)).toBe('');
  });

  test('primary x-form-field but no match in record returns empty (L123 arm1)', () => {
    const schema = { properties: { propX: { 'x-form-field': 'identifier' } } };
    expect(resolvePrimaryIdFromCandidate({ other: 'val' }, schema)).toBe('');
  });
});

// ---- readPrimaryValueFromSchemaPropertyNames (L134 arm1, L137 arm0+arm1) -------

describe('resolvePrimaryIdFromCandidate — schema property name primary resolution', () => {
  test('primary schema property with value → returned (L134 arm1, L137 arm0)', () => {
    const schema = { properties: { product_id: {} } };
    expect(resolvePrimaryIdFromCandidate({ product_id: 'p123' }, schema)).toBe('p123');
  });

  test('first primary prop empty, second has value (L134 arm1 ×2, L137 arm0+arm1)', () => {
    const schema = { properties: { NAMESPACE: {}, product_id: {} } };
    expect(resolvePrimaryIdFromCandidate({ NAMESPACE: '', product_id: 'val' }, schema)).toBe('val');
  });
});

// ---- projectForSchema first loop — L454 arm0 (key empty from toStringSafe) -----

describe('projectForSchema — x-form-field is non-string (L454 arm0)', () => {
  test('x-form-field is object → toStringSafe returns empty → skip', async () => {
    // eslint-disable-next-line @typescript-eslint/consistent-type-assertions
    const schema = { properties: { myProp: { 'x-form-field': {} as never } } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('myProp');
  });
});

// ---- projectForSchema first loop — L460 arm0 (empty plain object) ---------------

describe('projectForSchema — empty object coercion skipped (L460 arm0)', () => {
  test('JSON {} coerced to empty object → property omitted', async () => {
    const schema = { properties: { meta: { 'type': 'object', 'x-form-field': 'meta' } } };
    const result = await projectForSchema('cat', { meta: '{}' }, schema);
    expect(result).not.toHaveProperty('meta');
  });
});

// ---- projectForSchema — contact array input (L366 arm0) -------------------------

describe('projectForSchema — contact already an array (L366 arm0)', () => {
  test('contact is array → passed directly without split', async () => {
    const schema = { properties: { contacts: { type: 'array' } } };
    const result = await projectForSchema('cat', { contact: ['alice', 'bob'] as never }, schema);
    expect(result.contacts).toEqual(['alice', 'bob']);
  });
});

// ---- projectForSchema — contacts/corrIds coerced to undefined (L489/L499 arm1) -

describe('projectForSchema — contact/corrIds coerce-to-undefined skipped (L489/L499 arm1)', () => {
  test('contacts coerced to undefined when propDef is type=object and arr given (L489 arm1)', async () => {
    const schema = { properties: { contacts: { type: 'object' } } };
    const result = await projectForSchema('cat', { contact: 'alice' }, schema);
    expect(result).not.toHaveProperty('contacts');
  });

  test('correlationIds coerced to undefined when type=object (L499 arm1)', async () => {
    const schema = { properties: { correlationIds: { type: 'object' } } };
    const result = await projectForSchema('cat', { 'correlation-ids': 'id1' }, schema);
    expect(result).not.toHaveProperty('correlationIds');
  });
});

// ---- projectForSchema — correlationIdTypes (L505, L509 branches) ----------------

describe('projectForSchema — correlationIdTypes branch coverage', () => {
  test('form has correlation-id-types (not correlationIdTypes) → L505 arm1', async () => {
    const schema = { properties: { correlationIdTypes: { type: 'array' } } };
    const result = await projectForSchema('cat', { 'correlation-id-types': ['label:a'] as never }, schema);
    expect(result).toHaveProperty('correlationIdTypes');
  });

  test('citRaw is already an array → L509 arm1 (skip string-parse path)', async () => {
    const schema = { properties: { correlationIdTypes: { type: 'array' } } };
    const result = await projectForSchema('cat', { correlationIdTypes: ['lbl:v'] as never }, schema);
    expect(result).toHaveProperty('correlationIdTypes');
  });
});

// ---- projectForSchema — items=object, YAML parses to non-array (L390 arm1) -----

describe('projectForSchema — items=object non-array YAML result (L390 arm1)', () => {
  test('array-type with items.type=object, JSON object string → fallback to toUniqueStringArray', async () => {
    const schema = {
      properties: {
        items: { 'type': 'array', 'x-form-field': 'items', 'items': { type: 'object' } },
      },
    };
    const result = await projectForSchema('cat', { items: '{"key":"val"}' }, schema);
    expect(Array.isArray(result.items)).toBe(true);
  });
});

// ---- projectForSchema — shortDescription/summary/details/parentId arm1 ---------

describe('projectForSchema — optional text fields empty path (arm1)', () => {
  test('shortDescription absent in form → not set in candidate (L527 arm2)', async () => {
    const schema = { properties: { shortDescription: {} } };
    const result = await projectForSchema('cat', { other: 'val' }, schema);
    expect(result).not.toHaveProperty('shortDescription');
  });

  test('summary empty → not set in candidate (L536 arm1)', async () => {
    const schema = { properties: { summary: {} } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('summary');
  });

  test('details empty → not set in candidate (L541 arm1)', async () => {
    const schema = { properties: { details: {} } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('details');
  });

  test('parentId empty → not set in candidate (L546 arm1)', async () => {
    const schema = { properties: { parentId: {} } };
    const result = await projectForSchema('cat', {}, schema);
    expect(result).not.toHaveProperty('parentId');
  });
});

// ---- buildFormDataForHookValidationFromCandidate — mapSchemaCandidatePropsToForm branches

describe('buildFormDataForHookValidationFromCandidate — schema mapping branches', () => {
  test('schema prop missing from candidate → serialized empty → skip (L324 arm0)', async () => {
    const schema = { properties: { missingProp: {} } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { other: 'val' });
    expect(result).not.toHaveProperty('missingProp');
  });

  test('empty array candidate value → serialized empty → not set (L313 arm1)', async () => {
    const schema = { properties: {} };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { tags: [] });
    expect(result).not.toHaveProperty('tags');
  });

  test('non-plain-object propDef → ff = empty string (L326 arm1)', async () => {
    const schema = { properties: { myField: 'not-an-object' as never } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { myField: 'val' });
    expect(result.myField).toBe('val');
  });

  test('propDef with x-form-field → form field alias written (L327 arm0)', async () => {
    const schema = { properties: { myProp: { 'x-form-field': 'myAlias' } } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { myProp: 'val' });
    expect(result.myAlias).toBe('val');
    expect(result.myProp).toBe('val');
  });

  test('x-form-field alias already written → not overwritten (L327 binary-expr arm1)', async () => {
    const schema = { properties: { a: { 'x-form-field': 'shared' }, b: { 'x-form-field': 'shared' } } };
    const result = await buildFormDataForHookValidationFromCandidate('product', schema, { a: 'first', b: 'second' });
    expect(result.shared).toBe('first');
  });
});
