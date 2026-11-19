/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import {
  loadSchemaFieldAliasLookup,
  resolveMachineReadableRegistryField,
  buildRegistryValidationMachineReadableIssues,
  buildRegistryValidationAggregatePrCommentBody,
} from '../src/handlers/request/application/request-validation-posting.js';

const repoInfo = { owner: 'test-rvp', repo: 'test-rvp-repo' };

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCbs(text: string | null = null) {
  return {
    readRepoFileText: jest.fn().mockResolvedValue(text),
  } as any;
}

// loadSchemaFieldAliasLookup — covers helpers for alias building

describe('loadSchemaFieldAliasLookup', () => {
  it('returns empty map when schemaPath is empty', async () => {
    const result = await loadSchemaFieldAliasLookup({}, repoInfo, '', makeCbs());
    expect(result.size).toBe(0);
  });

  it('builds alias lookup for schema with properties (addSchemaFieldAlias continue arm)', async () => {
    const schema = {
      properties: {
        myField: { 'title': 'My Field', 'x-form-field': 'identifier' },
        tags: { title: 'Tags' },
      },
    };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias1', repo: 'r-alias1' }, 'schema.json', cbs);
    expect(result.size).toBeGreaterThan(0);
    expect(result.has('myfield')).toBe(true);
  });

  it('adds plural form for property not ending in s (else branch of singular/plural split)', async () => {
    const schema = { properties: { vendor: {} } };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias2', repo: 'r-alias2' }, 'schema.json', cbs);
    expect(result.has('vendor')).toBe(true);
    expect(result.has('vendors')).toBe(true);
  });

  it('skips non-plain-object propertyDef (early return in collectSchemaFieldAliasesForProperty)', async () => {
    const schema = { properties: { myProp: 'not-an-object' } };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias3', repo: 'r-alias3' }, 'schema.json', cbs);
    expect(result.has('myprop')).toBe(true);
  });

  it('handles schema without properties (if(props) false arm)', async () => {
    const schema = { type: 'object' };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias4', repo: 'r-alias4' }, 'schema.json', cbs);
    expect(result.size).toBe(0);
  });

  it('handles schema with non-array allOf (if(!Array.isArray) continue arm)', async () => {
    const schema = { allOf: 'bad-value', properties: {} };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias5', repo: 'r-alias5' }, 'schema.json', cbs);
    expect(result.size).toBe(0);
  });

  it('processes allOf schemas recursively', async () => {
    const schema = {
      allOf: [{ properties: { name: { title: 'Name' } } }, { properties: { code: { title: 'Code' } } }],
    };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias6', repo: 'r-alias6' }, 'schema.json', cbs);
    expect(result.has('name')).toBe(true);
    expect(result.has('code')).toBe(true);
  });

  it('returns empty map when file is not found', async () => {
    const cbs = makeCbs(null);
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias7', repo: 'r-alias7' }, 'schema.json', cbs);
    expect(result.size).toBe(0);
  });

  it('deduplicates when two properties normalize to the same alias (lookup.has(alias) return arm)', async () => {
    const schema = {
      properties: {
        'myProp': { title: 'My Prop' },
        'my-prop': { title: 'My Prop Again' },
      },
    };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await loadSchemaFieldAliasLookup({}, { owner: 'o-alias8', repo: 'r-alias8' }, 'schema.json', cbs);
    expect(result.has('myprop')).toBe(true);
  });
});

// resolveMachineReadableRegistryField

describe('resolveMachineReadableRegistryField', () => {
  it('returns "details" when fieldHint is empty (|| "details" arm)', async () => {
    const result = await resolveMachineReadableRegistryField({}, repoInfo, '', undefined, makeCbs());
    expect(result).toBe('details');
  });

  it('returns fieldHint when no schemaPath provided', async () => {
    const result = await resolveMachineReadableRegistryField({}, repoInfo, 'title', undefined, makeCbs());
    expect(result).toBe('title');
  });

  it('returns fallback when lookup is empty (lookup.size = 0)', async () => {
    const cbs = makeCbs(null);
    const result = await resolveMachineReadableRegistryField({}, repoInfo, 'myfield', 'schema.json', cbs);
    expect(result).toBe('myfield');
  });

  it('returns fallback when alias not found in lookup (|| fallback arm)', async () => {
    const schema = { properties: { name: {} } };
    const cbs = makeCbs(JSON.stringify(schema));
    const result = await resolveMachineReadableRegistryField(
      {},
      { owner: 'rvp-res1', repo: 'r-res1' },
      'unknownfield',
      'schema.json',
      cbs
    );
    expect(result).toBe('unknownfield');
  });
});

// buildRegistryValidationMachineReadableIssues

describe('buildRegistryValidationMachineReadableIssues', () => {
  it('handles null items (|| [] arm)', async () => {
    const result = await buildRegistryValidationMachineReadableIssues({}, repoInfo, null as any, makeCbs());
    expect(result).toEqual([]);
  });

  it('skips items with empty message (if(!message) continue arm)', async () => {
    const items = [{ message: '', filePath: 'foo.yaml' }] as any;
    const result = await buildRegistryValidationMachineReadableIssues({}, repoInfo, items, makeCbs());
    expect(result).toEqual([]);
  });

  it('omits filePath when normalized filePath is empty ({} spread arm)', async () => {
    const items = [{ message: '/title must be string', filePath: '' }] as any;
    const result = await buildRegistryValidationMachineReadableIssues({}, repoInfo, items, makeCbs());
    expect(result.length).toBe(1);
    expect(result[0]).not.toHaveProperty('filePath');
  });

  it('includes filePath when non-empty', async () => {
    const items = [{ message: '/name must be string', filePath: 'data/foo.yaml' }] as any;
    const result = await buildRegistryValidationMachineReadableIssues({}, repoInfo, items, makeCbs());
    expect(result[0]).toHaveProperty('filePath', 'data/foo.yaml');
  });
});

// buildRegistryValidationAggregatePrCommentBody

describe('buildRegistryValidationAggregatePrCommentBody', () => {
  it('returns empty string when entries are empty (if(!entries.length) arm)', async () => {
    const result = await buildRegistryValidationAggregatePrCommentBody({}, repoInfo, new Map(), [], makeCbs());
    expect(result).toBe('');
  });

  it('delegates to single-file body when exactly one entry', async () => {
    const byFile = new Map([['data/foo.yaml', ['/title must be string']]]);
    const result = await buildRegistryValidationAggregatePrCommentBody({}, repoInfo, byFile, [], makeCbs());
    expect(result).toContain('Detected issues');
    expect(result).toContain('data/foo.yaml');
  });

  it('builds aggregate body when multiple entries exist', async () => {
    const byFile = new Map([
      ['data/foo.yaml', ['/title must be string']],
      ['data/bar.yaml', ['/name must be string']],
    ]);
    const result = await buildRegistryValidationAggregatePrCommentBody({}, repoInfo, byFile, [], makeCbs());
    expect(typeof result).toBe('string');
    expect(result.length).toBeGreaterThan(0);
  });
});
