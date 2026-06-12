/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  matchRequestTypesForFile,
  pickRequestTypeForChangedResource,
  resolveRegistryDocResourceName,
  buildFormDataFromRegistryDoc,
} from '../src/handlers/request/domain/direct-pr-resource-mapping.js';

// ── matchRequestTypesForFile ──────────────────────────────────────────────────

test('matchRequestTypesForFile: returns [] when config is null', () => {
  expect(matchRequestTypesForFile(null, 'resources/foo.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: returns [] when config has empty requests', () => {
  expect(matchRequestTypesForFile({ requests: {} as any }, 'resources/foo.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: returns [] when requests is not plain object', () => {
  expect(matchRequestTypesForFile({ requests: 'bad' as any }, 'resources/foo.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: matches when filePath equals folder exactly', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(matchRequestTypesForFile(config, 'resources/products')).toContain('products');
});

test('matchRequestTypesForFile: matches when filePath starts with folder/', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(matchRequestTypesForFile(config, 'resources/products/foo.yaml')).toContain('products');
});

test('matchRequestTypesForFile: no match when path is a different folder', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(matchRequestTypesForFile(config, 'resources/services/bar.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: skips entry without folderName', () => {
  const config = { requests: { bad: {} } } as any;
  expect(matchRequestTypesForFile(config, 'resources/bad/foo.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: skips non-plain-object entry', () => {
  const config = { requests: { bad: 'not-an-object' } } as any;
  expect(matchRequestTypesForFile(config, 'resources/bad/foo.yaml')).toEqual([]);
});

test('matchRequestTypesForFile: normalizes backslashes in filePath', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(matchRequestTypesForFile(config, 'resources\\products\\foo.yaml')).toContain('products');
});

test('matchRequestTypesForFile: normalizes leading slashes in filePath', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(matchRequestTypesForFile(config, '/resources/products/foo.yaml')).toContain('products');
});

// ── pickRequestTypeForChangedResource ─────────────────────────────────────────

test('pickRequestTypeForChangedResource: returns empty string when no candidates', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/services/foo.yaml', {})).toBe('');
});

test('pickRequestTypeForChangedResource: returns single candidate directly', () => {
  const config = { requests: { products: { folderName: 'resources/products' } } } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/products/foo.yaml', {})).toBe('products');
});

test('pickRequestTypeForChangedResource: uses doc.type to disambiguate multiple candidates', () => {
  const config = {
    requests: {
      product: { folderName: 'resources' },
      systemNamespace: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'product' })).toBe('product');
});

test('pickRequestTypeForChangedResource: maps "system" doc type to "systemNamespace"', () => {
  const config = {
    requests: {
      systemNamespace: { folderName: 'resources' },
      product: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'system' })).toBe('systemNamespace');
});

test('pickRequestTypeForChangedResource: maps "authority" to "authorityNamespace"', () => {
  const config = {
    requests: {
      authorityNamespace: { folderName: 'resources' },
      product: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'authority' })).toBe(
    'authorityNamespace'
  );
});

test('pickRequestTypeForChangedResource: maps "subcontext" to "subContextNamespace"', () => {
  const config = {
    requests: {
      subContextNamespace: { folderName: 'resources' },
      product: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'subcontext' })).toBe(
    'subContextNamespace'
  );
});

test('pickRequestTypeForChangedResource: maps "vendor" to "vendor"', () => {
  const config = {
    requests: {
      vendor: { folderName: 'resources' },
      product: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'vendor' })).toBe('vendor');
});

test('pickRequestTypeForChangedResource: returns empty when doc type not in candidates', () => {
  const config = {
    requests: {
      product: { folderName: 'resources' },
      vendor: { folderName: 'resources' },
    },
  } as any;
  expect(pickRequestTypeForChangedResource(config, 'resources/foo.yaml', { type: 'unknown-type' })).toBe('');
});

// ── resolveRegistryDocResourceName ────────────────────────────────────────────

test('resolveRegistryDocResourceName: returns identifier when present', () => {
  expect(resolveRegistryDocResourceName({ identifier: 'sap.core' })).toBe('sap.core');
});

test('resolveRegistryDocResourceName: falls back to namespace', () => {
  expect(resolveRegistryDocResourceName({ namespace: 'sap.core' })).toBe('sap.core');
});

test('resolveRegistryDocResourceName: falls back to product-id', () => {
  expect(resolveRegistryDocResourceName({ 'product-id': 'prod-123' })).toBe('prod-123');
});

test('resolveRegistryDocResourceName: falls back to productId', () => {
  expect(resolveRegistryDocResourceName({ productId: 'prod-456' })).toBe('prod-456');
});

test('resolveRegistryDocResourceName: falls back to id', () => {
  expect(resolveRegistryDocResourceName({ id: 'my-id' })).toBe('my-id');
});

test('resolveRegistryDocResourceName: falls back to name', () => {
  expect(resolveRegistryDocResourceName({ name: 'my-name' })).toBe('my-name');
});

test('resolveRegistryDocResourceName: falls back to vendor', () => {
  expect(resolveRegistryDocResourceName({ vendor: 'sap' })).toBe('sap');
});

test('resolveRegistryDocResourceName: returns empty string when all keys empty', () => {
  expect(resolveRegistryDocResourceName({})).toBe('');
});

// ── buildFormDataFromRegistryDoc ──────────────────────────────────────────────

test('buildFormDataFromRegistryDoc: serializes string value', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'sap.core' });
  expect(out.identifier).toBe('sap.core');
});

test('buildFormDataFromRegistryDoc: serializes number value', () => {
  const out = buildFormDataFromRegistryDoc({ version: 42 });
  expect(out.version).toBe('42');
});

test('buildFormDataFromRegistryDoc: serializes boolean value', () => {
  const out = buildFormDataFromRegistryDoc({ active: true });
  expect(out.active).toBe('true');
});

test('buildFormDataFromRegistryDoc: serializes all-scalar array as newline-joined string', () => {
  const out = buildFormDataFromRegistryDoc({ contacts: ['alice@example.com', 'bob@example.com'] });
  expect(out.contacts).toBe('alice@example.com\nbob@example.com');
});

test('buildFormDataFromRegistryDoc: uses YAML.stringify for mixed array (not all scalars)', () => {
  const out = buildFormDataFromRegistryDoc({ contacts: ['alice', { email: 'bob@example.com' }] });
  expect(typeof out.contacts).toBe('string');
  expect(out.contacts.length).toBeGreaterThan(0);
});

test('buildFormDataFromRegistryDoc: uses YAML.stringify for plain object value', () => {
  const out = buildFormDataFromRegistryDoc({ nested: { key: 'value' } });
  expect(typeof out.nested).toBe('string');
  expect(out.nested).toContain('key');
});

test('buildFormDataFromRegistryDoc: sets identifier and namespace from resourceName when absent', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'sap.core' });
  expect(out.identifier).toBe('sap.core');
  expect(out.namespace).toBe('sap.core');
});

test('buildFormDataFromRegistryDoc: does not override existing identifier or namespace', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'id-val', namespace: 'ns-val' });
  expect(out.identifier).toBe('id-val');
  expect(out.namespace).toBe('ns-val');
});

test('buildFormDataFromRegistryDoc: skips null/undefined values', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: null, active: undefined });
  expect(out).not.toHaveProperty('identifier');
  expect(out).not.toHaveProperty('active');
});

test('buildFormDataFromRegistryDoc: sets name field from doc.name when present', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x', name: 'My Resource' });
  expect(out.name).toBe('My Resource');
});

test('buildFormDataFromRegistryDoc: sets description when present', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x', description: 'A description' });
  expect(out.description).toBe('A description');
});

test('buildFormDataFromRegistryDoc: sets title when present', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x', title: 'A Title' });
  expect(out.title).toBe('A Title');
});

test('buildFormDataFromRegistryDoc: sets vendor when present', () => {
  const out = buildFormDataFromRegistryDoc({ vendor: 'sap' });
  expect(out.vendor).toBe('sap');
});

test('buildFormDataFromRegistryDoc: serializes contact array to newline-joined string', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x', contact: ['alice', 'bob'] });
  expect(out.contact).toBe('alice\nbob');
});

test('buildFormDataFromRegistryDoc: serializes scalar contact string as-is', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x', contact: 'alice@example.com' });
  expect(out.contact).toBe('alice@example.com');
});

test('buildFormDataFromRegistryDoc: does not set contact when value is empty', () => {
  const out = buildFormDataFromRegistryDoc({ identifier: 'x' });
  expect(out).not.toHaveProperty('contact');
});
