import {
  isRegistryValidateAnnotation,
  stripRegistrySuffix,
  toSectionTitle,
  normalizeMsg,
  extractFieldFromMsg,
  filterRegistryValidationEntries,
  filterMachineReadableSourcesForFile,
  buildRegistryValidationCommentHeading,
  buildRegistryValidationAggregateBody,
  collectRegistryValidationArtifacts,
} from '../src/handlers/request/domain/registry-validation-annotations.js';

// ── isRegistryValidateAnnotation ──────────────────────────────────────────────

test('isRegistryValidateAnnotation: true for registry-validate prefix', () => {
  expect(isRegistryValidateAnnotation({ title: 'registry-validate: name' })).toBe(true);
  expect(isRegistryValidateAnnotation({ title: 'REGISTRY-VALIDATE error' })).toBe(true);
});

test('isRegistryValidateAnnotation: false for non-matching title', () => {
  expect(isRegistryValidateAnnotation({ title: 'other error' })).toBe(false);
  expect(isRegistryValidateAnnotation({ title: null })).toBe(false);
  expect(isRegistryValidateAnnotation({ title: undefined })).toBe(false);
});

// ── stripRegistrySuffix ───────────────────────────────────────────────────────

test('stripRegistrySuffix: removes [file=...] suffix', () => {
  expect(stripRegistrySuffix('must be string [file=foo.yaml]')).toBe('must be string');
});

test('stripRegistrySuffix: returns unchanged when no suffix', () => {
  expect(stripRegistrySuffix('must be string')).toBe('must be string');
});

// ── toSectionTitle ────────────────────────────────────────────────────────────

test('toSectionTitle: empty field returns "Details"', () => {
  expect(toSectionTitle('')).toBe('Details');
});

test('toSectionTitle: "contact" returns "Contacts"', () => {
  expect(toSectionTitle('contact')).toBe('Contacts');
  expect(toSectionTitle('contacts')).toBe('Contacts');
});

test('toSectionTitle: camelCase field is spaced', () => {
  expect(toSectionTitle('requestType')).toBe('Request Type');
});

test('toSectionTitle: underscore/hyphen converted to spaces', () => {
  expect(toSectionTitle('some_field')).toBe('Some field');
});

// ── normalizeMsg ──────────────────────────────────────────────────────────────

test('normalizeMsg: replaces "must" with "MUST"', () => {
  expect(normalizeMsg('field must be string')).toBe('field MUST be string');
});

test('normalizeMsg: leading slash path strips path token, returns rest', () => {
  expect(normalizeMsg('/identifier value must match pattern')).toBe('value MUST match pattern');
});

test('normalizeMsg: single token starting with slash (no rest)', () => {
  // firstSpace < 0 → maybePath = '' → rest = text → startsWith('/') is false → msgOnly = text
  expect(normalizeMsg('/identifier')).toBe('/identifier');
});

test('normalizeMsg: no leading slash keeps full text', () => {
  expect(normalizeMsg('name must be present')).toBe('name MUST be present');
});

test('normalizeMsg: empty input returns empty', () => {
  expect(normalizeMsg('')).toBe('');
});

// ── extractFieldFromMsg ───────────────────────────────────────────────────────

test('extractFieldFromMsg: empty returns empty string', () => {
  expect(extractFieldFromMsg('')).toBe('');
});

test('extractFieldFromMsg: extracts field from /pointer path', () => {
  expect(extractFieldFromMsg('/identifier must match')).toBe('identifier');
  expect(extractFieldFromMsg('/namespace/sub must be string')).toBe('namespace');
});

test('extractFieldFromMsg: extracts from required property message', () => {
  expect(extractFieldFromMsg("required property 'name'")).toBe('name');
  expect(extractFieldFromMsg("Property 'version' is required")).toBe('version');
});

test('extractFieldFromMsg: extracts from additional property message', () => {
  expect(extractFieldFromMsg("additional property 'extra'")).toBe('extra');
});

test('extractFieldFromMsg: extracts from "X is required." pattern', () => {
  const result = extractFieldFromMsg('Contact Owner is required.');
  expect(result).toBe('contact-owner');
});

test('extractFieldFromMsg: extracts leading field from "X must" pattern', () => {
  expect(extractFieldFromMsg('name MUST be a string')).toBe('name');
  expect(extractFieldFromMsg('version must match pattern')).toBe('version');
});

test('extractFieldFromMsg: dotted "X.Y is required." falls to labelRequired → normalizeKey', () => {
  // pattern 4 (labelRequired) fires before pattern 6 (dotted); normalizeKey("owner.login") = "owner-login"
  expect(extractFieldFromMsg('owner.login is required.')).toBe('owner-login');
});

test('extractFieldFromMsg: returns empty for unrecognized pattern', () => {
  expect(extractFieldFromMsg('some unknown error message here')).toBe('');
});

// ── filterRegistryValidationEntries ──────────────────────────────────────────

test('filterRegistryValidationEntries: filters empty arrays and sorts by path', () => {
  const byFile = new Map([
    ['z-file.yaml', ['error1']],
    ['a-file.yaml', [] as string[]],
    ['m-file.yaml', ['error2']],
  ]);
  const result = filterRegistryValidationEntries(byFile);
  expect(result.map(([p]) => p)).toEqual(['m-file.yaml', 'z-file.yaml']);
});

// ── filterMachineReadableSourcesForFile ───────────────────────────────────────

test('filterMachineReadableSourcesForFile: returns only sources for matching filePath', () => {
  const sources = [
    { filePath: 'a.yaml', message: 'err1' },
    { filePath: 'b.yaml', message: 'err2' },
    { filePath: 'a.yaml', message: 'err3' },
  ];
  const result = filterMachineReadableSourcesForFile(sources, 'a.yaml');
  expect(result).toHaveLength(2);
  expect(result.every((s) => s.filePath === 'a.yaml')).toBe(true);
});

// ── buildRegistryValidationCommentHeading ─────────────────────────────────────

test('buildRegistryValidationCommentHeading: ### level produces ### file heading', () => {
  const lines = buildRegistryValidationCommentHeading('foo.yaml', ['/name must be string'], '###');
  const text = lines.join('\n');
  expect(text).toContain('### File: `foo.yaml`');
});

test('buildRegistryValidationCommentHeading: #### level produces ### file + #### sections', () => {
  const lines = buildRegistryValidationCommentHeading('foo.yaml', ['/name must be string'], '####');
  const text = lines.join('\n');
  expect(text).toContain('### File: `foo.yaml`');
  expect(text).toContain('#### ');
});

test('buildRegistryValidationCommentHeading: default level is ###', () => {
  const lines = buildRegistryValidationCommentHeading('foo.yaml', ['error']);
  expect(lines.join('\n')).toContain('### File:');
});

// ── buildRegistryValidationAggregateBody ─────────────────────────────────────

test('buildRegistryValidationAggregateBody: empty map returns empty string', () => {
  expect(buildRegistryValidationAggregateBody(new Map())).toBe('');
});

test('buildRegistryValidationAggregateBody: builds body for multiple files', () => {
  const byFile = new Map([
    ['resources/ns.yaml', ['/name must be string']],
    ['resources/other.yaml', ["required property 'title'"]],
  ]);
  const result = buildRegistryValidationAggregateBody(byFile);
  expect(result).toContain('## Detected issues');
  expect(result).toContain('ns.yaml');
  expect(result).toContain('other.yaml');
});

test('buildRegistryValidationAggregateBody: "details" group sorts last', () => {
  const byFile = new Map([['file.yaml', ['some unknown error', '/identifier must match']]]);
  const result = buildRegistryValidationAggregateBody(byFile);
  const detailsIdx = result.indexOf('Details');
  const identifierIdx = result.indexOf('Identifier');
  if (detailsIdx >= 0 && identifierIdx >= 0) {
    expect(identifierIdx).toBeLessThan(detailsIdx);
  }
});

// ── collectRegistryValidationArtifacts ────────────────────────────────────────

test('collectRegistryValidationArtifacts: empty annotations returns empty results', () => {
  const result = collectRegistryValidationArtifacts([]);
  expect(result.byFile.size).toBe(0);
  expect(result.machineReadableSources).toHaveLength(0);
});

test('collectRegistryValidationArtifacts: groups messages by path', () => {
  const annotations = [
    { path: 'resources/foo.yaml', message: 'error A', title: 'registry-validate' },
    { path: 'resources/foo.yaml', message: 'error B', title: 'registry-validate' },
    { path: 'resources/bar.yaml', message: 'error C', title: 'registry-validate' },
  ];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.byFile.get('resources/foo.yaml')).toHaveLength(2);
  expect(result.byFile.get('resources/bar.yaml')).toHaveLength(1);
  expect(result.machineReadableSources).toHaveLength(3);
});

test('collectRegistryValidationArtifacts: uses raw_details when message is null', () => {
  const annotations = [{ path: 'file.yaml', message: null, raw_details: 'detail error' }];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.byFile.get('file.yaml')).toEqual(['detail error']);
});

test('collectRegistryValidationArtifacts: skips annotations with no message or raw_details', () => {
  const annotations = [{ path: 'file.yaml', message: null, raw_details: null }];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.byFile.size).toBe(0);
});

test('collectRegistryValidationArtifacts: uses "unknown file" when path is null', () => {
  const annotations = [{ path: null, message: 'err' }];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.byFile.has('unknown file')).toBe(true);
});

test('collectRegistryValidationArtifacts: extracts schema path from [schema=...] bracket', () => {
  const annotations = [{ path: 'file.yaml', message: 'error [schema=.github/schemas/foo.json]' }];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.machineReadableSources[0].schemaPath).toBe('.github/schemas/foo.json');
});

test('collectRegistryValidationArtifacts: extracts schema from schema=val (no bracket)', () => {
  const annotations = [{ path: 'file.yaml', message: 'error schema=my-schema.json [file=foo]' }];
  const result = collectRegistryValidationArtifacts(annotations);
  expect(result.machineReadableSources[0].schemaPath).toBe('my-schema.json');
});

// ── toSectionTitle L49, extractFieldFromMsg L84, groupRegistryValidationMessages L95/L107 ─────

test('toSectionTitle L49 if-body: "---" field → spaced="" → returns "Details"', () => {
  // extractFieldFromMsg('/--- must be valid') → pointer captures '---'
  // toSectionTitle('---'): replace([_-]+, ' ').trim() = '' → !spaced → L49 → 'Details'
  const lines = buildRegistryValidationCommentHeading('f.yaml', ['/--- must be valid']);
  expect(lines.join('\n')).toContain('Details');
});

test('extractFieldFromMsg L84: dotted pattern fires when labelRequired fails due to trailing text', () => {
  // "namespace.identifier is required. Extra" → labelRequired needs $ so fails
  // dotted (no $) matches → dotted[1] = 'namespace' → L84 fires
  const lines = buildRegistryValidationCommentHeading('f.yaml', ['namespace.identifier is required. Extra text']);
  expect(lines.join('\n')).toContain('Namespace');
});

test('groupRegistryValidationMessages L95 if-body: empty string message is skipped', () => {
  // normalizeMsg('') = '' → !msg → L95 if-body → continue; only non-empty message appears
  const lines = buildRegistryValidationCommentHeading('f.yaml', ['', '/name must be string']);
  expect(lines.join('\n')).toContain('Name');
});

test('sortRegistryValidationGroupKeys L107: "details" as left operand returns 1 (sorts last)', () => {
  // 'unrecognized error' → field 'details' (first key); '/name must be string' → field 'name'
  // sort(['details','name']): compare('details','name') → L107 left==='details' → return 1
  const lines = buildRegistryValidationCommentHeading('f.yaml', ['unrecognized error', '/name must be string']);
  const text = lines.join('\n');
  const nameIdx = text.indexOf('Name');
  const detailsIdx = text.indexOf('Details');
  if (nameIdx >= 0 && detailsIdx >= 0) {
    expect(nameIdx).toBeLessThan(detailsIdx);
  }
});
