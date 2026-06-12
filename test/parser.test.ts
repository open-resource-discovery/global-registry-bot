import { parseForm } from '../src/utils/parser.js';

const template = {
  body: [
    { id: 'productId', attributes: { label: 'Product ID' } },
    { id: 'description', attributes: { label: 'Description' } },
    { id: 'notes', attributes: { label: 'Notes' } },
    { id: 'naive', attributes: { label: 'Náïve Label' } },
  ],
};

test('parses headings and trims values', () => {
  const body = `
## Product ID
  ABC-123

## Description
Hello

World
`;
  expect(parseForm(body, template)).toEqual({
    productId: 'ABC-123',
    description: 'Hello\n\nWorld',
  });
});

test('extracts from code fences', () => {
  const body = `
## Description
\`\`\`yaml
foo: bar
baz: 1
\`\`\`
`;
  expect(parseForm(body, template)).toEqual({
    description: 'foo: bar\nbaz: 1',
  });
});

test('strips quotes, bullets and checkboxes', () => {
  const body = `
## Notes
> quoted
- [x] done
- plain
:
-
`;
  expect(parseForm(body, template)).toEqual({
    notes: 'quoted\ndone\nplain',
  });
});

test('skips "no response"', () => {
  const body = `
## Description
_no response_
`;
  expect(parseForm(body, template)).toEqual({});
});

test('matches heading containing label + id', () => {
  const body = `
## Product ID productId
XYZ
`;
  expect(parseForm(body, template)).toEqual({
    productId: 'XYZ',
  });
});

test('falls back to Key: Value scan with multiline values', () => {
  const body = `
Product ID: ABC
Description:
first
second
Unrelated: ignore
`;
  expect(parseForm(body, template)).toEqual({
    productId: 'ABC',
    description: 'first\nsecond',
  });
});

test('normalizes diacritics in template labels', () => {
  const body = `
## Naive Label
ok
`;
  expect(parseForm(body, template)).toEqual({
    naive: 'ok',
  });
});

test('removes HTML comments before parsing', () => {
  const body = `
## Description
Hello
<!-- hidden -->
World
`;
  expect(parseForm(body, template)).toEqual({
    description: 'Hello\n\nWorld',
  });
});

// L37 arm0 + L109 arm1: extractValue('') returns '' early → fallbackKvScan skips empty value
test('fallback KV: key with no inline value followed by another KV skips empty', () => {
  // No headings → triggers fallbackKvScan; 'Product ID:' has empty m[2] and next line is a KV
  // → multiline skipped → value='' → extractValue('') hits L37 and returns '' → L109 arm1 skips
  expect(parseForm('Product ID:\nDescription: hello', template)).toEqual({
    description: 'hello',
  });
});

// L25 arm0: sanitizeScalar receives '' because section content is a lone bullet '-'
// extractValue('-') → after /^\s*[:-]\s*$/ strip → '' → sanitizeScalar('') → !s → ''
test('section whose only content is a bare bullet is ignored (L25 arm0)', () => {
  expect(parseForm('## Description\n-\n', template)).toEqual({});
});

// L26 arm0: sanitizeScalar receives 'no response' after bullet prefix stripped by extractValue
// '- no response' passes L38 (has leading '- '), then bullet strip → 'no response' → sanitizeScalar hits L26
test('section "- no response" after bullet-strip triggers sanitizeScalar no-response check (L26 arm0)', () => {
  expect(parseForm('## Description\n- no response\n', template)).toEqual({});
});

// L53 arm1: template?.body || [] — template without body field
test('template without body returns empty result (L53 arm1)', () => {
  expect(parseForm('## Description\nhello\n', {})).toEqual({});
});

// L56 arm1: f?.attributes?.label || f.id — field without attributes uses id as label
test('field without attributes label falls back to id as heading key (L56 arm1)', () => {
  const tmpl = { body: [{ id: 'notes' }] };
  expect(parseForm('## notes\nhello\n', tmpl)).toEqual({ notes: 'hello' });
});
