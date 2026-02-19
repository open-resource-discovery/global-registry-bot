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
