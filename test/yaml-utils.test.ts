import { escRe } from '../src/handlers/request/utils/yaml.js';

test('escRe escapes regex metacharacters', () => {
  const s = 'a.*+?^${}()|[]\\b';
  const out = escRe(s);
  expect(out).toBe('a\\.\\*\\+\\?\\^\\$\\{\\}\\(\\)\\|\\[\\]\\\\b');
});

test('escRe default is empty string', () => {
  expect(escRe()).toBe('');
});

test('escaped string can be used as a literal regex', () => {
  const raw = 'hello.(world)?';
  const re = new RegExp(`^${escRe(raw)}$`);
  expect(re.test(raw)).toBe(true);
  expect(re.test('helloXworld')).toBe(false);
});
