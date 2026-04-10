import { addMonthsUtc, parseYmdUtc } from '../src/handlers/request/utils/time.js';

function ymd(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function expectDate(value: Date | null): Date {
  expect(value).not.toBeNull();
  if (value === null) {
    throw new Error('Expected a Date instance');
  }
  return value;
}

test('parseYmdUtc parses a valid YYYY-MM-DD as UTC date', () => {
  const dt = parseYmdUtc('2024-01-05');
  expect(ymd(expectDate(dt))).toBe('2024-01-05');
});

test('parseYmdUtc trims whitespace', () => {
  const dt = parseYmdUtc('  2024-01-05  ');
  expect(ymd(expectDate(dt))).toBe('2024-01-05');
});

test('parseYmdUtc returns null for invalid format', () => {
  expect(parseYmdUtc('2024-1-05')).toBeNull();
  expect(parseYmdUtc('2024/01/05')).toBeNull();
  expect(parseYmdUtc('')).toBeNull();
});

test('parseYmdUtc returns null for invalid calendar date', () => {
  expect(parseYmdUtc('2024-02-30')).toBeNull();
  expect(parseYmdUtc('2023-02-29')).toBeNull();
});

test('parseYmdUtc accepts leap day', () => {
  const dt = parseYmdUtc('2024-02-29');
  expect(ymd(expectDate(dt))).toBe('2024-02-29');
});

test('addMonthsUtc adds months in UTC and preserves day when possible', () => {
  const base = new Date(Date.UTC(2024, 0, 15)); // 2024-01-15
  const out = addMonthsUtc(base, 2);
  expect(ymd(out)).toBe('2024-03-15');
});

test('addMonthsUtc follows JS Date overflow rules (e.g., Jan 31 + 1 month)', () => {
  const base = new Date(Date.UTC(2024, 0, 31)); // 2024-01-31
  const out = addMonthsUtc(base, 1);
  expect(ymd(out)).toBe('2024-03-02'); // Feb 31 overflows into March
});
