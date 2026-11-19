/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect } from '@jest/globals';
import {
  dedupe,
  getValueAtInstancePath,
  filterNoisyOneOfTypeErrors,
  normalizeAjvMessage,
} from '../src/handlers/request/validation/ajv-error-postprocessing.js';

const helpers = {
  toStringSafe: (v: unknown): string => (v === null || v === undefined ? '' : String(v)),
  isPlainObject: (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v),
};

// ---------------------------------------------------------------------------
// dedupe
// ---------------------------------------------------------------------------

describe('dedupe', () => {
  it('deduplicates and trims strings', () => {
    expect(dedupe(['a', ' a ', 'b'])).toEqual(['a', 'b']);
  });

  it('returns empty array for non-array input', () => {
    expect(dedupe(null)).toEqual([]);
    expect(dedupe(undefined)).toEqual([]);
    expect(dedupe('string')).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// getValueAtInstancePath — array traversal (L33-35) + primitive mid-path (L43)
// ---------------------------------------------------------------------------

describe('getValueAtInstancePath', () => {
  it('returns obj when path is empty', () => {
    expect(getValueAtInstancePath({ a: 1 }, '', helpers)).toEqual({ a: 1 });
  });

  it('returns obj when path is single slash', () => {
    expect(getValueAtInstancePath({ a: 1 }, '/', helpers)).toEqual({ a: 1 });
  });

  it('navigates plain-object keys', () => {
    expect(getValueAtInstancePath({ a: { b: 42 } }, '/a/b', helpers)).toBe(42);
  });

  it('navigates array by numeric index (L33-35 array traversal arm)', () => {
    const arr = ['x', 'y', 'z'];
    expect(getValueAtInstancePath(arr, '/1', helpers)).toBe('y');
  });

  it('navigates nested array index inside object', () => {
    const obj = { items: ['first', 'second'] };
    expect(getValueAtInstancePath(obj, '/items/0', helpers)).toBe('first');
  });

  it('returns undefined for out-of-bounds array index', () => {
    expect(getValueAtInstancePath([1, 2], '/5', helpers)).toBeUndefined();
  });

  it('returns undefined when intermediate value is a primitive string (L43 return undefined)', () => {
    const obj = { name: 'string-value' };
    expect(getValueAtInstancePath(obj, '/name/nested', helpers)).toBeUndefined();
  });

  it('returns undefined when intermediate value is a number (L43)', () => {
    expect(getValueAtInstancePath({ n: 42 }, '/n/prop', helpers)).toBeUndefined();
  });

  it('returns undefined when cur is null mid-path (L31)', () => {
    expect(getValueAtInstancePath({ a: null }, '/a/b', helpers)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// filterNoisyOneOfTypeErrors
// ---------------------------------------------------------------------------

describe('filterNoisyOneOfTypeErrors', () => {
  it('returns all errors when no type-string suppression applies', () => {
    const errs = [
      { keyword: 'required', instancePath: '', message: 'required' },
      { keyword: 'pattern', instancePath: '/title', message: 'pattern fail' },
    ];
    const result = filterNoisyOneOfTypeErrors(errs, { title: 'val' }, helpers);
    expect(result).toHaveLength(2);
  });

  it('suppresses type:string error when value at path is array and a specific error exists there', () => {
    const candidate = { tags: ['a', 'b'] };
    const errs = [
      {
        keyword: 'type',
        instancePath: '/tags',
        message: 'must be string',
        params: { type: 'string' },
      },
      {
        keyword: 'minItems',
        instancePath: '/tags',
        message: 'must have at least 2 items',
        params: {},
      },
    ];
    const result = filterNoisyOneOfTypeErrors(errs, candidate, helpers);
    expect(result.some((e) => e.keyword === 'type' && e.instancePath === '/tags')).toBe(false);
    expect(result.some((e) => e.keyword === 'minItems')).toBe(true);
  });

  it('keeps type:string error when value at path is not an array', () => {
    const candidate = { title: 'hello' };
    const errs = [
      {
        keyword: 'type',
        instancePath: '/title',
        message: 'must be string',
        params: { type: 'string' },
      },
      {
        keyword: 'pattern',
        instancePath: '/title',
        message: 'pattern fail',
        params: {},
      },
    ];
    const result = filterNoisyOneOfTypeErrors(errs, candidate, helpers);
    expect(result.some((e) => e.keyword === 'type')).toBe(true);
  });

  it('returns empty array for non-array input', () => {
    expect(filterNoisyOneOfTypeErrors(null, {}, helpers)).toEqual([]);
    expect(filterNoisyOneOfTypeErrors('not-array', {}, helpers)).toEqual([]);
  });

  it('ignores non-plain-object entries in ajvErrs', () => {
    const errs = [null, 'string', { keyword: 'required', instancePath: '' }] as any;
    const result = filterNoisyOneOfTypeErrors(errs, {}, helpers);
    expect(result).toHaveLength(1);
    expect(result[0].keyword).toBe('required');
  });

  it('L64 arm1: undefined keyword treated as empty string — not added to hasSpecificErrorAtPath', () => {
    const candidate = { tags: ['a', 'b'] };
    const errs = [
      { keyword: 'type', instancePath: '/tags', message: 'must be string', params: { type: 'string' } },
      { instancePath: '/tags', message: 'companion error with no keyword' } as any,
    ];
    const result = filterNoisyOneOfTypeErrors(errs, candidate, helpers);
    expect(result.some((e: any) => e.keyword === 'type')).toBe(true);
  });

  it('L73 arm1: type:string error with empty instancePath uses empty-string path', () => {
    const candidate = { title: 'hello' };
    const errs = [{ keyword: 'type', instancePath: '', message: 'must be string', params: { type: 'string' } }];
    const result = filterNoisyOneOfTypeErrors(errs, candidate, helpers);
    expect(result.some((e: any) => e.keyword === 'type')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// normalizeAjvMessage
// ---------------------------------------------------------------------------

describe('normalizeAjvMessage', () => {
  it('returns empty string for empty input', () => {
    expect(normalizeAjvMessage('', helpers)).toBe('');
    expect(normalizeAjvMessage(null, helpers)).toBe('');
  });

  it('uppercases RFC keywords and capitalizes first char', () => {
    expect(normalizeAjvMessage('must be a string', helpers)).toBe('MUST be a string');
    expect(normalizeAjvMessage('should not be empty', helpers)).toBe('SHOULD NOT be empty');
  });

  it('capitalizes first letter of result', () => {
    expect(normalizeAjvMessage('required field missing', helpers)).toMatch(/^REQUIRED/);
  });
});
