/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect } from '@jest/globals';
import {
  isPlainObject,
  asError,
  getHttpStatus,
  getErrorMessage,
} from '../src/handlers/request/infrastructure/errors.js';
import {
  readRepoInfoFromPayload,
  readDefaultBranchFromPayload,
  readDefaultBranchFromPush,
  readPushChangedFiles,
  readPayloadLabelName,
} from '../src/handlers/request/infrastructure/request-context.js';

// ---------------------------------------------------------------------------
// errors.ts
// ---------------------------------------------------------------------------

describe('isPlainObject', () => {
  it('returns true for plain objects', () => {
    expect(isPlainObject({})).toBe(true);
    expect(isPlainObject({ a: 1 })).toBe(true);
  });

  it('returns false for null', () => {
    expect(isPlainObject(null)).toBe(false);
  });

  it('returns false for arrays', () => {
    expect(isPlainObject([])).toBe(false);
    expect(isPlainObject([1, 2])).toBe(false);
  });

  it('returns false for primitives', () => {
    expect(isPlainObject('str')).toBe(false);
    expect(isPlainObject(42)).toBe(false);
    expect(isPlainObject(true)).toBe(false);
    expect(isPlainObject(undefined)).toBe(false);
  });
});

describe('asError', () => {
  it('returns the same Error when value is already an Error', () => {
    const e = new Error('original');
    expect(asError(e)).toBe(e);
  });

  it('wraps non-Error values in a new Error', () => {
    expect(asError('oops').message).toBe('oops');
    expect(asError(42).message).toBe('42');
    expect(asError(null).message).toBe('null');
  });
});

describe('getHttpStatus', () => {
  it('returns the numeric status from a plain-object error', () => {
    expect(getHttpStatus({ status: 404 })).toBe(404);
    expect(getHttpStatus({ status: 500 })).toBe(500);
  });

  it('returns undefined for non-plain-object errors', () => {
    expect(getHttpStatus(new Error('x'))).toBeUndefined();
    expect(getHttpStatus(null)).toBeUndefined();
    expect(getHttpStatus('string')).toBeUndefined();
  });

  it('returns undefined when status is not a number', () => {
    expect(getHttpStatus({ status: '404' })).toBeUndefined();
    expect(getHttpStatus({ status: null })).toBeUndefined();
    expect(getHttpStatus({})).toBeUndefined();
  });
});

describe('getErrorMessage', () => {
  it('returns message from Error instances', () => {
    expect(getErrorMessage(new Error('boom'))).toBe('boom');
  });

  it('converts non-Error values to string', () => {
    expect(getErrorMessage('raw string')).toBe('raw string');
    expect(getErrorMessage(42)).toBe('42');
    expect(getErrorMessage(null)).toBe('null');
  });
});

// ---------------------------------------------------------------------------
// request-context.ts
// ---------------------------------------------------------------------------

const validPayload = {
  repository: {
    name: 'my-repo',
    owner: { login: 'my-org' },
    default_branch: 'main',
  },
};

describe('readRepoInfoFromPayload', () => {
  it('returns RepoInfo from a valid payload', () => {
    const info = readRepoInfoFromPayload(validPayload);
    expect(info).toEqual({ owner: 'my-org', repo: 'my-repo' });
  });

  it('returns null when payload is not a plain object', () => {
    expect(readRepoInfoFromPayload(null)).toBeNull();
    expect(readRepoInfoFromPayload('string')).toBeNull();
    expect(readRepoInfoFromPayload(42)).toBeNull();
  });

  it('returns null when repository is missing or not a plain object', () => {
    expect(readRepoInfoFromPayload({})).toBeNull();
    expect(readRepoInfoFromPayload({ repository: 'not-an-object' })).toBeNull();
  });

  it('returns null when owner is not a plain object', () => {
    expect(readRepoInfoFromPayload({ repository: { name: 'repo', owner: 'not-obj' } })).toBeNull();
  });

  it('returns null when repoName or ownerLogin is empty', () => {
    expect(readRepoInfoFromPayload({ repository: { name: '', owner: { login: 'org' } } })).toBeNull();
    expect(readRepoInfoFromPayload({ repository: { name: 'repo', owner: { login: '' } } })).toBeNull();
  });
});

describe('readDefaultBranchFromPayload', () => {
  it('returns default_branch from a valid payload', () => {
    expect(readDefaultBranchFromPayload(validPayload)).toBe('main');
  });

  it('returns empty string when payload is not a plain object', () => {
    expect(readDefaultBranchFromPayload(null)).toBe('');
    expect(readDefaultBranchFromPayload('string')).toBe('');
  });

  it('returns empty string when repository is missing or not a plain object', () => {
    expect(readDefaultBranchFromPayload({})).toBe('');
    expect(readDefaultBranchFromPayload({ repository: null })).toBe('');
  });
});

describe('readDefaultBranchFromPush', () => {
  it('delegates to readDefaultBranchFromPayload', () => {
    expect(readDefaultBranchFromPush(validPayload)).toBe('main');
    expect(readDefaultBranchFromPush(null)).toBe('');
  });
});

describe('readPushChangedFiles', () => {
  it('returns empty array for non-plain payload', () => {
    expect(readPushChangedFiles(null)).toEqual([]);
    expect(readPushChangedFiles('str')).toEqual([]);
  });

  it('returns empty array when commits is missing or not an array', () => {
    expect(readPushChangedFiles({})).toEqual([]);
    expect(readPushChangedFiles({ commits: 'not-array' })).toEqual([]);
  });

  it('collects added/modified/removed files from commits', () => {
    const payload = {
      commits: [
        { added: ['a.yaml', 'b.yaml'], modified: [], removed: [] },
        { added: [], modified: ['c.yaml'], removed: ['d.yaml'] },
      ],
    };
    const files = readPushChangedFiles(payload);
    expect(files).toEqual(['a.yaml', 'b.yaml', 'c.yaml', 'd.yaml']);
  });

  it('deduplicates files appearing in multiple commits', () => {
    const payload = {
      commits: [
        { added: ['same.yaml'], modified: [], removed: [] },
        { added: ['same.yaml'], modified: [], removed: [] },
      ],
    };
    expect(readPushChangedFiles(payload)).toEqual(['same.yaml']);
  });

  it('skips non-plain commits', () => {
    const payload = { commits: [null, 'str', { added: ['ok.yaml'], modified: [], removed: [] }] };
    expect(readPushChangedFiles(payload as any)).toEqual(['ok.yaml']);
  });

  it('normalizes backslashes and skips empty normalized paths', () => {
    const payload = {
      commits: [{ added: ['path\\to\\file.yaml', '   '], modified: [], removed: [] }],
    };
    const files = readPushChangedFiles(payload);
    expect(files).toContain('path/to/file.yaml');
    expect(files.filter((f) => !f)).toHaveLength(0);
  });

  it('handles non-array file arrays within commit keys', () => {
    const payload = {
      commits: [{ added: 'not-array', modified: ['ok.yaml'], removed: [] }],
    };
    expect(readPushChangedFiles(payload as any)).toEqual(['ok.yaml']);
  });
});

describe('readPayloadLabelName', () => {
  it('returns empty string for non-plain payload', () => {
    expect(readPayloadLabelName(null)).toBe('');
    expect(readPayloadLabelName('str')).toBe('');
  });

  it('returns string label directly (trimmed)', () => {
    expect(readPayloadLabelName({ label: '  my-label  ' })).toBe('my-label');
  });

  it('returns name from a plain-object label', () => {
    expect(readPayloadLabelName({ label: { name: 'obj-label' } })).toBe('obj-label');
  });

  it('returns empty string when label is neither string nor plain object', () => {
    expect(readPayloadLabelName({ label: 42 })).toBe('');
    expect(readPayloadLabelName({ label: null })).toBe('');
    expect(readPayloadLabelName({})).toBe('');
  });
});
