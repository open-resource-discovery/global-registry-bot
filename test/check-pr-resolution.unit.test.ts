/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, jest } from '@jest/globals';
import {
  readCheckRunFromPayload,
  readCheckRunPrNumbers,
  readCheckSuiteFromPayload,
  readCheckSuiteId,
  readCheckSuitePrNumbers,
  resolveCheckSuitePrNumbers,
} from '../src/handlers/request/application/check-pr-resolution.js';

const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v);

const cbs = { isPlainObject };

// ---------------------------------------------------------------------------
// readCheckRunFromPayload
// ---------------------------------------------------------------------------

describe('readCheckRunFromPayload', () => {
  it('returns null when payload is not a plain object (L52 true arm)', () => {
    expect(readCheckRunFromPayload(null, cbs)).toBeNull();
    expect(readCheckRunFromPayload('string', cbs)).toBeNull();
    expect(readCheckRunFromPayload(42, cbs)).toBeNull();
  });

  it('returns null when check_run is not a plain object', () => {
    expect(readCheckRunFromPayload({ check_run: null }, cbs)).toBeNull();
    expect(readCheckRunFromPayload({ check_run: 'bad' }, cbs)).toBeNull();
  });

  it('returns check_run when payload and check_run are plain objects', () => {
    const run = { id: 1 };
    expect(readCheckRunFromPayload({ check_run: run }, cbs)).toBe(run);
  });
});

// ---------------------------------------------------------------------------
// readCheckRunPrNumbers
// ---------------------------------------------------------------------------

describe('readCheckRunPrNumbers', () => {
  it('returns empty array when run is null (L61 false arm of Array.isArray)', () => {
    expect(readCheckRunPrNumbers(null)).toEqual([]);
  });

  it('returns empty array when pull_requests is null (L61 false arm)', () => {
    expect(readCheckRunPrNumbers({ pull_requests: null } as any)).toEqual([]);
  });

  it('skips pr entries where number is not a finite number (L66 false arm)', () => {
    expect(
      readCheckRunPrNumbers({ pull_requests: [{ number: 'x' }, { number: NaN }, { number: null }] } as any)
    ).toEqual([]);
  });

  it('deduplicates valid pr numbers', () => {
    const result = readCheckRunPrNumbers({ pull_requests: [{ number: 5 }, { number: 5 }, { number: 3 }] } as any);
    expect(result.sort((a, b) => a - b)).toEqual([3, 5]);
  });
});

// ---------------------------------------------------------------------------
// readCheckSuiteFromPayload
// ---------------------------------------------------------------------------

describe('readCheckSuiteFromPayload', () => {
  it('returns null when payload is not a plain object (L76 true arm)', () => {
    expect(readCheckSuiteFromPayload(null, cbs)).toBeNull();
    expect(readCheckSuiteFromPayload([], cbs)).toBeNull();
  });

  it('returns suite when both payload and check_suite are valid', () => {
    const suite = { id: 99 };
    expect(readCheckSuiteFromPayload({ check_suite: suite }, cbs)).toBe(suite);
  });
});

// ---------------------------------------------------------------------------
// readCheckSuiteId
// ---------------------------------------------------------------------------

describe('readCheckSuiteId', () => {
  it('returns null for null suite', () => {
    expect(readCheckSuiteId(null)).toBeNull();
  });

  it('returns null when id is NaN', () => {
    expect(readCheckSuiteId({ id: NaN } as any)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// readCheckSuitePrNumbers
// ---------------------------------------------------------------------------

describe('readCheckSuitePrNumbers', () => {
  it('returns empty array when suite is null (L92 false arm of ternary)', () => {
    expect(readCheckSuitePrNumbers(null)).toEqual([]);
  });

  it('returns empty array when pull_requests is null (L92 false arm)', () => {
    expect(readCheckSuitePrNumbers({ pull_requests: null } as any)).toEqual([]);
  });

  it('skips entries where number is not a finite number (L96 false arm)', () => {
    expect(readCheckSuitePrNumbers({ pull_requests: [{ number: 'bad' }, {}] } as any)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// resolveCheckSuitePrNumbers
// ---------------------------------------------------------------------------

describe('resolveCheckSuitePrNumbers', () => {
  const repoInfo = { owner: 'org', repo: 'r' };

  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  function makeCallbacks(overrides: Record<string, any> = {}) {
    return {
      isPlainObject,
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({ data: [] }),
      listPulls: jest.fn().mockResolvedValue({ data: [] }),
      ...overrides,
    } as any;
  }

  it('returns empty early when headSha is empty string', async () => {
    const callbacks = makeCallbacks();
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, '', callbacks);
    expect(result).toEqual([]);
    expect(callbacks.listPullRequestsAssociatedWithCommit).not.toHaveBeenCalled();
  });

  it('handles non-array data from listPullRequestsAssociatedWithCommit (L128 false arm)', async () => {
    const callbacks = makeCallbacks({
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({ data: 'not-array' }),
    });
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, 'abc123', callbacks);
    expect(result).toEqual([]);
  });

  it('skips non-plain-object items from commit API response (L132 true arm)', async () => {
    const callbacks = makeCallbacks({
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({ data: ['string', null, 42] }),
    });
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, 'sha1', callbacks);
    expect(result).toEqual([]);
  });

  it('skips items where number is not finite (L138 true arm)', async () => {
    const callbacks = makeCallbacks({
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({
        data: [
          { state: 'open', number: 'bad' },
          { state: 'open', number: null },
        ],
      }),
    });
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, 'sha2', callbacks);
    expect(result).toEqual([]);
  });

  it('falls to listPulls when fromCommit empty (L144 false), handles falsy data (L161)', async () => {
    const callbacks = makeCallbacks({
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({ data: [] }),
      listPulls: jest.fn().mockResolvedValue({ data: undefined }),
    });
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, 'sha3', callbacks);
    expect(result).toEqual([]);
  });

  it('skips listPulls entries with sha mismatch (L166 true arm)', async () => {
    const callbacks = makeCallbacks({
      listPullRequestsAssociatedWithCommit: jest.fn().mockResolvedValue({ data: [] }),
      listPulls: jest.fn().mockResolvedValue({
        data: [{ head: { sha: 'different' }, number: 99 }],
      }),
    });
    const result = await resolveCheckSuitePrNumbers({}, repoInfo, null, 'target-sha', callbacks);
    expect(result).toEqual([]);
  });
});
