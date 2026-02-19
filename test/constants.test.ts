import { beforeAll, describe, expect, it } from '@jest/globals';

type Mod = typeof import('../src/handlers/request/constants.js');
let mod: Mod;

beforeAll(async () => {
  mod = await import('../src/handlers/request/constants.js');
});

describe('src/handlers/request/constants.ts', () => {
  it('exports compatibility constants with expected defaults', () => {
    expect(mod.LABEL_AUTHOR).toBeNull();
    expect(mod.LABEL_REVIEW).toBeNull();
    expect(mod.LABEL_CPA).toBeNull();
    expect(mod.DOC_LINKS).toBeNull();

    expect(Array.isArray(mod.DEFAULT_APPROVERS)).toBe(true);
    expect(mod.DEFAULT_APPROVERS).toEqual([]);

    expect(Array.isArray(mod.CPA_ASSIGNEES)).toBe(true);
    expect(mod.CPA_ASSIGNEES).toEqual([]);

    expect(mod.NA_ALLOWED instanceof Set).toBe(true);
    expect(Array.from(mod.NA_ALLOWED)).toEqual([]);
  });

  describe('getStateLabelsFromConfig', () => {
    it('returns trimmed author/review labels when configured', () => {
      const cfg = {
        workflow: {
          labels: {
            authorAction: '  state:author  ',
            approverAction: 'state:review',
          },
        },
      };

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getStateLabelsFromConfig(cfg as any)).toEqual({
        author: 'state:author',
        review: 'state:review',
      });
    });

    it('returns null for missing/empty/non-string labels', () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getStateLabelsFromConfig(undefined as any)).toEqual({
        author: null,
        review: null,
      });

      const cfg = {
        workflow: {
          labels: {
            authorAction: '   ', // empty -> null
            approverAction: 123, // non-string -> null
          },
        },
      };

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getStateLabelsFromConfig(cfg as any)).toEqual({
        author: null,
        review: null,
      });
    });
  });

  describe('getApproversFromConfig', () => {
    it('returns [] when workflow.approvers is not an array', () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getApproversFromConfig(undefined as any)).toEqual([]);

      const cfg = { workflow: { approvers: 'nope' } };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getApproversFromConfig(cfg as any)).toEqual([]);
    });

    it('trims entries and drops empty ones', () => {
      const cfg = {
        workflow: {
          approvers: [' ap1 ', ' ', 'ap2', 123, null],
        },
      };

      // note: String(123) => "123", String(null) => "null"
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getApproversFromConfig(cfg as any)).toEqual(['ap1', 'ap2', '123', 'null']);
    });
  });

  describe('getDocLinksFromConfig', () => {
    it('returns docs string as-is when non-empty after trim', () => {
      const cfg = {
        workflow: {
          links: {
            docs: '  https://example.com/docs  ',
          },
        },
      };

      // function returns original string, not trimmed
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getDocLinksFromConfig(cfg as any)).toBe('  https://example.com/docs  ');
    });

    it('returns empty string when docs missing/empty/non-string', () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getDocLinksFromConfig(undefined as any)).toBe('');

      const cfg1 = { workflow: { links: { docs: '   ' } } };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getDocLinksFromConfig(cfg1 as any)).toBe('');

      const cfg2 = { workflow: { links: { docs: 42 } } };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(mod.getDocLinksFromConfig(cfg2 as any)).toBe('');
    });
  });

  it('DEFAULT_CONFIG has expected shape / null defaults', () => {
    expect(mod.DEFAULT_CONFIG).toBeTruthy();

    expect(mod.DEFAULT_CONFIG.requests).toEqual({});

    expect(mod.DEFAULT_CONFIG.pr).toBeTruthy();
    expect(mod.DEFAULT_CONFIG.pr?.branchNameTemplate).toBeNull();
    expect(mod.DEFAULT_CONFIG.pr?.titleTemplate).toBeNull();
    expect(mod.DEFAULT_CONFIG.pr?.commitMessageTemplate).toBeNull();
    expect(mod.DEFAULT_CONFIG.pr?.autoMerge).toEqual({ enabled: null, method: null });

    expect(mod.DEFAULT_CONFIG.workflow).toBeTruthy();
    expect(mod.DEFAULT_CONFIG.workflow?.labels).toEqual({
      global: null,
      authorAction: null,
      approverAction: null,
      approvalRequested: null,
      approvalSuccessful: null,
      approvalRejected: null,
      autoMergeCandidate: null,
    });
    expect(mod.DEFAULT_CONFIG.workflow?.approvers).toBeNull();
    expect(mod.DEFAULT_CONFIG.workflow?.links).toEqual({ docs: null });
  });

  it('STATIC_CONFIG_SCHEMA exposes key schema invariants', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const s = mod.STATIC_CONFIG_SCHEMA as any;

    expect(s).toBeTruthy();
    expect(s.type).toBe('object');
    expect(s.additionalProperties).toBe(false);
    expect(s.required).toContain('requests');

    // requests patternProperties exists
    expect(s.properties?.requests?.patternProperties).toBeTruthy();
    expect(s.properties.requests.patternProperties['^.+$']).toBeTruthy();

    // pr.autoMerge.method enum contains both REST and GraphQL variants + null
    const methodEnum = s.properties?.pr?.properties?.autoMerge?.properties?.method?.enum ?? [];
    expect(methodEnum).toEqual(
      expect.arrayContaining(['merge', 'squash', 'rebase', 'MERGE', 'SQUASH', 'REBASE', null])
    );
  });
});
