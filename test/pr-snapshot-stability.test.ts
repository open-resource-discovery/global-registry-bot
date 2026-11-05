/**
 * Regression tests for the snapshot-hash stability fix.
 *
 * Before the fix, create.ts used a local `readIssueBodyForSnapshot` that stripped
 * routing-lock and parent-approval markers but NOT the contact-approval marker.
 * `buildCompatibleRequestSnapshotHashes` used `readIssueBodyForProcessing` which
 * strips all three. When a contact-approval marker was present in the issue body at
 * PR creation time, the hashes diverged and the PR was falsely closed as "outdated".
 *
 * After the fix, create.ts uses the shared `readIssueBodyForProcessing` so both
 * creation and comparison use the same normalization policy.
 * A legacy compatibility hash using the old normalization is added to the accepted
 * set so PRs created by the buggy code are preserved when the request is unchanged
 * (subject to the limitation described at the bottom of this file).
 */

/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */
/* eslint-disable require-await */

import { beforeAll, describe, it, expect, jest } from '@jest/globals';

// ---------------------------------------------------------------------------
// Module-level mock for categoryFromTemplate (used by snapshot.ts internally).
// We pin it to return 'system' so hashes in this file are deterministic and
// match what the bot produces for System Namespace requests.
// ---------------------------------------------------------------------------

// These three are loaded dynamically (after mock registration) — declared here
// so they are accessible across all describe blocks.
let calcSnapshotHash: (form: Record<string, unknown>, template: any, body: string) => string;
let readIssueBodyForProcessing: (body: unknown) => string;
let readIssueBodyForSnapshotLegacy: (body: unknown) => string;
let extractHashFromPrBody: (body: string | null | undefined) => string | null;
let closeOutdatedRequestPrs: (...args: any[]) => Promise<void>;

beforeAll(async () => {
  jest.resetModules();

  // Pin categoryFromTemplate to 'system' so _category is stable across test runs
  await jest.unstable_mockModule('../src/handlers/request/template.js', () => ({
    categoryFromTemplate: () => 'system',
    loadTemplate: jest.fn(),
    parseForm: jest.fn(() => ({})),
  }));

  // Load real production modules after mock is registered
  const snapMod = await import('../src/handlers/request/pr/snapshot.js');
  const bodyMod = await import('../src/handlers/request/domain/issue-body-processing.js');
  const cleanupMod = await import('../src/handlers/request/application/outdated-request-pr-cleanup.js');

  calcSnapshotHash = snapMod.calcSnapshotHash as any;
  extractHashFromPrBody = snapMod.extractHashFromPrBody;
  readIssueBodyForProcessing = bodyMod.readIssueBodyForProcessing;
  readIssueBodyForSnapshotLegacy = (bodyMod as any).readIssueBodyForSnapshotLegacy;
  closeOutdatedRequestPrs = cleanupMod.closeOutdatedRequestPrs as any;
});

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

const TEMPLATE = {
  name: 'System Namespace Request',
  _meta: { requestType: 'system', root: 'namespaces', schema: 'schema.json', path: '.github/x.yml' },
  body: [
    { id: 'identifier', type: 'input' },
    { id: 'description', type: 'textarea' },
    { id: 'contact', type: 'textarea' },
  ],
};

const FORM = {
  identifier: 'google.drive',
  description: 'Google Drive integration namespace',
  contact: 'user@sap.com',
};

const FORM_BODY = '### Namespace\ngoogle.drive\n\n### Contact\nuser@sap.com';
const RL = '<!-- nsreq:routing-lock = {"v":1,"expected":"System Namespace"} -->';
const PA = '<!-- nsreq:parent-approval = {"v":1,"parent":"sap","target":"google.drive","owners":[]} -->';
const CA = '<!-- nsreq:contact-approval = {"v":1,"target":"google.drive","owners":["user@sap.com"]} -->';
const CA_APPROVED =
  '<!-- nsreq:contact-approval = {"v":1,"target":"google.drive","owners":["user@sap.com"],"approvedBy":"approver","approvedAt":"2026-01-01T00:00:00Z"} -->';

// Hash that create.ts (after fix) embeds: uses readIssueBodyForProcessing
function newPrHash(issueBody: string): string {
  return calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForProcessing(issueBody));
}

// Hash that create.ts (OLD code, before fix) embedded: strips RL+PA but NOT CA
function legacyPrHash(issueBody: string): string {
  return calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForSnapshotLegacy(issueBody));
}

// Mirrors buildCompatibleRequestSnapshotHashes from index.ts (including h3 legacy compat)
function buildCompatibleHashes(issueBody: string): string[] {
  const h1 = calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForProcessing(issueBody));
  const h2 = calcSnapshotHash(FORM, TEMPLATE, issueBody);
  const h3 = calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForSnapshotLegacy(issueBody));
  return [...new Set([h1, h2, h3].filter(Boolean))];
}

// ---------------------------------------------------------------------------
// Helpers for production-function integration tests
// ---------------------------------------------------------------------------

function mkOctokit(issueBody = '') {
  return {
    rest: {
      issues: {
        get: jest.fn(async () => ({ data: { body: issueBody } })) as jest.MockedFunction<any>,
        removeLabel: jest.fn(async () => ({})) as jest.MockedFunction<any>,
      },
      pulls: {
        list: jest.fn(async () => ({ data: [] })) as jest.MockedFunction<any>,
        update: jest.fn(async () => ({})) as jest.MockedFunction<any>,
      },
      git: {
        deleteRef: jest.fn(async () => ({})) as jest.MockedFunction<any>,
      },
    },
  };
}

function mkParams(issueNumber = 1) {
  return { owner: 'o', repo: 'r', issue_number: issueNumber };
}

function buildCallbacks(issueBody: string, formAtEval = FORM) {
  const acceptedHashes = (() => {
    const h1 = calcSnapshotHash(formAtEval, TEMPLATE, readIssueBodyForProcessing(issueBody));
    const h2 = calcSnapshotHash(formAtEval, TEMPLATE, issueBody);
    const h3 = calcSnapshotHash(formAtEval, TEMPLATE, readIssueBodyForSnapshotLegacy(issueBody));
    return [...new Set([h1, h2, h3].filter(Boolean))];
  })();

  const currentHash = acceptedHashes[0];

  return {
    parseForm: () => formAtEval as any,
    readIssueBodyForProcessing,
    buildCompatibleRequestSnapshotHashes: () => acceptedHashes,
    calcSnapshotHash: (f: any, t: any, b: string) => calcSnapshotHash(f, t, b),
    extractHashFromPrBody: (b: string) => extractHashFromPrBody(b),
    findOpenIssuePrs: jest.fn() as jest.MockedFunction<any>,
    resolveEffectiveConstants: () => ({ labelOnApproved: null }),
    postOnce: jest.fn() as jest.MockedFunction<any>,
    _acceptedHashes: acceptedHashes,
    _currentHash: currentHash,
  };
}

function prBody(issueNumber: number, embeddedHash: string): string {
  return `source: #${issueNumber}\n<!-- nsreq:snapshot-hash:${embeddedHash} -->`;
}

// ---------------------------------------------------------------------------
// Tests: normalization policy
// ---------------------------------------------------------------------------

describe('readIssueBodyForProcessing — normalization policy', () => {
  it('strips routing-lock marker', () => {
    expect(readIssueBodyForProcessing(`${FORM_BODY}\n\n${RL}\n`)).toBe(FORM_BODY);
  });

  it('strips parent-approval marker', () => {
    expect(readIssueBodyForProcessing(`${FORM_BODY}\n\n${PA}\n`)).toBe(FORM_BODY);
  });

  it('strips contact-approval marker', () => {
    expect(readIssueBodyForProcessing(`${FORM_BODY}\n\n${CA}\n`)).toBe(FORM_BODY);
  });

  it('strips all three markers in combination', () => {
    expect(readIssueBodyForProcessing(`${FORM_BODY}\n\n${RL}\n\n${PA}\n\n${CA}\n`)).toBe(FORM_BODY);
  });

  it('leaves user-authored content unchanged', () => {
    const body = 'My description with <!-- html comment --> inside';
    expect(readIssueBodyForProcessing(body)).toBe(body);
  });
});

describe('readIssueBodyForSnapshotLegacy — old normalization (strips RL+PA, NOT CA)', () => {
  it('strips routing-lock but NOT contact-approval', () => {
    const body = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const result = readIssueBodyForSnapshotLegacy(body);
    expect(result).not.toContain('nsreq:routing-lock');
    expect(result).toContain('nsreq:contact-approval');
  });

  it('strips parent-approval but NOT contact-approval', () => {
    const body = `${FORM_BODY}\n\n${PA}\n\n${CA}\n`;
    const result = readIssueBodyForSnapshotLegacy(body);
    expect(result).not.toContain('nsreq:parent-approval');
    expect(result).toContain('nsreq:contact-approval');
  });
});

// ---------------------------------------------------------------------------
// Tests: confirmed bug (OLD code behaviour)
// ---------------------------------------------------------------------------

describe('confirmed bug: old readIssueBodyForSnapshot diverged from readIssueBodyForProcessing', () => {
  it('OLD function: contact-approval marker NOT stripped → hash mismatch vs new comparison', () => {
    const body = `${FORM_BODY}\n\n${CA}\n`;
    const oldNorm = readIssueBodyForSnapshotLegacy(body);
    const newNorm = readIssueBodyForProcessing(body);
    expect(oldNorm).toContain('nsreq:contact-approval');
    expect(newNorm).not.toContain('nsreq:contact-approval');
    expect(oldNorm).not.toBe(newNorm);
  });

  it('OLD function: hash at PR creation ≠ h1 from buildCompatibleHashes when CA+RL present at creation', () => {
    const bodyAtCreation = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const oldHash = legacyPrHash(bodyAtCreation);
    const hashes = buildCompatibleHashes(bodyAtCreation);
    // h1 uses readIssueBodyForProcessing (strips CA), so old hash does NOT match h1
    const h1 = calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForProcessing(bodyAtCreation));
    expect(oldHash).not.toBe(h1);
    // With h3 (legacy compat), the old hash IS now in the accepted set
    expect(hashes).toContain(oldHash);
  });
});

// ---------------------------------------------------------------------------
// Tests: fix verification — new PR creation hash matches at re-evaluation
// ---------------------------------------------------------------------------

describe('fix verification: hash at PR creation matches buildCompatibleHashes after marker mutations', () => {
  it('RL present at creation, unchanged at evaluation → hash matches', () => {
    const body = `${FORM_BODY}\n\n${RL}\n`;
    const prHashValue = newPrHash(body);
    expect(buildCompatibleHashes(body)).toContain(prHashValue);
  });

  it('CA present at PR creation → hash still matches at re-evaluation (fix corrects the false-close bug)', () => {
    const body = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const prHashValue = newPrHash(body);
    expect(buildCompatibleHashes(body)).toContain(prHashValue);
  });

  it('RL rewritten after PR creation → hash still matches', () => {
    const bodyV1 = `${FORM_BODY}\n\n<!-- nsreq:routing-lock = {"v":1,"expected":"Old Label"} -->\n`;
    const bodyV2 = `${FORM_BODY}\n\n${RL}\n`;
    const prHashValue = newPrHash(bodyV1);
    expect(buildCompatibleHashes(bodyV2)).toContain(prHashValue);
  });

  it('PA inserted after PR creation → hash still matches', () => {
    const bodyAtCreation = `${FORM_BODY}\n\n${RL}\n`;
    const bodyAfterPa = `${FORM_BODY}\n\n${PA}\n\n${RL}\n`;
    const prHashValue = newPrHash(bodyAtCreation);
    expect(buildCompatibleHashes(bodyAfterPa)).toContain(prHashValue);
  });

  it('all three markers present at evaluation, none at creation → hash still matches', () => {
    const prHashValue = newPrHash(FORM_BODY);
    expect(buildCompatibleHashes(`${FORM_BODY}\n\n${RL}\n\n${PA}\n\n${CA}\n`)).toContain(prHashValue);
  });

  it('repeated re-evaluations with same body do not change accepted hash set', () => {
    const body = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const prHashValue = newPrHash(body);
    for (let i = 0; i < 5; i++) {
      expect(buildCompatibleHashes(body)).toContain(prHashValue);
    }
  });
});

// ---------------------------------------------------------------------------
// Tests: legacy PR compatibility (PRs created by OLD code before the fix)
// ---------------------------------------------------------------------------

describe('legacy PR compatibility: h3 preserves old-code PRs when request is unchanged', () => {
  it('Case B (CA+RL): old-code hash IS in accepted set when body is stable', () => {
    const body = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const oldHash = legacyPrHash(body);
    // Before this fix, h1+h2 only — old hash not in set → PR falsely closed
    const h1 = calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForProcessing(body));
    const h2 = calcSnapshotHash(FORM, TEMPLATE, body);
    expect([h1, h2]).not.toContain(oldHash);
    // With h3 (legacy compat hash), old hash IS now accepted
    expect(buildCompatibleHashes(body)).toContain(oldHash);
  });

  it('Case C (CA+PA): old-code hash IS in accepted set when body is stable', () => {
    const body = `${FORM_BODY}\n\n${PA}\n\n${CA}\n`;
    const oldHash = legacyPrHash(body);
    expect(buildCompatibleHashes(body)).toContain(oldHash);
  });

  it('Case D (all three): old-code hash IS in accepted set when body is stable', () => {
    const body = `${FORM_BODY}\n\n${RL}\n\n${PA}\n\n${CA}\n`;
    const oldHash = legacyPrHash(body);
    expect(buildCompatibleHashes(body)).toContain(oldHash);
  });

  it('LIMITATION: old-code hash is NOT in accepted set after CA is approved (content changes)', () => {
    const bodyAtCreation = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const bodyAfterApproval = `${FORM_BODY}\n\n${RL}\n\n${CA_APPROVED}\n`;
    const oldHash = legacyPrHash(bodyAtCreation);
    // CA JSON changes → oldNorm(bodyAfterApproval) ≠ oldNorm(bodyAtCreation) → h3 does not match
    expect(buildCompatibleHashes(bodyAfterApproval)).not.toContain(oldHash);
    // This means: legacy PRs where the CA marker was approved will still be re-created once.
    // This is acceptable because the re-created PR will use the new (fixed) hash.
  });
});

// ---------------------------------------------------------------------------
// Tests: negative cases — genuine form changes must still invalidate PRs
// ---------------------------------------------------------------------------

describe('negative cases: genuine form changes must still invalidate PRs', () => {
  it('changing the identifier field invalidates the PR hash', () => {
    const body = `${FORM_BODY}\n\n${RL}\n`;
    const prHashValue = newPrHash(body);
    const changedForm = { ...FORM, identifier: 'google.changed' };
    const h1 = calcSnapshotHash(changedForm, TEMPLATE, readIssueBodyForProcessing(body));
    const h2 = calcSnapshotHash(changedForm, TEMPLATE, body);
    const h3 = calcSnapshotHash(changedForm, TEMPLATE, readIssueBodyForSnapshotLegacy(body));
    expect([...new Set([h1, h2, h3])]).not.toContain(prHashValue);
  });

  it('changing the description field invalidates the PR hash', () => {
    const body = `${FORM_BODY}\n\n${RL}\n`;
    const prHashValue = newPrHash(body);
    const changedForm = { ...FORM, description: 'Completely different' };
    const h1 = calcSnapshotHash(changedForm, TEMPLATE, readIssueBodyForProcessing(body));
    const h2 = calcSnapshotHash(changedForm, TEMPLATE, body);
    const h3 = calcSnapshotHash(changedForm, TEMPLATE, readIssueBodyForSnapshotLegacy(body));
    expect([...new Set([h1, h2, h3])]).not.toContain(prHashValue);
  });

  it('unknown user-authored HTML comments are NOT silently removed', () => {
    const userComment = '<!-- My own comment: deployment notes -->';
    const body = `${FORM_BODY}\n\n${userComment}\n`;
    expect(readIssueBodyForProcessing(body)).toContain(userComment);
  });
});

// ---------------------------------------------------------------------------
// Production integration: closeOutdatedRequestPrs with real function implementations
// and mocked GitHub octokit operations only.
//
// These tests call the actual exported closeOutdatedRequestPrs function from
// outdated-request-pr-cleanup.ts with:
//   - Real calcSnapshotHash, readIssueBodyForProcessing, readIssueBodyForSnapshotLegacy
//   - Real extractHashFromPrBody
//   - Mocked octokit (pulls.update, git.deleteRef, issues.get, issues.removeLabel)
// ---------------------------------------------------------------------------

describe('closeOutdatedRequestPrs: production function with mocked GitHub operations', () => {
  it('unchanged request — PR hash from new create.ts: PR is NOT closed (close never called)', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const octokit = mkOctokit(issueBody);
    const cbs = buildCallbacks(issueBody);
    const prHashValue = newPrHash(issueBody);

    cbs.findOpenIssuePrs.mockResolvedValueOnce([
      { number: 42, head: { ref: 'req/google.drive-1' }, body: prBody(1, prHashValue) },
    ]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
    expect(octokit.rest.git.deleteRef).not.toHaveBeenCalled();
    expect(cbs.postOnce).not.toHaveBeenCalled();
  });

  it('changed form field — PR hash from old form: PR IS closed and postOnce called', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n`;
    // PR was created with original FORM
    const prHashValue = newPrHash(issueBody);
    // Re-evaluation sees changed form
    const changedForm = { ...FORM, identifier: 'google.changed' };
    const octokit = mkOctokit(issueBody);
    const cbs = buildCallbacks(issueBody, changedForm);

    cbs.findOpenIssuePrs.mockResolvedValueOnce([
      { number: 99, head: { ref: 'req/google.drive-1' }, body: prBody(1, prHashValue) },
    ]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    expect(octokit.rest.pulls.update).toHaveBeenCalledWith(
      expect.objectContaining({ pull_number: 99, state: 'closed' })
    );
    expect(cbs.postOnce).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.stringContaining('#99'),
      expect.anything()
    );
  });

  it('legacy PR (old-code hash, CA+RL present, body stable): PR is NOT closed via h3 compat hash', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const octokit = mkOctokit(issueBody);
    const cbs = buildCallbacks(issueBody);
    const oldHash = legacyPrHash(issueBody);

    // Verify this hash was NOT in h1+h2 only (proving h3 is doing the work)
    const h1 = calcSnapshotHash(FORM, TEMPLATE, readIssueBodyForProcessing(issueBody));
    const h2 = calcSnapshotHash(FORM, TEMPLATE, issueBody);
    expect([h1, h2]).not.toContain(oldHash);
    // But it IS in the full accepted set (h1+h2+h3)
    expect(cbs._acceptedHashes).toContain(oldHash);

    cbs.findOpenIssuePrs.mockResolvedValueOnce([
      { number: 77, head: { ref: 'req/google.drive-1' }, body: prBody(1, oldHash) },
    ]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
    expect(cbs.postOnce).not.toHaveBeenCalled();
  });

  it('multiple PRs for same issue: only stale PR closed, fresh and legacy PRs preserved', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const octokit = mkOctokit(issueBody);
    const cbs = buildCallbacks(issueBody);

    const freshHash = newPrHash(issueBody);
    const oldHash = legacyPrHash(issueBody);
    // Hash from a completely different form — truly stale
    const staleHash = calcSnapshotHash({ ...FORM, identifier: 'old.name' }, TEMPLATE, FORM_BODY);

    cbs.findOpenIssuePrs.mockResolvedValueOnce([
      { number: 10, head: { ref: 'req/fresh' }, body: prBody(1, freshHash) },
      { number: 11, head: { ref: 'req/legacy' }, body: prBody(1, oldHash) },
      { number: 12, head: { ref: 'req/stale' }, body: prBody(1, staleHash) },
    ]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    // Only PR #12 (stale) is closed
    const closedNums = octokit.rest.pulls.update.mock.calls.map((c: any) => c[0].pull_number);
    expect(closedNums).not.toContain(10);
    expect(closedNums).not.toContain(11);
    expect(closedNums).toContain(12);
  });

  it('no open PRs: no GitHub write calls made', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n`;
    const octokit = mkOctokit(issueBody);
    const cbs = buildCallbacks(issueBody);
    cbs.findOpenIssuePrs.mockResolvedValueOnce([]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
    expect(cbs.postOnce).not.toHaveBeenCalled();
  });

  it('repeated re-evaluations (idempotent): preserved PR never gets closed across 5 runs', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const prHashValue = newPrHash(issueBody);

    for (let i = 0; i < 5; i++) {
      const octokit = mkOctokit(issueBody);
      const cbs = buildCallbacks(issueBody);
      cbs.findOpenIssuePrs.mockResolvedValueOnce([
        { number: 7, head: { ref: 'req/google.drive-1' }, body: prBody(1, prHashValue) },
      ]);
      await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);
      expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
    }
  });

  it('pre-computed acceptedHashes option: uses given hash set without re-reading issue', async () => {
    const issueBody = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const octokit = mkOctokit(issueBody);
    const prHashValue = newPrHash(issueBody);
    const cbs = buildCallbacks(issueBody);

    cbs.findOpenIssuePrs.mockResolvedValueOnce([{ number: 55, head: { ref: 'req/a' }, body: prBody(1, prHashValue) }]);

    // Pass pre-computed hashes via options (simulates processPullRequestForAutoMerge path)
    const opts = {
      parsedFormData: FORM as any,
      currentHash: prHashValue,
      acceptedHashes: [prHashValue],
    };

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, opts, cbs as any);

    // options path skips issues.get
    expect(octokit.rest.issues.get).not.toHaveBeenCalled();
    expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
  });

  it('CA rewritten (approvedBy added) — new-code PR: still preserved (CA stripped, hash stable)', async () => {
    const bodyAtCreation = `${FORM_BODY}\n\n${RL}\n\n${CA}\n`;
    const bodyAtEval = `${FORM_BODY}\n\n${RL}\n\n${CA_APPROVED}\n`;
    const octokit = mkOctokit(bodyAtEval);

    // PR was created with new code — hash based on readIssueBodyForProcessing (strips CA)
    const prHashValue = newPrHash(bodyAtCreation);
    const cbs = buildCallbacks(bodyAtEval); // evaluated with new CA content

    cbs.findOpenIssuePrs.mockResolvedValueOnce([{ number: 33, head: { ref: 'req/a' }, body: prBody(1, prHashValue) }]);

    await closeOutdatedRequestPrs({ octokit } as any, mkParams(), TEMPLATE, {}, cbs as any);

    expect(octokit.rest.pulls.update).not.toHaveBeenCalled();
  });
});
