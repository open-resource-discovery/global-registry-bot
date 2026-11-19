/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, test, expect, jest } from '@jest/globals';
import {
  resolveParentOwnerLoginsForTarget,
  clearParentOwnerActionState,
  setParentOwnerActionState,
  assignParentOwnersForApproval,
  ensureContactApprovalMarker,
  ensureParentApprovalMarker,
  maybeRequireParentOwnerApproval,
  resolveRequestContactOwnerLogins,
} from '../src/handlers/request/application/owner-approval-requirements.js';

const ctx = {};
const params = { owner: 'org', repo: 'repo', issue_number: 1 };
const template = { _meta: { root: 'namespaces' } };

// Body with a contact-approval marker containing 2 owners (for sameNormalizedLoginSet length-mismatch test)
const CONTACT_APPROVAL_BODY_2 =
  '<!-- nsreq:contact-approval = {"v":1,"target":"my-ns","owners":["owner1","owner2"]} -->';

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function makeCallbacks(overrides: Record<string, unknown> = {}) {
  return {
    normalizeKey: jest.fn((v: unknown) =>
      String(v ?? '')
        .toLowerCase()
        .trim()
    ),
    labelsMatching: jest.fn((labels: string[], expected: string) =>
      labels.filter((l) => l.toLowerCase() === expected.toLowerCase())
    ),
    updateIssueBody: jest.fn().mockResolvedValue(undefined),
    readYamlFromRepo: jest.fn().mockResolvedValue({ contacts: 'owner1' }),
    extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['owner1'], emails: [] }),
    lookupGithubLoginsByEmail: jest.fn().mockResolvedValue([]),
    resolveEffectiveConstants: jest.fn().mockReturnValue({
      reviewRequestedLabels: [],
      labelOnApproved: 'Approved',
    }),
    resolveWorkflowLabel: jest.fn((_ctx: unknown, _key: unknown, fallback: string) => fallback),
    fetchIssueLabels: jest.fn().mockResolvedValue([]),
    removeExactLabelsFromIssue: jest.fn().mockResolvedValue(undefined),
    ensureLabelsPresentOnce: jest.fn().mockResolvedValue(undefined),
    ensureAssigneesPresent: jest.fn().mockResolvedValue(undefined),
    postOnce: jest.fn().mockResolvedValue(undefined),
    isSubContextRequestType: jest.fn().mockReturnValue(true),
    setStateLabel: jest.fn().mockResolvedValue(undefined),
    log: jest.fn(),
    ...overrides,
  } as any;
}

describe('resolveParentOwnerLoginsForTarget', () => {
  test('returns empty when requestType has no namespace keyword', async () => {
    const cbs = makeCallbacks();
    const result = await resolveParentOwnerLoginsForTarget(
      ctx,
      params,
      template,
      'sap.cloud.service',
      'create-type',
      cbs
    );
    expect(result).toEqual({ parent: '', owners: [] });
  });

  test('returns empty when namespace has ≤2 parts', async () => {
    const cbs = makeCallbacks();
    const result = await resolveParentOwnerLoginsForTarget(ctx, params, template, 'sap.cloud', 'namespace-type', cbs);
    expect(result).toEqual({ parent: '', owners: [] });
  });

  test('returns parent with empty owners when template has no root', async () => {
    const cbs = makeCallbacks();
    const result = await resolveParentOwnerLoginsForTarget(ctx, params, {}, 'sap.cloud.service', 'namespace-type', cbs);
    expect(result).toEqual({ parent: 'sap.cloud', owners: [] });
  });

  test('uses doc.contact when doc has no contacts key', async () => {
    const cbs = makeCallbacks({
      readYamlFromRepo: jest.fn().mockResolvedValue({ contact: 'owner-from-contact' }),
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['owner1'], emails: [] }),
    });
    await resolveParentOwnerLoginsForTarget(ctx, params, template, 'sap.cloud.service', 'namespace-type', cbs);
    expect(cbs.extractParentContactCandidates).toHaveBeenCalledWith('owner-from-contact');
  });

  test('uses doc.owners when doc has no contacts or contact key', async () => {
    const cbs = makeCallbacks({
      readYamlFromRepo: jest.fn().mockResolvedValue({ owners: 'owner-from-owners' }),
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['owner2'], emails: [] }),
    });
    await resolveParentOwnerLoginsForTarget(ctx, params, template, 'sap.cloud.service', 'namespace-type', cbs);
    expect(cbs.extractParentContactCandidates).toHaveBeenCalledWith('owner-from-owners');
  });

  test('uses doc.owner when doc has no contacts/contact/owners key', async () => {
    const cbs = makeCallbacks({
      readYamlFromRepo: jest.fn().mockResolvedValue({ owner: 'owner-from-owner' }),
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['owner3'], emails: [] }),
    });
    await resolveParentOwnerLoginsForTarget(ctx, params, template, 'sap.cloud.service', 'namespace-type', cbs);
    expect(cbs.extractParentContactCandidates).toHaveBeenCalledWith('owner-from-owner');
  });
});

describe('clearParentOwnerActionState', () => {
  test('skips fetchIssueLabels when currentLabels is provided as non-empty array', async () => {
    const cbs = makeCallbacks();
    await clearParentOwnerActionState(ctx, params, cbs, ['Some Label']);
    expect(cbs.fetchIssueLabels).not.toHaveBeenCalled();
  });

  test('removes parentOwnerAction label when it is found in provided currentLabels', async () => {
    const cbs = makeCallbacks({
      labelsMatching: jest.fn().mockReturnValue(['Parent Owner Action']),
    });
    await clearParentOwnerActionState(ctx, params, cbs, ['Parent Owner Action']);
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalledWith(ctx, params, ['Parent Owner Action']);
  });
});

describe('setParentOwnerActionState', () => {
  test('uses Approved fallback when labelOnApproved is empty string', async () => {
    const cbs = makeCallbacks({
      resolveEffectiveConstants: jest.fn().mockReturnValue({
        reviewRequestedLabels: [],
        labelOnApproved: '',
      }),
      labelsMatching: jest.fn().mockReturnValue(['Approved']),
    });
    await setParentOwnerActionState(ctx, params, cbs);
    expect(cbs.removeExactLabelsFromIssue).toHaveBeenCalled();
  });

  test('handles null reviewRequestedLabels with [] fallback without throwing', async () => {
    const cbs = makeCallbacks({
      resolveEffectiveConstants: jest.fn().mockReturnValue({
        reviewRequestedLabels: null,
        labelOnApproved: 'Approved',
      }),
    });
    await setParentOwnerActionState(ctx, params, cbs);
    expect(cbs.ensureLabelsPresentOnce).toHaveBeenCalled();
  });
});

describe('assignParentOwnersForApproval', () => {
  test('returns early without calling ensureAssigneesPresent when owners is empty array', async () => {
    const cbs = makeCallbacks();
    await assignParentOwnersForApproval(ctx, params, [], cbs);
    expect(cbs.ensureAssigneesPresent).not.toHaveBeenCalled();
  });

  test('handles null owners via [] fallback and returns early', async () => {
    const cbs = makeCallbacks();
    await assignParentOwnersForApproval(ctx, params, null as any, cbs);
    expect(cbs.ensureAssigneesPresent).not.toHaveBeenCalled();
  });
});

describe('ensureContactApprovalMarker', () => {
  test('sameNormalizedLoginSet different lengths → updates body and returns true', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: CONTACT_APPROVAL_BODY_2 };
    // current has ['owner1','owner2'], new meta has ['owner1'] → length mismatch
    const result = await ensureContactApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, target: 'my-ns', owners: ['owner1'] },
      cbs
    );
    expect(result).toBe(true);
    expect(cbs.updateIssueBody).toHaveBeenCalled();
  });

  test('returns false when meta.target is empty string', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '' };
    const result = await ensureContactApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, target: '', owners: ['owner1'] },
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when meta.owners is null (falls back to [] → empty owners)', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '' };
    const result = await ensureContactApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, target: 'my-ns', owners: null as any },
      cbs
    );
    expect(result).toBe(false);
  });

  test('meta=null + no current marker → returns false without updating body', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: 'plain issue body with no marker' };
    const result = await ensureContactApprovalMarker(ctx, params, issue as any, null, cbs);
    expect(result).toBe(false);
    expect(cbs.updateIssueBody).not.toHaveBeenCalled();
  });
});

describe('ensureParentApprovalMarker', () => {
  test('meta=null + no current parent marker → returns false without update', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: 'plain body without any marker' };
    const result = await ensureParentApprovalMarker(ctx, params, issue as any, null, cbs);
    expect(result).toBe(false);
    expect(cbs.updateIssueBody).not.toHaveBeenCalled();
  });

  test('handles null meta.owners with [] fallback; returns false when parent is empty', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '' };
    const result = await ensureParentApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, parent: '', target: 'sap.cloud', owners: null as any },
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when meta.parent is empty string', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '' };
    const result = await ensureParentApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, parent: '', target: 'sap.cloud', owners: ['owner1'] },
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when meta.target is empty string', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '' };
    const result = await ensureParentApprovalMarker(
      ctx,
      params,
      issue as any,
      { v: 1, parent: 'sap.cloud', target: '', owners: ['owner1'] },
      cbs
    );
    expect(result).toBe(false);
  });
});

describe('maybeRequireParentOwnerApproval', () => {
  test('returns false when requestType has no namespace keyword', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '', user: { login: 'user1' } };
    const result = await maybeRequireParentOwnerApproval(
      ctx,
      params,
      issue as any,
      template as any,
      'sap.cloud.service',
      'create-type',
      cbs
    );
    expect(result).toBe(false);
  });

  test('returns false when namespace has ≤2 parts', async () => {
    const cbs = makeCallbacks();
    const issue = { number: 1, body: '', user: { login: 'user1' } };
    const result = await maybeRequireParentOwnerApproval(
      ctx,
      params,
      issue as any,
      template as any,
      'sap.cloud',
      'namespace-type',
      cbs
    );
    expect(result).toBe(false);
  });

  test('isSubContextRequestType=false → clears marker with null when requester is parent owner', async () => {
    const cbs = makeCallbacks({
      isSubContextRequestType: jest.fn().mockReturnValue(false),
      readYamlFromRepo: jest.fn().mockResolvedValue({ contacts: 'requester' }),
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['requester'], emails: [] }),
    });
    const issue = { number: 1, body: '', user: { login: 'requester' } };
    const result = await maybeRequireParentOwnerApproval(
      ctx,
      params,
      issue as any,
      template as any,
      'sap.cloud.service',
      'namespace-type',
      cbs
    );
    expect(result).toBe(false);
    expect(cbs.isSubContextRequestType).toHaveBeenCalledWith('namespace-type');
  });
});

describe('resolveRequestContactOwnerLogins', () => {
  test('returns logins from formData.contact field', async () => {
    const cbs = makeCallbacks({
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['alice'], emails: [] }),
    });
    const result = await resolveRequestContactOwnerLogins(ctx, { contact: 'alice' } as any, cbs);
    expect(result).toEqual(['alice']);
    expect(cbs.extractParentContactCandidates).toHaveBeenCalledWith('alice');
  });

  test('falls back to formData.contacts when contact is absent', async () => {
    const cbs = makeCallbacks({
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: ['bob'], emails: [] }),
    });
    const result = await resolveRequestContactOwnerLogins(ctx, { contacts: 'bob' } as any, cbs);
    expect(result).toEqual(['bob']);
    expect(cbs.extractParentContactCandidates).toHaveBeenCalledWith('bob');
  });

  test('resolves email logins via lookupGithubLoginsByEmail', async () => {
    const cbs = makeCallbacks({
      extractParentContactCandidates: jest.fn().mockReturnValue({ logins: [], emails: ['alice@example.com'] }),
      lookupGithubLoginsByEmail: jest.fn().mockResolvedValue(['alice']),
    });
    const result = await resolveRequestContactOwnerLogins(ctx, {} as any, cbs);
    expect(result).toContain('alice');
  });
});
