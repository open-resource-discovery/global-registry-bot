/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-function-return-type */

import { jest } from '@jest/globals';

type AsyncMock<Args extends unknown[] = unknown[], Result = unknown> = jest.MockedFunction<
  (...args: Args) => Promise<Result>
>;

function mockAsync<Args extends unknown[] = unknown[], Result = unknown>(): AsyncMock<Args, Result> {
  return jest.fn((..._args: Args) => Promise.resolve(undefined as unknown as Result)) as AsyncMock<Args, Result>;
}

function mkCtx(config?: any) {
  const removeLabel = mockAsync<[any], unknown>();
  const addLabels = mockAsync<[any], unknown>();
  const addAssignees = mockAsync<[any], unknown>();

  return {
    resourceBotConfig: config,
    log: { warn: jest.fn() },
    octokit: {
      issues: { removeLabel, addLabels, addAssignees },
    },
  };
}

async function loadSubject(opts?: {
  stateLabels?: { author?: string | null; review?: string | null };
  approvers?: string[];
}) {
  jest.resetModules();

  const getStateLabelsFromConfig = jest
    .fn()
    .mockReturnValue(opts?.stateLabels ?? { author: 'state:author', review: 'state:review' });

  const getApproversFromConfig = jest.fn().mockReturnValue(opts?.approvers ?? ['ap1', 'ap2']);

  await jest.unstable_mockModule('../src/handlers/request/constants.js', () => ({
    getStateLabelsFromConfig,
    getApproversFromConfig,
  }));

  const mod = await import('../src/handlers/request/state.js');
  return { mod, mocks: { getStateLabelsFromConfig, getApproversFromConfig } };
}

function errWithStatus(message: string, status: number): any {
  const e: any = new Error(message);
  e.status = status;
  return e;
}

describe('src/handlers/request/state.ts', () => {
  describe('setStateLabel', () => {
    it('returns early when no add-label and no global labels', async () => {
      const { mod } = await loadSubject({
        stateLabels: { author: null, review: null },
      });

      const ctx = mkCtx({}); // no workflow.labels.global
      const issue = { labels: [{ name: 'x' }] };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 1 }, issue as any, 'author');

      expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
      expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
    });

    it('adds author label + missing global labels (global as array), without removal', async () => {
      const { mod, mocks } = await loadSubject({
        stateLabels: { author: 'state:author', review: 'state:review' },
      });

      const ctx = mkCtx({
        workflow: {
          labels: {
            global: [' g1 ', 'g2'],
          },
        },
      });

      const issue = {
        labels: ['existing', { name: 'g1' }], // g1 already present via object label
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 2 }, issue as any, 'author');

      expect(mocks.getStateLabelsFromConfig).toHaveBeenCalled();
      expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();

      const addCalls = ctx.octokit.issues.addLabels.mock.calls;
      expect(addCalls.length).toBe(1);
      expect(addCalls[0][0]).toEqual({
        owner: 'o',
        repo: 'r',
        issue_number: 2,
        labels: ['state:author', 'g2'], // g1 already present -> only g2 missing
      });
    });

    it('removes opposite label when switching state, even if nothing new to add', async () => {
      const { mod } = await loadSubject({
        stateLabels: { author: 'state:author', review: 'state:review' },
      });

      const ctx = mkCtx({
        workflow: {
          labels: { global: ['g1'] },
        },
      });

      const issue = {
        labels: ['state:review', 'state:author', 'g1'], // already has author+global, but contains review -> must remove review
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 3 }, issue as any, 'author');

      expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        issue_number: 3,
        name: 'state:review',
      });

      // nothing new to add
      expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
    });

    it('ignores non-secondary errors on removeLabel and still adds labels', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx({
        workflow: { labels: { global: [] } },
      });

      ctx.octokit.issues.removeLabel.mockRejectedValueOnce({
        status: 500,
        message: 'boom',
      });

      const issue = {
        labels: ['state:author'], // will be removed when setting review
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 4 }, issue as any, 'review');

      expect(ctx.log.warn).not.toHaveBeenCalled(); // non-secondary -> no warn, no early return

      expect(ctx.octokit.issues.removeLabel).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.addLabels).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.addLabels).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        issue_number: 4,
        labels: ['state:review'],
      });
    });

    it('secondary rate limit on removeLabel logs + returns early (no addLabels)', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx({
        workflow: { labels: { global: 'g1' } }, // global as string branch
      });

      ctx.octokit.issues.removeLabel.mockRejectedValueOnce(errWithStatus('Secondary rate limit exceeded', 403));

      const issue = {
        labels: ['state:review'], // will be removed when setting author
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 5 }, issue as any, 'author');

      expect(ctx.log.warn).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
    });

    it('secondary rate limit on addLabels logs + returns (after optional remove)', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx({
        workflow: { labels: { global: ['g1'] } },
      });

      ctx.octokit.issues.addLabels.mockRejectedValueOnce(errWithStatus('hit SECONDARY rate limit', 429));

      const issue = {
        labels: [], // nothing to remove, new labels will be attempted
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 6 }, issue as any, 'author');

      expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
      expect(ctx.octokit.issues.addLabels).toHaveBeenCalledTimes(1);
      expect(ctx.log.warn).toHaveBeenCalledTimes(1);
    });

    it('does nothing when shouldRemove=false and newLabels empty', async () => {
      const { mod } = await loadSubject();

      const ctx = mkCtx({
        workflow: { labels: { global: ['g1'] } },
      });

      const issue = {
        labels: ['state:author', 'g1'], // already has everything for "author"
      };

      await mod.setStateLabel(ctx as any, { owner: 'o', repo: 'r', issue_number: 7 }, issue as any, 'author');

      expect(ctx.octokit.issues.removeLabel).not.toHaveBeenCalled();
      expect(ctx.octokit.issues.addLabels).not.toHaveBeenCalled();
    });
  });

  describe('ensureAssigneesOnce', () => {
    it('uses explicit logins when provided, assigns only missing', async () => {
      const { mod } = await loadSubject({
        approvers: ['ap1', 'ap2'], // should NOT be used because logins provided
      });

      const ctx = mkCtx({});

      const issue = {
        assignees: [{ login: 'u1' }, { login: null }], // null ignored
      };

      await mod.ensureAssigneesOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 10 }, issue as any, [
        'u1',
        'u2',
      ]);

      expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        issue_number: 10,
        assignees: ['u2'],
      });
    });

    it('falls back to approvers from config when logins empty', async () => {
      const { mod } = await loadSubject({
        approvers: ['ap1', 'ap2'],
      });

      const ctx = mkCtx({});

      const issue = { assignees: [] };

      await mod.ensureAssigneesOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 11 }, issue as any, []);

      expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledTimes(1);
      expect(ctx.octokit.issues.addAssignees).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'r',
        issue_number: 11,
        assignees: ['ap1', 'ap2'],
      });
    });

    it('returns early when nothing missing', async () => {
      const { mod } = await loadSubject({ approvers: ['ap1'] });

      const ctx = mkCtx({});
      const issue = { assignees: [{ login: 'ap1' }] };

      await mod.ensureAssigneesOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 12 }, issue as any);

      expect(ctx.octokit.issues.addAssignees).not.toHaveBeenCalled();
    });

    it('ignores addAssignees errors (does not throw)', async () => {
      const { mod } = await loadSubject({ approvers: ['ap1'] });

      const ctx = mkCtx({});
      ctx.octokit.issues.addAssignees.mockRejectedValueOnce(new Error('boom'));

      const issue = { assignees: [] };

      await expect(
        mod.ensureAssigneesOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 13 }, issue as any)
      ).resolves.toBeUndefined();
    });

    it('returns early when no desired assignees exist (empty approvers + empty logins)', async () => {
      const { mod } = await loadSubject({ approvers: [] });

      const ctx = mkCtx({});
      const issue = { assignees: [] };

      await mod.ensureAssigneesOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 14 }, issue as any, []);

      expect(ctx.octokit.issues.addAssignees).not.toHaveBeenCalled();
    });
  });
});
