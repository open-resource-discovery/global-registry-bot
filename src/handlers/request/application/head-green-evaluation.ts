import {
  isBlockingCheckConclusion,
  isGreenCheckConclusion,
  summarizeHeadGreenRun,
  type HeadGreenRunSummary,
} from '../domain/check-conclusions.js';
import { toStringTrim } from '../domain/login-utils.js';

type RepoInfo = { owner: string; repo: string };

type RefCheckRunLike = {
  id?: number | null;
  name?: string | null;
  status?: string | null;
  conclusion?: string | null;
};

type HeadGreenEvaluation = {
  green: boolean;
  reason: string;
  latestRuns: HeadGreenRunSummary[];
  blockingRuns: HeadGreenRunSummary[];
  statusState?: string;
};

export type HeadGreenEvaluationContext = {
  octokit: {
    checks: {
      listForRef: (args: {
        owner: string;
        repo: string;
        ref: string;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: unknown }>;
    };
    repos: {
      getCombinedStatusForRef: (args: {
        owner: string;
        repo: string;
        ref: string;
      }) => Promise<{ data?: { state?: string | null } }>;
    };
  };
};

export type HeadGreenEvaluationCallbacks<ContextType> = {
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  getErrorMessage: (error: unknown) => string;
  getHttpStatus: (error: unknown) => number | undefined;
  logCheckRunsFetchFailed: (
    context: ContextType,
    args: { repoInfo: RepoInfo; headSha: string; error: unknown }
  ) => void;
};

export async function evaluateHeadGreenForApprovalReevaluation<ContextType extends HeadGreenEvaluationContext>(
  context: ContextType,
  repoInfo: RepoInfo,
  headSha: string,
  callbacks: HeadGreenEvaluationCallbacks<ContextType>
): Promise<HeadGreenEvaluation> {
  const ref = toStringTrim(headSha);
  if (!ref) {
    return {
      green: false,
      reason: 'missing-head-sha',
      latestRuns: [],
      blockingRuns: [],
    };
  }

  try {
    const all: RefCheckRunLike[] = [];
    let page = 1;

    while (true) {
      const res = await context.octokit.checks.listForRef({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        ref,
        per_page: 100,
        page,
      });

      const data = (res as { data?: unknown }).data;
      const runs =
        callbacks.isPlainObject(data) && Array.isArray(data['check_runs'])
          ? (data['check_runs'] as unknown as RefCheckRunLike[])
          : [];

      all.push(...runs);

      if (runs.length < 100) break;
      page += 1;
      if (page > 20) break;
    }

    const latestByName = new Map<string, RefCheckRunLike>();

    for (const run of all) {
      const name = toStringTrim(run?.name) || '__unnamed__';
      const currentId = typeof run?.id === 'number' ? run.id : -1;
      const prev = latestByName.get(name);
      const prevId = typeof prev?.id === 'number' ? prev.id : -1;

      if (!prev || currentId > prevId) {
        latestByName.set(name, run);
      }
    }

    if (latestByName.size > 0) {
      const latestRuns = Array.from(latestByName.values()).map(summarizeHeadGreenRun);

      const incompleteRuns = latestRuns.filter((run) => run.status !== 'completed');
      if (incompleteRuns.length) {
        return {
          green: false,
          reason: 'check-runs-not-completed',
          latestRuns,
          blockingRuns: incompleteRuns,
        };
      }

      const blockingRuns = latestRuns.filter(
        (run) => isBlockingCheckConclusion(run.conclusion) || !isGreenCheckConclusion(run.conclusion)
      );

      if (blockingRuns.length) {
        return {
          green: false,
          reason: 'check-runs-blocking-or-not-green',
          latestRuns,
          blockingRuns,
        };
      }

      const sawSuccess = latestRuns.some((run) => run.conclusion === 'success');
      if (!sawSuccess) {
        return {
          green: false,
          reason: 'no-success-check-run',
          latestRuns,
          blockingRuns: [],
        };
      }

      return {
        green: true,
        reason: 'check-runs-green',
        latestRuns,
        blockingRuns: [],
      };
    }
  } catch (error: unknown) {
    callbacks.logCheckRunsFetchFailed(context, { repoInfo, headSha: ref, error });
  }

  try {
    const res = await context.octokit.repos.getCombinedStatusForRef({
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      ref,
    });

    const statusState = toStringTrim(res?.data?.state).toLowerCase();

    return {
      green: statusState === 'success',
      reason: statusState === 'success' ? 'combined-status-success' : 'combined-status-not-success',
      latestRuns: [],
      blockingRuns: [],
      statusState,
    };
  } catch (_error: unknown) {
    return {
      green: false,
      reason: 'combined-status-fetch-failed',
      latestRuns: [],
      blockingRuns: [],
    };
  }
}
