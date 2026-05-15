import { toStringTrim } from '../domain/login-utils.js';

type RepoInfoBase = { owner: string; repo: string };

type PullRequestLikeBase = {
  number?: number | null;
  state?: string | null;
  head?: {
    sha?: string | null;
  } | null;
};

type CheckRunPullRequestRefBase = { number?: number | null };
type CheckSuitePullRequestRefBase = { number?: number | null };

type CheckRunLikeBase = {
  pull_requests?: CheckRunPullRequestRefBase[] | null;
};

type CheckSuiteLikeBase = {
  id?: number | null;
  pull_requests?: CheckSuitePullRequestRefBase[] | null;
};

export type CheckPrResolutionCallbacks<ContextType, PullRequestType extends PullRequestLikeBase> = {
  isPlainObject: (value: unknown) => value is Record<string, unknown>;
  listPullRequestsAssociatedWithCommit: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      commit_sha: string;
      per_page: number;
    }
  ) => Promise<{ data?: unknown }>;
  listPulls: (
    context: ContextType,
    args: {
      owner: string;
      repo: string;
      state: 'open';
      per_page: number;
      page: number;
    }
  ) => Promise<{ data?: PullRequestType[] }>;
};

export function readCheckRunFromPayload<CheckRunType extends CheckRunLikeBase>(
  payload: unknown,
  callbacks: Pick<CheckPrResolutionCallbacks<unknown, RepoInfoBase, PullRequestLikeBase>, 'isPlainObject'>
): CheckRunType | null {
  if (!callbacks.isPlainObject(payload)) return null;

  const run = payload['check_run'];
  if (!callbacks.isPlainObject(run)) return null;

  return run as unknown as CheckRunType;
}

export function readCheckRunPrNumbers<CheckRunType extends CheckRunLikeBase>(run: CheckRunType | null): number[] {
  const prs = Array.isArray(run?.pull_requests) ? run.pull_requests : [];
  const out: number[] = [];

  for (const pr of prs) {
    const n = pr?.number;
    if (typeof n === 'number' && Number.isFinite(n)) out.push(n);
  }

  return Array.from(new Set(out));
}

export function readCheckSuiteFromPayload<CheckSuiteType extends CheckSuiteLikeBase>(
  payload: unknown,
  callbacks: Pick<CheckPrResolutionCallbacks<unknown, RepoInfoBase, PullRequestLikeBase>, 'isPlainObject'>
): CheckSuiteType | null {
  if (!callbacks.isPlainObject(payload)) return null;
  const suite = payload['check_suite'];
  if (!callbacks.isPlainObject(suite)) return null;
  return suite as unknown as CheckSuiteType;
}

export function readCheckSuiteId<CheckSuiteType extends CheckSuiteLikeBase>(
  suite: CheckSuiteType | null
): number | null {
  const id = suite?.id;
  return typeof id === 'number' && Number.isFinite(id) ? id : null;
}

export function readCheckSuitePrNumbers<CheckSuiteType extends CheckSuiteLikeBase>(
  suite: CheckSuiteType | null
): number[] {
  const prs = Array.isArray(suite?.pull_requests) ? suite?.pull_requests : [];
  const out: number[] = [];
  for (const pr of prs) {
    const n = pr?.number;
    if (typeof n === 'number' && Number.isFinite(n)) out.push(n);
  }
  return out;
}

export async function resolveCheckSuitePrNumbers<
  ContextType,
  RepoInfoType extends RepoInfoBase,
  PullRequestType extends PullRequestLikeBase,
  CheckSuiteType extends CheckSuiteLikeBase,
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  suite: CheckSuiteType | null,
  headSha: string,
  callbacks: CheckPrResolutionCallbacks<ContextType, RepoInfoType, PullRequestType>
): Promise<number[]> {
  const direct = readCheckSuitePrNumbers(suite);
  if (direct.length) return Array.from(new Set(direct));

  const sha = toStringTrim(headSha);
  if (!sha) return [];

  try {
    const res = await callbacks.listPullRequestsAssociatedWithCommit(context, {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      commit_sha: sha,
      per_page: 100,
    });

    const data = (res as unknown as { data?: unknown }).data;
    const items = Array.isArray(data) ? data : [];

    const fromCommit = items
      .map((pr) => {
        if (!callbacks.isPlainObject(pr)) return null;

        const state = toStringTrim(pr['state']).toLowerCase();
        const number = pr['number'];

        if (state !== 'open') return null;
        if (typeof number !== 'number' || !Number.isFinite(number)) return null;

        return number;
      })
      .filter((n): n is number => typeof n === 'number');

    if (fromCommit.length) return Array.from(new Set(fromCommit));
  } catch {
    // ignore and fall through to the repo scan fallback
  }

  const matches: number[] = [];
  let page = 1;

  while (true) {
    const { data } = await callbacks.listPulls(context, {
      owner: repoInfo.owner,
      repo: repoInfo.repo,
      state: 'open',
      per_page: 100,
      page,
    });

    const prs = data || [];
    if (!prs.length) break;

    for (const pr of prs) {
      if (toStringTrim(pr.head?.sha) !== sha) continue;
      if (typeof pr.number !== 'number' || !Number.isFinite(pr.number)) continue;
      matches.push(pr.number);
    }

    if (prs.length < 100) break;
    page += 1;
    if (page > 20) break;
  }

  return Array.from(new Set(matches));
}
