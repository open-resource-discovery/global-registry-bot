type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  user?: { login?: string | null } | null;
};

type PullRequestCommitLike = {
  author?: { login?: string | null } | null;
  committer?: { login?: string | null } | null;
};

export type PullRequestAuthorResolutionContext = {
  octokit: {
    pulls: {
      listCommits: (args: {
        owner: string;
        repo: string;
        pull_number: number;
        per_page?: number;
        page?: number;
      }) => Promise<{ data?: PullRequestCommitLike[] }>;
    };
  };
};

export type PullRequestAuthorResolutionCallbacks = {
  normalizeLogin: (value: unknown) => string;
};

export async function resolvePullRequestRequestAuthorId<
  ContextType extends PullRequestAuthorResolutionContext,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  callbacks: PullRequestAuthorResolutionCallbacks
): Promise<string> {
  let page = 1;

  const blockedServiceUsers = new Set<string>([
    'web-flow-serviceuser',
    'global-registry-bot',
    'global-registry-bot[bot]',
    'my-registry-bot',
    'my-registry-bot[bot]',
    'github-actions[bot]',
    'github-actions',
  ]);

  const isUsableRequesterLogin = (value: unknown): string => {
    const login = callbacks.normalizeLogin(value);
    if (!login) return '';
    if (blockedServiceUsers.has(login.toLowerCase())) return '';
    return login;
  };

  let lastAuthorLogin = '';
  let lastCommitterLogin = '';
  let firstAuthorLogin = '';
  let firstCommitterLogin = '';

  try {
    while (true) {
      const res = await context.octokit.pulls.listCommits({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        per_page: 100,
        page,
      });

      const commits = Array.isArray(res?.data) ? res.data : [];
      if (!commits.length) break;

      for (const commit of commits) {
        const authorLogin = isUsableRequesterLogin(commit?.author?.login);
        const committerLogin = isUsableRequesterLogin(commit?.committer?.login);

        if (!firstAuthorLogin && authorLogin) firstAuthorLogin = authorLogin;
        if (!firstCommitterLogin && committerLogin) firstCommitterLogin = committerLogin;

        if (authorLogin) lastAuthorLogin = authorLogin;
        if (committerLogin) lastCommitterLogin = committerLogin;
      }

      if (commits.length < 100) break;
      page += 1;
      if (page > 20) break;
    }
  } catch {
    // Fall through to PR author fallback below
  }

  return (
    lastAuthorLogin ||
    lastCommitterLogin ||
    firstAuthorLogin ||
    firstCommitterLogin ||
    isUsableRequesterLogin(pr.user?.login) ||
    ''
  );
}
