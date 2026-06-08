type PageArgs = {
  per_page?: number;
  page?: number;
};

type IssueArgs = {
  owner: string;
  repo: string;
  issue_number: number;
};

type RepoArgs = {
  owner: string;
  repo: string;
};

type PullRequestNumberArgs = RepoArgs & {
  pull_number: number;
};

type ListPullRequestsArgs = RepoArgs &
  PageArgs & {
    state?: 'open' | 'closed' | 'all';
    head?: string;
    base?: string;
    sort?: 'created' | 'updated' | 'popularity' | 'long-running';
    direction?: 'asc' | 'desc';
  };

type PullRequestFilesArgs = PullRequestNumberArgs & PageArgs;
type PullRequestCommitsArgs = PullRequestNumberArgs & PageArgs;
type PullRequestReviewsArgs = PullRequestNumberArgs & PageArgs;

type PullRequestListResult = {
  data: unknown[];
};

type CheckRunsForRefArgs = RepoArgs &
  PageArgs & {
    ref: string;
    check_name?: string;
    status?: 'queued' | 'in_progress' | 'completed';
    filter?: 'latest' | 'all';
    app_id?: number;
  };

type CheckRunsForSuiteArgs = RepoArgs &
  PageArgs & {
    check_suite_id: number;
    check_name?: string;
    status?: 'queued' | 'in_progress' | 'completed';
    filter?: 'latest' | 'all';
  };

type CheckRunAnnotationsArgs = RepoArgs &
  PageArgs & {
    check_run_id: number;
  };

export type RepoContentArgs = RepoArgs & {
  path: string;
  ref?: string;
};

export type RepoBranchArgs = RepoArgs & {
  branch: string;
};

type CombinedStatusForRefArgs = RepoArgs &
  PageArgs & {
    ref: string;
  };
type PullRequestGetResult = {
  data: unknown;
};

type CheckRunsResult = {
  data: {
    total_count?: number;
    check_runs?: unknown[];
  };
};

type CheckRunAnnotationsResult = {
  data: unknown[];
};

export type RepoContentResult = {
  data: unknown;
};

export type RepoBranchResult = {
  data?: {
    commit?: {
      sha?: string | null;
    };
  };
};

export type IssueAddLabelsArgs = IssueArgs & {
  labels: string[];
};

export type IssueRemoveLabelArgs = IssueArgs & {
  name: string;
};

export type IssueUpdateArgs = IssueArgs & {
  title?: string;
  body?: string;
  state?: 'open' | 'closed';
  labels?: string[];
  assignees?: string[];
  milestone?: number | null;
  state_reason?: 'completed' | 'not_planned' | 'reopened' | null;
};

export type IssueAddAssigneesArgs = IssueArgs & {
  assignees: string[];
};

export type IssueMutationResult = unknown;

type CombinedStatusForRefResult = {
  data: {
    state?: string;
    total_count?: number;
    statuses?: unknown[];
  };
};

type PullRequestsOctokitApi = {
  get: (args: PullRequestNumberArgs) => Promise<PullRequestGetResult>;
  list: (args: ListPullRequestsArgs) => Promise<PullRequestListResult>;
  listFiles: (args: PullRequestFilesArgs) => Promise<PullRequestListResult>;
  listCommits: (args: PullRequestCommitsArgs) => Promise<PullRequestListResult>;
  listReviews: (args: PullRequestReviewsArgs) => Promise<PullRequestListResult>;
};

type IssuesLabelOctokitApi = {
  addLabels: (args: IssueAddLabelsArgs) => Promise<IssueMutationResult>;
  removeLabel: (args: IssueRemoveLabelArgs) => Promise<IssueMutationResult>;
};

type IssuesUpdateOctokitApi = {
  update: (args: IssueUpdateArgs) => Promise<IssueMutationResult>;
};

type IssuesAssigneesOctokitApi = {
  addAssignees: (args: IssueAddAssigneesArgs) => Promise<IssueMutationResult>;
};

export type GitHubGatewayIssues = {
  addIssueLabels: (args: IssueAddLabelsArgs) => Promise<IssueMutationResult>;
  removeIssueLabel: (args: IssueRemoveLabelArgs) => Promise<IssueMutationResult>;
  updateIssue: (args: IssueUpdateArgs) => Promise<IssueMutationResult>;
  addIssueAssignees: (args: IssueAddAssigneesArgs) => Promise<IssueMutationResult>;
};

export type GitHubGatewayPullRequests = {
  getPullRequest: (args: PullRequestNumberArgs) => Promise<PullRequestGetResult>;
  listPullRequests: (args: ListPullRequestsArgs) => Promise<PullRequestListResult>;
  listPullRequestFiles: (args: PullRequestFilesArgs) => Promise<PullRequestListResult>;
  listPullRequestCommits: (args: PullRequestCommitsArgs) => Promise<PullRequestListResult>;
  listPullRequestReviews: (args: PullRequestReviewsArgs) => Promise<PullRequestListResult>;
};

type ChecksOctokitApi = {
  listForRef: (args: CheckRunsForRefArgs) => Promise<CheckRunsResult>;
  listForSuite: (args: CheckRunsForSuiteArgs) => Promise<CheckRunsResult>;
  listAnnotations: (args: CheckRunAnnotationsArgs) => Promise<CheckRunAnnotationsResult>;
};

type ReposOctokitApi = {
  getContent: (args: RepoContentArgs) => Promise<RepoContentResult>;
  getBranch: (args: RepoBranchArgs) => Promise<RepoBranchResult>;
  getCombinedStatusForRef: (args: CombinedStatusForRefArgs) => Promise<CombinedStatusForRefResult>;
};
export type GitHubGatewayRepos = {
  getRepoContent: (args: RepoContentArgs) => Promise<RepoContentResult>;
  getBranch: (args: RepoBranchArgs) => Promise<RepoBranchResult>;
  getCombinedStatusForRef: (args: CombinedStatusForRefArgs) => Promise<CombinedStatusForRefResult>;
};

export type GitHubGatewayChecks = {
  listCheckRunsForRef: (args: CheckRunsForRefArgs) => Promise<CheckRunsResult>;
  listCheckRunsForSuite: (args: CheckRunsForSuiteArgs) => Promise<CheckRunsResult>;
  listCheckRunAnnotations: (args: CheckRunAnnotationsArgs) => Promise<CheckRunAnnotationsResult>;
};

export type GitHubGatewayGit = Record<string, never>;

export type GitHubGatewayActions = Record<string, never>;

export type GitHubGateway = {
  readonly issues: GitHubGatewayIssues;
  readonly pullRequests: GitHubGatewayPullRequests;
  readonly repos: GitHubGatewayRepos;
  readonly checks: GitHubGatewayChecks;
  readonly git: GitHubGatewayGit;
  readonly actions: GitHubGatewayActions;
};

export type GitHubGatewayContext = {
  readonly octokit: {
    readonly issues: IssuesLabelOctokitApi & IssuesUpdateOctokitApi & IssuesAssigneesOctokitApi;
    readonly pulls: PullRequestsOctokitApi;
    readonly repos: ReposOctokitApi;
    readonly checks: ChecksOctokitApi;
  };
};

export type GitHubIssueLabelsGatewayContext = {
  readonly octokit: {
    readonly issues: IssuesLabelOctokitApi;
  };
};

export type GitHubIssueUpdateGatewayContext = {
  readonly octokit: {
    readonly issues: IssuesUpdateOctokitApi;
  };
};

export type GitHubIssueAssigneesGatewayContext = {
  readonly octokit: {
    readonly issues: IssuesAssigneesOctokitApi;
  };
};

export type GitHubRepoContentGatewayContext = {
  readonly octokit: {
    readonly repos: {
      getContent: (args: RepoContentArgs) => Promise<RepoContentResult>;
    };
  };
};

export type GitHubRepoBranchGatewayContext = {
  readonly octokit: {
    readonly repos: {
      getBranch: (args: RepoBranchArgs) => Promise<RepoBranchResult>;
    };
  };
};

export function createGitHubRepoContentGateway(
  context: GitHubRepoContentGatewayContext
): Pick<GitHubGatewayRepos, 'getRepoContent'> {
  return {
    getRepoContent: async (args: RepoContentArgs): Promise<RepoContentResult> =>
      await context.octokit.repos.getContent(args),
  };
}

export function createGitHubRepoBranchGateway(
  context: GitHubRepoBranchGatewayContext
): Pick<GitHubGatewayRepos, 'getBranch'> {
  return {
    getBranch: async (args: RepoBranchArgs): Promise<RepoBranchResult> => await context.octokit.repos.getBranch(args),
  };
}

export function createGitHubIssueLabelsGateway(
  context: GitHubIssueLabelsGatewayContext
): Pick<GitHubGatewayIssues, 'addIssueLabels' | 'removeIssueLabel'> {
  return {
    addIssueLabels: async (args: IssueAddLabelsArgs): Promise<IssueMutationResult> =>
      await context.octokit.issues.addLabels(args),
    removeIssueLabel: async (args: IssueRemoveLabelArgs): Promise<IssueMutationResult> =>
      await context.octokit.issues.removeLabel(args),
  };
}

export function createGitHubIssueUpdateGateway(
  context: GitHubIssueUpdateGatewayContext
): Pick<GitHubGatewayIssues, 'updateIssue'> {
  return {
    updateIssue: async (args: IssueUpdateArgs): Promise<IssueMutationResult> =>
      await context.octokit.issues.update(args),
  };
}

export function createGitHubIssueAssigneesGateway(
  context: GitHubIssueAssigneesGatewayContext
): Pick<GitHubGatewayIssues, 'addIssueAssignees'> {
  return {
    addIssueAssignees: async (args: IssueAddAssigneesArgs): Promise<IssueMutationResult> =>
      await context.octokit.issues.addAssignees(args),
  };
}

export function createGitHubGateway(context: GitHubGatewayContext): GitHubGateway {
  return {
    issues: {
      ...createGitHubIssueLabelsGateway(context),
      ...createGitHubIssueUpdateGateway(context),
      ...createGitHubIssueAssigneesGateway(context),
    },
    pullRequests: {
      getPullRequest: async (args: PullRequestNumberArgs): Promise<PullRequestGetResult> =>
        await context.octokit.pulls.get(args),
      listPullRequests: async (args: ListPullRequestsArgs): Promise<PullRequestListResult> =>
        await context.octokit.pulls.list(args),
      listPullRequestFiles: async (args: PullRequestFilesArgs): Promise<PullRequestListResult> =>
        await context.octokit.pulls.listFiles(args),
      listPullRequestCommits: async (args: PullRequestCommitsArgs): Promise<PullRequestListResult> =>
        await context.octokit.pulls.listCommits(args),
      listPullRequestReviews: async (args: PullRequestReviewsArgs): Promise<PullRequestListResult> =>
        await context.octokit.pulls.listReviews(args),
    },
    repos: {
      ...createGitHubRepoContentGateway(context),
      ...createGitHubRepoBranchGateway(context),
      getCombinedStatusForRef: async (args: CombinedStatusForRefArgs): Promise<CombinedStatusForRefResult> =>
        await context.octokit.repos.getCombinedStatusForRef(args),
    },
    checks: {
      listCheckRunsForRef: async (args: CheckRunsForRefArgs): Promise<CheckRunsResult> =>
        await context.octokit.checks.listForRef(args),
      listCheckRunsForSuite: async (args: CheckRunsForSuiteArgs): Promise<CheckRunsResult> =>
        await context.octokit.checks.listForSuite(args),
      listCheckRunAnnotations: async (args: CheckRunAnnotationsArgs): Promise<CheckRunAnnotationsResult> =>
        await context.octokit.checks.listAnnotations(args),
    },
    git: {},
    actions: {},
  };
}
