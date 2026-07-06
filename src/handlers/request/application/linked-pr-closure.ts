import { findOpenIssuePrs } from '../pr/snapshot.js';

type SnapshotContext = Parameters<typeof findOpenIssuePrs>[0];
type LinkedPrClosureRepoInfo = Parameters<typeof findOpenIssuePrs>[1];

type LinkedPrClosureContext = SnapshotContext & {
  octokit: SnapshotContext['octokit'] & {
    pulls: SnapshotContext['octokit']['pulls'] & {
      update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
    };
  };
};

type CloseLinkedIssuePrsOptions<
  ContextType extends LinkedPrClosureContext,
  RepoInfoType extends LinkedPrClosureRepoInfo,
  PullRequestType extends { number: number },
> = {
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfoType) => Promise<PullRequestType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfoType) => number | null;
};

export async function closeLinkedIssuePrs<
  ContextType extends LinkedPrClosureContext,
  RepoInfoType extends LinkedPrClosureRepoInfo,
  PullRequestType extends { number: number },
>(
  context: ContextType,
  repoInfo: RepoInfoType,
  issueNumber: number,
  options: CloseLinkedIssuePrsOptions<ContextType, RepoInfoType, PullRequestType>
): Promise<number[]> {
  let prs: PullRequestType[];

  try {
    prs = (await findOpenIssuePrs(context, repoInfo, issueNumber)) as unknown as PullRequestType[];
  } catch {
    prs = [];
  }

  if (prs.length === 0) {
    prs = (await options.listOpenPullRequests(context, repoInfo)).filter(
      (pr) => options.parseLinkedIssueNumberFromPr(pr, repoInfo) === issueNumber
    );
  }

  const closed: number[] = [];

  for (const pr of prs) {
    try {
      await context.octokit.pulls.update({
        owner: repoInfo.owner,
        repo: repoInfo.repo,
        pull_number: pr.number,
        state: 'closed',
      });
      closed.push(pr.number);
    } catch {
      // ignore
    }
  }

  return closed;
}
