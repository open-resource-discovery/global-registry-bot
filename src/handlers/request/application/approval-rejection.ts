import { closeLinkedIssuePrs } from './linked-pr-closure.js';
import { postApprovalRejectedOnce } from './approval-outcome-posting.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';

type ApprovalPostingContext = Parameters<typeof postApprovalRejectedOnce>[0];

type RepoInfo = { owner: string; repo: string };
type IssueParams = { owner: string; repo: string; issue_number: number };
type IssueLike = { number: number; state?: string | null };

type ApprovalRejectionContext<PullRequestType extends { number: number }> = {
  state?: Record<string, unknown>;
  octokit: {
    rest: {
      pulls: {
        list: (args: {
          owner: string;
          repo: string;
          state: 'open' | 'closed' | 'all';
          per_page?: number;
          page?: number;
        }) => Promise<{ data: PullRequestType[] }>;
        update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
      };
      issues: { update: (args: IssueParams & { state: 'closed' }) => Promise<unknown> };
    };
  };
};

type RejectRequestFromApprovalHookOptions<
  ContextType extends ApprovalRejectionContext<PullRequestType>,
  PullRequestType extends { number: number },
> = {
  closeLinkedPrs?: boolean;
  minimizeTag?: string;
  listOpenPullRequests: (context: ContextType, repoInfo: RepoInfo) => Promise<PullRequestType[]>;
  parseLinkedIssueNumberFromPr: (pr: PullRequestType, repoInfo?: RepoInfo) => number | null;
};

export async function rejectRequestFromApprovalHook<
  PullRequestType extends { number: number },
  ContextType extends ApprovalRejectionContext<PullRequestType>,
>(
  context: ContextType,
  params: IssueParams,
  issue: IssueLike,
  decision: ApprovalDecision,
  options: RejectRequestFromApprovalHookOptions<ContextType, PullRequestType>
): Promise<void> {
  const repoInfo = { owner: params.owner, repo: params.repo };

  let closedPrs: number[] = [];
  if (options.closeLinkedPrs) {
    try {
      closedPrs = await closeLinkedIssuePrs(context, repoInfo, issue.number, {
        listOpenPullRequests: options.listOpenPullRequests,
        parseLinkedIssueNumberFromPr: options.parseLinkedIssueNumberFromPr,
      });
    } catch {
      // ignore
    }
  }

  const closedPrRefs = closedPrs.map((n) => `#${n}`).join(', ');
  const closedPrSection = closedPrs.length ? `\n\nClosed linked PR(s): ${closedPrRefs}.` : '';

  await postApprovalRejectedOnce(context as unknown as ApprovalPostingContext, params, decision, {
    bodySuffix: closedPrSection,
  });

  try {
    await context.octokit.rest.issues.update({ ...params, state: 'closed' });
    issue.state = 'closed';
  } catch {
    // ignore
  }
}
