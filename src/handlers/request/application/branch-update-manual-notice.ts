import { postOnce } from '../comments.js';

type PostOnceContext = Parameters<typeof postOnce>[0];

type RepoInfo = {
  owner: string;
  repo: string;
};

export async function postManualBranchUpdateNotice(
  context: PostOnceContext,
  repoInfo: RepoInfo,
  prNumber: number,
  message: string
): Promise<void> {
  await postOnce(
    context,
    { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: prNumber },
    `## Could not update PR branch automatically

The PR is approved, but the bot could not update the branch with the latest base branch.

Reason:
\`${message}\`

Please update the branch manually.`,
    { minimizeTag: 'nsreq:update-branch-failed' }
  );
}
