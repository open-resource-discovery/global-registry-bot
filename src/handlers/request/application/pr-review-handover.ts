import { setStateLabel, ensureAssigneesOnce } from '../state.js';
import { postOnce } from '../comments.js';
import { postApprovalUnknownOnce } from './approval-outcome-posting.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';

type HandoverContext = Parameters<typeof postOnce>[0] &
  Parameters<typeof setStateLabel>[0] &
  Parameters<typeof ensureAssigneesOnce>[0] & {
    octokit: {
      rest: {
        issues: {
          removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
        };
      };
    };
  };

type RepoInfo = { owner: string; repo: string };
type IssueParams = Parameters<typeof setStateLabel>[1];
type DirectPrApprovalOptions = { baseBranch?: string };

type EffectiveConstants = {
  globalLabels: string[];
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type PrReviewHandoverOptions<ContextType, PullRequestType, IssueType> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstants;
  prAsIssueLike: (pr: PullRequestType) => IssueType;
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  resolveDirectPrRequestTypes: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    options?: DirectPrApprovalOptions
  ) => Promise<string[]>;
  getUnknownManualApprovers: (decision: ApprovalDecision) => string[];
  resolveReviewAssigneesForRequestTypes: (context: ContextType, issue: IssueType, requestTypes: string[]) => string[];
  ensureAssigneesPresent: (context: ContextType, params: IssueParams, assignees: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: IssueParams, labels: string[]) => Promise<void>;
  calcStandaloneDirectPrSnapshotHash: (pr: PullRequestType, changedFiles: string[]) => string;
  buildReviewHandoverBody: (
    context: ContextType,
    snapshotHash?: string,
    options?: { target?: 'issue' | 'pull_request' }
  ) => string;
  toStringTrim: (value: unknown) => string;
  logHandover: (args: {
    context: ContextType;
    prNumber: number;
    requestTypes: string[];
    changedFiles: string[];
    assignees: string[];
    snapshotHash: string;
    decisionStatus: string;
  }) => void;
};

export async function handoverStandaloneDirectPrToReview<
  ContextType extends HandoverContext,
  PullRequestType extends { number: number },
  IssueType,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  decision: ApprovalDecision,
  directPrApprovalOptions: DirectPrApprovalOptions = {},
  options: PrReviewHandoverOptions<ContextType, PullRequestType, IssueType>
): Promise<void> {
  const eff = options.resolveEffectiveConstants(context);
  const params: IssueParams = { owner: repoInfo.owner, repo: repoInfo.repo, issue_number: pr.number };
  const prIssue = options.prAsIssueLike(pr);

  await setStateLabel(context, params, prIssue as Parameters<typeof setStateLabel>[2], 'review');

  const changedFiles = await options.listChangedYamlFilesForPrWithFallback(
    context,
    repoInfo,
    pr,
    directPrApprovalOptions.baseBranch
  );
  const requestTypes = await options.resolveDirectPrRequestTypes(context, repoInfo, pr, directPrApprovalOptions);

  const manualApproversOverride = options.getUnknownManualApprovers(decision);

  const assignees = manualApproversOverride.length
    ? manualApproversOverride
    : options.resolveReviewAssigneesForRequestTypes(context, prIssue, requestTypes);

  if (assignees.length) {
    await ensureAssigneesOnce(context, params, prIssue as Parameters<typeof ensureAssigneesOnce>[2], assignees);
    await options.ensureAssigneesPresent(context, params, assignees);
  }

  const labelsToAdd = [...(eff.globalLabels || []), ...(eff.reviewRequestedLabels || [])].filter(Boolean);
  await options.ensureLabelsPresentOnce(context, params, labelsToAdd);

  if (eff.labelOnApproved) {
    try {
      await context.octokit.rest.issues.removeLabel({
        ...params,
        name: eff.labelOnApproved,
      });
    } catch {
      // ignore
    }
  }

  const snapshotHash = options.calcStandaloneDirectPrSnapshotHash(pr, changedFiles);

  await postOnce(context, params, options.buildReviewHandoverBody(context, snapshotHash, { target: 'pull_request' }), {
    minimizeTag: 'nsreq:handover',
  });

  await postApprovalUnknownOnce(context, params, decision);

  options.logHandover({
    context,
    prNumber: pr.number,
    requestTypes,
    changedFiles,
    assignees,
    snapshotHash,
    decisionStatus: options.toStringTrim(decision.status) || 'none',
  });
}
