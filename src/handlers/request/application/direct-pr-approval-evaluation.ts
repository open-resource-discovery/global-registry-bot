import type { ApprovalDecision } from '../domain/approval-decision.js';
import { toStringTrim } from '../domain/login-utils.js';
import {
  evaluateChangedResourceApproval,
  type DirectPrChangedResourceApprovalCallbacks,
} from './direct-pr-changed-resource-approval.js';
import {
  resolvePullRequestRequestAuthorId,
  type PullRequestAuthorResolutionContext,
} from './pull-request-author-resolution.js';

type RepoInfo = { owner: string; repo: string };

type DirectPrApprovalOptions = {
  baseBranch?: string;
};

type PullRequestLike = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: { login?: string | null } | null;
  head?: { sha?: string | null; ref?: string | null } | null;
};

export type DirectPrApprovalEvaluationCallbacks<ContextType, PullRequestType extends PullRequestLike> = {
  listChangedYamlFilesForPrWithFallback: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    baseBranch?: string
  ) => Promise<string[]>;
  changedResourceApprovalCallbacks: DirectPrChangedResourceApprovalCallbacks<ContextType, PullRequestType>;
  logStart: (
    context: ContextType,
    args: { repoInfo: RepoInfo; pr: PullRequestType; requestAuthorId: string; changedFiles: string[] }
  ) => void;
  logSkipNoRegistryFiles: (context: ContextType, args: { pr: PullRequestType }) => void;
  logFileDecision: (
    context: ContextType,
    args: { pr: PullRequestType; filePath: string; requestAuthorId: string; decision: ApprovalDecision }
  ) => void;
};

export async function evaluateDirectPrOnApproval<
  ContextType extends PullRequestAuthorResolutionContext,
  PullRequestType extends PullRequestLike,
>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  requestAuthorIdOverride: string | undefined,
  options: DirectPrApprovalOptions = {},
  callbacks: DirectPrApprovalEvaluationCallbacks<ContextType, PullRequestType>
): Promise<ApprovalDecision> {
  const changedFiles = await callbacks.listChangedYamlFilesForPrWithFallback(context, repoInfo, pr, options.baseBranch);

  const fallbackRequestAuthorId = requestAuthorIdOverride
    ? ''
    : await resolvePullRequestRequestAuthorId(context, repoInfo, pr);

  const requestAuthorId = toStringTrim(requestAuthorIdOverride) || fallbackRequestAuthorId;

  callbacks.logStart(context, { repoInfo, pr, requestAuthorId, changedFiles });

  if (!changedFiles.length) {
    callbacks.logSkipNoRegistryFiles(context, { pr });
    return {};
  }

  let sawApproved = false;
  let sawNonApproved = false;
  let approvedComment = '';
  let firstUnknownDecision: ApprovalDecision | null = null;

  for (const filePath of changedFiles) {
    const decision = await evaluateChangedResourceApproval(
      context,
      repoInfo,
      pr,
      filePath,
      requestAuthorId,
      callbacks.changedResourceApprovalCallbacks
    );

    callbacks.logFileDecision(context, { pr, filePath, requestAuthorId, decision });

    if (decision.status === 'rejected') {
      return decision;
    }

    if (decision.status === 'approved') {
      sawApproved = true;
      if (!approvedComment) approvedComment = toStringTrim(decision.comment);
      continue;
    }

    sawNonApproved = true;
    if (decision.status === 'unknown' && !firstUnknownDecision) {
      firstUnknownDecision = decision;
    }
  }

  if (sawApproved && !sawNonApproved) {
    return {
      status: 'approved',
      ...(approvedComment ? { comment: approvedComment } : {}),
    };
  }

  if (firstUnknownDecision) {
    return firstUnknownDecision;
  }

  if (sawNonApproved) {
    return {
      status: 'unknown',
      reason: 'Manual review required because onApproval did not approve all changed registry files.',
    };
  }

  return {};
}
