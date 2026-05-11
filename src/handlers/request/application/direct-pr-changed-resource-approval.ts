import {
  normalizeApprovalDecision,
  promoteUnknownApprovalDecisionForDirectPrRequester,
  type ApprovalDecision,
} from '../domain/approval-decision.js';
import { buildFormDataFromRegistryDoc, resolveRegistryDocResourceName } from '../domain/direct-pr-resource-mapping.js';

type RepoInfo = { owner: string; repo: string };

type PullRequestLike = {
  number: number;
  title?: string | null;
  body?: string | null;
  state?: string | null;
  user?: { login?: string | null } | null;
};

type LabelLike = string | { name?: string | null };

type IssueLike = {
  number: number;
  title?: string | null;
  state?: string | null;
  body?: string | null;
  labels?: LabelLike[];
  user?: { login?: string | null } | null;
};

type FormData = Record<string, string>;

export type DirectPrChangedResourceApprovalCallbacks<ContextType, PullRequestType extends PullRequestLike> = {
  readRegistryDocForApproval: (
    context: ContextType,
    repoInfo: RepoInfo,
    pr: PullRequestType,
    filePath: string
  ) => Promise<Record<string, unknown> | null>;
  pickRequestTypeForChangedResource: (context: ContextType, filePath: string, doc: Record<string, unknown>) => string;
  runApprovalHook: (
    context: ContextType,
    repoInfo: RepoInfo,
    args: {
      requestType: string;
      namespace?: string | null;
      resourceName?: string | null;
      formData: FormData;
      issue: IssueLike;
      requestAuthorId?: string | null;
    }
  ) => Promise<ApprovalDecision | boolean>;
  logRegistryDocReadFailed: (
    context: ContextType,
    args: { repoInfo: RepoInfo; pr: PullRequestType; filePath: string }
  ) => void;
};

export async function evaluateChangedResourceApproval<ContextType, PullRequestType extends PullRequestLike>(
  context: ContextType,
  repoInfo: RepoInfo,
  pr: PullRequestType,
  filePath: string,
  requestAuthorId: string | undefined,
  callbacks: DirectPrChangedResourceApprovalCallbacks<ContextType, PullRequestType>
): Promise<ApprovalDecision> {
  const parsed = await callbacks.readRegistryDocForApproval(context, repoInfo, pr, filePath);
  if (!parsed) {
    callbacks.logRegistryDocReadFailed(context, { repoInfo, pr, filePath });
    return { status: 'unknown' };
  }

  const requestType = callbacks.pickRequestTypeForChangedResource(context, filePath, parsed);
  if (!requestType) return { status: 'unknown' };

  const resourceName = resolveRegistryDocResourceName(parsed);
  if (!resourceName) return { status: 'unknown' };

  const decision = normalizeApprovalDecision(
    await callbacks.runApprovalHook(context, repoInfo, {
      requestType,
      namespace: resourceName,
      resourceName,
      formData: buildFormDataFromRegistryDoc(parsed),
      requestAuthorId,
      issue: {
        number: pr.number,
        title: pr.title,
        body: pr.body,
        state: pr.state,
        user: pr.user,
        labels: [],
      },
    })
  );

  return promoteUnknownApprovalDecisionForDirectPrRequester(decision, requestAuthorId);
}
