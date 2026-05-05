import { postOnce } from '../comments.js';
import { buildApprovalRejectedBody, buildApprovalUnknownBody } from '../domain/approval-comment-rendering.js';
import { type ApprovalDecision } from '../domain/approval-decision.js';

type ApprovalOutcomePostingContext = Parameters<typeof postOnce>[0];
type ApprovalOutcomePostingParams = Parameters<typeof postOnce>[1];

const ON_APPROVAL_UNKNOWN_POST_INFLIGHT = new Map<string, Promise<void>>();

function issueScopedKey(params: ApprovalOutcomePostingParams, suffix: string): string {
  return `${params.owner}/${params.repo}#${params.issue_number}:${suffix}`.toLowerCase();
}

export async function postApprovalUnknownOnce(
  context: ApprovalOutcomePostingContext,
  params: ApprovalOutcomePostingParams,
  decision: ApprovalDecision
): Promise<void> {
  const key = issueScopedKey(params, 'on-approval-unknown');
  const existing = ON_APPROVAL_UNKNOWN_POST_INFLIGHT.get(key);
  if (existing) {
    await existing;
    return;
  }

  const pending = (async (): Promise<void> => {
    await postOnce(context, params, buildApprovalUnknownBody(decision), {
      minimizeTag: 'nsreq:on-approval:unknown',
    });
  })().finally(() => {
    ON_APPROVAL_UNKNOWN_POST_INFLIGHT.delete(key);
  });

  ON_APPROVAL_UNKNOWN_POST_INFLIGHT.set(key, pending);
  await pending;
}

export async function postApprovalRejectedOnce(
  context: ApprovalOutcomePostingContext,
  params: ApprovalOutcomePostingParams,
  decision: ApprovalDecision,
  options: { bodySuffix?: string } = {}
): Promise<void> {
  await postOnce(context, params, `${buildApprovalRejectedBody(decision)}${options.bodySuffix || ''}`, {
    minimizeTag: 'nsreq:on-approval:rejected',
  });
}
