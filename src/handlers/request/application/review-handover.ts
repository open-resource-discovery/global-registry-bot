import { setStateLabel, ensureAssigneesOnce } from '../state.js';
import { postOnce } from '../comments.js';

type HandoverContext = Parameters<typeof postOnce>[0] &
  Parameters<typeof setStateLabel>[0] &
  Parameters<typeof ensureAssigneesOnce>[0] & {
    octokit: {
      issues: {
        removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
      };
    };
  };

type IssueParams = Parameters<typeof setStateLabel>[1];
type IssueLike = Parameters<typeof setStateLabel>[2];

type EffectiveConstants = {
  globalLabels: string[];
  reviewRequestedLabels: string[];
  labelOnApproved: string | null;
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type ApproverRouting = {
  autoAssigneePoolUsernames: string[];
  approvalUsernames: string[];
};

type ReviewHandoverOptions<ContextType, ParamsType, IssueType> = {
  snapshotHash?: string;
  requestType?: string;
  extraApprovers?: string[];
  manualApproversOverride?: string[];
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstants;
  resolveApproverRoutingForRequestType: (
    context: ContextType,
    requestType: string | undefined,
    approverUsernames: string[],
    approverPoolUsernames: string[]
  ) => ApproverRouting;
  pickAutoAssigneeFromPool: (issue: IssueType, pool: string[]) => string[];
  uniqLogins: (logins: string[]) => string[];
  toStringTrim: (value: unknown) => string;
  ensureAssigneesPresent: (context: ContextType, params: ParamsType, assignees: string[]) => Promise<void>;
  ensureLabelsPresentOnce: (context: ContextType, params: ParamsType, labels: string[]) => Promise<void>;
  buildReviewHandoverBody: (context: ContextType, snapshotHash?: string) => string;
};

export async function handoverToCpa<
  ContextType extends HandoverContext,
  ParamsType extends IssueParams,
  IssueType extends IssueLike,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  _nsType: string,
  _namespace: string,
  _links: string[] = [],
  options: ReviewHandoverOptions<ContextType, ParamsType, IssueType>
): Promise<void> {
  const eff = options.resolveEffectiveConstants(context);

  await setStateLabel(context, params, issue, 'review');

  const approverRouting = options.resolveApproverRoutingForRequestType(
    context,
    options.requestType,
    eff.approverUsernames,
    eff.approverPoolUsernames
  );

  const assigneesForType = approverRouting.autoAssigneePoolUsernames.length
    ? options.pickAutoAssigneeFromPool(issue, approverRouting.autoAssigneePoolUsernames)
    : approverRouting.approvalUsernames;

  const manualApproversOverride = options.uniqLogins(
    (options.manualApproversOverride || []).map((value) => options.toStringTrim(value)).filter(Boolean)
  );

  const mergedAssignees = manualApproversOverride.length
    ? manualApproversOverride
    : options.uniqLogins([...(assigneesForType || []), ...(options.extraApprovers || []).filter(Boolean)]);

  await ensureAssigneesOnce(context, params, issue, mergedAssignees);
  await options.ensureAssigneesPresent(context, params, mergedAssignees);

  const labelsToAdd = [...(eff.globalLabels || []), ...(eff.reviewRequestedLabels || [])].filter(Boolean);

  await options.ensureLabelsPresentOnce(context, params, labelsToAdd);

  if (eff.labelOnApproved) {
    try {
      await context.octokit.issues.removeLabel({
        ...params,
        name: eff.labelOnApproved,
      });
    } catch {
      /* empty */
    }
  }

  const handoverMsg = options.buildReviewHandoverBody(context, options.snapshotHash);

  await postOnce(context, params, handoverMsg, { minimizeTag: 'nsreq:handover' });
}
