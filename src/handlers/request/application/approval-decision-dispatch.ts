import { normalizeApprovalDecision, type ApprovalDecision } from '../domain/approval-decision.js';
import { postApprovalUnknownOnce } from './approval-outcome-posting.js';

type DispatchContext = Parameters<typeof postApprovalUnknownOnce>[0];
type DispatchParams = Parameters<typeof postApprovalUnknownOnce>[1];

export type ApprovalHandlingResult = 'approved' | 'rejected' | 'continue';

type MaybeHandleApprovalDecisionOptions<ContextType, ParamsType, IssueType, TemplateType, FormDataType> = {
  resolveApprovalDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    requestType: string,
    namespace: string
  ) => Promise<ApprovalDecision | boolean>;
  handleApprovedDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    template: TemplateType,
    parsedFormData: FormDataType,
    decision: ApprovalDecision
  ) => Promise<void>;
  handleRejectedDecision: (
    context: ContextType,
    params: ParamsType,
    issue: IssueType,
    decision: ApprovalDecision
  ) => Promise<void>;
};

export async function maybeHandleApprovalDecision<
  ContextType extends DispatchContext,
  ParamsType extends DispatchParams,
  IssueType,
  TemplateType,
  FormDataType,
>(
  context: ContextType,
  params: ParamsType,
  issue: IssueType,
  template: TemplateType,
  parsedFormData: FormDataType,
  requestType: string,
  namespace: string,
  options: MaybeHandleApprovalDecisionOptions<ContextType, ParamsType, IssueType, TemplateType, FormDataType>
): Promise<ApprovalHandlingResult> {
  const decision = normalizeApprovalDecision(
    await options.resolveApprovalDecision(context, params, issue, template, parsedFormData, requestType, namespace)
  );

  if (decision.status === 'approved') {
    await options.handleApprovedDecision(context, params, issue, template, parsedFormData, decision);
    return 'approved';
  }

  if (decision.status === 'rejected') {
    await options.handleRejectedDecision(context, params, issue, decision);
    return 'rejected';
  }

  if (decision.status === 'unknown') {
    await postApprovalUnknownOnce(context, params, decision);
  }

  return 'continue';
}
