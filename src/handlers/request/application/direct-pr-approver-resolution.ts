type EffectiveConstants = {
  approverUsernames: string[];
  approverPoolUsernames: string[];
};

type RoutingResult = {
  approvalUsernames: string[];
  autoAssigneePoolUsernames: string[];
};

export type DirectPrApproverResolutionCallbacks<ContextType> = {
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstants;
  resolveApproverRoutingForRequestType: (
    context: ContextType,
    requestType: string | undefined | null,
    fallbackApprovers: string[],
    fallbackApproversPool: string[]
  ) => RoutingResult;
  uniqLogins: (values: string[]) => string[];
  toStringTrim: (value: unknown) => string;
};

export function resolveAllowedApproversForRequestTypes<ContextType>(
  context: ContextType,
  requestTypes: string[],
  callbacks: DirectPrApproverResolutionCallbacks<ContextType>
): string[] {
  const eff = callbacks.resolveEffectiveConstants(context);
  const types = Array.from(new Set((requestTypes || []).map(callbacks.toStringTrim).filter(Boolean)));

  if (!types.length) {
    return callbacks.resolveApproverRoutingForRequestType(context, '', eff.approverUsernames, eff.approverPoolUsernames)
      .approvalUsernames;
  }

  return callbacks.uniqLogins(
    types.flatMap(
      (requestType) =>
        callbacks.resolveApproverRoutingForRequestType(
          context,
          requestType,
          eff.approverUsernames,
          eff.approverPoolUsernames
        ).approvalUsernames
    )
  );
}
