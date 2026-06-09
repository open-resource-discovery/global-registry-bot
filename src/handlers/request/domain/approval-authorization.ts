export function isAuthorizedApprover(
  commenter: string,
  issueAuthor: string | undefined | null,
  allowedApprovers: string[]
): boolean {
  const commenterLc = String(commenter || '').toLowerCase();
  const hasConfiguredApprovers = Array.isArray(allowedApprovers) && allowedApprovers.length > 0;

  if (hasConfiguredApprovers) {
    return allowedApprovers.some((u) => String(u || '').toLowerCase() === commenterLc);
  }

  const issueAuthorLc = String(issueAuthor || '').toLowerCase();
  return Boolean(commenterLc && commenterLc !== issueAuthorLc);
}
