type ReviewHandoverTarget = 'issue' | 'pull_request';

type BuildReviewHandoverBodyOptions = {
  target?: ReviewHandoverTarget;
};

export function buildReviewHandoverBody(
  docsLinks: string,
  snapshotHash?: string,
  options: BuildReviewHandoverBodyOptions = {}
): string {
  const docsSection = docsLinks ? `\n\n${docsLinks.trim()}` : '';
  const snapshotMarker = snapshotHash ? `\n\n<!-- nsreq:snapshot:${snapshotHash} -->` : '';

  const target = options.target || 'issue';
  const instruction =
    target === 'pull_request'
      ? 'Once reviewed, please comment `Approved` to approve this PR for merge.'
      : 'Once reviewed, please comment `Approved` to create an automatic Pull Request.';

  return `### ✅ No issues detected

### ➡️ Routing to an approver for review

---

${instruction}${docsSection}${snapshotMarker}`;
}
