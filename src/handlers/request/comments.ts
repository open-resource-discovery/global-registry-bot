const POST_ONCE_COMMENTS_CACHE = new Map<string, IssueCommentLike[]>();

function clearPostOnceCacheForIssue(owner: string, repo: string, issueNumber: number): void {
  const prefix = `${owner}/${repo}#${issueNumber}:`;
  for (const key of POST_ONCE_COMMENTS_CACHE.keys()) {
    if (key.startsWith(prefix)) POST_ONCE_COMMENTS_CACHE.delete(key);
  }
}

export type ReportedContentClassifier = 'OUTDATED' | 'RESOLVED' | 'DUPLICATE' | 'OFF_TOPIC' | 'SPAM' | 'ABUSE';

export type PostOnceOptions = {
  perPage?: number;
  minimizeTag?: string;
  classifier?: ReportedContentClassifier;
};

export type CollapseBotCommentsByPrefixOptions = {
  perPage?: number;
  tagPrefix: string;
  keepTags?: string[];
  collapseBody?: string;
  classifier?: ReportedContentClassifier;
};

export type PostOnceParams = {
  owner?: string;
  repo?: string;
  issue_number?: number;
  pull_number?: number;
};

type CommentUserLike = {
  type?: string | null;
  login?: string | null;
};

export type IssueCommentLike = {
  id: number;
  body?: string | null;
  user?: CommentUserLike | null;
  node_id?: string | null;
};

type OctokitIssuesApi = {
  listComments: (params: {
    owner: string;
    repo: string;
    issue_number: number;
    per_page?: number;
  }) => Promise<{ data: IssueCommentLike[] }>;
  createComment: (params: {
    owner: string;
    repo: string;
    issue_number: number;
    body: string;
  }) => Promise<{ data: IssueCommentLike }>;
};

type OctokitLike = {
  issues: OctokitIssuesApi;
  graphql: (query: string, variables: { subjectId: string; classifier: ReportedContentClassifier }) => Promise<unknown>;
};

type RepoPayloadLike = {
  repository?: {
    owner?: { login?: string | null } | null;
    name?: string | null;
  } | null;
};

type LoggerLike = {
  warn?: (obj: unknown, msg?: string) => void;
};

type ContextLike = {
  octokit: OctokitLike;
  log?: LoggerLike;
  payload?: RepoPayloadLike;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toStringTrim(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).trim();
  return '';
}

function getHttpStatus(err: unknown): number | undefined {
  if (!isPlainObject(err)) return undefined;
  const status = err['status'];
  return typeof status === 'number' ? status : undefined;
}

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (isPlainObject(err) && typeof err['message'] === 'string') return String(err['message']);
  return String(err);
}

function isSecondaryRateLimit(err: unknown): boolean {
  const status = getHttpStatus(err);
  const msg = getErrorMessage(err);
  if (status !== 403 && status !== 429) return false;
  return /secondary rate limit/i.test(msg);
}

function normalizeBody(s: unknown): string {
  return toStringTrim(s).replaceAll('\r', '').trim().replaceAll(/\s+/g, ' ');
}

function isBotUser(u: CommentUserLike | null | undefined): boolean {
  const type = toStringTrim(u?.type);
  const login = toStringTrim(u?.login);
  return type === 'Bot' || /(\[bot\]|-bot)$/i.test(login);
}

const MINIMIZE_MUTATION = `
mutation($subjectId: ID!, $classifier: ReportedContentClassifiers!) {
  minimizeComment(input: { subjectId: $subjectId, classifier: $classifier }) {
    minimizedComment { isMinimized minimizedReason }
  }
}
`;

function extractCommentMarkers(body: unknown): string[] {
  const text = String(body || '');
  const out = new Set<string>();
  const re = /<!--\s*([^>]+?)\s*-->/g;

  for (const m of text.matchAll(re)) {
    const tag = toStringTrim(m[1]);
    if (tag) out.add(tag);
  }

  return Array.from(out);
}

function resolveCommentTarget(
  context: ContextLike,
  params: PostOnceParams
): { owner: string; repo: string; issueNumber: number | null } {
  const owner = toStringTrim(params.owner ?? context.payload?.repository?.owner?.login);
  const repo = toStringTrim(params.repo ?? context.payload?.repository?.name);
  const issueNumber =
    typeof (params.issue_number ?? params.pull_number) === 'number'
      ? ((params.issue_number ?? params.pull_number) as number)
      : null;

  return { owner, repo, issueNumber };
}

async function minimizeCommentByNodeId(
  context: ContextLike,
  nodeId: string,
  classifier: ReportedContentClassifier
): Promise<void> {
  try {
    await context.octokit.graphql(MINIMIZE_MUTATION, { subjectId: nodeId, classifier });
  } catch (err: unknown) {
    context.log?.warn?.({ err: getErrorMessage(err), nodeId }, 'minimizeComment failed');
  }
}

export async function collapseBotCommentsByPrefix(
  context: ContextLike,
  params: PostOnceParams,
  options: CollapseBotCommentsByPrefixOptions
): Promise<void> {
  const perPage = typeof options.perPage === 'number' ? options.perPage : 100;
  const tagPrefix = toStringTrim(options.tagPrefix);
  const keepTags = new Set(
    (Array.isArray(options.keepTags) ? options.keepTags : []).map((x) => toStringTrim(x)).filter(Boolean)
  );
  const collapseBody = toStringTrim(options.collapseBody) || 'Validation issues resolved.';
  const classifier: ReportedContentClassifier = options.classifier ?? 'RESOLVED';

  if (!tagPrefix) return;

  const { owner, repo, issueNumber } = resolveCommentTarget(context, params);
  if (!owner || !repo || issueNumber === null) {
    context.log?.warn?.({ owner, repo, issueNumber }, 'collapseBotCommentsByPrefix: missing owner/repo/issue_number');
    return;
  }

  clearPostOnceCacheForIssue(owner, repo, issueNumber);

  let comments: IssueCommentLike[] = [];
  try {
    const res = await context.octokit.issues.listComments({
      owner,
      repo,
      issue_number: issueNumber,
      per_page: perPage,
    });
    comments = Array.isArray(res.data) ? res.data : [];
  } catch (err: unknown) {
    if (isSecondaryRateLimit(err)) {
      context.log?.warn?.(
        { err: getErrorMessage(err), owner, repo, issueNumber },
        'collapseBotCommentsByPrefix:listComments hit secondary rate limit'
      );
      return;
    }
    throw err;
  }

  const tagsToCollapse = new Set<string>();

  for (const c of comments) {
    if (!isBotUser(c.user)) continue;

    for (const marker of extractCommentMarkers(c.body)) {
      if (!marker.startsWith(tagPrefix)) continue;
      if (keepTags.has(marker)) continue;
      tagsToCollapse.add(marker);
    }
  }

  for (const tag of tagsToCollapse) {
    const collapsed = await postOnce(context, { owner, repo, issue_number: issueNumber }, collapseBody, {
      perPage,
      minimizeTag: tag,
      classifier,
    });

    const nodeId = toStringTrim(collapsed?.node_id);
    if (nodeId) {
      await minimizeCommentByNodeId(context, nodeId, classifier);
    }
  }

  clearPostOnceCacheForIssue(owner, repo, issueNumber);
}

export async function postOnce(
  context: ContextLike,
  params: PostOnceParams,
  body: string,
  options: PostOnceOptions = {}
): Promise<IssueCommentLike | null> {
  const perPage = typeof options.perPage === 'number' ? options.perPage : 10;
  const minimizeTag = typeof options.minimizeTag === 'string' ? options.minimizeTag : undefined;
  const classifier: ReportedContentClassifier = options.classifier ?? 'OUTDATED';

  const tagMarker = minimizeTag ? `<!-- ${minimizeTag} -->` : '';
  const taggedBody = minimizeTag ? `${body}\n\n${tagMarker}` : body;

  // Catch previous bot comments
  const pageSize = minimizeTag === 'nsreq:handover' ? 100 : minimizeTag ? Math.max(perPage, 50) : perPage;

  const owner = toStringTrim(params.owner ?? context.payload?.repository?.owner?.login);
  const repo = toStringTrim(params.repo ?? context.payload?.repository?.name);
  const issueNumber = params.issue_number ?? params.pull_number;

  if (!owner || !repo || typeof issueNumber !== 'number') {
    context.log?.warn?.({ owner, repo, issueNumber }, 'postOnce: missing owner/repo/issue_number - skipping comment');
    return null;
  }

  const cacheKey = `${owner}/${repo}#${issueNumber}:${pageSize}:${tagMarker || 'no-tag'}`;

  let comments: IssueCommentLike[];

  const cached = POST_ONCE_COMMENTS_CACHE.get(cacheKey);
  if (cached) {
    comments = cached;
  } else {
    try {
      const res = await context.octokit.issues.listComments({
        owner,
        repo,
        issue_number: issueNumber,
        per_page: pageSize,
      });
      comments = Array.isArray(res.data) ? res.data : [];
      POST_ONCE_COMMENTS_CACHE.set(cacheKey, comments);
    } catch (err: unknown) {
      if (isSecondaryRateLimit(err)) {
        context.log?.warn?.(
          { err: getErrorMessage(err), cacheKey },
          'postOnce:listComments hit secondary rate limit, skipping comment'
        );
        return null;
      }
      throw err;
    }
  }

  const last = comments.length ? comments[comments.length - 1] : null;
  if (last && isBotUser(last.user) && normalizeBody(last.body) === normalizeBody(taggedBody)) {
    return last;
  }

  let created: IssueCommentLike;
  try {
    const res = await context.octokit.issues.createComment({
      owner,
      repo,
      issue_number: issueNumber,
      body: taggedBody,
    });
    created = res.data;

    // Update cache so follow-up postOnce calls see the new comment
    comments.push(created);
    POST_ONCE_COMMENTS_CACHE.set(cacheKey, comments);
  } catch (err: unknown) {
    if (isSecondaryRateLimit(err)) {
      context.log?.warn?.(
        { err: getErrorMessage(err), cacheKey },
        'postOnce:createComment hit secondary rate limit, skipping comment'
      );
      return null;
    }
    throw err;
  }

  if (minimizeTag && comments.length) {
    const minimizeAllNsreqComments = minimizeTag === 'nsreq:handover';
    const nsreqPrefixMarker = '<!-- nsreq:';

    for (const c of comments) {
      const isOlder = c.id !== created.id;
      const bodyStr = typeof c.body === 'string' ? c.body : '';
      const isTagged = minimizeAllNsreqComments ? bodyStr.includes(nsreqPrefixMarker) : bodyStr.includes(tagMarker);
      const isBotAuthor = isBotUser(c.user);
      const nodeId = toStringTrim(c.node_id);

      if (!isOlder || !isTagged || !isBotAuthor || !nodeId) continue;

      try {
        await context.octokit.graphql(MINIMIZE_MUTATION, { subjectId: nodeId, classifier });
      } catch (err: unknown) {
        const msg = getErrorMessage(err);

        // If GraphQL also runs into secondary limit, just stop minimizing
        if (/secondary rate limit/i.test(msg)) {
          context.log?.warn?.(
            { err: msg, commentId: c.id },
            'postOnce:minimize hit secondary rate limit, stopping minimization'
          );
          break;
        }

        context.log?.warn?.({ err: msg, commentId: c.id }, 'minimizeComment failed');
      }
    }
  }

  return created;
}
