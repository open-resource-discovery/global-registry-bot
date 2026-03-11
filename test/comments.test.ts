/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, expect, it, jest } from '@jest/globals';
import type { IssueCommentLike, ReportedContentClassifier } from '../src/handlers/request/comments.js';

type ListCommentsArgs = {
  owner: string;
  repo: string;
  issue_number: number;
  per_page?: number;
};

type CreateCommentArgs = {
  owner: string;
  repo: string;
  issue_number: number;
  body: string;
};

type ListCommentsFn = (params: ListCommentsArgs) => Promise<{ data: IssueCommentLike[] }>;
type CreateCommentFn = (params: CreateCommentArgs) => Promise<{ data: IssueCommentLike }>;
type GraphqlFn = (
  query: string,
  variables: { subjectId: string; classifier: ReportedContentClassifier }
) => Promise<unknown>;
type WarnFn = (obj: unknown, msg?: string) => void;

type Ctx = {
  octokit: {
    issues: {
      listComments: jest.MockedFunction<ListCommentsFn>;
      createComment: jest.MockedFunction<CreateCommentFn>;
    };
    graphql: jest.MockedFunction<GraphqlFn>;
  };
  log: { warn: jest.MockedFunction<WarnFn> };
  payload?: unknown;
};

type CtxBundle = {
  ctx: Ctx;
  mocks: {
    listComments: jest.MockedFunction<ListCommentsFn>;
    createComment: jest.MockedFunction<CreateCommentFn>;
    graphql: jest.MockedFunction<GraphqlFn>;
    warn: jest.MockedFunction<WarnFn>;
  };
};

function errWithStatus(message: string, status: number): { status: number; message: string } {
  return { status, message };
}

function mkCtx(payload?: unknown): CtxBundle {
  const listComments = jest.fn<ListCommentsFn>(() => Promise.resolve({ data: [] })).mockName('listComments');

  const createComment = jest
    .fn<CreateCommentFn>(() =>
      Promise.resolve({
        data: { id: 999, body: 'x', user: { type: 'Bot', login: 'bot' }, node_id: 'N999' },
      })
    )
    .mockName('createComment');

  const graphql = jest.fn<GraphqlFn>(() => Promise.resolve({ ok: true })).mockName('graphql');

  const warn = jest.fn<WarnFn>(() => undefined).mockName('warn');

  const ctx: Ctx = {
    octokit: { issues: { listComments, createComment }, graphql },
    log: { warn },
    payload,
  };

  return { ctx, mocks: { listComments, createComment, graphql, warn } };
}

type CommentsModule = typeof import('../src/handlers/request/comments.js');

function loadSubject(): Promise<CommentsModule> {
  jest.resetModules();
  return import('../src/handlers/request/comments.js');
}

describe('src/handlers/request/comments.ts', () => {
  it('returns null and warns when owner/repo/issue_number are missing', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    const res = await mod.postOnce(ctx as any, {}, 'hello');
    expect(res).toBeNull();

    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.listComments).not.toHaveBeenCalled();
    expect(mocks.createComment).not.toHaveBeenCalled();
  });

  it('uses payload fallback for owner/repo and pull_number as issueNumber', async () => {
    const mod = await loadSubject();

    const { ctx, mocks } = mkCtx({
      repository: { owner: { login: ' o ' }, name: ' r ' },
    });

    mocks.createComment.mockResolvedValueOnce({
      data: { id: 1, body: 'b', user: { type: 'Bot', login: 'x' }, node_id: 'N1' },
    });

    const out = await mod.postOnce(ctx as any, { pull_number: 7 }, 'b');

    expect(out?.id).toBe(1);

    expect(mocks.listComments).toHaveBeenCalledTimes(1);
    expect(mocks.listComments.mock.calls[0][0]).toEqual({
      owner: 'o',
      repo: 'r',
      issue_number: 7,
      per_page: 10,
    });

    expect(mocks.createComment).toHaveBeenCalledTimes(1);
    expect(mocks.createComment.mock.calls[0][0].issue_number).toBe(7);
  });

  it('creates comment once, caches, second call returns last bot comment without list/create', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({ data: [] });

    const created: IssueCommentLike = {
      id: 101,
      body: 'Hello world',
      user: { type: 'Bot', login: 'github-actions[bot]' },
      node_id: 'NODE101',
    };
    mocks.createComment.mockResolvedValueOnce({ data: created });

    const r1 = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 1 }, 'Hello world');

    expect(r1?.id).toBe(101);
    expect(mocks.listComments).toHaveBeenCalledTimes(1);
    expect(mocks.createComment).toHaveBeenCalledTimes(1);

    mocks.listComments.mockRejectedValueOnce(new Error('should not be called'));

    const r2 = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 1 }, 'Hello world');

    expect(r2?.id).toBe(101);
    expect(mocks.listComments).toHaveBeenCalledTimes(1);
    expect(mocks.createComment).toHaveBeenCalledTimes(1);
  });

  it('returns last bot comment when body matches after normalization (whitespace + CR)', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 5,
          body: 'Hello world\r\n\r\nline2',
          user: { type: 'Bot', login: 'x' },
          node_id: 'N5',
        },
      ],
    });

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 2 }, 'Hello   world\n\nline2');

    expect(out?.id).toBe(5);
    expect(mocks.createComment).not.toHaveBeenCalled();
  });

  it('minimizeTag: uses pageSize max(perPage, 50), appends marker, minimizes older tagged bot comments', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    const tag = 'nsreq:test';
    const marker = `<!-- ${tag} -->`;

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 1,
          body: `Old message\n\n${marker}`,
          user: { type: 'User', login: 'github-actions[bot]' },
          node_id: 'NODE1',
        },
        { id: 2, body: 'No marker', user: { type: 'Bot', login: 'x' }, node_id: 'NODE2' },
        { id: 3, body: `hi\n${marker}`, user: { type: 'User', login: 'human' }, node_id: 'NODE3' },
        { id: 4, body: `hi\n${marker}`, user: { type: 'User', login: 'my-bot' }, node_id: null },
      ],
    });

    const created: IssueCommentLike = {
      id: 10,
      body: `New body\n\n${marker}`,
      user: { type: 'Bot', login: 'github-actions[bot]' },
      node_id: 'NODE10',
    };
    mocks.createComment.mockResolvedValueOnce({ data: created });

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 3 }, 'New body', {
      minimizeTag: tag,
      perPage: 5,
      classifier: 'SPAM',
    });

    expect(out?.id).toBe(10);

    expect(mocks.listComments).toHaveBeenCalledTimes(1);
    expect(mocks.listComments.mock.calls[0][0]).toEqual({
      owner: 'o',
      repo: 'r',
      issue_number: 3,
      per_page: 50,
    });

    expect(mocks.createComment).toHaveBeenCalledTimes(1);
    expect(mocks.createComment.mock.calls[0][0].body).toBe(`New body\n\n${marker}`);

    expect(mocks.graphql).toHaveBeenCalledTimes(1);
    expect(mocks.graphql.mock.calls[0][1]).toEqual({ subjectId: 'NODE1', classifier: 'SPAM' });
  });

  it('listComments secondary rate limit -> warns + returns null', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockRejectedValueOnce(errWithStatus('Secondary rate limit exceeded', 403));

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 4 }, 'body');

    expect(out).toBeNull();
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.createComment).not.toHaveBeenCalled();
  });

  it('listComments non-secondary error -> rethrows', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockRejectedValueOnce(errWithStatus('boom', 500));

    await expect(mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 5 }, 'body')).rejects.toBeTruthy();

    expect(mocks.createComment).not.toHaveBeenCalled();
  });

  it('createComment secondary rate limit -> warns + returns null', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({ data: [] });
    mocks.createComment.mockRejectedValueOnce(errWithStatus('Secondary rate limit exceeded', 429));

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 6 }, 'body');

    expect(out).toBeNull();
    expect(mocks.warn).toHaveBeenCalledTimes(1);
  });

  it('createComment non-secondary error -> rethrows', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({ data: [] });
    mocks.createComment.mockRejectedValueOnce(new Error('boom'));

    await expect(mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 7 }, 'body')).rejects.toThrow('boom');
  });

  it('minimization stops on secondary rate limit graphql errors (break)', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    const tag = 'nsreq:min';
    const marker = `<!-- ${tag} -->`;

    mocks.listComments.mockResolvedValueOnce({
      data: [
        { id: 1, body: `a\n${marker}`, user: { type: 'Bot', login: 'x' }, node_id: 'N1' },
        { id: 2, body: `b\n${marker}`, user: { type: 'Bot', login: 'x' }, node_id: 'N2' },
      ],
    });

    mocks.createComment.mockResolvedValueOnce({
      data: { id: 99, body: `new\n\n${marker}`, user: { type: 'Bot', login: 'x' }, node_id: 'N99' },
    });

    mocks.graphql.mockRejectedValueOnce(new Error('Secondary rate limit hit'));

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 8 }, 'new', {
      minimizeTag: tag,
    });

    expect(out?.id).toBe(99);
    expect(mocks.graphql).toHaveBeenCalledTimes(1);
    expect(mocks.warn).toHaveBeenCalled();
  });

  it('minimization logs and continues on non-secondary graphql errors', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    const tag = 'nsreq:min2';
    const marker = `<!-- ${tag} -->`;

    mocks.listComments.mockResolvedValueOnce({
      data: [
        { id: 1, body: `a\n${marker}`, user: { type: 'Bot', login: 'x' }, node_id: 'N1' },
        { id: 2, body: `b\n${marker}`, user: { type: 'Bot', login: 'x' }, node_id: 'N2' },
      ],
    });

    mocks.createComment.mockResolvedValueOnce({
      data: {
        id: 100,
        body: `new\n\n${marker}`,
        user: { type: 'Bot', login: 'x' },
        node_id: 'N100',
      },
    });

    mocks.graphql.mockRejectedValueOnce(new Error('something else')).mockResolvedValueOnce({ ok: true });

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 9 }, 'new', {
      minimizeTag: tag,
    });

    expect(out?.id).toBe(100);
    expect(mocks.graphql).toHaveBeenCalledTimes(2);
    expect(mocks.warn).toHaveBeenCalled();
  });
  it('collapseBotCommentsByPrefix: returns early when tagPrefix is empty', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx({
      repository: { owner: { login: 'o' }, name: 'r' },
    });

    await mod.collapseBotCommentsByPrefix(ctx as any, { issue_number: 1 }, { tagPrefix: '   ' });

    expect(mocks.listComments).not.toHaveBeenCalled();
    expect(mocks.createComment).not.toHaveBeenCalled();
    expect(mocks.graphql).not.toHaveBeenCalled();
  });

  it('collapseBotCommentsByPrefix: warns when target cannot be resolved', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    await mod.collapseBotCommentsByPrefix(ctx as any, {}, { tagPrefix: 'nsreq:ci-validation:' });

    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.listComments).not.toHaveBeenCalled();
  });

  it('collapseBotCommentsByPrefix: secondary rate limit on listComments warns and returns', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx({
      repository: { owner: { login: 'o' }, name: 'r' },
    });

    mocks.listComments.mockRejectedValueOnce(errWithStatus('Secondary rate limit exceeded', 403));

    await mod.collapseBotCommentsByPrefix(ctx as any, { issue_number: 11 }, { tagPrefix: 'nsreq:ci-validation:' });

    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.createComment).not.toHaveBeenCalled();
  });

  it('collapseBotCommentsByPrefix: collapses matching bot comments, respects keepTags, and minimizes created comment', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx({
      repository: { owner: { login: 'o' }, name: 'r' },
    });

    mocks.listComments
      .mockResolvedValueOnce({
        data: [
          {
            id: 1,
            body: 'issue 1\n\n<!-- nsreq:ci-validation:file-a -->',
            user: { type: 'Bot', login: 'x' },
            node_id: 'N1',
          },
          {
            id: 2,
            body: 'issue 2\n\n<!-- nsreq:ci-validation:file-b -->',
            user: { type: 'Bot', login: 'x' },
            node_id: 'N2',
          },
          {
            id: 3,
            body: 'human\n\n<!-- nsreq:ci-validation:file-c -->',
            user: { type: 'User', login: 'human' },
            node_id: 'N3',
          },
        ],
      })
      .mockResolvedValueOnce({
        data: [
          {
            id: 10,
            body: 'Validation issues resolved.\n\n<!-- nsreq:ci-validation:file-a -->',
            user: { type: 'Bot', login: 'x' },
            node_id: 'NCREATED1',
          },
        ],
      });

    mocks.createComment.mockResolvedValueOnce({
      data: {
        id: 10,
        body: 'Validation issues resolved.\n\n<!-- nsreq:ci-validation:file-a -->',
        user: { type: 'Bot', login: 'x' },
        node_id: 'NCREATED1',
      },
    });

    await mod.collapseBotCommentsByPrefix(
      ctx as any,
      { issue_number: 12 },
      {
        tagPrefix: 'nsreq:ci-validation:',
        keepTags: ['nsreq:ci-validation:file-b'],
      }
    );

    expect(mocks.createComment).toHaveBeenCalledTimes(0);

    expect(mocks.graphql).toHaveBeenCalledTimes(1);
    expect(mocks.graphql.mock.calls[0][1]).toEqual({ subjectId: 'NCREATED1', classifier: 'RESOLVED' });
  });

  it('collapseBotCommentsByPrefix: warns when minimizing created comment fails', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx({
      repository: { owner: { login: 'o' }, name: 'r' },
    });

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 1,
          body: 'issue 1\n\n<!-- nsreq:ci-validation:file-a -->',
          user: { type: 'Bot', login: 'x' },
          node_id: 'N1',
        },
      ],
    });

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 10,
          body: 'Validation issues resolved.\n\n<!-- nsreq:ci-validation:file-a -->',
          user: { type: 'Bot', login: 'x' },
          node_id: 'NCREATED1',
        },
      ],
    });

    mocks.createComment.mockResolvedValueOnce({
      data: {
        id: 10,
        body: 'Validation issues resolved.\n\n<!-- nsreq:ci-validation:file-a -->',
        user: { type: 'Bot', login: 'x' },
        node_id: 'NCREATED1',
      },
    });

    mocks.graphql.mockRejectedValueOnce(new Error('minimize failed'));

    await mod.collapseBotCommentsByPrefix(ctx as any, { issue_number: 13 }, { tagPrefix: 'nsreq:ci-validation:' });

    expect(mocks.warn).toHaveBeenCalledWith({ err: 'minimize failed', nodeId: 'NCREATED1' }, 'minimizeComment failed');
  });

  it('postOnce: handover minimizeTag minimizes all older nsreq bot comments', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 1,
          body: 'older 1\n\n<!-- nsreq:ci-validation:file-a -->',
          user: { type: 'Bot', login: 'x' },
          node_id: 'N1',
        },
        {
          id: 2,
          body: 'older 2\n\n<!-- nsreq:handover -->',
          user: { type: 'Bot', login: 'x' },
          node_id: 'N2',
        },
      ],
    });

    mocks.createComment.mockResolvedValueOnce({
      data: {
        id: 3,
        body: 'handover\n\n<!-- nsreq:handover -->',
        user: { type: 'Bot', login: 'x' },
        node_id: 'N3',
      },
    });

    await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 14 }, 'handover', {
      minimizeTag: 'nsreq:handover',
    });

    expect(mocks.listComments.mock.calls[0][0].per_page).toBe(100);
    expect(mocks.graphql).toHaveBeenCalledTimes(2);
    expect(mocks.graphql.mock.calls[0][1]).toEqual({ subjectId: 'N1', classifier: 'OUTDATED' });
    expect(mocks.graphql.mock.calls[1][1]).toEqual({ subjectId: 'N2', classifier: 'OUTDATED' });
  });

  it('postOnce: identical last human comment still creates a new bot comment', async () => {
    const mod = await loadSubject();
    const { ctx, mocks } = mkCtx(undefined);

    mocks.listComments.mockResolvedValueOnce({
      data: [
        {
          id: 5,
          body: 'same body',
          user: { type: 'User', login: 'human' },
          node_id: 'N5',
        },
      ],
    });

    mocks.createComment.mockResolvedValueOnce({
      data: {
        id: 6,
        body: 'same body',
        user: { type: 'Bot', login: 'x' },
        node_id: 'N6',
      },
    });

    const out = await mod.postOnce(ctx as any, { owner: 'o', repo: 'r', issue_number: 15 }, 'same body');

    expect(out?.id).toBe(6);
    expect(mocks.createComment).toHaveBeenCalledTimes(1);
  });
});
