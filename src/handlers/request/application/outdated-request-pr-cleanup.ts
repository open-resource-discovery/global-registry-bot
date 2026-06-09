import { toStringTrim } from '../domain/login-utils.js';

type IssueParamsBase = {
  owner: string;
  repo: string;
  issue_number: number;
};

type RepoInfoBase = {
  owner: string;
  repo: string;
};

type IssueLikeBase = {
  body?: string | null;
};

type PullRequestLikeBase = {
  number: number;
  body?: string | null;
  head: {
    ref: string;
  };
};

type TemplateLikeBase = {
  [key: string]: unknown;
};

type FormDataBase = Record<string, string>;

type EffectiveConstantsBase = {
  labelOnApproved?: string | null;
};

type PostOnceOptionsBase = {
  minimizeTag?: string;
};

type OctokitLike<IssueType extends IssueLikeBase> = {
  issues: {
    get: (args: { owner: string; repo: string; issue_number: number }) => Promise<{ data?: IssueType }>;
    removeLabel: (args: { owner: string; repo: string; issue_number: number; name: string }) => Promise<unknown>;
  };
  pulls: {
    update: (args: { owner: string; repo: string; pull_number: number; state: 'closed' }) => Promise<unknown>;
  };
  git: {
    deleteRef: (args: { owner: string; repo: string; ref: string }) => Promise<unknown>;
  };
};

type ContextWithOctokit<IssueType extends IssueLikeBase> = {
  octokit: OctokitLike<IssueType>;
};

export type OutdatedRequestPrCleanupOptions<FormDataType extends FormDataBase> = {
  parsedFormData?: FormDataType;
  currentHash?: string;
  acceptedHashes?: string[];
};

export type OutdatedRequestPrCleanupCallbacks<
  ContextType,
  PullRequestType extends PullRequestLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
> = {
  parseForm: (body: string, template: TemplateType) => FormDataType;
  readIssueBodyForProcessing: (body: unknown) => string;
  buildCompatibleRequestSnapshotHashes: (
    issueBody: unknown,
    parsedFormData: FormDataType,
    template: TemplateType
  ) => string[];
  calcSnapshotHash: (formData: FormDataType, template: TemplateType, rawBody: string) => string;
  extractHashFromPrBody: (body: string) => string;
  findOpenIssuePrs: (context: ContextType, repo: RepoInfoBase, issueNumber: number) => Promise<PullRequestType[]>;
  resolveEffectiveConstants: (context: ContextType) => EffectiveConstantsType;
  postOnce: (
    context: ContextType,
    params: IssueParamsBase,
    body: string,
    options?: PostOnceOptionsBase
  ) => Promise<void>;
};

async function closePr<
  ContextType extends ContextWithOctokit<IssueType>,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
>(context: ContextType, params: ParamsType, prNum: number, ref: string): Promise<void> {
  try {
    await context.octokit.pulls.update({
      owner: params.owner,
      repo: params.repo,
      pull_number: prNum,
      state: 'closed',
    });
  } catch {
    // preserve best-effort close semantics
  }

  try {
    await context.octokit.git.deleteRef({
      owner: params.owner,
      repo: params.repo,
      ref: `heads/${ref}`,
    });
  } catch {
    // preserve best-effort branch cleanup semantics
  }
}

export async function closeOutdatedRequestPrs<
  ContextType extends ContextWithOctokit<IssueType>,
  ParamsType extends IssueParamsBase,
  IssueType extends IssueLikeBase,
  PullRequestType extends PullRequestLikeBase,
  TemplateType extends TemplateLikeBase,
  FormDataType extends FormDataBase,
  EffectiveConstantsType extends EffectiveConstantsBase,
>(
  context: ContextType,
  params: ParamsType,
  template: TemplateType,
  options: OutdatedRequestPrCleanupOptions<FormDataType> = {},
  callbacks: OutdatedRequestPrCleanupCallbacks<
    ContextType,
    PullRequestType,
    TemplateType,
    FormDataType,
    EffectiveConstantsType
  >
): Promise<void> {
  const { owner, repo, issue_number: issueNumber } = params;

  const ensureFormAndHash = async (): Promise<{
    parsedFormData: FormDataType;
    currentHash: string;
    acceptedHashes: string[];
  }> => {
    const { parsedFormData: givenForm, currentHash: givenHash, acceptedHashes: givenAcceptedHashes } = options;

    if (givenForm && givenHash) {
      const acceptedHashes = Array.from(
        new Set((givenAcceptedHashes || [givenHash]).map((value) => toStringTrim(value)).filter(Boolean))
      );

      return {
        parsedFormData: givenForm,
        currentHash: givenHash,
        acceptedHashes: acceptedHashes.length ? acceptedHashes : [givenHash],
      };
    }

    const { data } = await context.octokit.issues.get({ owner, repo, issue_number: issueNumber });
    const issue = (data || {}) as IssueType;
    const bodyStr = callbacks.readIssueBodyForProcessing(issue.body);
    const form = callbacks.parseForm(bodyStr, template);
    const acceptedHashes = callbacks.buildCompatibleRequestSnapshotHashes(issue.body, form, template);
    const currentHash = acceptedHashes[0] || callbacks.calcSnapshotHash(form, template, bodyStr);

    return {
      parsedFormData: form,
      currentHash,
      acceptedHashes,
    };
  };

  const { currentHash, acceptedHashes } = await ensureFormAndHash();
  const acceptedHashSet = new Set((acceptedHashes || []).map((value) => toStringTrim(value)).filter(Boolean));

  if (currentHash) acceptedHashSet.add(currentHash);

  const prs = await callbacks.findOpenIssuePrs(context, { owner, repo }, issueNumber);
  if (!prs.length) return;

  const eff = callbacks.resolveEffectiveConstants(context);
  const onApproved = eff.labelOnApproved;
  const closed: number[] = [];

  for (const pr of prs) {
    const prHash = callbacks.extractHashFromPrBody(toStringTrim(pr.body));

    if (!prHash) continue;
    if (acceptedHashSet.has(prHash)) continue;

    await closePr(context, params, pr.number, pr.head.ref);
    closed.push(pr.number);
  }

  if (!closed.length) return;

  const list = closed.map((n) => `#${n}`).join(', ');
  await callbacks.postOnce(
    context,
    { owner, repo, issue_number: issueNumber },
    `Form updated → closing outdated PR(s): ${list}. Please re-approve to open a new PR.`,
    { minimizeTag: 'nsreq:pr-outdated' }
  );

  if (!onApproved) return;
  try {
    await context.octokit.issues.removeLabel({ owner, repo, issue_number: issueNumber, name: onApproved });
  } catch {
    // preserve best-effort approved-label cleanup semantics
  }
}
